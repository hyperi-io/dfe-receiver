// Project:   dfe-receiver
// File:      src/buffer/mod.rs
// Purpose:   Memory buffer and disk spillover
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Buffer management with memory pressure detection.
//!
//! Provides in-memory batching with disk spillover when under pressure.

pub mod adapter;
pub mod rejects;
pub mod tiered;

pub use rejects::{Disposal, Rejects};
pub use tiered::{InMemoryBuffer, InMemoryBufferStats};
// Re-export CircuitState from scalo for convenience
pub use scalo::tiered_sink::CircuitState;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::sink::Sink as ReceiverSink;

/// Backend for message delivery — in-memory only or with disk spillover.
pub enum SinkBackend<S: ReceiverSink + 'static> {
    /// In-memory buffer with circuit breaker (default, no disk).
    InMemory(InMemoryBuffer<S>),
    /// scalo TieredSink with disk spool (opt-in via spillover config).
    Tiered(scalo::tiered_sink::TieredSink<adapter::ScaloSinkAdapter<S>>),
}

#[async_trait]
impl<S: ReceiverSink + 'static> ReceiverSink for SinkBackend<S> {
    async fn send(&self, topic: &str, payload: Bytes) -> crate::error::Result<()> {
        match self {
            SinkBackend::InMemory(buf) => buf.send(topic, payload).await,
            SinkBackend::Tiered(tiered) => {
                // Record-native spill: carry the topic as the routing key so the
                // TieredSink spills the whole Record (key + payload) and replays
                // it to the right topic on drain -- no bespoke byte framing.
                let record = scalo::transport::Record {
                    payload,
                    key: Some(Arc::from(topic)),
                    headers: Vec::new(),
                    metadata: scalo::transport::RecordMeta {
                        timestamp_ms: None,
                        format: scalo::transport::PayloadFormat::Auto,
                    },
                };
                tiered
                    .send(&record)
                    .await
                    .map_err(|e| crate::error::Error::Transport(e.to_string()))
            }
        }
    }

    async fn send_batch(&self, topic: &str, payloads: &[Bytes]) -> crate::error::Result<()> {
        match self {
            SinkBackend::InMemory(buf) => buf.send_batch(topic, payloads).await,
            // TieredSink decides hot path or spool per record.
            SinkBackend::Tiered(_) => {
                for payload in payloads {
                    self.send(topic, payload.clone()).await?;
                }
                Ok(())
            }
        }
    }

    fn refuses(&self, payload: &Bytes) -> Option<String> {
        match self {
            SinkBackend::InMemory(buf) => buf.refuses(payload),
            SinkBackend::Tiered(tiered) => tiered.inner().inner().refuses(payload),
        }
    }

    /// Flush the buffer into the primary, then the primary. The spool keeps
    /// what it holds for the next start.
    async fn flush(&self) -> crate::error::Result<()> {
        match self {
            SinkBackend::InMemory(buf) => buf.flush().await,
            SinkBackend::Tiered(tiered) => tiered.inner().inner().flush().await,
        }
    }

    fn is_healthy(&self) -> bool {
        match self {
            SinkBackend::InMemory(buf) => buf.is_healthy(),
            SinkBackend::Tiered(tiered) => {
                // Healthy if disk is available (sync check — cannot call async from sync)
                tiered.is_disk_available()
            }
        }
    }
}

impl<S: ReceiverSink + 'static> SinkBackend<S> {
    /// Get stats from the active backend.
    pub async fn stats(&self) -> InMemoryBufferStats {
        match self {
            SinkBackend::InMemory(buf) => buf.stats().await,
            SinkBackend::Tiered(tiered) => {
                // Map scalo stats into InMemoryBufferStats for compatibility
                let circuit_state = tiered.circuit_state().await;
                InMemoryBufferStats {
                    circuit_state,
                    consecutive_failures: 0, // scalo doesn't expose this directly
                    queue_size: tiered.spool_len().await,
                    queued_total: tiered.cold_path_count(),
                    drained_total: tiered.hot_path_count(),
                }
            }
        }
    }

    /// Start background drain task.
    ///
    /// For InMemory: spawns the existing drain loop from `InMemoryBuffer`.
    /// For Tiered: scalo manages its own drain task internally - this is a no-op.
    pub fn start_drain_task(self: Arc<Self>, shutdown: CancellationToken) {
        match self.as_ref() {
            SinkBackend::InMemory(_) => {
                // InMemoryBuffer needs its own drain loop: the tick moves the
                // queue, and only shutdown flushes the primary as well.
                let backend = Arc::clone(&self);
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                info!("Drain task stopping");
                                if let SinkBackend::InMemory(buf) = backend.as_ref() {
                                    let _ = buf.flush().await;
                                }
                                break;
                            }
                            _ = interval.tick() => {
                                if let SinkBackend::InMemory(buf) = backend.as_ref() {
                                    buf.drain_queued().await;
                                }
                            }
                        }
                    }
                });
            }
            SinkBackend::Tiered(_) => {
                // scalo TieredSink starts its own drain task in TieredSink::new()
            }
        }
    }
}

// Re-export MemoryGuard from scalo as the memory tracker.
// Replaces the bespoke BufferManager — same API, cgroup-aware auto-detection.
pub use scalo::memory::{MemoryGuard, MemoryGuardConfig, MemoryPressure};

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Counts the flushes that reach it.
    #[derive(Default)]
    struct Flushes(AtomicUsize);

    #[async_trait]
    impl ReceiverSink for Flushes {
        async fn send(&self, _topic: &str, _payload: Bytes) -> crate::error::Result<()> {
            Ok(())
        }

        async fn flush(&self) -> crate::error::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn is_healthy(&self) -> bool {
            true
        }
    }

    /// The spool keeps its records across a restart; the producer's queue
    /// drains only through the primary's flush.
    #[tokio::test]
    async fn a_spilling_backend_flushes_its_primary() {
        let dir = tempfile::tempdir().expect("spool dir");
        let primary = Arc::new(Flushes::default());
        let tiered = scalo::tiered_sink::TieredSink::new(
            adapter::ScaloSinkAdapter::new(Arc::clone(&primary), Rejects::default()),
            scalo::tiered_sink::TieredSinkConfig::new(dir.path().join("spool")),
        )
        .await
        .expect("tiered sink");
        let backend = SinkBackend::Tiered(tiered);

        backend.flush().await.expect("flush");
        assert_eq!(primary.0.load(Ordering::SeqCst), 1);
        if let SinkBackend::Tiered(tiered) = backend {
            tiered.shutdown().await;
        }
    }
}
