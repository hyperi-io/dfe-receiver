// Project:   dfe-receiver
// File:      src/buffer/mod.rs
// Purpose:   Memory buffer and disk spillover
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Buffer management with memory pressure detection.
//!
//! Provides in-memory batching with disk spillover when under pressure.

pub mod adapter;
pub mod tiered;

pub use tiered::{InMemoryBuffer, InMemoryBufferStats};
// Re-export CircuitState from hyperi-rustlib for convenience
pub use hyperi_rustlib::tiered_sink::CircuitState;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::BufferConfig;
use crate::sink::Sink as ReceiverSink;

/// Backend for message delivery — in-memory only or with disk spillover.
pub enum SinkBackend<S: ReceiverSink + 'static> {
    /// In-memory buffer with circuit breaker (default, no disk).
    InMemory(InMemoryBuffer<S>),
    /// Rustlib TieredSink with disk spool (opt-in via spillover config).
    Tiered(hyperi_rustlib::tiered_sink::TieredSink<adapter::RustlibSinkAdapter<S>>),
}

#[async_trait]
impl<S: ReceiverSink + 'static> ReceiverSink for SinkBackend<S> {
    async fn send(&self, topic: &str, payload: Bytes) -> crate::error::Result<()> {
        match self {
            SinkBackend::InMemory(buf) => buf.send(topic, payload).await,
            SinkBackend::Tiered(tiered) => {
                let encoded = adapter::encode_message(topic, &payload);
                tiered
                    .send(&encoded)
                    .await
                    .map_err(|e| crate::error::Error::Transport(e.to_string()))
            }
        }
    }

    async fn flush(&self) -> crate::error::Result<()> {
        match self {
            SinkBackend::InMemory(buf) => buf.flush().await,
            SinkBackend::Tiered(_) => Ok(()), // rustlib handles drain internally
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
                // Map rustlib stats into InMemoryBufferStats for compatibility
                let circuit_state = tiered.circuit_state().await;
                InMemoryBufferStats {
                    circuit_state,
                    consecutive_failures: 0, // rustlib doesn't expose this directly
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
    /// For Tiered: rustlib manages its own drain task internally — this is a no-op.
    pub fn start_drain_task(self: Arc<Self>, shutdown: CancellationToken) {
        match self.as_ref() {
            SinkBackend::InMemory(_) => {
                // InMemoryBuffer needs its own drain loop. We spawn a task
                // that periodically calls flush (which triggers try_drain).
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
                                    let _ = buf.flush().await;
                                }
                            }
                        }
                    }
                });
            }
            SinkBackend::Tiered(_) => {
                // Rustlib TieredSink starts its own drain task in TieredSink::new()
            }
        }
    }
}

/// Memory pressure levels.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MemoryPressure {
    /// Memory usage is low.
    Low,
    /// Memory usage is moderate.
    Medium,
    /// Memory usage is high - apply backpressure.
    High,
}

/// Buffer manager tracking memory usage.
pub struct BufferManager {
    total_bytes: AtomicU64,
    memory_limit: u64,
    pressure_threshold: f64,
    under_pressure: AtomicBool,
}

impl BufferManager {
    /// Create a new buffer manager.
    pub fn new(config: &BufferConfig) -> Self {
        // Auto-detect memory limit if not set
        let memory_limit = if config.memory_limit == 0 {
            // Default to 67% of available memory
            let mut sys = sysinfo::System::new();
            sys.refresh_memory();
            let available = sys.available_memory();
            (available * 67) / 100
        } else {
            config.memory_limit as u64
        };

        Self {
            total_bytes: AtomicU64::new(0),
            memory_limit,
            pressure_threshold: config.pressure_threshold,
            under_pressure: AtomicBool::new(false),
        }
    }

    /// Add bytes to the tracked total.
    #[inline]
    pub fn add_bytes(&self, bytes: u64) {
        let new_total = self.total_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.update_pressure(new_total);
    }

    /// Remove bytes from the tracked total.
    #[inline]
    pub fn remove_bytes(&self, bytes: u64) {
        let new_total = self.total_bytes.fetch_sub(bytes, Ordering::Relaxed) - bytes;
        self.update_pressure(new_total);
    }

    /// Get current memory pressure level.
    #[inline]
    pub fn pressure(&self) -> MemoryPressure {
        let total = self.total_bytes.load(Ordering::Relaxed);
        let ratio = total as f64 / self.memory_limit as f64;

        if ratio >= self.pressure_threshold {
            MemoryPressure::High
        } else if ratio >= 0.5 {
            MemoryPressure::Medium
        } else {
            MemoryPressure::Low
        }
    }

    /// Check if under memory pressure.
    #[inline]
    pub fn is_under_pressure(&self) -> bool {
        self.under_pressure.load(Ordering::Relaxed)
    }

    /// Get current total bytes.
    #[inline]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Get configured memory limit.
    #[inline]
    pub fn memory_limit(&self) -> u64 {
        self.memory_limit
    }

    /// Update pressure state.
    #[inline]
    fn update_pressure(&self, total: u64) {
        let ratio = total as f64 / self.memory_limit as f64;
        let under_pressure = ratio >= self.pressure_threshold;
        self.under_pressure.store(under_pressure, Ordering::Relaxed);
    }
}

impl Default for BufferManager {
    fn default() -> Self {
        Self::new(&BufferConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pressure() {
        let config = BufferConfig {
            memory_limit: 1000,
            pressure_threshold: 0.8,
            ..Default::default()
        };
        let manager = BufferManager::new(&config);

        // Low usage
        manager.add_bytes(100);
        assert_eq!(manager.pressure(), MemoryPressure::Low);
        assert!(!manager.is_under_pressure());

        // Medium usage
        manager.add_bytes(400);
        assert_eq!(manager.pressure(), MemoryPressure::Medium);
        assert!(!manager.is_under_pressure());

        // High usage
        manager.add_bytes(400);
        assert_eq!(manager.pressure(), MemoryPressure::High);
        assert!(manager.is_under_pressure());
    }
}
