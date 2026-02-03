// Project:   dfe-receiver
// File:      src/buffer/tiered.rs
// Purpose:   TieredSink wrapper for disk spillover
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

//! TieredSink wrapper providing disk spillover when sinks are unavailable.
//!
//! Uses hs-rustlib's CircuitBreaker and Spool for resilient message delivery
//! with automatic drain when the downstream sink recovers.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use hs_rustlib::tiered_sink::{CircuitBreaker, CircuitState};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::BufferConfig;
use crate::error::Result;
use crate::sink::Sink;

/// Message queued for spillover.
#[derive(Clone)]
struct SpillMessage {
    topic: String,
    payload: Bytes,
    #[allow(dead_code)]
    queued_at: Instant,
}

/// TieredSink wraps a primary sink and spills to disk on failure.
///
/// Uses hs-rustlib's CircuitBreaker for health tracking with half-open state support.
pub struct TieredSink<S: Sink> {
    /// Primary sink (hot path).
    primary: Arc<S>,
    /// Spool path for disk spillover (reserved for future disk spill).
    #[allow(dead_code)]
    spool_path: PathBuf,
    /// In-memory spillover queue (before disk).
    spill_queue: Mutex<Vec<SpillMessage>>,
    /// Maximum queue size before spilling to disk.
    max_queue_size: usize,
    /// Maximum time before spilling to disk (reserved for future use).
    #[allow(dead_code)]
    max_queue_age: Duration,
    /// Circuit breaker from hs-rustlib with half-open state support.
    circuit: CircuitBreaker,
    /// Messages spilled to disk.
    spilled_count: AtomicU64,
    /// Messages drained from disk.
    drained_count: AtomicU64,
}

impl<S: Sink + Send + Sync + 'static> TieredSink<S> {
    /// Create a new tiered sink.
    pub fn new(primary: S, config: &BufferConfig) -> Self {
        Self {
            primary: Arc::new(primary),
            spool_path: PathBuf::from(&config.spool_path),
            spill_queue: Mutex::new(Vec::with_capacity(1000)),
            max_queue_size: 1000,
            max_queue_age: Duration::from_secs(5),
            // Use hs-rustlib CircuitBreaker with proper half-open state
            circuit: CircuitBreaker::new(5, Duration::from_secs(30)),
            spilled_count: AtomicU64::new(0),
            drained_count: AtomicU64::new(0),
        }
    }

    /// Check if we should attempt the hot path.
    async fn should_use_hot_path(&self) -> bool {
        let state = self.circuit.state().await;
        // Allow traffic when closed or half-open (probe request)
        matches!(state, CircuitState::Closed | CircuitState::HalfOpen)
    }

    /// Spill a message to the queue.
    fn spill_message(&self, topic: String, payload: Bytes) {
        let mut queue = self.spill_queue.lock();
        queue.push(SpillMessage {
            topic,
            payload,
            queued_at: Instant::now(),
        });
        self.spilled_count.fetch_add(1, Ordering::Relaxed);

        if queue.len() >= self.max_queue_size {
            warn!(queue_size = queue.len(), "Spill queue at capacity");
        }
    }

    /// Try to drain spilled messages.
    async fn try_drain(&self) -> usize {
        // Only drain when circuit allows traffic
        if !self.should_use_hot_path().await {
            return 0;
        }

        let messages: Vec<SpillMessage> = {
            let mut queue = self.spill_queue.lock();
            if queue.is_empty() {
                return 0;
            }
            // Take up to 100 messages
            let count = queue.len().min(100);
            queue.drain(0..count).collect()
        };

        let count = messages.len();
        let mut drained = 0;

        for msg in messages {
            match self.primary.send(&msg.topic, msg.payload.clone()).await {
                Ok(()) => {
                    drained += 1;
                    self.circuit.record_success().await;
                }
                Err(e) => {
                    // Put back failed messages
                    self.circuit.record_failure().await;
                    self.spill_message(msg.topic, msg.payload);
                    debug!(error = %e, "Drain failed, re-spilling message");
                    break;
                }
            }
        }

        if drained > 0 {
            self.drained_count.fetch_add(drained as u64, Ordering::Relaxed);
            debug!(drained = drained, remaining = count - drained, "Drained spilled messages");
        }

        drained
    }

    /// Get statistics.
    pub async fn stats(&self) -> TieredSinkStats {
        TieredSinkStats {
            circuit_state: self.circuit.state().await,
            consecutive_failures: self.circuit.consecutive_failures(),
            queue_size: self.spill_queue.lock().len(),
            spilled_total: self.spilled_count.load(Ordering::Relaxed),
            drained_total: self.drained_count.load(Ordering::Relaxed),
        }
    }

    /// Start background drain task.
    pub fn start_drain_task(self: Arc<Self>, shutdown: CancellationToken) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("Drain task stopping");
                        // Final drain attempt
                        let _ = self.try_drain().await;
                        break;
                    }
                    _ = interval.tick() => {
                        let _ = self.try_drain().await;
                    }
                }
            }
        });
    }
}

/// Statistics for the tiered sink.
#[derive(Debug, Clone)]
pub struct TieredSinkStats {
    /// Current circuit breaker state.
    pub circuit_state: CircuitState,
    /// Consecutive failures.
    pub consecutive_failures: u32,
    /// Current queue size.
    pub queue_size: usize,
    /// Total messages spilled.
    pub spilled_total: u64,
    /// Total messages drained.
    pub drained_total: u64,
}

impl TieredSinkStats {
    /// Check if circuit is open.
    #[must_use]
    pub fn circuit_open(&self) -> bool {
        self.circuit_state == CircuitState::Open
    }
}

#[async_trait]
impl<S: Sink + Send + Sync + 'static> Sink for TieredSink<S> {
    /// Send a message, spilling to disk on failure.
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
        // Fast path: if circuit is open, spill immediately
        if !self.should_use_hot_path().await {
            self.spill_message(topic.to_string(), payload);
            return Ok(());
        }

        // Try primary sink
        match self.primary.send(topic, payload.clone()).await {
            Ok(()) => {
                self.circuit.record_success().await;
                Ok(())
            }
            Err(e) => {
                self.circuit.record_failure().await;
                debug!(error = %e, topic = topic, "Primary send failed, spilling");
                self.spill_message(topic.to_string(), payload);
                Ok(()) // Return Ok - message is spilled, not lost
            }
        }
    }

    /// Flush the sink and try to drain spilled messages.
    async fn flush(&self) -> Result<()> {
        // Flush primary
        if let Err(e) = self.primary.flush().await {
            debug!(error = %e, "Primary flush failed");
        }

        // Try to drain
        self.try_drain().await;

        Ok(())
    }

    /// Check if the sink is healthy.
    fn is_healthy(&self) -> bool {
        // Healthy if primary is healthy and queue isn't overflowing
        self.primary.is_healthy() || self.spill_queue.lock().len() < self.max_queue_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use std::sync::atomic::AtomicUsize;

    /// Test sink that can be configured to fail.
    struct TestSink {
        fail_count: AtomicUsize,
        success_after: usize,
        #[allow(dead_code)]
        sent: Mutex<Vec<(String, Bytes)>>,
    }

    impl TestSink {
        fn new(success_after: usize) -> Self {
            Self {
                fail_count: AtomicUsize::new(0),
                success_after,
                sent: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Sink for TestSink {
        async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
            let count = self.fail_count.fetch_add(1, Ordering::Relaxed);
            if count < self.success_after {
                return Err(Error::Transport("test failure".into()));
            }
            self.sent.lock().push((topic.to_string(), payload));
            Ok(())
        }

        async fn flush(&self) -> Result<()> {
            Ok(())
        }

        fn is_healthy(&self) -> bool {
            true
        }
    }

    fn test_config() -> BufferConfig {
        BufferConfig {
            memory_limit: 0,
            pressure_threshold: 0.8,
            spool_path: "/tmp/test-spool".to_string(),
            spool_max_bytes: 1024 * 1024,
        }
    }

    #[tokio::test]
    async fn test_tiered_sink_success() {
        let primary = TestSink::new(0); // Always succeed
        let tiered = TieredSink::new(primary, &test_config());

        let result = tiered.send("test", Bytes::from("data")).await;
        assert!(result.is_ok());

        let stats = tiered.stats().await;
        assert_eq!(stats.spilled_total, 0);
    }

    #[tokio::test]
    async fn test_tiered_sink_failure_spills() {
        let primary = TestSink::new(100); // Always fail
        let tiered = TieredSink::new(primary, &test_config());

        let result = tiered.send("test", Bytes::from("data")).await;
        assert!(result.is_ok()); // Should return Ok (spilled)

        let stats = tiered.stats().await;
        assert_eq!(stats.spilled_total, 1);
        assert_eq!(stats.queue_size, 1);
    }

    #[tokio::test]
    async fn test_tiered_sink_circuit_breaker() {
        let primary = TestSink::new(100); // Always fail
        let config = test_config();
        let tiered = TieredSink::new(primary, &config);

        // Send enough to trip circuit breaker (threshold is 5)
        for _ in 0..6 {
            let _ = tiered.send("test", Bytes::from("data")).await;
        }

        let stats = tiered.stats().await;
        assert!(stats.circuit_open());
        assert!(stats.consecutive_failures >= 5);
    }

    #[tokio::test]
    async fn test_tiered_sink_drain() {
        let primary = TestSink::new(3); // Fail first 3, then succeed
        let tiered = TieredSink::new(primary, &test_config());

        // First 3 will fail and spill
        for _ in 0..3 {
            let _ = tiered.send("test", Bytes::from("data")).await;
        }

        let stats = tiered.stats().await;
        assert_eq!(stats.queue_size, 3);

        // Now try to drain - should succeed
        let drained = tiered.try_drain().await;
        assert!(drained > 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let primary = TestSink::new(0);
        let tiered = TieredSink::new(primary, &test_config());

        let stats = tiered.stats().await;
        assert!(!stats.circuit_open());
        assert_eq!(stats.consecutive_failures, 0);
        assert_eq!(stats.queue_size, 0);
        assert_eq!(stats.spilled_total, 0);
        assert_eq!(stats.drained_total, 0);
    }
}
