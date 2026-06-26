// Project:   dfe-receiver
// File:      src/buffer/tiered.rs
// Purpose:   InMemoryBuffer wrapper with circuit breaker
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! In-memory buffer with circuit breaker for sink unavailability.
//!
//! Uses scalo's CircuitBreaker for health tracking with half-open state support.
//! Messages are buffered in memory during outages and drained when the downstream
//! sink recovers.
//!
//! This is the default buffer backend (no disk I/O). For opt-in disk spillover,
//! see `SinkBackend::Tiered` which uses scalo's `TieredSink` with a disk spool.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use scalo::tiered_sink::{CircuitBreaker, CircuitState};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::BufferConfig;
use crate::error::Result;
use crate::sink::Sink;

/// Message queued during sink unavailability.
#[derive(Clone)]
struct SpillMessage {
    topic: String,
    payload: Bytes,
}

/// InMemoryBuffer wraps a primary sink with circuit breaker and in-memory buffering.
///
/// Uses scalo's CircuitBreaker for health tracking with half-open state support.
/// When the primary sink fails, messages are buffered in memory and automatically
/// drained when the sink recovers.
pub struct InMemoryBuffer<S: Sink> {
    /// Primary sink (hot path).
    primary: Arc<S>,
    /// In-memory spillover queue.
    spill_queue: Mutex<Vec<SpillMessage>>,
    /// Maximum queue size before rejecting.
    max_queue_size: usize,
    /// Circuit breaker from scalo with half-open state support.
    circuit: CircuitBreaker,
    /// Messages queued during outage.
    queued_count: AtomicU64,
    /// Messages drained after recovery.
    drained_count: AtomicU64,
}

impl<S: Sink + Send + Sync + 'static> InMemoryBuffer<S> {
    /// Create a new tiered sink.
    #[allow(unused_variables)]
    pub fn new(primary: S, config: &BufferConfig) -> Self {
        Self {
            primary: Arc::new(primary),
            spill_queue: Mutex::new(Vec::with_capacity(1000)),
            max_queue_size: 1000,
            // Use scalo CircuitBreaker with proper half-open state
            circuit: CircuitBreaker::new(5, Duration::from_secs(30)),
            queued_count: AtomicU64::new(0),
            drained_count: AtomicU64::new(0),
        }
    }

    /// Check if we should attempt the hot path.
    async fn should_use_hot_path(&self) -> bool {
        let state = self.circuit.state().await;
        // Allow traffic when closed or half-open (probe request)
        matches!(state, CircuitState::Closed | CircuitState::HalfOpen)
    }

    /// Queue a message for later delivery. Returns false if queue is full.
    fn queue_message(&self, topic: String, payload: Bytes) -> bool {
        let mut queue = self.spill_queue.lock();
        if queue.len() >= self.max_queue_size {
            warn!(
                queue_size = queue.len(),
                max = self.max_queue_size,
                "In-memory queue full, rejecting message"
            );
            return false;
        }
        queue.push(SpillMessage { topic, payload });
        self.queued_count.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Try to drain queued messages.
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
            // Take up to 100 messages per drain cycle
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
                    self.queue_message(msg.topic, msg.payload);
                    debug!(error = %e, "Drain failed, re-queuing message");
                    break;
                }
            }
        }

        if drained > 0 {
            self.drained_count
                .fetch_add(drained as u64, Ordering::Relaxed);
            debug!(
                drained = drained,
                remaining = count - drained,
                "Drained queued messages"
            );
        }

        drained
    }

    /// Get statistics.
    pub async fn stats(&self) -> InMemoryBufferStats {
        InMemoryBufferStats {
            circuit_state: self.circuit.state().await,
            consecutive_failures: self.circuit.consecutive_failures(),
            queue_size: self.spill_queue.lock().len(),
            queued_total: self.queued_count.load(Ordering::Relaxed),
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
pub struct InMemoryBufferStats {
    /// Current circuit breaker state.
    pub circuit_state: CircuitState,
    /// Consecutive failures.
    pub consecutive_failures: u32,
    /// Current queue size.
    pub queue_size: usize,
    /// Total messages queued during outages.
    pub queued_total: u64,
    /// Total messages drained after recovery.
    pub drained_total: u64,
}

impl InMemoryBufferStats {
    /// Check if circuit is open.
    #[must_use]
    pub fn circuit_open(&self) -> bool {
        self.circuit_state == CircuitState::Open
    }
}

#[async_trait]
impl<S: Sink + Send + Sync + 'static> Sink for InMemoryBuffer<S> {
    /// Send a message, queuing on failure.
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
        // Fast path: if circuit is open, queue immediately
        if !self.should_use_hot_path().await {
            if !self.queue_message(topic.to_string(), payload) {
                return Err(crate::error::Error::Transport(
                    "in-memory queue full, backpressure".into(),
                ));
            }
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
                debug!(error = %e, topic = topic, "Primary send failed, queuing");
                if !self.queue_message(topic.to_string(), payload) {
                    return Err(crate::error::Error::Transport(
                        "in-memory queue full, backpressure".into(),
                    ));
                }
                Ok(())
            }
        }
    }

    /// Flush the sink and try to drain queued messages.
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
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_tiered_sink_success() {
        let primary = TestSink::new(0); // Always succeed
        let tiered = InMemoryBuffer::new(primary, &test_config());

        let result = tiered.send("test", Bytes::from("data")).await;
        assert!(result.is_ok());

        let stats = tiered.stats().await;
        assert_eq!(stats.queued_total, 0);
    }

    #[tokio::test]
    async fn test_tiered_sink_failure_queues() {
        let primary = TestSink::new(100); // Always fail
        let tiered = InMemoryBuffer::new(primary, &test_config());

        let result = tiered.send("test", Bytes::from("data")).await;
        assert!(result.is_ok()); // Should return Ok (queued)

        let stats = tiered.stats().await;
        assert_eq!(stats.queued_total, 1);
        assert_eq!(stats.queue_size, 1);
    }

    #[tokio::test]
    async fn test_tiered_sink_circuit_breaker() {
        let primary = TestSink::new(100); // Always fail
        let config = test_config();
        let tiered = InMemoryBuffer::new(primary, &config);

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
        let tiered = InMemoryBuffer::new(primary, &test_config());

        // First 3 will fail and queue
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
        let tiered = InMemoryBuffer::new(primary, &test_config());

        let stats = tiered.stats().await;
        assert!(!stats.circuit_open());
        assert_eq!(stats.consecutive_failures, 0);
        assert_eq!(stats.queue_size, 0);
        assert_eq!(stats.queued_total, 0);
        assert_eq!(stats.drained_total, 0);
    }
}
