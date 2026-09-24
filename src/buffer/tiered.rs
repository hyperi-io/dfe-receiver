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

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use scalo::tiered_sink::{CircuitBreaker, CircuitState};
use tracing::{debug, warn};

use crate::buffer::Rejects;
use crate::config::{BufferConfig, QueueBound};
use crate::error::{Error, Result};
use crate::sink::Sink;

/// Messages taken per drain cycle.
const DRAIN_BATCH: usize = 100;

/// How long `flush` keeps draining before it gives up and reports what is left.
/// Sits under a Kubernetes `terminationGracePeriodSeconds` of 30.
const FLUSH_DEADLINE: Duration = Duration::from_secs(10);

/// Message queued during sink unavailability.
#[derive(Clone)]
struct SpillMessage {
    topic: String,
    payload: Bytes,
}

impl SpillMessage {
    /// Bytes this message holds, for the queue's byte bound.
    fn weight(&self) -> usize {
        self.topic.len() + self.payload.len()
    }
}

/// The spill queue and the bytes it holds, under one lock.
#[derive(Default)]
struct SpillQueue {
    messages: VecDeque<SpillMessage>,
    bytes: usize,
}

impl SpillQueue {
    /// Whether one more message fits under `bound`.
    fn accepts(&self, bound: QueueBound, weight: usize) -> bool {
        match bound {
            QueueBound::Records(max) => self.messages.len() < max,
            QueueBound::Bytes(max) => self.bytes + weight <= max,
        }
    }

    /// Whether the queue is at its bound and can take nothing more.
    fn is_full(&self, bound: QueueBound) -> bool {
        !self.accepts(bound, 1)
    }

    fn push_back(&mut self, msg: SpillMessage) {
        self.bytes += msg.weight();
        self.messages.push_back(msg);
    }

    /// Return undelivered messages to the front, so drain order is preserved.
    fn push_front_all(&mut self, msgs: impl DoubleEndedIterator<Item = SpillMessage>) {
        for msg in msgs.rev() {
            self.bytes += msg.weight();
            self.messages.push_front(msg);
        }
    }

    fn take(&mut self, count: usize) -> Vec<SpillMessage> {
        let taken: Vec<SpillMessage> = self.messages.drain(0..count).collect();
        self.bytes -= taken.iter().map(SpillMessage::weight).sum::<usize>();
        taken
    }
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
    spill_queue: Mutex<SpillQueue>,
    /// What the queue refuses at, from `buffer.memory_limit` and
    /// `buffer.pressure_threshold`.
    bound: QueueBound,
    /// Circuit breaker from scalo with half-open state support.
    circuit: CircuitBreaker,
    /// Where records the primary refuses for good go instead of the queue.
    rejects: Rejects,
    /// Messages queued during outage.
    queued_count: AtomicU64,
    /// Messages drained after recovery.
    drained_count: AtomicU64,
}

impl<S: Sink + Send + Sync + 'static> InMemoryBuffer<S> {
    /// Create a new tiered sink that drops what the primary refuses for good.
    pub fn new(primary: S, config: &BufferConfig) -> Self {
        let bound = config.queue_bound();
        debug!(?bound, "In-memory buffer bound");
        Self {
            primary: Arc::new(primary),
            spill_queue: Mutex::new(SpillQueue::default()),
            bound,
            // Use scalo CircuitBreaker with proper half-open state
            circuit: CircuitBreaker::new(5, Duration::from_secs(30)),
            rejects: Rejects::default(),
            queued_count: AtomicU64::new(0),
            drained_count: AtomicU64::new(0),
        }
    }

    /// Send records the primary refuses for good to `rejects`.
    #[must_use]
    pub fn with_rejects(mut self, rejects: Rejects) -> Self {
        self.rejects = rejects;
        self
    }

    /// Check if we should attempt the hot path.
    async fn should_use_hot_path(&self) -> bool {
        let state = self.circuit.state().await;
        // Allow traffic when closed or half-open (probe request)
        matches!(state, CircuitState::Closed | CircuitState::HalfOpen)
    }

    /// Queue a message for later delivery. Returns false if queue is full.
    fn queue_message(&self, topic: String, payload: Bytes) -> bool {
        let msg = SpillMessage { topic, payload };
        let weight = msg.weight();
        let mut queue = self.spill_queue.lock();
        if !queue.accepts(self.bound, weight) {
            warn!(
                queue_size = queue.messages.len(),
                queue_bytes = queue.bytes,
                bound = ?self.bound,
                "In-memory queue full, rejecting message"
            );
            return false;
        }
        queue.push_back(msg);
        self.queued_count.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Try to drain queued messages.
    ///
    /// Returns how many left the queue, delivered or refused for good: a batch
    /// of refusals is progress, and `drain_until_idle` stops on zero.
    async fn try_drain(&self) -> usize {
        // Only drain when circuit allows traffic
        if !self.should_use_hot_path().await {
            return 0;
        }

        let messages: Vec<SpillMessage> = {
            let mut queue = self.spill_queue.lock();
            let count = queue.messages.len().min(DRAIN_BATCH);
            if count == 0 {
                return 0;
            }
            queue.take(count)
        };

        let count = messages.len();
        let mut drained = 0;
        let mut rejected = 0;
        let mut pending = messages.into_iter();
        let mut failed = None;

        for msg in pending.by_ref() {
            match self.primary.send(&msg.topic, msg.payload.clone()).await {
                Ok(()) => {
                    drained += 1;
                    self.circuit.record_success().await;
                }
                // Requeued, a record refused for good would hold up every record behind it.
                Err(Error::Rejected(reason)) => {
                    rejected += 1;
                    self.rejects
                        .dispose(&msg.topic, &msg.payload, &reason)
                        .await;
                }
                Err(e) => {
                    self.circuit.record_failure().await;
                    debug!(error = %e, "Drain failed, re-queuing message");
                    failed = Some(msg);
                    break;
                }
            }
        }

        // The failed message and everything behind it go back at the front, so a
        // mid-batch failure loses nothing.
        if let Some(msg) = failed {
            let mut restore = vec![msg];
            restore.extend(pending);
            self.spill_queue.lock().push_front_all(restore.into_iter());
        }

        if drained > 0 || rejected > 0 {
            self.drained_count
                .fetch_add(drained as u64, Ordering::Relaxed);
            debug!(
                drained = drained,
                rejected = rejected,
                remaining = count - drained - rejected,
                "Drained queued messages"
            );
        }

        drained + rejected
    }

    /// Get statistics.
    pub async fn stats(&self) -> InMemoryBufferStats {
        let queue_size = self.spill_queue.lock().messages.len();
        InMemoryBufferStats {
            circuit_state: self.circuit.state().await,
            consecutive_failures: self.circuit.consecutive_failures(),
            queue_size,
            queued_total: self.queued_count.load(Ordering::Relaxed),
            drained_total: self.drained_count.load(Ordering::Relaxed),
        }
    }

    /// Move queued records into the primary until the queue empties, a drain
    /// stops making progress, or the deadline passes.
    async fn drain_until_idle(&self) {
        let deadline = tokio::time::Instant::now() + FLUSH_DEADLINE;
        loop {
            let queued = self.spill_queue.lock().messages.len();
            if queued == 0 {
                break;
            }
            if self.try_drain().await == 0 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                let remaining = self.spill_queue.lock().messages.len();
                warn!(remaining, "Drain deadline reached, records still queued");
                break;
            }
        }
    }

    /// Drain queued records into the primary, leaving the primary's own
    /// in-flight records alone.
    ///
    /// This is what the periodic drain runs. A queued record waiting on a
    /// broker outage is not a lost record, so this path must not call
    /// [`Sink::flush`], whose timeout reports records lost on exit.
    pub async fn drain_queued(&self) {
        self.drain_until_idle().await;
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
                return Err(Error::Transport(
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
            // Queued, a record refused for good would hold up every record behind it.
            Err(Error::Rejected(reason)) => {
                self.rejects.dispose(topic, &payload, &reason).await;
                Ok(())
            }
            Err(e) => {
                self.circuit.record_failure().await;
                debug!(error = %e, topic = topic, "Primary send failed, queuing");
                if !self.queue_message(topic.to_string(), payload) {
                    return Err(Error::Transport(
                        "in-memory queue full, backpressure".into(),
                    ));
                }
                Ok(())
            }
        }
    }

    /// Flush the sink and drain queued messages.
    async fn flush(&self) -> Result<()> {
        // Flush primary
        if let Err(e) = self.primary.flush().await {
            debug!(error = %e, "Primary flush failed");
        }

        // try_drain moves at most DRAIN_BATCH per call, so one call leaves the
        // rest of the queue behind.
        self.drain_until_idle().await;

        Ok(())
    }

    /// Check if the sink is healthy.
    fn is_healthy(&self) -> bool {
        // Healthy while the primary is up, or the queue can still take a record.
        self.primary.is_healthy() || !self.spill_queue.lock().is_full(self.bound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

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

    /// Fill a buffer whose primary always fails, and return how many records it
    /// took before it refused.
    async fn fill_until_refused(config: &BufferConfig, limit: usize) -> usize {
        let buffer = InMemoryBuffer::new(TestSink::new(usize::MAX), config);
        for held in 0..limit {
            if buffer.send("t", Bytes::from("123456789")).await.is_err() {
                return held;
            }
        }
        limit
    }

    /// An auto-detected memory limit gives no byte figure, so the queue counts
    /// records -- 1000 of them.
    #[tokio::test]
    async fn default_config_holds_a_thousand_records() {
        let held = fill_until_refused(&BufferConfig::default(), 1_200).await;
        assert_eq!(held, 1_000, "default queue took {held} records");
    }

    /// A configured memory limit bounds the queue in bytes, and the share of it
    /// the queue may take comes from `pressure_threshold`.
    #[tokio::test]
    async fn a_configured_memory_limit_bounds_the_queue_in_bytes() {
        let config = BufferConfig {
            // 10 bytes per record: the "t" topic plus a 9-byte payload.
            memory_limit: 100,
            pressure_threshold: 0.5,
            ..Default::default()
        };
        let held = fill_until_refused(&config, 100).await;
        assert_eq!(held, 5, "a 50-byte budget took {held} records");
    }

    #[tokio::test]
    async fn a_higher_pressure_threshold_holds_more() {
        let config = BufferConfig {
            memory_limit: 100,
            pressure_threshold: 1.0,
            ..Default::default()
        };
        let held = fill_until_refused(&config, 100).await;
        assert_eq!(held, 10, "a 100-byte budget took {held} records");
    }

    /// Sink that accepts a set number of messages, then fails for good.
    struct CapSink {
        remaining: AtomicUsize,
    }

    #[async_trait]
    impl Sink for CapSink {
        async fn send(&self, _topic: &str, _payload: Bytes) -> Result<()> {
            if self
                .remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_err()
            {
                return Err(Error::Transport("cap reached".into()));
            }
            Ok(())
        }

        async fn flush(&self) -> Result<()> {
            Ok(())
        }

        fn is_healthy(&self) -> bool {
            true
        }
    }

    /// A mid-batch drain failure returns the rest of the batch to the queue
    /// rather than dropping it.
    #[tokio::test]
    async fn a_failed_drain_keeps_the_records_behind_it() {
        let buffer = InMemoryBuffer::new(
            CapSink {
                remaining: AtomicUsize::new(0),
            },
            &test_config(),
        );

        // Four sends fail and queue -- one short of tripping the circuit.
        for _ in 0..4 {
            let _ = buffer.send("t", Bytes::from("data")).await;
        }
        assert_eq!(buffer.stats().await.queue_size, 4);

        // The primary takes one more, then fails for the rest of the batch.
        buffer.primary.remaining.store(1, Ordering::Relaxed);
        assert_eq!(buffer.try_drain().await, 1);
        assert_eq!(
            buffer.stats().await.queue_size,
            3,
            "the records behind the failure were dropped"
        );
    }

    /// Sink that refuses until `accept` is set, and counts its flushes.
    struct FlakySink {
        accept: AtomicBool,
        flushes: AtomicUsize,
    }

    #[async_trait]
    impl Sink for FlakySink {
        async fn send(&self, _topic: &str, _payload: Bytes) -> Result<()> {
            if self.accept.load(Ordering::Relaxed) {
                Ok(())
            } else {
                Err(Error::Transport("refusing".into()))
            }
        }

        async fn flush(&self) -> Result<()> {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn is_healthy(&self) -> bool {
            true
        }
    }

    /// Queue more than one drain batch, with the circuit opened by the failures
    /// that queued them.
    async fn buffer_with_a_full_queue(queued: usize) -> InMemoryBuffer<FlakySink> {
        let buffer = InMemoryBuffer::new(
            FlakySink {
                accept: AtomicBool::new(false),
                flushes: AtomicUsize::new(0),
            },
            &test_config(),
        );

        for _ in 0..queued {
            let _ = buffer.send("t", Bytes::from("data")).await;
        }
        assert_eq!(buffer.stats().await.queue_size, queued);

        // Let the primary take them, and close the circuit those failures opened.
        buffer.primary.accept.store(true, Ordering::Relaxed);
        buffer.circuit.reset().await;
        buffer
    }

    /// `flush` empties the queue rather than stopping after `DRAIN_BATCH`.
    #[tokio::test]
    async fn flush_drains_past_a_single_batch() {
        let queued = DRAIN_BATCH * 2 + 7;
        let buffer = buffer_with_a_full_queue(queued).await;

        buffer.flush().await.unwrap();

        assert_eq!(
            buffer.stats().await.queue_size,
            0,
            "flush stopped short of the whole queue"
        );
        assert_eq!(buffer.stats().await.drained_total, queued as u64);
    }

    /// The periodic drain moves the queue without flushing the primary, whose
    /// timeout reports records lost on exit.
    #[tokio::test]
    async fn the_periodic_drain_does_not_flush_the_primary() {
        let buffer = buffer_with_a_full_queue(DRAIN_BATCH + 1).await;

        buffer.drain_queued().await;

        assert_eq!(buffer.stats().await.queue_size, 0);
        assert_eq!(
            buffer.primary.flushes.load(Ordering::Relaxed),
            0,
            "the periodic drain flushed the primary"
        );

        buffer.flush().await.unwrap();
        assert_eq!(buffer.primary.flushes.load(Ordering::Relaxed), 1);
    }

    /// Sink that is down until `up` is set, then refuses a `bad` payload for
    /// good, fails `flaky` once as a transient error, and takes the rest.
    struct RefusingSink {
        up: AtomicBool,
        flaky: Mutex<Option<Bytes>>,
        taken: Mutex<Vec<Bytes>>,
    }

    impl RefusingSink {
        fn down() -> Self {
            Self {
                up: AtomicBool::new(false),
                flaky: Mutex::new(None),
                taken: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Sink for RefusingSink {
        async fn send(&self, _topic: &str, payload: Bytes) -> Result<()> {
            if !self.up.load(Ordering::Relaxed) {
                return Err(Error::Transport("destination down".into()));
            }
            if payload.as_ref() == b"bad" {
                return Err(Error::Rejected("destination refused the record".into()));
            }
            let mut flaky = self.flaky.lock();
            if flaky.as_ref() == Some(&payload) {
                *flaky = None;
                return Err(Error::Transport("transient failure".into()));
            }
            drop(flaky);
            self.taken.lock().push(payload);
            Ok(())
        }

        async fn flush(&self) -> Result<()> {
            Ok(())
        }

        fn is_healthy(&self) -> bool {
            self.up.load(Ordering::Relaxed)
        }
    }

    /// Queue `payloads` against a destination that is down, then bring it up
    /// and close the circuit the outage opened.
    async fn queued_behind_an_outage(payloads: &[&'static str]) -> InMemoryBuffer<RefusingSink> {
        let buffer = InMemoryBuffer::new(RefusingSink::down(), &test_config());
        for payload in payloads {
            buffer
                .send("t", Bytes::from_static(payload.as_bytes()))
                .await
                .unwrap();
        }
        assert_eq!(buffer.stats().await.queue_size, payloads.len());
        buffer.primary.up.store(true, Ordering::Relaxed);
        buffer.circuit.reset().await;
        buffer
    }

    /// A whole drain batch of refusals is still progress, so the flush carries
    /// on to the records behind it.
    #[tokio::test]
    async fn a_batch_of_rejected_records_does_not_stop_the_flush() {
        let mut payloads = vec!["bad"; DRAIN_BATCH];
        payloads.push("good");
        let buffer = queued_behind_an_outage(&payloads).await;

        buffer.flush().await.unwrap();

        assert_eq!(
            buffer.stats().await.queue_size,
            0,
            "the flush stopped at a batch of refused records"
        );
        assert_eq!(*buffer.primary.taken.lock(), [Bytes::from_static(b"good")]);
    }

    /// A record the destination refuses for good leaves the queue, so the
    /// records behind it still drain, in order.
    #[tokio::test]
    async fn a_rejected_record_does_not_hold_up_the_records_behind_it() {
        let buffer = queued_behind_an_outage(&["bad", "good-1", "good-2"]).await;

        assert_eq!(buffer.try_drain().await, 3);
        assert_eq!(
            buffer.stats().await.queue_size,
            0,
            "the rejected record is still at the head of the queue"
        );
        assert_eq!(
            *buffer.primary.taken.lock(),
            [Bytes::from_static(b"good-1"), Bytes::from_static(b"good-2")]
        );
        assert_eq!(buffer.stats().await.drained_total, 2);
    }

    /// A transient failure mid-drain keeps the failed record and everything
    /// behind it, and they go out in their original order on the next drain.
    #[tokio::test]
    async fn a_transient_failure_keeps_drain_order() {
        let buffer = queued_behind_an_outage(&["one", "two", "three"]).await;
        *buffer.primary.flaky.lock() = Some(Bytes::from_static(b"two"));

        assert_eq!(buffer.try_drain().await, 1);
        assert_eq!(buffer.stats().await.queue_size, 2);
        assert_eq!(buffer.try_drain().await, 2);
        assert_eq!(
            *buffer.primary.taken.lock(),
            [
                Bytes::from_static(b"one"),
                Bytes::from_static(b"two"),
                Bytes::from_static(b"three"),
            ]
        );
    }

    /// A record refused on the direct path never enters the queue.
    #[tokio::test]
    async fn a_record_rejected_on_the_direct_path_is_not_queued() {
        let buffer = InMemoryBuffer::new(RefusingSink::down(), &test_config());
        buffer.primary.up.store(true, Ordering::Relaxed);

        buffer.send("t", Bytes::from_static(b"bad")).await.unwrap();

        assert_eq!(buffer.stats().await.queue_size, 0);
        assert!(buffer.primary.taken.lock().is_empty());
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
