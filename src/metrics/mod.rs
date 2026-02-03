// Project:   dfe-receiver
// File:      src/metrics/mod.rs
// Purpose:   Prometheus metrics and KEDA scaling
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

//! Prometheus metrics for dfe-receiver.
//!
//! Exposes counters, gauges, and histograms for monitoring and KEDA scaling.
//! Includes a compound scaling metric for autoscaling decisions.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// Metrics collector for dfe-receiver.
#[derive(Debug)]
pub struct Metrics {
    // Counters
    requests_total: AtomicU64,
    requests_success: AtomicU64,
    requests_error: AtomicU64,
    bytes_received: AtomicU64,
    messages_batched: AtomicU64,
    messages_sent_kafka: AtomicU64,
    messages_sent_loader: AtomicU64,
    messages_dlq: AtomicU64,
    messages_spilled: AtomicU64,
    messages_drained: AtomicU64,

    // Gauges
    batch_queue_size: AtomicU64,
    batch_queue_bytes: AtomicU64,
    spool_bytes: AtomicU64,
    spool_messages: AtomicU64,
    active_connections: AtomicU64,
    memory_used_bytes: AtomicU64,
    memory_limit_bytes: AtomicU64,

    // Rate tracking for KEDA
    rate_window: RwLock<RateWindow>,
}

/// Sliding window for rate calculation.
#[derive(Debug)]
struct RateWindow {
    samples: Vec<(Instant, u64)>,
    window_size: Duration,
}

impl RateWindow {
    fn new(window_size: Duration) -> Self {
        Self {
            samples: Vec::with_capacity(60),
            window_size,
        }
    }

    fn add_sample(&mut self, value: u64) {
        let now = Instant::now();
        self.samples.push((now, value));

        // Remove old samples
        if let Some(cutoff) = now.checked_sub(self.window_size) {
            self.samples.retain(|(t, _)| *t > cutoff);
        }
    }

    fn rate_per_second(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }

        let Some(first) = self.samples.first() else {
            return 0.0;
        };
        let Some(last) = self.samples.last() else {
            return 0.0;
        };

        let duration = last.0.duration_since(first.0);
        if duration.is_zero() {
            return 0.0;
        }

        let delta = last.1.saturating_sub(first.1);
        delta as f64 / duration.as_secs_f64()
    }
}

impl Metrics {
    /// Create a new metrics collector.
    pub fn new() -> Self {
        Self {
            requests_total: AtomicU64::new(0),
            requests_success: AtomicU64::new(0),
            requests_error: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            messages_batched: AtomicU64::new(0),
            messages_sent_kafka: AtomicU64::new(0),
            messages_sent_loader: AtomicU64::new(0),
            messages_dlq: AtomicU64::new(0),
            messages_spilled: AtomicU64::new(0),
            messages_drained: AtomicU64::new(0),
            batch_queue_size: AtomicU64::new(0),
            batch_queue_bytes: AtomicU64::new(0),
            spool_bytes: AtomicU64::new(0),
            spool_messages: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            memory_used_bytes: AtomicU64::new(0),
            memory_limit_bytes: AtomicU64::new(0),
            rate_window: RwLock::new(RateWindow::new(Duration::from_secs(60))),
        }
    }

    /// Increment total requests counter.
    #[inline]
    pub fn inc_requests_total(&self) {
        let count = self.requests_total.fetch_add(1, Ordering::Relaxed) + 1;
        self.rate_window.write().add_sample(count);
    }

    /// Increment successful requests counter.
    #[inline]
    pub fn inc_requests_success(&self) {
        self.requests_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment error requests counter.
    #[inline]
    pub fn inc_requests_error(&self) {
        self.requests_error.fetch_add(1, Ordering::Relaxed);
    }

    /// Add bytes received.
    #[inline]
    pub fn add_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Increment messages batched counter.
    #[inline]
    pub fn inc_messages_batched(&self) {
        self.messages_batched.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment messages sent to Kafka counter.
    #[inline]
    pub fn add_messages_sent_kafka(&self, count: u64) {
        self.messages_sent_kafka.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment messages sent to loader counter.
    #[inline]
    pub fn add_messages_sent_loader(&self, count: u64) {
        self.messages_sent_loader.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment DLQ messages counter.
    #[inline]
    pub fn inc_messages_dlq(&self) {
        self.messages_dlq.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment spilled messages counter.
    #[inline]
    pub fn add_messages_spilled(&self, count: u64) {
        self.messages_spilled.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment drained messages counter.
    #[inline]
    pub fn add_messages_drained(&self, count: u64) {
        self.messages_drained.fetch_add(count, Ordering::Relaxed);
    }

    /// Set batch queue size gauge.
    #[inline]
    pub fn set_batch_queue_size(&self, size: u64) {
        self.batch_queue_size.store(size, Ordering::Relaxed);
    }

    /// Set batch queue bytes gauge.
    #[inline]
    pub fn set_batch_queue_bytes(&self, bytes: u64) {
        self.batch_queue_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Set spool bytes gauge.
    #[inline]
    pub fn set_spool_bytes(&self, bytes: u64) {
        self.spool_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Set spool messages gauge.
    #[inline]
    pub fn set_spool_messages(&self, count: u64) {
        self.spool_messages.store(count, Ordering::Relaxed);
    }

    /// Increment active connections.
    #[inline]
    pub fn inc_active_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active connections.
    #[inline]
    pub fn dec_active_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Set memory usage.
    #[inline]
    pub fn set_memory_usage(&self, used: u64, limit: u64) {
        self.memory_used_bytes.store(used, Ordering::Relaxed);
        self.memory_limit_bytes.store(limit, Ordering::Relaxed);
    }

    /// Get batch queue bytes for backpressure calculation.
    #[inline]
    pub fn get_batch_queue_bytes(&self) -> u64 {
        self.batch_queue_bytes.load(Ordering::Relaxed)
    }

    /// Get request rate per second.
    pub fn request_rate(&self) -> f64 {
        self.rate_window.read().rate_per_second()
    }

    /// Calculate compound KEDA scaling metric.
    ///
    /// Returns a value from 0.0-100.0 representing load:
    /// - 0-25: Low load, scale down
    /// - 25-50: Normal load
    /// - 50-75: Medium load
    /// - 75-100: High load, scale up
    pub fn keda_scaling_metric(&self) -> f64 {
        let queue_size = self.batch_queue_size.load(Ordering::Relaxed) as f64;
        let spool_messages = self.spool_messages.load(Ordering::Relaxed) as f64;
        let active_conns = self.active_connections.load(Ordering::Relaxed) as f64;
        let rate = self.request_rate();

        let memory_used = self.memory_used_bytes.load(Ordering::Relaxed) as f64;
        let memory_limit = self.memory_limit_bytes.load(Ordering::Relaxed) as f64;
        let memory_ratio = if memory_limit > 0.0 {
            memory_used / memory_limit
        } else {
            0.0
        };

        // Weighted components (tuned for PB/s scale)
        let queue_score = (queue_size / 10_000.0).min(1.0) * 25.0; // 10K queue = 25%
        let spool_score = (spool_messages / 1_000.0).min(1.0) * 20.0; // 1K spilled = 20%
        let conn_score = (active_conns / 1_000.0).min(1.0) * 15.0; // 1K conns = 15%
        let rate_score = (rate / 100_000.0).min(1.0) * 25.0; // 100K/s = 25%
        let memory_score = memory_ratio * 15.0; // 100% memory = 15%

        (queue_score + spool_score + conn_score + rate_score + memory_score).min(100.0)
    }

    /// Render metrics in Prometheus format.
    pub fn render(&self) -> String {
        let mut output = String::with_capacity(4096);

        // Counters
        output.push_str("# HELP receiver_requests_total Total number of requests received\n");
        output.push_str("# TYPE receiver_requests_total counter\n");
        output.push_str(&format!(
            "receiver_requests_total {}\n",
            self.requests_total.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_requests_success Total successful requests\n");
        output.push_str("# TYPE receiver_requests_success counter\n");
        output.push_str(&format!(
            "receiver_requests_success {}\n",
            self.requests_success.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_requests_error Total failed requests\n");
        output.push_str("# TYPE receiver_requests_error counter\n");
        output.push_str(&format!(
            "receiver_requests_error {}\n",
            self.requests_error.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_bytes_received_total Total bytes received\n");
        output.push_str("# TYPE receiver_bytes_received_total counter\n");
        output.push_str(&format!(
            "receiver_bytes_received_total {}\n",
            self.bytes_received.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_messages_batched_total Total messages batched\n");
        output.push_str("# TYPE receiver_messages_batched_total counter\n");
        output.push_str(&format!(
            "receiver_messages_batched_total {}\n",
            self.messages_batched.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_messages_sent_kafka_total Messages sent to Kafka\n");
        output.push_str("# TYPE receiver_messages_sent_kafka_total counter\n");
        output.push_str(&format!(
            "receiver_messages_sent_kafka_total {}\n",
            self.messages_sent_kafka.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_messages_sent_loader_total Messages sent to loader\n");
        output.push_str("# TYPE receiver_messages_sent_loader_total counter\n");
        output.push_str(&format!(
            "receiver_messages_sent_loader_total {}\n",
            self.messages_sent_loader.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_messages_dlq_total Messages sent to DLQ\n");
        output.push_str("# TYPE receiver_messages_dlq_total counter\n");
        output.push_str(&format!(
            "receiver_messages_dlq_total {}\n",
            self.messages_dlq.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_messages_spilled_total Messages spilled to disk\n");
        output.push_str("# TYPE receiver_messages_spilled_total counter\n");
        output.push_str(&format!(
            "receiver_messages_spilled_total {}\n",
            self.messages_spilled.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_messages_drained_total Messages drained from spool\n");
        output.push_str("# TYPE receiver_messages_drained_total counter\n");
        output.push_str(&format!(
            "receiver_messages_drained_total {}\n",
            self.messages_drained.load(Ordering::Relaxed)
        ));

        // Gauges
        output.push_str("# HELP receiver_batch_queue_size Current messages in batch queue\n");
        output.push_str("# TYPE receiver_batch_queue_size gauge\n");
        output.push_str(&format!(
            "receiver_batch_queue_size {}\n",
            self.batch_queue_size.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_batch_queue_bytes Current bytes in batch queue\n");
        output.push_str("# TYPE receiver_batch_queue_bytes gauge\n");
        output.push_str(&format!(
            "receiver_batch_queue_bytes {}\n",
            self.batch_queue_bytes.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_spool_bytes Current bytes in disk spool\n");
        output.push_str("# TYPE receiver_spool_bytes gauge\n");
        output.push_str(&format!(
            "receiver_spool_bytes {}\n",
            self.spool_bytes.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_spool_messages Current messages in disk spool\n");
        output.push_str("# TYPE receiver_spool_messages gauge\n");
        output.push_str(&format!(
            "receiver_spool_messages {}\n",
            self.spool_messages.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_active_connections Current active connections\n");
        output.push_str("# TYPE receiver_active_connections gauge\n");
        output.push_str(&format!(
            "receiver_active_connections {}\n",
            self.active_connections.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_memory_used_bytes Current memory usage\n");
        output.push_str("# TYPE receiver_memory_used_bytes gauge\n");
        output.push_str(&format!(
            "receiver_memory_used_bytes {}\n",
            self.memory_used_bytes.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_memory_limit_bytes Memory limit\n");
        output.push_str("# TYPE receiver_memory_limit_bytes gauge\n");
        output.push_str(&format!(
            "receiver_memory_limit_bytes {}\n",
            self.memory_limit_bytes.load(Ordering::Relaxed)
        ));

        // KEDA scaling metric
        output.push_str("# HELP receiver_keda_scaling_metric Compound scaling metric for KEDA (0-100)\n");
        output.push_str("# TYPE receiver_keda_scaling_metric gauge\n");
        output.push_str(&format!(
            "receiver_keda_scaling_metric {:.2}\n",
            self.keda_scaling_metric()
        ));

        output.push_str("# HELP receiver_request_rate_per_second Current request rate\n");
        output.push_str("# TYPE receiver_request_rate_per_second gauge\n");
        output.push_str(&format!(
            "receiver_request_rate_per_second {:.2}\n",
            self.request_rate()
        ));

        output
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_counters() {
        let metrics = Metrics::new();

        metrics.inc_requests_total();
        metrics.inc_requests_total();
        metrics.inc_requests_success();

        let output = metrics.render();
        assert!(output.contains("receiver_requests_total 2"));
        assert!(output.contains("receiver_requests_success 1"));
    }

    #[test]
    fn test_metrics_gauges() {
        let metrics = Metrics::new();

        metrics.set_batch_queue_size(100);
        metrics.set_batch_queue_bytes(1024);

        assert_eq!(metrics.get_batch_queue_bytes(), 1024);

        let output = metrics.render();
        assert!(output.contains("receiver_batch_queue_size 100"));
        assert!(output.contains("receiver_batch_queue_bytes 1024"));
    }

    #[test]
    fn test_keda_scaling_metric_zero() {
        let metrics = Metrics::new();

        // With no load, metric should be near 0
        let metric = metrics.keda_scaling_metric();
        assert!(metric >= 0.0);
        assert!(metric <= 10.0);
    }

    #[test]
    fn test_keda_scaling_metric_high_queue() {
        let metrics = Metrics::new();

        // High queue should increase metric
        metrics.set_batch_queue_size(10_000);
        let metric = metrics.keda_scaling_metric();
        assert!(metric >= 20.0);
    }

    #[test]
    fn test_keda_scaling_metric_max() {
        let metrics = Metrics::new();

        metrics.set_batch_queue_size(100_000);
        metrics.set_spool_messages(10_000);
        metrics.set_memory_usage(1_000_000, 1_000_000);

        for _ in 0..1000 {
            metrics.inc_active_connections();
        }

        let metric = metrics.keda_scaling_metric();
        assert!(metric <= 100.0);
    }

    #[test]
    fn test_spill_metrics() {
        let metrics = Metrics::new();

        metrics.add_messages_spilled(10);
        metrics.add_messages_drained(5);

        let output = metrics.render();
        assert!(output.contains("receiver_messages_spilled_total 10"));
        assert!(output.contains("receiver_messages_drained_total 5"));
    }

    #[test]
    fn test_memory_metrics() {
        let metrics = Metrics::new();

        metrics.set_memory_usage(500_000, 1_000_000);

        let output = metrics.render();
        assert!(output.contains("receiver_memory_used_bytes 500000"));
        assert!(output.contains("receiver_memory_limit_bytes 1000000"));
    }
}
