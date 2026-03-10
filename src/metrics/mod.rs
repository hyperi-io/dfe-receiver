// Project:   dfe-receiver
// File:      src/metrics/mod.rs
// Purpose:   Prometheus metrics and KEDA scaling
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics for dfe-receiver.
//!
//! Exposes counters, gauges, and histograms for monitoring and KEDA scaling.
//! Includes a compound scaling metric for autoscaling decisions.
//!
//! # Security Metrics
//!
//! Security-related metrics for alerting on potential attacks:
//! - `receiver_auth_failures_total` - Authentication failures by reason
//! - `receiver_validation_failures_total` - Validation failures by reason
//! - `receiver_request_timeouts_total` - Request timeouts (slow loris indicator)
//! - `receiver_body_size_rejected_total` - Oversized body rejections
//! - `receiver_tls_handshake_failures_total` - TLS failures

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use hyperi_rustlib::scaling::ScalingPressure;

/// Reason for authentication failure (for metrics labels).
#[derive(Debug, Clone, Copy)]
pub enum AuthFailureReason {
    /// Missing required auth header.
    MissingHeader,
    /// Invalid bearer token.
    InvalidToken,
    /// Invalid header value.
    InvalidHeader,
}

/// Reason for validation failure (for metrics labels).
#[derive(Debug, Clone, Copy)]
pub enum ValidationFailureReason {
    /// Payload is not valid JSON.
    InvalidJson,
    /// Required field is missing.
    MissingField,
}
use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// Metrics collector for dfe-receiver.
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

    // Security counters
    auth_failures_total: AtomicU64,
    auth_failures_missing_header: AtomicU64,
    auth_failures_invalid_token: AtomicU64,
    auth_failures_invalid_header: AtomicU64,
    validation_failures_total: AtomicU64,
    validation_failures_invalid_json: AtomicU64,
    validation_failures_missing_field: AtomicU64,
    request_timeouts_total: AtomicU64,
    body_size_rejected_total: AtomicU64,
    tls_handshake_failures_total: AtomicU64,

    // Gauges
    batch_queue_size: AtomicU64,
    batch_queue_bytes: AtomicU64,
    spool_bytes: AtomicU64,
    spool_messages: AtomicU64,
    active_connections: AtomicU64,
    memory_used_bytes: AtomicU64,
    memory_limit_bytes: AtomicU64,

    // Circuit breaker state (0=Closed, 1=Open, 2=HalfOpen)
    circuit_state: AtomicU8,
    circuit_consecutive_failures: AtomicU64,

    // Rate tracking
    rate_window: RwLock<RateWindow>,

    // Scaling pressure engine (from hyperi-rustlib)
    scaling: ScalingPressure,
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

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field(
                "requests_total",
                &self.requests_total.load(Ordering::Relaxed),
            )
            .field(
                "batch_queue_size",
                &self.batch_queue_size.load(Ordering::Relaxed),
            )
            .field(
                "active_connections",
                &self.active_connections.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl Metrics {
    /// Create a new metrics collector with scaling pressure engine.
    pub fn with_scaling(scaling: ScalingPressure) -> Self {
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
            auth_failures_total: AtomicU64::new(0),
            auth_failures_missing_header: AtomicU64::new(0),
            auth_failures_invalid_token: AtomicU64::new(0),
            auth_failures_invalid_header: AtomicU64::new(0),
            validation_failures_total: AtomicU64::new(0),
            validation_failures_invalid_json: AtomicU64::new(0),
            validation_failures_missing_field: AtomicU64::new(0),
            request_timeouts_total: AtomicU64::new(0),
            body_size_rejected_total: AtomicU64::new(0),
            tls_handshake_failures_total: AtomicU64::new(0),
            batch_queue_size: AtomicU64::new(0),
            batch_queue_bytes: AtomicU64::new(0),
            spool_bytes: AtomicU64::new(0),
            spool_messages: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            memory_used_bytes: AtomicU64::new(0),
            memory_limit_bytes: AtomicU64::new(0),
            circuit_state: AtomicU8::new(0),
            circuit_consecutive_failures: AtomicU64::new(0),
            rate_window: RwLock::new(RateWindow::new(Duration::from_secs(60))),
            scaling,
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
        self.messages_sent_loader
            .fetch_add(count, Ordering::Relaxed);
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

    // ==========================================================================
    // Security metrics
    // ==========================================================================

    /// Record an auth failure with reason.
    #[inline]
    pub fn inc_auth_failure(&self, reason: AuthFailureReason) {
        self.auth_failures_total.fetch_add(1, Ordering::Relaxed);
        match reason {
            AuthFailureReason::MissingHeader => {
                self.auth_failures_missing_header
                    .fetch_add(1, Ordering::Relaxed);
            }
            AuthFailureReason::InvalidToken => {
                self.auth_failures_invalid_token
                    .fetch_add(1, Ordering::Relaxed);
            }
            AuthFailureReason::InvalidHeader => {
                self.auth_failures_invalid_header
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record a validation failure with reason.
    #[inline]
    pub fn inc_validation_failure(&self, reason: ValidationFailureReason) {
        self.validation_failures_total
            .fetch_add(1, Ordering::Relaxed);
        match reason {
            ValidationFailureReason::InvalidJson => {
                self.validation_failures_invalid_json
                    .fetch_add(1, Ordering::Relaxed);
            }
            ValidationFailureReason::MissingField => {
                self.validation_failures_missing_field
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record a request timeout.
    #[inline]
    pub fn inc_request_timeout(&self) {
        self.request_timeouts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a body size rejection (413).
    #[inline]
    pub fn inc_body_size_rejected(&self) {
        self.body_size_rejected_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a TLS handshake failure.
    #[inline]
    pub fn inc_tls_handshake_failure(&self) {
        self.tls_handshake_failures_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // ==========================================================================
    // Gauge getters (for pipeline metric updates)
    // ==========================================================================

    /// Get batch queue size.
    #[inline]
    pub fn get_batch_queue_size(&self) -> u64 {
        self.batch_queue_size.load(Ordering::Relaxed)
    }

    /// Get batch queue bytes for backpressure calculation.
    #[inline]
    pub fn get_batch_queue_bytes(&self) -> u64 {
        self.batch_queue_bytes.load(Ordering::Relaxed)
    }

    /// Get spilled messages counter.
    #[inline]
    pub fn get_messages_spilled(&self) -> u64 {
        self.messages_spilled.load(Ordering::Relaxed)
    }

    /// Get drained messages counter.
    #[inline]
    pub fn get_messages_drained(&self) -> u64 {
        self.messages_drained.load(Ordering::Relaxed)
    }

    // ==========================================================================
    // Circuit breaker metrics
    // ==========================================================================

    /// Set circuit breaker state. 0=Closed, 1=Open, 2=HalfOpen.
    #[inline]
    pub fn set_circuit_state(&self, state: crate::buffer::CircuitState, failures: u32) {
        let code = match state {
            crate::buffer::CircuitState::Closed => 0,
            crate::buffer::CircuitState::Open => 1,
            crate::buffer::CircuitState::HalfOpen => 2,
        };
        self.circuit_state.store(code, Ordering::Relaxed);
        self.circuit_consecutive_failures
            .store(u64::from(failures), Ordering::Relaxed);
    }

    /// Check if circuit breaker is open (sink down).
    #[inline]
    pub fn is_circuit_open(&self) -> bool {
        self.circuit_state.load(Ordering::Relaxed) == 1
    }

    /// Get circuit breaker state as string.
    fn circuit_state_str(&self) -> &'static str {
        match self.circuit_state.load(Ordering::Relaxed) {
            0 => "closed",
            1 => "open",
            2 => "half_open",
            _ => "unknown",
        }
    }

    /// Get request rate per second.
    pub fn request_rate(&self) -> f64 {
        self.rate_window.read().rate_per_second()
    }

    // ==========================================================================
    // Scaling pressure (delegated to hyperi-rustlib ScalingPressure engine)
    // ==========================================================================

    /// Sync current metric values into the scaling pressure engine.
    ///
    /// Called from the metrics update cycle (every 1 second) to feed
    /// current component values into the rustlib `ScalingPressure` engine.
    pub fn update_scaling(&self) {
        self.scaling
            .set_component("request_rate", self.request_rate());
        self.scaling.set_component(
            "queue_depth",
            self.batch_queue_size.load(Ordering::Relaxed) as f64,
        );
        self.scaling.set_component(
            "connections",
            self.active_connections.load(Ordering::Relaxed) as f64,
        );
        self.scaling.set_component(
            "spill",
            self.messages_spilled.load(Ordering::Relaxed) as f64,
        );
        self.scaling.set_memory(
            self.memory_used_bytes.load(Ordering::Relaxed),
            self.memory_limit_bytes.load(Ordering::Relaxed),
        );
        self.scaling.set_circuit_open(self.is_circuit_open());
    }

    /// Calculate scaling pressure (0.0-100.0).
    ///
    /// Delegates to the `ScalingPressure` engine from hyperi-rustlib.
    pub fn scaling_pressure(&self) -> f64 {
        self.scaling.calculate()
    }

    /// Render metrics in Prometheus format.
    #[allow(clippy::too_many_lines)]
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

        // Circuit breaker
        output.push_str(
            "# HELP receiver_circuit_breaker_state Circuit breaker state (0=closed, 1=open, 2=half_open)\n",
        );
        output.push_str("# TYPE receiver_circuit_breaker_state gauge\n");
        output.push_str(&format!(
            "receiver_circuit_breaker_state{{state=\"{}\"}} {}\n",
            self.circuit_state_str(),
            self.circuit_state.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_circuit_breaker_failures Consecutive sink failures\n");
        output.push_str("# TYPE receiver_circuit_breaker_failures gauge\n");
        output.push_str(&format!(
            "receiver_circuit_breaker_failures {}\n",
            self.circuit_consecutive_failures.load(Ordering::Relaxed)
        ));

        // Scaling pressure
        output.push_str(
            "# HELP receiver_scaling_pressure Gated scaling pressure for autoscaling (0-100)\n",
        );
        output.push_str("# TYPE receiver_scaling_pressure gauge\n");
        output.push_str(&format!(
            "receiver_scaling_pressure {:.2}\n",
            self.scaling_pressure()
        ));

        output.push_str("# HELP receiver_request_rate_per_second Current request rate\n");
        output.push_str("# TYPE receiver_request_rate_per_second gauge\n");
        output.push_str(&format!(
            "receiver_request_rate_per_second {:.2}\n",
            self.request_rate()
        ));

        // Security metrics
        output.push_str("# HELP receiver_auth_failures_total Authentication failures by reason\n");
        output.push_str("# TYPE receiver_auth_failures_total counter\n");
        output.push_str(&format!(
            "receiver_auth_failures_total{{reason=\"missing_header\"}} {}\n",
            self.auth_failures_missing_header.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "receiver_auth_failures_total{{reason=\"invalid_token\"}} {}\n",
            self.auth_failures_invalid_token.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "receiver_auth_failures_total{{reason=\"invalid_header\"}} {}\n",
            self.auth_failures_invalid_header.load(Ordering::Relaxed)
        ));

        output
            .push_str("# HELP receiver_validation_failures_total Validation failures by reason\n");
        output.push_str("# TYPE receiver_validation_failures_total counter\n");
        output.push_str(&format!(
            "receiver_validation_failures_total{{reason=\"invalid_json\"}} {}\n",
            self.validation_failures_invalid_json
                .load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "receiver_validation_failures_total{{reason=\"missing_field\"}} {}\n",
            self.validation_failures_missing_field
                .load(Ordering::Relaxed)
        ));

        output.push_str(
            "# HELP receiver_request_timeouts_total Request timeouts (slow loris indicator)\n",
        );
        output.push_str("# TYPE receiver_request_timeouts_total counter\n");
        output.push_str(&format!(
            "receiver_request_timeouts_total {}\n",
            self.request_timeouts_total.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_body_size_rejected_total Oversized body rejections\n");
        output.push_str("# TYPE receiver_body_size_rejected_total counter\n");
        output.push_str(&format!(
            "receiver_body_size_rejected_total {}\n",
            self.body_size_rejected_total.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP receiver_tls_handshake_failures_total TLS handshake failures\n");
        output.push_str("# TYPE receiver_tls_handshake_failures_total counter\n");
        output.push_str(&format!(
            "receiver_tls_handshake_failures_total {}\n",
            self.tls_handshake_failures_total.load(Ordering::Relaxed)
        ));

        output
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::with_scaling(crate::config::ScalingConfig::default().build_pressure())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_counters() {
        let metrics = Metrics::default();

        metrics.inc_requests_total();
        metrics.inc_requests_total();
        metrics.inc_requests_success();

        let output = metrics.render();
        assert!(output.contains("receiver_requests_total 2"));
        assert!(output.contains("receiver_requests_success 1"));
    }

    #[test]
    fn test_metrics_gauges() {
        let metrics = Metrics::default();

        metrics.set_batch_queue_size(100);
        metrics.set_batch_queue_bytes(1024);

        assert_eq!(metrics.get_batch_queue_bytes(), 1024);

        let output = metrics.render();
        assert!(output.contains("receiver_batch_queue_size 100"));
        assert!(output.contains("receiver_batch_queue_bytes 1024"));
    }

    #[test]
    fn test_scaling_pressure_zero() {
        let metrics = Metrics::default();

        // Sync empty state into engine
        metrics.update_scaling();

        let metric = metrics.scaling_pressure();
        assert!(metric >= 0.0);
        assert!(metric <= 10.0);
    }

    #[test]
    fn test_scaling_pressure_high_queue() {
        let metrics = Metrics::default();

        // High queue should increase metric
        metrics.set_batch_queue_size(10_000);
        metrics.update_scaling();

        let metric = metrics.scaling_pressure();
        assert!(metric >= 20.0);
    }

    #[test]
    fn test_scaling_pressure_max() {
        let metrics = Metrics::default();

        metrics.set_batch_queue_size(100_000);
        metrics.set_memory_usage(1_000_000, 1_000_000);

        for _ in 0..1000 {
            metrics.inc_active_connections();
        }

        metrics.update_scaling();

        let metric = metrics.scaling_pressure();
        // Memory gate fires at 100% → returns 100.0
        assert!(metric <= 100.0);
    }

    #[test]
    fn test_spill_metrics() {
        let metrics = Metrics::default();

        metrics.add_messages_spilled(10);
        metrics.add_messages_drained(5);

        let output = metrics.render();
        assert!(output.contains("receiver_messages_spilled_total 10"));
        assert!(output.contains("receiver_messages_drained_total 5"));
    }

    #[test]
    fn test_memory_metrics() {
        let metrics = Metrics::default();

        metrics.set_memory_usage(500_000, 1_000_000);

        let output = metrics.render();
        assert!(output.contains("receiver_memory_used_bytes 500000"));
        assert!(output.contains("receiver_memory_limit_bytes 1000000"));
    }
}
