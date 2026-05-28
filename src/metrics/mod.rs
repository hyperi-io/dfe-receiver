// Project:   dfe-receiver
// File:      src/metrics/mod.rs
// Purpose:   Prometheus metrics and KEDA scaling (dfe_receiver namespace)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics for dfe-receiver.
//!
//! Uses `MetricsManager` with namespace `dfe_receiver` and rustlib
//! `dfe_groups` for standardised metric groups. Receiver-specific
//! counters use the `metrics` crate directly with transport labels.
//!
//! # Security Metrics
//!
//! Security-related metrics for alerting on potential attacks:
//! - `dfe_receiver_auth_failures_total` - Authentication failures by reason
//! - `dfe_receiver_validation_failures_total` - Validation failures by reason
//! - `dfe_receiver_request_timeouts_total` - Request timeouts (slow loris indicator)
//! - `dfe_receiver_body_size_rejected_total` - Oversized body rejections
//! - `dfe_receiver_tls_handshake_failures_total` - TLS failures

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use hyperi_rustlib::metrics::DfeMetrics;
use hyperi_rustlib::metrics::MetricsManager;
use hyperi_rustlib::metrics::dfe_groups::{
    AppMetrics, BackpressureMetrics, BufferMetrics, CircuitBreakerMetrics, SinkMetrics,
};
use hyperi_rustlib::metrics::{
    AuthFailureReason as RlAuthReason, TransportKind, ValidationFailureReason as RlValidationReason,
};
use hyperi_rustlib::scaling::{RateWindow, ScalingPressure};

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

/// Metrics collector for dfe-receiver.
///
/// Wraps rustlib metric groups (`AppMetrics`, `BufferMetrics`, etc.) plus
/// receiver-specific counters with transport labels. Atomics are retained
/// for values the scaling pressure engine reads back.
pub struct Metrics {
    // Atomics for scaling engine read-back and test getter access
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

    // Security counters (atomics for test getter access)
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

    // Gauge atomics (for scaling read-back)
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
    rate_window: RateWindow,

    // Scaling pressure engine (from hyperi-rustlib)
    scaling: ScalingPressure,

    // Standard DFE metrics (dual-emit `dfe_*` alongside `dfe_receiver_*`).
    // None in tests (no global recorder); Some in prod after MetricsManager.
    dfe: Option<DfeMetrics>,

    // Rustlib metric groups (None in tests without MetricsManager)
    app_group: Option<AppMetrics>,
    buffer_group: Option<BufferMetrics>,
    sink_group: Option<SinkMetrics>,
    cb_group: Option<CircuitBreakerMetrics>,
    bp_group: Option<BackpressureMetrics>,
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
    ///
    /// No MetricsManager — use for tests or standalone contexts.
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
            rate_window: RateWindow::new(Duration::from_mins(1)),
            scaling,
            dfe: None,
            app_group: None,
            buffer_group: None,
            sink_group: None,
            cb_group: None,
            bp_group: None,
        }
    }

    /// Create a metrics collector with standard DFE metrics and metric groups.
    ///
    /// Creates a `MetricsManager` with namespace `dfe_receiver`, registers
    /// all metric groups, and calls `DfeMetrics::register()` for platform metrics.
    /// Returns both the `Metrics` and the `MetricsManager` — caller must use the
    /// returned manager for `start_server()`. Creating a second `MetricsManager`
    /// will panic (global Prometheus recorder can only be installed once).
    pub fn with_dfe_metrics(scaling: ScalingPressure) -> (Self, MetricsManager) {
        let manager = MetricsManager::new("dfe_receiver");
        let metrics = Self::register_on(scaling, &manager);
        (metrics, manager)
    }

    /// Register receiver metric groups on an existing `MetricsManager`.
    ///
    /// Use this when `ServiceRuntime` has already created the manager and
    /// installed the global Prometheus recorder.
    pub fn register_on(scaling: ScalingPressure, manager: &MetricsManager) -> Self {
        let app = AppMetrics::new(manager, env!("CARGO_PKG_VERSION"), "dev");
        let buffer = BufferMetrics::new(manager);
        let sink = SinkMetrics::new(manager);
        let cb = CircuitBreakerMetrics::new(manager);
        let bp = BackpressureMetrics::new(manager);
        let dfe = DfeMetrics::register(manager);

        describe_receiver_metrics();

        let mut metrics = Self::with_scaling(scaling);
        metrics.dfe = Some(dfe);
        metrics.app_group = Some(app);
        metrics.buffer_group = Some(buffer);
        metrics.sink_group = Some(sink);
        metrics.cb_group = Some(cb);
        metrics.bp_group = Some(bp);
        metrics
    }

    // ======================================================================
    // Request counters (with transport label)
    // ======================================================================

    /// Increment total requests counter.
    /// Rate window sampled every 100 requests to reduce write lock contention.
    #[inline]
    pub fn inc_requests_total(&self, transport: &str) {
        let count = self.requests_total.fetch_add(1, Ordering::Relaxed) + 1;
        if count.is_multiple_of(100) {
            self.rate_window.record(count);
        }
        metrics::counter!("dfe_receiver_requests_total", "transport" => transport.to_string())
            .increment(1);
        if let Some(ref dfe) = self.dfe {
            dfe.records_received(1);
        }
        if let Some(ref app) = self.app_group {
            app.record_received(1);
        }
    }

    /// Increment successful requests counter.
    #[inline]
    pub fn inc_requests_success(&self, transport: &str) {
        self.requests_success.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "dfe_receiver_requests_success_total",
            "transport" => transport.to_string()
        )
        .increment(1);
        if let Some(ref dfe) = self.dfe {
            dfe.records_delivered(1);
        }
        if let Some(ref app) = self.app_group {
            app.record_processed(1);
        }
    }

    /// Increment error requests counter.
    #[inline]
    pub fn inc_requests_error(&self, transport: &str) {
        self.requests_error.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "dfe_receiver_requests_error_total",
            "transport" => transport.to_string()
        )
        .increment(1);
        if let Some(ref app) = self.app_group {
            app.record_error(1);
        }
    }

    /// Add bytes received.
    #[inline]
    pub fn add_bytes_received(&self, transport: &str, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
        metrics::counter!(
            "dfe_receiver_bytes_received_total",
            "transport" => transport.to_string()
        )
        .increment(bytes);
        if let Some(ref app) = self.app_group {
            app.record_bytes_received(bytes);
        }
    }

    // ======================================================================
    // Pipeline counters (no transport label)
    // ======================================================================

    /// Increment messages batched counter.
    #[inline]
    pub fn inc_messages_batched(&self) {
        self.messages_batched.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment messages sent to Kafka counter.
    #[inline]
    pub fn add_messages_sent_kafka(&self, count: u64) {
        self.messages_sent_kafka.fetch_add(count, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.transport_sent(TransportKind::Kafka, count);
        }
    }

    /// Increment messages sent to loader counter.
    #[inline]
    pub fn add_messages_sent_loader(&self, count: u64) {
        self.messages_sent_loader
            .fetch_add(count, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            // Loader sink rides the gRPC transport.
            dfe.transport_sent(TransportKind::Grpc, count);
        }
    }

    /// Increment DLQ messages counter.
    #[inline]
    pub fn inc_messages_dlq(&self) {
        self.messages_dlq.fetch_add(1, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.records_dlq(1);
        }
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

    // ======================================================================
    // Gauges
    // ======================================================================

    /// Set batch queue size gauge.
    #[inline]
    pub fn set_batch_queue_size(&self, size: u64) {
        self.batch_queue_size.store(size, Ordering::Relaxed);
        if let Some(ref buf) = self.buffer_group {
            buf.buffer_records.set(size as f64);
        }
    }

    /// Set batch queue bytes gauge.
    #[inline]
    pub fn set_batch_queue_bytes(&self, bytes: u64) {
        self.batch_queue_bytes.store(bytes, Ordering::Relaxed);
        if let Some(ref buf) = self.buffer_group {
            buf.buffer_bytes.set(bytes as f64);
        }
    }

    /// Set spool bytes gauge.
    #[inline]
    pub fn set_spool_bytes(&self, bytes: u64) {
        self.spool_bytes.store(bytes, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.spool_bytes(bytes as f64);
        }
    }

    /// Set spool messages gauge.
    #[inline]
    pub fn set_spool_messages(&self, count: u64) {
        self.spool_messages.store(count, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.spool_messages(count as f64);
        }
    }

    /// Increment active connections.
    #[inline]
    pub fn inc_active_connections(&self, transport: &str) {
        let count = self.active_connections.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!(
            "dfe_receiver_active_connections",
            "transport" => transport.to_string()
        )
        .set(count as f64);
    }

    /// Decrement active connections.
    #[inline]
    pub fn dec_active_connections(&self, transport: &str) {
        let count = self
            .active_connections
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        metrics::gauge!(
            "dfe_receiver_active_connections",
            "transport" => transport.to_string()
        )
        .set(count as f64);
    }

    /// Record request duration.
    #[inline]
    pub fn record_request_duration(&self, transport: &str, duration_secs: f64) {
        metrics::histogram!(
            "dfe_receiver_request_duration_seconds",
            "transport" => transport.to_string()
        )
        .record(duration_secs);
    }

    /// Set memory usage.
    #[inline]
    pub fn set_memory_usage(&self, used: u64, limit: u64) {
        self.memory_used_bytes.store(used, Ordering::Relaxed);
        self.memory_limit_bytes.store(limit, Ordering::Relaxed);
        if let Some(ref app) = self.app_group {
            app.set_memory(used, limit);
        }
    }

    // ======================================================================
    // Security metrics
    // ======================================================================

    /// Record an auth failure with reason.
    #[inline]
    pub fn inc_auth_failure(&self, reason: AuthFailureReason) {
        self.auth_failures_total.fetch_add(1, Ordering::Relaxed);
        // Local label string for the bespoke dfe_receiver_* metric, plus the
        // standardised rustlib enum for DfeMetrics. rustlib v2.8.0 typed the
        // auth_failure label (RFC 6749 codes); our fine-grained local reasons
        // map to the closest rustlib variant.
        let (reason_str, dfe_reason) = match reason {
            AuthFailureReason::MissingHeader => {
                self.auth_failures_missing_header
                    .fetch_add(1, Ordering::Relaxed);
                ("missing_header", RlAuthReason::Unauthorized)
            }
            AuthFailureReason::InvalidToken => {
                self.auth_failures_invalid_token
                    .fetch_add(1, Ordering::Relaxed);
                ("invalid_token", RlAuthReason::MalformedToken)
            }
            AuthFailureReason::InvalidHeader => {
                self.auth_failures_invalid_header
                    .fetch_add(1, Ordering::Relaxed);
                ("invalid_header", RlAuthReason::MalformedToken)
            }
        };
        metrics::counter!(
            "dfe_receiver_auth_failures_total",
            "reason" => reason_str.to_string()
        )
        .increment(1);
        if let Some(ref dfe) = self.dfe {
            dfe.auth_failure(dfe_reason);
        }
    }

    /// Record a validation failure with reason.
    #[inline]
    pub fn inc_validation_failure(&self, reason: ValidationFailureReason) {
        self.validation_failures_total
            .fetch_add(1, Ordering::Relaxed);
        // Local label string + standardised rustlib enum (v2.8.0 typed the
        // validation_failure label). InvalidJson maps to EncodingError (the
        // input bytes can't be decoded into a JSON value); MissingField maps
        // exactly to FieldMissing.
        let (reason_str, dfe_reason) = match reason {
            ValidationFailureReason::InvalidJson => {
                self.validation_failures_invalid_json
                    .fetch_add(1, Ordering::Relaxed);
                ("invalid_json", RlValidationReason::EncodingError)
            }
            ValidationFailureReason::MissingField => {
                self.validation_failures_missing_field
                    .fetch_add(1, Ordering::Relaxed);
                ("missing_field", RlValidationReason::FieldMissing)
            }
        };
        metrics::counter!(
            "dfe_receiver_validation_failures_total",
            "reason" => reason_str.to_string()
        )
        .increment(1);
        if let Some(ref dfe) = self.dfe {
            dfe.validation_failure(dfe_reason);
        }
    }

    /// Record a request timeout.
    #[inline]
    pub fn inc_request_timeout(&self) {
        self.request_timeouts_total.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("dfe_receiver_request_timeouts_total").increment(1);
    }

    /// Record a body size rejection (413).
    #[inline]
    pub fn inc_body_size_rejected(&self) {
        self.body_size_rejected_total
            .fetch_add(1, Ordering::Relaxed);
        metrics::counter!("dfe_receiver_body_size_rejected_total").increment(1);
    }

    /// Record a TLS handshake failure.
    #[inline]
    pub fn inc_tls_handshake_failure(&self) {
        self.tls_handshake_failures_total
            .fetch_add(1, Ordering::Relaxed);
        metrics::counter!("dfe_receiver_tls_handshake_failures_total").increment(1);
    }

    // ======================================================================
    // Gauge getters (for pipeline metric updates and tests)
    // ======================================================================

    /// Get total requests count.
    #[inline]
    pub fn get_requests_total(&self) -> u64 {
        self.requests_total.load(Ordering::Relaxed)
    }

    /// Get successful requests count.
    #[inline]
    pub fn get_requests_success(&self) -> u64 {
        self.requests_success.load(Ordering::Relaxed)
    }

    /// Get error requests count.
    #[inline]
    pub fn get_requests_error(&self) -> u64 {
        self.requests_error.load(Ordering::Relaxed)
    }

    /// Get total bytes received.
    #[inline]
    pub fn get_bytes_received(&self) -> u64 {
        self.bytes_received.load(Ordering::Relaxed)
    }

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

    /// Get active connections count.
    #[inline]
    pub fn get_active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Get auth failure count.
    #[inline]
    pub fn get_auth_failures_total(&self) -> u64 {
        self.auth_failures_total.load(Ordering::Relaxed)
    }

    /// Get validation failure count.
    #[inline]
    pub fn get_validation_failures_total(&self) -> u64 {
        self.validation_failures_total.load(Ordering::Relaxed)
    }

    /// Get TLS handshake failure count.
    #[inline]
    pub fn get_tls_handshake_failures_total(&self) -> u64 {
        self.tls_handshake_failures_total.load(Ordering::Relaxed)
    }

    // ======================================================================
    // Circuit breaker metrics
    // ======================================================================

    /// Set circuit breaker state. 0=Closed, 1=Open, 2=HalfOpen.
    #[inline]
    pub fn set_circuit_state(&self, state: crate::buffer::CircuitState, failures: u32) {
        let (code, state_str) = match state {
            crate::buffer::CircuitState::Closed => (0, "closed"),
            crate::buffer::CircuitState::Open => (1, "open"),
            crate::buffer::CircuitState::HalfOpen => (2, "half_open"),
        };
        self.circuit_state.store(code, Ordering::Relaxed);
        self.circuit_consecutive_failures
            .store(u64::from(failures), Ordering::Relaxed);
        if let Some(ref cb) = self.cb_group {
            cb.set_state("kafka", code);
            cb.record_transition("kafka", state_str);
        }
    }

    /// Check if circuit breaker is open (sink down).
    #[inline]
    pub fn is_circuit_open(&self) -> bool {
        self.circuit_state.load(Ordering::Relaxed) == 1
    }

    /// Get request rate per second.
    pub fn request_rate(&self) -> f64 {
        self.rate_window.rate_per_second()
    }

    // ======================================================================
    // Backpressure metrics
    // ======================================================================

    /// Record a backpressure event.
    #[inline]
    pub fn record_backpressure(&self) {
        if let Some(ref bp) = self.bp_group {
            bp.record_event();
        }
    }

    // ======================================================================
    // Scaling pressure (delegated to hyperi-rustlib ScalingPressure engine)
    // ======================================================================

    /// Sync current metric values into the scaling pressure engine.
    ///
    /// Called from the metrics update cycle (every 1 second) to feed
    /// current component values into the rustlib `ScalingPressure` engine.
    /// Also emits standard `dfe_scaling_*` gauges when `DfeMetrics` is active.
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

        let mem_used = self.memory_used_bytes.load(Ordering::Relaxed);
        let mem_limit = self.memory_limit_bytes.load(Ordering::Relaxed);
        self.scaling.set_memory(mem_used, mem_limit);

        let circuit_open = self.is_circuit_open();
        self.scaling.set_circuit_open(circuit_open);

        // Dual-emit scaling gauges via DfeMetrics
        if let Some(ref dfe) = self.dfe {
            let pressure = self.scaling.calculate();
            dfe.scaling_pressure(pressure);
            dfe.scaling_circuit_open(circuit_open);
            let mem_ratio = if mem_limit > 0 {
                mem_used as f64 / mem_limit as f64
            } else {
                0.0
            };
            dfe.scaling_memory_pressure(mem_ratio);
        }
    }

    /// Calculate scaling pressure (0.0-100.0).
    ///
    /// Delegates to the `ScalingPressure` engine from hyperi-rustlib.
    pub fn scaling_pressure(&self) -> f64 {
        self.scaling.calculate()
    }
}

/// Describe receiver-specific metrics that take labels.
fn describe_receiver_metrics() {
    metrics::describe_counter!(
        "dfe_receiver_requests_total",
        "Total requests received by transport"
    );
    metrics::describe_counter!(
        "dfe_receiver_requests_success_total",
        "Total successful requests by transport"
    );
    metrics::describe_counter!(
        "dfe_receiver_requests_error_total",
        "Total failed requests by transport"
    );
    metrics::describe_counter!(
        "dfe_receiver_bytes_received_total",
        "Total bytes received by transport"
    );
    metrics::describe_counter!(
        "dfe_receiver_auth_failures_total",
        "Authentication failures by reason"
    );
    metrics::describe_counter!(
        "dfe_receiver_validation_failures_total",
        "Validation failures by reason"
    );
    metrics::describe_counter!(
        "dfe_receiver_request_timeouts_total",
        "Request timeouts (slow loris indicator)"
    );
    metrics::describe_counter!(
        "dfe_receiver_body_size_rejected_total",
        "Oversized body rejections"
    );
    metrics::describe_counter!(
        "dfe_receiver_tls_handshake_failures_total",
        "TLS handshake failures"
    );
    metrics::describe_counter!(
        "dfe_receiver_messages_spilled_total",
        "Messages spilled to disk"
    );
    metrics::describe_counter!(
        "dfe_receiver_messages_drained_total",
        "Messages drained from spool"
    );

    // Request latency
    metrics::describe_histogram!(
        "dfe_receiver_request_duration_seconds",
        "End-to-end request processing latency"
    );
    metrics::describe_gauge!(
        "dfe_receiver_active_connections",
        "Currently active inbound connections"
    );

    // Kafka outbound
    metrics::describe_histogram!(
        "dfe_receiver_kafka_send_duration_seconds",
        "Kafka producer send latency"
    );
    metrics::describe_counter!("dfe_receiver_kafka_sends_total", "Total Kafka sends");
    metrics::describe_counter!(
        "dfe_receiver_kafka_bytes_sent_total",
        "Total bytes sent to Kafka"
    );
    metrics::describe_counter!("dfe_receiver_kafka_send_errors_total", "Kafka send errors");

    // EPS
    metrics::describe_gauge!(
        "dfe_receiver_events_per_second",
        "Current events per second (1s sample)"
    );
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

        metrics.inc_requests_total("test");
        metrics.inc_requests_total("test");
        metrics.inc_requests_success("test");

        assert_eq!(metrics.get_requests_total(), 2);
        assert_eq!(metrics.get_requests_success(), 1);
    }

    #[test]
    fn test_metrics_gauges() {
        let metrics = Metrics::default();

        metrics.set_batch_queue_size(100);
        metrics.set_batch_queue_bytes(1024);

        assert_eq!(metrics.get_batch_queue_size(), 100);
        assert_eq!(metrics.get_batch_queue_bytes(), 1024);
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
            metrics.inc_active_connections("test");
        }

        metrics.update_scaling();

        let metric = metrics.scaling_pressure();
        // Memory gate fires at 100% -> returns 100.0
        assert!(metric <= 100.0);
    }

    #[test]
    fn test_spill_metrics() {
        let metrics = Metrics::default();

        metrics.add_messages_spilled(10);
        metrics.add_messages_drained(5);

        assert_eq!(metrics.get_messages_spilled(), 10);
        assert_eq!(metrics.get_messages_drained(), 5);
    }

    #[test]
    fn test_memory_metrics() {
        let metrics = Metrics::default();
        metrics.set_memory_usage(500_000, 1_000_000);
        // Verify via scaling engine read-back (atomics)
        assert_eq!(metrics.memory_used_bytes.load(Ordering::Relaxed), 500_000);
        assert_eq!(
            metrics.memory_limit_bytes.load(Ordering::Relaxed),
            1_000_000
        );
    }

    #[test]
    fn test_security_counters() {
        let metrics = Metrics::default();

        metrics.inc_auth_failure(AuthFailureReason::MissingHeader);
        metrics.inc_auth_failure(AuthFailureReason::InvalidToken);
        metrics.inc_validation_failure(ValidationFailureReason::InvalidJson);
        metrics.inc_tls_handshake_failure();

        assert_eq!(metrics.get_auth_failures_total(), 2);
        assert_eq!(metrics.get_validation_failures_total(), 1);
        assert_eq!(metrics.get_tls_handshake_failures_total(), 1);
    }

    #[test]
    fn test_transport_label_counters() {
        let metrics = Metrics::default();

        metrics.inc_requests_total("http");
        metrics.inc_requests_total("grpc");
        metrics.inc_requests_total("http");

        // Atomics aggregate all transports
        assert_eq!(metrics.get_requests_total(), 3);
    }
}
