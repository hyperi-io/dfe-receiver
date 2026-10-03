// Project:   dfe-receiver
// File:      src/metrics/mod.rs
// Purpose:   Prometheus metrics and KEDA scaling (dfe namespace, receiver_* names)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics for dfe-receiver.
//!
//! The scalo metric groups and `ServiceMetrics` carry the platform set. Every
//! series the receiver records under its own `receiver_*` name is registered
//! on the same `MetricsManager` with its type, labels and description, so the
//! manifest `metrics-manifest` prints lists it. Names are bare unless
//! `metrics.namespace` sets a prefix.
//!
//! `records_*` count records and `receiver_requests_*` count requests. The
//! pipeline counts a record as received once, when a listener first offers
//! it, and again as taken or refused for good.
//!
//! # Security Metrics
//!
//! Security-related metrics for alerting on potential attacks:
//! - `receiver_auth_failures_total` - Authentication failures by reason
//! - `receiver_validation_failures_total` - Validation failures by reason
//! - `receiver_request_timeouts_total` - Request timeouts by transport (slow loris indicator)
//! - `receiver_body_size_rejected_total` - Oversized body rejections by transport
//! - `receiver_ip_filter_rejected_total` - Connections and datagrams the IP filter refused, by transport
//! - `receiver_rate_limited_total` - Requests the per-IP rate limit refused with 429, by transport
//! - `receiver_tls_handshake_failures_total` - TLS failures

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use scalo::metrics::MetricType;
use scalo::metrics::MetricsManager;
use scalo::metrics::ServiceMetrics;
use scalo::metrics::groups::{
    AppMetrics, BackpressureMetrics, BufferMetrics, CircuitBreakerMetrics, SinkMetrics,
};
use scalo::metrics::{
    AuthFailureReason as RlAuthReason, ValidationFailureReason as RlValidationReason,
};
use scalo::scaling::{RateWindow, ScalingPressure};

/// Reason for authentication failure (for metrics labels).
#[derive(Debug, Clone, Copy)]
pub enum AuthFailureReason {
    /// Missing required auth header.
    MissingHeader,
    /// Invalid bearer token.
    InvalidToken,
    /// Invalid header value.
    InvalidHeader,
    /// A webhook HMAC signature did not verify.
    InvalidSignature,
    /// A webhook signature was valid but its timestamp fell outside the
    /// replay window.
    StaleSignature,
}

impl AuthFailureReason {
    /// Every reason, in the order [`index`](Self::index) numbers them.
    pub const ALL: [Self; 5] = [
        Self::MissingHeader,
        Self::InvalidToken,
        Self::InvalidHeader,
        Self::InvalidSignature,
        Self::StaleSignature,
    ];

    /// The `reason` label on `receiver_auth_failures_total`.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::MissingHeader => "missing_header",
            Self::InvalidToken => "invalid_token",
            Self::InvalidHeader => "invalid_header",
            Self::InvalidSignature => "invalid_signature",
            Self::StaleSignature => "stale_signature",
        }
    }

    /// Position in [`ALL`](Self::ALL), for state kept per reason.
    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::MissingHeader => 0,
            Self::InvalidToken => 1,
            Self::InvalidHeader => 2,
            Self::InvalidSignature => 3,
            Self::StaleSignature => 4,
        }
    }
}

/// Reason for validation failure (for metrics labels).
#[derive(Debug, Clone, Copy)]
pub enum ValidationFailureReason {
    /// Payload is not valid JSON.
    InvalidJson,
    /// Required field is missing.
    MissingField,
    /// Payload nests deeper than the parser can take without exhausting its stack.
    NestingTooDeep,
}

impl ValidationFailureReason {
    /// Every reason, in the order [`index`](Self::index) numbers them.
    pub const ALL: [Self; 3] = [Self::InvalidJson, Self::MissingField, Self::NestingTooDeep];

    /// The `reason` label on `receiver_validation_failures_total`.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::InvalidJson => "invalid_json",
            Self::MissingField => "missing_field",
            Self::NestingTooDeep => "nesting_too_deep",
        }
    }

    /// Position in [`ALL`](Self::ALL), for state kept per reason.
    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::InvalidJson => 0,
            Self::MissingField => 1,
            Self::NestingTooDeep => 2,
        }
    }
}

/// Why a record was dropped with no way to tell its sender.
#[derive(Debug, Clone, Copy)]
pub enum DropReason {
    /// The pipeline could not take it, on a transport with no answer to carry
    /// a retry (UDP).
    Unavailable,
    /// The pipeline refused it for good, on a transport whose only answer is
    /// an acknowledgement or nothing.
    Rejected,
    /// Shutdown came while the record was held for a retry.
    Shutdown,
}

impl DropReason {
    fn label(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::Rejected => "rejected",
            Self::Shutdown => "shutdown",
        }
    }
}

/// Metrics collector for dfe-receiver.
///
/// Wraps scalo metric groups (`AppMetrics`, `BufferMetrics`, etc.) plus
/// receiver-specific counters with transport labels. Atomics are retained
/// for values the scaling pressure engine reads back.
pub struct Metrics {
    // Atomics for scaling engine read-back and test getter access
    requests_total: AtomicU64,
    requests_success: AtomicU64,
    requests_error: AtomicU64,
    bytes_received: AtomicU64,
    messages_batched: AtomicU64,
    messages_dlq: AtomicU64,
    messages_spilled: AtomicU64,
    messages_drained: AtomicU64,
    records_dropped: AtomicU64,
    records_received: AtomicU64,
    records_taken: AtomicU64,
    records_refused: AtomicU64,

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
    ip_filter_rejected_total: AtomicU64,
    rate_limited_total: AtomicU64,
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

    // Scaling pressure engine (from scalo). Shared `Arc` with the runtime's
    // engine -- the one `/scaling/pressure` serves to KEDA -- so the
    // once-per-second `update_scaling` feed drives the gauge KEDA scales on.
    scaling: Arc<ScalingPressure>,

    // Standard DFE metrics (dual-emit `dfe_*` alongside `dfe_receiver_*`).
    // None in tests (no global recorder); Some in prod after MetricsManager.
    dfe: Option<ServiceMetrics>,

    // scalo metric groups (None in tests without MetricsManager)
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
    /// No MetricsManager -- use for tests or standalone contexts.
    pub fn with_scaling(scaling: Arc<ScalingPressure>) -> Self {
        Self {
            requests_total: AtomicU64::new(0),
            requests_success: AtomicU64::new(0),
            requests_error: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            messages_batched: AtomicU64::new(0),
            messages_dlq: AtomicU64::new(0),
            messages_spilled: AtomicU64::new(0),
            messages_drained: AtomicU64::new(0),
            records_dropped: AtomicU64::new(0),
            records_received: AtomicU64::new(0),
            records_taken: AtomicU64::new(0),
            records_refused: AtomicU64::new(0),
            auth_failures_total: AtomicU64::new(0),
            auth_failures_missing_header: AtomicU64::new(0),
            auth_failures_invalid_token: AtomicU64::new(0),
            auth_failures_invalid_header: AtomicU64::new(0),
            validation_failures_total: AtomicU64::new(0),
            validation_failures_invalid_json: AtomicU64::new(0),
            validation_failures_missing_field: AtomicU64::new(0),
            request_timeouts_total: AtomicU64::new(0),
            body_size_rejected_total: AtomicU64::new(0),
            ip_filter_rejected_total: AtomicU64::new(0),
            rate_limited_total: AtomicU64::new(0),
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
    /// Creates a `MetricsManager` with namespace `dfe`, registers
    /// all metric groups, and calls `ServiceMetrics::register()` for platform metrics.
    /// Returns both the `Metrics` and the `MetricsManager` -- caller must use the
    /// returned manager for `start_server()`. Creating a second `MetricsManager`
    /// will panic (global Prometheus recorder can only be installed once).
    pub fn with_dfe_metrics(scaling: Arc<ScalingPressure>) -> (Self, MetricsManager) {
        let manager = MetricsManager::new("dfe");
        let metrics = Self::register_on(scaling, &manager);
        (metrics, manager)
    }

    /// Register receiver metric groups on an existing `MetricsManager`.
    ///
    /// Use this when `ServiceRuntime` has already created the manager and
    /// installed the global Prometheus recorder.
    pub fn register_on(scaling: Arc<ScalingPressure>, manager: &MetricsManager) -> Self {
        let app = AppMetrics::new(manager, env!("CARGO_PKG_VERSION"), "dev");
        let buffer = BufferMetrics::new(manager);
        let sink = SinkMetrics::new(manager);
        let cb = CircuitBreakerMetrics::new(manager);
        let bp = BackpressureMetrics::new(manager);
        let dfe = ServiceMetrics::register(manager);
        register_series(manager, RECEIVER_SERIES, "receiver");
        // The flow series too, so the manifest is the same whether flow runs or not.
        crate::server::flow::metrics::register_series_on(manager);

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
    ///
    /// A request is not a record: the records it carries count in
    /// [`add_records_received`](Self::add_records_received). The rate window
    /// is sampled every 100 requests to reduce write lock contention.
    #[inline]
    pub fn inc_requests_total(&self, transport: &str) {
        let count = self.requests_total.fetch_add(1, Ordering::Relaxed) + 1;
        if count.is_multiple_of(100) {
            self.rate_window.record(count);
        }
        metrics::counter!("receiver_requests_total", "transport" => transport.to_string())
            .increment(1);
    }

    /// Increment successful requests counter.
    #[inline]
    pub fn inc_requests_success(&self, transport: &str) {
        self.requests_success.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "receiver_requests_success_total",
            "transport" => transport.to_string()
        )
        .increment(1);
    }

    /// Increment error requests counter.
    #[inline]
    pub fn inc_requests_error(&self, transport: &str) {
        self.requests_error.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "receiver_requests_error_total",
            "transport" => transport.to_string()
        )
        .increment(1);
    }

    // ======================================================================
    // Record counters (no transport label)
    // ======================================================================

    /// Count records a listener offered the pipeline, once per record.
    ///
    /// `records_received_total` is owned by `ServiceMetrics`; the app group's
    /// handle names the same series, so it is counted here alone.
    #[inline]
    pub fn add_records_received(&self, count: u64) {
        self.records_received.fetch_add(count, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.records_received(count);
        }
    }

    /// Count what the pipeline made of records: `taken` in
    /// `records_processed_total` and `records_delivered_total`, `refused` for
    /// good in `records_error_total`.
    #[inline]
    pub fn add_records_settled(&self, taken: u64, refused: u64) {
        // Zero is skipped: a per-record listener settles one record per call.
        if taken > 0 {
            self.records_taken.fetch_add(taken, Ordering::Relaxed);
            if let Some(ref dfe) = self.dfe {
                dfe.records_delivered(taken);
            }
            if let Some(ref app) = self.app_group {
                app.record_processed(taken);
            }
        }
        if refused > 0 {
            self.records_refused.fetch_add(refused, Ordering::Relaxed);
            if let Some(ref app) = self.app_group {
                app.record_error(refused);
            }
        }
    }

    /// Get the records received count.
    #[inline]
    pub fn get_records_received(&self) -> u64 {
        self.records_received.load(Ordering::Relaxed)
    }

    /// Get the records taken count.
    #[inline]
    pub fn get_records_taken(&self) -> u64 {
        self.records_taken.load(Ordering::Relaxed)
    }

    /// Get the records refused-for-good count.
    #[inline]
    pub fn get_records_refused(&self) -> u64 {
        self.records_refused.load(Ordering::Relaxed)
    }

    /// Record a per-protocol parse/decode failure.
    ///
    /// Distinct from `inc_requests_error`, which conflates protocol decode
    /// failures with downstream process failures. This counter isolates the
    /// ingress-decode failure rate per protocol (syslog/gelf/otlp/...), the
    /// signal an operator needs to tell "the wire is malformed" apart from
    /// "the sink is unhappy". Also feeds the standard `ServiceMetrics`
    /// validation-failure counter (encoding category).
    #[inline]
    pub fn inc_parse_failure(&self, transport: &str) {
        metrics::counter!(
            "receiver_parse_failures_total",
            "transport" => transport.to_string()
        )
        .increment(1);
        if let Some(ref dfe) = self.dfe {
            dfe.validation_failure(RlValidationReason::EncodingError);
        }
    }

    /// Count records dropped with no way to tell the sender, by transport and
    /// reason.
    #[inline]
    pub fn add_records_dropped(&self, transport: &str, reason: DropReason, count: u64) {
        if count == 0 {
            return;
        }
        self.records_dropped.fetch_add(count, Ordering::Relaxed);
        metrics::counter!(
            "receiver_records_dropped_total",
            "transport" => transport.to_string(),
            "reason" => reason.label()
        )
        .increment(count);
    }

    /// Get the dropped-records count, across every transport and reason.
    #[inline]
    pub fn get_records_dropped(&self) -> u64 {
        self.records_dropped.load(Ordering::Relaxed)
    }

    /// Add bytes received.
    #[inline]
    pub fn add_bytes_received(&self, transport: &str, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
        metrics::counter!(
            "receiver_bytes_received_total",
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

    /// Increment DLQ messages counter.
    #[inline]
    pub fn inc_messages_dlq(&self) {
        self.messages_dlq.fetch_add(1, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.records_dlq(1);
        }
    }

    /// Get DLQ messages counter.
    #[inline]
    pub fn get_messages_dlq(&self) -> u64 {
        self.messages_dlq.load(Ordering::Relaxed)
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

    /// Publish the destination buffers' running totals: records held back from
    /// their sink, and records sent on from the hold since.
    pub fn set_buffered_totals(&self, spilled: u64, drained: u64) {
        metrics::counter!("receiver_messages_spilled_total").absolute(spilled);
        metrics::counter!("receiver_messages_drained_total").absolute(drained);
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

    /// Count an inbound connection open on `transport` until the returned
    /// guard drops.
    #[must_use = "the connection counts as open only while the guard lives"]
    pub fn open_connection(self: &Arc<Self>, transport: &'static str) -> ConnectionGuard {
        self.inc_active_connections(transport);
        ConnectionGuard {
            metrics: Arc::clone(self),
            transport,
        }
    }

    /// Increment active connections.
    ///
    /// The gauge moves per transport; the atomic behind the scaling score's
    /// `connections` component holds the total across every listener.
    #[inline]
    pub fn inc_active_connections(&self, transport: &str) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        metrics::gauge!(
            "receiver_active_connections",
            "transport" => transport.to_string()
        )
        .increment(1.0);
    }

    /// Decrement active connections, never below zero.
    #[inline]
    pub fn dec_active_connections(&self, transport: &str) {
        let _ = self
            .active_connections
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
        metrics::gauge!(
            "receiver_active_connections",
            "transport" => transport.to_string()
        )
        .decrement(1.0);
    }

    /// Record request duration.
    #[inline]
    pub fn record_request_duration(&self, transport: &str, duration_secs: f64) {
        metrics::histogram!(
            "receiver_request_duration_seconds",
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
        // scalo typed the ServiceMetrics auth_failure label (RFC 6749 codes);
        // each local reason maps to the closest scalo variant.
        let dfe_reason = match reason {
            AuthFailureReason::MissingHeader => {
                self.auth_failures_missing_header
                    .fetch_add(1, Ordering::Relaxed);
                RlAuthReason::Unauthorized
            }
            AuthFailureReason::InvalidToken => {
                self.auth_failures_invalid_token
                    .fetch_add(1, Ordering::Relaxed);
                RlAuthReason::MalformedToken
            }
            AuthFailureReason::InvalidHeader => {
                self.auth_failures_invalid_header
                    .fetch_add(1, Ordering::Relaxed);
                RlAuthReason::MalformedToken
            }
            AuthFailureReason::InvalidSignature => RlAuthReason::InvalidSignature,
            AuthFailureReason::StaleSignature => RlAuthReason::Expired,
        };
        metrics::counter!(
            "receiver_auth_failures_total",
            "reason" => reason.label()
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
        // Standardised scalo enum (scalo typed the validation_failure label).
        // InvalidJson maps to EncodingError (the input bytes can't be decoded
        // into a JSON value); MissingField maps exactly to FieldMissing;
        // NestingTooDeep is a bound, so OutOfRange.
        let dfe_reason = match reason {
            ValidationFailureReason::InvalidJson => {
                self.validation_failures_invalid_json
                    .fetch_add(1, Ordering::Relaxed);
                RlValidationReason::EncodingError
            }
            ValidationFailureReason::MissingField => {
                self.validation_failures_missing_field
                    .fetch_add(1, Ordering::Relaxed);
                RlValidationReason::FieldMissing
            }
            ValidationFailureReason::NestingTooDeep => RlValidationReason::OutOfRange,
        };
        metrics::counter!(
            "receiver_validation_failures_total",
            "reason" => reason.label()
        )
        .increment(1);
        if let Some(ref dfe) = self.dfe {
            dfe.validation_failure(dfe_reason);
        }
    }

    /// Record a request timed out on `transport` (408).
    #[inline]
    pub fn inc_request_timeout(&self, transport: &str) {
        self.request_timeouts_total.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "receiver_request_timeouts_total",
            "transport" => transport.to_string()
        )
        .increment(1);
    }

    /// Record a request refused on `transport` for its size: 413 over HTTP,
    /// `OUT_OF_RANGE` or `RESOURCE_EXHAUSTED` over gRPC.
    #[inline]
    pub fn inc_body_size_rejected(&self, transport: &str) {
        self.body_size_rejected_total
            .fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "receiver_body_size_rejected_total",
            "transport" => transport.to_string()
        )
        .increment(1);
    }

    /// Record a connection, or a datagram, the IP filter refused on `transport`.
    #[inline]
    pub fn inc_ip_filter_rejected(&self, transport: &str) {
        self.ip_filter_rejected_total
            .fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "receiver_ip_filter_rejected_total",
            "transport" => transport.to_string()
        )
        .increment(1);
    }

    /// Record a request the per-IP rate limit refused on `transport` (429).
    #[inline]
    pub fn inc_rate_limited(&self, transport: &str) {
        self.rate_limited_total.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "receiver_rate_limited_total",
            "transport" => transport.to_string()
        )
        .increment(1);
    }

    /// Record a TLS handshake failure.
    #[inline]
    pub fn inc_tls_handshake_failure(&self) {
        self.tls_handshake_failures_total
            .fetch_add(1, Ordering::Relaxed);
        metrics::counter!("receiver_tls_handshake_failures_total").increment(1);
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

    /// Get oversized-body rejection count.
    #[inline]
    pub fn get_body_size_rejected_total(&self) -> u64 {
        self.body_size_rejected_total.load(Ordering::Relaxed)
    }

    /// Get request timeout count.
    #[inline]
    pub fn get_request_timeouts_total(&self) -> u64 {
        self.request_timeouts_total.load(Ordering::Relaxed)
    }

    /// Get IP-filter rejection count.
    #[inline]
    pub fn get_ip_filter_rejected_total(&self) -> u64 {
        self.ip_filter_rejected_total.load(Ordering::Relaxed)
    }

    /// Get rate-limit refusal count.
    #[inline]
    pub fn get_rate_limited_total(&self) -> u64 {
        self.rate_limited_total.load(Ordering::Relaxed)
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
    // Scaling pressure (delegated to scalo ScalingPressure engine)
    // ======================================================================

    /// Sync current metric values into the scaling pressure engine.
    ///
    /// Called from the metrics update cycle (every 1 second) to feed
    /// current component values into the scalo `ScalingPressure` engine.
    /// Also emits standard `dfe_scaling_*` gauges when `ServiceMetrics` is active.
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

        // Dual-emit scaling gauges via ServiceMetrics
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
    /// Delegates to the `ScalingPressure` engine from scalo.
    pub fn scaling_pressure(&self) -> f64 {
        self.scaling.calculate()
    }
}

/// An inbound connection counted on `receiver_active_connections` until dropped.
pub struct ConnectionGuard {
    metrics: Arc<Metrics>,
    transport: &'static str,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.metrics.dec_active_connections(self.transport);
    }
}

/// One series as the manifest describes it: name, type, label keys, description.
pub(crate) type SeriesSpec = (
    &'static str,
    MetricType,
    &'static [&'static str],
    &'static str,
);

/// Every series the receiver records under its own name.
const RECEIVER_SERIES: &[SeriesSpec] = &[
    (
        "receiver_requests_total",
        MetricType::Counter,
        &["transport"],
        "Requests received, by transport; records_received_total counts the records they carry",
    ),
    (
        "receiver_requests_success_total",
        MetricType::Counter,
        &["transport"],
        "Total successful requests by transport",
    ),
    (
        "receiver_requests_error_total",
        MetricType::Counter,
        &["transport"],
        "Total failed requests by transport",
    ),
    (
        "receiver_parse_failures_total",
        MetricType::Counter,
        &["transport"],
        "Per-protocol ingress parse/decode failures by transport",
    ),
    (
        "receiver_bytes_received_total",
        MetricType::Counter,
        &["transport"],
        "Total bytes received by transport",
    ),
    (
        "receiver_records_dropped_total",
        MetricType::Counter,
        &["transport", "reason"],
        "Records dropped with no way to tell the sender, by transport and reason",
    ),
    (
        "receiver_request_duration_seconds",
        MetricType::Histogram,
        &["transport"],
        "End-to-end request processing latency",
    ),
    (
        "receiver_active_connections",
        MetricType::Gauge,
        &["transport"],
        "Currently active inbound connections",
    ),
    (
        "receiver_auth_failures_total",
        MetricType::Counter,
        &["reason"],
        "Authentication failures by reason",
    ),
    (
        "receiver_validation_failures_total",
        MetricType::Counter,
        &["reason"],
        "Validation failures by reason",
    ),
    (
        "receiver_request_timeouts_total",
        MetricType::Counter,
        &["transport"],
        "Requests answered 408 for taking longer than the request timeout, by transport (slow loris indicator)",
    ),
    (
        "receiver_body_size_rejected_total",
        MetricType::Counter,
        &["transport"],
        "Requests refused for their size, by transport (HTTP 413, gRPC OUT_OF_RANGE or RESOURCE_EXHAUSTED)",
    ),
    (
        "receiver_ip_filter_rejected_total",
        MetricType::Counter,
        &["transport"],
        "Connections, and syslog UDP datagrams, the IP filter refused, by transport",
    ),
    (
        "receiver_rate_limited_total",
        MetricType::Counter,
        &["transport"],
        "Requests the per-IP rate limit (server.rate_limit) refused with 429, by transport",
    ),
    (
        "receiver_tls_handshake_failures_total",
        MetricType::Counter,
        &[],
        "TLS handshake failures",
    ),
    (
        "receiver_records_rejected_total",
        MetricType::Counter,
        &["outcome"],
        "Records a destination refused for good, by outcome (dead_lettered, dropped or dlq_refused)",
    ),
    (
        "receiver_messages_spilled_total",
        MetricType::Counter,
        &[],
        "Records a destination's buffer held back from its sink, in memory or in the disk spool",
    ),
    (
        "receiver_messages_drained_total",
        MetricType::Counter,
        &[],
        "Held-back records that have since left the buffer for the sink",
    ),
    (
        "receiver_kafka_send_duration_seconds",
        MetricType::Histogram,
        &[],
        "Time librdkafka took to queue a record",
    ),
    (
        "receiver_kafka_sends_total",
        MetricType::Counter,
        &[],
        "Records librdkafka queued",
    ),
    (
        "receiver_kafka_bytes_sent_total",
        MetricType::Counter,
        &[],
        "Bytes queued to Kafka",
    ),
    (
        "receiver_kafka_send_errors_total",
        MetricType::Counter,
        &[],
        "Records librdkafka refused to queue",
    ),
    (
        "receiver_kafka_delivered_total",
        MetricType::Counter,
        &[],
        "Records a broker acknowledged, from librdkafka delivery reports",
    ),
    (
        "receiver_kafka_delivery_failures_total",
        MetricType::Counter,
        &["reason"],
        "Records no broker confirmed, by librdkafka error code; a timed-out record may still have been written",
    ),
    (
        "receiver_dlq_start_failures_total",
        MetricType::Counter,
        &["backend"],
        "DLQ starts that failed, by the backend that refused (kafka, file or other); the receiver runs on without a DLQ",
    ),
    (
        "receiver_events_per_second",
        MetricType::Gauge,
        &[],
        "Requests per second over the last minute; a request carries one record or more",
    ),
    (
        crate::sink::grpc::SEND_FAILURES_TOTAL,
        MetricType::Counter,
        &["reason"],
        "Records a gRPC destination did not take, by reason (unavailable: down, refusing \
         connections, busy or past its deadline; failed: it answered with an error)",
    ),
];

/// Register every series in `series` on `manager` under `group`, so the
/// manifest lists it with its type and labels.
pub(crate) fn register_series(manager: &MetricsManager, series: &[SeriesSpec], group: &str) {
    for &(name, kind, labels, description) in series {
        match kind {
            MetricType::Counter => {
                let _ = manager.counter_with_labels(name, description, labels, group);
            }
            MetricType::Gauge => {
                let _ = manager.gauge_with_labels(name, description, labels, group);
            }
            MetricType::Histogram => {
                let _ = manager.histogram_with_labels(name, description, labels, group, None);
            }
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::with_scaling(Arc::new(
            crate::config::ScalingConfig::default().build_pressure(),
        ))
    }
}

/// Test doubles that read what the receiver recorded: counter totals and log lines.
#[cfg(test)]
pub(crate) mod testing {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use parking_lot::Mutex;

    /// Every line a test subscriber writes, for `tracing_subscriber::fmt().with_writer`.
    #[derive(Clone, Default)]
    pub(crate) struct LogLines(Arc<Mutex<Vec<u8>>>);

    impl LogLines {
        /// Everything written so far.
        pub(crate) fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock()).into_owned()
        }
    }

    impl std::io::Write for LogLines {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogLines {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Sums each counter across its label sets, as a `sum()` over the name reads it.
    ///
    /// Install it with `metrics::set_default_local_recorder` before the metrics
    /// are built: a metric group binds its handles when it is built.
    #[derive(Default)]
    pub(crate) struct CounterTotals {
        counters: Mutex<HashMap<String, Arc<AtomicU64>>>,
    }

    impl CounterTotals {
        /// What counter `name` holds, 0 when nothing recorded it.
        pub(crate) fn total(&self, name: &str) -> u64 {
            self.counters
                .lock()
                .get(name)
                .map_or(0, |cell| cell.load(Ordering::Relaxed))
        }
    }

    impl metrics::Recorder for CounterTotals {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            let cell = Arc::clone(
                self.counters
                    .lock()
                    .entry(key.name().to_string())
                    .or_default(),
            );
            metrics::Counter::from_arc(cell)
        }

        fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::CounterTotals;
    use super::*;

    #[test]
    fn the_destination_send_failure_counter_is_in_the_manifest() {
        let manager = MetricsManager::with_config(scalo::metrics::MetricsConfig::offline(""));
        let _ = Metrics::register_on(
            Arc::new(crate::config::ScalingConfig::default().build_pressure()),
            &manager,
        );

        let manifest = manager.registry().manifest();
        let entry = manifest
            .metrics
            .iter()
            .find(|m| m.name.ends_with(crate::sink::grpc::SEND_FAILURES_TOTAL))
            .expect("the gRPC destination send-failure counter is in the manifest");
        assert_eq!(entry.labels, vec!["reason".to_string()]);
    }

    /// Every Rust source file under `dir`, read whole.
    fn rust_sources(dir: &std::path::Path) -> Vec<String> {
        let mut sources = Vec::new();
        for entry in std::fs::read_dir(dir).expect("read the source tree") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                sources.extend(rust_sources(&path));
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                sources.push(std::fs::read_to_string(&path).expect("read a source file"));
            }
        }
        sources
    }

    /// The names every `counter!`, `gauge!` and `histogram!` under `src/`
    /// records, and the first argument of each that names its metric through
    /// a binding rather than a string literal.
    fn recorded_names() -> (
        std::collections::BTreeSet<String>,
        std::collections::BTreeSet<String>,
    ) {
        let (mut literal, mut bound) = (
            std::collections::BTreeSet::new(),
            std::collections::BTreeSet::new(),
        );
        let src = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
        // Built at run time, so this scan does not find its own patterns.
        let opens = ["counter", "gauge", "histogram"].map(|kind| format!("{kind}!("));
        for source in rust_sources(src) {
            for open in &opens {
                let mut rest = source.as_str();
                while let Some(at) = rest.find(open.as_str()) {
                    let described = rest[..at].ends_with("describe_");
                    rest = rest[at + open.len()..].trim_start();
                    if described {
                        continue;
                    }
                    if let Some(quoted) = rest.strip_prefix('"') {
                        let end = quoted.find('"').expect("a closed string literal");
                        literal.insert(quoted[..end].to_string());
                    } else {
                        let end = rest.find([',', ')']).unwrap_or(rest.len());
                        bound.insert(rest[..end].trim().to_string());
                    }
                }
            }
        }
        (literal, bound)
    }

    /// The names the flow handler's adapters are built with.
    fn flow_adapter_names() -> Vec<String> {
        let source = include_str!("../server/flow/metrics.rs");
        source
            .split("name: \"")
            .skip(1)
            .filter_map(|rest| rest.split('"').next())
            .map(ToString::to_string)
            .collect()
    }

    /// Every metric the receiver records is in the manifest `metrics-manifest`
    /// prints, so no series reaches Prometheus undescribed.
    #[test]
    fn every_metric_the_receiver_records_is_in_the_manifest() {
        let manager = MetricsManager::with_config(scalo::metrics::MetricsConfig::offline(""));
        let _metrics = Metrics::register_on(
            Arc::new(crate::config::ScalingConfig::default().build_pressure()),
            &manager,
        );
        let manifest: std::collections::BTreeSet<String> = manager
            .registry()
            .manifest()
            .metrics
            .into_iter()
            .map(|descriptor| descriptor.name)
            .collect();

        let (mut recorded, bound) = recorded_names();
        assert_eq!(
            bound,
            ["SEND_FAILURES_TOTAL", "self.name"]
                .map(ToString::to_string)
                .into(),
            "a metric named through a new binding needs a place in this test"
        );
        recorded.insert(crate::sink::grpc::SEND_FAILURES_TOTAL.to_string());
        recorded.extend(flow_adapter_names());
        assert!(recorded.len() > 30, "the scan found {recorded:?}");

        let missing: Vec<&String> = recorded
            .iter()
            .filter(|name| !manifest.contains(*name))
            .collect();
        assert!(
            missing.is_empty(),
            "recorded but not in the manifest: {missing:?}"
        );
    }

    /// A request is not a record: the requests counters leave every
    /// `records_*` series alone.
    #[test]
    fn requests_leave_the_record_counters_alone() {
        let manager = MetricsManager::with_config(scalo::metrics::MetricsConfig::offline(""));
        let totals = CounterTotals::default();
        metrics::with_local_recorder(&totals, || {
            let metrics = Metrics::register_on(
                Arc::new(crate::config::ScalingConfig::default().build_pressure()),
                &manager,
            );
            metrics.inc_requests_total("http");
            metrics.inc_requests_success("http");
            metrics.inc_requests_error("http");
        });

        assert_eq!(totals.total("receiver_requests_total"), 1);
        for name in [
            "records_received_total",
            "records_processed_total",
            "records_delivered_total",
            "records_error_total",
        ] {
            assert_eq!(totals.total(name), 0, "{name}");
        }
    }

    /// Records count one each, received and then taken or refused.
    #[test]
    fn records_count_one_each() {
        let manager = MetricsManager::with_config(scalo::metrics::MetricsConfig::offline(""));
        let totals = CounterTotals::default();
        let metrics = metrics::with_local_recorder(&totals, || {
            let metrics = Metrics::register_on(
                Arc::new(crate::config::ScalingConfig::default().build_pressure()),
                &manager,
            );
            metrics.add_records_received(5);
            metrics.add_records_settled(3, 1);
            metrics
        });

        assert_eq!(totals.total("records_received_total"), 5);
        assert_eq!(totals.total("records_processed_total"), 3);
        assert_eq!(totals.total("records_delivered_total"), 3);
        assert_eq!(totals.total("records_error_total"), 1);
        assert_eq!(metrics.get_records_received(), 5);
        assert_eq!(metrics.get_records_taken(), 3);
        assert_eq!(metrics.get_records_refused(), 1);
    }

    /// The buffers' running totals reach their counters, and a lower reading
    /// never takes a counter backwards.
    #[test]
    fn buffered_totals_reach_their_counters() {
        let metrics = Metrics::default();
        let totals = CounterTotals::default();
        metrics::with_local_recorder(&totals, || {
            metrics.set_buffered_totals(7, 4);
            metrics.set_buffered_totals(6, 3);
        });

        assert_eq!(totals.total("receiver_messages_spilled_total"), 7);
        assert_eq!(totals.total("receiver_messages_drained_total"), 4);
    }

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

    /// A connection counts from its guard's creation to its drop, and a stray
    /// decrement never wraps the count.
    #[test]
    fn a_connection_counts_until_its_guard_drops() {
        let metrics = Arc::new(Metrics::default());
        let first = metrics.open_connection("http");
        let second = metrics.open_connection("grpc");
        assert_eq!(metrics.get_active_connections(), 2);

        drop(first);
        assert_eq!(metrics.get_active_connections(), 1);
        drop(second);
        assert_eq!(metrics.get_active_connections(), 0);

        metrics.dec_active_connections("http");
        assert_eq!(metrics.get_active_connections(), 0);
    }

    /// Open connections feed the scaling score's `connections` component,
    /// which reaches its full weight at `saturation_connections`.
    #[test]
    fn open_connections_feed_the_connections_component() {
        let config = crate::config::ScalingConfig::default();
        let metrics = Arc::new(Metrics::default());
        metrics.update_scaling();
        assert!(
            metrics.scaling_pressure().abs() < 1e-9,
            "an idle receiver scores 0"
        );

        let open: Vec<ConnectionGuard> = (0..config.saturation_connections as usize)
            .map(|_| metrics.open_connection("http"))
            .collect();
        metrics.update_scaling();
        let expected = config.weight_connections * 100.0;
        assert!(
            (metrics.scaling_pressure() - expected).abs() < 1e-9,
            "a saturated connections component scores its weight: {}",
            metrics.scaling_pressure()
        );

        drop(open);
        metrics.update_scaling();
        assert!(metrics.scaling_pressure().abs() < 1e-9);
    }

    /// The timeout and size counters carry the transport they happened on.
    #[test]
    fn refusals_count_under_their_transport() {
        let metrics = Metrics::default();
        let totals = CounterTotals::default();
        metrics::with_local_recorder(&totals, || {
            metrics.inc_request_timeout("otlp");
            metrics.inc_body_size_rejected("splunk_hec");
            metrics.inc_body_size_rejected("grpc");
        });

        assert_eq!(metrics.get_request_timeouts_total(), 1);
        assert_eq!(metrics.get_body_size_rejected_total(), 2);
        assert_eq!(totals.total("receiver_request_timeouts_total"), 1);
        assert_eq!(totals.total("receiver_body_size_rejected_total"), 2);
    }

    #[test]
    fn dropped_records_sum_across_transports_and_reasons() {
        let metrics = Metrics::default();

        metrics.add_records_dropped("syslog", DropReason::Unavailable, 2);
        metrics.add_records_dropped("fluent", DropReason::Rejected, 1);
        metrics.add_records_dropped("gelf", DropReason::Shutdown, 0);

        assert_eq!(metrics.get_records_dropped(), 3);
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
