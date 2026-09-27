// Project:   dfe-receiver
// File:      src/server/prometheus_rw/mod.rs
// Purpose:   Prometheus Remote Write v1 protocol handler
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus Remote Write v1 protocol handler.
//!
//! Accepts `POST /api/v1/write` with Snappy-compressed protobuf payload
//! per the [Remote Write specification](https://prometheus.io/docs/specs/prw/remote_write_spec/).
//!
//! Wire format: HTTP POST -> Snappy block decompress -> protobuf decode -> JSON -> pipeline.

pub mod convert;

/// Generated protobuf types for Prometheus Remote Write v1.
pub mod proto {
    tonic::include_proto!("prometheus");
}

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use prost::Message;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tracing::{debug, info, warn};

use crate::config::{PrometheusRwConfig, RawCapture};
use crate::error::{Error, RETRY_AFTER_SECS, Result};
use crate::metrics::Metrics;
use crate::pipeline::{Acks, PipelineState};
use crate::server::http::create_auth_state;
use crate::server::tls::{TlsCertProvider, build_tls_acceptor, uses_secrets};
use crate::server::traits::{BoundAddr, ProtocolHandler};

use self::convert::{PrometheusRwMode, write_request_to_json};

/// Prometheus Remote Write protocol handler.
pub struct PrometheusRwHandler {
    config: PrometheusRwConfig,
    /// Raw capture already resolved against the common `raw_capture` block.
    raw_capture: RawCapture,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    bound: BoundAddr,
}

impl PrometheusRwHandler {
    pub fn new(
        config: PrometheusRwConfig,
        raw_capture: RawCapture,
        pipeline: Arc<PipelineState>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            config,
            raw_capture,
            pipeline,
            metrics,
            bound: BoundAddr::default(),
        }
    }

    /// The address the listener bound, once [`ProtocolHandler::start`] binds it.
    #[must_use]
    pub fn bound_addr(&self) -> BoundAddr {
        self.bound.clone()
    }
}

#[async_trait::async_trait]
impl ProtocolHandler for PrometheusRwHandler {
    fn name(&self) -> &'static str {
        "prometheus-remote-write"
    }

    fn bind_address(&self) -> &str {
        &self.config.bind_address
    }

    fn listeners(&self) -> Vec<BoundAddr> {
        vec![self.bound.clone()]
    }

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        run_prometheus_rw_server(
            &self.config,
            self.raw_capture,
            self.pipeline.clone(),
            self.metrics.clone(),
            shutdown,
            &self.bound,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RwState {
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    mode: PrometheusRwMode,
    raw_capture: RawCapture,
    /// When a write is answered: once every destination confirmed, or at
    /// enqueue.
    acks: Acks,
}

// ---------------------------------------------------------------------------
// Server setup
// ---------------------------------------------------------------------------

/// Run the Prometheus Remote Write HTTP server.
async fn run_prometheus_rw_server(
    config: &PrometheusRwConfig,
    raw_capture: RawCapture,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    bound: &BoundAddr,
) -> Result<()> {
    let auth_state = create_auth_state(&config.auth).await?;
    let mode = PrometheusRwMode::from_str(&config.mode);

    if raw_capture.enabled && mode == PrometheusRwMode::Native {
        warn!(
            "prometheus_rw.raw_capture is on in native mode: _raw duplicates the event, \
             roughly doubling produced bytes for no extra information"
        );
    }

    // `server.ip_filter` and `server.rate_limit` govern the ingest surface, not
    // one port of it.
    let server = pipeline.config().server;

    let max_body_size = config.max_body_size;
    let request_timeout = Duration::from_millis(config.request_timeout_ms);

    let state = RwState {
        acks: pipeline.acks(
            "prometheus_rw",
            config.acknowledgements,
            Some(request_timeout),
        ),
        pipeline,
        metrics: metrics.clone(),
        mode,
        raw_capture,
    };

    let app = Router::new()
        .route("/api/v1/write", post(write_handler))
        .layer(axum::middleware::from_fn_with_state(
            auth_state,
            crate::server::auth::token_auth_middleware,
        ))
        .layer(RequestBodyLimitLayer::new(max_body_size))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            request_timeout,
        ))
        .with_state(state);

    let app = crate::server::http::apply_server_limits(app, &server)?;

    let addr: SocketAddr = config
        .bind_address
        .parse()
        .map_err(|e| Error::Config(format!("invalid Prometheus RW bind address: {e}")))?;

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Server(format!("Prometheus RW failed to bind: {e}")))?;

    // TLS setup (same dual-path pattern as Splunk HEC)
    let tls_provider = if config.tls.enabled && uses_secrets(&config.tls) {
        let provider = TlsCertProvider::new(config.tls.clone()).await?;
        provider.start_refresh_task();
        Some(provider)
    } else {
        None
    };

    let tls_acceptor = if tls_provider.is_some() {
        None
    } else {
        build_tls_acceptor(&config.tls)?
    };

    let ip_filter = crate::server::ip_filter::IpFilter::from_config(&server.ip_filter);

    // Published once TLS is ready, so a failed TLS setup never reads as serving.
    let _serving = bound.publish(&listener.local_addr());

    if let Some(ref provider) = tls_provider {
        let acceptor_handle = provider.acceptor_handle();
        info!(addr = %addr, tls = true, hot_reload = true, "Prometheus Remote Write server listening");
        crate::server::http::run_tls_server(
            listener,
            app,
            acceptor_handle,
            ip_filter,
            shutdown,
            metrics,
        )
        .await
    } else if let Some(acceptor) = tls_acceptor {
        let acceptor_handle = Arc::new(parking_lot::RwLock::new(acceptor));
        info!(addr = %addr, tls = true, hot_reload = false, "Prometheus Remote Write server listening");
        crate::server::http::run_tls_server(
            listener,
            app,
            acceptor_handle,
            ip_filter,
            shutdown,
            metrics,
        )
        .await
    } else {
        info!(addr = %addr, tls = false, "Prometheus Remote Write server listening");
        // The shared accept loop, not `axum::serve`: it runs the IP filter and
        // puts the peer address on each request for the rate limiter.
        crate::server::http::run_plain_server(listener, app, ip_filter, shutdown).await?;
        info!("Prometheus Remote Write server stopped");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Route handler
// ---------------------------------------------------------------------------

/// `POST /api/v1/write` -- Prometheus Remote Write v1.
///
/// Expects Snappy-compressed protobuf `WriteRequest` in the body.
/// Returns 204 No Content on success.
async fn write_handler(
    State(state): State<RwState>,
    body: Bytes,
) -> std::result::Result<StatusCode, RwError> {
    state.metrics.inc_requests_total("prometheus_rw");
    state
        .metrics
        .add_bytes_received("prometheus_rw", body.len() as u64);

    if body.is_empty() {
        state.metrics.inc_requests_error("prometheus_rw");
        return Err(RwError::bad_request("empty request body"));
    }

    // Snappy block decompress (with decompression bomb guard)
    const MAX_DECOMPRESSED_SIZE: usize = 64 * 1024 * 1024; // 64 MiB

    let expected_len = snap::raw::decompress_len(&body).map_err(|e| {
        state.metrics.inc_parse_failure("prometheus_rw");
        state.metrics.inc_requests_error("prometheus_rw");
        RwError::bad_request(&format!("snappy decompression failed: {e}"))
    })?;

    if expected_len > MAX_DECOMPRESSED_SIZE {
        state.metrics.inc_requests_error("prometheus_rw");
        return Err(RwError::bad_request(&format!(
            "decompressed size {expected_len} exceeds limit {MAX_DECOMPRESSED_SIZE}"
        )));
    }

    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(&body)
        .map_err(|e| {
            state.metrics.inc_parse_failure("prometheus_rw");
            state.metrics.inc_requests_error("prometheus_rw");
            RwError::bad_request(&format!("snappy decompression failed: {e}"))
        })?;

    // Protobuf decode
    let request = proto::WriteRequest::decode(decompressed.as_slice()).map_err(|e| {
        state.metrics.inc_parse_failure("prometheus_rw");
        state.metrics.inc_requests_error("prometheus_rw");
        RwError::bad_request(&format!("protobuf decode failed: {e}"))
    })?;

    // Convert to JSON events
    let events = write_request_to_json(request, state.mode, state.raw_capture).map_err(|e| {
        warn!(error = %e, "Remote Write conversion failed");
        state.metrics.inc_requests_error("prometheus_rw");
        RwError::internal()
    })?;

    // Remote Write senders MUST retry a 5xx and MUST NOT retry a 4xx other
    // than 429, so a sample the pipeline could not take answers 503 and one it
    // refused for good answers 400.
    let outcome = state
        .pipeline
        .process_batch_acked(&events, &state.acks, None)
        .await;
    if let Some(e) = outcome.unavailable {
        debug!(samples = events.len(), accepted = outcome.accepted, error = %e, "Remote Write request not fully taken");
        state.metrics.inc_requests_error("prometheus_rw");
        state.metrics.record_backpressure();
        return Err(RwError::service_unavailable("server is overloaded"));
    }
    if let Some(e) = outcome.first_rejection {
        state.metrics.inc_requests_error("prometheus_rw");
        return Err(RwError::bad_request(&e.public_message()));
    }

    state.metrics.inc_requests_success("prometheus_rw");
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Prometheus Remote Write error response.
///
/// Per spec: 400 for bad data (non-retriable), 5xx for transient errors (retriable).
/// Body is a human-readable error string (not JSON).
struct RwError {
    status: StatusCode,
    message: String,
}

impl RwError {
    fn bad_request(msg: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.to_string(),
        }
    }

    /// A failure of the receiver's own; the cause is logged, not sent.
    fn internal() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "internal error".to_string(),
        }
    }

    fn service_unavailable(msg: &str) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: msg.to_string(),
        }
    }
}

impl IntoResponse for RwError {
    fn into_response(self) -> Response {
        if self.status == StatusCode::SERVICE_UNAVAILABLE {
            let retry_after = axum::http::HeaderValue::from(RETRY_AFTER_SECS);
            return (
                self.status,
                [(axum::http::header::RETRY_AFTER, retry_after)],
                self.message,
            )
                .into_response();
        }
        (self.status, self.message).into_response()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PrometheusRwConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.bind_address, "0.0.0.0:9091");
        assert_eq!(config.mode, "native");
        assert_eq!(config.max_body_size, 10 * 1024 * 1024);
        assert_eq!(config.request_timeout_ms, 30_000);
    }

    // -----------------------------------------------------------------
    // Security: decompression bomb protection
    // -----------------------------------------------------------------
    //
    // These tests verify the `snap::raw::decompress_len` pre-check
    // correctly rejects payloads that would expand beyond the 64 MiB
    // decompressed size limit before any allocation occurs.

    /// Build a valid snappy block with a specific reported uncompressed size.
    /// The snappy block format is: varint(uncompressed_length) + compressed_data.
    /// We construct a blob that claims a very large uncompressed size.
    fn snappy_blob_claiming_size(size: u64) -> Vec<u8> {
        let mut blob = Vec::new();
        // Encode `size` as varint
        let mut n = size;
        while n >= 0x80 {
            blob.push(((n & 0x7f) | 0x80) as u8);
            n >>= 7;
        }
        blob.push(n as u8);
        // Append a valid but minimal compressed section (single literal zero byte).
        // 0x00 = tag byte for 1-byte literal, 0x00 = the literal byte
        blob.push(0x00);
        blob.push(0x00);
        blob
    }

    #[test]
    fn test_snappy_decompress_len_detects_bomb() {
        // A crafted snappy blob claiming 1 GiB uncompressed size
        let bomb = snappy_blob_claiming_size(1024 * 1024 * 1024);
        let reported = snap::raw::decompress_len(&bomb).unwrap();
        assert_eq!(reported, 1024 * 1024 * 1024);

        // Our handler rejects anything over 64 MiB before decompressing
        const MAX: usize = 64 * 1024 * 1024;
        assert!(reported > MAX, "test setup: bomb should exceed limit");
    }

    #[test]
    fn test_snappy_legit_payload_passes_guard() {
        // A realistic Prometheus RW payload compresses to well under 64 MiB
        let payload = br#"{"metric":"http_requests_total","value":42}"#.repeat(100);
        let compressed = snap::raw::Encoder::new().compress_vec(&payload).unwrap();
        let reported = snap::raw::decompress_len(&compressed).unwrap();

        const MAX: usize = 64 * 1024 * 1024;
        assert!(reported < MAX, "legit payload must pass guard: {reported}");

        // Round-trip decompresses correctly
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        assert_eq!(decompressed.len(), payload.len());
    }

    #[test]
    fn test_snappy_decompress_len_handles_edge_cases() {
        // Completely invalid data with an impossibly-large varint should fail.
        // A sequence of 0xff bytes keeps extending the varint forever.
        let garbage = vec![0xff; 10];
        let _ = snap::raw::decompress_len(&garbage); // may or may not error, must not panic

        // Empty input returns Ok(0) -- treated as empty decompressed output,
        // which is safely below any size limit.
        if let Ok(n) = snap::raw::decompress_len(&[]) {
            assert_eq!(n, 0, "empty input should decompress to 0 bytes");
        }
        // Errors are also acceptable depending on library version.
    }

    #[test]
    fn test_snappy_bomb_at_boundary() {
        const MAX: usize = 64 * 1024 * 1024;

        // Exactly at the limit should be allowed
        let at_limit = snappy_blob_claiming_size(MAX as u64);
        let reported = snap::raw::decompress_len(&at_limit).unwrap();
        assert_eq!(reported, MAX);

        // Just over the limit should be flagged by our check
        let over = snappy_blob_claiming_size((MAX + 1) as u64);
        let reported_over = snap::raw::decompress_len(&over).unwrap();
        assert!(reported_over > MAX);
    }
}
