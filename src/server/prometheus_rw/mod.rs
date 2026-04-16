// Project:   dfe-receiver
// File:      src/server/prometheus_rw/mod.rs
// Purpose:   Prometheus Remote Write v1 protocol handler
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus Remote Write v1 protocol handler.
//!
//! Accepts `POST /api/v1/write` with Snappy-compressed protobuf payload
//! per the [Remote Write specification](https://prometheus.io/docs/specs/prw/remote_write_spec/).
//!
//! Wire format: HTTP POST → Snappy block decompress → protobuf decode → JSON → pipeline.

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
use tracing::info;

use crate::config::PrometheusRwConfig;
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::http::create_auth_state;
use crate::server::tls::{TlsCertProvider, build_tls_acceptor, uses_secrets};
use crate::server::traits::ProtocolHandler;

use self::convert::{PrometheusRwMode, write_request_to_json};

/// Prometheus Remote Write protocol handler.
pub struct PrometheusRwHandler {
    config: PrometheusRwConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
}

impl PrometheusRwHandler {
    pub fn new(
        config: PrometheusRwConfig,
        pipeline: Arc<PipelineState>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            config,
            pipeline,
            metrics,
        }
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

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        run_prometheus_rw_server(
            &self.config,
            self.pipeline.clone(),
            self.metrics.clone(),
            shutdown,
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
}

// ---------------------------------------------------------------------------
// Server setup
// ---------------------------------------------------------------------------

/// Run the Prometheus Remote Write HTTP server.
async fn run_prometheus_rw_server(
    config: &PrometheusRwConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<()> {
    let auth_state = create_auth_state(&config.auth).await?;
    let mode = PrometheusRwMode::from_str(&config.mode);

    let state = RwState {
        pipeline,
        metrics: metrics.clone(),
        mode,
    };

    let max_body_size = config.max_body_size;
    let request_timeout = Duration::from_millis(config.request_timeout_ms);

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

    let ip_filter = crate::server::ip_filter::IpFilter::disabled();

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
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .map_err(|e| Error::Server(format!("Prometheus RW server error: {e}")))?;
        info!("Prometheus Remote Write server stopped");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Route handler
// ---------------------------------------------------------------------------

/// `POST /api/v1/write` — Prometheus Remote Write v1.
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
            state.metrics.inc_requests_error("prometheus_rw");
            RwError::bad_request(&format!("snappy decompression failed: {e}"))
        })?;

    // Protobuf decode
    let request = proto::WriteRequest::decode(decompressed.as_slice()).map_err(|e| {
        state.metrics.inc_requests_error("prometheus_rw");
        RwError::bad_request(&format!("protobuf decode failed: {e}"))
    })?;

    // Convert to JSON events
    let events = write_request_to_json(request, state.mode).map_err(|e| {
        state.metrics.inc_requests_error("prometheus_rw");
        RwError::internal(&e.to_string())
    })?;

    // Batch-process all events through the pipeline
    let (success, first_err) = state.pipeline.process_batch(&events).await;
    if let Some(ref e) = first_err
        && success == 0
    {
        state.metrics.inc_requests_error("prometheus_rw");
        return if e.to_string().contains("pressure") {
            Err(RwError::service_unavailable("backpressure"))
        } else {
            Err(RwError::internal(&e.to_string()))
        };
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

    fn internal(msg: &str) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.to_string(),
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
}
