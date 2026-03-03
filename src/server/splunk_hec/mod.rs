// Project:   dfe-receiver
// File:      src/server/splunk_hec/mod.rs
// Purpose:   Splunk HEC (HTTP Event Collector) protocol handler
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Splunk HEC protocol handler.
//!
//! Implements a Splunk HEC-compatible HTTP server that accepts events via:
//! - `POST /services/collector/event` — JSON events with metadata
//! - `POST /services/collector/raw` — Raw text events
//! - `GET  /services/collector/health` — Health check
//!
//! Supports `Authorization: Splunk <token>` and `Authorization: Bearer <token>`.

pub mod convert;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tracing::info;

use crate::config::SplunkHecConfig;
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::http::create_auth_state;
use crate::server::tls::{build_tls_acceptor, uses_secrets, TlsCertProvider};
use crate::server::traits::ProtocolHandler;

use self::convert::{hec_event_to_json, parse_hec_events, raw_to_json, RawMetadata};

/// Splunk HEC protocol handler.
pub struct SplunkHecHandler {
    config: SplunkHecConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
}

impl SplunkHecHandler {
    /// Create a new Splunk HEC handler.
    pub fn new(
        config: SplunkHecConfig,
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
impl ProtocolHandler for SplunkHecHandler {
    fn name(&self) -> &'static str {
        "splunk-hec"
    }

    fn bind_address(&self) -> &str {
        &self.config.bind_address
    }

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        run_hec_server(
            &self.config,
            self.pipeline.clone(),
            self.metrics.clone(),
            shutdown,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// HEC response types
// ---------------------------------------------------------------------------

/// Standard HEC JSON response.
#[derive(Debug, Serialize)]
struct HecResponse {
    text: String,
    code: i32,
}

impl HecResponse {
    fn success() -> Self {
        Self {
            text: "Success".into(),
            code: 0,
        }
    }
}

/// HEC error with HTTP status code and HEC-format JSON body.
struct HecError {
    status: StatusCode,
    response: HecResponse,
}

impl HecError {
    fn no_data() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            response: HecResponse {
                text: "No data".into(),
                code: 5,
            },
        }
    }

    fn invalid_data(detail: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            response: HecResponse {
                text: format!("Invalid data format: {detail}"),
                code: 6,
            },
        }
    }

    fn event_required() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            response: HecResponse {
                text: "Event field is required".into(),
                code: 12,
            },
        }
    }

    fn internal(detail: &str) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            response: HecResponse {
                text: format!("Internal server error: {detail}"),
                code: 8,
            },
        }
    }

    fn server_busy() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            response: HecResponse {
                text: "Server is busy".into(),
                code: 9,
            },
        }
    }
}

impl IntoResponse for HecError {
    fn into_response(self) -> Response {
        (self.status, Json(self.response)).into_response()
    }
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct HecState {
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
}

// ---------------------------------------------------------------------------
// Server setup
// ---------------------------------------------------------------------------

/// Run the Splunk HEC HTTP server.
async fn run_hec_server(
    config: &SplunkHecConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<()> {
    // Create auth state (reuse HTTP handler's bearer token loading)
    let auth_state = create_auth_state(&config.auth).await?;

    let state = HecState {
        pipeline,
        metrics: metrics.clone(),
    };

    // Security configuration
    let max_body_size = config.max_body_size;
    let request_timeout = Duration::from_millis(config.request_timeout_ms);

    // Build router with HEC endpoints
    let app = Router::new()
        .route("/services/collector/event", post(event_handler))
        .route("/services/collector/event/1.0", post(event_handler))
        .route("/services/collector/raw", post(raw_handler))
        .route("/services/collector/raw/1.0", post(raw_handler))
        .route("/services/collector/health", get(health_handler))
        .route("/services/collector/health/1.0", get(health_handler))
        // Auth middleware — validates Splunk/Bearer tokens via existing auth system
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
        .map_err(|e| Error::Config(format!("invalid HEC bind address: {e}")))?;

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Server(format!("HEC failed to bind: {e}")))?;

    // TLS setup (same pattern as HTTP handler)
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

    if let Some(ref provider) = tls_provider {
        // Hot-reloadable TLS via TlsCertProvider
        let acceptor_handle = provider.acceptor_handle();
        info!(addr = %addr, tls = true, hot_reload = true, "Splunk HEC server listening");
        crate::server::http::run_tls_server(listener, app, acceptor_handle, shutdown, metrics).await
    } else if let Some(acceptor) = tls_acceptor {
        // Static TLS (no secrets, no hot-reload)
        let acceptor_handle = Arc::new(parking_lot::RwLock::new(acceptor));
        info!(addr = %addr, tls = true, hot_reload = false, "Splunk HEC server listening");
        crate::server::http::run_tls_server(listener, app, acceptor_handle, shutdown, metrics).await
    } else {
        info!(addr = %addr, tls = false, "Splunk HEC server listening");
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .map_err(|e| Error::Server(format!("HEC server error: {e}")))?;
        info!("Splunk HEC server stopped");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

/// `POST /services/collector/event` — JSON events with metadata.
#[inline]
async fn event_handler(
    State(state): State<HecState>,
    body: Bytes,
) -> std::result::Result<Json<HecResponse>, HecError> {
    state.metrics.inc_requests_total();
    state.metrics.add_bytes_received(body.len() as u64);

    if body.is_empty() {
        state.metrics.inc_requests_error();
        return Err(HecError::no_data());
    }

    let events = parse_hec_events(&body).map_err(|e| {
        state.metrics.inc_requests_error();
        let msg = e.to_string();
        if msg.contains("blank") {
            HecError::event_required()
        } else if msg.contains("no data") {
            HecError::no_data()
        } else {
            HecError::invalid_data(&msg)
        }
    })?;

    for event in events {
        let json = hec_event_to_json(event).map_err(|e| {
            state.metrics.inc_requests_error();
            HecError::invalid_data(&e.to_string())
        })?;

        state.pipeline.process(json).await.map_err(|e| {
            state.metrics.inc_requests_error();
            if e.to_string().contains("pressure") {
                HecError::server_busy()
            } else {
                HecError::internal(&e.to_string())
            }
        })?;
    }

    state.metrics.inc_requests_success();
    Ok(Json(HecResponse::success()))
}

/// `POST /services/collector/raw` — Raw text events.
#[inline]
async fn raw_handler(
    State(state): State<HecState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> std::result::Result<Json<HecResponse>, HecError> {
    state.metrics.inc_requests_total();
    state.metrics.add_bytes_received(body.len() as u64);

    if body.is_empty() {
        state.metrics.inc_requests_error();
        return Err(HecError::no_data());
    }

    // Extract metadata from query parameters, falling back to headers
    let metadata = RawMetadata {
        host: params
            .get("host")
            .cloned()
            .or_else(|| header_str(&headers, "x-splunk-request-host")),
        source: params
            .get("source")
            .cloned()
            .or_else(|| header_str(&headers, "x-splunk-request-source")),
        sourcetype: params
            .get("sourcetype")
            .cloned()
            .or_else(|| header_str(&headers, "x-splunk-request-sourcetype")),
        index: params
            .get("index")
            .cloned()
            .or_else(|| header_str(&headers, "x-splunk-request-index")),
    };

    // Split raw body by newlines and process each line
    for line in body.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }

        let json = raw_to_json(line, &metadata).map_err(|e| {
            state.metrics.inc_requests_error();
            HecError::internal(&e.to_string())
        })?;
        state.pipeline.process(json).await.map_err(|e| {
            state.metrics.inc_requests_error();
            if e.to_string().contains("pressure") {
                HecError::server_busy()
            } else {
                HecError::internal(&e.to_string())
            }
        })?;
    }

    state.metrics.inc_requests_success();
    Ok(Json(HecResponse::success()))
}

/// `GET /services/collector/health` — Health check.
async fn health_handler(State(state): State<HecState>) -> (StatusCode, Json<HecResponse>) {
    if state.pipeline.is_ready() {
        (
            StatusCode::OK,
            Json(HecResponse {
                text: "HEC is healthy".into(),
                code: 17,
            }),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HecResponse {
                text: "HEC is unhealthy".into(),
                code: 18,
            }),
        )
    }
}

/// Extract a header value as a String.
fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SplunkHecConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.bind_address, "0.0.0.0:8088");
        assert_eq!(config.max_body_size, 10 * 1024 * 1024);
        assert_eq!(config.request_timeout_ms, 30_000);
    }

    #[test]
    fn test_hec_response_success() {
        let resp = HecResponse::success();
        assert_eq!(resp.code, 0);
        assert_eq!(resp.text, "Success");
    }
}
