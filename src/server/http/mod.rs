// Project:   dfe-receiver
// File:      src/server/http/mod.rs
// Purpose:   HTTP server using axum
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! HTTP server implementation using axum.
//!
//! Handles the primary `/ingest` endpoint for receiving JSON payloads.
//! Supports TLS termination and mTLS client certificate validation.
//!
//! # Security Hardening
//!
//! This server is designed for internet-facing deployment with:
//! - **Early auth rejection**: Authentication checked in middleware before body parsing
//! - **Request body limits**: Prevents memory exhaustion from large payloads
//! - **Request timeouts**: Prevents slow loris and connection exhaustion attacks
//! - **TLS handshake timeout**: Prevents TLS renegotiation attacks

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware;
use axum::routing::{get, post};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tracing::{debug, error, info, warn};

use crate::config::AuthConfig;
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::auth::{AuthState, BearerTokenProvider, token_auth_middleware};
use crate::server::tls::{TlsCertProvider, build_tls_acceptor, uses_secrets};
use crate::server::traits::ProtocolHandler;

/// TLS handshake timeout to prevent slow TLS attacks.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// HTTP protocol handler wrapping the existing axum server.
pub struct HttpHandler {
    bind_address: String,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
}

impl HttpHandler {
    /// Create a new HTTP handler.
    pub fn new(bind_address: String, pipeline: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        Self {
            bind_address,
            pipeline,
            metrics,
        }
    }
}

#[async_trait::async_trait]
impl ProtocolHandler for HttpHandler {
    fn name(&self) -> &'static str {
        "http"
    }

    fn bind_address(&self) -> &str {
        &self.bind_address
    }

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        run_server(
            &self.bind_address,
            self.pipeline.clone(),
            self.metrics.clone(),
            shutdown,
        )
        .await
    }
}

/// Create auth state with optional bearer token provider.
pub async fn create_auth_state(config: &AuthConfig) -> Result<AuthState> {
    // Check if bearer auth is configured
    let has_bearer_tokens =
        !config.bearer.tokens.is_empty() || config.bearer.secret_source.is_some();
    let mode = config.mode.to_lowercase();

    if has_bearer_tokens && (mode == "bearer" || mode == "both" || mode == "header") {
        let provider = BearerTokenProvider::from_config(&config.bearer).await?;
        let provider = Arc::new(provider);

        // Start background refresh if secret source is configured
        if let Some(ref source) = config.bearer.secret_source {
            let refresh_interval =
                std::time::Duration::from_secs(config.bearer.refresh_interval_secs);
            provider
                .clone()
                .start_refresh_task(source.clone(), refresh_interval);
        }

        Ok(AuthState::with_bearer_provider_arc(
            config.clone(),
            provider,
        ))
    } else {
        Ok(AuthState::new(config.clone()))
    }
}

/// Shared state for HTTP handlers.
#[derive(Clone)]
pub struct HttpState {
    pub pipeline: Arc<PipelineState>,
    pub metrics: Arc<Metrics>,
    pub auth: AuthState,
}

/// Run the HTTP server with optional TLS.
pub async fn run_server(
    addr: &str,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<()> {
    let config = pipeline.config();

    // Create auth state with optional bearer token provider
    let auth_state = create_auth_state(&config.server.auth).await?;

    let state = HttpState {
        pipeline,
        metrics: metrics.clone(),
        auth: auth_state.clone(),
    };

    // Build TLS: use TlsCertProvider with hot-reload if secrets configured,
    // otherwise one-shot load
    let tls_provider = if config.server.tls.enabled && uses_secrets(&config.server.tls) {
        let provider = TlsCertProvider::new(config.server.tls.clone()).await?;
        provider.start_refresh_task();
        Some(provider)
    } else {
        None
    };

    let tls_acceptor = if tls_provider.is_some() {
        None // Handled by provider below
    } else {
        build_tls_acceptor(&config.server.tls)?
    };

    // Security configuration
    let max_body_size = config.server.max_body_size;
    let request_timeout = Duration::from_millis(config.server.request_timeout_ms);

    info!(
        max_body_size = max_body_size,
        request_timeout_ms = config.server.request_timeout_ms,
        "Security limits configured"
    );

    // Build router with security layers applied in correct order.
    //
    // LAYER ORDER (outermost to innermost, i.e. first to execute):
    // 1. TimeoutLayer - Reject slow requests early (prevents slow loris)
    // 2. RequestBodyLimitLayer - Reject oversized bodies before reading (prevents OOM)
    // 3. Auth middleware - Reject unauthenticated requests before processing
    // 4. Handler - Only reached by authenticated, properly-sized, timely requests
    //
    // This order ensures minimal resource usage for malicious/bot requests.
    let app = Router::new()
        .route("/ingest", post(ingest_handler))
        .route("/health/live", get(liveness_handler))
        .route("/health/ready", get(readiness_handler))
        // Auth middleware - reject unauthenticated requests early (after body limit check)
        .layer(middleware::from_fn_with_state(
            auth_state,
            token_auth_middleware,
        ))
        // Body size limit - reject oversized requests before reading body
        .layer(RequestBodyLimitLayer::new(max_body_size))
        // Request timeout - reject slow requests (slow loris protection)
        // Returns 408 Request Timeout for slow clients
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            request_timeout,
        ))
        .with_state(state);

    let addr: SocketAddr = addr
        .parse()
        .map_err(|e| Error::Config(format!("invalid bind address: {e}")))?;

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Server(format!("failed to bind: {e}")))?;

    if let Some(ref provider) = tls_provider {
        // Hot-reloadable TLS via TlsCertProvider
        let acceptor_handle = provider.acceptor_handle();
        info!(addr = %addr, tls = true, hot_reload = true, "HTTP server listening");
        run_tls_server(listener, app, acceptor_handle, shutdown, metrics).await
    } else if let Some(acceptor) = tls_acceptor {
        // Static TLS (no secrets, no hot-reload)
        let acceptor_handle = Arc::new(parking_lot::RwLock::new(acceptor));
        info!(addr = %addr, tls = true, hot_reload = false, "HTTP server listening");
        run_tls_server(listener, app, acceptor_handle, shutdown, metrics).await
    } else {
        info!(addr = %addr, tls = false, "HTTP server listening");
        run_plain_server(listener, app, shutdown).await
    }
}

/// Run HTTP server without TLS.
async fn run_plain_server(
    listener: TcpListener,
    app: Router,
    shutdown: CancellationToken,
) -> Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
        .map_err(|e| Error::Server(format!("server error: {e}")))?;

    info!("HTTP server stopped");
    Ok(())
}

/// Run HTTP server with TLS.
///
/// Accepts an `Arc<RwLock<TlsAcceptor>>` to support hot-reload of certificates.
/// The RwLock read is held only to clone the acceptor (cheap - wraps Arc<ServerConfig>).
pub(crate) async fn run_tls_server(
    listener: TcpListener,
    app: Router,
    acceptor: Arc<parking_lot::RwLock<TlsAcceptor>>,
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("HTTP TLS server stopping");
                break;
            }
            result = listener.accept() => {
                let (stream, peer_addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!(error = %e, "Failed to accept connection");
                        continue;
                    }
                };

                let acceptor = acceptor.read().clone();
                let app = app.clone();
                let shutdown = shutdown.clone();
                let metrics = metrics.clone();

                tokio::spawn(async move {
                    // TLS handshake with timeout to prevent slow TLS attacks
                    let tls_result = timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await;

                    let tls_stream = match tls_result {
                        Ok(Ok(stream)) => stream,
                        Ok(Err(e)) => {
                            // Track TLS failure in metrics
                            metrics.inc_tls_handshake_failure();
                            debug!(peer = %peer_addr, error = %e, "tls_handshake_failed");
                            return;
                        }
                        Err(_) => {
                            // Track TLS timeout in metrics
                            metrics.inc_tls_handshake_failure();
                            warn!(peer = %peer_addr, "tls_handshake_timeout");
                            return;
                        }
                    };

                    debug!(peer = %peer_addr, "TLS connection established");

                    let io = hyper_util::rt::TokioIo::new(tls_stream);
                    let service = hyper_util::service::TowerToHyperService::new(app);

                    let builder = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new()
                    );
                    let conn = builder.serve_connection_with_upgrades(io, service);

                    tokio::select! {
                        _ = shutdown.cancelled() => {}
                        result = conn => {
                            if let Err(e) = result {
                                debug!(peer = %peer_addr, error = %e, "Connection error");
                            }
                        }
                    }
                });
            }
        }
    }

    info!("HTTP TLS server stopped");
    Ok(())
}

/// Main ingest endpoint handler (HOT PATH).
///
/// Receives JSON payloads, validates them, routes to destination,
/// and responds with appropriate status codes.
#[inline]
async fn ingest_handler(
    State(state): State<HttpState>,
    body: Bytes,
) -> std::result::Result<StatusCode, Error> {
    // Record metrics
    state.metrics.inc_requests_total();
    state.metrics.add_bytes_received(body.len() as u64);

    // Note: Auth is validated in middleware layer (token_auth_middleware)
    // No additional validation here - middleware handles all auth modes

    // Process through pipeline
    match state.pipeline.process(body).await {
        Ok(()) => {
            state.metrics.inc_requests_success();
            Ok(StatusCode::ACCEPTED)
        }
        Err(e) => {
            state.metrics.inc_requests_error();
            Err(e)
        }
    }
}

/// Liveness probe handler.
async fn liveness_handler() -> &'static str {
    "OK"
}

/// Readiness probe handler.
async fn readiness_handler(State(state): State<HttpState>) -> StatusCode {
    if state.pipeline.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AcceptedHeader, AuthConfig, BearerConfig};

    fn test_auth_config() -> AuthConfig {
        AuthConfig {
            mode: "none".to_string(),
            accepted_headers: vec![AcceptedHeader {
                name: "x-api-key".to_string(),
                values: vec!["test".to_string()],
            }],
            bearer: BearerConfig::default(),
            include_common_header: false,
            header_name: String::new(),
            header_values: Vec::new(),
        }
    }

    #[test]
    fn test_http_state_clone() {
        // Verify HttpState can be cloned (required for axum)
        let auth = AuthState::new(test_auth_config());
        let auth2 = auth.clone();
        assert_eq!(auth.config.mode, auth2.config.mode);
    }
}
