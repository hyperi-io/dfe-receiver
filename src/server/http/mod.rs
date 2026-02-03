// Project:   dfe-receiver
// File:      src/server/http/mod.rs
// Purpose:   HTTP server using axum
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

//! HTTP server implementation using axum.
//!
//! Handles the primary `/ingest` endpoint for receiving JSON payloads.
//! Supports TLS termination and mTLS client certificate validation.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::middleware;
use axum::routing::{get, post};
use axum::Router;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::auth::{header_auth_middleware, validate_header_auth, AuthState};
use crate::server::tls::build_tls_acceptor;

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
    let auth_state = AuthState::new(config.server.auth.clone());

    let state = HttpState {
        pipeline,
        metrics,
        auth: auth_state.clone(),
    };

    // Build TLS acceptor if enabled
    let tls_acceptor = build_tls_acceptor(&config.server.tls)?;

    // Build router with auth middleware
    let app = Router::new()
        .route("/ingest", post(ingest_handler))
        .route("/health/live", get(liveness_handler))
        .route("/health/ready", get(readiness_handler))
        .layer(middleware::from_fn_with_state(
            auth_state,
            header_auth_middleware,
        ))
        .with_state(state);

    let addr: SocketAddr = addr
        .parse()
        .map_err(|e| Error::Config(format!("invalid bind address: {e}")))?;

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Server(format!("failed to bind: {e}")))?;

    match tls_acceptor {
        Some(acceptor) => {
            info!(addr = %addr, tls = true, "HTTP server listening");
            run_tls_server(listener, app, acceptor, shutdown).await
        }
        None => {
            info!(addr = %addr, tls = false, "HTTP server listening");
            run_plain_server(listener, app, shutdown).await
        }
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
async fn run_tls_server(
    listener: TcpListener,
    app: Router,
    acceptor: TlsAcceptor,
    shutdown: CancellationToken,
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

                let acceptor = acceptor.clone();
                let app = app.clone();
                let shutdown = shutdown.clone();

                tokio::spawn(async move {
                    match acceptor.accept(stream).await {
                        Ok(tls_stream) => {
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
                        }
                        Err(e) => {
                            debug!(peer = %peer_addr, error = %e, "TLS handshake failed");
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
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<StatusCode, Error> {
    // Record metrics
    state.metrics.inc_requests_total();
    state.metrics.add_bytes_received(body.len() as u64);

    // Validate auth (belt and suspenders - middleware should have caught this)
    if let Some(auth_err) = validate_header_auth(&state.auth.config, &headers) {
        state.metrics.inc_requests_error();
        return Err(Error::Auth(auth_err.message));
    }

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
    use crate::config::{AcceptedHeader, AuthConfig};

    fn test_auth_config() -> AuthConfig {
        AuthConfig {
            mode: "none".to_string(),
            accepted_headers: vec![AcceptedHeader {
                name: "x-api-key".to_string(),
                values: vec!["test".to_string()],
            }],
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
