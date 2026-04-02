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
use axum::response::IntoResponse;
use axum::routing::{get, post};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower_governor::GovernorLayer;
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::key_extractor::SmartIpKeyExtractor;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tracing::{debug, error, info, warn};

use hyperi_rustlib::logger::security::{self, SecurityOutcome};

use crate::config::{AuthConfig, SharedConfig};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::auth::{AuthState, BearerTokenProvider, token_auth_middleware};
use crate::server::ip_filter::IpFilter;
use crate::server::tls::{TlsCertProvider, build_tls_acceptor, uses_secrets};
use crate::server::traits::ProtocolHandler;

/// TLS handshake timeout to prevent slow TLS attacks.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Time allowed for a client to send request headers after connecting.
/// Defends against slowloris attacks where clients send headers very slowly.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Idle connection timeout — close connections with no active streams.
const CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Build a hyper HTTP connection builder with hardened timeouts.
///
/// Used by both TLS and plain-text server paths to ensure consistent
/// slowloris protection. The timer is required by hyper when
/// `header_read_timeout` is set.
fn hardened_http_builder() -> hyper_util::server::conn::auto::Builder<hyper_util::rt::TokioExecutor>
{
    let mut builder =
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
    builder
        .http1()
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .keep_alive(true)
        .timer(hyper_util::rt::TokioTimer::new());
    builder
        .http2()
        .keep_alive_timeout(CONNECTION_IDLE_TIMEOUT)
        .timer(hyper_util::rt::TokioTimer::new());
    builder
}

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

/// Spawn a background task that watches for auth config changes and updates
/// bearer tokens when the config is hot-reloaded.
///
/// Compares the auth config on each `SharedConfig` version bump. If bearer
/// tokens changed, swaps them atomically via `BearerTokenProvider::update_tokens`.
pub fn spawn_auth_reload_watcher(
    shared_config: SharedConfig,
    auth_state: AuthState,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut rx = shared_config.subscribe();
        let mut current_auth = shared_config.get().server.auth.clone();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                result = rx.changed() => {
                    if result.is_err() {
                        break; // Channel closed
                    }
                    let new_config = shared_config.get();
                    let new_auth = &new_config.server.auth;

                    if *new_auth != current_auth {
                        info!("Auth config changed, reloading");

                        // Update bearer tokens if provider exists and tokens changed
                        if let Some(ref provider) = auth_state.bearer_provider
                            && new_auth.bearer.tokens != current_auth.bearer.tokens
                        {
                            provider.update_tokens(new_auth.bearer.tokens.clone());
                            info!(
                                count = new_auth.bearer.tokens.len(),
                                "Bearer tokens reloaded from config"
                            );
                        }

                        security::config_changed(
                            "auth_reload",
                            "system",
                            "auth configuration updated via config reload",
                        );

                        current_auth = new_auth.clone();
                    }
                }
            }
        }

        debug!("Auth reload watcher stopped");
    });
}

/// Shared state for HTTP handlers.
#[derive(Clone)]
pub struct HttpState {
    pub pipeline: Arc<PipelineState>,
    pub metrics: Arc<Metrics>,
    pub auth: AuthState,
    pub ip_filter: IpFilter,
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

    // Watch for config changes and update auth state (bearer tokens)
    spawn_auth_reload_watcher(
        pipeline.shared_config(),
        auth_state.clone(),
        shutdown.clone(),
    );

    let ip_filter = IpFilter::from_config(&config.server.ip_filter);

    let state = HttpState {
        pipeline,
        metrics: metrics.clone(),
        auth: auth_state.clone(),
        ip_filter: ip_filter.clone(),
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
    let max_concurrent = config.server.max_concurrent_requests;
    let rate_limit_config = &config.server.rate_limit;

    info!(
        max_body_size = max_body_size,
        request_timeout_ms = config.server.request_timeout_ms,
        max_concurrent_requests = max_concurrent,
        rate_limit_enabled = rate_limit_config.enabled,
        ip_filter_mode = %config.server.ip_filter.mode,
        "Security limits configured"
    );

    // Build router with security layers applied in correct order.
    //
    // LAYER ORDER (outermost to innermost, i.e. first to execute):
    // 1. IP filter - Reject banned IPs immediately (zero work done)
    // 2. Rate limit - Reject IPs exceeding rate (per-IP GCRA)
    // 3. ConcurrencyLimit - Reject when too many in-flight requests
    // 4. TimeoutLayer - Reject slow requests early (prevents slow loris)
    // 5. RequestBodyLimitLayer - Reject oversized bodies before reading
    // 6. Auth middleware - Reject unauthenticated requests before processing
    // 7. Handler - Only reached by filtered, rate-limited, authenticated requests
    //
    // This order ensures minimal resource usage for malicious/bot requests.
    let mut app = Router::new()
        .route("/ingest", post(ingest_handler))
        .route("/health/live", get(liveness_handler))
        .route("/health/ready", get(readiness_handler))
        // Auth middleware
        .layer(middleware::from_fn_with_state(
            auth_state,
            token_auth_middleware,
        ))
        // Body size limit
        .layer(RequestBodyLimitLayer::new(max_body_size))
        // Request timeout (408 for slow clients)
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            request_timeout,
        ))
        .with_state(state);

    // Concurrency limit (0 = unlimited)
    if max_concurrent > 0 {
        app = app.layer(GlobalConcurrencyLimitLayer::new(max_concurrent));
    }

    // Per-IP rate limiting via GCRA (tower-governor).
    // SmartIpKeyExtractor: checks X-Forwarded-For, X-Real-IP, Forwarded
    // headers first, then falls back to peer IP.
    if rate_limit_config.enabled {
        let governor_conf = GovernorConfigBuilder::default()
            .per_second(rate_limit_config.requests_per_second)
            .burst_size(rate_limit_config.burst)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .ok_or_else(|| Error::Config("invalid rate_limit configuration".into()))?;

        app = app.layer(GovernorLayer::new(governor_conf));
        info!(
            rps = rate_limit_config.requests_per_second,
            burst = rate_limit_config.burst,
            "Per-IP rate limiting enabled"
        );
    }

    // IP filter is checked in the ingest handler via HttpState (not as
    // middleware) because our hyper serve pattern doesn't use
    // into_make_service_with_connect_info and ConnectInfo isn't available.
    // Peer IP is available in the TLS/plain accept loops but not propagated
    // to axum request extensions. For now, the filter is applied at the
    // handler level via state — still rejects before pipeline processing.

    let addr: SocketAddr = addr
        .parse()
        .map_err(|e| Error::Config(format!("invalid bind address: {e}")))?;

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Server(format!("failed to bind: {e}")))?;

    if let Some(ref provider) = tls_provider {
        let acceptor_handle = provider.acceptor_handle();
        info!(addr = %addr, tls = true, hot_reload = true, "HTTP server listening");
        run_tls_server(listener, app, acceptor_handle, ip_filter, shutdown, metrics).await
    } else if let Some(acceptor) = tls_acceptor {
        let acceptor_handle = Arc::new(parking_lot::RwLock::new(acceptor));
        info!(addr = %addr, tls = true, hot_reload = false, "HTTP server listening");
        run_tls_server(listener, app, acceptor_handle, ip_filter, shutdown, metrics).await
    } else {
        info!(addr = %addr, tls = false, "HTTP server listening");
        run_plain_server(listener, app, ip_filter, shutdown).await
    }
}

/// Run HTTP server without TLS.
///
/// Uses hyper low-level APIs (instead of `axum::serve`) to gain control over
/// connection-level timeouts. This protects against slowloris attacks where
/// `axum::serve` has no native defence.
async fn run_plain_server(
    listener: TcpListener,
    app: Router,
    ip_filter: IpFilter,
    shutdown: CancellationToken,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("HTTP server stopping");
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

                // IP filter at connection level — reject before any HTTP work
                if !ip_filter.is_allowed(peer_addr.ip()) {
                    debug!(peer = %peer_addr, "connection rejected by IP filter");
                    drop(stream);
                    continue;
                }

                let app = app.clone();
                let shutdown = shutdown.clone();

                tokio::spawn(async move {
                    let io = hyper_util::rt::TokioIo::new(stream);
                    let service = hyper_util::service::TowerToHyperService::new(app);

                    let builder = hardened_http_builder();
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
    ip_filter: IpFilter,
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

                // IP filter at connection level — reject before TLS handshake
                if !ip_filter.is_allowed(peer_addr.ip()) {
                    debug!(peer = %peer_addr, "connection rejected by IP filter");
                    drop(stream);
                    continue;
                }

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
                            security::tls_event(
                                "handshake",
                                SecurityOutcome::Failure,
                                Some(&e.to_string()),
                                Some(peer_addr.ip()),
                            );
                            return;
                        }
                        Err(_) => {
                            // Track TLS timeout in metrics
                            metrics.inc_tls_handshake_failure();
                            warn!(peer = %peer_addr, "tls_handshake_timeout");
                            security::tls_event(
                                "handshake",
                                SecurityOutcome::Failure,
                                Some("handshake_timeout"),
                                Some(peer_addr.ip()),
                            );
                            return;
                        }
                    };

                    debug!(peer = %peer_addr, "TLS connection established");

                    let io = hyper_util::rt::TokioIo::new(tls_stream);
                    let service = hyper_util::service::TowerToHyperService::new(app);

                    let builder = hardened_http_builder();
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
///
/// Returns 503 with Retry-After header when the pipeline is not ready
/// (memory pressure, sink failure, shutdown). This gives clients a clear
/// backpressure signal to back off rather than accepting and dropping.
#[inline]
async fn ingest_handler(
    State(state): State<HttpState>,
    body: Bytes,
) -> std::result::Result<impl axum::response::IntoResponse, Error> {
    // Shed load when pipeline is not ready (memory pressure, sink down, draining)
    if !state.pipeline.is_ready() {
        debug!(
            transport = "http",
            "Request rejected — pipeline not ready (backpressure)"
        );
        state.metrics.inc_requests_total("http");
        state.metrics.inc_requests_error("http");
        state.metrics.record_backpressure();
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "5")],
            "server is overloaded",
        )
            .into_response());
    }

    let body_len = body.len();
    debug!(
        transport = "http",
        bytes = body_len,
        "HTTP ingest request received"
    );

    // Record metrics
    state.metrics.inc_requests_total("http");
    state.metrics.add_bytes_received("http", body_len as u64);

    // Note: Auth is validated in middleware layer (token_auth_middleware)
    // No additional validation here - middleware handles all auth modes

    // Process through pipeline (timed)
    let start = std::time::Instant::now();
    let result = state.pipeline.process(body).await;
    let elapsed = start.elapsed();
    state
        .metrics
        .record_request_duration("http", elapsed.as_secs_f64());

    match result {
        Ok(()) => {
            debug!(
                transport = "http",
                bytes = body_len,
                duration_us = elapsed.as_micros(),
                "HTTP ingest request accepted"
            );
            state.metrics.inc_requests_success("http");
            Ok(StatusCode::ACCEPTED.into_response())
        }
        Err(e) => {
            debug!(
                transport = "http",
                bytes = body_len,
                duration_us = elapsed.as_micros(),
                error = %e,
                "HTTP ingest request failed"
            );
            state.metrics.inc_requests_error("http");
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
