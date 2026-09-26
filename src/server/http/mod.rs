// Project:   dfe-receiver
// File:      src/server/http/mod.rs
// Purpose:   HTTP server using axum
// Language:  Rust
//
// License:   BUSL-1.1
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

use scalo::logger::security::{self, SecurityOutcome};

use crate::config::{AuthConfig, SharedConfig};
use crate::error::{Error, Result, unavailable_response};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::auth::{AuthState, BearerTokenProvider, token_auth_middleware};
use crate::server::ip_filter::IpFilter;
use crate::server::tls::{TlsCertProvider, build_tls_acceptor, uses_secrets};
use crate::server::traits::{BoundAddr, ProtocolHandler};
use crate::validation::depth::{MAX_BATCH_DEPTH, MAX_PARSE_DEPTH, json_depth_within};

/// TLS handshake timeout to prevent slow TLS attacks.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Time allowed for a client to send request headers after connecting.
/// Defends against slowloris attacks where clients send headers very slowly.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Idle connection timeout -- close connections with no active streams.
const CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_mins(1);

/// Nanoseconds in a second, the numerator of the rate limiter's period.
const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// Most events one batched POST may carry.
///
/// `max_body_size` alone does not bound the split: a body of `[1,1,1,...]`
/// yields one `Bytes` per two input bytes, so a 10 MiB body would allocate a
/// vector tens of times its size. At this cap a full body still allows about
/// 100 bytes per event, well under any real one.
const MAX_BATCH_EVENTS: usize = 100_000;

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
    bound: BoundAddr,
}

impl HttpHandler {
    /// Create a new HTTP handler.
    pub fn new(bind_address: String, pipeline: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        Self {
            bind_address,
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
impl ProtocolHandler for HttpHandler {
    fn name(&self) -> &'static str {
        "http"
    }

    fn bind_address(&self) -> &str {
        &self.bind_address
    }

    fn listeners(&self) -> Vec<BoundAddr> {
        vec![self.bound.clone()]
    }

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        serve(
            &self.bind_address,
            self.pipeline.clone(),
            self.metrics.clone(),
            shutdown,
            &self.bound,
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
                            provider.update_tokens(&new_auth.bearer.tokens);
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
    serve(addr, pipeline, metrics, shutdown, &BoundAddr::default()).await
}

/// Run the HTTP server, publishing the address it binds to `bound`.
async fn serve(
    addr: &str,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    bound: &BoundAddr,
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
        pipeline: pipeline.clone(),
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

    info!(
        max_body_size = max_body_size,
        request_timeout_ms = config.server.request_timeout_ms,
        max_concurrent_requests = config.server.max_concurrent_requests,
        rate_limit_enabled = config.server.rate_limit.enabled,
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
    // /livez + /readyz are the whole probe surface, matching scalo. No aliases:
    // an alias that keeps answering 200 hides a probe still aimed at a retired
    // name, which is how this service's chart and image drifted apart for six
    // days and three thousand restarts. A startupProbe targets /livez.
    //
    // These are registered HERE rather than inherited from scalo's HttpServer
    // because this server is hand-rolled on hyper's low-level API for
    // connection control -- it will never pick up scalo's routes automatically.
    let mut app = Router::new()
        .route("/ingest", post(ingest_handler))
        .route("/livez", get(liveness_handler))
        .route("/readyz", get(readiness_handler))
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

    // The webhook routes join here, after the auth middleware and body limit
    // above have been applied to the ingest routes, so they keep their own
    // per-caller auth and body limit while sharing everything applied below.
    if config.webhook.enabled && config.webhook.bind_address.is_none() {
        let webhook = crate::server::webhook::build_router(
            &config.webhook,
            pipeline.clone(),
            metrics.clone(),
        )
        .await?;
        app = app.merge(webhook);
        info!(
            callers = config.webhook.callers.len(),
            "Webhook intake sharing the ingest listener"
        );
    }

    let app = apply_server_limits(app, &config.server)?;

    // The IP filter runs in the accept loops below, before any HTTP work, and
    // `connection_service` hands each request the peer address the rate
    // limiter falls back to.

    let addr: SocketAddr = addr
        .parse()
        .map_err(|e| Error::Config(format!("invalid bind address: {e}")))?;

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Server(format!("failed to bind: {e}")))?;
    let _serving = bound.publish(&listener.local_addr());

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

/// Apply the server-wide admission limits: the global concurrency cap and the
/// per-IP GCRA rate limit.
///
/// Outermost layers, added last so they wrap every route on the router,
/// including ones merged in after the ingest routes' own middleware. Every HTTP
/// listener calls this -- ingest, the webhook's own listener, Splunk HEC,
/// Prometheus remote write and OTLP HTTP -- so none of them bypasses a limit
/// `server.*` reads as covering the whole ingest surface.
///
/// Each call builds its own governor, so the per-IP budget is per listener: a
/// client saturating HEC does not consume the OTLP budget for the same IP.
///
/// `requests_per_second` is a rate, and the governor is configured by the
/// interval between replenishments -- see [`replenish_period`].
pub(crate) fn apply_server_limits(
    mut app: Router,
    server: &crate::config::ServerConfig,
) -> Result<Router> {
    // Concurrency limit (0 = unlimited)
    if server.max_concurrent_requests > 0 {
        app = app.layer(GlobalConcurrencyLimitLayer::new(
            server.max_concurrent_requests,
        ));
    }

    // Per-IP rate limiting via GCRA (tower-governor).
    // SmartIpKeyExtractor: checks X-Forwarded-For, X-Real-IP, Forwarded
    // headers first, then falls back to peer IP.
    let rate_limit_config = &server.rate_limit;
    if rate_limit_config.enabled {
        let period = replenish_period(rate_limit_config.requests_per_second).ok_or_else(|| {
            Error::Config(format!(
                "server.rate_limit.requests_per_second is {}, which has no replenish \
                 period -- it must be between 1 and {NANOS_PER_SECOND}",
                rate_limit_config.requests_per_second
            ))
        })?;
        let governor_conf = GovernorConfigBuilder::default()
            .period(period)
            .burst_size(rate_limit_config.burst)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .ok_or_else(|| Error::Config("invalid rate_limit configuration".into()))?;

        app = app.layer(GovernorLayer::new(governor_conf));
        info!(
            rps = rate_limit_config.requests_per_second,
            burst = rate_limit_config.burst,
            period_ms = period.as_secs_f64() * 1000.0,
            "Per-IP rate limiting enabled"
        );
    }

    Ok(app)
}

/// The interval after which the governor replenishes one request of the quota.
///
/// `GovernorConfigBuilder` is configured by that interval, not by a rate:
/// `per_second(n)` sets the period to n SECONDS, which is one request every n
/// seconds. A sustained rate of n requests per second is its reciprocal, so the
/// period is derived here rather than handed to a setter that reads the rate as
/// an interval.
///
/// `None` for a rate with no usable period: zero has none, and a rate finer than
/// one request per nanosecond rounds down to none.
fn replenish_period(requests_per_second: u64) -> Option<Duration> {
    let nanos = NANOS_PER_SECOND.checked_div(requests_per_second)?;
    (nanos > 0).then(|| Duration::from_nanos(nanos))
}

/// Run HTTP server without TLS.
///
/// Uses hyper low-level APIs (instead of `axum::serve`) to gain control over
/// connection-level timeouts. This protects against slowloris attacks where
/// `axum::serve` has no native defence.
///
/// Every plaintext HTTP listener runs here rather than on `axum::serve`, so
/// each gets the accept-loop IP filter, the hardened header-read timeout, and
/// the peer address the rate limiter keys on when no proxy header names the
/// client.
pub(crate) async fn run_plain_server(
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

                // IP filter at connection level -- reject before any HTTP work
                if !ip_filter.admits(peer_addr) {
                    drop(stream);
                    continue;
                }

                let app = app.clone();
                let shutdown = shutdown.clone();

                tokio::spawn(async move {
                    let io = hyper_util::rt::TokioIo::new(stream);
                    let service = connection_service(app, peer_addr);

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

/// The router as a hyper service for one connection, with the peer address
/// on every request so the rate limiter keys on it when no proxy header names
/// the client.
fn connection_service(
    app: Router,
    peer_addr: SocketAddr,
) -> hyper_util::service::TowerToHyperService<
    tower::util::MapRequest<
        Router,
        impl FnMut(
            axum::extract::Request<hyper::body::Incoming>,
        ) -> axum::extract::Request<hyper::body::Incoming>
        + Clone,
    >,
> {
    hyper_util::service::TowerToHyperService::new(tower::ServiceExt::map_request(
        app,
        move |mut request: axum::extract::Request<hyper::body::Incoming>| {
            request
                .extensions_mut()
                .insert(axum::extract::ConnectInfo(peer_addr));
            request
        },
    ))
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

                // IP filter at connection level -- reject before TLS handshake
                if !ip_filter.admits(peer_addr) {
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
                    let service = connection_service(app, peer_addr);

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
            "Request rejected -- pipeline not ready (backpressure)"
        );
        state.metrics.inc_requests_total("http");
        state.metrics.inc_requests_error("http");
        state.metrics.record_backpressure();
        return Ok(unavailable_response("server is overloaded"));
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
    let batch = split_batch_body(&body);
    let events = match &batch {
        Some(Ok(payloads)) => payloads.len(),
        _ => 1,
    };
    // A batch fails as a whole, so without the count a partial failure and a
    // total one look the same. A retryable failure is the answer over an
    // earlier refusal, so the sender resends rather than dropping the batch.
    let (accepted, result) = match batch {
        Some(Ok(payloads)) => {
            let outcome = state.pipeline.process_batch(&payloads).await;
            (outcome.accepted, outcome.into_error().map_or(Ok(()), Err))
        }
        Some(Err(oversize)) => (0, Err(oversize)),
        None => {
            let result = state.pipeline.process(body).await;
            (usize::from(result.is_ok()), result)
        }
    };
    let elapsed = start.elapsed();
    state
        .metrics
        .record_request_duration("http", elapsed.as_secs_f64());

    match result {
        Ok(()) => {
            debug!(
                transport = "http",
                bytes = body_len,
                events,
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
                events,
                accepted,
                duration_us = elapsed.as_micros(),
                error = %e,
                "HTTP ingest request failed"
            );
            state.metrics.inc_requests_error("http");
            Err(e)
        }
    }
}

/// Split a batched ingest body into one payload per event.
///
/// Covers the two shapes a single POST can carry more than one event in: a
/// top-level JSON array, and newline-delimited JSON. Returns None for a body
/// that is one event, which takes the unsplit path.
fn split_batch_body(body: &Bytes) -> Option<Result<Vec<Bytes>>> {
    split_json_array(body).or_else(|| split_ndjson(body))
}

/// Split a newline-delimited JSON body into one payload per line.
///
/// The deployment contract advertises NDJSON on this endpoint, but an
/// unsplit NDJSON body fails validation as a whole and lands in the DLQ, so
/// the client sees 202 and no data.
///
/// Splitting needs a rule that a pretty-printed single object cannot trip,
/// since its lines are not JSON on their own: a body only splits when it has
/// more than one non-blank line AND the first parses as a complete value. A
/// later malformed line is left to per-event validation, which routes it to
/// the DLQ without taking the rest of the batch with it.
fn split_ndjson(body: &Bytes) -> Option<Result<Vec<Bytes>>> {
    let lines: Vec<&[u8]> = body
        .split(|&b| b == b'\n')
        .map(<[u8]>::trim_ascii)
        .filter(|line| !line.is_empty())
        // Bound the scan itself: without it a body of "1\n" repeated builds a
        // slice per two bytes before the cap below could reject it.
        .take(MAX_BATCH_EVENTS + 1)
        .collect();

    if lines.len() < 2 {
        return None;
    }
    // Too deep to parse stays unsplit, and the pipeline refuses the whole body.
    if !json_depth_within(lines[0], MAX_PARSE_DEPTH) {
        return None;
    }
    sonic_rs::from_slice::<sonic_rs::LazyValue>(lines[0]).ok()?;
    if lines.len() > MAX_BATCH_EVENTS {
        return Some(Err(Error::Validation(format!(
            "batch exceeds {MAX_BATCH_EVENTS} events"
        ))));
    }
    Some(Ok(lines.into_iter().map(Bytes::copy_from_slice).collect()))
}

/// Split a top-level JSON array body into one payload per element.
///
/// A batched POST carries `[{...},{...}]`; forwarded whole it becomes one
/// message holding an array, which downstream reads as a single event and
/// rejects. Returns None for anything that is not a well-formed array, which
/// stays a single event. Every other transport already splits before the
/// pipeline.
///
/// Each element is copied out as its original bytes: an integer wider than
/// `u64` does not survive a parse to a value tree and back.
///
/// `Some(Err(..))` is an array that exceeded [`MAX_BATCH_EVENTS`] and must be
/// rejected rather than forwarded whole.
pub(crate) fn split_json_array(body: &Bytes) -> Option<Result<Vec<Bytes>>> {
    if *body.iter().find(|b| !b.is_ascii_whitespace())? != b'[' {
        return None;
    }
    // Too deep to split stays whole, and the pipeline refuses it.
    if !json_depth_within(body, MAX_BATCH_DEPTH) {
        return None;
    }
    // The element iterator stops at the closing bracket, so anything trailing
    // it would be dropped silently; this rejects the whole body instead.
    sonic_rs::from_slice::<sonic_rs::LazyValue>(body).ok()?;
    // A &[u8] input keeps every LazyValue borrowed from the body; a &Bytes
    // input would copy it into a FastStr first.
    let mut payloads = Vec::new();
    for element in sonic_rs::to_array_iter(&body[..]) {
        if payloads.len() == MAX_BATCH_EVENTS {
            return Some(Err(Error::Validation(format!(
                "batch exceeds {MAX_BATCH_EVENTS} events"
            ))));
        }
        payloads.push(Bytes::copy_from_slice(
            element.ok()?.as_raw_str().as_bytes(),
        ));
    }
    Some(Ok(payloads))
}

/// Liveness probe handler.
async fn liveness_handler() -> &'static str {
    "OK"
}

/// Readiness probe handler: the same answer as the kubelet's `/readyz` on the metrics port.
async fn readiness_handler(State(state): State<HttpState>) -> StatusCode {
    if state.pipeline.probe_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AcceptedHeader, AuthConfig, BearerConfig};

    fn split_ok(body: &Bytes) -> Vec<Bytes> {
        split_json_array(body)
            .expect("an array body splits")
            .expect("within the batch cap")
    }

    fn batch_ok(body: &Bytes) -> Vec<Bytes> {
        split_batch_body(body)
            .expect("a batched body splits")
            .expect("within the batch cap")
    }

    #[test]
    fn an_ndjson_body_becomes_one_event_per_line() {
        let body = Bytes::from("{\"a\":1}\n{\"a\":2}\n{\"a\":3}");
        let payloads = batch_ok(&body);
        assert_eq!(payloads.len(), 3);
        assert_eq!(payloads[0], Bytes::from(r#"{"a":1}"#));
        assert_eq!(payloads[2], Bytes::from(r#"{"a":3}"#));
    }

    #[test]
    fn ndjson_blank_and_crlf_lines_do_not_become_events() {
        let body = Bytes::from("{\"a\":1}\r\n\r\n{\"a\":2}\n\n");
        let payloads = batch_ok(&body);
        assert_eq!(payloads.len(), 2);
        assert_eq!(payloads[0], Bytes::from(r#"{"a":1}"#));
        assert_eq!(payloads[1], Bytes::from(r#"{"a":2}"#));
    }

    #[test]
    fn a_pretty_printed_object_is_not_shredded_into_lines() {
        // Its first line is not a complete JSON value, which is what keeps a
        // multi-line single object off the NDJSON path.
        let body = Bytes::from("{\n  \"a\": 1,\n  \"b\": 2\n}");
        assert!(split_batch_body(&body).is_none());
    }

    #[test]
    fn a_later_malformed_ndjson_line_still_splits_for_per_event_validation() {
        let body = Bytes::from("{\"a\":1}\nnot json\n{\"a\":2}");
        let payloads = batch_ok(&body);
        assert_eq!(payloads.len(), 3);
        assert_eq!(payloads[1], Bytes::from("not json"));
    }

    #[test]
    fn a_single_line_object_takes_the_unsplit_path() {
        assert!(split_batch_body(&Bytes::from(r#"{"a":1}"#)).is_none());
        assert!(split_batch_body(&Bytes::from("{\"a\":1}\n")).is_none());
    }

    #[test]
    fn ndjson_over_the_cap_is_rejected() {
        let body = Bytes::from(vec!["1"; MAX_BATCH_EVENTS + 1].join("\n"));
        let err = split_batch_body(&body)
            .expect("still a batch")
            .expect_err("over the cap");
        assert!(matches!(err, Error::Validation(_)), "{err}");
    }

    #[test]
    fn a_json_array_body_splits_into_one_payload_per_element() {
        let body = Bytes::from(r#"[{"a":1},{"a":2},{"a":3}]"#);
        let payloads = split_ok(&body);
        assert_eq!(payloads.len(), 3);
        assert_eq!(payloads[0], Bytes::from(r#"{"a":1}"#));
        assert_eq!(payloads[2], Bytes::from(r#"{"a":3}"#));
    }

    #[test]
    fn leading_whitespace_does_not_hide_an_array() {
        let body = Bytes::from("  \n\t[{\"a\":1}]");
        assert_eq!(split_ok(&body).len(), 1);
    }

    #[test]
    fn a_single_object_stays_one_event() {
        let body = Bytes::from(r#"{"a":1}"#);
        assert!(split_json_array(&body).is_none());
    }

    #[test]
    fn a_non_json_body_stays_one_event() {
        assert!(split_json_array(&Bytes::from("not json at all")).is_none());
        assert!(split_json_array(&Bytes::from("")).is_none());
    }

    #[test]
    fn a_malformed_array_is_not_split_and_is_left_to_validation() {
        let body = Bytes::from(r#"[{"a":1},"#);
        assert!(split_json_array(&body).is_none());
    }

    #[test]
    fn an_element_reaches_kafka_byte_identical_to_the_same_object_posted_alone() {
        // Each case reaches the destination untouched when posted alone, so it
        // must survive batching too.
        let cases = [
            r#"{"id":123456789012345678901234}"#,
            r#"{"a":"A"}"#,
            r#"{"a":1,"a":2}"#,
            r#"{"a":1.0,"b":1e2}"#,
        ];
        for object in cases {
            let body = Bytes::from(format!("[{object}]"));
            let payloads = split_ok(&body);
            assert_eq!(payloads.len(), 1);
            assert_eq!(
                payloads[0],
                Bytes::from(object),
                "batching rewrote the element"
            );
        }
    }

    #[test]
    fn an_empty_array_carries_no_events() {
        assert!(split_ok(&Bytes::from("[]")).is_empty());
    }

    #[test]
    fn an_array_at_the_cap_still_splits() {
        let body = Bytes::from(format!("[{}]", vec!["1"; MAX_BATCH_EVENTS].join(",")));
        assert_eq!(split_ok(&body).len(), MAX_BATCH_EVENTS);
    }

    #[test]
    fn an_array_over_the_cap_is_rejected_rather_than_forwarded_whole() {
        // Without the cap a body of two-byte elements allocates a vector many
        // times the body size, which max_body_size does not bound.
        let body = Bytes::from(format!("[{}]", vec!["1"; MAX_BATCH_EVENTS + 1].join(",")));
        let err = split_json_array(&body)
            .expect("still an array")
            .expect_err("over the cap");
        assert!(
            matches!(err, Error::Validation(_)),
            "an oversize batch must be a client error: {err}"
        );
    }

    #[test]
    fn trailing_content_after_the_array_is_not_split() {
        let body = Bytes::from(r#"[{"a":1}] and then some"#);
        assert!(split_json_array(&body).is_none());
    }

    #[test]
    fn nested_arrays_split_only_at_the_top_level() {
        let body = Bytes::from(r#"[[1,2],{"a":[3]}]"#);
        let payloads = split_ok(&body);
        assert_eq!(payloads.len(), 2);
        assert_eq!(payloads[0], Bytes::from("[1,2]"));
        assert_eq!(payloads[1], Bytes::from(r#"{"a":[3]}"#));
    }

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

    #[test]
    fn the_replenish_period_is_the_reciprocal_of_the_rate() {
        // The trap this guards: the builder's per_second(n) sets the period to
        // n SECONDS, so a rate handed straight to it delivers one request every
        // n seconds -- n squared times tighter than asked. At the documented
        // default of 100 the period is 10ms, not 100s.
        assert_eq!(replenish_period(100), Some(Duration::from_millis(10)));
        assert_eq!(replenish_period(4), Some(Duration::from_millis(250)));
        assert_eq!(replenish_period(1000), Some(Duration::from_millis(1)));
    }

    #[test]
    fn a_rate_of_one_is_the_one_value_a_period_and_a_rate_agree_on() {
        // The single rate where the two readings coincide, so a test set here
        // cannot tell them apart.
        assert_eq!(replenish_period(1), Some(Duration::from_secs(1)));
    }

    #[test]
    fn a_rate_with_no_usable_period_is_refused_rather_than_dividing_by_zero() {
        assert_eq!(replenish_period(0), None);
        assert_eq!(replenish_period(NANOS_PER_SECOND + 1), None);
        assert_eq!(
            replenish_period(NANOS_PER_SECOND),
            Some(Duration::from_nanos(1))
        );
    }
}
