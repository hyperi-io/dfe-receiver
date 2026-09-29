// Project:   dfe-receiver
// File:      src/server/auth.rs
// Purpose:   Authentication middleware
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Authentication middleware for header, bearer token, and mTLS validation.
//!
//! Supports:
//! - Static header-based authentication (x-api-key, x-hyperi-agent)
//! - Bearer token authentication with secret manager integration
//! - mTLS client certificate validation
//!
//! Bearer tokens can be loaded from:
//! - Static configuration (for dev)
//! - Any `provider:path[:key]` spec [`crate::secrets`] reads -- a mounted
//!   Kubernetes Secret (`file:`), an OpenBao field (`vault:`), an environment
//!   variable (`env:`)

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use parking_lot::RwLock;
use ring::digest;
use scalo::SensitiveString;
use scalo::logger::log_debounced;
use scalo::logger::security::{self, SecurityEvent, SecurityOutcome};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::config::{AuthConfig, BearerConfig};
use crate::error::Result;
use crate::metrics::{AuthFailureReason, Metrics};

/// Shortest gap between two log lines for one failure reason, or one misconfiguration.
const AUTH_LOG_INTERVAL_MS: u64 = 5_000;

/// When each failure reason last wrote its log line, indexed by [`AuthFailureReason::index`].
static AUTH_FAILURE_LOGGED: [AtomicU64; AuthFailureReason::ALL.len()] =
    [const { AtomicU64::new(0) }; AuthFailureReason::ALL.len()];

/// When a request last found the auth configuration unusable.
static AUTH_MISCONFIGURED_LOGGED: AtomicU64 = AtomicU64::new(0);

/// Authentication mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    /// No authentication required.
    None,
    /// Header-based authentication (static values).
    Header,
    /// Bearer token authentication (supports secret manager).
    Bearer,
    /// mTLS client certificate authentication.
    Mtls,
    /// Both header/bearer and mTLS required.
    Both,
}

impl AuthMode {
    /// Parse from string.
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "header" => Self::Header,
            "bearer" => Self::Bearer,
            "mtls" => Self::Mtls,
            "both" => Self::Both,
            _ => Self::None,
        }
    }

    /// Check if this mode requires header or bearer validation.
    pub fn requires_token_auth(self) -> bool {
        matches!(self, Self::Header | Self::Bearer | Self::Both)
    }
}

/// Shared authentication state.
#[derive(Clone)]
pub struct AuthState {
    pub config: Arc<AuthConfig>,
    pub bearer_provider: Option<Arc<BearerTokenProvider>>,
}

impl AuthState {
    /// Create new auth state from config.
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config: Arc::new(config),
            bearer_provider: None,
        }
    }

    /// Create auth state with bearer token provider.
    pub fn with_bearer_provider(config: AuthConfig, provider: BearerTokenProvider) -> Self {
        Self {
            config: Arc::new(config),
            bearer_provider: Some(Arc::new(provider)),
        }
    }

    /// Create auth state with an Arc-wrapped bearer token provider.
    pub fn with_bearer_provider_arc(
        config: AuthConfig,
        provider: Arc<BearerTokenProvider>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            bearer_provider: Some(provider),
        }
    }
}

/// SHA-256 hash of a bearer token, stored as a fixed-size array.
///
/// Tokens are hashed before storage to prevent timing-attack side channels
/// (hash lookup is constant-time per bucket) and to avoid holding plaintext
/// tokens in memory where they could appear in core dumps.
type TokenHash = [u8; 32];

/// Hash a bearer token using SHA-256.
#[inline]
fn hash_token(token: &str) -> TokenHash {
    let d = digest::digest(&digest::SHA256, token.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}

/// Bearer token provider with dynamic secret loading.
///
/// Supports loading tokens from:
/// - Static configuration
/// - OpenBao/Vault
/// - Files (K8s secrets)
///
/// Tokens are stored as SHA-256 hashes to prevent timing-attack side channels
/// and avoid holding plaintext tokens in memory.
pub struct BearerTokenProvider {
    /// SHA-256 hashes of valid tokens (thread-safe for hot reloading).
    token_hashes: RwLock<HashSet<TokenHash>>,
    /// Shutdown signal for background refresh.
    shutdown_tx: broadcast::Sender<()>,
}

impl BearerTokenProvider {
    /// Create a new bearer token provider with static tokens.
    pub fn new(tokens: &[SensitiveString]) -> Self {
        let hash_set: HashSet<TokenHash> = tokens.iter().map(|t| hash_token(t.expose())).collect();
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            token_hashes: RwLock::new(hash_set),
            shutdown_tx,
        }
    }

    /// Create a bearer token provider from configuration.
    ///
    /// If `secret_source` is configured, tokens will be loaded dynamically.
    /// Otherwise, static tokens from config are used.
    ///
    /// # Errors
    ///
    /// Returns an error when the secret source fails to load AND no static
    /// tokens are configured to fall back to. A zero-token provider accepts
    /// nothing, so reporting success there is a total auth outage behind a
    /// warn line. "Using static tokens" only holds when there are static
    /// tokens.
    ///
    /// `create_auth_state` builds a provider for `header` mode too, so
    /// `mode: header` with an unloadable `bearer.secret_source` and no static
    /// tokens also refuses to start rather than serving header-only auth -- a
    /// secret source that cannot load is a misconfiguration the operator has
    /// to see.
    pub async fn from_config(config: &BearerConfig) -> Result<Self> {
        let provider = Self::new(&config.tokens);

        // If secret source is configured, load tokens from secret manager
        if let Some(ref source) = config.secret_source
            && let Err(e) = provider.load_tokens(source).await
        {
            if config.tokens.is_empty() {
                return Err(crate::error::Error::Config(format!(
                    "bearer secret_source '{source}' failed to load ({e}) and no \
                     static bearer tokens are configured -- refusing to start with \
                     no usable tokens"
                )));
            }
            warn!(error = %e, source = %source, "Failed to load bearer tokens from secret, using static tokens");
        }

        Ok(provider)
    }

    /// Load tokens from a secret source.
    ///
    /// The source is a `provider:path[:key]` reference as
    /// [`crate::secrets::read`] accepts; the content is a newline- or
    /// comma-separated token list.
    async fn load_tokens(&self, source: &str) -> Result<()> {
        let content = crate::secrets::read(source).await?;
        let new_hashes: HashSet<TokenHash> = content
            .lines()
            .flat_map(|line| line.split(','))
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(hash_token)
            .collect();

        let count = new_hashes.len();
        info!(count, source = %source, "Loaded bearer tokens from secret");
        *self.token_hashes.write() = new_hashes;

        security::token_rotated(
            "bearer_refresh",
            &format!("{count} tokens loaded from secret"),
        );

        Ok(())
    }

    /// Start background token refresh task.
    pub fn start_refresh_task(self: Arc<Self>, source: String, interval: Duration) {
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // Skip immediate tick

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Bearer token refresh task stopping");
                        break;
                    }
                    _ = ticker.tick() => {
                        if let Err(e) = self.load_tokens(&source).await {
                            error!(error = %e, "Failed to refresh bearer tokens");
                            security::SecurityEvent::new(
                                "token.rotated",
                                "bearer_refresh",
                                security::SecurityOutcome::Error,
                            )
                            .reason("refresh_failed")
                            .detail(&e.to_string())
                            .emit();
                        }
                    }
                }
            }
        });
    }

    /// Check if a token is valid (constant-time via hash lookup).
    #[inline]
    pub fn is_valid(&self, token: &str) -> bool {
        let candidate = hash_token(token);
        self.token_hashes.read().contains(&candidate)
    }

    /// Update tokens (for rotation callbacks).
    pub fn update_tokens(&self, tokens: &[SensitiveString]) {
        let hash_set: HashSet<TokenHash> = tokens.iter().map(|t| hash_token(t.expose())).collect();
        let count = hash_set.len();
        info!(count, "Bearer tokens updated");
        *self.token_hashes.write() = hash_set;

        security::token_rotated("bearer_update", &format!("{count} tokens loaded"));
    }

    /// Get current token count.
    pub fn token_count(&self) -> usize {
        self.token_hashes.read().len()
    }

    /// Shutdown the refresh task.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

impl Drop for BearerTokenProvider {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Extract client IP from request headers (respects X-Forwarded-For from trusted proxies).
///
/// Returns the first IP from X-Forwarded-For if present, otherwise X-Real-IP.
/// Returns both a display string (for existing log fields) and a parsed `IpAddr`
/// (for security events).
fn extract_client_ip(headers: &axum::http::HeaderMap) -> (Option<String>, Option<IpAddr>) {
    // X-Forwarded-For may contain multiple IPs: "client, proxy1, proxy2"
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok())
        && let Some(first_ip) = xff.split(',').next()
    {
        let ip_str = first_ip.trim().to_string();
        let parsed = ip_str.parse::<IpAddr>().ok();
        return (Some(ip_str), parsed);
    }
    // Fallback to X-Real-IP
    if let Some(ip_str) = headers
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
    {
        let parsed = ip_str.parse::<IpAddr>().ok();
        return (Some(ip_str), parsed);
    }
    (None, None)
}

/// Probe paths served without authentication.
///
/// `/livez` and `/readyz` are the whole probe surface (see
/// `server::http::build_router`), and both are registered on the same router the
/// auth layer wraps, so the exemption has to live here.
pub const PROBE_PATHS: [&str; 2] = ["/livez", "/readyz"];

/// Record a request refused for its credentials on `transport`.
///
/// Every refusal counts on `receiver_auth_failures_total`. The security event
/// is written at most once per `AUTH_LOG_INTERVAL_MS` per reason, so a
/// credential spray cannot drive the log at the rate it sends.
pub fn record_auth_failure(
    metrics: &Metrics,
    transport: &str,
    reason: AuthFailureReason,
    source_ip: Option<IpAddr>,
    resource: Option<&str>,
) {
    metrics.inc_auth_failure(reason);
    if !log_debounced(&AUTH_FAILURE_LOGGED[reason.index()], AUTH_LOG_INTERVAL_MS) {
        return;
    }
    let mut event = SecurityEvent::new("auth.failure", transport, SecurityOutcome::Failure)
        .reason(reason.label());
    if let Some(ip) = source_ip {
        event = event.source_ip(ip);
    }
    if let Some(resource) = resource {
        event = event.resource(resource);
    }
    event.emit();
}

/// Record a request refused on `transport` for its credentials, when the
/// refusal is the client's; a misconfiguration is the operator's and counts
/// nowhere.
fn record_refusal(
    metrics: &Metrics,
    transport: &str,
    err: &AuthError,
    source_ip: Option<IpAddr>,
    resource: Option<&str>,
) {
    if let Some(reason) = err.reason {
        record_auth_failure(metrics, transport, reason, source_ip, resource);
    }
}

/// Log that a request found the auth configuration unusable, at most once per
/// [`AUTH_LOG_INTERVAL_MS`].
fn log_misconfigured(detail: &str) {
    if log_debounced(&AUTH_MISCONFIGURED_LOGGED, AUTH_LOG_INTERVAL_MS) {
        error!(detail, "authentication is required but cannot be checked");
    }
}

/// What [`token_auth_middleware`] checks a request against, and where it
/// records a refusal.
#[derive(Clone)]
pub struct TokenAuth {
    auth: AuthState,
    metrics: Arc<Metrics>,
    transport: &'static str,
}

impl TokenAuth {
    /// Check requests against `auth`, recording refusals on `metrics` under `transport`.
    #[must_use]
    pub fn new(auth: AuthState, metrics: Arc<Metrics>, transport: &'static str) -> Self {
        Self {
            auth,
            metrics,
            transport,
        }
    }
}

/// A tonic interceptor checking the `authorization` metadata key against
/// `auth`'s bearer tokens, recording refusals on `metrics` under `transport`.
///
/// Header auth never runs here: a gRPC request carries metadata, not the HTTP
/// headers `accepted_headers` names.
pub fn grpc_auth_interceptor(
    auth: AuthState,
    metrics: Arc<Metrics>,
    transport: &'static str,
) -> impl Fn(tonic::Request<()>) -> std::result::Result<tonic::Request<()>, tonic::Status> + Clone {
    move |req: tonic::Request<()>| {
        let mut headers = axum::http::HeaderMap::new();
        if let Some(auth_value) = req.metadata().get("authorization")
            && let Ok(s) = auth_value.to_str()
            && let Ok(hv) = axum::http::HeaderValue::from_str(s)
        {
            headers.insert("authorization", hv);
        }

        if let Some(err) = validate_bearer_auth(&auth, &headers) {
            let peer = req.remote_addr().map(|addr| addr.ip());
            record_refusal(&metrics, transport, &err, peer, None);
            return Err(tonic::Status::unauthenticated(err.message));
        }

        Ok(req)
    }
}

/// Token-based authentication middleware.
///
/// Validates authentication based on the configured mode:
/// - `header`: Static header values
/// - `bearer`: Bearer tokens (static or from secret manager)
/// - `both`: Requires both token auth and mTLS
///
/// [`PROBE_PATHS`] are exempt: they carry no data and kubelet cannot
/// authenticate.
///
/// A refusal is recorded by [`record_auth_failure`].
pub async fn token_auth_middleware(
    State(gate): State<TokenAuth>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let auth = &gate.auth;
    let mode = AuthMode::from_str(&auth.config.mode);

    // Kubelet does not send credentials, so an authenticated probe path fails
    // liveness and crashloops a pod whose service is perfectly healthy. The
    // exemption is exact-match on the two registered routes -- `path()` excludes
    // the query string, and a prefix match would also exempt anything nested
    // under those names.
    if PROBE_PATHS.contains(&request.uri().path()) {
        return next.run(request).await;
    }

    // Skip if auth mode doesn't require token auth
    if !mode.requires_token_auth() {
        return next.run(request).await;
    }

    // Check authentication based on mode
    let auth_result = match mode {
        AuthMode::Bearer => validate_bearer_auth(auth, request.headers()),
        AuthMode::Header | AuthMode::Both => {
            // Try bearer first if provider is configured, then fall back to header
            if auth.bearer_provider.is_some() {
                match validate_bearer_auth(auth, request.headers()) {
                    None => None,
                    Some(_) => validate_header_auth(&auth.config, request.headers()),
                }
            } else {
                validate_header_auth(&auth.config, request.headers())
            }
        }
        _ => None,
    };

    match auth_result {
        None => next.run(request).await,
        Some(err) => {
            let (client_ip_str, header_ip) = extract_client_ip(request.headers());
            let peer = request
                .extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .map(|ConnectInfo(addr)| addr.ip());
            debug!(
                transport = gate.transport,
                client_ip = client_ip_str.as_deref().unwrap_or("unknown"),
                auth_mode = ?mode,
                failure_reason = %err.message,
                status_code = err.status.as_u16(),
                "auth_failure"
            );
            record_refusal(
                &gate.metrics,
                gate.transport,
                &err,
                header_ip.or(peer),
                Some(request.uri().path()),
            );
            err.into_response()
        }
    }
}

/// Validate bearer token authentication.
///
/// Checks the `Authorization: Bearer <token>` header against valid tokens.
/// Returns `None` if authentication passes, `Some(AuthError)` on failure.
///
/// A missing provider is a misconfiguration, not a pass. `create_auth_state`
/// only builds a provider when `bearer.tokens` is non-empty or
/// `bearer.secret_source` is set, so `mode: bearer` with neither leaves
/// `bearer_provider` at `None`. `None` from this function means "no
/// objection", which the middleware serves as authenticated -- so a missing
/// provider must report an error instead. Mirrors what `validate_header_auth`
/// does for an empty accepted-headers list.
///
/// The `Header`/`Both` branch of `token_auth_middleware` only calls this when
/// `bearer_provider.is_some()`, so header auth is unaffected.
#[inline]
pub fn validate_bearer_auth(
    auth: &AuthState,
    headers: &axum::http::HeaderMap,
) -> Option<AuthError> {
    let Some(ref provider) = auth.bearer_provider else {
        log_misconfigured("bearer auth is required but no token provider is configured");
        return Some(AuthError::misconfigured());
    };

    // Check for Authorization header
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    let Some(auth_value) = auth_header else {
        return Some(AuthError::refused(
            AuthFailureReason::MissingHeader,
            "missing_authorization_header",
        ));
    };

    // Parse "Bearer <token>" or "Splunk <token>" format
    let token = if let Some(token) = auth_value.strip_prefix("Bearer ") {
        token.trim()
    } else if let Some(token) = auth_value.strip_prefix("bearer ") {
        token.trim()
    } else if let Some(token) = auth_value.strip_prefix("Splunk ") {
        token.trim()
    } else if let Some(token) = auth_value.strip_prefix("splunk ") {
        token.trim()
    } else {
        return Some(AuthError::refused(
            AuthFailureReason::InvalidToken,
            "invalid_bearer_format",
        ));
    };

    // Validate token
    if provider.is_valid(token) {
        debug!("Bearer token accepted");
        None
    } else {
        Some(AuthError::refused(
            AuthFailureReason::InvalidToken,
            "invalid_bearer_token",
        ))
    }
}

/// Authentication error for explicit error handling.
#[derive(Debug)]
pub struct AuthError {
    pub status: StatusCode,
    pub message: String,
    /// Why the client's credentials failed; `None` when the configuration is
    /// what failed, which is the operator's to fix and not a client refusal.
    pub reason: Option<AuthFailureReason>,
}

impl AuthError {
    /// A 401 for credentials that failed for `reason`.
    fn refused(reason: AuthFailureReason, message: &str) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.to_string(),
            reason: Some(reason),
        }
    }

    /// A 500 for auth the configuration asks for and cannot check.
    fn misconfigured() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "server_misconfigured".to_string(),
            reason: None,
        }
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

/// Validate header authentication against accepted headers.
///
/// Returns `None` if authentication passes (any accepted header matches).
/// Returns `Some(AuthError)` if no accepted header is valid.
#[inline]
pub fn validate_header_auth(
    config: &AuthConfig,
    headers: &axum::http::HeaderMap,
) -> Option<AuthError> {
    let mode = AuthMode::from_str(&config.mode);

    // Skip if auth mode doesn't require headers
    if mode == AuthMode::None || mode == AuthMode::Mtls {
        return None;
    }

    let accepted = config.effective_headers();

    // If no headers configured, reject
    if accepted.is_empty() {
        log_misconfigured("header auth is required but no accepted headers are configured");
        return Some(AuthError::misconfigured());
    }

    // Check each accepted header - any valid one passes
    for accepted_header in &accepted {
        if let Some(header_value) = headers
            .get(&accepted_header.name)
            .and_then(|v| v.to_str().ok())
        {
            // If values list is empty, any non-empty value is accepted
            if accepted_header.values.is_empty() {
                debug!(header = %accepted_header.name, "Auth header accepted (any value)");
                return None;
            }

            // Compared as SHA-256 hashes, as bearer tokens are, so the time taken
            // says nothing about how much of a secret value matched.
            let offered = hash_token(header_value);
            if accepted_header
                .values
                .iter()
                .any(|allowed| hash_token(allowed.expose()) == offered)
            {
                debug!(header = %accepted_header.name, "Auth header accepted");
                return None;
            }
        }
    }

    // Determine failure reason for metrics/logging
    // Check if any expected header is present (but with wrong value)
    let has_header = accepted.iter().any(|h| headers.contains_key(&h.name));

    Some(if has_header {
        AuthError::refused(AuthFailureReason::InvalidHeader, "invalid_header_value")
    } else {
        AuthError::refused(AuthFailureReason::MissingHeader, "missing_auth_header")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AcceptedHeader, BearerConfig};
    use axum::http::HeaderMap;

    fn test_config() -> AuthConfig {
        AuthConfig {
            mode: "header".to_string(),
            accepted_headers: vec![AcceptedHeader {
                name: "x-api-key".to_string(),
                values: vec!["valid-key".into(), "another-key".into()],
            }],
            bearer: BearerConfig::default(),
            include_common_header: false,
            header_name: String::new(),
            header_values: Vec::new(),
        }
    }

    fn multi_header_config() -> AuthConfig {
        AuthConfig {
            mode: "header".to_string(),
            accepted_headers: vec![
                AcceptedHeader {
                    name: "x-api-key".to_string(),
                    values: vec!["api-secret".into()],
                },
                AcceptedHeader {
                    name: "authorization".to_string(),
                    values: vec!["Bearer token123".into()],
                },
                AcceptedHeader {
                    name: "x-custom-auth".to_string(),
                    values: vec![], // Any value accepted
                },
            ],
            bearer: BearerConfig::default(),
            include_common_header: false,
            header_name: String::new(),
            header_values: Vec::new(),
        }
    }

    fn bearer_config() -> AuthConfig {
        AuthConfig {
            mode: "bearer".to_string(),
            accepted_headers: vec![],
            bearer: BearerConfig {
                tokens: vec!["secret-token-1".into(), "secret-token-2".into()],
                secret_source: None,
                refresh_interval_secs: 300,
            },
            include_common_header: false,
            header_name: String::new(),
            header_values: Vec::new(),
        }
    }

    #[test]
    fn test_auth_mode_parsing() {
        assert_eq!(AuthMode::from_str("none"), AuthMode::None);
        assert_eq!(AuthMode::from_str("header"), AuthMode::Header);
        assert_eq!(AuthMode::from_str("mtls"), AuthMode::Mtls);
        assert_eq!(AuthMode::from_str("both"), AuthMode::Both);
        assert_eq!(AuthMode::from_str("HEADER"), AuthMode::Header);
        assert_eq!(AuthMode::from_str("unknown"), AuthMode::None);
    }

    #[test]
    fn test_validate_header_auth_none_mode() {
        let mut config = test_config();
        config.mode = "none".to_string();

        let headers = HeaderMap::new();
        assert!(validate_header_auth(&config, &headers).is_none());
    }

    #[test]
    fn test_validate_header_auth_valid() {
        let config = test_config();

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "valid-key".parse().unwrap());

        assert!(validate_header_auth(&config, &headers).is_none());
    }

    #[test]
    fn test_validate_header_auth_invalid_value() {
        let config = test_config();

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "wrong-key".parse().unwrap());

        let err = validate_header_auth(&config, &headers);
        assert!(err.is_some());
        assert_eq!(err.unwrap().status, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_validate_header_auth_missing() {
        let config = test_config();
        let headers = HeaderMap::new();

        let err = validate_header_auth(&config, &headers);
        assert!(err.is_some());
        assert_eq!(err.unwrap().status, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_validate_header_auth_mtls_mode() {
        let mut config = test_config();
        config.mode = "mtls".to_string();

        // Should skip header check for mtls-only mode
        let headers = HeaderMap::new();
        assert!(validate_header_auth(&config, &headers).is_none());
    }

    #[test]
    fn test_validate_header_auth_both_mode() {
        let mut config = test_config();
        config.mode = "both".to_string();

        // Should require header for both mode
        let headers = HeaderMap::new();
        let err = validate_header_auth(&config, &headers);
        assert!(err.is_some());

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "valid-key".parse().unwrap());
        assert!(validate_header_auth(&config, &headers).is_none());
    }

    #[test]
    fn test_multi_header_first_matches() {
        let config = multi_header_config();

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "api-secret".parse().unwrap());

        assert!(validate_header_auth(&config, &headers).is_none());
    }

    #[test]
    fn test_multi_header_second_matches() {
        let config = multi_header_config();

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token123".parse().unwrap());

        assert!(validate_header_auth(&config, &headers).is_none());
    }

    #[test]
    fn test_multi_header_any_value_accepted() {
        let config = multi_header_config();

        // x-custom-auth accepts any value
        let mut headers = HeaderMap::new();
        headers.insert("x-custom-auth", "literally-anything".parse().unwrap());

        assert!(validate_header_auth(&config, &headers).is_none());
    }

    #[test]
    fn test_multi_header_none_match() {
        let config = multi_header_config();

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "wrong-value".parse().unwrap());

        let err = validate_header_auth(&config, &headers);
        assert!(err.is_some());
        assert_eq!(err.unwrap().status, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_legacy_header_config() {
        // Test backwards compatibility with legacy single header config
        let config = AuthConfig {
            mode: "header".to_string(),
            accepted_headers: vec![],
            bearer: BearerConfig::default(),
            include_common_header: false,
            header_name: "x-legacy-header".to_string(),
            header_values: vec!["legacy-value".into()],
        };

        let mut headers = HeaderMap::new();
        headers.insert("x-legacy-header", "legacy-value".parse().unwrap());

        assert!(validate_header_auth(&config, &headers).is_none());
    }

    #[test]
    fn test_effective_headers_merges_legacy() {
        let config = AuthConfig {
            mode: "header".to_string(),
            accepted_headers: vec![AcceptedHeader {
                name: "x-new-header".to_string(),
                values: vec!["new-value".into()],
            }],
            bearer: BearerConfig::default(),
            include_common_header: false,
            header_name: "x-legacy-header".to_string(),
            header_values: vec!["legacy-value".into()],
        };

        let effective = config.effective_headers();
        assert_eq!(effective.len(), 2);
        assert!(effective.iter().any(|h| h.name == "x-new-header"));
        assert!(effective.iter().any(|h| h.name == "x-legacy-header"));
    }

    // Bearer token tests

    #[test]
    fn test_auth_mode_bearer() {
        assert_eq!(AuthMode::from_str("bearer"), AuthMode::Bearer);
        assert!(AuthMode::Bearer.requires_token_auth());
    }

    #[test]
    fn test_bearer_provider_new() {
        let tokens: Vec<SensitiveString> = vec!["token1".into(), "token2".into()];
        let provider = BearerTokenProvider::new(&tokens);

        assert_eq!(provider.token_count(), 2);
        assert!(provider.is_valid("token1"));
        assert!(provider.is_valid("token2"));
        assert!(!provider.is_valid("invalid"));
    }

    #[test]
    fn test_bearer_provider_update_tokens() {
        let provider = BearerTokenProvider::new(&["old-token".into()]);
        assert!(provider.is_valid("old-token"));
        assert!(!provider.is_valid("new-token"));

        provider.update_tokens(&["new-token".into()]);
        assert!(!provider.is_valid("old-token"));
        assert!(provider.is_valid("new-token"));
    }

    #[test]
    fn test_validate_bearer_auth_valid() {
        let config = bearer_config();
        let provider = BearerTokenProvider::new(&config.bearer.tokens);
        let auth = AuthState::with_bearer_provider(config, provider);

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token-1".parse().unwrap());

        assert!(validate_bearer_auth(&auth, &headers).is_none());
    }

    #[test]
    fn test_validate_bearer_auth_lowercase_bearer() {
        let config = bearer_config();
        let provider = BearerTokenProvider::new(&config.bearer.tokens);
        let auth = AuthState::with_bearer_provider(config, provider);

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "bearer secret-token-2".parse().unwrap());

        assert!(validate_bearer_auth(&auth, &headers).is_none());
    }

    #[test]
    fn test_validate_bearer_auth_invalid_token() {
        let config = bearer_config();
        let provider = BearerTokenProvider::new(&config.bearer.tokens);
        let auth = AuthState::with_bearer_provider(config, provider);

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer wrong-token".parse().unwrap());

        let err = validate_bearer_auth(&auth, &headers);
        assert!(err.is_some());
        assert_eq!(err.unwrap().status, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_validate_bearer_auth_missing_header() {
        let config = bearer_config();
        let provider = BearerTokenProvider::new(&config.bearer.tokens);
        let auth = AuthState::with_bearer_provider(config, provider);

        let headers = HeaderMap::new();

        let err = validate_bearer_auth(&auth, &headers);
        assert!(err.is_some());
        assert_eq!(err.unwrap().message, "missing_authorization_header");
    }

    #[test]
    fn test_validate_bearer_auth_wrong_format() {
        let config = bearer_config();
        let provider = BearerTokenProvider::new(&config.bearer.tokens);
        let auth = AuthState::with_bearer_provider(config, provider);

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic dXNlcjpwYXNz".parse().unwrap());

        let err = validate_bearer_auth(&auth, &headers);
        assert!(err.is_some());
        assert_eq!(err.unwrap().message, "invalid_bearer_format");
    }

    #[test]
    fn test_validate_bearer_auth_no_provider_is_misconfiguration() {
        let config = bearer_config();
        let auth = AuthState::new(config);

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer any-token".parse().unwrap());

        // A missing provider under bearer mode must be reported, not skipped:
        // the middleware reads "no objection" as "authenticated".
        let err = validate_bearer_auth(&auth, &headers)
            .expect("missing bearer provider must not be treated as a pass");
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.message, "server_misconfigured");
    }

    #[test]
    fn test_validate_bearer_auth_no_provider_no_header() {
        // No provider and no header: still an error, not a pass.
        let config = bearer_config();
        let auth = AuthState::new(config);
        let headers = HeaderMap::new();

        assert!(
            validate_bearer_auth(&auth, &headers).is_some(),
            "missing bearer provider must not be treated as a pass"
        );
    }

    // Common header tests

    #[test]
    fn test_auth_config_partial_eq() {
        let config1 = AuthConfig::default();
        let config2 = AuthConfig::default();
        assert_eq!(config1, config2);

        let config3 = AuthConfig {
            mode: "bearer".to_string(),
            ..AuthConfig::default()
        };
        assert_ne!(config1, config3);
    }

    #[tokio::test]
    async fn test_bearer_token_update() {
        let provider = BearerTokenProvider::new(&["old_token".into()]);
        assert!(provider.is_valid("old_token"));
        assert!(!provider.is_valid("new_token"));

        provider.update_tokens(&["new_token".into()]);
        assert!(provider.is_valid("new_token"));
        assert!(!provider.is_valid("old_token"));
    }

    #[tokio::test]
    async fn test_auth_reload_updates_tokens() {
        use crate::config::{Config, SharedConfig};

        let config = Config::default();
        let shared = SharedConfig::new(config);

        // Simulate initial token load
        let provider = BearerTokenProvider::new(&["initial".into()]);
        assert!(provider.is_valid("initial"));

        // Simulate what the reload watcher does on config change
        let new_config = shared.get();
        let new_tokens: Vec<SensitiveString> = vec!["rotated".into()];
        provider.update_tokens(&new_tokens);
        assert!(provider.is_valid("rotated"));
        assert!(!provider.is_valid("initial"));

        // Verify shared config exposes auth section
        assert_eq!(new_config.server.auth.mode, "none");
    }

    #[test]
    fn test_include_common_header_default() {
        let config = AuthConfig::default();
        assert!(config.include_common_header);
        let effective = config.effective_headers();
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].name, "x-hyperi-agent");
        assert_eq!(effective[0].values, vec![SensitiveString::new("1.0")]);
    }

    #[test]
    fn test_include_common_header_disabled() {
        let config = AuthConfig {
            include_common_header: false,
            ..AuthConfig::default()
        };
        let effective = config.effective_headers();
        assert!(effective.is_empty());
    }

    #[test]
    fn test_include_common_header_not_duplicated() {
        let config = AuthConfig {
            include_common_header: true,
            accepted_headers: vec![AcceptedHeader {
                name: "x-hyperi-agent".to_string(),
                values: vec!["2.0".into()],
            }],
            ..AuthConfig::default()
        };
        let effective = config.effective_headers();
        // Should not add a second x-hyperi-agent
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].values, vec![SensitiveString::new("2.0")]);
    }

    // ---------------------------------------------------------------------
    // Security: constant-time token validation
    // ---------------------------------------------------------------------

    #[test]
    fn test_hash_token_deterministic() {
        // Same input must always produce same hash (required for lookup)
        let h1 = hash_token("my-secret-token");
        let h2 = hash_token("my-secret-token");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_token_distinct() {
        // Different inputs must produce different hashes
        let h1 = hash_token("token-a");
        let h2 = hash_token("token-b");
        assert_ne!(h1, h2);

        // Even single-byte differences must differ
        let h3 = hash_token("secret");
        let h4 = hash_token("Secret");
        assert_ne!(h3, h4);
    }

    #[test]
    fn test_hash_token_empty_and_unicode() {
        // Empty string must produce a stable hash
        let h_empty = hash_token("");
        assert_eq!(h_empty, hash_token(""));

        // Unicode must be handled correctly (UTF-8 bytes)
        let h_unicode = hash_token("токен-тест-🔒");
        assert_eq!(h_unicode, hash_token("токен-тест-🔒"));
        assert_ne!(h_unicode, h_empty);

        // Very long tokens (e.g. JWTs) must hash without issue
        let long_token = "a".repeat(4096);
        let h_long = hash_token(&long_token);
        assert_eq!(h_long.len(), 32);
    }

    #[test]
    fn test_bearer_validation_timing_attack_resistant() {
        // With hash-based lookup, the timing of validation should not
        // depend on how many characters of the token match a stored one.
        // This is a structural test: verify plaintext is never compared.
        let provider = BearerTokenProvider::new(&["aaaaaaaaaaaaaaaaaaaa".into()]);

        // These tokens all share a prefix with the valid token but
        // must all be rejected in uniform time (hash lookup).
        let near_matches = vec![
            String::new(),
            "a".to_string(),
            "aaaaaaaaaa".to_string(),            // half prefix
            "aaaaaaaaaaaaaaaaaaa".to_string(),   // missing last char
            "aaaaaaaaaaaaaaaaaaab".to_string(),  // last char wrong
            "aaaaaaaaaaaaaaaaaaaax".to_string(), // too long
        ];
        for t in near_matches {
            assert!(!provider.is_valid(&t), "should reject: {t:?}");
        }
        assert!(provider.is_valid("aaaaaaaaaaaaaaaaaaaa"));
    }

    #[test]
    fn test_bearer_provider_duplicate_tokens_deduped() {
        // Multiple identical tokens should collapse into one hash entry
        let tokens: Vec<SensitiveString> = vec![
            "same".into(),
            "same".into(),
            "same".into(),
            "different".into(),
        ];
        let provider = BearerTokenProvider::new(&tokens);
        assert_eq!(provider.token_count(), 2);
        assert!(provider.is_valid("same"));
        assert!(provider.is_valid("different"));
    }

    #[test]
    fn test_bearer_provider_empty_token_never_valid() {
        // Empty token must never authenticate (common injection/misconfig)
        let provider = BearerTokenProvider::new(&["real-token".into()]);
        assert!(!provider.is_valid(""));
    }

    #[test]
    fn test_bearer_provider_update_clears_old_tokens() {
        // Rotation must fully replace prior set (no leakage of old tokens)
        let provider = BearerTokenProvider::new(&["a".into(), "b".into(), "c".into()]);
        assert_eq!(provider.token_count(), 3);

        provider.update_tokens(&["d".into()]);
        assert_eq!(provider.token_count(), 1);
        assert!(!provider.is_valid("a"));
        assert!(!provider.is_valid("b"));
        assert!(!provider.is_valid("c"));
        assert!(provider.is_valid("d"));

        // Update to empty set revokes all
        provider.update_tokens(&[]);
        assert_eq!(provider.token_count(), 0);
        assert!(!provider.is_valid("d"));
    }

    /// Every line a test subscriber writes.
    #[derive(Clone, Default)]
    struct Captured(Arc<parking_lot::Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// A spray of refused requests writes one log line and counts every refusal.
    #[tokio::test(flavor = "current_thread")]
    async fn a_credential_spray_logs_one_line_and_counts_every_attempt() {
        let captured = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _default = tracing::subscriber::set_default(subscriber);
        AUTH_FAILURE_LOGGED[AuthFailureReason::MissingHeader.index()]
            .store(0, std::sync::atomic::Ordering::Relaxed);

        let metrics = Arc::new(Metrics::default());
        let app = axum::Router::new()
            .route("/ingest", axum::routing::post(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                TokenAuth::new(AuthState::new(test_config()), metrics.clone(), "http"),
                token_auth_middleware,
            ));
        for _ in 0..50 {
            let request = Request::post("/ingest").body(Body::empty()).unwrap();
            let response = tower::ServiceExt::oneshot(app.clone(), request)
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }

        let log = String::from_utf8(captured.0.lock().clone()).unwrap();
        assert_eq!(
            log.lines().count(),
            1,
            "one line for the whole spray: {log}"
        );
        assert!(log.contains("auth.failure"), "{log}");
        assert!(log.contains("missing_header"), "{log}");
        assert_eq!(metrics.get_auth_failures_total(), 50);
    }

    /// A configuration that cannot check credentials is the operator's fault:
    /// it answers 500 and counts no client failure.
    #[test]
    fn a_misconfiguration_is_not_a_client_auth_failure() {
        let err = validate_bearer_auth(&AuthState::new(bearer_config()), &HeaderMap::new())
            .expect("a missing provider refuses");
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(err.reason.is_none());

        let metrics = Metrics::default();
        record_refusal(&metrics, "http", &err, None, None);
        assert_eq!(metrics.get_auth_failures_total(), 0);
    }
}
