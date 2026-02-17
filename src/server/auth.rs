// Project:   dfe-receiver
// File:      src/server/auth.rs
// Purpose:   Authentication middleware
// Language:  Rust
//
// License:   FSL-1.1-ALv2
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
//! - OpenBao/Vault via hyperi-rustlib secrets
//! - AWS Secrets Manager
//! - File (K8s secrets mounted as files)

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::config::{AuthConfig, BearerConfig};
use crate::error::Result;

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

/// Bearer token provider with dynamic secret loading.
///
/// Supports loading tokens from:
/// - Static configuration
/// - OpenBao/Vault
/// - AWS Secrets Manager
/// - Files (K8s secrets)
pub struct BearerTokenProvider {
    /// Current valid tokens (thread-safe for hot reloading).
    tokens: RwLock<HashSet<String>>,
    /// Shutdown signal for background refresh.
    shutdown_tx: broadcast::Sender<()>,
}

impl BearerTokenProvider {
    /// Create a new bearer token provider with static tokens.
    pub fn new(tokens: Vec<String>) -> Self {
        let token_set: HashSet<String> = tokens.into_iter().collect();
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            tokens: RwLock::new(token_set),
            shutdown_tx,
        }
    }

    /// Create a bearer token provider from configuration.
    ///
    /// If `secret_source` is configured, tokens will be loaded dynamically.
    /// Otherwise, static tokens from config are used.
    pub async fn from_config(config: &BearerConfig) -> Result<Self> {
        let provider = Self::new(config.tokens.clone());

        // If secret source is configured, load tokens from secret manager
        if let Some(ref source) = config.secret_source {
            if let Err(e) = provider.load_from_secret(source).await {
                warn!(error = %e, source = %source, "Failed to load bearer tokens from secret, using static tokens");
            }
        }

        Ok(provider)
    }

    /// Load tokens from a secret source.
    ///
    /// Format: "provider:path" or "provider:path:key"
    ///
    /// Supported providers:
    /// - `file`: Load from file path (e.g., "file:/etc/secrets/tokens")
    /// - `vault` or `openbao`: Load from Vault/OpenBao (e.g., "vault:secret/data/auth:tokens")
    /// - `aws`: Load from AWS Secrets Manager (e.g., "aws:prod/auth/tokens:bearer")
    ///
    /// For vault/aws providers, you need to configure the provider credentials in
    /// the application secrets config.
    async fn load_from_secret(&self, source: &str) -> Result<()> {
        let parts: Vec<&str> = source.splitn(3, ':').collect();
        if parts.len() < 2 {
            return Err(crate::error::Error::Config(format!(
                "Invalid secret source format: {source}. Expected 'provider:path' or 'provider:path:key'"
            )));
        }

        let provider_name = parts[0];
        let path = parts[1];
        let key = parts.get(2).copied();

        // Use hyperi-rustlib secrets manager
        use hyperi_rustlib::secrets::{SecretSource, SecretsConfig, SecretsManager};

        // Build the secret source based on provider
        let secret_source = match provider_name {
            "file" => SecretSource::File {
                path: path.to_string(),
            },
            "vault" | "openbao" => SecretSource::OpenBao {
                path: path.to_string(),
                key: key.unwrap_or("value").to_string(),
            },
            "aws" => SecretSource::Aws {
                secret_id: path.to_string(),
                key: key.map(String::from),
            },
            _ => {
                return Err(crate::error::Error::Config(format!(
                    "Unknown secret provider: {provider_name}. Supported: file, vault, openbao, aws"
                )));
            }
        };

        // Configure secrets manager with this source
        let config = SecretsConfig {
            sources: [("bearer_tokens".into(), secret_source)]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let secrets = SecretsManager::new(config)?;

        // For file sources, use get_file directly for simplicity
        let secret_value = if provider_name == "file" {
            secrets.get_file(path).await?
        } else {
            secrets.get("bearer_tokens").await?
        };

        // Parse tokens (newline or comma separated)
        let content = secret_value.as_str()?;
        let new_tokens: HashSet<String> = content
            .lines()
            .flat_map(|line| line.split(','))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        info!(count = new_tokens.len(), source = %source, "Loaded bearer tokens from secret");
        *self.tokens.write() = new_tokens;

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
                        if let Err(e) = self.load_from_secret(&source).await {
                            error!(error = %e, "Failed to refresh bearer tokens");
                        }
                    }
                }
            }
        });
    }

    /// Check if a token is valid.
    #[inline]
    pub fn is_valid(&self, token: &str) -> bool {
        self.tokens.read().contains(token)
    }

    /// Update tokens (for rotation callbacks).
    pub fn update_tokens(&self, tokens: Vec<String>) {
        let token_set: HashSet<String> = tokens.into_iter().collect();
        info!(count = token_set.len(), "Bearer tokens updated");
        *self.tokens.write() = token_set;
    }

    /// Get current token count.
    pub fn token_count(&self) -> usize {
        self.tokens.read().len()
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
fn extract_client_ip(headers: &axum::http::HeaderMap) -> Option<String> {
    // X-Forwarded-For may contain multiple IPs: "client, proxy1, proxy2"
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        if let Some(first_ip) = xff.split(',').next() {
            return Some(first_ip.trim().to_string());
        }
    }
    // Fallback to X-Real-IP
    headers
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
}

/// Token-based authentication middleware.
///
/// Validates authentication based on the configured mode:
/// - `header`: Static header values
/// - `bearer`: Bearer tokens (static or from secret manager)
/// - `both`: Requires both token auth and mTLS
///
/// Logs failures at WARN level with structured fields for security monitoring.
pub async fn token_auth_middleware(
    State(auth): State<AuthState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let mode = AuthMode::from_str(&auth.config.mode);

    // Skip if auth mode doesn't require token auth
    if !mode.requires_token_auth() {
        return next.run(request).await;
    }

    // Extract client IP for logging (before consuming request)
    let client_ip = extract_client_ip(request.headers());

    // Check authentication based on mode
    let auth_result = match mode {
        AuthMode::Bearer => validate_bearer_auth(&auth, request.headers()),
        AuthMode::Header | AuthMode::Both => {
            // Try bearer first if provider is configured, then fall back to header
            if auth.bearer_provider.is_some() {
                match validate_bearer_auth(&auth, request.headers()) {
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
            // Log auth failure with structured fields for security monitoring
            // This uses WARN level - high enough to be captured in production,
            // but not ERROR (which would trigger alerts for expected traffic)
            warn!(
                client_ip = client_ip.as_deref().unwrap_or("unknown"),
                auth_mode = ?mode,
                failure_reason = %err.message,
                status_code = err.status.as_u16(),
                "auth_failure"
            );
            err.into_response()
        }
    }
}

/// Validate bearer token authentication.
///
/// Checks the `Authorization: Bearer <token>` header against valid tokens.
/// Returns `None` if authentication passes, `Some(AuthError)` on failure.
#[inline]
pub fn validate_bearer_auth(
    auth: &AuthState,
    headers: &axum::http::HeaderMap,
) -> Option<AuthError> {
    let Some(ref provider) = auth.bearer_provider else {
        // No bearer provider configured, skip bearer auth
        return None;
    };

    // Check for Authorization header
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    let Some(auth_value) = auth_header else {
        return Some(AuthError {
            status: StatusCode::UNAUTHORIZED,
            message: "missing_authorization_header".into(),
        });
    };

    // Parse "Bearer <token>" format
    let token = if let Some(token) = auth_value.strip_prefix("Bearer ") {
        token.trim()
    } else if let Some(token) = auth_value.strip_prefix("bearer ") {
        token.trim()
    } else {
        return Some(AuthError {
            status: StatusCode::UNAUTHORIZED,
            message: "invalid_bearer_format".into(),
        });
    };

    // Validate token
    if provider.is_valid(token) {
        debug!("Bearer token accepted");
        None
    } else {
        Some(AuthError {
            status: StatusCode::UNAUTHORIZED,
            message: "invalid_bearer_token".into(),
        })
    }
}

/// Authentication error for explicit error handling.
#[derive(Debug)]
pub struct AuthError {
    pub status: StatusCode,
    pub message: String,
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
        error!("No accepted headers configured but header auth required");
        return Some(AuthError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "server_misconfigured".into(),
        });
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

            // Check if value is in allowed list
            if accepted_header.values.contains(&header_value.to_string()) {
                debug!(header = %accepted_header.name, "Auth header accepted");
                return None;
            }
        }
    }

    // Determine failure reason for metrics/logging
    // Check if any expected header is present (but with wrong value)
    let has_header = accepted.iter().any(|h| headers.contains_key(&h.name));

    Some(AuthError {
        status: StatusCode::UNAUTHORIZED,
        message: if has_header {
            "invalid_header_value".into()
        } else {
            "missing_auth_header".into()
        },
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
                values: vec!["valid-key".to_string(), "another-key".to_string()],
            }],
            bearer: BearerConfig::default(),
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
                    values: vec!["api-secret".to_string()],
                },
                AcceptedHeader {
                    name: "authorization".to_string(),
                    values: vec!["Bearer token123".to_string()],
                },
                AcceptedHeader {
                    name: "x-custom-auth".to_string(),
                    values: vec![], // Any value accepted
                },
            ],
            bearer: BearerConfig::default(),
            header_name: String::new(),
            header_values: Vec::new(),
        }
    }

    fn bearer_config() -> AuthConfig {
        AuthConfig {
            mode: "bearer".to_string(),
            accepted_headers: vec![],
            bearer: BearerConfig {
                tokens: vec!["secret-token-1".to_string(), "secret-token-2".to_string()],
                secret_source: None,
                refresh_interval_secs: 300,
            },
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
            header_name: "x-legacy-header".to_string(),
            header_values: vec!["legacy-value".to_string()],
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
                values: vec!["new-value".to_string()],
            }],
            bearer: BearerConfig::default(),
            header_name: "x-legacy-header".to_string(),
            header_values: vec!["legacy-value".to_string()],
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
        let tokens = vec!["token1".to_string(), "token2".to_string()];
        let provider = BearerTokenProvider::new(tokens);

        assert_eq!(provider.token_count(), 2);
        assert!(provider.is_valid("token1"));
        assert!(provider.is_valid("token2"));
        assert!(!provider.is_valid("invalid"));
    }

    #[test]
    fn test_bearer_provider_update_tokens() {
        let provider = BearerTokenProvider::new(vec!["old-token".to_string()]);
        assert!(provider.is_valid("old-token"));
        assert!(!provider.is_valid("new-token"));

        provider.update_tokens(vec!["new-token".to_string()]);
        assert!(!provider.is_valid("old-token"));
        assert!(provider.is_valid("new-token"));
    }

    #[test]
    fn test_validate_bearer_auth_valid() {
        let config = bearer_config();
        let provider = BearerTokenProvider::new(config.bearer.tokens.clone());
        let auth = AuthState::with_bearer_provider(config, provider);

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token-1".parse().unwrap());

        assert!(validate_bearer_auth(&auth, &headers).is_none());
    }

    #[test]
    fn test_validate_bearer_auth_lowercase_bearer() {
        let config = bearer_config();
        let provider = BearerTokenProvider::new(config.bearer.tokens.clone());
        let auth = AuthState::with_bearer_provider(config, provider);

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "bearer secret-token-2".parse().unwrap());

        assert!(validate_bearer_auth(&auth, &headers).is_none());
    }

    #[test]
    fn test_validate_bearer_auth_invalid_token() {
        let config = bearer_config();
        let provider = BearerTokenProvider::new(config.bearer.tokens.clone());
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
        let provider = BearerTokenProvider::new(config.bearer.tokens.clone());
        let auth = AuthState::with_bearer_provider(config, provider);

        let headers = HeaderMap::new();

        let err = validate_bearer_auth(&auth, &headers);
        assert!(err.is_some());
        assert_eq!(err.unwrap().message, "missing_authorization_header");
    }

    #[test]
    fn test_validate_bearer_auth_wrong_format() {
        let config = bearer_config();
        let provider = BearerTokenProvider::new(config.bearer.tokens.clone());
        let auth = AuthState::with_bearer_provider(config, provider);

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic dXNlcjpwYXNz".parse().unwrap());

        let err = validate_bearer_auth(&auth, &headers);
        assert!(err.is_some());
        assert_eq!(err.unwrap().message, "invalid_bearer_format");
    }

    #[test]
    fn test_validate_bearer_auth_no_provider() {
        let config = bearer_config();
        let auth = AuthState::new(config);

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer any-token".parse().unwrap());

        // No provider configured, should skip bearer auth
        assert!(validate_bearer_auth(&auth, &headers).is_none());
    }
}
