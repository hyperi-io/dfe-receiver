// Project:   dfe-receiver
// File:      src/server/auth.rs
// Purpose:   Authentication middleware
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

//! Authentication middleware for header and mTLS validation.
//!
//! Supports multiple accepted headers - any one valid header passes authentication.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tracing::{debug, warn};

use crate::config::AuthConfig;

/// Authentication mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    /// No authentication required.
    None,
    /// Header-based authentication.
    Header,
    /// mTLS client certificate authentication.
    Mtls,
    /// Both header and mTLS required.
    Both,
}

impl AuthMode {
    /// Parse from string.
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "header" => Self::Header,
            "mtls" => Self::Mtls,
            "both" => Self::Both,
            _ => Self::None,
        }
    }
}

/// Shared authentication state.
#[derive(Clone)]
pub struct AuthState {
    pub config: Arc<AuthConfig>,
}

impl AuthState {
    /// Create new auth state from config.
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

/// Header-based authentication middleware.
///
/// Validates that at least one of the configured headers is present with an allowed value.
pub async fn header_auth_middleware(
    State(auth): State<AuthState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let mode = AuthMode::from_str(&auth.config.mode);

    // Skip if auth mode doesn't require headers
    if mode == AuthMode::None || mode == AuthMode::Mtls {
        return next.run(request).await;
    }

    // Check accepted headers
    match validate_header_auth(&auth.config, request.headers()) {
        None => next.run(request).await,
        Some(err) => err.into_response(),
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
pub fn validate_header_auth(config: &AuthConfig, headers: &axum::http::HeaderMap) -> Option<AuthError> {
    let mode = AuthMode::from_str(&config.mode);

    // Skip if auth mode doesn't require headers
    if mode == AuthMode::None || mode == AuthMode::Mtls {
        return None;
    }

    let accepted = config.effective_headers();

    // If no headers configured, reject
    if accepted.is_empty() {
        warn!("No accepted headers configured but header auth required");
        return Some(AuthError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "Server misconfigured".into(),
        });
    }

    // Check each accepted header - any valid one passes
    for accepted_header in &accepted {
        if let Some(header_value) = headers.get(&accepted_header.name).and_then(|v| v.to_str().ok()) {
            // If values list is empty, any non-empty value is accepted
            if accepted_header.values.is_empty() {
                debug!(header = %accepted_header.name, "Auth header accepted (any value)");
                return None;
            }

            // Check if value is in allowed list
            if accepted_header.values.contains(&header_value.to_string()) {
                debug!(header = %accepted_header.name, value = %header_value, "Auth header accepted");
                return None;
            }
        }
    }

    // No valid header found - log which headers were expected
    let expected: Vec<_> = accepted.iter().map(|h| h.name.as_str()).collect();
    warn!(expected_headers = ?expected, "No valid auth header found");

    Some(AuthError {
        status: StatusCode::UNAUTHORIZED,
        message: "Missing or invalid authorization header".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AcceptedHeader;
    use axum::http::HeaderMap;

    fn test_config() -> AuthConfig {
        AuthConfig {
            mode: "header".to_string(),
            accepted_headers: vec![AcceptedHeader {
                name: "x-api-key".to_string(),
                values: vec!["valid-key".to_string(), "another-key".to_string()],
            }],
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
            header_name: "x-legacy-header".to_string(),
            header_values: vec!["legacy-value".to_string()],
        };

        let effective = config.effective_headers();
        assert_eq!(effective.len(), 2);
        assert!(effective.iter().any(|h| h.name == "x-new-header"));
        assert!(effective.iter().any(|h| h.name == "x-legacy-header"));
    }
}
