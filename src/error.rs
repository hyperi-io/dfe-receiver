// Project:   dfe-receiver
// File:      src/error.rs
// Purpose:   Centralised error types
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Centralised error types for dfe-receiver.
//!
//! Uses `thiserror` for ergonomic error derivation with automatic
//! `From` implementations for common conversions.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

/// Main error type for dfe-receiver.
#[derive(Error, Debug)]
pub enum Error {
    /// Configuration loading or validation error.
    #[error("configuration error: {0}")]
    Config(String),

    /// JSON validation failed.
    #[error("JSON validation failed: {0}")]
    Validation(String),

    /// Routing error (no matching route, invalid field).
    #[error("routing error: {0}")]
    Routing(String),

    /// Kafka producer error.
    #[error("Kafka error: {0}")]
    Kafka(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// TLS/certificate error.
    #[error("TLS error: {0}")]
    Tls(String),

    /// Authentication failed.
    #[error("authentication failed: {0}")]
    Auth(String),

    /// Server error.
    #[error("server error: {0}")]
    Server(String),

    /// Transport error (dfe-loader connection).
    #[error("transport error: {0}")]
    Transport(String),

    /// The destination refused this record and refuses it on every retry.
    #[error("record rejected by destination: {0}")]
    Rejected(String),

    /// Buffer/memory error.
    #[error("buffer error: {0}")]
    Buffer(String),

    /// Shutdown requested.
    #[error("shutdown requested")]
    Shutdown,

    /// Secrets management error.
    #[error("secrets error: {0}")]
    Secrets(#[from] scalo::SecretsError),
}

/// Result type alias for dfe-receiver operations.
pub type Result<T> = std::result::Result<T, Error>;

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Config(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Config(s.to_string())
    }
}

impl From<serde_yaml_ng::Error> for Error {
    fn from(err: serde_yaml_ng::Error) -> Self {
        Error::Config(format!("YAML parse error: {err}"))
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Validation(format!("JSON error: {err}"))
    }
}

/// Convert errors to HTTP responses for axum handlers.
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Error::Validation(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            Error::Auth(msg) => (StatusCode::UNAUTHORIZED, msg.clone()),
            Error::Routing(_) => (StatusCode::BAD_REQUEST, "routing error".to_string()),
            Error::Kafka(_) | Error::Transport(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "service unavailable".to_string(),
            ),
            Error::Buffer(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "service under pressure".to_string(),
            ),
            Error::Shutdown => (StatusCode::SERVICE_UNAVAILABLE, "shutting down".to_string()),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            ),
        };

        let body = serde_json::json!({
            "error": message,
        });

        (status, axum::Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

    use super::*;

    #[test]
    fn test_error_display_all_variants() {
        // Every variant must produce a sensible Display format
        let cases = vec![
            (Error::Config("c".into()), "configuration error: c"),
            (Error::Validation("v".into()), "JSON validation failed: v"),
            (Error::Routing("r".into()), "routing error: r"),
            (Error::Kafka("k".into()), "Kafka error: k"),
            (Error::Tls("t".into()), "TLS error: t"),
            (Error::Auth("a".into()), "authentication failed: a"),
            (Error::Server("s".into()), "server error: s"),
            (Error::Transport("tp".into()), "transport error: tp"),
            (
                Error::Rejected("rj".into()),
                "record rejected by destination: rj",
            ),
            (Error::Buffer("b".into()), "buffer error: b"),
            (Error::Shutdown, "shutdown requested"),
        ];
        for (err, expected) in cases {
            assert_eq!(err.to_string(), expected);
        }
    }

    #[test]
    fn test_error_from_string() {
        let err: Error = "test error".into();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn test_error_from_str() {
        let err: Error = String::from("owned error").into();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn test_error_from_serde_json() {
        // Real serde_json parse error
        let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let err: Error = json_err.into();
        assert!(matches!(err, Error::Validation(_)));
        assert!(err.to_string().contains("JSON"));
    }

    #[test]
    fn test_error_from_serde_yaml() {
        let yaml_err =
            serde_yaml_ng::from_str::<serde_yaml_ng::Value>("::: invalid :::").unwrap_err();
        let err: Error = yaml_err.into();
        assert!(matches!(err, Error::Config(_)));
        assert!(err.to_string().contains("YAML"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::other("disk full");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
        assert!(err.to_string().contains("I/O"));
    }

    // ---------------------------------------------------------------------
    // HTTP response mapping — verify security-critical status codes
    // ---------------------------------------------------------------------

    async fn response_status_and_body(err: Error) -> (StatusCode, String) {
        let resp = err.into_response();
        let status = resp.status();
        let body = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        (status, text)
    }

    #[tokio::test]
    async fn test_response_validation_returns_400_with_detail() {
        let (status, body) =
            response_status_and_body(Error::Validation("malformed JSON at offset 42".into())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        // Validation errors may surface user-facing detail
        assert!(body.contains("malformed JSON"));
    }

    #[tokio::test]
    async fn test_response_auth_returns_401() {
        let (status, body) =
            response_status_and_body(Error::Auth("missing bearer token".into())).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(body.contains("missing bearer"));
    }

    #[tokio::test]
    async fn test_response_routing_does_not_leak_details() {
        // Routing errors should NOT leak internal details to clients
        let (status, body) = response_status_and_body(Error::Routing(
            "internal topic_not_found: secret_config_topic_42".into(),
        ))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        // Internal secrets must not appear in client response
        assert!(
            !body.contains("secret_config_topic"),
            "internal routing details leaked: {body}"
        );
        assert!(body.contains("routing error"));
    }

    #[tokio::test]
    async fn test_response_kafka_masked_as_service_unavailable() {
        // Kafka errors should NOT leak broker addresses or topic names
        let (status, body) = response_status_and_body(Error::Kafka(
            "broker kafka-internal.svc.cluster.local:9092 timeout".into(),
        ))
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            !body.contains("kafka-internal"),
            "internal broker address leaked: {body}"
        );
        assert!(
            !body.contains("cluster.local"),
            "cluster details leaked: {body}"
        );
    }

    #[tokio::test]
    async fn test_response_transport_masked() {
        let (status, body) = response_status_and_body(Error::Transport(
            "connection refused to grpc://loader.internal:6000".into(),
        ))
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!body.contains("loader.internal"));
        assert!(!body.contains("grpc://"));
    }

    #[tokio::test]
    async fn test_response_buffer_returns_503() {
        let (status, body) =
            response_status_and_body(Error::Buffer("memory exhausted at 4GB".into())).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        // Memory limits are sensitive infra info
        assert!(!body.contains("4GB"));
    }

    #[tokio::test]
    async fn test_response_shutdown_returns_503() {
        let (status, body) = response_status_and_body(Error::Shutdown).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("shutting down"));
    }

    #[tokio::test]
    async fn test_response_config_returns_500_without_detail() {
        // Config errors are internal — should never expose config contents
        let (status, body) = response_status_and_body(Error::Config(
            "failed to parse DATABASE_URL=postgres://secret:pass@host/db".into(),
        ))
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            !body.contains("DATABASE_URL"),
            "env var name leaked: {body}"
        );
        assert!(!body.contains("secret"), "credential leaked: {body}");
        assert!(!body.contains("pass@host"), "credential leaked: {body}");
    }

    #[tokio::test]
    async fn test_response_tls_returns_500_without_detail() {
        // TLS errors should NOT leak cert paths or key details
        let (status, body) = response_status_and_body(Error::Tls(
            "/etc/secrets/private-key.pem: permission denied".into(),
        ))
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!body.contains("private-key.pem"), "key path leaked: {body}");
        assert!(!body.contains("/etc/secrets"));
    }

    #[tokio::test]
    async fn test_response_io_returns_500_without_path() {
        let io_err = std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "/var/secret/config.yaml: not found",
        );
        let err: Error = io_err.into();
        let (status, body) = response_status_and_body(err).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!body.contains("/var/secret"));
    }
}
