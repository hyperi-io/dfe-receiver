// Project:   dfe-receiver
// File:      src/error.rs
// Purpose:   Centralised error types
// Language:  Rust
//
// License:   FSL-1.1-ALv2
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
    Kafka(#[from] rdkafka::error::KafkaError),

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

    /// Buffer/memory error.
    #[error("buffer error: {0}")]
    Buffer(String),

    /// Shutdown requested.
    #[error("shutdown requested")]
    Shutdown,

    /// Secrets management error.
    #[error("secrets error: {0}")]
    Secrets(#[from] hs_rustlib::SecretsError),
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

impl From<serde_yaml::Error> for Error {
    fn from(err: serde_yaml::Error) -> Self {
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
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::Validation("invalid JSON".to_string());
        assert_eq!(err.to_string(), "JSON validation failed: invalid JSON");
    }

    #[test]
    fn test_error_from_string() {
        let err: Error = "test error".into();
        assert!(matches!(err, Error::Config(_)));
    }
}
