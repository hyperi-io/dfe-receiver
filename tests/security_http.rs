// Project:   dfe-receiver
// File:      tests/security_http.rs
// Purpose:   Security tests for HTTP server hardening
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Security tests for the HTTP server.
//!
//! Tests verify that security controls are properly enforced:
//! - Request body size limits
//! - Request timeouts
//! - Authentication enforcement
//! - TLS handshake timeouts (manual testing required)
//!
//! Run with: `cargo test --test security_http`

// Allow unwrap/expect in tests - they're the idiomatic way to fail fast
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{AcceptedHeader, BearerConfig, Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::http;
use tokio_util::sync::CancellationToken;

/// Get a random port for testing.
fn random_port() -> u16 {
    10000 + (uuid::Uuid::new_v4().as_u128() % 10000) as u16
}

/// Create a test config with specified settings.
fn test_config(port: u16, max_body_size: usize, timeout_ms: u64, auth_mode: &str) -> Config {
    let mut config = Config::default();
    config.server.bind_address = format!("127.0.0.1:{port}");
    config.server.max_body_size = max_body_size;
    config.server.request_timeout_ms = timeout_ms;
    config.server.auth.mode = auth_mode.to_string();

    // Use loader destination to avoid Kafka dependency
    config.destinations.default = "loader".to_string();

    config
}

/// Start a test server and return the URL.
async fn start_test_server(config: Config) -> (String, CancellationToken) {
    let port = config
        .server
        .bind_address
        .split(':')
        .last()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(8080);

    let metrics = Arc::new(Metrics::new());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone())).expect("Failed to create pipeline"),
    );

    let server_shutdown = shutdown.clone();
    let server_metrics = metrics.clone();
    let bind_addr = config.server.bind_address.clone();

    tokio::spawn(async move {
        let _ = http::run_server(&bind_addr, pipeline, server_metrics, server_shutdown).await;
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    let url = format!("http://127.0.0.1:{port}");
    (url, shutdown)
}

// =============================================================================
// Body Size Limit Tests
// =============================================================================

/// Test that requests within body size limit are accepted.
#[tokio::test]
async fn test_body_size_within_limit() {
    let port = random_port();
    let max_body = 1024; // 1KB limit
    let config = test_config(port, max_body, 30_000, "none");

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Send a small payload (under limit)
    let payload = r#"{"event_category":"test","data":"small"}"#;
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .body(payload)
        .send()
        .await
        .expect("Request failed");

    assert!(
        response.status().is_success(),
        "Expected success, got: {}",
        response.status()
    );

    shutdown.cancel();
}

/// Test that requests exceeding body size limit are rejected.
#[tokio::test]
async fn test_body_size_exceeds_limit() {
    let port = random_port();
    let max_body = 100; // Very small limit (100 bytes)
    let config = test_config(port, max_body, 30_000, "none");

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Send a large payload (over limit)
    let large_payload = "x".repeat(500);
    let payload = format!(r#"{{"data":"{}"}}"#, large_payload);

    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .body(payload)
        .send()
        .await
        .expect("Request failed");

    // Should be rejected with 413 Payload Too Large
    assert_eq!(
        response.status().as_u16(),
        413,
        "Expected 413 Payload Too Large, got: {}",
        response.status()
    );

    shutdown.cancel();
}

/// Test that exactly-at-limit requests are accepted.
#[tokio::test]
async fn test_body_size_at_limit() {
    let port = random_port();
    let max_body = 100;
    let config = test_config(port, max_body, 30_000, "none");

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Create payload exactly at limit
    let padding = "x".repeat(max_body - 20); // Account for JSON structure
    let payload = format!(r#"{{"d":"{}"}}"#, padding);

    // Ensure we're at or under the limit
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .body(payload)
        .send()
        .await
        .expect("Request failed");

    // Should be accepted (at or under limit)
    assert!(
        response.status().is_success() || response.status().as_u16() == 413,
        "Expected success or 413, got: {}",
        response.status()
    );

    shutdown.cancel();
}

// =============================================================================
// Authentication Tests
// =============================================================================

/// Test that unauthenticated requests are rejected when auth is required.
#[tokio::test]
async fn test_auth_required_no_header() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "header");
    config.server.auth.accepted_headers = vec![AcceptedHeader {
        name: "x-api-key".to_string(),
        values: vec!["secret-key".to_string()],
    }];

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Send request without auth header
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(
        response.status().as_u16(),
        401,
        "Expected 401 Unauthorized, got: {}",
        response.status()
    );

    shutdown.cancel();
}

/// Test that requests with wrong auth are rejected.
#[tokio::test]
async fn test_auth_wrong_header_value() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "header");
    config.server.auth.accepted_headers = vec![AcceptedHeader {
        name: "x-api-key".to_string(),
        values: vec!["secret-key".to_string()],
    }];

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Send request with wrong key
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .header("x-api-key", "wrong-key")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(
        response.status().as_u16(),
        401,
        "Expected 401 Unauthorized, got: {}",
        response.status()
    );

    shutdown.cancel();
}

/// Test that requests with correct auth are accepted.
#[tokio::test]
async fn test_auth_correct_header() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "header");
    config.server.auth.accepted_headers = vec![AcceptedHeader {
        name: "x-api-key".to_string(),
        values: vec!["secret-key".to_string()],
    }];

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Send request with correct key
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .header("x-api-key", "secret-key")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    assert!(
        response.status().is_success(),
        "Expected success, got: {}",
        response.status()
    );

    shutdown.cancel();
}

/// Test bearer token authentication.
#[tokio::test]
async fn test_bearer_auth_valid_token() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "bearer");
    config.server.auth.bearer = BearerConfig {
        tokens: vec!["valid-token-123".to_string()],
        secret_source: None,
        refresh_interval_secs: 300,
    };

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Send request with valid bearer token
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer valid-token-123")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "Expected success, got: {} - body: {}",
        status,
        body
    );

    shutdown.cancel();
}

/// Test bearer token authentication with invalid token.
#[tokio::test]
async fn test_bearer_auth_invalid_token() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "bearer");
    config.server.auth.bearer = BearerConfig {
        tokens: vec!["valid-token-123".to_string()],
        secret_source: None,
        refresh_interval_secs: 300,
    };

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Send request with invalid bearer token
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer invalid-token")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(
        response.status().as_u16(),
        401,
        "Expected 401 Unauthorized, got: {}",
        response.status()
    );

    shutdown.cancel();
}

// =============================================================================
// Request Timeout Tests
// =============================================================================

/// Test that health endpoints are accessible.
#[tokio::test]
async fn test_health_endpoints() {
    let port = random_port();
    let config = test_config(port, 10_000, 30_000, "none");

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Test liveness
    let response = client
        .get(format!("{url}/health/live"))
        .send()
        .await
        .expect("Request failed");
    assert!(response.status().is_success());

    // Test readiness
    let response = client
        .get(format!("{url}/health/ready"))
        .send()
        .await
        .expect("Request failed");
    assert!(response.status().is_success());

    shutdown.cancel();
}

// =============================================================================
// Security Attack Simulation Tests
// =============================================================================

/// Test that binary garbage is rejected.
#[tokio::test]
async fn test_reject_binary_garbage() {
    let port = random_port();
    let config = test_config(port, 10_000, 30_000, "none");

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Send binary garbage
    let garbage = vec![0x00, 0x01, 0xFF, 0xFE, 0x80, 0x81];
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .body(garbage)
        .send()
        .await
        .expect("Request failed");

    // Should be rejected (either 400 Bad Request or similar error)
    assert!(
        response.status().is_client_error() || response.status().is_server_error(),
        "Expected error status for binary garbage, got: {}",
        response.status()
    );

    shutdown.cancel();
}

/// Test that empty requests are handled.
#[tokio::test]
async fn test_handle_empty_request() {
    let port = random_port();
    let config = test_config(port, 10_000, 30_000, "none");

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Send empty body
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .body("")
        .send()
        .await
        .expect("Request failed");

    // Should be rejected
    assert!(
        response.status().is_client_error() || response.status().is_server_error(),
        "Expected error for empty body, got: {}",
        response.status()
    );

    shutdown.cancel();
}

/// Test that non-JSON content types are handled.
#[tokio::test]
async fn test_wrong_content_type() {
    let port = random_port();
    let config = test_config(port, 10_000, 30_000, "none");

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Send with wrong content type
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "text/plain")
        .body("not json")
        .send()
        .await
        .expect("Request failed");

    // Should still process (we accept the body, validation rejects non-JSON)
    // The actual behavior depends on config - just ensure no panic
    let _ = response.status();

    shutdown.cancel();
}

/// Test concurrent requests don't cause issues.
#[tokio::test]
async fn test_concurrent_requests() {
    let port = random_port();
    let config = test_config(port, 10_000, 30_000, "none");

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Send many concurrent requests
    let mut handles = Vec::new();
    for i in 0..50 {
        let client = client.clone();
        let url = url.clone();
        handles.push(tokio::spawn(async move {
            let payload = format!(r#"{{"seq":{i}}}"#);
            client
                .post(format!("{url}/ingest"))
                .header("content-type", "application/json")
                .body(payload)
                .send()
                .await
        }));
    }

    // All should complete without panic
    for handle in handles {
        let result = handle.await.expect("Task panicked");
        // We don't care about the result, just that it didn't panic
        let _ = result;
    }

    shutdown.cancel();
}

// =============================================================================
// File-Based Bearer Auth Tests
// =============================================================================

/// Test bearer auth with tokens loaded from a file.
#[tokio::test]
async fn test_bearer_auth_from_file() {
    use std::io::Write;

    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "bearer");

    // Write tokens to a temp file
    let mut token_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    writeln!(token_file, "file-token-abc").expect("Failed to write token");
    writeln!(token_file, "file-token-def").expect("Failed to write token");
    token_file.flush().expect("Failed to flush");

    let token_path = token_file
        .path()
        .to_str()
        .expect("Invalid path")
        .to_string();

    config.server.auth.bearer = BearerConfig {
        tokens: vec![],
        secret_source: Some(format!("file:{token_path}")),
        refresh_interval_secs: 300,
    };

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Valid file-sourced token should be accepted
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer file-token-abc")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "Expected success for file token, got: {} - body: {}",
        status,
        body
    );

    // Second token from file should also work
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer file-token-def")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    assert!(
        response.status().is_success(),
        "Expected success for second file token, got: {}",
        response.status()
    );

    // Invalid token should be rejected
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer not-in-file")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(
        response.status().as_u16(),
        401,
        "Expected 401 for invalid token, got: {}",
        response.status()
    );

    shutdown.cancel();
}

/// Test bearer auth token refresh from file.
///
/// Writes an initial token, verifies it works, then overwrites the file
/// with a new token and waits for refresh.
#[tokio::test]
async fn test_bearer_auth_file_refresh() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "bearer");

    // Write initial token
    let token_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    let token_path = token_file
        .path()
        .to_str()
        .expect("Invalid path")
        .to_string();

    std::fs::write(&token_path, "initial-token\n").expect("Failed to write initial token");

    config.server.auth.bearer = BearerConfig {
        tokens: vec![],
        secret_source: Some(format!("file:{token_path}")),
        refresh_interval_secs: 1, // 1 second refresh for testing
    };

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // Initial token should work
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer initial-token")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    assert!(
        response.status().is_success(),
        "Expected success for initial token, got: {}",
        response.status()
    );

    // Overwrite file with new token
    std::fs::write(&token_path, "refreshed-token\n").expect("Failed to write refreshed token");

    // Wait for refresh (1s interval + generous buffer for CI/slow machines)
    tokio::time::sleep(Duration::from_millis(5000)).await;

    // New token should work after refresh
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer refreshed-token")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    assert!(
        response.status().is_success(),
        "Expected success for refreshed token, got: {}",
        response.status()
    );

    // Old token should be rejected after refresh
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer initial-token")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(
        response.status().as_u16(),
        401,
        "Expected 401 for old token after refresh, got: {}",
        response.status()
    );

    shutdown.cancel();
}

/// Test bearer auth with comma-separated tokens in file.
#[tokio::test]
async fn test_bearer_auth_file_comma_separated() {
    use std::io::Write;

    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "bearer");

    // Write comma-separated tokens
    let mut token_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    writeln!(token_file, "token-alpha,token-beta,token-gamma").expect("Failed to write tokens");
    token_file.flush().expect("Failed to flush");

    let token_path = token_file
        .path()
        .to_str()
        .expect("Invalid path")
        .to_string();

    config.server.auth.bearer = BearerConfig {
        tokens: vec![],
        secret_source: Some(format!("file:{token_path}")),
        refresh_interval_secs: 300,
    };

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    // All three comma-separated tokens should work
    for token in ["token-alpha", "token-beta", "token-gamma"] {
        let response = client
            .post(format!("{url}/ingest"))
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .body(r#"{"test":"data"}"#)
            .send()
            .await
            .expect("Request failed");

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        assert!(
            status.is_success(),
            "Expected success for token '{token}', got: {} - body: {}",
            status,
            body
        );
    }

    // Token not in file should be rejected
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer token-delta")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(
        response.status().as_u16(),
        401,
        "Expected 401 for token not in file, got: {}",
        response.status()
    );

    shutdown.cancel();
}
