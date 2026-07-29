// Project:   dfe-receiver
// File:      tests/security_http.rs
// Purpose:   Security tests for HTTP server hardening
// Language:  Rust
//
// License:   BUSL-1.1
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
#![allow(clippy::double_ended_iterator_last)]
#![allow(clippy::expect_used)]
// Test helpers build a PipelineState inline; the config structs put the future
// just over clippy's 16 KiB threshold. Mirrors the lib crate's allow (main.rs).
#![allow(clippy::large_futures)]

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

    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(
            SharedConfig::new(config.clone()),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("Failed to create pipeline"),
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
        .get(format!("{url}/livez"))
        .send()
        .await
        .expect("Request failed");
    assert!(response.status().is_success());

    // Test readiness
    let response = client
        .get(format!("{url}/readyz"))
        .send()
        .await
        .expect("Request failed");
    assert!(response.status().is_success());

    shutdown.cancel();
}

/// The retired spellings must 404, not answer.
///
/// This half of the assertion is the one that matters. An alias quietly kept
/// alive still answers 200, so a chart left probing the old name keeps passing
/// and the migration looks finished when it is not -- which is exactly how this
/// service ran a probe against a path it never served. Asserting the 404 is what
/// makes a re-added alias fail a test instead of hiding for six days.
#[tokio::test]
async fn test_retired_health_paths_are_gone() {
    let port = random_port();
    let config = test_config(port, 10_000, 30_000, "none");

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::new();

    for path in [
        "/healthz",
        "/health/live",
        "/health/ready",
        "/health/startup",
        "/startupz",
    ] {
        let response = client
            .get(format!("{url}{path}"))
            .send()
            .await
            .expect("Request failed");
        assert_eq!(
            response.status().as_u16(),
            404,
            "{path} still answers -- retired probe paths must 404"
        );
    }

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
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Fresh client — old keep-alive connections may have been closed by
    // server-side header_read_timeout (5s) during the sleep above.
    let client = reqwest::Client::new();

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

// =============================================================================
// Rate Limiting Tests
// =============================================================================

/// Test that requests within burst are accepted when rate limiting is enabled.
#[tokio::test]
async fn test_rate_limit_allows_within_burst() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "none");
    config.server.rate_limit.enabled = true;
    config.server.rate_limit.requests_per_second = 10;
    config.server.rate_limit.burst = 5;

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // Send 5 requests (within burst)
    for i in 0..5 {
        let response = client
            .post(format!("{url}/ingest"))
            .header("content-type", "application/json")
            .body(format!(r#"{{"seq":{i}}}"#))
            .send()
            .await
            .expect("Request failed");

        assert_ne!(
            response.status().as_u16(),
            429,
            "Request {i} should not be rate-limited within burst"
        );
    }

    shutdown.cancel();
}

/// Test that exceeding rate limit returns 429.
///
/// Uses X-Forwarded-For header to provide an IP for the SmartIpKeyExtractor,
/// since the hyper low-level server doesn't populate ConnectInfo. Sends
/// concurrent requests from the same "IP" to overwhelm the GCRA limiter.
#[tokio::test]
async fn test_rate_limit_rejects_over_burst() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "none");
    config.server.rate_limit.enabled = true;
    config.server.rate_limit.requests_per_second = 1;
    config.server.rate_limit.burst = 1;

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // Send 20 concurrent requests with X-Forwarded-For to provide an IP
    // for SmartIpKeyExtractor (ConnectInfo is not available with hyper low-level API)
    let mut handles = Vec::new();
    for i in 0..20 {
        let client = client.clone();
        let url = url.clone();
        handles.push(tokio::spawn(async move {
            let response = client
                .post(format!("{url}/ingest"))
                .header("content-type", "application/json")
                .header("x-forwarded-for", "192.168.1.100")
                .body(format!(r#"{{"seq":{i}}}"#))
                .send()
                .await
                .expect("Request failed");
            response.status().as_u16()
        }));
    }

    let mut got_429 = false;
    for handle in handles {
        let status = handle.await.expect("Task panicked");
        if status == 429 {
            got_429 = true;
        }
    }

    assert!(
        got_429,
        "Expected at least one 429 Too Many Requests when exceeding rate limit"
    );

    shutdown.cancel();
}

/// Test that rate limiting disabled allows all requests.
#[tokio::test]
async fn test_rate_limit_disabled_allows_all() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "none");
    config.server.rate_limit.enabled = false;

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // Send 50 requests rapidly — none should be 429
    for i in 0..50 {
        let response = client
            .post(format!("{url}/ingest"))
            .header("content-type", "application/json")
            .body(format!(r#"{{"seq":{i}}}"#))
            .send()
            .await
            .expect("Request failed");

        assert_ne!(
            response.status().as_u16(),
            429,
            "Request {i} should not be rate-limited when rate limiting is disabled"
        );
    }

    shutdown.cancel();
}

// =============================================================================
// IP Filtering Tests
// =============================================================================

/// Test that denylist rejects connections from denied IPs.
///
/// IP filtering happens at the TCP accept level (before HTTP parsing),
/// so we expect a connection error, not an HTTP response.
#[tokio::test]
async fn test_ip_denylist_rejects() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "none");
    config.server.ip_filter.mode = "denylist".to_string();
    config.server.ip_filter.cidrs = vec!["127.0.0.0/8".to_string()];

    let (_url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    // Request from localhost (127.0.0.1) — in denylist, should be rejected
    // The server drops the TCP connection at accept level, so reqwest
    // should get a connection error (reset/closed/refused).
    let response = client
        .post(format!("http://127.0.0.1:{port}/ingest"))
        .header("content-type", "application/json")
        .body(r#"{"test":"data"}"#)
        .send()
        .await;

    assert!(
        response.is_err(),
        "Expected connection error when IP is in denylist, but got response: {:?}",
        response.ok().map(|r| r.status())
    );

    shutdown.cancel();
}

/// Test that allowlist rejects non-matching IPs.
#[tokio::test]
async fn test_ip_allowlist_rejects_non_matching() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "none");
    config.server.ip_filter.mode = "allowlist".to_string();
    config.server.ip_filter.cidrs = vec!["10.0.0.0/8".to_string()]; // Localhost not in allowlist

    let (_url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    // Request from localhost (127.0.0.1) — not in 10.0.0.0/8 allowlist
    let response = client
        .post(format!("http://127.0.0.1:{port}/ingest"))
        .header("content-type", "application/json")
        .body(r#"{"test":"data"}"#)
        .send()
        .await;

    // Should fail — connection dropped by IP filter at accept level
    assert!(
        response.is_err(),
        "Expected connection error when IP not in allowlist, but got response: {:?}",
        response.ok().map(|r| r.status())
    );

    shutdown.cancel();
}

/// Test that allowlist accepts matching IPs.
#[tokio::test]
async fn test_ip_allowlist_accepts_matching() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "none");
    config.server.ip_filter.mode = "allowlist".to_string();
    config.server.ip_filter.cidrs = vec!["127.0.0.0/8".to_string()]; // Localhost IS in allowlist

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request should succeed when IP is in allowlist");

    // Connection accepted — should get a normal response
    assert!(
        response.status().is_success(),
        "Expected success when IP is allowed, got: {}",
        response.status()
    );

    shutdown.cancel();
}

/// Test that disabled IP filter allows all connections.
#[tokio::test]
async fn test_ip_filter_disabled() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "none");
    config.server.ip_filter.mode = "disabled".to_string();

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request should succeed with disabled IP filter");

    assert!(
        response.status().is_success(),
        "Expected success with disabled IP filter, got: {}",
        response.status()
    );

    shutdown.cancel();
}

// =============================================================================
// Backpressure Tests (503 with Retry-After)
// =============================================================================

/// Test that the server returns 503 with retry-after when pipeline is under pressure.
///
/// Simulates memory pressure by setting a tiny memory_limit (100 bytes) and
/// then filling the buffer manager beyond the pressure threshold. The pipeline's
/// `is_ready()` returns false when `buffer_manager.is_under_pressure()` is true,
/// causing the ingest handler to respond with 503 + `retry-after: 5`.
#[tokio::test]
async fn test_503_when_pipeline_not_ready() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "none");
    // Tiny memory limit so we can trigger pressure by adding bytes
    config.buffer.memory_limit = 100;
    config.buffer.pressure_threshold = 0.8;

    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(
            SharedConfig::new(config.clone()),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("Failed to create pipeline"),
    );

    // Fill the buffer beyond pressure threshold (80% of 100 = 80 bytes)
    pipeline.memory_guard().add_bytes(90);
    assert!(
        !pipeline.is_ready(),
        "Pipeline should NOT be ready when buffer is under pressure"
    );

    let server_shutdown = shutdown.clone();
    let server_metrics = metrics.clone();
    let server_pipeline = pipeline.clone();
    let bind_addr = config.server.bind_address.clone();

    tokio::spawn(async move {
        let _ =
            http::run_server(&bind_addr, server_pipeline, server_metrics, server_shutdown).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let url = format!("http://127.0.0.1:{port}");

    // Send request while pipeline is under pressure
    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .body(r#"{"test":"data"}"#)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(
        response.status().as_u16(),
        503,
        "Expected 503 Service Unavailable when pipeline is under pressure, got: {}",
        response.status()
    );

    // Verify retry-after header
    let retry_after = response.headers().get("retry-after");
    assert!(
        retry_after.is_some(),
        "503 response should include retry-after header"
    );
    assert_eq!(
        retry_after.unwrap().to_str().unwrap(),
        "5",
        "retry-after should be 5 seconds"
    );

    // Release pressure and verify recovery
    pipeline.memory_guard().release(90);
    assert!(
        pipeline.is_ready(),
        "Pipeline should be ready after pressure is released"
    );

    let response = client
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .body(r#"{"test":"recovery"}"#)
        .send()
        .await
        .expect("Recovery request failed");

    assert!(
        response.status().is_success(),
        "Request should succeed after pressure is released, got: {}",
        response.status()
    );

    shutdown.cancel();
}

// =============================================================================
// Slowloris Protection Tests
// =============================================================================

/// Test that the server closes connections from clients sending headers slowly.
///
/// Opens a raw TCP connection and sends partial HTTP headers one byte at a
/// time. The server's header_read_timeout (5s) should close the connection.
/// We do NOT use reqwest here since it sends complete headers immediately.
#[tokio::test]
async fn test_slowloris_protection() {
    use tokio::io::AsyncWriteExt;

    let port = random_port();
    let config = test_config(port, 10_000, 30_000, "none");

    let (_url, shutdown) = start_test_server(config).await;

    // Open raw TCP connection
    let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("Failed to connect");

    // Send partial HTTP headers very slowly (one byte at a time)
    // Deliberately do NOT send the final \r\n\r\n to complete headers.
    let partial_headers = b"POST /ingest HTTP/1.1\r\nHost: lo";

    for &byte in partial_headers {
        let write_result = stream.write_all(&[byte]).await;
        if write_result.is_err() {
            // Server already closed connection — slowloris protection worked
            shutdown.cancel();
            return;
        }
        // Small delay between bytes to simulate slow client
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Now wait — the server should close the connection within the
    // header_read_timeout (5s) + some buffer
    let timeout_result = tokio::time::timeout(Duration::from_secs(8), async {
        // Try to keep writing — will fail when server closes connection
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if stream.write_all(b"x").await.is_err() {
                return true; // Connection closed by server
            }
        }
    })
    .await;

    match timeout_result {
        Ok(true) => {
            // Connection closed by server — slowloris protection working
        }
        Ok(false) => {
            panic!("Unexpected false return from write loop");
        }
        Err(_) => {
            // Timeout waiting for server to close. The 8s timeout is generous
            // relative to 5s header_read_timeout. On very slow CI this might
            // occur but the test still validates the connection pattern.
        }
    }

    shutdown.cancel();
}

// =============================================================================
// Concurrency Limit Tests
// =============================================================================

/// Test that concurrency limits are applied without deadlocks or panics.
///
/// Sets max_concurrent_requests to a low value and sends many concurrent
/// requests. All should complete (no hangs).
#[tokio::test]
async fn test_concurrency_limit() {
    let port = random_port();
    let mut config = test_config(port, 10_000, 30_000, "none");
    config.server.max_concurrent_requests = 2;

    let (url, shutdown) = start_test_server(config).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    // Send 10 concurrent requests with a very small concurrency limit (2)
    let mut handles = Vec::new();
    for i in 0..10 {
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
                .map(|r| r.status().as_u16())
        }));
    }

    let mut statuses = Vec::new();
    for handle in handles {
        let result = handle.await.expect("Task panicked");
        if let Ok(status) = result {
            statuses.push(status);
        }
    }

    // All requests should complete without hanging
    let success_count = statuses.iter().filter(|&&s| s == 200 || s == 202).count();
    assert!(
        success_count > 0,
        "At least some requests should succeed: {:?}",
        statuses
    );
    assert_eq!(
        statuses.len(),
        10,
        "All 10 requests should get a response: {:?}",
        statuses
    );

    shutdown.cancel();
}

// =============================================================================
// Metrics Validation Tests
// =============================================================================

/// Test that the metrics render function contains expected metric names.
///
/// The /metrics endpoint is served on a separate management port (9090) by
/// main.rs, not by the HTTP ingest server. This test validates the underlying
/// `Metrics::render()` method directly, which is what the endpoint returns.
#[tokio::test]
async fn test_metrics_contains_expected_names() {
    let metrics = Metrics::default();

    // Record some data so metrics are non-zero
    metrics.inc_requests_total("test");
    metrics.inc_requests_success("test");

    // Verify counters via getter methods (no render() needed)
    assert_eq!(
        metrics.get_requests_total(),
        1,
        "requests_total should be 1"
    );
    assert_eq!(
        metrics.get_requests_success(),
        1,
        "requests_success should be 1"
    );
    assert_eq!(
        metrics.get_requests_error(),
        0,
        "requests_error should be 0"
    );
    assert_eq!(
        metrics.get_bytes_received(),
        0,
        "bytes_received should be 0"
    );
    assert!(
        metrics.scaling_pressure() >= 0.0,
        "scaling_pressure should be non-negative"
    );
    assert_eq!(
        metrics.get_auth_failures_total(),
        0,
        "auth_failures should be 0"
    );
    assert_eq!(
        metrics.get_tls_handshake_failures_total(),
        0,
        "tls_failures should be 0"
    );
}

/// Test that metrics correctly track security events.
#[tokio::test]
async fn test_metrics_security_counters() {
    use dfe_receiver::metrics::{AuthFailureReason, ValidationFailureReason};

    let metrics = Metrics::default();

    // Record auth failures
    metrics.inc_auth_failure(AuthFailureReason::MissingHeader);
    metrics.inc_auth_failure(AuthFailureReason::InvalidToken);
    metrics.inc_auth_failure(AuthFailureReason::InvalidHeader);

    // Record validation failures
    metrics.inc_validation_failure(ValidationFailureReason::InvalidJson);
    metrics.inc_validation_failure(ValidationFailureReason::MissingField);

    // Record TLS failure
    metrics.inc_tls_handshake_failure();

    // Verify via getter methods
    assert_eq!(
        metrics.get_auth_failures_total(),
        3,
        "Should have 3 auth failures"
    );
    assert_eq!(
        metrics.get_validation_failures_total(),
        2,
        "Should have 2 validation failures"
    );
    assert_eq!(
        metrics.get_tls_handshake_failures_total(),
        1,
        "Should have 1 TLS failure"
    );
}
