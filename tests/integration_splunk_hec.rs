// Project:   dfe-receiver
// File:      tests/integration_splunk_hec.rs
// Purpose:   Integration tests for Splunk HEC protocol handler
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for the Splunk HEC handler.
//!
//! These tests start a real Splunk HEC handler and send requests via reqwest.
//! No external binaries are needed — HEC is just HTTP.
//!
//! Run with: `cargo test --test integration_splunk_hec`

// Allow unwrap/expect in tests - they're the idiomatic way to fail fast
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::splunk_hec::SplunkHecHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use tokio_util::sync::CancellationToken;

/// Get a random port for testing.
fn random_port() -> u16 {
    10000 + (uuid::Uuid::new_v4().as_u128() % 10000) as u16
}

/// Create a minimal config for testing with Splunk HEC enabled.
fn test_config(hec_port: u16) -> Config {
    let mut config = Config::default();
    // HTTP server still needs a bind address (always enabled)
    let http_port = random_port();
    config.server.bind_address = format!("127.0.0.1:{http_port}");
    config.server.auth.mode = "none".to_string();
    // Enable Splunk HEC
    config.splunk_hec.enabled = true;
    config.splunk_hec.bind_address = format!("127.0.0.1:{hec_port}");
    config.splunk_hec.auth.mode = "none".to_string();
    // Use loader destination to avoid Kafka dependency
    config.destinations.default = "loader".to_string();
    config
}

/// Create a config with bearer auth enabled.
fn test_config_with_auth(hec_port: u16, token: &str) -> Config {
    let mut config = test_config(hec_port);
    config.splunk_hec.auth.mode = "bearer".to_string();
    config.splunk_hec.auth.bearer.tokens = vec![token.to_string()];
    config
}

/// Start the HEC handler and return (shutdown_token, metrics, base_url).
async fn start_hec_handler(config: Config) -> (CancellationToken, Arc<Metrics>, String) {
    let hec_port = config
        .splunk_hec
        .bind_address
        .split(':')
        .last()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap();

    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()))
            .await
            .expect("Failed to create pipeline"),
    );

    let handler = SplunkHecHandler::new(config.splunk_hec.clone(), pipeline, metrics.clone());

    let handler_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = handler.start(handler_shutdown).await;
    });

    // Wait for handler to start listening
    tokio::time::sleep(Duration::from_millis(300)).await;

    let url = format!("http://127.0.0.1:{hec_port}");
    (shutdown, metrics, url)
}

/// Check metrics render output for received events.
fn events_received(metrics: &Metrics) -> u64 {
    let output = metrics.render();
    for line in output.lines() {
        if line.starts_with("receiver_requests_total ") {
            return line
                .trim_start_matches("receiver_requests_total ")
                .trim()
                .parse()
                .unwrap_or(0);
        }
    }
    0
}

// =============================================================================
// Event Endpoint Tests
// =============================================================================

/// Test sending a single HEC event.
#[tokio::test]
async fn test_hec_single_event() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics, url) = start_hec_handler(config).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{url}/services/collector/event"))
        .body(r#"{"event":"hello world"}"#)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["text"], "Success");
    assert_eq!(body["code"], 0);

    assert!(
        events_received(&metrics) > 0,
        "Expected events to be received"
    );

    shutdown.cancel();
}

/// Test sending batched NDJSON events.
#[tokio::test]
async fn test_hec_batch_events() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics, url) = start_hec_handler(config).await;

    let client = reqwest::Client::new();
    let body = r#"{"event":"one"}
{"event":"two"}
{"event":"three"}"#;

    let resp = client
        .post(format!("{url}/services/collector/event"))
        .body(body)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["code"], 0);

    // Metric counts requests, not individual events — 1 request with 3 events
    let received = events_received(&metrics);
    assert!(received >= 1, "Expected at least 1 request, got {received}");

    shutdown.cancel();
}

/// Test sending event with full HEC metadata.
#[tokio::test]
async fn test_hec_event_with_metadata() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, _metrics, url) = start_hec_handler(config).await;

    let client = reqwest::Client::new();
    let body = r#"{"event":{"msg":"test"},"time":1447828325.5,"host":"web01","source":"app","sourcetype":"json","index":"main","fields":{"env":"prod"}}"#;

    let resp = client
        .post(format!("{url}/services/collector/event"))
        .body(body)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["code"], 0);

    shutdown.cancel();
}

/// Test that empty body returns HEC error code 5 (no data).
#[tokio::test]
async fn test_hec_empty_body() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, _metrics, url) = start_hec_handler(config).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{url}/services/collector/event"))
        .body("")
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(resp.status(), 400);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["code"], 5);

    shutdown.cancel();
}

/// Test that invalid JSON returns HEC error code 6 (invalid data format).
#[tokio::test]
async fn test_hec_invalid_json() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, _metrics, url) = start_hec_handler(config).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{url}/services/collector/event"))
        .body("not json at all")
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(resp.status(), 400);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["code"], 6);

    shutdown.cancel();
}

// =============================================================================
// Raw Endpoint Tests
// =============================================================================

/// Test sending raw text events.
#[tokio::test]
async fn test_hec_raw_events() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics, url) = start_hec_handler(config).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{url}/services/collector/raw?source=syslog&sourcetype=syslog"
        ))
        .body("line one\nline two\nline three\n")
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["code"], 0);

    // Metric counts requests, not individual events — 1 request with 3 lines
    let received = events_received(&metrics);
    assert!(received >= 1, "Expected at least 1 request, got {received}");

    shutdown.cancel();
}

// =============================================================================
// Health Endpoint Tests
// =============================================================================

/// Test health endpoint returns correct HEC format.
#[tokio::test]
async fn test_hec_health() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, _metrics, url) = start_hec_handler(config).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{url}/services/collector/health"))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["text"], "HEC is healthy");
    assert_eq!(json["code"], 17);

    shutdown.cancel();
}

// =============================================================================
// Authentication Tests
// =============================================================================

/// Test that Splunk auth prefix is accepted.
#[tokio::test]
async fn test_hec_auth_splunk_prefix() {
    let port = random_port();
    let token = "test-hec-token-123";
    let config = test_config_with_auth(port, token);
    let (shutdown, _metrics, url) = start_hec_handler(config).await;

    let client = reqwest::Client::new();

    // Valid Splunk auth
    let resp = client
        .post(format!("{url}/services/collector/event"))
        .header("Authorization", format!("Splunk {token}"))
        .body(r#"{"event":"test"}"#)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(resp.status(), 200);

    shutdown.cancel();
}

/// Test that Bearer auth prefix is also accepted.
#[tokio::test]
async fn test_hec_auth_bearer_prefix() {
    let port = random_port();
    let token = "test-hec-token-456";
    let config = test_config_with_auth(port, token);
    let (shutdown, _metrics, url) = start_hec_handler(config).await;

    let client = reqwest::Client::new();

    // Valid Bearer auth
    let resp = client
        .post(format!("{url}/services/collector/event"))
        .header("Authorization", format!("Bearer {token}"))
        .body(r#"{"event":"test"}"#)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(resp.status(), 200);

    shutdown.cancel();
}

/// Test that missing/invalid auth is rejected.
#[tokio::test]
async fn test_hec_auth_rejected() {
    let port = random_port();
    let config = test_config_with_auth(port, "correct-token");
    let (shutdown, _metrics, url) = start_hec_handler(config).await;

    let client = reqwest::Client::new();

    // No auth header
    let resp = client
        .post(format!("{url}/services/collector/event"))
        .body(r#"{"event":"test"}"#)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(resp.status(), 401);

    // Wrong token
    let resp = client
        .post(format!("{url}/services/collector/event"))
        .header("Authorization", "Splunk wrong-token")
        .body(r#"{"event":"test"}"#)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(resp.status(), 401);

    shutdown.cancel();
}

/// Test the /services/collector/event/1.0 versioned endpoint.
#[tokio::test]
async fn test_hec_versioned_endpoint() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, _metrics, url) = start_hec_handler(config).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{url}/services/collector/event/1.0"))
        .body(r#"{"event":"versioned"}"#)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(resp.status(), 200);

    shutdown.cancel();
}
