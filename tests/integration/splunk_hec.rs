// Project:   dfe-receiver
// File:      tests/integration_splunk_hec.rs
// Purpose:   Integration tests for Splunk HEC protocol handler
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::double_ended_iterator_last)]

//! Integration tests for the Splunk HEC handler.
//!
//! These tests start a real Splunk HEC handler and send requests via reqwest.
//! No external binaries are needed — HEC is just HTTP.
//!
//! Run with: `cargo test --test integration_splunk_hec`

// Allow unwrap/expect in tests - they're the idiomatic way to fail fast
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
// Test helpers build a PipelineState inline; the config structs put the future
// just over clippy's 16 KiB threshold. Mirrors the lib crate's allow (main.rs).
#![allow(clippy::large_futures)]

use std::sync::Arc;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::splunk_hec::SplunkHecHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use tokio_util::sync::CancellationToken;

/// Create a minimal config for testing with Splunk HEC enabled.
fn test_config() -> Config {
    let mut config = Config::default();
    // HTTP server still needs a bind address (always enabled)
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    // Enable Splunk HEC
    config.splunk_hec.enabled = true;
    config.splunk_hec.bind_address = "127.0.0.1:0".to_string();
    config.splunk_hec.auth.mode = "none".to_string();
    // The loader on its memory transport: accepted, sent nowhere, no broker.
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config
}

/// Create a config with bearer auth enabled.
fn test_config_with_auth(token: &str) -> Config {
    let mut config = test_config();
    config.splunk_hec.auth.mode = "bearer".to_string();
    config.splunk_hec.auth.bearer.tokens = vec![token.into()];
    config
}

/// Start the HEC handler and return (shutdown_token, metrics, base_url).
async fn start_hec_handler(config: Config) -> (CancellationToken, Arc<Metrics>, String) {
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

    let handler = SplunkHecHandler::new(
        config.splunk_hec.clone(),
        config.raw_capture_for(&config.splunk_hec.raw_capture),
        pipeline,
        metrics.clone(),
    );
    let bound = handler.bound_addr();

    let handler_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(handler_shutdown).await });
    let addr = crate::common::bound_addr("HEC", &bound, &mut task).await;

    (shutdown, metrics, format!("http://{addr}"))
}

/// Get total events received from metrics.
fn events_received(metrics: &Metrics) -> u64 {
    metrics.get_requests_total()
}

// =============================================================================
// Event Endpoint Tests
// =============================================================================

/// Test sending a single HEC event.
#[tokio::test]
async fn test_hec_single_event() {
    let config = test_config();
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
    let config = test_config();
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
    let config = test_config();
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
    let config = test_config();
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
    let config = test_config();
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
    let config = test_config();
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
    let config = test_config();
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
    let token = "test-hec-token-123";
    let config = test_config_with_auth(token);
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
    let token = "test-hec-token-456";
    let config = test_config_with_auth(token);
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
    let config = test_config_with_auth("correct-token");
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
    let config = test_config();
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

// =============================================================================
// Content-Encoding and the bare route
// =============================================================================

fn gzip(data: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

/// A receiver whose loader is an in-process destination, so a test sees what
/// was stored.
async fn start_with_destination(
    max_body_size: Option<usize>,
) -> (
    CancellationToken,
    Arc<Metrics>,
    String,
    scalo::transport::grpc::GrpcTransport,
) {
    let (endpoint, destination) = crate::common::grpc_destination().await;
    let mut config = test_config();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(endpoint);
    config.routing.dlq.enabled = false;
    if let Some(size) = max_body_size {
        config.splunk_hec.max_body_size = size;
    }
    let (shutdown, metrics, url) = start_hec_handler(config).await;
    (shutdown, metrics, url, destination)
}

/// What a sender's body and path get from the listener, and how many records
/// land: gzip is undone on `/event`, the bare path and `/raw` (the OTel
/// Collector's splunkhec exporter gzips by default), and any other encoding is
/// refused 415 with nothing stored.
#[tokio::test]
async fn hec_reads_gzip_and_refuses_other_encodings() {
    // (path, Content-Encoding, body, status, records stored)
    let events = br#"{"event":"one"}{"event":"two"}"#;
    let lines = b"line one\nline two\nline three\n";
    let cases = vec![
        ("event", Some("gzip"), gzip(events), 200, 2),
        ("event", None, events.to_vec(), 200, 2),
        ("", None, events.to_vec(), 200, 2),
        ("", Some("gzip"), gzip(events), 200, 2),
        ("raw", Some("gzip"), gzip(lines), 200, 3),
        ("event", Some("zstd"), gzip(events), 415, 0),
        ("raw", Some("deflate"), gzip(lines), 415, 0),
        ("event", Some("gzip"), b"not gzip".to_vec(), 400, 0),
        ("raw", Some("gzip"), b"not gzip".to_vec(), 400, 0),
    ];
    for (path, encoding, body, status, stored) in cases {
        let (shutdown, metrics, url, destination) = start_with_destination(None).await;
        let mut request = reqwest::Client::new().post(format!(
            "{url}/services/collector{}",
            if path.is_empty() {
                String::new()
            } else {
                format!("/{path}")
            }
        ));
        if let Some(encoding) = encoding {
            request = request.header("Content-Encoding", encoding);
        }
        let resp = request.body(body).send().await.expect("request failed");
        let got = resp.status().as_u16();
        let text = resp.text().await.unwrap();
        assert_eq!(got, status, "{path} {encoding:?}: {text}");

        let wait = if stored == 0 {
            std::time::Duration::from_millis(500)
        } else {
            std::time::Duration::from_secs(10)
        };
        let records = crate::common::stored_within(&destination, stored, wait).await;
        assert_eq!(records.len(), stored, "{path} {encoding:?}: {records:?}");
        if path == "raw" && stored > 0 {
            let messages: Vec<String> = records
                .iter()
                .map(|r| {
                    let v: serde_json::Value = serde_json::from_slice(r).unwrap();
                    v["message"].as_str().unwrap().to_string()
                })
                .collect();
            assert_eq!(messages, ["line one", "line two", "line three"]);
        }
        assert_eq!(
            metrics.get_unsupported_encoding_total(),
            u64::from(status == 415),
            "{path} {encoding:?}"
        );
        shutdown.cancel();
    }
}

/// A gzip body that inflates past `max_body_size` is refused 413 and stores
/// nothing: the cap applies to the decompressed bytes, not the wire bytes.
#[tokio::test]
async fn hec_refuses_a_gzip_bomb() {
    let (shutdown, _metrics, url, destination) = start_with_destination(Some(2048)).await;
    let bomb = gzip(&vec![b'a'; 1 << 20]);
    assert!(
        bomb.len() < 2048,
        "test setup: the bomb must fit the wire cap"
    );

    let resp = reqwest::Client::new()
        .post(format!("{url}/services/collector/raw"))
        .header("Content-Encoding", "gzip")
        .body(bomb)
        .send()
        .await
        .expect("request failed");

    assert_eq!(resp.status(), 413);
    let stored =
        crate::common::stored_within(&destination, 0, std::time::Duration::from_millis(500)).await;
    assert!(stored.is_empty(), "stored {} records", stored.len());
    shutdown.cancel();
}
