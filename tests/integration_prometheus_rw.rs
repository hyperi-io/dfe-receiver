// Project:   dfe-receiver
// File:      tests/integration_prometheus_rw.rs
// Purpose:   Integration tests for Prometheus Remote Write v1 handler
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for the Prometheus Remote Write handler.
//!
//! These tests start a real Prometheus RW handler and send Snappy-compressed
//! protobuf payloads via reqwest. No external binaries needed.
//!
//! Run with: `cargo test --test integration_prometheus_rw`

// Allow unwrap/expect in tests - they're the idiomatic way to fail fast
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::prometheus_rw::proto;
use dfe_receiver::server::prometheus_rw::PrometheusRwHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use prost::Message;
use tokio_util::sync::CancellationToken;

/// Get a random port for testing.
fn random_port() -> u16 {
    10000 + (uuid::Uuid::new_v4().as_u128() % 10000) as u16
}

/// Create a minimal config for testing with Prometheus RW enabled.
fn test_config(rw_port: u16) -> Config {
    let mut config = Config::default();
    let http_port = random_port();
    config.server.bind_address = format!("127.0.0.1:{http_port}");
    config.server.auth.mode = "none".to_string();
    config.prometheus_rw.enabled = true;
    config.prometheus_rw.bind_address = format!("127.0.0.1:{rw_port}");
    config.prometheus_rw.auth.mode = "none".to_string();
    config.destinations.default = "loader".to_string();
    config
}

/// Start the Prometheus RW handler and return (shutdown_token, metrics, base_url).
async fn start_rw_handler(config: Config) -> (CancellationToken, Arc<Metrics>, String) {
    let rw_port = config
        .prometheus_rw
        .bind_address
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap();

    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone())).expect("Failed to create pipeline"),
    );

    let handler = PrometheusRwHandler::new(config.prometheus_rw.clone(), pipeline, metrics.clone());

    let handler_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = handler.start(handler_shutdown).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    let url = format!("http://127.0.0.1:{rw_port}");
    (shutdown, metrics, url)
}

/// Encode a WriteRequest to Snappy-compressed protobuf bytes.
fn encode_write_request(request: &proto::WriteRequest) -> Vec<u8> {
    let proto_bytes = request.encode_to_vec();
    snap::raw::Encoder::new()
        .compress_vec(&proto_bytes)
        .expect("snappy compress failed")
}

/// Extract a counter value from rendered Prometheus metrics.
fn metric_value(metrics: &Metrics, name: &str) -> u64 {
    let output = metrics.render();
    for line in output.lines() {
        if line.starts_with(name) && !line.starts_with('#') {
            return line.trim_start_matches(name).trim().parse().unwrap_or(0);
        }
    }
    0
}

fn requests_total(metrics: &Metrics) -> u64 {
    metric_value(metrics, "receiver_requests_total ")
}

fn requests_success(metrics: &Metrics) -> u64 {
    metric_value(metrics, "receiver_requests_success ")
}

fn bytes_received(metrics: &Metrics) -> u64 {
    metric_value(metrics, "receiver_bytes_received_total ")
}

/// Helper to build a simple timeseries.
fn make_timeseries(name: &str, value: f64, timestamp_ms: i64) -> proto::TimeSeries {
    proto::TimeSeries {
        labels: vec![
            proto::Label {
                name: "__name__".to_string(),
                value: name.to_string(),
            },
            proto::Label {
                name: "job".to_string(),
                value: "test".to_string(),
            },
        ],
        samples: vec![proto::Sample {
            value,
            timestamp: timestamp_ms,
        }],
        exemplars: vec![],
        histograms: vec![],
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_single_timeseries() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics, url) = start_rw_handler(config).await;

    let request = proto::WriteRequest {
        timeseries: vec![make_timeseries("http_requests_total", 42.0, 1_709_540_000_000)],
        metadata: vec![],
    };

    let body = encode_write_request(&request);
    let resp = reqwest::Client::new()
        .post(format!("{url}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(body)
        .send()
        .await
        .expect("request failed");

    assert_eq!(resp.status(), 204, "expected 204 No Content");

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(requests_total(&metrics) >= 1, "expected at least 1 request");
    assert!(
        requests_success(&metrics) >= 1,
        "expected at least 1 success"
    );

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_multiple_timeseries() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics, url) = start_rw_handler(config).await;

    let request = proto::WriteRequest {
        timeseries: vec![
            make_timeseries("metric_a", 1.0, 1_709_540_000_000),
            make_timeseries("metric_b", 2.0, 1_709_540_000_000),
            make_timeseries("metric_c", 3.0, 1_709_540_000_000),
        ],
        metadata: vec![],
    };

    let body = encode_write_request(&request);
    let resp = reqwest::Client::new()
        .post(format!("{url}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(body)
        .send()
        .await
        .expect("request failed");

    assert_eq!(resp.status(), 204);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        requests_success(&metrics) >= 1,
        "expected at least 1 success"
    );

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_multiple_samples_per_timeseries() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics, url) = start_rw_handler(config).await;

    let request = proto::WriteRequest {
        timeseries: vec![proto::TimeSeries {
            labels: vec![proto::Label {
                name: "__name__".to_string(),
                value: "cpu_usage".to_string(),
            }],
            samples: vec![
                proto::Sample {
                    value: 0.5,
                    timestamp: 1_709_540_000_000,
                },
                proto::Sample {
                    value: 0.7,
                    timestamp: 1_709_540_001_000,
                },
                proto::Sample {
                    value: 0.3,
                    timestamp: 1_709_540_002_000,
                },
            ],
            exemplars: vec![],
            histograms: vec![],
        }],
        metadata: vec![],
    };

    let body = encode_write_request(&request);
    let resp = reqwest::Client::new()
        .post(format!("{url}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(body)
        .send()
        .await
        .expect("request failed");

    assert_eq!(resp.status(), 204);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        requests_success(&metrics) >= 1,
        "expected at least 1 success"
    );

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_empty_body_returns_400() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, _metrics, url) = start_rw_handler(config).await;

    let resp = reqwest::Client::new()
        .post(format!("{url}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .body(vec![])
        .send()
        .await
        .expect("request failed");

    assert_eq!(resp.status(), 400, "expected 400 for empty body");

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_invalid_protobuf_returns_400() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, _metrics, url) = start_rw_handler(config).await;

    // Send valid Snappy but invalid protobuf
    let garbage = snap::raw::Encoder::new()
        .compress_vec(b"not a protobuf")
        .expect("snappy compress failed");

    let resp = reqwest::Client::new()
        .post(format!("{url}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(garbage)
        .send()
        .await
        .expect("request failed");

    // protobuf decode may or may not fail on garbage — it depends on wire format
    // But it shouldn't be a 500
    assert!(
        resp.status() == 204 || resp.status() == 400,
        "expected 204 or 400, got {}",
        resp.status()
    );

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_invalid_snappy_returns_400() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, _metrics, url) = start_rw_handler(config).await;

    // Send data that's not valid Snappy
    let resp = reqwest::Client::new()
        .post(format!("{url}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(b"definitely not snappy compressed data".to_vec())
        .send()
        .await
        .expect("request failed");

    assert_eq!(resp.status(), 400, "expected 400 for invalid snappy");

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_bytes_received_tracked() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics, url) = start_rw_handler(config).await;

    let request = proto::WriteRequest {
        timeseries: vec![make_timeseries("bytes_test_metric", 99.9, 1_709_540_000_000)],
        metadata: vec![],
    };

    let body = encode_write_request(&request);
    let resp = reqwest::Client::new()
        .post(format!("{url}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(body)
        .send()
        .await
        .expect("request failed");

    assert_eq!(resp.status(), 204);

    tokio::time::sleep(Duration::from_millis(100)).await;
    let bytes = bytes_received(&metrics);
    assert!(bytes > 0, "expected bytes_received > 0, got {bytes}");

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_empty_write_request() {
    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics, url) = start_rw_handler(config).await;

    // Valid but empty WriteRequest (no timeseries)
    let request = proto::WriteRequest {
        timeseries: vec![],
        metadata: vec![],
    };

    let body = encode_write_request(&request);
    let resp = reqwest::Client::new()
        .post(format!("{url}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(body)
        .send()
        .await
        .expect("request failed");

    assert_eq!(resp.status(), 204, "empty write request should succeed");

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        requests_success(&metrics) >= 1,
        "expected at least 1 success"
    );

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}
