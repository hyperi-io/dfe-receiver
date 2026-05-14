// Project:   dfe-receiver
// File:      tests/integration_otlp.rs
// Purpose:   Integration tests for OTLP gRPC and HTTP protocol handlers
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for the OTLP handler.
//!
//! Tests send real OTLP protobuf data over gRPC (port 4317) and HTTP (port 4318)
//! to a running OTLP handler and verify the pipeline processes them.
//!
//! Run with: `cargo test --test integration_otlp`

// Only compile when otlp feature is enabled
#![cfg(feature = "otlp")]
// Allow unwrap/expect in tests
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::otlp::OtlpHandler;
use dfe_receiver::server::otlp::pb;
use dfe_receiver::server::traits::ProtocolHandler;
use prost::Message;
use tokio_util::sync::CancellationToken;

/// Get a random port for testing.
fn random_port() -> u16 {
    10000 + (uuid::Uuid::new_v4().as_u128() % 10000) as u16
}

/// Create a minimal config for testing with OTLP enabled.
fn test_config(grpc_port: u16, http_port: u16) -> Config {
    let mut config = Config::default();
    let main_http_port = random_port();
    config.server.bind_address = format!("127.0.0.1:{main_http_port}");
    config.server.auth.mode = "none".to_string();
    config.otlp.enabled = true;
    config.otlp.grpc_bind_address = format!("127.0.0.1:{grpc_port}");
    config.otlp.http_bind_address = format!("127.0.0.1:{http_port}");
    config.otlp.auth.mode = "none".to_string();
    config.destinations.default = "loader".to_string();
    config
}

/// Start the OTLP handler and return (shutdown_token, metrics).
async fn start_otlp_handler(config: Config) -> (CancellationToken, Arc<Metrics>) {
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

    let handler = OtlpHandler::new(config.otlp.clone(), pipeline, metrics.clone());

    let handler_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = handler.start(handler_shutdown).await;
    });

    // Wait for handler to start listening
    tokio::time::sleep(Duration::from_millis(500)).await;

    (shutdown, metrics)
}

/// Get total requests from metrics.
fn requests_total(metrics: &Metrics) -> u64 {
    metrics.get_requests_total()
}

/// Get bytes received from metrics.
fn bytes_received(metrics: &Metrics) -> u64 {
    metrics.get_bytes_received()
}

/// Build a minimal OTLP ExportLogsServiceRequest with one log record.
fn build_logs_request() -> pb::collector::logs::v1::ExportLogsServiceRequest {
    use pb::common::v1::{AnyValue, KeyValue, any_value};
    use pb::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use pb::resource::v1::Resource;

    pb::collector::logs::v1::ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".into())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    observed_time_unix_nano: 1_700_000_000_000_000_000,
                    severity_number: 9, // INFO
                    severity_text: "INFO".into(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("Test log message".into())),
                    }),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Build a minimal OTLP ExportTraceServiceRequest with one span.
fn build_traces_request() -> pb::collector::trace::v1::ExportTraceServiceRequest {
    use pb::common::v1::{AnyValue, KeyValue, any_value};
    use pb::resource::v1::Resource;
    use pb::trace::v1::{ResourceSpans, ScopeSpans, Span};

    pb::collector::trace::v1::ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".into())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    parent_span_id: vec![],
                    name: "test-span".into(),
                    kind: 1, // INTERNAL
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    end_time_unix_nano: 1_700_000_001_000_000_000,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: None,
                    trace_state: String::new(),
                    flags: 0,
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Build a minimal OTLP ExportMetricsServiceRequest with one gauge.
fn build_metrics_request() -> pb::collector::metrics::v1::ExportMetricsServiceRequest {
    use pb::common::v1::{AnyValue, KeyValue, any_value};
    use pb::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, number_data_point,
    };
    use pb::resource::v1::Resource;

    pb::collector::metrics::v1::ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".into())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "test.gauge".into(),
                    description: "A test gauge".into(),
                    unit: "bytes".into(),
                    metadata: vec![],
                    data: Some(pb::metrics::v1::metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: 0,
                            time_unix_nano: 1_700_000_000_000_000_000,
                            value: Some(number_data_point::Value::AsDouble(42.0)),
                            exemplars: vec![],
                            flags: 0,
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

// =============================================================================
// gRPC Tests (port 4317)
// =============================================================================

/// Test sending logs via OTLP gRPC.
#[tokio::test]
async fn test_otlp_grpc_logs() {
    use pb::collector::logs::v1::logs_service_client::LogsServiceClient;

    let grpc_port = random_port();
    let http_port = random_port();
    let config = test_config(grpc_port, http_port);
    let (shutdown, metrics) = start_otlp_handler(config).await;

    let mut client = LogsServiceClient::connect(format!("http://127.0.0.1:{grpc_port}"))
        .await
        .expect("Failed to connect to OTLP gRPC");

    let request = build_logs_request();
    let response = client.export(request).await;
    assert!(
        response.is_ok(),
        "OTLP gRPC logs export failed: {response:?}"
    );

    assert!(
        requests_total(&metrics) > 0,
        "Expected requests to be counted"
    );
    assert!(bytes_received(&metrics) > 0, "Expected bytes to be counted");

    shutdown.cancel();
}

/// Test sending traces via OTLP gRPC.
#[tokio::test]
async fn test_otlp_grpc_traces() {
    use pb::collector::trace::v1::trace_service_client::TraceServiceClient;

    let grpc_port = random_port();
    let http_port = random_port();
    let config = test_config(grpc_port, http_port);
    let (shutdown, metrics) = start_otlp_handler(config).await;

    let mut client = TraceServiceClient::connect(format!("http://127.0.0.1:{grpc_port}"))
        .await
        .expect("Failed to connect to OTLP gRPC");

    let request = build_traces_request();
    let response = client.export(request).await;
    assert!(
        response.is_ok(),
        "OTLP gRPC traces export failed: {response:?}"
    );

    assert!(
        requests_total(&metrics) > 0,
        "Expected requests to be counted"
    );

    shutdown.cancel();
}

/// Test sending metrics via OTLP gRPC.
#[tokio::test]
async fn test_otlp_grpc_metrics() {
    use pb::collector::metrics::v1::metrics_service_client::MetricsServiceClient;

    let grpc_port = random_port();
    let http_port = random_port();
    let config = test_config(grpc_port, http_port);
    let (shutdown, metrics) = start_otlp_handler(config).await;

    let mut client = MetricsServiceClient::connect(format!("http://127.0.0.1:{grpc_port}"))
        .await
        .expect("Failed to connect to OTLP gRPC");

    let request = build_metrics_request();
    let response = client.export(request).await;
    assert!(
        response.is_ok(),
        "OTLP gRPC metrics export failed: {response:?}"
    );

    assert!(
        requests_total(&metrics) > 0,
        "Expected requests to be counted"
    );

    shutdown.cancel();
}

// =============================================================================
// HTTP Tests (port 4318)
// =============================================================================

/// Test sending logs via OTLP HTTP.
#[tokio::test]
async fn test_otlp_http_logs() {
    let grpc_port = random_port();
    let http_port = random_port();
    let config = test_config(grpc_port, http_port);
    let (shutdown, metrics) = start_otlp_handler(config).await;

    let client = reqwest::Client::new();
    let request = build_logs_request();
    let body = request.encode_to_vec();

    let resp = client
        .post(format!("http://127.0.0.1:{http_port}/v1/logs"))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("Failed to send OTLP HTTP request");

    assert_eq!(
        resp.status(),
        200,
        "OTLP HTTP logs failed: {:?}",
        resp.text().await
    );

    assert!(
        requests_total(&metrics) > 0,
        "Expected requests to be counted"
    );
    assert!(bytes_received(&metrics) > 0, "Expected bytes to be counted");

    shutdown.cancel();
}

/// Test sending traces via OTLP HTTP.
#[tokio::test]
async fn test_otlp_http_traces() {
    let grpc_port = random_port();
    let http_port = random_port();
    let config = test_config(grpc_port, http_port);
    let (shutdown, metrics) = start_otlp_handler(config).await;

    let client = reqwest::Client::new();
    let request = build_traces_request();
    let body = request.encode_to_vec();

    let resp = client
        .post(format!("http://127.0.0.1:{http_port}/v1/traces"))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("Failed to send OTLP HTTP request");

    assert_eq!(
        resp.status(),
        200,
        "OTLP HTTP traces failed: {:?}",
        resp.text().await
    );

    assert!(
        requests_total(&metrics) > 0,
        "Expected requests to be counted"
    );

    shutdown.cancel();
}

/// Test sending metrics via OTLP HTTP.
#[tokio::test]
async fn test_otlp_http_metrics() {
    let grpc_port = random_port();
    let http_port = random_port();
    let config = test_config(grpc_port, http_port);
    let (shutdown, metrics) = start_otlp_handler(config).await;

    let client = reqwest::Client::new();
    let request = build_metrics_request();
    let body = request.encode_to_vec();

    let resp = client
        .post(format!("http://127.0.0.1:{http_port}/v1/metrics"))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("Failed to send OTLP HTTP request");

    assert_eq!(
        resp.status(),
        200,
        "OTLP HTTP metrics failed: {:?}",
        resp.text().await
    );

    assert!(
        requests_total(&metrics) > 0,
        "Expected requests to be counted"
    );

    shutdown.cancel();
}

/// Test that invalid protobuf returns 400 on HTTP.
#[tokio::test]
async fn test_otlp_http_invalid_protobuf() {
    let grpc_port = random_port();
    let http_port = random_port();
    let config = test_config(grpc_port, http_port);
    let (shutdown, _metrics) = start_otlp_handler(config).await;

    let client = reqwest::Client::new();

    let resp = client
        .post(format!("http://127.0.0.1:{http_port}/v1/logs"))
        .header("content-type", "application/x-protobuf")
        .body("not valid protobuf")
        .send()
        .await
        .expect("Failed to send request");

    // Should be a client error (4xx or 5xx depending on implementation)
    assert!(
        resp.status().is_client_error() || resp.status().is_server_error(),
        "Expected error status for invalid protobuf, got {}",
        resp.status()
    );

    shutdown.cancel();
}

/// Test that OTLP HTTP rejects unsupported JSON content-type.
#[tokio::test]
async fn test_otlp_http_json_unsupported() {
    let grpc_port = random_port();
    let http_port = random_port();
    let config = test_config(grpc_port, http_port);
    let (shutdown, _metrics) = start_otlp_handler(config).await;

    let client = reqwest::Client::new();

    let resp = client
        .post(format!("http://127.0.0.1:{http_port}/v1/logs"))
        .header("content-type", "application/json")
        .body(r#"{"resourceLogs":[]}"#)
        .send()
        .await
        .expect("Failed to send request");

    assert!(
        resp.status().is_client_error() || resp.status().is_server_error(),
        "Expected error for unsupported JSON content-type, got {}",
        resp.status()
    );

    shutdown.cancel();
}

/// Test bytes received tracking across multiple requests.
#[tokio::test]
async fn test_otlp_bytes_received_tracked() {
    let grpc_port = random_port();
    let http_port = random_port();
    let config = test_config(grpc_port, http_port);
    let (shutdown, metrics) = start_otlp_handler(config).await;

    let client = reqwest::Client::new();

    // Send logs
    let request = build_logs_request();
    let body = request.encode_to_vec();
    let resp = client
        .post(format!("http://127.0.0.1:{http_port}/v1/logs"))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("Failed to send request");
    assert_eq!(resp.status(), 200);

    // Send traces
    let request = build_traces_request();
    let body = request.encode_to_vec();
    let resp = client
        .post(format!("http://127.0.0.1:{http_port}/v1/traces"))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("Failed to send request");
    assert_eq!(resp.status(), 200);

    // Verify cumulative metrics
    assert!(
        requests_total(&metrics) >= 2,
        "Expected at least 2 requests, got {}",
        requests_total(&metrics)
    );
    assert!(bytes_received(&metrics) > 0, "Expected bytes to be tracked");

    shutdown.cancel();
}
