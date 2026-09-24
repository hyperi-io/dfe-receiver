// Project:   dfe-receiver
// File:      tests/integration_otlp.rs
// Purpose:   Integration tests for OTLP gRPC and HTTP protocol handlers
// Language:  Rust
//
// License:   BUSL-1.1
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
// Test helpers build a PipelineState inline; the config structs put the future
// just over clippy's 16 KiB threshold. Mirrors the lib crate's allow (main.rs).
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::sync::Arc;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::otlp::OtlpHandler;
use dfe_receiver::server::otlp::pb;
use dfe_receiver::server::traits::ProtocolHandler;
use prost::Message;
use tokio_util::sync::CancellationToken;

/// Create a minimal config for testing with OTLP enabled.
fn test_config() -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    config.otlp.enabled = true;
    config.otlp.grpc_bind_address = "127.0.0.1:0".to_string();
    config.otlp.http_bind_address = "127.0.0.1:0".to_string();
    config.otlp.auth.mode = "none".to_string();
    // The loader on its memory transport: accepted, sent nowhere, no broker.
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config
}

/// A running OTLP handler and the addresses its two listeners bound.
struct Started {
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
    grpc: SocketAddr,
    http: SocketAddr,
}

/// Start the OTLP handler and wait for both listeners to bind.
async fn start_otlp_handler(config: Config) -> Started {
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

    let handler = OtlpHandler::new(
        config.otlp.clone(),
        config.raw_capture_for(&config.otlp.raw_capture),
        pipeline,
        metrics.clone(),
    );
    let grpc_bound = handler.grpc_bound_addr();
    let http_bound = handler.http_bound_addr();

    let handler_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(handler_shutdown).await });
    let grpc = crate::common::bound_addr("OTLP gRPC", &grpc_bound, &mut task).await;
    let http = crate::common::bound_addr("OTLP HTTP", &http_bound, &mut task).await;

    Started {
        shutdown,
        metrics,
        grpc,
        http,
    }
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

    let otlp = start_otlp_handler(test_config()).await;

    let mut client = LogsServiceClient::connect(format!("http://{}", otlp.grpc))
        .await
        .expect("Failed to connect to OTLP gRPC");

    let request = build_logs_request();
    let response = client.export(request).await;
    assert!(
        response.is_ok(),
        "OTLP gRPC logs export failed: {response:?}"
    );

    assert!(
        requests_total(&otlp.metrics) > 0,
        "Expected requests to be counted"
    );
    assert!(
        bytes_received(&otlp.metrics) > 0,
        "Expected bytes to be counted"
    );

    otlp.shutdown.cancel();
}

/// Test sending traces via OTLP gRPC.
#[tokio::test]
async fn test_otlp_grpc_traces() {
    use pb::collector::trace::v1::trace_service_client::TraceServiceClient;

    let otlp = start_otlp_handler(test_config()).await;

    let mut client = TraceServiceClient::connect(format!("http://{}", otlp.grpc))
        .await
        .expect("Failed to connect to OTLP gRPC");

    let request = build_traces_request();
    let response = client.export(request).await;
    assert!(
        response.is_ok(),
        "OTLP gRPC traces export failed: {response:?}"
    );

    assert!(
        requests_total(&otlp.metrics) > 0,
        "Expected requests to be counted"
    );

    otlp.shutdown.cancel();
}

/// Test sending metrics via OTLP gRPC.
#[tokio::test]
async fn test_otlp_grpc_metrics() {
    use pb::collector::metrics::v1::metrics_service_client::MetricsServiceClient;

    let otlp = start_otlp_handler(test_config()).await;

    let mut client = MetricsServiceClient::connect(format!("http://{}", otlp.grpc))
        .await
        .expect("Failed to connect to OTLP gRPC");

    let request = build_metrics_request();
    let response = client.export(request).await;
    assert!(
        response.is_ok(),
        "OTLP gRPC metrics export failed: {response:?}"
    );

    assert!(
        requests_total(&otlp.metrics) > 0,
        "Expected requests to be counted"
    );

    otlp.shutdown.cancel();
}

// =============================================================================
// HTTP Tests (port 4318)
// =============================================================================

/// Test sending logs via OTLP HTTP.
#[tokio::test]
async fn test_otlp_http_logs() {
    let otlp = start_otlp_handler(test_config()).await;

    let client = reqwest::Client::new();
    let request = build_logs_request();
    let body = request.encode_to_vec();

    let resp = client
        .post(format!("http://{}/v1/logs", otlp.http))
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
        requests_total(&otlp.metrics) > 0,
        "Expected requests to be counted"
    );
    assert!(
        bytes_received(&otlp.metrics) > 0,
        "Expected bytes to be counted"
    );

    otlp.shutdown.cancel();
}

/// Test sending traces via OTLP HTTP.
#[tokio::test]
async fn test_otlp_http_traces() {
    let otlp = start_otlp_handler(test_config()).await;

    let client = reqwest::Client::new();
    let request = build_traces_request();
    let body = request.encode_to_vec();

    let resp = client
        .post(format!("http://{}/v1/traces", otlp.http))
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
        requests_total(&otlp.metrics) > 0,
        "Expected requests to be counted"
    );

    otlp.shutdown.cancel();
}

/// Test sending metrics via OTLP HTTP.
#[tokio::test]
async fn test_otlp_http_metrics() {
    let otlp = start_otlp_handler(test_config()).await;

    let client = reqwest::Client::new();
    let request = build_metrics_request();
    let body = request.encode_to_vec();

    let resp = client
        .post(format!("http://{}/v1/metrics", otlp.http))
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
        requests_total(&otlp.metrics) > 0,
        "Expected requests to be counted"
    );

    otlp.shutdown.cancel();
}

/// Test that invalid protobuf returns 400 on HTTP.
#[tokio::test]
async fn test_otlp_http_invalid_protobuf() {
    let otlp = start_otlp_handler(test_config()).await;

    let client = reqwest::Client::new();

    let resp = client
        .post(format!("http://{}/v1/logs", otlp.http))
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

    otlp.shutdown.cancel();
}

/// Test that OTLP HTTP rejects unsupported JSON content-type.
#[tokio::test]
async fn test_otlp_http_json_unsupported() {
    let otlp = start_otlp_handler(test_config()).await;

    let client = reqwest::Client::new();

    let resp = client
        .post(format!("http://{}/v1/logs", otlp.http))
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

    otlp.shutdown.cancel();
}

/// Test bytes received tracking across multiple requests.
#[tokio::test]
async fn test_otlp_bytes_received_tracked() {
    let otlp = start_otlp_handler(test_config()).await;

    let client = reqwest::Client::new();

    // Send logs
    let request = build_logs_request();
    let body = request.encode_to_vec();
    let resp = client
        .post(format!("http://{}/v1/logs", otlp.http))
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
        .post(format!("http://{}/v1/traces", otlp.http))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("Failed to send request");
    assert_eq!(resp.status(), 200);

    // Verify cumulative metrics
    assert!(
        requests_total(&otlp.metrics) >= 2,
        "Expected at least 2 requests, got {}",
        requests_total(&otlp.metrics)
    );
    assert!(
        bytes_received(&otlp.metrics) > 0,
        "Expected bytes to be tracked"
    );

    otlp.shutdown.cancel();
}

// =============================================================================
// Auth on the HTTP endpoint
// =============================================================================
//
// `otlp.auth` is one block for both endpoints. It was applied to the gRPC
// server only: run_http_server built its Router with three POST routes and no
// auth layer, so a bearer mode with tokens configured closed 4317 and left 4318
// accepting anything.

/// A config with OTLP bearer auth armed on both endpoints.
fn bearer_config() -> Config {
    let mut config = test_config();
    config.otlp.auth.mode = "bearer".to_string();
    config.otlp.auth.bearer.tokens = vec!["otlp-secret".to_string()];
    config
}

#[tokio::test]
async fn otlp_http_rejects_a_post_with_no_token() {
    let otlp = start_otlp_handler(bearer_config()).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{}/v1/logs", otlp.http))
        .header("content-type", "application/x-protobuf")
        .body(build_logs_request().encode_to_vec())
        .send()
        .await
        .expect("Failed to send OTLP HTTP request");

    assert_eq!(
        resp.status(),
        401,
        "an unauthenticated post reached the OTLP HTTP endpoint under bearer mode"
    );
    assert_eq!(
        requests_total(&otlp.metrics),
        0,
        "the rejected post must not reach the pipeline"
    );

    otlp.shutdown.cancel();
}

#[tokio::test]
async fn otlp_http_rejects_a_post_with_the_wrong_token() {
    let otlp = start_otlp_handler(bearer_config()).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{}/v1/traces", otlp.http))
        .header("content-type", "application/x-protobuf")
        .header("authorization", "Bearer not-the-token")
        .body(build_traces_request().encode_to_vec())
        .send()
        .await
        .expect("Failed to send OTLP HTTP request");

    assert_eq!(resp.status(), 401);

    otlp.shutdown.cancel();
}

#[tokio::test]
async fn otlp_http_accepts_a_post_with_the_configured_token() {
    let otlp = start_otlp_handler(bearer_config()).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{}/v1/logs", otlp.http))
        .header("content-type", "application/x-protobuf")
        .header("authorization", "Bearer otlp-secret")
        .body(build_logs_request().encode_to_vec())
        .send()
        .await
        .expect("Failed to send OTLP HTTP request");

    assert_eq!(
        resp.status(),
        200,
        "a correctly authenticated post was refused: {:?}",
        resp.text().await
    );
    assert!(requests_total(&otlp.metrics) > 0);

    otlp.shutdown.cancel();
}
