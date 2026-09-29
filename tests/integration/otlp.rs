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
use std::time::{Duration, Instant};

use dfe_receiver::config::{BUS_DESTINATION, Config, SharedConfig};
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

/// A 10 MiB export is taken: the gRPC endpoint decodes up to
/// `otlp.max_message_size` (16 MiB), past tonic's own 4 MiB default.
#[tokio::test]
async fn test_otlp_grpc_takes_a_ten_mebibyte_export() {
    use pb::collector::logs::v1::logs_service_client::LogsServiceClient;
    use pb::common::v1::{AnyValue, any_value};

    let otlp = start_otlp_handler(test_config()).await;
    let mut request = build_logs_request();
    request.resource_logs[0].scope_logs[0].log_records[0].body = Some(AnyValue {
        value: Some(any_value::Value::StringValue("x".repeat(10 * 1024 * 1024))),
    });

    let mut client = LogsServiceClient::connect(format!("http://{}", otlp.grpc))
        .await
        .expect("Failed to connect to OTLP gRPC");
    let response = client.export(request).await;
    otlp.shutdown.cancel();

    assert!(
        response.is_ok(),
        "a 10 MiB export was refused: {response:?}"
    );
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

/// A held OTLP HTTP export is answered at `otlp.http_max_hold_ms`, not at the
/// 20 s Kafka message timeout, so the exporter is still waiting for the answer.
#[tokio::test]
async fn otlp_http_answers_a_held_export_at_its_hold_cap() {
    let mut config = test_config();
    config.destinations.default = BUS_DESTINATION.into();
    // TEST-NET-1 (RFC 5737): never routable, so no broker confirms the record.
    config.kafka.brokers = vec!["192.0.2.1:9092".to_string()];
    config.otlp.http_max_hold_ms = 1_000;
    let otlp = start_otlp_handler(config).await;

    let started = Instant::now();
    let resp = reqwest::Client::new()
        .post(format!("http://{}/v1/logs", otlp.http))
        .header("content-type", "application/x-protobuf")
        .body(build_logs_request().encode_to_vec())
        .send()
        .await
        .expect("Failed to send OTLP HTTP request");
    let answered_in = started.elapsed();

    assert_eq!(resp.status(), 503);
    assert!(resp.headers().contains_key("retry-after"));
    assert!(
        answered_in < Duration::from_secs(10),
        "answered after {answered_in:?}, not at the 1 s hold cap"
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
    config.otlp.auth.bearer.tokens = vec!["otlp-secret".into()];
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

/// A refused credential counts on the auth-failure counter on either endpoint.
#[tokio::test]
async fn otlp_counts_a_refused_credential_on_both_endpoints() {
    use pb::collector::logs::v1::logs_service_client::LogsServiceClient;

    let otlp = start_otlp_handler(bearer_config()).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{}/v1/logs", otlp.http))
        .header("content-type", "application/x-protobuf")
        .body(build_logs_request().encode_to_vec())
        .send()
        .await
        .expect("Failed to send OTLP HTTP request");
    assert_eq!(resp.status(), 401);

    let mut client = LogsServiceClient::connect(format!("http://{}", otlp.grpc))
        .await
        .expect("Failed to connect to OTLP gRPC");
    let status = client
        .export(build_logs_request())
        .await
        .expect_err("an export with no token is refused");
    otlp.shutdown.cancel();

    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert_eq!(otlp.metrics.get_auth_failures_total(), 2);
}

// =============================================================================
// Request limits on the HTTP endpoint, and the counters behind them
// =============================================================================

/// An OTLP HTTP export past `server.max_body_size` is refused with 413 before
/// any handler runs, and counted.
#[tokio::test]
async fn otlp_http_refuses_a_body_past_the_server_limit() {
    let mut config = test_config();
    config.server.max_body_size = 1024;
    let otlp = start_otlp_handler(config).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{}/v1/logs", otlp.http))
        .header("content-type", "application/x-protobuf")
        .body(vec![0u8; 4096])
        .send()
        .await
        .expect("Failed to send OTLP HTTP request");
    otlp.shutdown.cancel();

    assert_eq!(resp.status(), 413);
    assert_eq!(otlp.metrics.get_body_size_rejected_total(), 1);
    assert_eq!(
        requests_total(&otlp.metrics),
        0,
        "the refused export must not reach a handler"
    );
}

/// An export larger than axum's own 2 MiB default but inside
/// `server.max_body_size` is taken: the configured limit is the one that applies.
#[tokio::test]
async fn otlp_http_takes_a_body_up_to_the_server_limit() {
    use pb::common::v1::{AnyValue, any_value};

    let otlp = start_otlp_handler(test_config()).await;
    let mut request = build_logs_request();
    request.resource_logs[0].scope_logs[0].log_records[0].body = Some(AnyValue {
        value: Some(any_value::Value::StringValue("x".repeat(3 * 1024 * 1024))),
    });

    let resp = reqwest::Client::new()
        .post(format!("http://{}/v1/logs", otlp.http))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("Failed to send OTLP HTTP request");
    otlp.shutdown.cancel();

    assert_eq!(
        resp.status(),
        200,
        "a 3 MiB export under the 10 MiB server limit was refused"
    );
}

/// An OTLP HTTP export that stalls mid-body is answered 408 once
/// `server.request_timeout_ms` passes, and counted.
#[tokio::test]
async fn otlp_http_times_out_a_stalled_export() {
    let mut config = test_config();
    config.server.request_timeout_ms = 200;
    let otlp = start_otlp_handler(config).await;

    let status =
        crate::common::post_stalled_body(otlp.http, "/v1/logs", Duration::from_secs(5)).await;
    otlp.shutdown.cancel();

    assert_eq!(status, Some(408));
    assert_eq!(otlp.metrics.get_request_timeouts_total(), 1);
}

/// A gRPC export past `otlp.max_message_size`, which tonic refuses before any
/// handler runs, is counted as refused for its size.
#[tokio::test]
async fn otlp_grpc_counts_an_export_refused_for_its_size() {
    use pb::collector::logs::v1::logs_service_client::LogsServiceClient;
    use pb::common::v1::{AnyValue, any_value};

    let mut config = test_config();
    config.otlp.max_message_size = 1024;
    let otlp = start_otlp_handler(config).await;
    let mut request = build_logs_request();
    request.resource_logs[0].scope_logs[0].log_records[0].body = Some(AnyValue {
        value: Some(any_value::Value::StringValue("x".repeat(8 * 1024))),
    });

    let mut client = LogsServiceClient::connect(format!("http://{}", otlp.grpc))
        .await
        .expect("Failed to connect to OTLP gRPC");
    let status = client
        .export(request)
        .await
        .expect_err("an export past the limit is refused");
    otlp.shutdown.cancel();

    assert_eq!(status.code(), tonic::Code::OutOfRange);
    assert_eq!(otlp.metrics.get_body_size_rejected_total(), 1);
}

/// A connection open on either OTLP endpoint counts in the active-connection
/// gauge until it closes.
#[tokio::test]
async fn otlp_counts_open_connections_on_both_endpoints() {
    let otlp = start_otlp_handler(test_config()).await;
    let wait = Duration::from_secs(5);

    let grpc = tokio::net::TcpStream::connect(otlp.grpc).await.unwrap();
    let http = tokio::net::TcpStream::connect(otlp.http).await.unwrap();
    assert!(
        crate::common::eventually(wait, || otlp.metrics.get_active_connections() == 2).await,
        "two open connections, counted {}",
        otlp.metrics.get_active_connections()
    );

    drop(grpc);
    drop(http);
    assert!(
        crate::common::eventually(wait, || otlp.metrics.get_active_connections() == 0).await,
        "both closed, counted {}",
        otlp.metrics.get_active_connections()
    );
    otlp.shutdown.cancel();
}
