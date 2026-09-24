// Project:   dfe-receiver
// File:      tests/integration/protocol_kafka_roundtrip.rs
// Purpose:   End-to-end protocol → receiver → Kafka roundtrip tests
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! End-to-end tests that exercise the full path from an external protocol
//! client (Prometheus RW, Splunk HEC) through the receiver to Kafka, using
//! a testcontainers-managed Kafka broker.
//!
//! For OTLP, see `otlp.rs` (the existing tests already cover OTLP→pipeline).
//! Adding Kafka to every handler test would multiply test time; instead we
//! cover the representative cases here and rely on the unit+convert tests
//! for protocol-specific details.
//!
//! Containers are auto-stopped when the test function returns.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::prometheus_rw::PrometheusRwHandler;
use dfe_receiver::server::prometheus_rw::proto;
use dfe_receiver::server::splunk_hec::SplunkHecHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use dfe_receiver::server::webhook::WebhookHandler;
use prost::Message;
use tokio_util::sync::CancellationToken;

use crate::common::{kafka_backend, kafka_consume_next, kafka_consumer, test_topic};
use crate::test_name;

/// Build a config wired to route via Kafka (rather than the in-memory loader).
fn kafka_config(kf: &crate::common::KafkaTestConfig, topic: &str) -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();

    config.kafka = kf.to_receiver_kafka_config();
    config.destinations.default = "kafka".into();
    // Route all traffic to the test topic (no _land suffix by overriding default_source)
    config.routing.default_source = topic.trim_end_matches("_land").to_string();
    config.routing.topic_suffix = "_land".to_string();
    if !topic.ends_with("_land") {
        // Use an empty suffix so the topic name is verbatim
        config.routing.topic_suffix = String::new();
        config.routing.default_source = topic.to_string();
    }

    config
}

// =========================================================================
// Prometheus Remote Write → Kafka
// =========================================================================

#[tokio::test]
async fn test_prometheus_rw_to_kafka_roundtrip() {
    let Some((_handle, kf)) = kafka_backend(test_name!()).await else {
        eprintln!("Skipping: no Kafka backend available");
        return;
    };

    let topic = test_topic("promrw");
    let mut config = kafka_config(&kf, &topic);
    config.prometheus_rw.enabled = true;
    config.prometheus_rw.bind_address = "127.0.0.1:0".to_string();
    config.prometheus_rw.mode = "native".to_string();
    config.prometheus_rw.auth.mode = "none".to_string();

    // Subscribe BEFORE sending
    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Start receiver
    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(
            SharedConfig::new(config.clone()),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("pipeline init"),
    );
    let handler = PrometheusRwHandler::new(
        config.prometheus_rw.clone(),
        config.raw_capture_for(&config.prometheus_rw.raw_capture),
        pipeline,
        metrics.clone(),
    );
    let bound = handler.bound_addr();
    let handler_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(handler_shutdown).await });
    let rw = crate::common::bound_addr("remote write", &bound, &mut task).await;

    // Build a valid Prometheus RW WriteRequest
    let request = proto::WriteRequest {
        timeseries: vec![proto::TimeSeries {
            labels: vec![
                proto::Label {
                    name: "__name__".to_string(),
                    value: "cpu_usage".to_string(),
                },
                proto::Label {
                    name: "host".to_string(),
                    value: "server-1".to_string(),
                },
            ],
            samples: vec![proto::Sample {
                value: 42.5,
                timestamp: 1_771_459_200_000,
            }],
            exemplars: vec![],
            histograms: vec![],
        }],
        metadata: vec![],
    };

    let encoded = request.encode_to_vec();
    let compressed = snap::raw::Encoder::new().compress_vec(&encoded).unwrap();

    // POST to receiver
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{rw}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .header("X-Prometheus-Remote-Write-Version", "0.1.0")
        .body(compressed)
        .send()
        .await
        .expect("POST failed");
    assert!(
        resp.status().is_success(),
        "receiver rejected: {}",
        resp.status()
    );

    // Verify message arrived on Kafka topic
    let received = kafka_consume_next(&consumer, Duration::from_secs(30))
        .await
        .expect("no message arrived on Kafka topic");

    // The message should be a JSON representation of the metric
    let json_text = String::from_utf8_lossy(&received);
    assert!(
        json_text.contains("cpu_usage"),
        "expected metric name in payload: {json_text}"
    );

    shutdown.cancel();
}

// =========================================================================
// Splunk HEC → Kafka
// =========================================================================

#[tokio::test]
async fn test_splunk_hec_to_kafka_roundtrip() {
    let Some((_handle, kf)) = kafka_backend(test_name!()).await else {
        eprintln!("Skipping: no Kafka backend available");
        return;
    };

    let topic = test_topic("hec");
    let mut config = kafka_config(&kf, &topic);
    config.splunk_hec.enabled = true;
    config.splunk_hec.bind_address = "127.0.0.1:0".to_string();
    config.splunk_hec.auth.mode = "none".to_string();

    // Subscribe to Kafka
    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Start HEC handler
    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(
            SharedConfig::new(config.clone()),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("pipeline init"),
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
    let hec = crate::common::bound_addr("HEC", &bound, &mut task).await;

    // Send an HEC event
    let event = serde_json::json!({
        "event": {"message": "test event", "level": "info"},
        "source": "test-source",
        "sourcetype": "test:type",
    });
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{hec}/services/collector/event"))
        .header("Content-Type", "application/json")
        .body(event.to_string())
        .send()
        .await
        .expect("POST failed");
    assert!(
        resp.status().is_success(),
        "HEC rejected: {}",
        resp.status()
    );

    // Verify Kafka delivery
    let received = kafka_consume_next(&consumer, Duration::from_secs(30))
        .await
        .expect("no message arrived on Kafka topic");
    let text = String::from_utf8_lossy(&received);
    assert!(text.contains("test event"), "payload mismatch: {text}");

    shutdown.cancel();
}

// =========================================================================
// Webhook -> Kafka
// =========================================================================

/// The captured runZero alert-rule webhook, delivered in `header` mode (the
/// only mode runZero can drive: its webhook channel carries a URL and static
/// headers, nothing signed), lands on the caller's topic stamped with
/// `_source` and `_timestamp_receiver`.
#[tokio::test]
async fn test_webhook_to_kafka_roundtrip() {
    use std::io::Write;

    use dfe_receiver::config::{
        WebhookAuthConfig, WebhookAuthMode, WebhookBody, WebhookCallerConfig,
    };

    let Some((_handle, kf)) = kafka_backend(test_name!()).await else {
        eprintln!("Skipping: no Kafka backend available");
        return;
    };

    // The fixture: method, headers (secret redacted) and the body as posted.
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/webhook/runzero-alert.json"))
            .expect("fixture parses");
    assert_eq!(fixture["method"], "POST");
    assert_eq!(fixture["path"], "/webhook/runzero");
    let body = serde_json::to_vec(&fixture["body"]).unwrap();

    // The caller's secret lives in a file, as a mounted Secret would.
    let mut secret_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(secret_file, "runzero-webhook-shared-secret").unwrap();
    secret_file.flush().unwrap();

    // The caller's topic is fixed by config and carries no suffix.
    let topic = test_topic("webhook");
    let mut config = kafka_config(&kf, &topic);
    config.webhook.enabled = true;
    config.webhook.bind_address = Some("127.0.0.1:0".to_string());
    config.webhook.callers = vec![WebhookCallerConfig {
        name: "runzero".to_string(),
        topic: topic.clone(),
        auth: WebhookAuthConfig {
            mode: WebhookAuthMode::Header,
            secret_source: format!("file:{}", secret_file.path().display()),
            refresh_interval_secs: 0,
            header: "x-webhook-secret".to_string(),
            ..WebhookAuthConfig::default()
        },
        body: WebhookBody::Single,
        filter: String::new(),
    }];

    // Subscribe BEFORE sending
    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(
            SharedConfig::new(config.clone()),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("pipeline init"),
    );
    let handler = WebhookHandler::new(config, pipeline, metrics.clone());
    let bound = handler.bound_addr();
    let handler_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(handler_shutdown).await });
    let webhook = crate::common::bound_addr("webhook", &bound, &mut task).await;

    // Replay the capture: runZero's headers, the shared secret in place of
    // the redacted value.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{webhook}/webhook/runzero"))
        .header("content-type", "application/json")
        .header(
            "user-agent",
            fixture["headers"]["user-agent"].as_str().unwrap(),
        )
        .header("accept-encoding", "gzip")
        .header("x-webhook-secret", "runzero-webhook-shared-secret")
        .body(body.clone())
        .send()
        .await
        .expect("POST failed");
    assert_eq!(
        resp.status(),
        202,
        "webhook rejected: {}",
        resp.text().await.unwrap()
    );

    let received = kafka_consume_next(&consumer, Duration::from_secs(30))
        .await
        .expect("no message arrived on the caller's topic");
    let record: serde_json::Value =
        serde_json::from_slice(&received).expect("the record on Kafka is JSON");
    assert_eq!(record["_source"], "runzero", "record: {record}");
    assert!(
        record["_timestamp_receiver"].is_u64(),
        "record must carry the receiver timestamp: {record}"
    );
    assert_eq!(
        record["text"], fixture["body"]["text"],
        "the product's fields must arrive untouched: {record}"
    );

    shutdown.cancel();
}

/// In `body: array` mode one bad element refuses the whole request, and the
/// elements before it never reach the topic, so the sender's retry does not
/// duplicate them.
#[tokio::test]
async fn test_webhook_array_with_a_bad_element_delivers_nothing() {
    use std::io::Write;

    use dfe_receiver::config::{
        WebhookAuthConfig, WebhookAuthMode, WebhookBody, WebhookCallerConfig,
    };

    let Some((_handle, kf)) = kafka_backend(test_name!()).await else {
        eprintln!("Skipping: no Kafka backend available");
        return;
    };

    let mut secret_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(secret_file, "bulk-webhook-shared-secret").unwrap();
    secret_file.flush().unwrap();

    let topic = test_topic("webhook-array");
    let mut config = kafka_config(&kf, &topic);
    config.webhook.enabled = true;
    config.webhook.bind_address = Some("127.0.0.1:0".to_string());
    config.webhook.callers = vec![WebhookCallerConfig {
        name: "bulk".to_string(),
        topic: topic.clone(),
        auth: WebhookAuthConfig {
            mode: WebhookAuthMode::Header,
            secret_source: format!("file:{}", secret_file.path().display()),
            refresh_interval_secs: 0,
            header: "x-webhook-secret".to_string(),
            ..WebhookAuthConfig::default()
        },
        body: WebhookBody::Array,
        filter: String::new(),
    }];

    // Subscribe BEFORE sending
    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(
            SharedConfig::new(config.clone()),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("pipeline init"),
    );
    let handler = WebhookHandler::new(config, pipeline, metrics.clone());
    let bound = handler.bound_addr();
    let handler_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(handler_shutdown).await });
    let webhook = crate::common::bound_addr("webhook", &bound, &mut task).await;

    let client = reqwest::Client::new();
    let url = format!("http://{webhook}/webhook/bulk");

    // An object followed by a number: the request is refused as a whole.
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .header("x-webhook-secret", "bulk-webhook-shared-secret")
        .body(r#"[{"a":1}, 5]"#)
        .send()
        .await
        .expect("POST failed");
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "record_not_an_object");

    // A good request after it: the first record on the topic must be this
    // one, not the refused request's leading element.
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .header("x-webhook-secret", "bulk-webhook-shared-secret")
        .body(r#"[{"a":2}]"#)
        .send()
        .await
        .expect("POST failed");
    assert_eq!(resp.status(), 202, "body: {}", resp.text().await.unwrap());

    let received = kafka_consume_next(&consumer, Duration::from_secs(30))
        .await
        .expect("the accepted record never arrived on the caller's topic");
    let record: serde_json::Value = serde_json::from_slice(&received).unwrap();
    assert_eq!(
        record["a"], 2,
        "an element of the refused request reached the topic: {record}"
    );
    // Nothing follows it, so a leaked element on another partition shows too.
    let extra = kafka_consume_next(&consumer, Duration::from_secs(3)).await;
    assert!(
        extra.is_none(),
        "an element of the refused request reached the topic: {}",
        String::from_utf8_lossy(extra.as_deref().unwrap_or_default())
    );

    shutdown.cancel();
}

// =========================================================================
// HTTP ingest → Kafka (validates the primary HTTP path with real Kafka)
// =========================================================================

#[tokio::test]
async fn test_http_to_kafka_roundtrip() {
    let Some((_handle, kf)) = kafka_backend(test_name!()).await else {
        eprintln!("Skipping: no Kafka backend available");
        return;
    };

    let topic = test_topic("http");
    let config = kafka_config(&kf, &topic);

    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Process payload directly through the pipeline (skipping the HTTP server
    // since we have no HTTP handler to spin up here easily — the goal is
    // validating Kafka delivery, not HTTP parsing).
    let pipeline = Arc::new(
        PipelineState::new(
            SharedConfig::new(config),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("pipeline init"),
    );

    let payload = Bytes::from(r#"{"event":"http-to-kafka-test","id":42,"source":"integration"}"#);
    pipeline
        .process(payload.clone())
        .await
        .expect("pipeline processing failed");

    // Verify Kafka delivery
    let received = kafka_consume_next(&consumer, Duration::from_secs(30))
        .await
        .expect("no message arrived on Kafka topic");
    let text = String::from_utf8_lossy(&received);
    assert!(
        text.contains("http-to-kafka-test"),
        "payload mismatch: {text}"
    );
}
