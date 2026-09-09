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
use prost::Message;
use tokio_util::sync::CancellationToken;

use crate::common::{kafka_backend, kafka_consume_next, kafka_consumer, test_topic};
use crate::test_name;

/// A port the OS says is free right now.
///
/// Guessing a random port in 30000-50000 collides on a busy runner -- that
/// range is also where testcontainers maps its host ports -- and a collision
/// makes the handler's bind fail, so the port never accepts and the wait below
/// burns its whole budget on a server that was never listening.
fn random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// Poll the loopback port until it accepts a TCP connection or the budget
/// is exhausted. Replaces blind `sleep` waits that race the spawned
/// handler's bind and produce ConnectionRefused on busy ARC runners.
///
/// The budget is generous because it is protecting against a race, not
/// measuring anything: a loaded machine takes longer to get the handler bound,
/// and the poll returns the instant it is up, so a larger ceiling costs
/// nothing on a quiet one.
const PORT_WAIT_BUDGET: Duration = Duration::from_secs(30);
const PORT_POLL_INTERVAL: Duration = Duration::from_millis(50);

async fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    let deadline = tokio::time::Instant::now() + PORT_WAIT_BUDGET;
    while tokio::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(PORT_POLL_INTERVAL).await;
    }
    panic!("port {port} never accepted connections within {PORT_WAIT_BUDGET:?}");
}

/// Build a config wired to route via Kafka (rather than the in-memory loader).
fn kafka_config(kf: &crate::common::KafkaTestConfig, topic: &str) -> Config {
    let mut config = Config::default();
    let http_port = random_port();
    config.server.bind_address = format!("127.0.0.1:{http_port}");
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
    let rw_port = random_port();
    config.prometheus_rw.enabled = true;
    config.prometheus_rw.bind_address = format!("127.0.0.1:{rw_port}");
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
    let handler_shutdown = shutdown.clone();
    tokio::spawn(async move {
        // Surfaced, not swallowed: a failed bind here used to be invisible,
        // and the only symptom was the port wait below timing out.
        if let Err(e) = handler.start(handler_shutdown).await {
            eprintln!("handler exited with an error: {e}");
        }
    });
    wait_for_port(rw_port).await;

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
        .post(format!("http://127.0.0.1:{rw_port}/api/v1/write"))
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
    let hec_port = random_port();
    config.splunk_hec.enabled = true;
    config.splunk_hec.bind_address = format!("127.0.0.1:{hec_port}");
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
    let handler_shutdown = shutdown.clone();
    tokio::spawn(async move {
        // Surfaced, not swallowed: a failed bind here used to be invisible,
        // and the only symptom was the port wait below timing out.
        if let Err(e) = handler.start(handler_shutdown).await {
            eprintln!("handler exited with an error: {e}");
        }
    });
    wait_for_port(hec_port).await;

    // Send an HEC event
    let event = serde_json::json!({
        "event": {"message": "test event", "level": "info"},
        "source": "test-source",
        "sourcetype": "test:type",
    });
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://127.0.0.1:{hec_port}/services/collector/event"
        ))
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
