// Project:   dfe-receiver
// File:      tests/integration_kafka.rs
// Purpose:   Integration tests for Kafka end-to-end flow
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::collapsible_if)]
#![allow(clippy::match_wild_err_arm)]
#![allow(clippy::field_reassign_with_default)]
#![allow(clippy::manual_string_new)]
#![allow(clippy::unused_async)]

//! Integration tests for sending data through the receiver to Kafka.
//!
//! These tests require either:
//! 1. Environment variables from `.env` for existing Kafka cluster
//! 2. Docker Compose with Strimzi (see docker-compose.test.yaml)
//!
//! Run with: `cargo test --test integration_kafka -- --ignored`
//!
//! Environment variables:
//! - KAFKA_BROKERS: Kafka broker addresses (default: localhost:9092)
//! - KAFKA_SASL_USER: SASL username (optional)
//! - KAFKA_SASL_PASSWORD: SASL password (optional)
//! - KAFKA_SASL_MECHANISM: SASL mechanism (optional, e.g., SCRAM-SHA-512)
//! - KAFKA_SECURITY_PROTOCOL: Security protocol (optional, e.g., SASL_PLAINTEXT)
//! - TEST_TOPIC_PREFIX: Prefix for test topics (default: dfe-receiver-test)

// Allow unwrap/expect in tests - they're the idiomatic way to fail fast
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use tokio::time::timeout;

use super::common::{kafka_test_config, test_topic};

/// Check if Kafka is available (uses dual-mode config).
async fn kafka_available() -> bool {
    let kf = kafka_test_config();
    if !kf.is_reachable() {
        return false;
    }

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &kf.brokers);
    config.set("socket.timeout.ms", "10000");
    config.set("metadata.request.timeout.ms", "10000");
    kf.apply_sasl(&mut config);

    let producer: Result<FutureProducer, _> = config.create();
    match producer {
        Ok(p) => p
            .client()
            .fetch_metadata(None, Duration::from_secs(5))
            .is_ok(),
        Err(_) => false,
    }
}

/// Create a Kafka producer with dual-mode configuration.
fn create_producer() -> FutureProducer {
    let kf = kafka_test_config();
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &kf.brokers);
    config.set("message.timeout.ms", "30000");
    config.set("acks", "all");
    kf.apply_sasl(&mut config);
    config.create().expect("Failed to create Kafka producer")
}

/// Send each record on its own at once, through the receiver's librdkafka overrides.
fn send_immediately(config: &mut dfe_receiver::config::Config) {
    for (key, value) in [("linger.ms", "0"), ("batch.num.messages", "1")] {
        config
            .kafka
            .librdkafka_overrides
            .insert(key.to_string(), value.to_string());
    }
}

/// Create a Kafka consumer with dual-mode configuration.
fn create_consumer(group_id: &str) -> StreamConsumer {
    let kf = kafka_test_config();
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &kf.brokers);
    config.set("group.id", group_id);
    config.set("auto.offset.reset", "earliest");
    config.set("enable.auto.commit", "false");
    config.set("session.timeout.ms", "10000");
    kf.apply_sasl(&mut config);
    config.create().expect("Failed to create Kafka consumer")
}

// =============================================================================
// Integration Tests
// =============================================================================

/// Test basic Kafka connectivity.
#[tokio::test]
#[ignore = "requires Kafka - run with --ignored"]
async fn test_kafka_connectivity() {
    if !kafka_available().await {
        eprintln!(
            "Skipping: Kafka not available (TEST_MODE={:?})",
            super::common::TestMode::detect()
        );
        return;
    }
}

/// Test sending a message directly to Kafka and consuming it.
#[tokio::test]
#[ignore = "requires Kafka - run with --ignored"]
async fn test_kafka_roundtrip() {
    if !kafka_available().await {
        eprintln!("Skipping test: Kafka not available");
        return;
    }

    let topic = test_topic("roundtrip");
    let producer = create_producer();
    let consumer = create_consumer(&format!("test-group-{}", uuid::Uuid::new_v4()));

    // Subscribe to topic
    consumer
        .subscribe(&[&topic])
        .expect("Failed to subscribe to topic");

    // Produce a message
    let payload = r#"{"event_category":"test","data":"hello"}"#;
    let record: FutureRecord<'_, str, str> = FutureRecord::to(&topic).payload(payload);

    producer
        .send(record, Duration::from_secs(10))
        .await
        .expect("Failed to send message");

    // Consume the message
    let result = timeout(Duration::from_secs(30), async {
        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    let payload = msg.payload().expect("Empty payload");
                    return String::from_utf8_lossy(payload).to_string();
                }
                Err(e) => {
                    eprintln!("Consumer error: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
    .await;

    match result {
        Ok(received) => {
            assert_eq!(received, payload);
            println!("✓ Message roundtrip successful: {received}");
        }
        Err(_) => {
            panic!("Timeout waiting for message");
        }
    }
}

/// Test sending multiple messages with batching.
#[tokio::test]
#[ignore = "requires Kafka - run with --ignored"]
async fn test_kafka_batch_send() {
    if !kafka_available().await {
        eprintln!("Skipping test: Kafka not available");
        return;
    }

    let topic = test_topic("batch");
    let producer = create_producer();
    let consumer = create_consumer(&format!("test-group-{}", uuid::Uuid::new_v4()));

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Send multiple messages - need to own the payloads
    let message_count = 100;
    let payloads: Vec<String> = (0..message_count)
        .map(|i| format!(r#"{{"event_category":"batch","seq":{i}}}"#))
        .collect();

    let mut futures = Vec::with_capacity(message_count);
    for payload in &payloads {
        let record: FutureRecord<'_, str, str> = FutureRecord::to(&topic).payload(payload);
        futures.push(producer.send(record, Duration::from_secs(10)));
    }

    // Wait for all sends
    for (i, future) in futures.into_iter().enumerate() {
        future.await.unwrap_or_else(|e| {
            panic!("Send {i} failed: {e:?}");
        });
    }

    println!("✓ Sent {message_count} messages");

    // Consume and count
    let mut received = 0;
    let result = timeout(Duration::from_secs(30), async {
        while received < message_count {
            match consumer.recv().await {
                Ok(_) => {
                    received += 1;
                }
                Err(e) => {
                    eprintln!("Consumer error: {e}");
                }
            }
        }
        received
    })
    .await;

    match result {
        Ok(count) => {
            assert_eq!(count, message_count);
            println!("✓ Received all {count} messages");
        }
        Err(_) => {
            panic!("Timeout: only received {received}/{message_count} messages");
        }
    }
}

/// Test the dfe-receiver KafkaSink directly.
#[tokio::test]
#[ignore = "requires Kafka - run with --ignored"]
async fn test_receiver_kafka_sink() {
    use dfe_receiver::sink::Sink;
    use dfe_receiver::sink::kafka::KafkaSink;

    if !kafka_available().await {
        eprintln!("Skipping test: Kafka not available");
        return;
    }

    let kf = kafka_test_config();
    let config = kf.to_receiver_kafka_config();

    let topic = test_topic("sink");
    let sink = KafkaSink::new(&config, None).expect("Failed to create KafkaSink");

    // Send messages through the sink
    for i in 0..10 {
        let payload = Bytes::from(format!(r#"{{"event_category":"sink_test","seq":{i}}}"#));
        sink.send(&topic, payload).await.expect("Sink send failed");
    }

    // Flush to ensure delivery
    sink.flush().await.expect("Sink flush failed");

    println!("✓ KafkaSink sent 10 messages to {topic}");

    // Verify messages arrived
    let consumer = create_consumer(&format!("test-group-{}", uuid::Uuid::new_v4()));
    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let mut received = 0;
    let result = timeout(Duration::from_secs(30), async {
        while received < 10 {
            if consumer.recv().await.is_ok() {
                received += 1;
            }
        }
        received
    })
    .await;

    assert_eq!(result.unwrap_or(0), 10, "Expected 10 messages");
    println!("✓ Verified 10 messages received");
}

/// Test full pipeline: HTTP -> Validation -> Routing -> Kafka.
#[tokio::test]
#[ignore = "requires Kafka - run with --ignored"]
async fn test_full_pipeline_to_kafka() {
    use dfe_receiver::config::SharedConfig;
    use dfe_receiver::pipeline::PipelineState;

    if !kafka_available().await {
        eprintln!("Skipping test: Kafka not available");
        return;
    }

    let kf = kafka_test_config();
    let mut config = kf.to_receiver_config();

    // Configure routing to use test topic
    let topic = test_topic("pipeline");
    config.routing.default_source = topic.clone();
    config.routing.topic_suffix = "".to_string(); // No suffix for test

    // Use small batch settings for tests to send immediately
    send_immediately(&mut config);

    // Create pipeline
    let pipeline = PipelineState::new(
        SharedConfig::new(config),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("Failed to create pipeline");

    // Send messages through pipeline
    for i in 0..5 {
        let payload = Bytes::from(format!(
            r#"{{"event_category":"pipeline_test","message":"hello","seq":{i}}}"#
        ));
        pipeline
            .process(payload)
            .await
            .expect("Pipeline process failed");
    }

    println!("✓ Pipeline processed 5 messages");

    // Give time for delivery
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify messages arrived
    let consumer = create_consumer(&format!("test-group-{}", uuid::Uuid::new_v4()));
    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let mut received = 0;
    let result = timeout(Duration::from_secs(30), async {
        while received < 5 {
            if consumer.recv().await.is_ok() {
                received += 1;
            }
        }
        received
    })
    .await;

    assert_eq!(result.unwrap_or(0), 5, "Expected 5 messages in {topic}");
    println!("✓ Verified 5 messages in Kafka topic: {topic}");
}

/// Test HTTP server end-to-end with Kafka.
#[tokio::test]
#[ignore = "requires Kafka - run with --ignored"]
async fn test_http_to_kafka() {
    use dfe_receiver::metrics::Metrics;
    use dfe_receiver::pipeline::Orchestrator;
    use dfe_receiver::server::http::HttpHandler;
    use dfe_receiver::server::traits::ProtocolHandler;
    use tokio_util::sync::CancellationToken;

    if !kafka_available().await {
        eprintln!("Skipping test: Kafka not available");
        return;
    }

    let kf = kafka_test_config();
    let mut config = kf.to_receiver_config();

    // Configure routing
    let topic = test_topic("http");
    config.routing.default_source = topic.clone();
    config.routing.topic_suffix = "".to_string();

    // Use small batch settings for tests to send immediately
    send_immediately(&mut config);

    // Port 0, read back once bound: a port picked and handed over can be taken
    // by another process before the handler binds it.
    config.server.bind_address = "127.0.0.1:0".to_string();

    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();

    // Create orchestrator and get pipeline state
    let orchestrator = Orchestrator::new(config.clone(), metrics.clone(), shutdown.clone())
        .await
        .expect("Failed to create orchestrator");
    let pipeline = orchestrator.state();

    // Spawn HTTP server
    let handler = HttpHandler::new(config.server.bind_address.clone(), pipeline, metrics);
    let bound = handler.bound_addr();
    let server_shutdown = shutdown.clone();
    let mut server_handle = tokio::spawn(async move { handler.start(server_shutdown).await });
    let addr = super::common::bound_addr("HTTP", &bound, &mut server_handle).await;

    // Send HTTP request
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/ingest");

    for i in 0..3 {
        let payload = format!(r#"{{"event_category":"http_test","seq":{i}}}"#);
        let response = client
            .post(&url)
            .header("content-type", "application/json")
            .body(payload)
            .send()
            .await
            .expect("HTTP request failed");

        assert!(
            response.status().is_success(),
            "HTTP request failed with status: {}",
            response.status()
        );
    }

    println!("✓ Sent 3 HTTP requests");

    // Give time for processing
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Shutdown server
    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), server_handle).await;

    // Verify messages in Kafka
    let consumer = create_consumer(&format!("test-group-{}", uuid::Uuid::new_v4()));
    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let mut received = 0;
    let result = timeout(Duration::from_secs(30), async {
        while received < 3 {
            if consumer.recv().await.is_ok() {
                received += 1;
            }
        }
        received
    })
    .await;

    assert_eq!(result.unwrap_or(0), 3, "Expected 3 messages in {topic}");
    println!("✓ Verified 3 messages in Kafka via HTTP: {topic}");
}

/// Test routing to different topics based on event_category.
#[tokio::test]
#[ignore = "requires Kafka - run with --ignored"]
async fn test_category_routing() {
    use dfe_receiver::config::SharedConfig;
    use dfe_receiver::pipeline::PipelineState;

    if !kafka_available().await {
        eprintln!("Skipping test: Kafka not available");
        return;
    }

    let kf = kafka_test_config();
    let mut config = kf.to_receiver_config();

    // Configure category-based routing
    let auth_topic = test_topic("auth");
    let network_topic = test_topic("network");
    let default_topic = test_topic("default");

    config
        .routing
        .source_to_topic
        .insert("authentication".to_string(), auth_topic.clone());
    config
        .routing
        .source_to_topic
        .insert("network".to_string(), network_topic.clone());
    config.routing.default_source = default_topic.clone();
    config.routing.topic_suffix = "".to_string();
    config.routing.source_rules = vec![dfe_receiver::config::SourceRule {
        field: "event_category".to_string(),
        mode: "key_value_use".to_string(),
        match_value: None,
        source: None,
    }];

    // Create pipeline
    let pipeline = PipelineState::new(
        SharedConfig::new(config),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("Failed to create pipeline");

    // Send messages with different categories
    pipeline
        .process(Bytes::from(
            r#"{"event_category":"authentication","user":"alice"}"#,
        ))
        .await
        .expect("Process failed");
    pipeline
        .process(Bytes::from(
            r#"{"event_category":"network","ip":"10.0.0.1"}"#,
        ))
        .await
        .expect("Process failed");
    pipeline
        .process(Bytes::from(r#"{"event_category":"unknown","data":"test"}"#))
        .await
        .expect("Process failed");

    println!("✓ Sent 3 messages with different categories");

    // Give time for delivery
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify each topic has 1 message
    for (topic, name) in [
        (&auth_topic, "auth"),
        (&network_topic, "network"),
        (&default_topic, "default"),
    ] {
        let consumer = create_consumer(&format!("test-group-{}", uuid::Uuid::new_v4()));
        consumer.subscribe(&[topic]).expect("Failed to subscribe");

        let result = timeout(Duration::from_secs(10), consumer.recv()).await;
        assert!(result.is_ok(), "Expected message in {name} topic: {topic}");
        println!("✓ Verified message in {name} topic: {topic}");
    }
}

/// Test DLQ routing for invalid messages.
#[tokio::test]
#[ignore = "requires Kafka - run with --ignored"]
async fn test_dlq_routing() {
    use dfe_receiver::config::SharedConfig;
    use dfe_receiver::pipeline::PipelineState;

    if !kafka_available().await {
        eprintln!("Skipping test: Kafka not available");
        return;
    }

    let kf = kafka_test_config();
    let mut config = kf.to_receiver_config();

    // Configure DLQ
    let dlq_topic = test_topic("dlq");
    config.routing.dlq.enabled = true;
    config.routing.dlq.topic = dlq_topic.clone();
    config.routing.topic_suffix = "".to_string();

    // Require a field that won't be present
    config.validation.required_fields = vec!["required_field".to_string()];
    config.validation.dlq_on_invalid = true;

    // Create pipeline
    let pipeline = PipelineState::new(
        SharedConfig::new(config),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("Failed to create pipeline");

    // Send message missing required field - should go to DLQ
    let result = pipeline
        .process(Bytes::from(
            r#"{"event_category":"test","data":"no required field"}"#,
        ))
        .await;

    // Should succeed (routed to DLQ)
    assert!(result.is_ok(), "DLQ routing should succeed: {result:?}");

    println!("✓ Invalid message routed to DLQ");

    // Give time for delivery
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify message in DLQ
    let consumer = create_consumer(&format!("test-group-{}", uuid::Uuid::new_v4()));
    consumer
        .subscribe(&[&dlq_topic])
        .expect("Failed to subscribe");

    let result = timeout(Duration::from_secs(10), consumer.recv()).await;
    assert!(result.is_ok(), "Expected message in DLQ: {dlq_topic}");
    println!("✓ Verified message in DLQ: {dlq_topic}");
}
