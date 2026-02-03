// Project:   dfe-receiver
// File:      tests/integration_kafka.rs
// Purpose:   Integration tests for Kafka end-to-end flow
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

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

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use tokio::time::timeout;

/// Load environment variables from .env file if present.
fn load_env() {
    let _ = dotenvy::dotenv();
}

/// Get Kafka brokers from environment or use default.
fn kafka_brokers() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

/// Get test topic prefix from environment or use default.
fn test_topic_prefix() -> String {
    std::env::var("TEST_TOPIC_PREFIX").unwrap_or_else(|_| "dfe-receiver-test".to_string())
}

/// Create a unique test topic name.
fn test_topic(suffix: &str) -> String {
    format!("{}-{}-{}", test_topic_prefix(), suffix, uuid::Uuid::new_v4())
}

/// Check if Kafka is available.
async fn kafka_available() -> bool {
    load_env();

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", kafka_brokers());
    config.set("socket.timeout.ms", "5000");
    config.set("metadata.request.timeout.ms", "5000");

    // Add SASL if configured
    if let Ok(user) = std::env::var("KAFKA_SASL_USER") {
        if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD") {
            let mechanism = std::env::var("KAFKA_SASL_MECHANISM")
                .unwrap_or_else(|_| "PLAIN".to_string());
            let protocol = std::env::var("KAFKA_SECURITY_PROTOCOL")
                .unwrap_or_else(|_| "SASL_PLAINTEXT".to_string());

            config.set("security.protocol", &protocol);
            config.set("sasl.mechanism", &mechanism);
            config.set("sasl.username", &user);
            config.set("sasl.password", &password);
        }
    }

    let producer: Result<FutureProducer, _> = config.create();
    match producer {
        Ok(p) => {
            // Try to get metadata to verify connection
            match p.client().fetch_metadata(None, Duration::from_secs(5)) {
                Ok(_) => true,
                Err(e) => {
                    eprintln!("Kafka metadata fetch failed: {e}");
                    false
                }
            }
        }
        Err(e) => {
            eprintln!("Kafka producer creation failed: {e}");
            false
        }
    }
}

/// Create a Kafka producer with proper configuration.
fn create_producer() -> FutureProducer {
    load_env();

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", kafka_brokers());
    config.set("message.timeout.ms", "30000");
    config.set("acks", "all");

    // Add SASL if configured
    if let Ok(user) = std::env::var("KAFKA_SASL_USER") {
        if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD") {
            let mechanism = std::env::var("KAFKA_SASL_MECHANISM")
                .unwrap_or_else(|_| "PLAIN".to_string());
            let protocol = std::env::var("KAFKA_SECURITY_PROTOCOL")
                .unwrap_or_else(|_| "SASL_PLAINTEXT".to_string());

            config.set("security.protocol", &protocol);
            config.set("sasl.mechanism", &mechanism);
            config.set("sasl.username", &user);
            config.set("sasl.password", &password);
        }
    }

    config.create().expect("Failed to create Kafka producer")
}

/// Create a Kafka consumer with proper configuration.
fn create_consumer(group_id: &str) -> StreamConsumer {
    load_env();

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", kafka_brokers());
    config.set("group.id", group_id);
    config.set("auto.offset.reset", "earliest");
    config.set("enable.auto.commit", "false");
    config.set("session.timeout.ms", "10000");

    // Add SASL if configured
    if let Ok(user) = std::env::var("KAFKA_SASL_USER") {
        if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD") {
            let mechanism = std::env::var("KAFKA_SASL_MECHANISM")
                .unwrap_or_else(|_| "PLAIN".to_string());
            let protocol = std::env::var("KAFKA_SECURITY_PROTOCOL")
                .unwrap_or_else(|_| "SASL_PLAINTEXT".to_string());

            config.set("security.protocol", &protocol);
            config.set("sasl.mechanism", &mechanism);
            config.set("sasl.username", &user);
            config.set("sasl.password", &password);
        }
    }

    config.create().expect("Failed to create Kafka consumer")
}

// =============================================================================
// Integration Tests
// =============================================================================

/// Test basic Kafka connectivity.
#[tokio::test]
#[ignore = "requires Kafka - run with --ignored"]
async fn test_kafka_connectivity() {
    assert!(
        kafka_available().await,
        "Kafka is not available. Set KAFKA_BROKERS or start docker-compose."
    );
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

    consumer
        .subscribe(&[&topic])
        .expect("Failed to subscribe");

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
    use dfe_receiver::config::KafkaConfig;
    use dfe_receiver::sink::kafka::KafkaSink;
    use dfe_receiver::sink::Sink;

    if !kafka_available().await {
        eprintln!("Skipping test: Kafka not available");
        return;
    }

    load_env();

    // Build config from environment
    let mut config = KafkaConfig::default();
    config.brokers = kafka_brokers()
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    config.client_id = "dfe-receiver-test".to_string();

    // Add SASL if configured
    if let Ok(user) = std::env::var("KAFKA_SASL_USER") {
        if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD") {
            let mechanism = std::env::var("KAFKA_SASL_MECHANISM")
                .unwrap_or_else(|_| "PLAIN".to_string());

            config.sasl = Some(dfe_receiver::config::SaslConfig {
                enabled: true,
                mechanism,
                username: user,
                password,
            });
        }
    }

    let topic = test_topic("sink");
    let sink = KafkaSink::new(&config).expect("Failed to create KafkaSink");

    // Send messages through the sink
    for i in 0..10 {
        let payload = Bytes::from(format!(r#"{{"event_category":"sink_test","seq":{i}}}"#));
        sink.send(&topic, payload)
            .await
            .expect("Sink send failed");
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
    use dfe_receiver::config::{Config, SaslConfig};
    use dfe_receiver::pipeline::PipelineState;

    if !kafka_available().await {
        eprintln!("Skipping test: Kafka not available");
        return;
    }

    load_env();

    // Build config
    let mut config = Config::default();
    config.kafka.brokers = kafka_brokers()
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    config.kafka.client_id = "dfe-receiver-test".to_string();

    // Add SASL if configured
    if let Ok(user) = std::env::var("KAFKA_SASL_USER") {
        if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD") {
            let mechanism = std::env::var("KAFKA_SASL_MECHANISM")
                .unwrap_or_else(|_| "PLAIN".to_string());

            config.kafka.sasl = Some(SaslConfig {
                enabled: true,
                mechanism,
                username: user,
                password,
            });
        }
    }

    // Configure routing to use test topic
    let topic = test_topic("pipeline");
    config.routing.default_topic = topic.clone();
    config.routing.topic_suffix = "".to_string(); // No suffix for test

    // Use small batch settings for tests to send immediately
    config.kafka.producer.batch_messages = 1;
    config.kafka.producer.linger_ms = 0;

    // Create pipeline
    let pipeline = PipelineState::new(config).expect("Failed to create pipeline");

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
    use dfe_receiver::config::{Config, SaslConfig};
    use dfe_receiver::metrics::Metrics;
    use dfe_receiver::pipeline::Orchestrator;
    use dfe_receiver::server::http;
    use tokio_util::sync::CancellationToken;

    if !kafka_available().await {
        eprintln!("Skipping test: Kafka not available");
        return;
    }

    load_env();

    // Build config
    let mut config = Config::default();
    config.kafka.brokers = kafka_brokers()
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    config.kafka.client_id = "dfe-receiver-test".to_string();

    // Add SASL if configured
    if let Ok(user) = std::env::var("KAFKA_SASL_USER") {
        if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD") {
            let mechanism = std::env::var("KAFKA_SASL_MECHANISM")
                .unwrap_or_else(|_| "PLAIN".to_string());

            config.kafka.sasl = Some(SaslConfig {
                enabled: true,
                mechanism,
                username: user,
                password,
            });
        }
    }

    // Configure routing
    let topic = test_topic("http");
    config.routing.default_topic = topic.clone();
    config.routing.topic_suffix = "".to_string();

    // Use small batch settings for tests to send immediately
    config.kafka.producer.batch_messages = 1;
    config.kafka.producer.linger_ms = 0;

    // Use a random port
    let port = 10000 + (uuid::Uuid::new_v4().as_u128() % 10000) as u16;
    config.server.bind_address = format!("127.0.0.1:{port}");

    let metrics = Arc::new(Metrics::new());
    let shutdown = CancellationToken::new();

    // Create orchestrator and get pipeline state
    let orchestrator = Orchestrator::new(config.clone(), metrics.clone(), shutdown.clone())
        .expect("Failed to create orchestrator");
    let pipeline = orchestrator.state();

    // Spawn HTTP server
    let server_shutdown = shutdown.clone();
    let server_metrics = metrics.clone();
    let server_pipeline = pipeline.clone();
    let bind_addr = config.server.bind_address.clone();

    let server_handle = tokio::spawn(async move {
        let _ = http::run_server(&bind_addr, server_pipeline, server_metrics, server_shutdown).await;
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send HTTP request
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/ingest");

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
    use dfe_receiver::config::{Config, SaslConfig};
    use dfe_receiver::pipeline::PipelineState;

    if !kafka_available().await {
        eprintln!("Skipping test: Kafka not available");
        return;
    }

    load_env();

    // Build config
    let mut config = Config::default();
    config.kafka.brokers = kafka_brokers()
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    config.kafka.client_id = "dfe-receiver-test".to_string();

    // Add SASL if configured
    if let Ok(user) = std::env::var("KAFKA_SASL_USER") {
        if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD") {
            let mechanism = std::env::var("KAFKA_SASL_MECHANISM")
                .unwrap_or_else(|_| "PLAIN".to_string());

            config.kafka.sasl = Some(SaslConfig {
                enabled: true,
                mechanism,
                username: user,
                password,
            });
        }
    }

    // Configure category-based routing
    let auth_topic = test_topic("auth");
    let network_topic = test_topic("network");
    let default_topic = test_topic("default");

    config.routing.category_to_topic.insert("authentication".to_string(), auth_topic.clone());
    config.routing.category_to_topic.insert("network".to_string(), network_topic.clone());
    config.routing.default_topic = default_topic.clone();
    config.routing.topic_suffix = "".to_string();
    config.routing.topic_fields = vec!["event_category".to_string()];

    // Create pipeline
    let pipeline = PipelineState::new(config).expect("Failed to create pipeline");

    // Send messages with different categories
    pipeline
        .process(Bytes::from(r#"{"event_category":"authentication","user":"alice"}"#))
        .await
        .expect("Process failed");
    pipeline
        .process(Bytes::from(r#"{"event_category":"network","ip":"10.0.0.1"}"#))
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
    use dfe_receiver::config::{Config, SaslConfig};
    use dfe_receiver::pipeline::PipelineState;

    if !kafka_available().await {
        eprintln!("Skipping test: Kafka not available");
        return;
    }

    load_env();

    // Build config
    let mut config = Config::default();
    config.kafka.brokers = kafka_brokers()
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    config.kafka.client_id = "dfe-receiver-test".to_string();

    // Add SASL if configured
    if let Ok(user) = std::env::var("KAFKA_SASL_USER") {
        if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD") {
            let mechanism = std::env::var("KAFKA_SASL_MECHANISM")
                .unwrap_or_else(|_| "PLAIN".to_string());

            config.kafka.sasl = Some(SaslConfig {
                enabled: true,
                mechanism,
                username: user,
                password,
            });
        }
    }

    // Configure DLQ
    let dlq_topic = test_topic("dlq");
    config.routing.dlq.enabled = true;
    config.routing.dlq.topic = dlq_topic.clone();
    config.routing.topic_suffix = "".to_string();

    // Require a field that won't be present
    config.validation.required_fields = vec!["required_field".to_string()];
    config.validation.dlq_on_invalid = true;

    // Create pipeline
    let pipeline = PipelineState::new(config).expect("Failed to create pipeline");

    // Send message missing required field - should go to DLQ
    let result = pipeline
        .process(Bytes::from(r#"{"event_category":"test","data":"no required field"}"#))
        .await;

    // Should succeed (routed to DLQ)
    assert!(result.is_ok(), "DLQ routing should succeed: {result:?}");

    println!("✓ Invalid message routed to DLQ");

    // Give time for delivery
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify message in DLQ
    let consumer = create_consumer(&format!("test-group-{}", uuid::Uuid::new_v4()));
    consumer.subscribe(&[&dlq_topic]).expect("Failed to subscribe");

    let result = timeout(Duration::from_secs(10), consumer.recv()).await;
    assert!(result.is_ok(), "Expected message in DLQ: {dlq_topic}");
    println!("✓ Verified message in DLQ: {dlq_topic}");
}
