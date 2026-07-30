// Project:   dfe-receiver
// File:      tests/integration/kafka_sink.rs
// Purpose:   End-to-end Kafka sink tests via testcontainers
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka sink integration tests.
//!
//! These tests spin up a fresh Kafka container per test via testcontainers-rs,
//! produce a message via `Sink::send`, then consume it back and verify the
//! payload round-trips correctly.
//!
//! Containers are stopped automatically when the test function returns (via
//! `ContainerAsync`'s `Drop` impl).
//!
//! Tests skip (not fail) when Docker is unavailable.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::time::Duration;

use bytes::Bytes;
use dfe_receiver::sink::Sink;
use dfe_receiver::sink::kafka::KafkaSink;

use crate::common::{kafka_backend, kafka_consume_next, kafka_consumer, test_topic};

/// Build a KafkaSink pointed at the given test backend config.
fn make_sink(kf: &crate::common::KafkaTestConfig) -> KafkaSink {
    let cfg = kf.to_receiver_kafka_config();
    KafkaSink::new(&cfg).expect("KafkaSink creation failed")
}

#[tokio::test]
async fn test_kafka_sink_send_and_consume() {
    let Some((_handle, kf)) = kafka_backend().await else {
        eprintln!("Skipping: no Kafka backend available (live auth failed and Docker unreachable)");
        return;
    };

    let topic = test_topic("send");
    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let sink = make_sink(&kf);
    let payload = format!(
        r#"{{"test":"kafka-sink","ts":"{}","id":{}}}"#,
        chrono::Utc::now().to_rfc3339(),
        uuid::Uuid::new_v4()
    );

    sink.send(&topic, Bytes::from(payload.clone()))
        .await
        .expect("send failed");
    sink.flush().await.expect("flush failed");

    let received = kafka_consume_next(&consumer, Duration::from_secs(30))
        .await
        .expect("message never arrived on topic");

    assert_eq!(received, payload.as_bytes(), "round-trip payload mismatch");
    assert!(sink.is_healthy(), "sink should be healthy after success");
}

#[tokio::test]
async fn test_kafka_sink_send_many() {
    let Some((_handle, kf)) = kafka_backend().await else {
        eprintln!("Skipping: no Kafka backend available (live auth failed and Docker unreachable)");
        return;
    };

    let topic = test_topic("batch");
    // Subscribe and wait for consumer to actually join the group
    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let sink = make_sink(&kf);

    for i in 0..100 {
        let payload = format!(r#"{{"seq":{i},"data":"batch-test"}}"#);
        sink.send(&topic, Bytes::from(payload))
            .await
            .expect("batch send failed");
    }
    sink.flush().await.expect("flush failed");

    // Collect messages with generous timeout — librdkafka may take a few
    // seconds to materialise all 100 records to the consumer.
    let mut count = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    while tokio::time::Instant::now() < deadline && count < 100 {
        match kafka_consume_next(&consumer, Duration::from_secs(5)).await {
            Some(_) => count += 1,
            None => {
                // No message in 5s — break early if we've already collected most
                if count >= 80 {
                    break;
                }
            }
        }
    }
    assert!(count >= 80, "expected ≥80 messages, got {count}/100");
}

#[tokio::test]
async fn test_kafka_sink_binary_payload() {
    let Some((_handle, kf)) = kafka_backend().await else {
        eprintln!("Skipping: no Kafka backend available (live auth failed and Docker unreachable)");
        return;
    };

    let topic = test_topic("binary");
    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let sink = make_sink(&kf);
    // Binary payload (null bytes, high bytes) must survive Kafka
    let payload: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
    sink.send(&topic, Bytes::from(payload.clone()))
        .await
        .expect("binary send failed");
    sink.flush().await.expect("flush failed");

    let received = kafka_consume_next(&consumer, Duration::from_secs(30))
        .await
        .expect("binary payload never arrived");
    assert_eq!(received, payload, "binary payload corrupted in transit");
}

#[tokio::test]
async fn test_kafka_sink_large_payload() {
    let Some((_handle, kf)) = kafka_backend().await else {
        eprintln!("Skipping: no Kafka backend available (live auth failed and Docker unreachable)");
        return;
    };

    let topic = test_topic("large");
    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let sink = make_sink(&kf);
    // 512 KiB — exercises librdkafka's internal batching/chunking
    let payload: Vec<u8> = (0..512 * 1024).map(|i| (i % 256) as u8).collect();
    sink.send(&topic, Bytes::from(payload.clone()))
        .await
        .expect("large send failed");
    sink.flush().await.expect("flush failed");

    let received = kafka_consume_next(&consumer, Duration::from_mins(1))
        .await
        .expect("large payload never arrived");
    assert_eq!(received.len(), payload.len(), "large payload size mismatch");
    assert_eq!(received, payload, "large payload content mismatch");
}

#[tokio::test]
async fn test_kafka_sink_multiple_topics() {
    let Some((_handle, kf)) = kafka_backend().await else {
        eprintln!("Skipping: no Kafka backend available (live auth failed and Docker unreachable)");
        return;
    };

    let topics = [
        test_topic("multi-a"),
        test_topic("multi-b"),
        test_topic("multi-c"),
    ];

    let consumers: Vec<_> = topics
        .iter()
        .map(|t| kafka_consumer(&kf, t).expect("consumer setup"))
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let sink = make_sink(&kf);
    for (i, topic) in topics.iter().enumerate() {
        let payload = format!(r#"{{"topic_idx":{i}}}"#);
        sink.send(topic, Bytes::from(payload))
            .await
            .expect("multi-topic send failed");
    }
    sink.flush().await.expect("flush failed");

    for (i, (topic, consumer)) in topics.iter().zip(consumers.iter()).enumerate() {
        let received = kafka_consume_next(consumer, Duration::from_secs(30))
            .await
            .unwrap_or_else(|| panic!("topic {topic} has no message"));
        let text = String::from_utf8_lossy(&received);
        assert!(
            text.contains(&format!("\"topic_idx\":{i}")),
            "topic {topic} got wrong payload: {text}"
        );
    }
}

#[tokio::test]
async fn test_kafka_sink_invalid_topic_recoverable() {
    let Some((_handle, kf)) = kafka_backend().await else {
        eprintln!("Skipping: no Kafka backend available (live auth failed and Docker unreachable)");
        return;
    };

    let sink = make_sink(&kf);
    // Null bytes in topic name are invalid. The sink must surface as error
    // (not panic) and remain usable for subsequent sends.
    let _ = sink.send("topic\0with\0nulls", Bytes::from(r#"{}"#)).await;

    // Recovery test: valid send should still work
    let good_topic = test_topic("recovery");
    sink.send(&good_topic, Bytes::from(r#"{"recovered":true}"#))
        .await
        .expect("sink unusable after invalid topic attempt");
    sink.flush().await.expect("flush should succeed");
}

/// A flush that times out with messages in flight must return `Err`.
///
/// `PipelineOrchestrator::run` gates its shutdown handling on
/// `if let Err(e) = kafka.flush().await`. An `error!` line plus `Ok(())`
/// leaves that branch unreachable however many messages are stranded, and the
/// process exits reporting a clean shutdown while losing every undelivered
/// record.
///
/// No broker and no Docker needed, deliberately: librdkafka accepts produce
/// calls into its local queue whether or not a broker is reachable, so an
/// unroutable address leaves messages in flight -- and this rule has to hold
/// in the configuration where container tests skip.
///
/// Takes ~30s: `KafkaSink::flush` hardcodes a 30-second librdkafka flush
/// timeout, and the profile's `message.timeout.ms` is longer than that, so
/// the records are still queued when the flush gives up.
#[tokio::test]
async fn test_kafka_sink_flush_timeout_is_an_error_not_a_clean_shutdown() {
    use dfe_receiver::config::KafkaConfig;

    // TEST-NET-1 (RFC 5737), reserved for documentation -- never routable.
    let cfg = KafkaConfig {
        brokers: vec!["192.0.2.1:9092".to_string()],
        ..KafkaConfig::default()
    };
    let sink = KafkaSink::new(&cfg).expect("producer construction is local-only");

    sink.send("unreachable-topic", Bytes::from(r#"{"stranded":true}"#))
        .await
        .expect("librdkafka queues locally regardless of broker reachability");

    let result = sink.flush().await;
    assert!(
        result.is_err(),
        "flush reported success with messages still in flight -- the \
         orchestrator's shutdown-flush check cannot fire and the records are \
         lost silently"
    );
    assert!(
        !sink.is_healthy(),
        "a sink that could not flush must not report healthy"
    );
}
