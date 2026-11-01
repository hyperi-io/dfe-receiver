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
//! Tests skip when Docker is unavailable outside CI, and fail in it.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dfe_receiver::buffer::Rejects;
use dfe_receiver::buffer::adapter::ScaloSinkAdapter;
use dfe_receiver::sink::Sink;
use dfe_receiver::sink::kafka::KafkaSink;
use scalo::tiered_sink::{TieredSink, TieredSinkConfig};
use scalo::transport::{PayloadFormat, Record, RecordMeta};

use crate::common::{
    kafka_backend, kafka_consume_next, kafka_consumer, kafka_plain_config, start_kafka_container,
    test_topic,
};
use crate::{skip_if_no_docker, test_name};

/// Build a KafkaSink pointed at the given test backend config.
fn make_sink(kf: &crate::common::KafkaTestConfig) -> KafkaSink {
    let cfg = kf.to_receiver_kafka_config();
    KafkaSink::new(&cfg, None).expect("KafkaSink creation failed")
}

#[tokio::test]
async fn test_kafka_sink_send_and_consume() {
    let Some((_handle, kf)) = kafka_backend(test_name!()).await else {
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
    let Some((_handle, kf)) = kafka_backend(test_name!()).await else {
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

    // The flush confirmed every record, so every one is on the topic.
    let mut seen = std::collections::BTreeSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    while tokio::time::Instant::now() < deadline && seen.len() < 100 {
        if let Some(payload) = kafka_consume_next(&consumer, Duration::from_secs(5)).await {
            let record: serde_json::Value = serde_json::from_slice(&payload).expect("JSON record");
            seen.insert(record["seq"].as_u64().expect("seq"));
        }
    }
    assert_eq!(
        seen.len(),
        100,
        "only {} of 100 records arrived",
        seen.len()
    );
}

#[tokio::test]
async fn test_kafka_sink_binary_payload() {
    let Some((_handle, kf)) = kafka_backend(test_name!()).await else {
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
    let Some((_handle, kf)) = kafka_backend(test_name!()).await else {
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
    let Some((_handle, kf)) = kafka_backend(test_name!()).await else {
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
    let Some((_handle, kf)) = kafka_backend(test_name!()).await else {
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

/// A record answered at enqueue outlives a broker outage longer than the
/// producer's message timeout: librdkafka gives it up, the sink queues it
/// again, and it lands once the broker is back.
#[tokio::test]
async fn an_enqueued_record_survives_an_outage_past_the_message_timeout() {
    const RECORDS: i64 = 20;
    skip_if_no_docker!();
    let (broker, bootstrap) = start_kafka_container(test_name!())
        .await
        .expect("Kafka container");
    let kf = kafka_plain_config(&bootstrap);
    let topic = test_topic("outage");
    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Paused before the sink first reaches it, so no produce request is ever
    // in flight: a record the broker already holds would land whatever the
    // sink did with its delivery report.
    broker.pause().await.expect("pause the broker");
    let mut cfg = kf.to_receiver_kafka_config();
    cfg.librdkafka_overrides
        .insert("message.timeout.ms".to_string(), "2000".to_string());
    let sink = KafkaSink::new(&cfg, None).expect("KafkaSink creation failed");
    for seq in 0..RECORDS {
        sink.send(&topic, Bytes::from(format!(r#"{{"seq":{seq}}}"#)))
            .await
            .expect("librdkafka queues locally while the broker is paused");
    }
    // Past message.timeout.ms, so librdkafka has given every record up.
    tokio::time::sleep(Duration::from_secs(6)).await;
    broker.unpause().await.expect("unpause the broker");

    let mut seen = std::collections::BTreeSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while seen.len() < RECORDS as usize && tokio::time::Instant::now() < deadline {
        let Some(payload) = kafka_consume_next(&consumer, Duration::from_secs(5)).await else {
            continue;
        };
        let record: serde_json::Value = serde_json::from_slice(&payload).expect("JSON record");
        if let Some(seq) = record["seq"].as_i64() {
            seen.insert(seq);
        }
    }
    assert_eq!(
        seen.len(),
        RECORDS as usize,
        "only {seen:?} of {RECORDS} records outlived the outage"
    );
}

/// The spool a [`KafkaSink`] drains through, at `dir`: a pass every 5 ms, and
/// a probe one send timeout after its circuit opens.
fn kafka_spool(dir: &std::path::Path) -> TieredSinkConfig {
    let mut config = TieredSinkConfig::new(dir.join("kafka"));
    config.drain_interval_ms = 5;
    config.circuit_reset_timeout_ms = config.send_timeout_ms;
    config
}

/// The record a [`SinkBackend`](dfe_receiver::buffer::SinkBackend) spools for
/// `payload` on `topic`.
fn spooled_record(topic: &str, payload: String) -> Record {
    Record {
        payload: Bytes::from(payload),
        key: Some(Arc::from(topic)),
        headers: Vec::new(),
        metadata: RecordMeta {
            timestamp_ms: None,
            format: PayloadFormat::Json,
        },
    }
}

/// Spool `payloads` for `topic` at `dir`, in order.
///
/// The producer behind the spool can hold one record and is already holding
/// it, against a broker that does not exist, so every record it is offered
/// is refused and spooled.
async fn spool_records(dir: &std::path::Path, topic: &str, payloads: &[String]) {
    let mut cfg = dfe_receiver::config::KafkaConfig {
        brokers: vec!["192.0.2.1:9092".to_string()],
        ..dfe_receiver::config::KafkaConfig::default()
    };
    cfg.librdkafka_overrides
        .insert("queue.buffering.max.messages".to_string(), "1".to_string());
    let full = KafkaSink::new(&cfg, None).expect("KafkaSink creation failed");
    full.send(topic, Bytes::from_static(b"{}"))
        .await
        .expect("librdkafka queues one record locally");
    let spool = TieredSink::new(
        ScaloSinkAdapter::new(Arc::new(full), Rejects::default()),
        kafka_spool(dir),
    )
    .await
    .expect("spool opens");
    for payload in payloads {
        spool
            .send(&spooled_record(topic, payload.clone()))
            .await
            .expect("a full producer queue spools the record");
    }
    assert_eq!(
        spool.spool_len().await,
        payloads.len(),
        "not every record spooled"
    );
    spool.shutdown().await;
}

/// A spooled record leaves the spool only once a broker confirms it.
///
/// A record the drain hands to librdkafka is held in memory until its delivery
/// report, so a crash in between loses it unless the spool still has it. With
/// the broker paused no report arrives, so every record stays spooled, and
/// each lands once the broker is back.
#[tokio::test]
async fn a_spooled_record_stays_spooled_until_a_broker_confirms_it() {
    const RECORDS: usize = 5;
    skip_if_no_docker!();
    let (broker, bootstrap) = start_kafka_container(test_name!())
        .await
        .expect("Kafka container");
    let kf = kafka_plain_config(&bootstrap);
    let topic = test_topic("spool-ack");
    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let dir = tempfile::tempdir().expect("spool dir");
    let payloads: Vec<String> = (0..RECORDS)
        .map(|seq| format!(r#"{{"seq":{seq}}}"#))
        .collect();
    spool_records(dir.path(), &topic, &payloads).await;

    // Paused before the sink first reaches it, so no produce request is in
    // flight when the drain starts.
    broker.pause().await.expect("pause the broker");
    let mut cfg = kf.to_receiver_kafka_config();
    cfg.librdkafka_overrides
        .insert("message.timeout.ms".to_string(), "60000".to_string());
    let sink = KafkaSink::new(&cfg, None).expect("KafkaSink creation failed");
    let spool = TieredSink::new(
        ScaloSinkAdapter::new(Arc::new(sink), Rejects::default()),
        kafka_spool(dir.path()),
    )
    .await
    .expect("the spool reopens");
    // Long enough for the drain to offer every record, and for none to get a
    // delivery report.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let held = spool.spool_len().await;
    broker.unpause().await.expect("unpause the broker");
    assert_eq!(
        held, RECORDS,
        "the spool let go of records no broker confirmed"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    while !spool.spool_is_empty().await && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        spool.spool_is_empty().await,
        "{} records still spooled a minute after the broker came back",
        spool.spool_len().await
    );
    spool.shutdown().await;

    let mut seen = std::collections::BTreeSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    while seen.len() < RECORDS && tokio::time::Instant::now() < deadline {
        let Some(payload) = kafka_consume_next(&consumer, Duration::from_secs(5)).await else {
            continue;
        };
        let record: serde_json::Value = serde_json::from_slice(&payload).expect("JSON record");
        if let Some(seq) = record["seq"].as_u64() {
            seen.insert(seq);
        }
    }
    assert_eq!(
        seen.len(),
        RECORDS,
        "only {seen:?} of {RECORDS} records landed"
    );
}

/// Create `topic`, one partition, with the topic `config` given.
async fn create_topic(kf: &crate::common::KafkaTestConfig, topic: &str, config: &[(&str, &str)]) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    let mut client = rdkafka::ClientConfig::new();
    client.set("bootstrap.servers", &kf.brokers);
    kf.apply_sasl(&mut client);
    let admin: AdminClient<DefaultClientContext> = client.create().expect("admin client");
    let new_topic = config.iter().fold(
        NewTopic::new(topic, 1, TopicReplication::Fixed(1)),
        |new_topic, (key, value)| new_topic.set(key, value),
    );
    for created in admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await
        .expect("create topics")
    {
        created.expect("topic created");
    }
}

/// A spooled record the broker refuses for good leaves the spool through the
/// reject path, so the records spooled behind it still drain.
///
/// The topic takes records of at most 1024 bytes and librdkafka sends up to
/// its own default, so only the broker refuses the first record.
#[tokio::test]
async fn a_spooled_record_the_broker_refuses_for_good_leaves_the_spool() {
    skip_if_no_docker!();
    let (_broker, bootstrap) = start_kafka_container(test_name!())
        .await
        .expect("Kafka container");
    let kf = kafka_plain_config(&bootstrap);
    let topic = test_topic("spool-refused");
    create_topic(&kf, &topic, &[("max.message.bytes", "1024")]).await;
    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let dir = tempfile::tempdir().expect("spool dir");
    let oversized = format!(r#"{{"seq":0,"pad":"{}"}}"#, "x".repeat(4096));
    spool_records(dir.path(), &topic, &[oversized, r#"{"seq":1}"#.to_string()]).await;

    let mut cfg = kf.to_receiver_kafka_config();
    // Compressed, the padding would fit under the topic's limit.
    cfg.librdkafka_overrides
        .insert("compression.type".to_string(), "none".to_string());
    let sink = KafkaSink::new(&cfg, None).expect("KafkaSink creation failed");
    let spool = TieredSink::new(
        ScaloSinkAdapter::new(Arc::new(sink), Rejects::default()),
        kafka_spool(dir.path()),
    )
    .await
    .expect("the spool reopens");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !spool.spool_is_empty().await && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let left = spool.spool_len().await;
    spool.shutdown().await;
    assert_eq!(left, 0, "the refused record holds up the spool");

    let landed = kafka_consume_next(&consumer, Duration::from_secs(30))
        .await
        .expect("the record behind the refused one never landed");
    let record: serde_json::Value = serde_json::from_slice(&landed).expect("JSON record");
    assert_eq!(record["seq"], 1, "{record}");
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
    let sink = KafkaSink::new(&cfg, None).expect("producer construction is local-only");

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
