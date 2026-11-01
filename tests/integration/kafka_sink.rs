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
use tokio_util::sync::CancellationToken;

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

/// Spool each `(topic, payload)` record at `dir`, in order.
///
/// The producer behind the spool can hold one record and is already holding
/// it, against a broker that does not exist, so every record it is offered
/// is refused and spooled.
async fn spool_records(dir: &std::path::Path, records: &[(&str, String)]) {
    let mut cfg = dfe_receiver::config::KafkaConfig {
        brokers: vec!["192.0.2.1:9092".to_string()],
        ..dfe_receiver::config::KafkaConfig::default()
    };
    cfg.librdkafka_overrides
        .insert("queue.buffering.max.messages".to_string(), "1".to_string());
    let full = KafkaSink::new(&cfg, None).expect("KafkaSink creation failed");
    full.send("filler", Bytes::from_static(b"{}"))
        .await
        .expect("librdkafka queues one record locally");
    let spool = TieredSink::new(
        ScaloSinkAdapter::new(Arc::new(full), Rejects::default()),
        kafka_spool(dir),
    )
    .await
    .expect("spool opens");
    for (topic, payload) in records {
        spool
            .send(&spooled_record(topic, payload.clone()))
            .await
            .expect("a full producer queue spools the record");
    }
    assert_eq!(
        spool.spool_len().await,
        records.len(),
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
    let records: Vec<(&str, String)> = (0..RECORDS)
        .map(|seq| (topic.as_str(), format!(r#"{{"seq":{seq}}}"#)))
        .collect();
    spool_records(dir.path(), &records).await;

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

/// Producer settings under which librdkafka refuses a record over 1024 bytes
/// at enqueue.
const REFUSED_AT_ENQUEUE: &[(&str, &str)] = &[("message.max.bytes", "1024")];

/// Producer settings under which a record to a topic the cluster lacks is
/// refused for good on its delivery report, a second after it is queued.
const REFUSED_AT_DELIVERY: &[(&str, &str)] = &[
    ("allow.auto.create.topics", "false"),
    ("topic.metadata.propagation.max.ms", "1000"),
];

/// A spooled record the broker refuses for good leaves the spool through the
/// reject path, so the records spooled behind it still drain.
#[tokio::test]
async fn a_spooled_record_the_broker_refuses_for_good_leaves_the_spool() {
    skip_if_no_docker!();
    let (_broker, bootstrap) = start_kafka_container(test_name!())
        .await
        .expect("Kafka container");
    let kf = kafka_plain_config(&bootstrap);
    let topic = test_topic("spool-refused");
    let missing = test_topic("missing");
    create_topic(&kf, &topic, &[]).await;
    let consumer = kafka_consumer(&kf, &topic).expect("consumer setup");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let dir = tempfile::tempdir().expect("spool dir");
    spool_records(
        dir.path(),
        &[
            (missing.as_str(), r#"{"seq":0}"#.to_string()),
            (topic.as_str(), r#"{"seq":1}"#.to_string()),
        ],
    )
    .await;

    let mut cfg = kf.to_receiver_kafka_config();
    for (key, value) in REFUSED_AT_DELIVERY {
        cfg.librdkafka_overrides
            .insert((*key).to_string(), (*value).to_string());
    }
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

/// A receiver whose every record goes to `topic`, its producer run with
/// `overrides`, writing dead letters under `dlq`.
fn bus_to(
    kf: &crate::common::KafkaTestConfig,
    topic: &str,
    overrides: &[(&str, &str)],
    dlq: Option<&std::path::Path>,
) -> dfe_receiver::config::Config {
    use dfe_receiver::config::{Config, DestinationRef, DestinationSpec, KafkaDestination};

    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.kafka = kf.to_receiver_kafka_config();
    // Compressed, the padding would fit under the topic's limit.
    config
        .kafka
        .librdkafka_overrides
        .insert("compression.type".to_string(), "none".to_string());
    for (key, value) in overrides {
        config
            .kafka
            .librdkafka_overrides
            .insert((*key).to_string(), (*value).to_string());
    }
    config.destinations.named.insert(
        "refusing".to_string(),
        DestinationSpec {
            grpc: None,
            kafka: Some(KafkaDestination {
                topic: Some(topic.to_string()),
            }),
        },
    );
    config.destinations.default = DestinationRef::One("refusing".to_string());
    config.routing.dlq.enabled = dlq.is_some();
    if let Some(dir) = dlq {
        config.routing.dlq.mode = "file_only".to_string();
        config.routing.dlq.kafka_enabled = false;
        config.routing.dlq.file_path = dir.display().to_string();
    }
    config
}

/// The HTTP status a held request of one `record` is answered with, through
/// [`bus_to`] with the same arguments.
async fn held_answer(
    kf: &crate::common::KafkaTestConfig,
    topic: &str,
    overrides: &[(&str, &str)],
    dlq: Option<&std::path::Path>,
    record: String,
) -> reqwest::StatusCode {
    use dfe_receiver::config::SharedConfig;
    use dfe_receiver::pipeline::PipelineState;
    use dfe_receiver::server::http::HttpHandler;
    use dfe_receiver::server::traits::ProtocolHandler;

    let config = bus_to(kf, topic, overrides, dlq);
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("pipeline init"),
    );
    let handler = HttpHandler::new(
        config.server.bind_address.clone(),
        pipeline,
        Arc::new(dfe_receiver::metrics::Metrics::default()),
    );
    let bound = handler.bound_addr();
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(server_shutdown).await });
    let addr = crate::common::bound_addr("HTTP", &bound, &mut task).await;

    let status = reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .header("content-type", "application/json")
        .body(record)
        .send()
        .await
        .expect("request failed")
        .status();
    shutdown.cancel();
    status
}

/// The dead letters written under `dir`, one per line of every file in it.
fn dead_letters(dir: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                dead_letters(&path)
            } else {
                std::fs::read_to_string(&path).map_or(0, |text| text.lines().count())
            }
        })
        .sum()
}

/// A held request whose record the broker refuses for good on its delivery
/// report is answered as one librdkafka refuses at enqueue: accepted once a
/// DLQ holds it, a bad request where none is configured. Neither is retried.
#[tokio::test]
async fn a_held_record_refused_at_delivery_is_answered_as_one_refused_at_enqueue() {
    skip_if_no_docker!();
    let (_broker, bootstrap) = start_kafka_container(test_name!())
        .await
        .expect("Kafka container");
    let kf = kafka_plain_config(&bootstrap);
    let topic = test_topic("held-refused");
    let missing = test_topic("missing");
    create_topic(&kf, &topic, &[]).await;
    let record = || format!(r#"{{"pad":"{}"}}"#, "x".repeat(4096));

    for (case, with_dlq, want) in [
        ("no DLQ", false, reqwest::StatusCode::BAD_REQUEST),
        ("file DLQ", true, reqwest::StatusCode::ACCEPTED),
    ] {
        let dir = tempfile::tempdir().expect("DLQ dir");
        let enqueue_dlq = dir.path().join("enqueue");
        let delivery_dlq = dir.path().join("delivery");
        let dlq_at = |path: &std::path::PathBuf| with_dlq.then_some(path.clone());

        let at_enqueue = held_answer(
            &kf,
            &topic,
            REFUSED_AT_ENQUEUE,
            dlq_at(&enqueue_dlq).as_deref(),
            record(),
        )
        .await;
        let at_delivery = held_answer(
            &kf,
            &missing,
            REFUSED_AT_DELIVERY,
            dlq_at(&delivery_dlq).as_deref(),
            record(),
        )
        .await;

        assert_eq!(at_enqueue, want, "{case}: the enqueue-time answer moved");
        assert_eq!(
            at_delivery, at_enqueue,
            "{case}: a refusal on the delivery report is answered differently"
        );
        let want_letters = usize::from(with_dlq);
        assert_eq!(dead_letters(&enqueue_dlq), want_letters, "{case}: enqueue");
        assert_eq!(
            dead_letters(&delivery_dlq),
            want_letters,
            "{case}: delivery"
        );
    }
}

/// A broker's size refusal fails the whole batch librdkafka sent, so it says
/// nothing about any one record in it: each is answered retryable, never
/// refused for good, and a record that fits is not dead-lettered with one
/// that does not.
#[tokio::test]
async fn a_broker_size_refusal_is_not_pinned_on_a_record() {
    use dfe_receiver::config::SharedConfig;
    use dfe_receiver::pipeline::PipelineState;

    skip_if_no_docker!();
    let (_broker, bootstrap) = start_kafka_container(test_name!())
        .await
        .expect("Kafka container");
    let kf = kafka_plain_config(&bootstrap);
    let topic = test_topic("size-refused");
    create_topic(&kf, &topic, &[("max.message.bytes", "1024")]).await;
    let fits = Bytes::from_static(br#"{"n":1}"#);
    let oversized = Bytes::from(format!(r#"{{"pad":"{}"}}"#, "x".repeat(4096)));
    let state = PipelineState::new(
        SharedConfig::new(bus_to(&kf, &topic, &[], None)),
        CancellationToken::new(),
    )
    .await
    .expect("pipeline init");
    let acks = state.acks(
        "http",
        scalo::transport::AcknowledgementsConfig::default(),
        Some(Duration::from_secs(30)),
    );

    let outcome = state
        .process_batch_acked(
            &[fits, oversized],
            &acks,
            None,
            &crate::common::http_ingest(),
        )
        .await;

    assert_eq!(
        outcome.rejected, 0,
        "a size refusal was pinned on a record: {outcome:?}"
    );
    assert!(
        matches!(outcome.unavailable, Some(ref e) if e.is_retryable()),
        "{outcome:?}"
    );
}

/// A recorder summing every series of one counter, whatever its labels.
struct CounterTotal {
    name: &'static str,
    total: Arc<std::sync::atomic::AtomicU64>,
}

impl CounterTotal {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            total: Arc::default(),
        }
    }

    fn get(&self) -> u64 {
        self.total.load(std::sync::atomic::Ordering::Relaxed)
    }
}

struct Adds(Arc<std::sync::atomic::AtomicU64>);

impl metrics::CounterFn for Adds {
    fn increment(&self, value: u64) {
        self.0
            .fetch_add(value, std::sync::atomic::Ordering::Relaxed);
    }

    fn absolute(&self, _: u64) {}
}

impl metrics::Recorder for CounterTotal {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        if key.name() == self.name {
            metrics::Counter::from_arc(Arc::new(Adds(Arc::clone(&self.total))))
        } else {
            metrics::Counter::noop()
        }
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// A held record refused for good adds no usage, on its delivery report as at
/// enqueue: usage counts only what was taken.
#[tokio::test]
async fn a_held_record_refused_for_good_counts_no_usage() {
    use dfe_receiver::config::SharedConfig;
    use dfe_receiver::pipeline::PipelineState;

    skip_if_no_docker!();
    let (_broker, bootstrap) = start_kafka_container(test_name!())
        .await
        .expect("Kafka container");
    let kf = kafka_plain_config(&bootstrap);
    let topic = test_topic("held-usage");
    let missing = test_topic("missing");
    create_topic(&kf, &topic, &[]).await;
    let small = Bytes::from_static(br#"{"n":1}"#);
    let oversized = Bytes::from(format!(r#"{{"pad":"{}"}}"#, "x".repeat(4096)));

    for (case, to, overrides, record, want) in [
        ("taken", &topic, &[][..], &small, (1, 0, small.len())),
        (
            "refused at enqueue",
            &topic,
            REFUSED_AT_ENQUEUE,
            &oversized,
            (0, 1, 0),
        ),
        (
            "refused at delivery",
            &missing,
            REFUSED_AT_DELIVERY,
            &small,
            (0, 1, 0),
        ),
    ] {
        let usage = CounterTotal::new("usage_ingested_bytes_total");
        let _local = metrics::set_default_local_recorder(&usage);
        let state = PipelineState::new(
            SharedConfig::new(bus_to(&kf, to, overrides, None)),
            CancellationToken::new(),
        )
        .await
        .expect("pipeline init");
        let acks = state.acks(
            "http",
            scalo::transport::AcknowledgementsConfig::default(),
            Some(Duration::from_secs(30)),
        );

        let outcome = state
            .process_batch_acked(
                std::slice::from_ref(record),
                &acks,
                None,
                &crate::common::http_ingest(),
            )
            .await;

        assert_eq!(
            (outcome.accepted, outcome.rejected, usage.get() as usize),
            want,
            "{case}: (accepted, rejected, usage bytes) from {outcome:?}"
        );
    }
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
