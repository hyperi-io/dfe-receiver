// Project:   dfe-receiver
// File:      src/sink/kafka/mod.rs
// Purpose:   Kafka producer sink that observes librdkafka delivery reports
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka sink over a receiver-owned librdkafka producer.
//!
//! Batching and compression stay librdkafka's, built from the same profile and
//! sizing surface scalo's `KafkaProducer` applies. The producer is constructed
//! here rather than taken from scalo because the delivery callback lives on the
//! context handed to librdkafka at creation and scalo's context has no delivery
//! body (scalo-rs#26).
//!
//! Owning the context is what makes a broker-side refusal visible. `send`
//! returns once the record is QUEUED; whether a broker ever took it arrives
//! later on the delivery report, which is where
//! `receiver_kafka_delivery_failures_total` and the sink's health flag come
//! from. Enqueue counters and the per-send duration histogram stay on `send`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use rdkafka::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::util::Timeout;
use rustc_hash::FxHashSet;
use scalo::transport::kafka::PRODUCER_HIGH_THROUGHPUT;
use tracing::{debug, error, info, trace};

use crate::config::KafkaConfig;
use crate::error::{Error, Result};
use crate::sink::Sink;

/// How long the shutdown flush gives librdkafka to drain.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

/// Sampled counter for Kafka enqueue errors (log 1 in 1000).
static KAFKA_ERRORS: AtomicU64 = AtomicU64::new(0);

/// What the broker did with records the sink has already answered for.
///
/// Written by [`DeliveryObserver`] on librdkafka's own polling thread, read by
/// [`KafkaSink::is_healthy`].
struct DeliveryState {
    /// False from the moment a delivery report comes back failed, true again on
    /// the next successful one. Kept apart from the enqueue flag because a
    /// queue that still accepts records would otherwise mask a broker that
    /// refuses every one of them.
    delivering: AtomicBool,

    /// Topics whose first delivery failure has been logged. A refusing broker
    /// reports per record, so the log gets one line per topic and the counters
    /// carry the volume. Bounded by the configured topic set.
    logged: RwLock<FxHashSet<Box<str>>>,
}

impl DeliveryState {
    fn new() -> Self {
        Self {
            delivering: AtomicBool::new(true),
            logged: RwLock::new(FxHashSet::default()),
        }
    }

    /// Whether this is the first delivery failure seen on `topic`.
    fn first_failure_on(&self, topic: &str) -> bool {
        if self.logged.read().contains(topic) {
            return false;
        }
        self.logged.write().insert(Box::from(topic))
    }
}

/// The producer context librdkafka calls once a record has been acknowledged,
/// refused or timed out.
struct DeliveryObserver {
    state: Arc<DeliveryState>,
}

impl DeliveryObserver {
    /// A broker took the record.
    fn delivered(&self) {
        metrics::counter!("receiver_kafka_delivered_total").increment(1);
        self.state.delivering.store(true, Ordering::Relaxed);
    }

    /// No broker took the record, and the sender was told otherwise long ago.
    fn failed(&self, topic: &str, err: &KafkaError) {
        metrics::counter!(
            "receiver_kafka_delivery_failures_total",
            "reason" => failure_reason(err)
        )
        .increment(1);
        self.state.delivering.store(false, Ordering::Relaxed);
        if self.state.first_failure_on(topic) {
            error!(
                topic,
                error = %err,
                "Kafka delivery failed -- the broker did not take a record the \
                 receiver had already accepted (first failure on this topic)"
            );
        }
    }
}

impl ClientContext for DeliveryObserver {}

impl ProducerContext for DeliveryObserver {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, (): Self::DeliveryOpaque) {
        match result {
            Ok(_) => self.delivered(),
            Err((err, msg)) => self.failed(msg.topic(), err),
        }
    }
}

/// The `reason` label for a delivery failure.
///
/// librdkafka's error codes are a closed enum, so the label cannot explode.
fn failure_reason(err: &KafkaError) -> String {
    match err {
        KafkaError::MessageProduction(code) => format!("{code:?}"),
        // A delivery report always carries a production error. Anything else is
        // librdkafka surprising us and does not deserve a label of its own.
        _ => "other".to_string(),
    }
}

/// Build the producer's librdkafka config from the scalo Kafka config.
///
/// The key order is scalo's `KafkaProducer::new`: profile defaults, then the
/// operator's `librdkafka_overrides`, then the sizing surface, which wins. Keep
/// it in step -- a different order runs different batching, compression and ack
/// settings than the rest of the platform.
fn producer_client_config(config: &scalo::transport::KafkaConfig) -> ClientConfig {
    let mut client = ClientConfig::new();

    client.set("bootstrap.servers", config.brokers.join(","));
    client.set("client.id", &config.client_id);

    client.set("security.protocol", &config.security_protocol);
    if let Some(ref mechanism) = config.sasl_mechanism {
        client.set("sasl.mechanism", mechanism);
    }
    if let Some(ref username) = config.sasl_username {
        client.set("sasl.username", username);
    }
    if let Some(ref password) = config.sasl_password {
        client.set("sasl.password", password.expose());
    }

    if let Some(ref ca) = config.ssl_ca_location {
        client.set("ssl.ca.location", ca);
    }
    if let Some(ref cert) = config.ssl_certificate_location {
        client.set("ssl.certificate.location", cert);
    }
    if let Some(ref key) = config.ssl_key_location {
        client.set("ssl.key.location", key);
    }
    if config.ssl_skip_verify {
        client.set("enable.ssl.certificate.verification", "false");
    }

    for (key, value) in PRODUCER_HIGH_THROUGHPUT {
        client.set(*key, *value);
    }
    for (key, value) in &config.librdkafka_overrides {
        client.set(key, value);
    }
    for (key, value) in config.sizing.resolved_producer_map() {
        client.set(key, value);
    }

    client
}

/// Kafka sink backed by a `ThreadedProducer` and its delivery reports.
pub struct KafkaSink {
    producer: ThreadedProducer<DeliveryObserver>,
    delivery: Arc<DeliveryState>,

    /// False when librdkafka refuses to take a record into its queue, true
    /// again on the next accepted enqueue.
    queueing: AtomicBool,
}

impl KafkaSink {
    /// Create a new Kafka sink.
    pub fn new(config: &KafkaConfig) -> Result<Self> {
        let scalo_config = config.to_scalo_kafka_config_for_producer();
        let delivery = Arc::new(DeliveryState::new());
        let observer = DeliveryObserver {
            state: Arc::clone(&delivery),
        };

        let producer: ThreadedProducer<DeliveryObserver> = producer_client_config(&scalo_config)
            .create_with_context(observer)
            .map_err(|e| Error::Transport(format!("failed to create Kafka producer: {e}")))?;

        info!(
            brokers = ?config.brokers,
            profile = "high_throughput",
            "Kafka producer initialised"
        );

        Ok(Self {
            producer,
            delivery,
            queueing: AtomicBool::new(true),
        })
    }

    /// [`Sink::flush`] with the timeout as a parameter.
    ///
    /// `rd_kafka_flush` blocks until every record is delivered or the timeout
    /// expires, so it runs on the blocking pool. The shutdown path awaits this,
    /// and holding a runtime worker for the whole budget stalls the other
    /// listeners' shutdown and the metrics server with them.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] when the timeout expires with records still
    /// in flight -- those are lost when the process exits.
    pub(crate) async fn flush_within(&self, timeout: Duration) -> Result<()> {
        // A clone shares the producer; only the last one dropped stops the
        // polling thread, so the sink keeps its producer while this runs.
        let producer = self.producer.clone();
        let remaining = tokio::task::spawn_blocking(move || {
            let _ = producer.flush(Timeout::After(timeout));
            producer.in_flight_count().max(0) as usize
        })
        .await
        .map_err(|e| Error::Transport(format!("kafka flush task failed: {e}")))?;

        if remaining > 0 {
            error!(remaining, "Kafka flush timed out with messages in flight");
            self.delivery.delivering.store(false, Ordering::Relaxed);
            return Err(Error::Transport(format!(
                "kafka flush timed out with {remaining} messages still in flight -- \
                 they are lost on exit"
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for KafkaSink {
    /// Queue a record for delivery.
    ///
    /// Returns once librdkafka has taken the record into its own queue, which
    /// is not delivery: [`DeliveryObserver`] reports what a broker made of it
    /// later.
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
        let start = Instant::now();
        let bytes = payload.len() as u64;

        trace!(topic, bytes, "Kafka produce enqueue");

        let record: BaseRecord<'_, (), [u8]> = BaseRecord::to(topic).payload(payload.as_ref());
        match self.producer.send(record) {
            Ok(()) => {
                let elapsed = start.elapsed();
                let elapsed_secs = elapsed.as_secs_f64();
                metrics::histogram!("receiver_kafka_send_duration_seconds").record(elapsed_secs);
                metrics::counter!("receiver_kafka_sends_total").increment(1);
                metrics::counter!("receiver_kafka_bytes_sent_total").increment(bytes);
                debug!(
                    topic,
                    bytes,
                    duration_us = elapsed.as_micros(),
                    "Kafka message enqueued"
                );
                self.queueing.store(true, Ordering::Relaxed);
                Ok(())
            }
            Err((e, _)) => {
                metrics::counter!("receiver_kafka_send_errors_total").increment(1);
                // librdkafka refuses the same bytes on every retry, and the queue itself is fine.
                if e.rdkafka_error_code() == Some(RDKafkaErrorCode::MessageSizeTooLarge) {
                    return Err(Error::Rejected(format!("kafka refused the record: {e}")));
                }
                if scalo::logger::log_sampled(&KAFKA_ERRORS, 1000) {
                    let total = KAFKA_ERRORS.load(Ordering::Relaxed);
                    error!(error = %e, topic, total_errors = total, "Kafka send failed (1 in 1000)");
                }
                self.queueing.store(false, Ordering::Relaxed);
                Err(Error::Transport(format!("kafka send failed: {e}")))
            }
        }
    }

    /// Flush queued records, then report what librdkafka could not deliver.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] when the timeout expires with records still
    /// in flight -- those are lost when the process exits.
    async fn flush(&self) -> Result<()> {
        self.flush_within(FLUSH_TIMEOUT).await
    }

    /// Healthy while librdkafka takes records AND a broker delivers them.
    ///
    /// Two flags rather than one: an accepted enqueue says nothing about the
    /// broker, so it must not clear a delivery failure.
    fn is_healthy(&self) -> bool {
        self.queueing.load(Ordering::Relaxed) && self.delivery.delivering.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TEST-NET-1 (RFC 5737), reserved for documentation -- never routable.
    const UNROUTABLE_BROKER: &str = "192.0.2.1:9092";

    /// A sink config pointed at nothing, with the local message timeout set so
    /// a test does not wait out librdkafka's five-minute default.
    fn unroutable_config(message_timeout_ms: &str) -> KafkaConfig {
        let mut config = KafkaConfig {
            brokers: vec![UNROUTABLE_BROKER.to_string()],
            ..KafkaConfig::default()
        };
        config.librdkafka_overrides.insert(
            "message.timeout.ms".to_string(),
            message_timeout_ms.to_string(),
        );
        config
    }

    fn timed_out() -> KafkaError {
        KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut)
    }

    #[test]
    fn failure_reason_is_the_librdkafka_error_code() {
        assert_eq!(failure_reason(&timed_out()), "MessageTimedOut");
    }

    #[test]
    fn failure_reason_falls_back_for_a_non_production_error() {
        assert_eq!(failure_reason(&KafkaError::Canceled), "other");
    }

    /// A broker refusing every record reports per record. Only the first line
    /// per topic is logged; the counters carry the rest.
    #[test]
    fn a_topic_logs_its_first_delivery_failure_only() {
        let state = DeliveryState::new();
        assert!(state.first_failure_on("events"));
        assert!(!state.first_failure_on("events"));
        assert!(state.first_failure_on("audit"));
    }

    #[test]
    fn health_follows_the_delivery_report_in_both_directions() {
        let state = Arc::new(DeliveryState::new());
        let observer = DeliveryObserver {
            state: Arc::clone(&state),
        };

        observer.failed("events", &timed_out());
        assert!(!state.delivering.load(Ordering::Relaxed));

        observer.delivered();
        assert!(state.delivering.load(Ordering::Relaxed));
    }

    /// A record no broker ever takes has to reach the sink's health flag.
    ///
    /// No broker and no Docker: `message.timeout.ms` expires locally against an
    /// unroutable address, so this is librdkafka's own delivery report carrying
    /// its own error code, not a hand-built one.
    #[tokio::test]
    async fn a_broker_that_never_takes_a_record_makes_the_sink_unhealthy() {
        let sink = KafkaSink::new(&unroutable_config("1000")).unwrap();
        sink.send("stranded", Bytes::from_static(b"{}"))
            .await
            .expect("librdkafka queues locally regardless of broker reachability");
        assert!(
            sink.is_healthy(),
            "the record is only queued -- nothing is known about the broker yet"
        );

        // The report lands on librdkafka's polling thread once the local
        // message timeout expires.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while sink.is_healthy() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert!(
            !sink.is_healthy(),
            "the broker never took the record and the sink still reports healthy -- \
             a delivery failure is invisible to /readyz and to the dashboard"
        );
    }

    /// An accepted enqueue says nothing about the broker, so it must not clear
    /// a delivery failure. One flag for both would go healthy again on the very
    /// next record, and a broker refusing every one of them would read as
    /// healthy for as long as the local queue had room.
    #[tokio::test]
    async fn an_accepted_enqueue_does_not_clear_a_delivery_failure() {
        let sink = KafkaSink::new(&unroutable_config("60000")).unwrap();
        sink.delivery.delivering.store(false, Ordering::Relaxed);

        sink.send("stranded", Bytes::from_static(b"{}"))
            .await
            .expect("librdkafka queues locally regardless of broker reachability");

        assert!(
            !sink.is_healthy(),
            "a queued record cleared a broker-side delivery failure"
        );
    }

    /// A record over `message.max.bytes` is refused for good at enqueue, and
    /// the producer queue stays healthy: one oversized record says nothing
    /// about the next.
    #[tokio::test]
    async fn a_record_over_the_size_ceiling_is_rejected() {
        let sink = KafkaSink::new(&unroutable_config("60000")).unwrap();
        let ceiling = scalo::transport::kafka::MESSAGE_MAX_BYTES as usize;

        let result = sink
            .send("events", Bytes::from(vec![b'x'; ceiling + 1]))
            .await;

        assert!(matches!(result, Err(Error::Rejected(_))), "got {result:?}");
        assert!(sink.is_healthy());
    }

    /// The shutdown flush must not hold a runtime worker.
    ///
    /// One worker, so an inline `rd_kafka_flush` starves every other task for
    /// the whole timeout. On the real shutdown path those tasks are the other
    /// listeners and the metrics server.
    #[tokio::test(flavor = "current_thread")]
    async fn flush_leaves_the_runtime_worker_free() {
        let sink = KafkaSink::new(&unroutable_config("60000")).unwrap();
        sink.send("stranded", Bytes::from_static(b"{}"))
            .await
            .expect("librdkafka queues locally regardless of broker reachability");

        let ticks = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&ticks);
        let ticker = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });

        let result = sink.flush_within(Duration::from_millis(500)).await;
        ticker.abort();

        assert!(
            result.is_err(),
            "the record cannot be delivered, so the flush has to time out"
        );
        assert!(
            ticks.load(Ordering::Relaxed) > 0,
            "no other task ran during the flush -- it held the only runtime worker"
        );
    }
}
