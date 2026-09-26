// Project:   dfe-receiver
// File:      src/sink/kafka/mod.rs
// Purpose:   Kafka producer sink that observes librdkafka delivery reports
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka sink over a receiver-owned librdkafka producer.
//!
//! Batching and compression stay librdkafka's, configured by scalo's
//! `producer_client_config`, the builder scalo's `KafkaProducer` uses. The
//! producer is constructed here rather than taken from scalo because the
//! delivery callback lives on the context handed to librdkafka at creation and
//! scalo's context has no delivery body (scalo-rs#26).
//!
//! Owning the context is what makes a broker-side refusal visible. `send`
//! returns once the record is QUEUED; whether a broker ever took it arrives
//! later on the delivery report, which is where
//! `receiver_kafka_delivery_failures_total` and the sink's health flag come
//! from. [`KafkaSink::send_held`] carries a piece of a held request with the
//! record, and the report settles it. Enqueue counters and the per-send
//! duration histogram stay on the enqueue.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
use scalo::transport::kafka::{PRODUCER_HIGH_THROUGHPUT, producer_client_config};
use scalo::transport::{DeliveryStatus, PieceFinalizer};
use tracing::{debug, error, info, trace, warn};

use crate::config::KafkaConfig;
use crate::error::{Error, Result};
use crate::sink::{FailureLatch, SINK_RETRY_AFTER, Sink};

/// librdkafka's delivery opaque: the held request's piece the report settles,
/// or none for a record answered at enqueue.
type Settlement = Box<Option<PieceFinalizer>>;

/// How long the shutdown flush gives librdkafka to drain.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

/// Sampled counter for Kafka enqueue errors (log 1 in 1000).
static KAFKA_ERRORS: AtomicU64 = AtomicU64::new(0);

/// What the broker did with the records the sink queued.
///
/// Written by [`DeliveryObserver`] on librdkafka's own polling thread, read by
/// [`KafkaSink::is_healthy`].
struct DeliveryState {
    /// Tripped by a failed delivery report, cleared by the next successful
    /// one. Kept apart from the enqueue latch because a queue that still
    /// accepts records would otherwise mask a broker that refuses every one of
    /// them.
    failures: FailureLatch,

    /// Topics whose first delivery failure has been logged. A refusing broker
    /// reports per record, so the log gets one line per topic and the counters
    /// carry the volume. Bounded by the configured topic set.
    logged: RwLock<FxHashSet<Box<str>>>,

    /// Records librdkafka has queued whose delivery report is not recorded yet.
    /// Its own out-queue length also counts the stats, log and error events on
    /// its main queue, so it is not a record count.
    awaiting_report: AtomicU64,
}

impl DeliveryState {
    fn new() -> Self {
        Self {
            failures: FailureLatch::default(),
            logged: RwLock::new(FxHashSet::default()),
            awaiting_report: AtomicU64::new(0),
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
        self.state.failures.clear();
    }

    /// No broker confirmed the record. A message timeout can expire while a
    /// produce request is in flight, so a broker may still have appended it.
    fn failed(&self, topic: &str, err: &KafkaError) {
        metrics::counter!(
            "receiver_kafka_delivery_failures_total",
            "reason" => failure_reason(err)
        )
        .increment(1);
        self.state.failures.trip();
        if self.state.first_failure_on(topic) {
            error!(
                topic,
                error = %err,
                "Kafka delivery failed -- no broker confirmed a record \
                 (first failure on this topic)"
            );
        }
    }
}

impl ClientContext for DeliveryObserver {}

impl ProducerContext for DeliveryObserver {
    type DeliveryOpaque = Settlement;

    fn delivery(&self, result: &DeliveryResult<'_>, settlement: Self::DeliveryOpaque) {
        let status = match result {
            Ok(_) => {
                self.delivered();
                DeliveryStatus::Delivered
            }
            Err((err, msg)) => {
                self.failed(msg.topic(), err);
                DeliveryStatus::Errored
            }
        };
        if let Some(piece) = *settlement {
            piece.report(status);
        }
        self.state.awaiting_report.fetch_sub(1, Ordering::Relaxed);
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

/// Build the producer's librdkafka config with scalo's producer builder.
///
/// scalo's high-throughput profile and `message_timeout`, as
/// `message.timeout.ms`, form the profile-defaults layer, so the sizing
/// surface and an operator's override under either librdkafka name win over both.
fn producer_config(
    config: &scalo::transport::KafkaConfig,
    message_timeout: Option<Duration>,
) -> ClientConfig {
    let timeout_ms = message_timeout.map(|timeout| timeout.as_millis().to_string());
    let mut defaults = PRODUCER_HIGH_THROUGHPUT.to_vec();
    if let Some(ref ms) = timeout_ms {
        defaults.push((MESSAGE_TIMEOUT_KEY, ms.as_str()));
    }
    producer_client_config(config, &defaults)
}

/// The librdkafka key bounding how long a record may wait for its broker.
const MESSAGE_TIMEOUT_KEY: &str = "message.timeout.ms";

/// Kafka sink backed by a `ThreadedProducer` and its delivery reports.
pub struct KafkaSink {
    producer: ThreadedProducer<DeliveryObserver>,
    delivery: Arc<DeliveryState>,

    /// Tripped when librdkafka refuses to take a record into its queue,
    /// cleared by the next accepted enqueue.
    refusing: FailureLatch,

    /// The `message.timeout.ms` this producer runs with for held answers,
    /// `None` for librdkafka's own.
    #[cfg(test)]
    held_message_timeout: Option<Duration>,
}

impl KafkaSink {
    /// Create a new Kafka sink.
    ///
    /// `held_message_timeout` is the `message.timeout.ms` to run with while
    /// some listener holds its answer for a delivery report, unless the
    /// operator set one: a request whose hold ran out is resent, and a first
    /// copy still queued past it would reach the broker as well.
    pub fn new(config: &KafkaConfig, held_message_timeout: Option<Duration>) -> Result<Self> {
        let scalo_config = config.to_scalo_kafka_config_for_producer();
        let delivery = Arc::new(DeliveryState::new());
        let observer = DeliveryObserver {
            state: Arc::clone(&delivery),
        };

        if let Some(held) = held_message_timeout
            && let Some(set) = scalo_config.librdkafka_overrides.get(MESSAGE_TIMEOUT_KEY)
            && set.parse::<u128>().is_ok_and(|ms| ms >= held.as_millis())
        {
            warn!(
                message_timeout_ms = %set,
                held_ms = held.as_millis(),
                "kafka.librdkafka_overrides sets message.timeout.ms past the time a \
                 listener holds its answer, so a request can be answered unavailable \
                 while its record is still queued and later lands twice"
            );
        }

        let producer: ThreadedProducer<DeliveryObserver> =
            producer_config(&scalo_config, held_message_timeout)
                .create_with_context(observer)
                .map_err(|e| Error::Transport(format!("failed to create Kafka producer: {e}")))?;

        info!(
            brokers = ?config.brokers,
            profile = "high_throughput",
            held_message_timeout_ms = held_message_timeout.map(|t| t.as_millis()),
            "Kafka producer initialised"
        );

        Ok(Self {
            producer,
            delivery,
            refusing: FailureLatch::default(),
            #[cfg(test)]
            held_message_timeout,
        })
    }

    /// The `message.timeout.ms` this producer was built with for held answers.
    #[cfg(test)]
    pub(crate) fn held_message_timeout(&self) -> Option<Duration> {
        self.held_message_timeout
    }

    /// Queue a record whose delivery report settles `piece`.
    ///
    /// # Errors
    ///
    /// The enqueue error, with the piece when librdkafka handed it back, so the
    /// caller settles a record refused for good elsewhere. A piece dropped
    /// unreported counts as `Errored`.
    pub fn send_held(
        &self,
        topic: &str,
        payload: &Bytes,
        piece: PieceFinalizer,
    ) -> std::result::Result<(), (Error, Option<PieceFinalizer>)> {
        self.enqueue(topic, payload, Box::new(Some(piece)))
            .map_err(|(e, settlement)| (e, *settlement))
    }

    /// Hand one record to librdkafka's queue, with the piece its report
    /// settles.
    fn enqueue(
        &self,
        topic: &str,
        payload: &Bytes,
        settlement: Settlement,
    ) -> std::result::Result<(), (Error, Settlement)> {
        let start = Instant::now();
        let bytes = payload.len() as u64;

        trace!(topic, bytes, "Kafka produce enqueue");

        let record: BaseRecord<'_, (), [u8], Settlement> =
            BaseRecord::with_opaque_to(topic, settlement).payload(payload.as_ref());
        // Counted before the enqueue, since the report can land before `send` returns.
        self.delivery
            .awaiting_report
            .fetch_add(1, Ordering::Relaxed);
        match self.producer.send(record) {
            Ok(()) => {
                let elapsed = start.elapsed();
                metrics::histogram!("receiver_kafka_send_duration_seconds")
                    .record(elapsed.as_secs_f64());
                metrics::counter!("receiver_kafka_sends_total").increment(1);
                metrics::counter!("receiver_kafka_bytes_sent_total").increment(bytes);
                debug!(
                    topic,
                    bytes,
                    duration_us = elapsed.as_micros(),
                    "Kafka message enqueued"
                );
                self.refusing.clear();
                Ok(())
            }
            Err((e, record)) => {
                self.delivery
                    .awaiting_report
                    .fetch_sub(1, Ordering::Relaxed);
                metrics::counter!("receiver_kafka_send_errors_total").increment(1);
                // librdkafka refuses the same bytes on every retry, and the queue itself is fine.
                if e.rdkafka_error_code() == Some(RDKafkaErrorCode::MessageSizeTooLarge) {
                    return Err((
                        Error::Rejected(format!("kafka refused the record: {e}")),
                        record.delivery_opaque,
                    ));
                }
                if scalo::logger::log_sampled(&KAFKA_ERRORS, 1000) {
                    let total = KAFKA_ERRORS.load(Ordering::Relaxed);
                    error!(error = %e, topic, total_errors = total, "Kafka send failed (1 in 1000)");
                }
                self.refusing.trip();
                Err((
                    Error::Transport(format!("kafka send failed: {e}")),
                    record.delivery_opaque,
                ))
            }
        }
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
        let delivery = Arc::clone(&self.delivery);
        let remaining = tokio::task::spawn_blocking(move || {
            // An Err only says the timeout expired, so the count says how many records it stranded.
            let _ = producer.flush(Timeout::After(timeout));
            delivery.awaiting_report.load(Ordering::Relaxed)
        })
        .await
        .map_err(|e| Error::Transport(format!("kafka flush task failed: {e}")))?;

        if remaining > 0 {
            error!(remaining, "Kafka flush timed out with messages in flight");
            self.delivery.failures.trip();
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
    /// is not delivery: `DeliveryObserver` reports what a broker made of it
    /// later.
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
        self.enqueue(topic, &payload, Box::new(None))
            .map_err(|(e, _)| e)
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

    /// Healthy while librdkafka takes records AND a broker delivers them, and
    /// again [`SINK_RETRY_AFTER`] after a failure, so traffic tries it.
    ///
    /// Two latches rather than one: an accepted enqueue says nothing about the
    /// broker, so it must not clear a delivery failure.
    fn is_healthy(&self) -> bool {
        self.refusing.admits(SINK_RETRY_AFTER) && self.delivery.failures.admits(SINK_RETRY_AFTER)
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

    /// The default config turns librdkafka stats off on the client the sink
    /// builds, over the profile's own interval: nothing here reads them.
    #[test]
    fn the_default_client_builds_no_stats() {
        assert!(
            PRODUCER_HIGH_THROUGHPUT
                .iter()
                .any(|(key, _)| *key == "statistics.interval.ms"),
            "the profile no longer sets a stats interval, so this test proves nothing"
        );
        let scalo_config = KafkaConfig::default().to_scalo_kafka_config_for_producer();
        let client = producer_config(&scalo_config, None);
        assert_eq!(client.get("statistics.interval.ms"), Some("0"));
    }

    /// Holding answers bounds how long a record waits for its broker, and an
    /// operator's own setting still wins, under either librdkafka name.
    #[test]
    fn a_held_message_timeout_applies_unless_the_operator_set_one() {
        let held = Some(Duration::from_secs(20));
        let scalo_config = KafkaConfig::default().to_scalo_kafka_config_for_producer();
        assert_eq!(
            producer_config(&scalo_config, None).get(MESSAGE_TIMEOUT_KEY),
            None,
            "answers at enqueue keep librdkafka's own timeout"
        );
        assert_eq!(
            producer_config(&scalo_config, held).get(MESSAGE_TIMEOUT_KEY),
            Some("20000")
        );

        let overridden = unroutable_config("5000").to_scalo_kafka_config_for_producer();
        assert_eq!(
            producer_config(&overridden, held).get(MESSAGE_TIMEOUT_KEY),
            Some("5000")
        );

        let mut by_alias = KafkaConfig::default();
        by_alias
            .librdkafka_overrides
            .insert("delivery.timeout.ms".to_string(), "5000".to_string());
        let client = producer_config(&by_alias.to_scalo_kafka_config_for_producer(), held);
        assert_eq!(client.get("delivery.timeout.ms"), Some("5000"));
        assert_eq!(
            client.get(MESSAGE_TIMEOUT_KEY),
            None,
            "both names set leave the timeout to hash order"
        );
    }

    /// The value librdkafka itself runs for `key` once it takes `client`.
    fn librdkafka_runs(client: &ClientConfig, key: &str) -> String {
        client
            .create_native_config()
            .expect("librdkafka takes every key the config sets")
            .get(key)
            .expect("librdkafka knows the key")
    }

    /// An operator's override wins over the sizing surface, and one under the
    /// other librdkafka name replaces the sizing default instead of racing it.
    #[test]
    fn an_override_wins_over_sizing_under_either_librdkafka_name() {
        let sized = KafkaConfig::default()
            .to_scalo_kafka_config_for_producer()
            .sizing
            .resolved_producer_map();
        assert_eq!(
            sized.get("compression.type").map(String::as_str),
            Some("zstd"),
            "sizing no longer sets the codec, so this test proves nothing"
        );
        assert!(
            sized.get("linger.ms").is_some_and(|ms| ms != "5"),
            "sizing no longer sets a linger other than 5, so this test proves nothing"
        );

        let mut named = KafkaConfig::default();
        for (key, value) in [("compression.type", "lz4"), ("linger.ms", "5")] {
            named
                .librdkafka_overrides
                .insert(key.to_string(), value.to_string());
        }
        let client = producer_config(&named.to_scalo_kafka_config_for_producer(), None);
        assert_eq!(client.get("compression.type"), Some("lz4"));
        assert_eq!(client.get("linger.ms"), Some("5"));
        assert_eq!(
            client.get("compression.level"),
            None,
            "zstd's level stays off another codec"
        );
        assert_eq!(librdkafka_runs(&client, "compression.codec"), "lz4");
        assert_eq!(librdkafka_runs(&client, "linger.ms"), "5");

        let mut by_alias = KafkaConfig::default();
        by_alias
            .librdkafka_overrides
            .insert("compression.codec".to_string(), "lz4".to_string());
        let client = producer_config(&by_alias.to_scalo_kafka_config_for_producer(), None);
        assert_eq!(client.get("compression.codec"), Some("lz4"));
        assert_eq!(
            client.get("compression.type"),
            None,
            "both names set leave the codec to hash order"
        );
        assert_eq!(librdkafka_runs(&client, "compression.codec"), "lz4");
    }

    fn held_ticket() -> (
        scalo::transport::ack::Tickets,
        scalo::transport::ack::Ticket,
    ) {
        let tickets = scalo::transport::ack::Tickets::new("test", 1 << 20);
        let ticket = tickets
            .admit(2, std::time::Instant::now() + Duration::from_secs(30))
            .expect("admitted");
        (tickets, ticket)
    }

    /// A record no broker confirms settles its held request `Errored`.
    ///
    /// No broker and no Docker: `message.timeout.ms` expires locally against an
    /// unroutable address, and the failed report reaches the request.
    #[tokio::test]
    async fn a_delivery_failure_reaches_the_held_request() {
        let sink = KafkaSink::new(&unroutable_config("1000"), None).unwrap();
        let (_tickets, ticket) = held_ticket();

        sink.send_held("stranded", &Bytes::from_static(b"{}"), ticket.piece())
            .unwrap_or_else(|(e, _)| panic!("librdkafka queues locally: {e}"));

        let outcome = tokio::time::timeout(Duration::from_secs(20), ticket.outcome())
            .await
            .expect("the report arrives once the message timeout expires");
        assert_eq!(
            outcome,
            scalo::transport::ack::TicketOutcome::Errored,
            "a record no broker confirmed must not answer success"
        );
    }

    /// A record answered at enqueue lasts a broker outage only as long as its
    /// producer's message timeout: once librdkafka gives it up, nothing resends
    /// it. A held timeout on the same producer would cut that to the hold.
    #[tokio::test]
    async fn an_enqueued_record_lasts_only_as_long_as_its_producers_timeout() {
        let config = KafkaConfig {
            brokers: vec![UNROUTABLE_BROKER.to_string()],
            ..KafkaConfig::default()
        };
        let held = KafkaSink::new(&config, Some(Duration::from_millis(500))).unwrap();
        let own = KafkaSink::new(&config, None).unwrap();
        for sink in [&held, &own] {
            sink.send("stranded", Bytes::from_static(b"{}"))
                .await
                .unwrap();
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while held.delivery.awaiting_report.load(Ordering::Relaxed) > 0
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            held.delivery.awaiting_report.load(Ordering::Relaxed),
            0,
            "the held timeout never gave the record up"
        );
        assert!(
            held.delivery.failures.is_tripped(),
            "and reported it failed"
        );
        assert_eq!(
            own.delivery.awaiting_report.load(Ordering::Relaxed),
            1,
            "librdkafka's own timeout still holds the record"
        );
    }

    /// A record refused at enqueue hands its piece back, so the caller can
    /// dead-letter it rather than fail the whole request.
    #[tokio::test]
    async fn a_record_refused_at_enqueue_hands_its_piece_back() {
        let sink = KafkaSink::new(&unroutable_config("60000"), None).unwrap();
        let (_tickets, ticket) = held_ticket();
        let ceiling = scalo::transport::kafka::MESSAGE_MAX_BYTES as usize;

        let refused = sink.send_held(
            "events",
            &Bytes::from(vec![b'x'; ceiling + 1]),
            ticket.piece(),
        );

        let Err((Error::Rejected(_), Some(piece))) = refused else {
            panic!("expected a refusal with its piece, got {refused:?}");
        };
        piece.report(DeliveryStatus::Rejected);
        assert_eq!(
            ticket.outcome().await,
            scalo::transport::ack::TicketOutcome::Rejected
        );
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
        assert!(state.failures.is_tripped());

        observer.delivered();
        assert!(!state.failures.is_tripped());
    }

    /// A record no broker ever takes has to reach the sink's health flag.
    ///
    /// No broker and no Docker: `message.timeout.ms` expires locally against an
    /// unroutable address, so this is librdkafka's own delivery report carrying
    /// its own error code, not a hand-built one.
    #[tokio::test]
    async fn a_broker_that_never_takes_a_record_makes_the_sink_unhealthy() {
        let sink = KafkaSink::new(&unroutable_config("1000"), None).unwrap();
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
        let sink = KafkaSink::new(&unroutable_config("60000"), None).unwrap();
        sink.delivery.failures.trip();

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
        let sink = KafkaSink::new(&unroutable_config("60000"), None).unwrap();
        let ceiling = scalo::transport::kafka::MESSAGE_MAX_BYTES as usize;

        let result = sink
            .send("events", Bytes::from(vec![b'x'; ceiling + 1]))
            .await;

        assert!(matches!(result, Err(Error::Rejected(_))), "got {result:?}");
        assert!(sink.is_healthy());
    }

    /// A record librdkafka refused never gets a delivery report, so it must not
    /// count as in flight at the shutdown flush.
    #[tokio::test]
    async fn a_refused_record_leaves_nothing_in_flight() {
        let sink = KafkaSink::new(&unroutable_config("60000"), None).unwrap();
        let ceiling = scalo::transport::kafka::MESSAGE_MAX_BYTES as usize;
        let refused = sink
            .send("events", Bytes::from(vec![b'x'; ceiling + 1]))
            .await;
        assert!(
            matches!(refused, Err(Error::Rejected(_))),
            "got {refused:?}"
        );

        let flushed = sink.flush_within(Duration::ZERO).await;
        assert!(
            flushed.is_ok(),
            "a refused record read as stranded: {flushed:?}"
        );
    }

    /// A timed-out flush reports the records still in flight, not librdkafka's
    /// out-queue length, which also counts the stats, log and error events
    /// waiting on the main queue.
    ///
    /// The test holds a read lock the first-failure log needs to write, so the
    /// polling thread stalls inside the one record's delivery report and stats
    /// events queue up behind it. A zero timeout flushes without polling them
    /// away.
    #[tokio::test]
    async fn a_timed_out_flush_counts_records_not_queued_events() {
        let mut config = unroutable_config("1000");
        config
            .librdkafka_overrides
            .insert("statistics.interval.ms".to_string(), "200".to_string());
        let sink = KafkaSink::new(&config, None).unwrap();
        sink.send("stranded", Bytes::from_static(b"{}"))
            .await
            .expect("librdkafka queues locally regardless of broker reachability");

        let state = Arc::clone(&sink.delivery);
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let _read = state.logged.read();
            let _ = held_tx.send(());
            let _ = release_rx.recv();
        });
        held_rx.recv().expect("the lock holder started");

        // Health drops as the report starts, just before it blocks on the lock.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while sink.is_healthy() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let report_started = !sink.is_healthy();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let result = sink.flush_within(Duration::ZERO).await;

        // Released before any assert: dropping the sink joins the stalled polling thread.
        let _ = release_tx.send(());
        holder.join().expect("the lock holder finished");

        assert!(report_started, "the record's delivery report never arrived");
        let message = result
            .expect_err("the record's report has not finished, so it is still in flight")
            .to_string();
        assert!(message.contains("with 1 messages"), "{message}");
    }

    /// The shutdown flush must not hold a runtime worker.
    ///
    /// One worker, so an inline `rd_kafka_flush` starves every other task for
    /// the whole timeout. On the real shutdown path those tasks are the other
    /// listeners and the metrics server.
    #[tokio::test(flavor = "current_thread")]
    async fn flush_leaves_the_runtime_worker_free() {
        let sink = KafkaSink::new(&unroutable_config("60000"), None).unwrap();
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
