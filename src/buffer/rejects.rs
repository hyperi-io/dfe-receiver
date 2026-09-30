// Project:   dfe-receiver
// File:      src/buffer/rejects.rs
// Purpose:   Take records a destination refuses for good off the delivery path
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Records a destination refuses for good.
//!
//! Only a refusal the record itself proves permanent comes here -- a payload
//! over the destination's size ceiling. Buffered and retried in order, it
//! would sit at the head of the queue and hold up every record behind it. It
//! goes to the DLQ when one is configured and is dropped when not, counted
//! either way in `receiver_records_rejected_total`. A write the DLQ does not
//! confirm leaves the record where it was -- queued, spilled, or its sender told
//! to resend -- so it is offered again. An entry no DLQ backend can ever hold
//! is dropped instead, since every retry would be refused the same.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use scalo::dlq::{Dlq, DlqEntry, DlqError};
use scalo::transport::{DeliveryStatus, PieceFinalizer};
use tracing::{error, warn};

use crate::error::{Error, Result};
use crate::metrics::Metrics;

/// Sampled log counter for refused records (logs the first, then 1 in 100).
static REJECTED: AtomicU64 = AtomicU64::new(0);

/// Sampled log counter for DLQ writes not confirmed, which repeat on every
/// offer while the DLQ is down.
static DLQ_REFUSED: AtomicU64 = AtomicU64::new(0);

/// Sampled log counter for dead letters no DLQ backend can hold.
static UNWRITABLE: AtomicU64 = AtomicU64::new(0);

/// What became of a refused record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum Disposal {
    /// The DLQ took it.
    DeadLettered,
    /// No DLQ is configured, or none of its backends can hold the entry, so
    /// it was dropped.
    Dropped,
    /// The DLQ did not take it.
    Refused,
}

impl Disposal {
    fn label(self) -> &'static str {
        match self {
            Self::DeadLettered => "dead_lettered",
            Self::Dropped => "dropped",
            Self::Refused => "dlq_refused",
        }
    }

    /// The answer for a sender not yet answered: a record the DLQ did not
    /// take is one it must send again.
    ///
    /// # Errors
    ///
    /// [`Error::Transport`] when the DLQ did not take the record.
    pub fn answer(self) -> Result<()> {
        match self {
            Self::DeadLettered | Self::Dropped => Ok(()),
            Self::Refused => Err(dlq_refused()),
        }
    }
}

/// The retryable error for a refused record the DLQ did not take.
fn dlq_refused() -> Error {
    Error::Transport("the dead-letter queue did not take a refused record".into())
}

/// What a dead letter the DLQ was asked to confirm came to.
#[derive(Debug)]
#[must_use]
pub(crate) enum DeadLetter {
    /// The DLQ holds the entry.
    Written,
    /// No DLQ backend can ever hold the entry, so it was dropped and counted
    /// in `pipeline_dead_letters_dropped_total`.
    Unwritable,
}

/// Write `entry` and wait until the DLQ reports whether it holds it.
///
/// The answer is about this entry alone: another writer's refusal never
/// reaches it. An entry every backend refuses by size is never written, since
/// each retry would be refused the same.
///
/// # Errors
///
/// The DLQ's refusal of a write that can clear on a retry.
pub(crate) async fn dead_letter_confirmed(
    dlq: &Dlq,
    entry: DlqEntry,
) -> std::result::Result<DeadLetter, DlqError> {
    if let Some(refusal) = dlq.refusal(&entry) {
        metrics::counter!("pipeline_dead_letters_dropped_total", "reason" => refusal.as_str())
            .increment(1);
        if scalo::logger::log_sampled(&UNWRITABLE, 100) {
            let total = UNWRITABLE.load(Ordering::Relaxed);
            error!(
                error = %refusal,
                destination = entry.destination.as_deref().unwrap_or(""),
                total,
                "No DLQ backend can hold this dead letter; dropped it (logged 1 in 100)"
            );
        }
        return Ok(DeadLetter::Unwritable);
    }
    dlq.write_confirmed(vec![entry])
        .await
        .map(|()| DeadLetter::Written)
}

/// Log a DLQ write that was not confirmed, the first and then 1 in 100.
fn log_dlq_refusal(error: &DlqError, topic: &str) {
    if scalo::logger::log_sampled(&DLQ_REFUSED, 100) {
        let total = DLQ_REFUSED.load(Ordering::Relaxed);
        error!(
            error = %error,
            topic,
            total,
            "DLQ did not confirm a refused record; it is offered again (logged 1 in 100)"
        );
    }
}

/// Where a record goes once its destination has refused it for good.
///
/// The default has no DLQ, so it drops what it is given.
#[derive(Clone, Default)]
pub struct Rejects {
    dlq: Option<Arc<Dlq>>,
    /// Counts dead-lettered records into `dfe_records_dlq_total`.
    metrics: Option<Arc<Metrics>>,
}

impl Rejects {
    /// Dead-letter refused records to `dlq`, or drop them when it is `None`.
    #[must_use]
    pub fn new(dlq: Option<Arc<Dlq>>, metrics: Option<Arc<Metrics>>) -> Self {
        Self { dlq, metrics }
    }

    /// Take `payload` off the delivery path: dead-letter it once the DLQ
    /// confirms the write, or drop it where no DLQ is configured or none of
    /// its backends can hold it.
    ///
    /// [`Disposal::Refused`] leaves the record with the caller, to keep and
    /// offer again.
    pub async fn dispose(&self, topic: &str, payload: &Bytes, reason: &str) -> Disposal {
        let disposal = match &self.dlq {
            Some(dlq) => match dead_letter_confirmed(dlq, entry(topic, payload, reason)).await {
                Ok(DeadLetter::Written) => Disposal::DeadLettered,
                Ok(DeadLetter::Unwritable) => Disposal::Dropped,
                Err(e) => {
                    log_dlq_refusal(&e, topic);
                    Disposal::Refused
                }
            },
            None => Disposal::Dropped,
        };
        self.count(disposal, topic, payload, reason);
        disposal
    }

    /// Settle a refused record of a held request.
    ///
    /// With a DLQ the piece reports `Rejected` once the DLQ confirmed the
    /// write; a write it refused leaves the piece unreported, which counts as
    /// `Errored`, so the sender resends. With no DLQ, or one none of whose
    /// backends can hold the entry, the refusal is the sender's to hear: the
    /// piece reports `Dropped` and the error names the record as refused for
    /// good.
    ///
    /// # Errors
    ///
    /// [`Error::Transport`] when the DLQ did not confirm the write, and
    /// [`Error::Rejected`] when the record cannot be dead-lettered.
    pub async fn dispose_held(
        &self,
        topic: &str,
        payload: &Bytes,
        reason: &str,
        piece: PieceFinalizer,
    ) -> Result<()> {
        let Some(dlq) = &self.dlq else {
            piece.report(DeliveryStatus::Dropped);
            self.count(Disposal::Dropped, topic, payload, reason);
            return Err(Error::Rejected(reason.to_string()));
        };
        match dead_letter_confirmed(dlq, entry(topic, payload, reason)).await {
            Ok(DeadLetter::Written) => {
                piece.report(DeliveryStatus::Rejected);
                self.count(Disposal::DeadLettered, topic, payload, reason);
                Ok(())
            }
            Ok(DeadLetter::Unwritable) => {
                piece.report(DeliveryStatus::Dropped);
                self.count(Disposal::Dropped, topic, payload, reason);
                Err(Error::Rejected(reason.to_string()))
            }
            Err(e) => {
                log_dlq_refusal(&e, topic);
                self.count(Disposal::Refused, topic, payload, reason);
                Err(dlq_refused())
            }
        }
    }

    fn count(&self, disposal: Disposal, topic: &str, payload: &Bytes, reason: &str) {
        metrics::counter!("receiver_records_rejected_total", "outcome" => disposal.label())
            .increment(1);
        if let (Disposal::DeadLettered, Some(metrics)) = (disposal, &self.metrics) {
            metrics.inc_messages_dlq();
        }

        if scalo::logger::log_sampled(&REJECTED, 100) {
            let total = REJECTED.load(Ordering::Relaxed);
            let bytes = payload.len();
            match disposal {
                Disposal::DeadLettered => warn!(
                    topic,
                    reason,
                    bytes,
                    total,
                    "Destination refused a record for good; dead-lettered it (logged 1 in 100)"
                ),
                Disposal::Dropped => error!(
                    topic,
                    reason,
                    bytes,
                    total,
                    "Destination refused a record for good; dropped it (logged 1 in 100)"
                ),
                Disposal::Refused => error!(
                    topic,
                    reason,
                    bytes,
                    total,
                    "Destination refused a record for good and the DLQ did not take it (logged 1 in 100)"
                ),
            }
        }
    }
}

/// The DLQ entry for a record `topic` refused.
fn entry(topic: &str, payload: &Bytes, reason: &str) -> DlqEntry {
    let entry = DlqEntry::new("receiver", reason, payload.to_vec());
    if topic.is_empty() {
        entry
    } else {
        entry.with_destination(topic)
    }
}

/// DLQ fixtures shared by the buffer tests.
#[cfg(test)]
pub(crate) mod test_dlq {
    use std::sync::Arc;

    use scalo::dlq::Dlq;
    use tokio_util::sync::CancellationToken;

    /// A file-only DLQ writing under `dir`.
    pub(crate) fn file_dlq(dir: &std::path::Path) -> Arc<Dlq> {
        let config = crate::config::DlqConfig {
            mode: "file_only".to_string(),
            file_path: dir.display().to_string(),
            kafka_enabled: false,
            ..crate::config::DlqConfig::default()
        }
        .to_scalo_config();
        Arc::new(Dlq::spawn(&config, "receiver", None, CancellationToken::new()).unwrap())
    }

    /// A file-only DLQ whose every write fails: its service directory is a
    /// regular file.
    pub(crate) fn refusing_dlq(dir: &std::path::Path) -> Arc<Dlq> {
        let dlq = file_dlq(dir);
        let service_dir = dir.join("receiver");
        let _ = std::fs::remove_dir_all(&service_dir);
        std::fs::write(&service_dir, b"not a directory").unwrap();
        dlq
    }

    /// A Kafka-only DLQ at a broker nothing reaches, so its one backend has a
    /// size ceiling and nothing beside it takes what that refuses.
    pub(crate) fn kafka_only_dlq() -> Arc<Dlq> {
        let config = crate::config::DlqConfig {
            mode: "kafka_only".to_string(),
            file_enabled: false,
            ..crate::config::DlqConfig::default()
        }
        .to_scalo_config();
        let kafka = crate::config::KafkaConfig {
            brokers: vec!["192.0.2.1:9092".to_string()],
            ..crate::config::KafkaConfig::default()
        }
        .to_scalo_kafka_config()
        .unwrap();
        Arc::new(Dlq::spawn(&config, "receiver", Some(&kafka), CancellationToken::new()).unwrap())
    }

    /// A record whose DLQ entry, base64 payload included, is over any Kafka
    /// ceiling.
    pub(crate) fn oversize_record() -> bytes::Bytes {
        bytes::Bytes::from(vec![
            b'x';
            scalo::transport::kafka::MESSAGE_MAX_BYTES as usize
        ])
    }
}

/// A recorder counting each counter by `name{label=value,...}`.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct CountedKeys(std::sync::Mutex<std::collections::HashMap<String, Arc<AtomicU64>>>);

#[cfg(test)]
impl CountedKeys {
    /// What the counter keyed `key` reached.
    pub(crate) fn get(&self, key: &str) -> u64 {
        self.0
            .lock()
            .unwrap()
            .get(key)
            .map_or(0, |hits| hits.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
struct Hits(Arc<AtomicU64>);

#[cfg(test)]
impl metrics::CounterFn for Hits {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

#[cfg(test)]
impl metrics::Recorder for CountedKeys {
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
        let labels: Vec<String> = key
            .labels()
            .map(|l| format!("{}={}", l.key(), l.value()))
            .collect();
        let name = format!("{}{{{}}}", key.name(), labels.join(","));
        let hits = Arc::clone(self.0.lock().unwrap().entry(name).or_default());
        metrics::Counter::from_arc(Arc::new(Hits(hits)))
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

#[cfg(test)]
mod tests {
    use super::test_dlq::{self, file_dlq, refusing_dlq};
    use super::*;
    use base64::Engine;
    use scalo::transport::ack::{TicketOutcome, Tickets};

    fn held_ticket(tickets: &Tickets) -> scalo::transport::ack::Ticket {
        tickets
            .admit(
                1,
                std::time::Instant::now() + std::time::Duration::from_secs(10),
            )
            .unwrap()
    }

    /// A refused record reaches the configured DLQ with its reason, its
    /// destination and the original bytes, and counts as dead-lettered.
    #[tokio::test]
    async fn a_refused_record_lands_in_the_dlq() {
        let dir = tempfile::tempdir().unwrap();
        let dlq = file_dlq(dir.path());
        let metrics = Arc::new(Metrics::default());
        let payload = Bytes::from_static(br#"{"x":1}"#);

        let disposal = Rejects::new(Some(Arc::clone(&dlq)), Some(Arc::clone(&metrics)))
            .dispose("events", &payload, "destination refused the record")
            .await;
        dlq.flush().await.unwrap();

        assert_eq!(disposal, Disposal::DeadLettered);
        assert_eq!(metrics.get_messages_dlq(), 1);

        let written = std::fs::read_to_string(dir.path().join("receiver/dlq.ndjson")).unwrap();
        let lines: Vec<&str> = written.lines().collect();
        assert_eq!(
            lines.len(),
            1,
            "one refused record, one DLQ entry: {written}"
        );
        let entry: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(entry["reason"], "destination refused the record");
        assert_eq!(entry["destination"], "events");
        assert_eq!(
            entry["payload"],
            base64::engine::general_purpose::STANDARD.encode(&payload)
        );
    }

    /// Run `future` on a runtime of this thread, counted by `recorder`.
    fn counted<T>(recorder: &CountedKeys, future: impl std::future::Future<Output = T>) -> T {
        metrics::with_local_recorder(recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(future)
        })
    }

    /// A held record too large for every DLQ backend is dropped and counted
    /// without a write, and its sender hears it refused for good rather than
    /// told to retry a write that can never land.
    #[test]
    fn a_dead_letter_no_backend_can_hold_is_dropped_and_answered() {
        let recorder = CountedKeys::default();
        let (result, outcome) = counted(&recorder, async {
            let rejects = Rejects::new(Some(test_dlq::kafka_only_dlq()), None);
            let tickets = Tickets::new("test", 1 << 20);
            let ticket = held_ticket(&tickets);
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                rejects.dispose_held(
                    "events",
                    &test_dlq::oversize_record(),
                    "too large",
                    ticket.piece(),
                ),
            )
            .await
            .expect("answered without waiting on a write");
            (result, ticket.outcome().await)
        });

        assert!(matches!(result, Err(Error::Rejected(_))), "got {result:?}");
        assert_eq!(outcome, TicketOutcome::Dropped);
        assert_eq!(
            recorder.get("pipeline_dead_letters_dropped_total{reason=too_large}"),
            1
        );
        assert_eq!(
            recorder.get("receiver_records_rejected_total{outcome=dropped}"),
            1
        );
    }

    /// Answered at enqueue, the same record leaves the queue dropped rather
    /// than waiting at its head for a write that can never land.
    #[test]
    fn a_queued_dead_letter_no_backend_can_hold_is_dropped() {
        let recorder = CountedKeys::default();
        let disposal = counted(&recorder, async {
            Rejects::new(Some(test_dlq::kafka_only_dlq()), None)
                .dispose("events", &test_dlq::oversize_record(), "too large")
                .await
        });

        assert_eq!(disposal, Disposal::Dropped);
        assert_eq!(
            recorder.get("pipeline_dead_letters_dropped_total{reason=too_large}"),
            1
        );
    }

    /// A write the DLQ does not confirm is reported, not counted as taken.
    #[tokio::test]
    async fn a_dlq_that_does_not_confirm_the_write_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::default());

        let disposal = Rejects::new(Some(refusing_dlq(dir.path())), Some(Arc::clone(&metrics)))
            .dispose("events", &Bytes::from_static(b"{}"), "too large")
            .await;

        assert_eq!(disposal, Disposal::Refused);
        assert_eq!(metrics.get_messages_dlq(), 0);
    }

    /// With no DLQ the record is dropped, and a drop is not a dead-letter.
    #[tokio::test]
    async fn a_dropped_record_does_not_count_as_dead_lettered() {
        let metrics = Arc::new(Metrics::default());

        let disposal = Rejects::new(None, Some(Arc::clone(&metrics)))
            .dispose("events", &Bytes::from_static(b"{}"), "too large")
            .await;

        assert_eq!(disposal, Disposal::Dropped);
        assert_eq!(metrics.get_messages_dlq(), 0);
    }

    /// A held record reports `Rejected` only once the DLQ confirmed the write.
    #[tokio::test]
    async fn a_held_record_is_rejected_once_the_dlq_confirms_it() {
        let dir = tempfile::tempdir().unwrap();
        let rejects = Rejects::new(Some(file_dlq(dir.path())), None);
        let tickets = Tickets::new("test", 1 << 20);
        let ticket = held_ticket(&tickets);

        rejects
            .dispose_held(
                "events",
                &Bytes::from_static(b"{}"),
                "too large",
                ticket.piece(),
            )
            .await
            .unwrap();

        assert_eq!(ticket.outcome().await, TicketOutcome::Rejected);
    }

    /// A DLQ that refuses the write settles the held request `Errored`, never
    /// dropped: the DLQ did not confirm it, so the sender keeps the record.
    #[tokio::test]
    async fn a_dlq_refusal_is_errored_not_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let rejects = Rejects::new(Some(refusing_dlq(dir.path())), None);
        let tickets = Tickets::new("test", 1 << 20);
        let ticket = held_ticket(&tickets);

        let result = rejects
            .dispose_held(
                "events",
                &Bytes::from_static(b"{}"),
                "too large",
                ticket.piece(),
            )
            .await;

        assert!(
            matches!(result, Err(ref e) if e.is_retryable()),
            "got {result:?}"
        );
        assert_eq!(ticket.outcome().await, TicketOutcome::Errored);
    }

    /// With no DLQ a held record's refusal goes back to its sender, and the
    /// request's other records are not held up by it.
    #[tokio::test]
    async fn with_no_dlq_the_sender_hears_the_refusal() {
        let rejects = Rejects::new(None, None);
        let tickets = Tickets::new("test", 1 << 20);
        let ticket = held_ticket(&tickets);

        let result = rejects
            .dispose_held(
                "events",
                &Bytes::from_static(b"{}"),
                "too large",
                ticket.piece(),
            )
            .await;

        assert!(matches!(result, Err(Error::Rejected(_))), "got {result:?}");
        assert_eq!(ticket.outcome().await, TicketOutcome::Dropped);
    }

    /// At enqueue, a DLQ refusal is answered as retryable, so the sender keeps
    /// the record.
    #[test]
    fn a_dlq_refusal_is_answered_as_retryable() {
        assert!(Disposal::DeadLettered.answer().is_ok());
        assert!(Disposal::Dropped.answer().is_ok());
        assert!(Disposal::Refused.answer().is_err_and(|e| e.is_retryable()));
    }
}
