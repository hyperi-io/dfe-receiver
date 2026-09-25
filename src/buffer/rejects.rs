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
//! either way in `receiver_records_rejected_total`.

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

/// What became of a refused record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum Disposal {
    /// The DLQ took it.
    DeadLettered,
    /// No DLQ is configured, so it was dropped.
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

/// Write `entry` and wait until the DLQ reports whether it holds it.
///
/// `send` returning means the entry is queued; the flush after it is what
/// reports a write the DLQ refused.
pub(crate) async fn dead_letter_confirmed(
    dlq: &Dlq,
    entry: DlqEntry,
) -> std::result::Result<(), DlqError> {
    dlq.send(entry).await?;
    dlq.flush().await
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

    /// Take `payload` off the delivery path: queue it on the DLQ, or drop it.
    pub async fn dispose(&self, topic: &str, payload: &Bytes, reason: &str) -> Disposal {
        let disposal = match &self.dlq {
            Some(dlq) => match dlq.send(entry(topic, payload, reason)).await {
                Ok(()) => Disposal::DeadLettered,
                Err(e) => {
                    error!(error = %e, topic, "DLQ did not take a refused record");
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
    /// `Errored`, so the sender resends. With no DLQ the refusal is the
    /// sender's to hear: the piece reports `Dropped` and the error names the
    /// record as refused for good.
    ///
    /// # Errors
    ///
    /// [`Error::Transport`] when the DLQ did not confirm the write, and
    /// [`Error::Rejected`] when there is no DLQ.
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
            Ok(()) => {
                piece.report(DeliveryStatus::Rejected);
                self.count(Disposal::DeadLettered, topic, payload, reason);
                Ok(())
            }
            Err(e) => {
                error!(error = %e, topic, "DLQ did not confirm a refused record; the sender is told to resend it");
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

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use scalo::transport::ack::{TicketOutcome, Tickets};
    use tokio_util::sync::CancellationToken;

    /// A file-only DLQ writing under `dir`.
    fn file_dlq(dir: &std::path::Path) -> Arc<Dlq> {
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
    fn refusing_dlq(dir: &std::path::Path) -> Arc<Dlq> {
        let dlq = file_dlq(dir);
        let service_dir = dir.join("receiver");
        let _ = std::fs::remove_dir_all(&service_dir);
        std::fs::write(&service_dir, b"not a directory").unwrap();
        dlq
    }

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
