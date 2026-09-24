// Project:   dfe-receiver
// File:      src/buffer/rejects.rs
// Purpose:   Take records a destination refuses for good off the delivery path
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Records a destination refuses for good.
//!
//! A refusal the destination repeats on every retry cannot be buffered and
//! retried in order: the record would sit at the head of the queue and hold up
//! every record behind it. It goes to the DLQ when one is configured and is
//! dropped when not, counted either way in `receiver_records_rejected_total`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use scalo::dlq::{Dlq, DlqEntry};
use tracing::{error, warn};

/// Sampled log counter for refused records (logs the first, then 1 in 100).
static REJECTED: AtomicU64 = AtomicU64::new(0);

/// What became of a refused record.
#[derive(Clone, Copy)]
enum Outcome {
    DeadLettered,
    Dropped,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Self::DeadLettered => "dead_lettered",
            Self::Dropped => "dropped",
        }
    }
}

/// Where a record goes once its destination has refused it for good.
///
/// The default has no DLQ, so it drops what it is given.
#[derive(Clone, Default)]
pub struct Rejects {
    dlq: Option<Arc<Dlq>>,
}

impl Rejects {
    /// Dead-letter refused records to `dlq`, or drop them when it is `None`.
    #[must_use]
    pub fn new(dlq: Option<Arc<Dlq>>) -> Self {
        Self { dlq }
    }

    /// Take `payload` off the delivery path: dead-letter it, or drop it.
    pub async fn dispose(&self, topic: &str, payload: &Bytes, reason: &str) {
        let outcome = match &self.dlq {
            Some(dlq) => {
                let mut entry = DlqEntry::new("receiver", reason, payload.to_vec());
                if !topic.is_empty() {
                    entry = entry.with_destination(topic);
                }
                match dlq.send(entry).await {
                    Ok(()) => Outcome::DeadLettered,
                    Err(e) => {
                        error!(error = %e, topic, "DLQ did not take a refused record");
                        Outcome::Dropped
                    }
                }
            }
            None => Outcome::Dropped,
        };

        metrics::counter!("receiver_records_rejected_total", "outcome" => outcome.label())
            .increment(1);

        if scalo::logger::log_sampled(&REJECTED, 100) {
            let total = REJECTED.load(Ordering::Relaxed);
            let bytes = payload.len();
            match outcome {
                Outcome::DeadLettered => warn!(
                    topic,
                    reason,
                    bytes,
                    total,
                    "Destination refused a record for good; dead-lettered it (logged 1 in 100)"
                ),
                Outcome::Dropped => error!(
                    topic,
                    reason,
                    bytes,
                    total,
                    "Destination refused a record for good; dropped it (logged 1 in 100)"
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use tokio_util::sync::CancellationToken;

    /// A refused record reaches the configured DLQ with its reason, its
    /// destination and the original bytes.
    #[tokio::test]
    async fn a_refused_record_lands_in_the_dlq() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::config::DlqConfig {
            mode: "file_only".to_string(),
            file_path: dir.path().display().to_string(),
            kafka_enabled: false,
            ..crate::config::DlqConfig::default()
        }
        .to_scalo_config();
        let dlq =
            Arc::new(Dlq::spawn(&config, "receiver", None, CancellationToken::new()).unwrap());
        let payload = Bytes::from_static(br#"{"x":1}"#);

        Rejects::new(Some(Arc::clone(&dlq)))
            .dispose("events", &payload, "destination refused the record")
            .await;
        dlq.flush().await.unwrap();

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
}
