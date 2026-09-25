// Project:   dfe-receiver
// File:      src/buffer/adapter.rs
// Purpose:   Expose a receiver Sink as a scalo TransportSender for TieredSink
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Expose the receiver's [`Sink`](ReceiverSink) as a scalo [`TransportSender`].
//!
//! scalo's `TieredSink` wraps a `TransportSender` and, on the cold path, spills
//! whole [`Record`](scalo::transport::Record)s to disk -- payload,
//! routing key and headers all survive a
//! replay, with zero serialisation cost on the happy path. The receiver routes
//! by topic, so this adapter maps `Record.key` -> topic and forwards the payload
//! to the inner receiver sink.
//!
//! A transient inner-sink failure is reported as [`SendResult::Backpressured`]
//! (not `Fatal`) so the TieredSink spills the record rather than dropping it --
//! preserving at-least-once delivery across a downstream outage. A record the
//! inner sink refuses for good is settled here through [`Rejects`] and reported
//! as sent once the DLQ holds it, so it is not replayed; a DLQ write that is
//! not confirmed reports `Backpressured`, so the record stays spilled and is
//! offered again.

use std::sync::Arc;

use bytes::Bytes;
use scalo::transport::{SendResult, TransportBase, TransportResult, TransportSender};

use crate::buffer::{Disposal, Rejects};
use crate::error::Error;
use crate::sink::Sink as ReceiverSink;

/// Adapts a receiver [`Sink`](ReceiverSink) to scalo's [`TransportSender`], so it
/// can be wrapped directly in a `TieredSink` for disk spillover.
pub struct ScaloSinkAdapter<S: ReceiverSink> {
    inner: Arc<S>,
    rejects: Rejects,
}

impl<S: ReceiverSink + 'static> ScaloSinkAdapter<S> {
    pub fn new(inner: Arc<S>, rejects: Rejects) -> Self {
        Self { inner, rejects }
    }

    /// The receiver sink this adapter sends through.
    pub fn inner(&self) -> &Arc<S> {
        &self.inner
    }
}

impl<S: ReceiverSink + 'static> TransportBase for ScaloSinkAdapter<S> {
    async fn close(&self) -> TransportResult<()> {
        // Best-effort flush; the inner sink owns its own connection lifecycle.
        let _ = self.inner.flush().await;
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        self.inner.is_healthy()
    }

    fn name(&self) -> &'static str {
        "receiver-sink-adapter"
    }
}

impl<S: ReceiverSink + 'static> TransportSender for ScaloSinkAdapter<S> {
    async fn send(&self, key: &str, payload: Bytes) -> SendResult {
        match self.inner.send(key, payload.clone()).await {
            Ok(()) => SendResult::Ok,
            // Spilled, a record refused for good would be replayed forever
            // ahead of everything behind it on disk, so it leaves once settled.
            Err(Error::Rejected(reason)) => {
                match self.rejects.dispose(key, &payload, &reason).await {
                    Disposal::DeadLettered | Disposal::Dropped => SendResult::Ok,
                    Disposal::Refused => SendResult::Backpressured,
                }
            }
            // Transient: surface as Backpressured so the TieredSink spills to
            // disk (at-least-once) instead of dropping the record.
            Err(_) => SendResult::Backpressured,
        }
    }

    // send_batch uses the trait default: it forwards each Record via `send`,
    // keyed by the Record's own `key` -- exactly the topic routing we want.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::rejects::test_dlq;
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use scalo::transport::{PayloadFormat, Record, RecordMeta};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A receiver `Sink` test double: records (topic, payload) pairs, can be
    /// toggled to fail every send (to drive the Backpressured/spill path), and
    /// refuses a `bad` payload for good.
    struct FakeSink {
        healthy: AtomicBool,
        fail: AtomicBool,
        received: Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl FakeSink {
        fn new() -> Self {
            Self {
                healthy: AtomicBool::new(true),
                fail: AtomicBool::new(false),
                received: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ReceiverSink for FakeSink {
        async fn send(&self, topic: &str, payload: Bytes) -> crate::error::Result<()> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::Transport("forced failure".into()));
            }
            if payload.as_ref() == b"bad" {
                return Err(Error::Rejected("destination refused the record".into()));
            }
            self.received
                .lock()
                .push((topic.to_string(), payload.to_vec()));
            Ok(())
        }

        async fn flush(&self) -> crate::error::Result<()> {
            Ok(())
        }

        fn is_healthy(&self) -> bool {
            self.healthy.load(Ordering::SeqCst)
        }
    }

    fn record(topic: &str, payload: &[u8]) -> Record {
        Record {
            payload: Bytes::copy_from_slice(payload),
            key: Some(Arc::from(topic)),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Auto,
            },
        }
    }

    #[tokio::test]
    async fn send_forwards_key_as_topic() {
        let fake = Arc::new(FakeSink::new());
        let adapter = ScaloSinkAdapter::new(Arc::clone(&fake), Rejects::default());

        let result = adapter.send("orders", Bytes::from_static(b"body")).await;
        assert!(matches!(result, SendResult::Ok));

        let got = fake.received.lock();
        assert_eq!(got.as_slice(), &[("orders".to_string(), b"body".to_vec())]);
    }

    #[tokio::test]
    async fn send_failure_reports_backpressured_for_spill() {
        let fake = Arc::new(FakeSink::new());
        fake.fail.store(true, Ordering::SeqCst);
        let adapter = ScaloSinkAdapter::new(fake, Rejects::default());

        // A transient failure must spill (Backpressured), never drop (Fatal).
        let result = adapter.send("orders", Bytes::from_static(b"body")).await;
        assert!(matches!(result, SendResult::Backpressured));
    }

    #[tokio::test]
    async fn a_refused_record_is_settled_not_spilled() {
        let fake = Arc::new(FakeSink::new());
        let adapter = ScaloSinkAdapter::new(Arc::clone(&fake), Rejects::default());

        // Backpressured would spill it and replay it forever.
        let result = adapter.send("orders", Bytes::from_static(b"bad")).await;
        assert!(matches!(result, SendResult::Ok), "got {result:?}");
        assert!(fake.received.lock().is_empty());
    }

    /// Through the real TieredSink: a record refused for good leaves the disk
    /// spool, so the records spilled behind it still reach the destination.
    #[tokio::test]
    async fn a_refused_record_does_not_block_the_disk_drain() {
        let dir = tempfile::tempdir().unwrap();
        let fake = Arc::new(FakeSink::new());
        fake.fail.store(true, Ordering::SeqCst);
        let mut config = scalo::tiered_sink::TieredSinkConfig::new(dir.path().join("spool"));
        config.circuit_failure_threshold = 1;
        config.circuit_reset_timeout_ms = 20;
        config.drain_interval_ms = 5;
        let tiered = scalo::tiered_sink::TieredSink::new(
            ScaloSinkAdapter::new(Arc::clone(&fake), Rejects::default()),
            config,
        )
        .await
        .unwrap();

        for payload in [&b"bad"[..], b"good-1", b"good-2"] {
            tiered.send(&record("orders", payload)).await.unwrap();
        }
        assert_eq!(tiered.spool_len().await, 3, "all three spilled");

        fake.fail.store(false, Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while !tiered.spool_is_empty().await && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert!(
            tiered.spool_is_empty().await,
            "the refused record is still at the head of the spool"
        );
        assert_eq!(
            fake.received.lock().as_slice(),
            &[
                ("orders".to_string(), b"good-1".to_vec()),
                ("orders".to_string(), b"good-2".to_vec()),
            ]
        );
        tiered.shutdown().await;
    }

    /// A refused record the DLQ does not confirm stays with the spool.
    #[tokio::test]
    async fn a_dlq_refusal_reports_backpressured() {
        let dir = tempfile::tempdir().unwrap();
        let fake = Arc::new(FakeSink::new());
        let rejects = Rejects::new(Some(test_dlq::refusing_dlq(dir.path())), None);
        let adapter = ScaloSinkAdapter::new(Arc::clone(&fake), rejects);

        let result = adapter.send("orders", Bytes::from_static(b"bad")).await;
        assert!(
            matches!(result, SendResult::Backpressured),
            "got {result:?}"
        );
    }

    /// Through the real TieredSink: a refused record the DLQ did not take is
    /// kept on disk, and leaves once the DLQ holds it.
    #[tokio::test]
    async fn a_dlq_refusal_keeps_the_record_spilled_until_the_dlq_takes_it() {
        let dir = tempfile::tempdir().unwrap();
        let dlq_dir = dir.path().join("dlq");
        std::fs::create_dir(&dlq_dir).unwrap();
        let rejects = Rejects::new(Some(test_dlq::refusing_dlq(&dlq_dir)), None);
        let mut config = scalo::tiered_sink::TieredSinkConfig::new(dir.path().join("spool"));
        config.circuit_failure_threshold = 1;
        config.circuit_reset_timeout_ms = 20;
        config.drain_interval_ms = 5;
        let tiered = scalo::tiered_sink::TieredSink::new(
            ScaloSinkAdapter::new(Arc::new(FakeSink::new()), rejects),
            config,
        )
        .await
        .unwrap();

        tiered.send(&record("orders", b"bad")).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(tiered.spool_len().await, 1, "the record left the spool");

        std::fs::remove_file(dlq_dir.join("receiver")).unwrap();
        std::fs::create_dir(dlq_dir.join("receiver")).unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while !tiered.spool_is_empty().await && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            tiered.spool_is_empty().await,
            "the DLQ took it, the spool kept it"
        );
        let written = std::fs::read_to_string(dlq_dir.join("receiver/dlq.ndjson")).unwrap();
        assert_eq!(written.lines().count(), 1, "{written}");
        tiered.shutdown().await;
    }

    #[tokio::test]
    async fn send_batch_default_routes_each_record_by_key() {
        let fake = Arc::new(FakeSink::new());
        let adapter = ScaloSinkAdapter::new(Arc::clone(&fake), Rejects::default());

        let batch = [record("topic-a", b"a"), record("topic-b", b"b")];
        let result = adapter.send_batch(&batch).await;
        assert!(matches!(result, SendResult::Ok));

        let got = fake.received.lock();
        assert_eq!(
            got.as_slice(),
            &[
                ("topic-a".to_string(), b"a".to_vec()),
                ("topic-b".to_string(), b"b".to_vec()),
            ]
        );
    }

    #[tokio::test]
    async fn is_healthy_reflects_inner() {
        let fake = Arc::new(FakeSink::new());
        let adapter = ScaloSinkAdapter::new(Arc::clone(&fake), Rejects::default());
        assert!(adapter.is_healthy());

        fake.healthy.store(false, Ordering::SeqCst);
        assert!(!adapter.is_healthy());
    }
}
