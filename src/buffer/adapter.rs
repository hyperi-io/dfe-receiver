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
//! whole [`Record`]s to disk -- payload,
//! routing key and headers all survive a
//! replay, with zero serialisation cost on the happy path. The receiver routes
//! by topic, so this adapter maps `Record.key` -> topic and forwards the payload
//! to the inner receiver sink.
//!
//! The drain removes a spooled record once this adapter answers `Ok`, so a
//! drained record is answered only once the inner sink confirms it: the Kafka
//! sink's delivery report, or the answer of a sink that confirms on `send`. A
//! batch is queued whole before any confirmation is awaited. A record the
//! hot path offers inside `answered_at_enqueue` is answered once queued, as
//! its sender is answered.
//!
//! A transient inner-sink failure, or a confirmation that does not come, is
//! reported as [`SendResult::Backpressured`] (not `Fatal`) so the TieredSink
//! keeps the record spilled rather than dropping it -- preserving
//! at-least-once delivery across a downstream outage. A record the inner sink
//! refuses for good, at enqueue or on its delivery report, is settled here
//! through [`Rejects`] and reported
//! as sent once the DLQ holds it, so it is not replayed; a DLQ write that is
//! not confirmed reports `Backpressured`, so the record stays spilled and is
//! offered again.

use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use scalo::transport::{Record, SendResult, TransportBase, TransportResult, TransportSender};

use crate::buffer::{Disposal, Rejects};
use crate::error::Error;
use crate::sink::Sink as ReceiverSink;

tokio::task_local! {
    /// Set while a TieredSink hot path sends for a sender answered at enqueue.
    static AT_ENQUEUE: ();
}

/// Run `send`, a `TieredSink::send`, for a sender answered once its record is
/// queued: a record it takes on the hot path is answered at the inner sink's
/// enqueue, not its confirmation.
pub(crate) async fn answered_at_enqueue<F: Future>(send: F) -> F::Output {
    AT_ENQUEUE.scope((), send).await
}

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

    /// Send `records`, answering at enqueue inside [`answered_at_enqueue`]
    /// and on confirmation everywhere else.
    async fn deliver<'r>(
        &self,
        records: impl IntoIterator<Item = (&'r str, &'r Bytes)> + Send,
    ) -> SendResult {
        if AT_ENQUEUE.try_with(|()| ()).is_ok() {
            self.queue(records).await
        } else {
            self.confirm(records).await
        }
    }

    /// Hand each record to the inner sink, answering once it is queued.
    async fn queue<'r>(
        &self,
        records: impl IntoIterator<Item = (&'r str, &'r Bytes)> + Send,
    ) -> SendResult {
        for (key, payload) in records {
            match self.inner.send(key, payload.clone()).await {
                Ok(()) => {}
                Err(Error::Rejected(reason)) => {
                    if !self.settle_refused(key, payload, &reason).await {
                        return SendResult::Backpressured;
                    }
                }
                // Transient: surface as Backpressured so the TieredSink spills to
                // disk (at-least-once) instead of dropping the record.
                Err(_) => return SendResult::Backpressured,
            }
        }
        SendResult::Ok
    }

    /// Hand every record to the inner sink, then answer once it confirmed
    /// each one. Records refused for good are settled only once nothing in
    /// the batch is left to retry, so a retried batch dead-letters them once.
    async fn confirm<'r>(
        &self,
        records: impl IntoIterator<Item = (&'r str, &'r Bytes)> + Send,
    ) -> SendResult {
        let mut queued = Vec::new();
        let mut refused = Vec::new();
        for (key, payload) in records {
            match self.inner.send_confirmed(key, payload).await {
                Ok(confirmation) => queued.push((key, payload, confirmation)),
                Err(Error::Rejected(reason)) => refused.push((key, payload, reason)),
                // The records already queued still land: the spool offers the
                // batch again, so they are delivered twice, never lost.
                Err(_) => return SendResult::Backpressured,
            }
        }
        for (key, payload, confirmation) in queued {
            match confirmation.confirmed().await {
                Ok(()) => {}
                Err(Error::Rejected(reason)) => refused.push((key, payload, reason)),
                Err(_) => return SendResult::Backpressured,
            }
        }
        for (key, payload, reason) in refused {
            if !self.settle_refused(key, payload, &reason).await {
                return SendResult::Backpressured;
            }
        }
        SendResult::Ok
    }

    /// Take a record the inner sink refuses for good off the spool through
    /// [`Rejects`]. Spilled, it would be replayed forever ahead of everything
    /// behind it on disk.
    ///
    /// Returns whether it is settled: false when the DLQ refused it, so it
    /// stays spilled.
    async fn settle_refused(&self, key: &str, payload: &Bytes, reason: &str) -> bool {
        match self.rejects.dispose(key, payload, reason).await {
            Disposal::DeadLettered | Disposal::Dropped => true,
            Disposal::Refused => false,
        }
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
        self.deliver([(key, &payload)]).await
    }

    async fn send_batch(&self, records: &[Record]) -> SendResult {
        self.deliver(records.iter().map(routed)).await
    }
}

/// A record as the topic its `key` names and its payload.
fn routed(record: &Record) -> (&str, &Bytes) {
    (record.key.as_deref().unwrap_or(""), &record.payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::rejects::test_dlq;
    use crate::sink::Confirmation;
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use scalo::transport::{PayloadFormat, RecordMeta};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A receiver `Sink` test double: records (topic, payload) pairs, can be
    /// toggled to fail every send (to drive the Backpressured/spill path),
    /// refuses a `bad` payload for good, and can hold the confirmations it
    /// owes unanswered, as a broker that has not reported does.
    struct FakeSink {
        healthy: AtomicBool,
        fail: AtomicBool,
        hold: AtomicBool,
        held: Mutex<Vec<tokio::sync::oneshot::Sender<crate::error::Result<()>>>>,
        received: Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl FakeSink {
        fn new() -> Self {
            Self {
                healthy: AtomicBool::new(true),
                fail: AtomicBool::new(false),
                hold: AtomicBool::new(false),
                held: Mutex::new(Vec::new()),
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

        async fn send_confirmed(
            &self,
            topic: &str,
            payload: &Bytes,
        ) -> crate::error::Result<Confirmation> {
            self.send(topic, payload.clone()).await?;
            if !self.hold.load(Ordering::SeqCst) {
                return Ok(Confirmation::settled(Ok(())));
            }
            let (answer, confirmation) = Confirmation::pending();
            self.held.lock().push(answer);
            Ok(confirmation)
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
                format: PayloadFormat::Json,
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
        let queued = answered_at_enqueue(adapter.send("orders", Bytes::from_static(b"body"))).await;
        assert!(
            matches!(queued, SendResult::Backpressured),
            "got {queued:?}"
        );
    }

    #[tokio::test]
    async fn a_refused_record_is_settled_not_spilled() {
        let fake = Arc::new(FakeSink::new());
        let adapter = ScaloSinkAdapter::new(Arc::clone(&fake), Rejects::default());

        // Backpressured would spill it and replay it forever.
        let result = adapter.send("orders", Bytes::from_static(b"bad")).await;
        assert!(matches!(result, SendResult::Ok), "got {result:?}");
        let queued = answered_at_enqueue(adapter.send("orders", Bytes::from_static(b"bad"))).await;
        assert!(matches!(queued, SendResult::Ok), "got {queued:?}");
        assert!(fake.received.lock().is_empty());
    }

    /// Away from the hot path the answer waits for every record's
    /// confirmation, and a confirmation that fails keeps the batch spilled.
    #[tokio::test]
    async fn a_batch_is_answered_once_every_record_is_confirmed() {
        let fake = Arc::new(FakeSink::new());
        fake.hold.store(true, Ordering::SeqCst);
        let adapter = Arc::new(ScaloSinkAdapter::new(Arc::clone(&fake), Rejects::default()));
        let batch = [record("orders", b"one"), record("orders", b"two")];

        let sending = Arc::clone(&adapter);
        let mut answer = tokio::spawn(async move { sending.send_batch(&batch).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!answer.is_finished(), "answered before any confirmation");
        assert_eq!(fake.held.lock().len(), 2, "both queued before any wait");

        let first = fake.held.lock().remove(0);
        first.send(Ok(())).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !answer.is_finished(),
            "answered with one record unconfirmed"
        );

        let second = fake.held.lock().remove(0);
        second
            .send(Err(Error::Transport("no broker confirmed it".into())))
            .unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), &mut answer)
            .await
            .expect("answered once both reported")
            .expect("send task");
        assert!(
            matches!(result, SendResult::Backpressured),
            "got {result:?}"
        );
    }

    /// A record its delivery report refuses for good is settled through the
    /// reject path, once the rest of the batch is confirmed; any other failed
    /// report keeps the batch spilled.
    #[tokio::test]
    async fn a_delivery_refusal_is_settled_and_any_other_failure_is_retried() {
        let dir = tempfile::tempdir().unwrap();
        let cases = [
            (
                "refused, no DLQ: dropped",
                Rejects::default(),
                Err(Error::Rejected("kafka refused the record".into())),
                SendResult::Ok,
            ),
            (
                "refused, the DLQ refuses: kept",
                Rejects::new(Some(test_dlq::refusing_dlq(dir.path())), None),
                Err(Error::Rejected("kafka refused the record".into())),
                SendResult::Backpressured,
            ),
            (
                "not delivered: kept",
                Rejects::default(),
                Err(Error::Transport("kafka did not deliver the record".into())),
                SendResult::Backpressured,
            ),
            ("delivered", Rejects::default(), Ok(()), SendResult::Ok),
        ];
        for (case, rejects, report, want) in cases {
            let fake = Arc::new(FakeSink::new());
            fake.hold.store(true, Ordering::SeqCst);
            let adapter = Arc::new(ScaloSinkAdapter::new(Arc::clone(&fake), rejects));
            let batch = [record("orders", b"one"), record("orders", b"two")];

            let sending = Arc::clone(&adapter);
            let answer = tokio::spawn(async move { sending.send_batch(&batch).await });
            while fake.held.lock().len() < 2 {
                tokio::task::yield_now().await;
            }
            let (first, second) = {
                let mut held = fake.held.lock();
                (held.remove(0), held.remove(0))
            };
            first.send(report).unwrap();
            second.send(Ok(())).unwrap();

            let got = tokio::time::timeout(std::time::Duration::from_secs(5), answer)
                .await
                .expect("answered once both reported")
                .expect("send task");
            assert_eq!(
                std::mem::discriminant(&got),
                std::mem::discriminant(&want),
                "{case}: got {got:?}"
            );
        }
    }

    /// The hot path for a sender answered at enqueue answers once the record
    /// is queued, whatever the confirmation does.
    #[tokio::test]
    async fn the_hot_path_answered_at_enqueue_does_not_wait() {
        let fake = Arc::new(FakeSink::new());
        fake.hold.store(true, Ordering::SeqCst);
        let adapter = ScaloSinkAdapter::new(Arc::clone(&fake), Rejects::default());

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            answered_at_enqueue(adapter.send_batch(&[record("orders", b"one")])),
        )
        .await
        .expect("answered without a confirmation");

        assert!(matches!(result, SendResult::Ok), "got {result:?}");
        assert_eq!(fake.received.lock().len(), 1);
        assert!(
            fake.held.lock().is_empty(),
            "queued awaiting a confirmation"
        );
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
    async fn send_batch_routes_each_record_by_key() {
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
