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
//! whole [`Record`](scalo::transport::Record)s to disk -- payload, routing key and headers all survive a
//! replay, with zero serialisation cost on the happy path. The receiver routes
//! by topic, so this adapter maps `Record.key` -> topic and forwards the payload
//! to the inner receiver sink.
//!
//! A transient inner-sink failure is reported as [`SendResult::Backpressured`]
//! (not `Fatal`) so the TieredSink spills the record rather than dropping it --
//! preserving at-least-once delivery across a downstream outage.

use std::sync::Arc;

use bytes::Bytes;
use scalo::transport::{SendResult, TransportBase, TransportResult, TransportSender};

use crate::sink::Sink as ReceiverSink;

/// Adapts a receiver [`Sink`](ReceiverSink) to scalo's [`TransportSender`], so it
/// can be wrapped directly in a `TieredSink` for disk spillover.
pub struct ScaloSinkAdapter<S: ReceiverSink> {
    inner: Arc<S>,
}

impl<S: ReceiverSink + 'static> ScaloSinkAdapter<S> {
    pub fn new(inner: Arc<S>) -> Self {
        Self { inner }
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
        match self.inner.send(key, payload).await {
            Ok(()) => SendResult::Ok,
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
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use scalo::transport::{PayloadFormat, Record, RecordMeta};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A receiver `Sink` test double: records (topic, payload) pairs and can be
    /// toggled to fail every send (to drive the Backpressured/spill path).
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
                return Err(crate::error::Error::Transport("forced failure".into()));
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
        let adapter = ScaloSinkAdapter::new(Arc::clone(&fake));

        let result = adapter.send("orders", Bytes::from_static(b"body")).await;
        assert!(matches!(result, SendResult::Ok));

        let got = fake.received.lock();
        assert_eq!(got.as_slice(), &[("orders".to_string(), b"body".to_vec())]);
    }

    #[tokio::test]
    async fn send_failure_reports_backpressured_for_spill() {
        let fake = Arc::new(FakeSink::new());
        fake.fail.store(true, Ordering::SeqCst);
        let adapter = ScaloSinkAdapter::new(fake);

        // A transient failure must spill (Backpressured), never drop (Fatal).
        let result = adapter.send("orders", Bytes::from_static(b"body")).await;
        assert!(matches!(result, SendResult::Backpressured));
    }

    #[tokio::test]
    async fn send_batch_default_routes_each_record_by_key() {
        let fake = Arc::new(FakeSink::new());
        let adapter = ScaloSinkAdapter::new(Arc::clone(&fake));

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
        let adapter = ScaloSinkAdapter::new(Arc::clone(&fake));
        assert!(adapter.is_healthy());

        fake.healthy.store(false, Ordering::SeqCst);
        assert!(!adapter.is_healthy());
    }
}
