// Project:   dfe-receiver
// File:      src/sink/grpc/mod.rs
// Purpose:   dfe-loader gRPC transport sink
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-loader gRPC transport sink.
//!
//! Sends messages directly to dfe-loader using the DFE native gRPC protocol
//! (scalo `GrpcTransport`, client-only mode). Used when
//! `loader.transport = "grpc"`, enabling receiver→loader communication
//! without Kafka (e.g., inside dfe-docker).

use async_trait::async_trait;
use bytes::Bytes;
use scalo::transport::{GrpcConfig, GrpcTransport, Record, SendResult};
use scalo::transport::{TransportBase, TransportSender};
use tracing::{debug, error, info};

use crate::error::{Error, Result};
use crate::sink::{FailureLatch, SINK_RETRY_AFTER, Sink};

/// The client config a sink dials with; `None` keeps scalo's own deadline.
fn client_config(endpoint: &str, send_timeout_ms: Option<u64>) -> GrpcConfig {
    let mut config = GrpcConfig::client(endpoint);
    if let Some(timeout) = send_timeout_ms {
        config.send_timeout_ms = timeout;
    }
    config
}

/// A record as the gRPC transport carries it: no wire key, the source travels
/// inside the payload.
fn unkeyed(payload: Bytes) -> Record {
    Record {
        payload,
        key: None,
        headers: Vec::new(),
        metadata: scalo::transport::RecordMeta {
            timestamp_ms: None,
            format: scalo::transport::PayloadFormat::Auto,
        },
    }
}

/// dfe-loader sink using DFE native gRPC transport.
pub struct GrpcSink {
    transport: GrpcTransport,
    /// The largest payload the transport will encode.
    max_message_size: usize,
    /// Tripped by a send the destination refused, cleared by one it took.
    failures: FailureLatch,
}

impl GrpcSink {
    /// Create a new gRPC loader sink connecting to the given endpoint.
    ///
    /// `send_timeout_ms` bounds a single RPC, so a loader that accepts the
    /// connection and then stops answering cannot hold a sender task forever;
    /// `None` keeps scalo's 30s default.
    ///
    /// Uses lazy connection — does not fail until the first RPC.
    pub async fn new(endpoint: &str, send_timeout_ms: Option<u64>) -> Result<Self> {
        let config = client_config(endpoint, send_timeout_ms);
        let transport = GrpcTransport::new(&config)
            .await
            .map_err(|e| Error::Transport(format!("gRPC sink init failed: {e}")))?;

        info!(
            endpoint = %endpoint,
            send_timeout_ms = config.send_timeout_ms,
            "gRPC loader sink initialised"
        );

        Ok(Self {
            transport,
            max_message_size: config.max_message_size,
            failures: FailureLatch::default(),
        })
    }

    /// Record what the destination made of a send.
    fn note(&self, result: &SendResult, records: usize) {
        match result {
            SendResult::Ok => {
                debug!(records, "Sent to gRPC destination");
                self.failures.clear();
            }
            SendResult::Fatal(e) => {
                error!(error = %e, records, "gRPC destination send failed");
                self.failures.trip();
            }
            SendResult::Backpressured | SendResult::FilteredDlq => {}
        }
    }
}

/// The receiver's verdict on one send result.
///
/// scalo's `Fatal` carries no gRPC status, so a refusal of this one record
/// cannot be told from a destination refusing every record: it stays
/// transient, queued and retried in order. `FilteredDlq` is scalo's verdict on
/// the record itself, which a retry would only repeat, so it is rejected.
pub(crate) fn push_outcome(result: SendResult) -> Result<()> {
    match result {
        SendResult::Ok => Ok(()),
        SendResult::Backpressured => Err(Error::Transport("gRPC loader backpressured".into())),
        SendResult::Fatal(e) => Err(Error::Transport(format!("gRPC loader send failed: {e}"))),
        SendResult::FilteredDlq => Err(Error::Rejected(
            "gRPC transport routed the record to a dead-letter queue".into(),
        )),
    }
}

#[async_trait]
impl Sink for GrpcSink {
    /// Send a message to dfe-loader via gRPC.
    ///
    /// The topic is passed as the `key` to the DFE Push RPC, where dfe-loader
    /// uses it for routing.
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
        // scalo's single-record send reports an over-limit payload as backpressure.
        if let Some(reason) = self.refuses(&payload) {
            return Err(Error::Rejected(reason));
        }
        // The clone is a refcount bump, not a payload copy.
        let result = self.transport.send(topic, payload).await;
        self.note(&result, 1);
        push_outcome(result)
    }

    /// Send the records in one `RouteBatch` call, split only where the
    /// message size limit requires it.
    ///
    /// A failure after the first split leaves the earlier parts delivered, and
    /// the caller's retry sends them again: duplicates, never loss.
    async fn send_batch(&self, _topic: &str, payloads: &[Bytes]) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }
        let records: Vec<Record> = payloads.iter().cloned().map(unkeyed).collect();
        let result = self.transport.send_batch(&records).await;
        self.note(&result, records.len());
        push_outcome(result)
    }

    /// A record over the transport's message size limit, measured as the
    /// batch call frames it.
    fn refuses(&self, payload: &Bytes) -> Option<String> {
        if payload.len() > self.max_message_size {
            return Some(format!(
                "{}-byte record exceeds the gRPC max_message_size of {}",
                payload.len(),
                self.max_message_size
            ));
        }
        self.transport
            .dead_letter_reason(&unkeyed(payload.clone()))
            .map(|reason| reason.to_string())
    }

    /// Flush pending messages.
    ///
    /// No-op for gRPC -- acknowledgement is implicit in the RPC response.
    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// Healthy while the destination takes records, and again
    /// [`SINK_RETRY_AFTER`] after it refused one, so traffic tries it.
    fn is_healthy(&self) -> bool {
        self.failures.admits(SINK_RETRY_AFTER) && self.transport.is_healthy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_grpc_sink_lazy_connect() {
        // Lazy connection — init succeeds even with no server at the endpoint
        let sink = GrpcSink::new("http://localhost:19999", None).await.unwrap();
        assert!(sink.is_healthy());
    }

    #[test]
    fn a_configured_deadline_bounds_the_push_rpc() {
        let config = client_config("http://loader:6000", Some(1_500));
        assert_eq!(config.send_timeout_ms, 1_500);
    }

    #[test]
    fn no_configured_deadline_keeps_scalos_own() {
        let default = GrpcConfig::client("http://loader:6000").send_timeout_ms;
        let config = client_config("http://loader:6000", None);
        assert_eq!(config.send_timeout_ms, default);
    }

    /// Only the record itself can prove a refusal permanent: a destination's
    /// refusal stays transient, and scalo's per-record `FilteredDlq` is rejected.
    #[test]
    fn a_destination_refusal_stays_transient() {
        use scalo::transport::TransportError;

        assert!(push_outcome(SendResult::Ok).is_ok());
        assert!(matches!(
            push_outcome(SendResult::Backpressured),
            Err(Error::Transport(_))
        ));
        assert!(matches!(
            push_outcome(SendResult::Fatal(TransportError::Send("invalid".into()))),
            Err(Error::Transport(_))
        ));
        assert!(matches!(
            push_outcome(SendResult::FilteredDlq),
            Err(Error::Rejected(_))
        ));
    }

    /// scalo reports an over-limit single send as backpressure, which would
    /// requeue a record that can never be sent.
    #[tokio::test]
    async fn a_record_over_the_message_size_limit_is_rejected() {
        let limit = GrpcConfig::client("http://127.0.0.1:1").max_message_size;
        let sink = GrpcSink::new("http://127.0.0.1:1", None).await.unwrap();

        let result = sink.send("t", Bytes::from(vec![b'x'; limit + 1])).await;

        assert!(matches!(result, Err(Error::Rejected(_))), "got {result:?}");
    }

    /// The batch path screens with the same limit, framing included, so a
    /// record a batch would leave out is refused before it is sent.
    #[tokio::test]
    async fn a_record_the_batch_cannot_frame_is_refused_up_front() {
        let limit = GrpcConfig::client("http://127.0.0.1:1").max_message_size;
        let sink = GrpcSink::new("http://127.0.0.1:1", None).await.unwrap();

        assert!(sink.refuses(&Bytes::from(vec![b'x'; limit])).is_some());
        assert!(sink.refuses(&Bytes::from_static(b"{}")).is_none());
    }

    #[tokio::test]
    async fn test_grpc_sink_send_fails_no_server() {
        let sink = GrpcSink::new("http://localhost:19998", None).await.unwrap();
        let payload = Bytes::from(r#"{"test": "data"}"#);

        // Send will fail (no server) — error is expected
        let result = sink.send("test-topic", payload).await;
        assert!(result.is_err());
    }
}
