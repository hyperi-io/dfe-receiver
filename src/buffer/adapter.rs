// Project:   dfe-receiver
// File:      src/buffer/adapter.rs
// Purpose:   Adapter between receiver's Sink trait and scalo's tiered_sink::Sink
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Adapter between receiver's Sink trait and scalo's tiered_sink::Sink.
//!
//! Encodes topic + payload as: `[topic_len: u32 LE][topic bytes][payload bytes]`
//! This allows the spool to store topic-routed messages as raw bytes.

use std::sync::Arc;

use bytes::Bytes;
use scalo::tiered_sink::{Sink as RustlibSink, SinkError};

use crate::sink::Sink as ReceiverSink;

/// Adapts a receiver Sink to scalo's tiered_sink::Sink interface.
pub struct RustlibSinkAdapter<S: ReceiverSink> {
    inner: Arc<S>,
}

impl<S: ReceiverSink + 'static> RustlibSinkAdapter<S> {
    pub fn new(inner: Arc<S>) -> Self {
        Self { inner }
    }
}

/// Encode topic + payload into a single byte buffer.
///
/// Wire format: `[topic_len: u32 LE][topic bytes][payload bytes]`
pub fn encode_message(topic: &str, payload: &Bytes) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let topic_len = topic_bytes.len() as u32;
    let mut buf = Vec::with_capacity(4 + topic_bytes.len() + payload.len());
    buf.extend_from_slice(&topic_len.to_le_bytes());
    buf.extend_from_slice(topic_bytes);
    buf.extend_from_slice(payload);
    buf
}

/// Decode topic + payload from a byte buffer.
///
/// Returns `None` if the buffer is too short or contains invalid UTF-8 in the topic.
pub fn decode_message(data: &[u8]) -> Option<(&str, &[u8])> {
    if data.len() < 4 {
        return None;
    }
    let topic_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < 4 + topic_len {
        return None;
    }
    let topic = std::str::from_utf8(&data[4..4 + topic_len]).ok()?;
    let payload = &data[4 + topic_len..];
    Some((topic, payload))
}

/// Error type for the adapter, wrapping receiver sink errors.
#[derive(Debug, thiserror::Error)]
#[error("sink adapter error: {0}")]
pub struct AdapterError(String);

impl<S: ReceiverSink + 'static> RustlibSink for RustlibSinkAdapter<S> {
    type Error = AdapterError;

    async fn try_send(&self, data: &[u8]) -> std::result::Result<(), SinkError<Self::Error>> {
        let (topic, payload) = decode_message(data)
            .ok_or_else(|| SinkError::Fatal(AdapterError("invalid message encoding".into())))?;

        self.inner
            .send(topic, Bytes::copy_from_slice(payload))
            .await
            .map_err(|_| SinkError::Unavailable)
    }

    async fn health_check(&self) -> std::result::Result<(), Self::Error> {
        if self.inner.is_healthy() {
            Ok(())
        } else {
            Err(AdapterError("sink unhealthy".into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let topic = "my_topic";
        let payload = Bytes::from("hello world");
        let encoded = encode_message(topic, &payload);

        let (decoded_topic, decoded_payload) = decode_message(&encoded).unwrap();
        assert_eq!(decoded_topic, topic);
        assert_eq!(decoded_payload, payload.as_ref());
    }

    #[test]
    fn test_encode_decode_empty_topic() {
        let topic = "";
        let payload = Bytes::from("data");
        let encoded = encode_message(topic, &payload);

        let (decoded_topic, decoded_payload) = decode_message(&encoded).unwrap();
        assert_eq!(decoded_topic, "");
        assert_eq!(decoded_payload, b"data");
    }

    #[test]
    fn test_encode_decode_empty_payload() {
        let topic = "topic";
        let payload = Bytes::new();
        let encoded = encode_message(topic, &payload);

        let (decoded_topic, decoded_payload) = decode_message(&encoded).unwrap();
        assert_eq!(decoded_topic, "topic");
        assert!(decoded_payload.is_empty());
    }

    #[test]
    fn test_decode_too_short() {
        assert!(decode_message(&[]).is_none());
        assert!(decode_message(&[1, 0, 0]).is_none());
    }

    #[test]
    fn test_decode_truncated_topic() {
        // Claims topic is 10 bytes but only 2 bytes follow the header
        let data = [10, 0, 0, 0, b'a', b'b'];
        assert!(decode_message(&data).is_none());
    }

    #[test]
    fn test_encode_decode_binary_payload() {
        let topic = "binary_test";
        let payload = Bytes::from(vec![0u8, 1, 2, 255, 254, 253]);
        let encoded = encode_message(topic, &payload);

        let (decoded_topic, decoded_payload) = decode_message(&encoded).unwrap();
        assert_eq!(decoded_topic, topic);
        assert_eq!(decoded_payload, &[0u8, 1, 2, 255, 254, 253]);
    }

    #[test]
    fn test_encode_decode_large_topic() {
        let topic = "a".repeat(1000);
        let payload = Bytes::from("payload");
        let encoded = encode_message(&topic, &payload);

        let (decoded_topic, decoded_payload) = decode_message(&encoded).unwrap();
        assert_eq!(decoded_topic, topic);
        assert_eq!(decoded_payload, b"payload");
    }
}
