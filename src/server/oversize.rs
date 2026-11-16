// Project:   dfe-receiver
// File:      src/server/oversize.rs
// Purpose:   Dead-letter a frame a raw TCP listener skipped for its size
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Frames a raw TCP listener skips for their size.
//!
//! A syslog or GELF message over its listener's limit costs that message and
//! never the connection, and the message is not lost: its first bytes go to the
//! DLQ with the reason and its full length, and the listener counts it in
//! `receiver_records_dropped_total{reason="oversize"}` as before. A framing
//! codec hands over each skipped frame as [`Frame::Oversize`] in its place in
//! the stream, so a flood of them backs up into the socket as the DLQ confirms
//! each write, and the codec never holds more than one frame's head.
//!
//! These protocols carry no acknowledgement, so a DLQ write that is not
//! confirmed is offered again until it is, holding the connection as a pipeline
//! that cannot take a record does ([`super::hold`]).

use bytes::{Bytes, BytesMut};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::hold::{FIRST_RETRY, LAST_RETRY};
use crate::buffer::rejects::Disposal;
use crate::metrics::{DropReason, Metrics};
use crate::pipeline::PipelineState;

/// The most bytes of a skipped frame a codec keeps for its dead letter: the
/// DLQ's own default payload cap, which it applies again to a smaller one.
pub const HEAD_BYTES: usize = scalo::dlq::DEFAULT_MAX_PAYLOAD_BYTES;

/// A frame skipped for its size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Oversize {
    /// The frame's first bytes, up to [`HEAD_BYTES`]. A frame cut short by its
    /// connection closing holds what arrived.
    pub head: Bytes,
    /// The frame's length: its octet count, or the bytes up to its delimiter.
    pub original_bytes: usize,
}

/// What a framing codec hands its listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// One message, without its framing.
    Message(Bytes),
    /// A message over the size limit, which the codec skipped.
    Oversize(Oversize),
}

/// The head and length of a frame the codec is still skipping.
#[derive(Debug, Default)]
pub struct Skipping {
    head: BytesMut,
    original_bytes: usize,
}

impl Skipping {
    /// A frame of a known length, whose bytes arrive as `keep` is given them.
    pub fn counted(original_bytes: usize) -> Self {
        Self {
            head: BytesMut::new(),
            original_bytes,
        }
    }

    /// A frame of unknown length, which `grow` measures.
    pub fn unmeasured() -> Self {
        Self::default()
    }

    /// Keep `chunk` as far as the head has room.
    pub fn keep(&mut self, chunk: &[u8]) {
        let room = HEAD_BYTES - self.head.len();
        self.head.extend_from_slice(&chunk[..chunk.len().min(room)]);
    }

    /// Count `chunk` toward the length and keep what the head has room for.
    pub fn grow(&mut self, chunk: &[u8]) {
        self.keep(chunk);
        self.original_bytes += chunk.len();
    }

    /// The skipped frame, leaving this ready for the next.
    pub fn finish(&mut self) -> Oversize {
        Oversize {
            head: std::mem::take(&mut self.head).freeze(),
            original_bytes: std::mem::take(&mut self.original_bytes),
        }
    }
}

/// One frame of `head` and `original_bytes`, whole in the codec's buffer.
pub fn whole(frame: &[u8], original_bytes: usize) -> Oversize {
    Oversize {
        head: Bytes::copy_from_slice(&frame[..frame.len().min(HEAD_BYTES)]),
        original_bytes,
    }
}

/// Count `frames` as dropped for their size and dead-letter each, waiting on
/// the DLQ until it has taken or refused every one for good.
///
/// Returns false when shutdown came first, and the caller must stop reading.
pub async fn dead_letter(
    frames: &mut Vec<Oversize>,
    pipeline: &PipelineState,
    metrics: &Metrics,
    transport: &str,
    listener: &str,
    limit: usize,
    shutdown: &CancellationToken,
) -> bool {
    metrics.add_records_dropped(transport, DropReason::Oversize, frames.len() as u64);
    let reason = format!("frame over {limit} bytes on {listener}");
    for frame in frames.drain(..) {
        let mut wait = FIRST_RETRY;
        loop {
            let disposal = pipeline
                .rejects()
                .dispose_head(&frame.head, frame.original_bytes, &reason)
                .await;
            if disposal != Disposal::Refused {
                break;
            }
            debug!(
                transport,
                listener, "DLQ did not take an oversize frame; holding the connection"
            );
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return false,
                () = tokio::time::sleep(wait) => {}
            }
            wait = (wait * 2).min(LAST_RETRY);
        }
    }
    true
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_skipped_frame_keeps_its_head_and_counts_its_length() {
        let mut frame = Skipping::unmeasured();
        let chunk = vec![b'x'; HEAD_BYTES];
        frame.grow(&chunk);
        frame.grow(&chunk);
        let out = frame.finish();
        assert_eq!(out.head.len(), HEAD_BYTES);
        assert_eq!(out.original_bytes, 2 * HEAD_BYTES);
        assert_eq!(frame.finish().original_bytes, 0, "ready for the next");
    }

    #[test]
    fn a_counted_frame_keeps_its_declared_length_whatever_arrived() {
        let mut frame = Skipping::counted(100_000);
        frame.keep(b"abc");
        let out = frame.finish();
        assert_eq!(&out.head[..], b"abc");
        assert_eq!(out.original_bytes, 100_000);
    }

    #[test]
    fn a_whole_frame_is_cut_to_the_head_limit() {
        let frame = vec![b'y'; HEAD_BYTES + 10];
        let out = whole(&frame, frame.len());
        assert_eq!(out.head.len(), HEAD_BYTES);
        assert_eq!(out.original_bytes, HEAD_BYTES + 10);
    }
}
