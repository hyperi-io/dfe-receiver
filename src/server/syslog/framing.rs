// Project:   dfe-receiver
// File:      src/server/syslog/framing.rs
// Purpose:   RFC 6587 syslog TCP framing codec
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Syslog TCP framing codec (RFC 6587).
//!
//! Auto-detects between two framing methods per message:
//! - **Octet counting**: `<length> <message>` (first byte is ASCII digit 1-9)
//! - **Non-transparent framing**: newline-delimited (fallback)
//!
//! A frame is its bytes: the encoding is the caller's to check, so a line that
//! is not UTF-8 costs that line and not the connection.

use std::io;

use bytes::{Bytes, BytesMut};
use tokio_util::codec::Decoder;

/// Default maximum syslog message size (64 KB).
const DEFAULT_MAX_LENGTH: usize = 64 * 1024;

/// Syslog TCP framing decoder implementing RFC 6587.
///
/// Auto-detects octet-counting vs newline-delimited framing per message
/// by peeking at the first byte of each frame.
pub struct SyslogFrameDecoder {
    max_length: usize,
}

impl SyslogFrameDecoder {
    /// Create a new decoder with the specified maximum message size.
    pub fn new(max_length: usize) -> Self {
        Self { max_length }
    }
}

impl Default for SyslogFrameDecoder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_LENGTH)
    }
}

impl Decoder for SyslogFrameDecoder {
    type Item = Bytes;
    type Error = io::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        // Skip leading newlines (empty lines between messages)
        while !src.is_empty() && src[0] == b'\n' {
            let _ = src.split_to(1);
        }

        if src.is_empty() {
            return Ok(None);
        }

        // Auto-detect framing: if first byte is ASCII digit 1-9, use octet counting
        if src[0] >= b'1' && src[0] <= b'9' {
            self.decode_octet_counted(src)
        } else {
            self.decode_newline_delimited(src)
        }
    }

    /// A sender that closes after a last line with no newline still sent that
    /// line, so it is the final frame. A partial octet-counted frame is not.
    fn decode_eof(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        if let Some(frame) = self.decode(src)? {
            return Ok(Some(frame));
        }
        let rest = src.split();
        let rest = rest.trim_ascii();
        if rest.is_empty() || octet_count(rest).is_some() {
            return Ok(None);
        }
        Ok(Some(Bytes::copy_from_slice(rest)))
    }
}

/// The octet count a frame opens with, when it opens `<digits><SP>`.
fn octet_count(frame: &[u8]) -> Option<usize> {
    if !matches!(frame.first(), Some(b'1'..=b'9')) {
        return None;
    }
    let space = frame.iter().position(|&b| b == b' ')?;
    std::str::from_utf8(&frame[..space]).ok()?.parse().ok()
}

impl SyslogFrameDecoder {
    /// Decode an octet-counted frame: `<length><SP><message>`.
    fn decode_octet_counted(
        &self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Bytes>, io::Error> {
        // Find the space separator after the length prefix
        let space_pos = match src.iter().position(|&b| b == b' ') {
            Some(pos) => pos,
            None => {
                // Haven't received the full length prefix yet
                if src.len() > 10 {
                    // Length prefix shouldn't be more than ~10 digits
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "octet count prefix too long",
                    ));
                }
                return Ok(None);
            }
        };

        // Parse the length prefix
        let length_str = match std::str::from_utf8(&src[..space_pos]) {
            Ok(s) => s,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "octet count prefix is not valid UTF-8",
                ));
            }
        };

        let msg_len: usize = match length_str.parse() {
            Ok(n) => n,
            Err(_) => {
                // Not a valid number — fall back to newline framing
                return self.decode_newline_delimited(src);
            }
        };

        if msg_len > self.max_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "syslog message too large: {msg_len} bytes (max {max})",
                    max = self.max_length
                ),
            ));
        }

        // Total bytes needed: length prefix + space + message
        let total = space_pos + 1 + msg_len;
        if src.len() < total {
            // Reserve space for the full message
            src.reserve(total - src.len());
            return Ok(None);
        }

        // Extract the message (skip length prefix + space)
        let frame = src.split_to(total).freeze();
        Ok(Some(frame.slice((space_pos + 1)..)))
    }

    /// Decode a newline-delimited (non-transparent) frame.
    fn decode_newline_delimited(
        &self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Bytes>, io::Error> {
        // Find the newline delimiter
        let newline_pos = match src.iter().position(|&b| b == b'\n') {
            Some(pos) => pos,
            None => {
                // Check if buffer exceeds max length
                if src.len() > self.max_length {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "syslog message too large: {} bytes without newline (max {})",
                            src.len(),
                            self.max_length
                        ),
                    ));
                }
                return Ok(None);
            }
        };

        if newline_pos > self.max_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "syslog message too large: {newline_pos} bytes (max {max})",
                    max = self.max_length
                ),
            ));
        }

        // Extract message up to newline, consume the newline. Leading newlines
        // were skipped, so the frame is never empty.
        let frame = src.split_to(newline_pos + 1).freeze();
        Ok(Some(frame.slice(..newline_pos)))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    use bytes::BufMut;

    fn decode_all(decoder: &mut SyslogFrameDecoder, data: &[u8]) -> Vec<String> {
        decode_frames(decoder, data, false)
            .into_iter()
            .map(|frame| String::from_utf8(frame.to_vec()).unwrap())
            .collect()
    }

    /// Every frame `data` holds, read to the end of the stream when `eof`.
    fn decode_frames(decoder: &mut SyslogFrameDecoder, data: &[u8], eof: bool) -> Vec<Bytes> {
        let mut buf = BytesMut::from(data);
        let mut results = Vec::new();
        loop {
            match decoder.decode(&mut buf) {
                Ok(Some(msg)) => results.push(msg),
                Ok(None) => break,
                Err(e) => panic!("decode error: {e}"),
            }
        }
        if eof && let Some(last) = decoder.decode_eof(&mut buf).unwrap() {
            results.push(last);
        }
        results
    }

    /// The bytes sent, whether the stream then ends, and the frames expected.
    type Case = (&'static [u8], bool, &'static [&'static [u8]]);

    /// Each framing decodes, and the decoder hands back bytes and never fails
    /// on their encoding, so a line that is not UTF-8 cannot end the
    /// connection and lose the rest. At the end of the stream a last line with
    /// no newline is still a frame, and a partial octet-counted frame is not.
    #[test]
    fn every_frame_survives_its_neighbours_encoding_and_a_missing_last_newline() {
        let cases: [Case; 9] = [
            (b"<14>hello world\n", false, &[b"<14>hello world"]),
            // "11 " is the prefix, then 11 bytes of message.
            (b"11 <14>hello w", false, &[b"<14>hello w"]),
            (b"\n\n<14>hello\n\n", false, &[b"<14>hello"]),
            (
                b"<14>caf\xe9\n<14>next\n",
                false,
                &[b"<14>caf\xe9", b"<14>next"],
            ),
            (
                b"9 <14>caf\xe9!9 <14>next1",
                false,
                &[b"<14>caf\xe9!", b"<14>next1"],
            ),
            (b"<14>one\n<14>last", true, &[b"<14>one", b"<14>last"]),
            (
                b"2026-10-31T10:00:00Z host app: no PRI",
                true,
                &[b"2026-10-31T10:00:00Z host app: no PRI"],
            ),
            (b"<14>one\n20 <14>cut", true, &[b"<14>one"]),
            (b"<14>one\n\n \t", true, &[b"<14>one"]),
        ];
        for (data, eof, expected) in cases {
            let frames = decode_frames(&mut SyslogFrameDecoder::default(), data, eof);
            let frames: Vec<&[u8]> = frames.iter().map(|f| &f[..]).collect();
            assert_eq!(frames, expected, "{:?}", String::from_utf8_lossy(data));
        }
    }

    #[test]
    fn test_newline_delimited_multiple() {
        let mut decoder = SyslogFrameDecoder::default();
        let msgs = decode_all(&mut decoder, b"<14>msg one\n<14>msg two\n<14>msg three\n");
        assert_eq!(msgs, vec!["<14>msg one", "<14>msg two", "<14>msg three"]);
    }

    #[test]
    fn test_octet_counted_multiple() {
        let mut decoder = SyslogFrameDecoder::default();
        let mut data = BytesMut::new();
        let msg1 = "<14>msg one";
        let msg2 = "<14>msg two";
        data.put(format!("{} {}", msg1.len(), msg1).as_bytes());
        data.put(format!("{} {}", msg2.len(), msg2).as_bytes());
        let frames = decode_all(&mut decoder, &data);
        assert_eq!(frames, vec!["<14>msg one", "<14>msg two"]);
    }

    #[test]
    fn test_partial_read_newline() {
        let mut decoder = SyslogFrameDecoder::default();
        let mut buf = BytesMut::from(&b"<14>partial"[..]);

        // No newline yet — should return None
        let result = decoder.decode(&mut buf).unwrap();
        assert!(result.is_none());

        // Add the rest
        buf.put(&b" message\n"[..]);
        let result = decoder.decode(&mut buf).unwrap();
        assert_eq!(result.unwrap(), "<14>partial message");
    }

    #[test]
    fn test_partial_read_octet_counted() {
        let mut decoder = SyslogFrameDecoder::default();
        let mut buf = BytesMut::from(&b"20 <14>part"[..]);

        // Not enough data yet
        let result = decoder.decode(&mut buf).unwrap();
        assert!(result.is_none());

        // Add the rest (need 20 bytes total message)
        buf.put(&b"ial message!"[..]);
        let result = decoder.decode(&mut buf).unwrap();
        assert_eq!(result.unwrap(), "<14>partial message!");
    }

    #[test]
    fn test_oversized_message_rejected() {
        let mut decoder = SyslogFrameDecoder::new(10);
        let mut buf =
            BytesMut::from(&b"<14>this is a very long message that exceeds the limit\n"[..]);
        let result = decoder.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_oversized_octet_counted_rejected() {
        let mut decoder = SyslogFrameDecoder::new(10);
        let mut buf = BytesMut::from(&b"50 <14>this message is too large"[..]);
        let result = decoder.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_input() {
        let mut decoder = SyslogFrameDecoder::default();
        let mut buf = BytesMut::new();
        let result = decoder.decode(&mut buf).unwrap();
        assert!(result.is_none());
    }
}
