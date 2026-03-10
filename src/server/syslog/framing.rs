// Project:   dfe-receiver
// File:      src/server/syslog/framing.rs
// Purpose:   RFC 6587 syslog TCP framing codec
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Syslog TCP framing codec (RFC 6587).
//!
//! Auto-detects between two framing methods per message:
//! - **Octet counting**: `<length> <message>` (first byte is ASCII digit 1-9)
//! - **Non-transparent framing**: newline-delimited (fallback)

use std::io;

use bytes::BytesMut;
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
    type Item = String;
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
}

impl SyslogFrameDecoder {
    /// Decode an octet-counted frame: `<length><SP><message>`.
    fn decode_octet_counted(
        &self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<String>, io::Error> {
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
        let frame = src.split_to(total);
        let msg_bytes = &frame[(space_pos + 1)..];

        match std::str::from_utf8(msg_bytes) {
            Ok(s) => Ok(Some(s.to_string())),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "syslog message is not valid UTF-8",
            )),
        }
    }

    /// Decode a newline-delimited (non-transparent) frame.
    fn decode_newline_delimited(
        &self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<String>, io::Error> {
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

        // Extract message up to newline, consume the newline
        let frame = src.split_to(newline_pos + 1);
        let msg_bytes = &frame[..newline_pos];

        // Skip empty lines
        if msg_bytes.is_empty() {
            return Ok(None);
        }

        match std::str::from_utf8(msg_bytes) {
            Ok(s) => Ok(Some(s.to_string())),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "syslog message is not valid UTF-8",
            )),
        }
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
        let mut buf = BytesMut::from(data);
        let mut results = Vec::new();
        loop {
            match decoder.decode(&mut buf) {
                Ok(Some(msg)) => results.push(msg),
                Ok(None) => break,
                Err(e) => panic!("decode error: {e}"),
            }
        }
        results
    }

    #[test]
    fn test_newline_delimited_single() {
        let mut decoder = SyslogFrameDecoder::default();
        let msgs = decode_all(&mut decoder, b"<14>hello world\n");
        assert_eq!(msgs, vec!["<14>hello world"]);
    }

    #[test]
    fn test_newline_delimited_multiple() {
        let mut decoder = SyslogFrameDecoder::default();
        let msgs = decode_all(&mut decoder, b"<14>msg one\n<14>msg two\n<14>msg three\n");
        assert_eq!(msgs, vec!["<14>msg one", "<14>msg two", "<14>msg three"]);
    }

    #[test]
    fn test_octet_counted_single() {
        let mut decoder = SyslogFrameDecoder::default();
        let msgs = decode_all(&mut decoder, b"11 <14>hello w");
        // "11 " is 3 bytes prefix, then 11 bytes of message = "<14>hello w"
        assert_eq!(msgs, vec!["<14>hello w"]);
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
    fn test_empty_lines_skipped() {
        let mut decoder = SyslogFrameDecoder::default();
        let msgs = decode_all(&mut decoder, b"\n\n<14>hello\n\n");
        assert_eq!(msgs, vec!["<14>hello"]);
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
