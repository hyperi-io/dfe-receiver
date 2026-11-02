// Project:   dfe-receiver
// File:      src/server/syslog/framing.rs
// Purpose:   RFC 6587 syslog TCP framing codec
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Syslog TCP framing codec (RFC 6587).
//!
//! Each frame is either octet-counted (`<count> <message>`) or one LF-terminated
//! line. A frame is octet-counted only when it opens with a count, ONE space and
//! the `<` of a PRI, so a line that happens to open with digits, like
//! `2026 Oct 11 ...`, is read as a line and does not swallow the bytes after it.
//!
//! A message over the size limit costs that message and never the connection:
//! an octet-counted one is skipped by its count, a line up to its LF. Each is
//! counted for [`SyslogFrameDecoder::take_discarded`]. A count above the frame
//! limit is not trusted as a count, and that frame is read as a line.
//!
//! A frame is its bytes: the encoding is the caller's to check, so a line that
//! is not UTF-8 costs that line and not the connection.

use std::io;

use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

/// Default maximum syslog message size (64 KiB).
pub const DEFAULT_MAX_LENGTH: usize = 64 * 1024;

/// Default largest octet count trusted as one (256 KiB).
pub const DEFAULT_MAX_FRAME: usize = 256 * 1024;

/// Where the decoder is between frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// At the start of a frame.
    Start,
    /// Inside an oversize octet-counted message, with this many bytes left.
    Skip(usize),
    /// Inside an oversize line, up to its LF.
    Discard,
}

/// How a frame opens.
enum Opening {
    /// `<count> <`: an octet-counted frame whose message starts after `header` bytes.
    Counted { header: usize, count: usize },
    /// Digits so far with nothing yet to tell a count from a line.
    Undecided,
    /// Anything else, framed by its LF.
    Line,
}

/// Syslog TCP framing decoder implementing RFC 6587.
pub struct SyslogFrameDecoder {
    max_length: usize,
    max_frame: usize,
    state: State,
    discarded: u64,
}

impl SyslogFrameDecoder {
    /// A decoder taking messages up to `max_length` bytes, with counts trusted
    /// up to [`DEFAULT_MAX_FRAME`].
    pub fn new(max_length: usize) -> Self {
        Self::with_max_frame(max_length, DEFAULT_MAX_FRAME)
    }

    /// A decoder taking messages up to `max_length` bytes, with counts trusted
    /// up to `max_frame`, raised to `max_length` when below it.
    pub fn with_max_frame(max_length: usize, max_frame: usize) -> Self {
        Self {
            max_length,
            max_frame: max_frame.max(max_length),
            state: State::Start,
            discarded: 0,
        }
    }

    /// Messages discarded for their size since the last call.
    pub fn take_discarded(&mut self) -> u64 {
        std::mem::take(&mut self.discarded)
    }

    /// How the frame at the start of `src` opens.
    fn opening(&self, src: &[u8]) -> Opening {
        if !matches!(src.first(), Some(b'1'..=b'9')) {
            return Opening::Line;
        }
        let mut count: usize = 0;
        for (i, &b) in src.iter().enumerate() {
            if b.is_ascii_digit() {
                count = match count
                    .checked_mul(10)
                    .and_then(|c| c.checked_add(usize::from(b - b'0')))
                {
                    Some(c) if c <= self.max_frame => c,
                    _ => return Opening::Line,
                };
                continue;
            }
            return match (b, src.get(i + 1)) {
                (b' ', Some(b'<')) => Opening::Counted {
                    header: i + 1,
                    count,
                },
                (b' ', None) => Opening::Undecided,
                _ => Opening::Line,
            };
        }
        Opening::Undecided
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

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match self.state {
                State::Start => {}
                State::Skip(left) => {
                    let take = left.min(src.len());
                    src.advance(take);
                    if take < left {
                        self.state = State::Skip(left - take);
                        return Ok(None);
                    }
                    self.state = State::Start;
                }
                State::Discard => {
                    let Some(lf) = memchr::memchr(b'\n', src) else {
                        src.clear();
                        return Ok(None);
                    };
                    src.advance(lf + 1);
                    self.state = State::Start;
                }
            }

            let blank = src.iter().take_while(|&&b| b == b'\n').count();
            src.advance(blank);
            if src.is_empty() {
                return Ok(None);
            }

            match self.opening(src) {
                Opening::Counted { header, count } if count > self.max_length => {
                    self.discarded += 1;
                    src.advance(header);
                    self.state = State::Skip(count);
                    continue;
                }
                Opening::Counted { header, count } => {
                    let total = header + count;
                    if src.len() < total {
                        src.reserve(total - src.len());
                        return Ok(None);
                    }
                    let frame = src.split_to(total).freeze();
                    return Ok(Some(frame.slice(header..)));
                }
                Opening::Undecided => return Ok(None),
                Opening::Line => {}
            }

            match memchr::memchr(b'\n', src) {
                Some(lf) if lf <= self.max_length => {
                    let frame = src.split_to(lf + 1).freeze();
                    return Ok(Some(frame.slice(..lf)));
                }
                Some(lf) => {
                    self.discarded += 1;
                    src.advance(lf + 1);
                }
                None if src.len() > self.max_length => {
                    self.discarded += 1;
                    src.clear();
                    self.state = State::Discard;
                    return Ok(None);
                }
                None => return Ok(None),
            }
        }
    }

    /// A sender that closes after a last line with no newline still sent that
    /// line, so it is the final frame. A partial octet-counted frame is not.
    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(frame) = self.decode(src)? {
            return Ok(Some(frame));
        }
        let rest = src.split();
        let rest = rest.trim_ascii();
        if rest.is_empty()
            || self.state != State::Start
            || matches!(self.opening(rest), Opening::Counted { .. })
        {
            return Ok(None);
        }
        Ok(Some(Bytes::copy_from_slice(rest)))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Every frame `data` holds, fed `chunk` bytes at a time and read to the
    /// end of the stream when `eof`, with the count of messages discarded.
    fn decode_chunked(
        decoder: &mut SyslogFrameDecoder,
        data: &[u8],
        chunk: usize,
        eof: bool,
    ) -> (Vec<Bytes>, u64) {
        let mut buf = BytesMut::new();
        let mut frames = Vec::new();
        for piece in data.chunks(chunk.max(1)) {
            buf.extend_from_slice(piece);
            while let Some(frame) = decoder.decode(&mut buf).unwrap() {
                frames.push(frame);
            }
        }
        if eof && let Some(last) = decoder.decode_eof(&mut buf).unwrap() {
            frames.push(last);
        }
        (frames, decoder.take_discarded())
    }

    /// The bytes sent, whether the stream then ends, the frames expected and
    /// how many messages are discarded for their size.
    struct Case {
        name: &'static str,
        data: Vec<u8>,
        eof: bool,
        frames: &'static [&'static [u8]],
        discarded: u64,
    }

    fn case(
        name: &'static str,
        data: impl Into<Vec<u8>>,
        eof: bool,
        frames: &'static [&'static [u8]],
        discarded: u64,
    ) -> Case {
        Case {
            name,
            data: data.into(),
            eof,
            frames,
            discarded,
        }
    }

    /// The default limits, except where a case sets its own.
    fn cases() -> Vec<Case> {
        let big_octet = format!("70000 <14>{}", "a".repeat(70_000 - 4));
        let big_line = format!("<14>{}\n", "b".repeat(70_000));
        vec![
            case("lf", *b"<14>hello world\n", false, &[b"<14>hello world"], 0),
            case("octet", *b"11 <14>hello w", false, &[b"<14>hello w"], 0),
            case(
                "octet_pair",
                *b"11 <14>msg one11 <14>msg two",
                false,
                &[b"<14>msg one", b"<14>msg two"],
                0,
            ),
            case(
                "blank_lines",
                *b"\n\n<14>hello\n\n",
                false,
                &[b"<14>hello"],
                0,
            ),
            // A frame is its bytes: an invalid UTF-8 neighbour costs nothing.
            case(
                "lf_not_utf8",
                *b"<14>caf\xe9\n<14>next\n",
                false,
                &[b"<14>caf\xe9", b"<14>next"],
                0,
            ),
            case(
                "octet_not_utf8",
                *b"9 <14>caf\xe9!9 <14>next1",
                false,
                &[b"<14>caf\xe9!", b"<14>next1"],
                0,
            ),
            // Digits that open a line are not a count.
            case(
                "line_opens_with_a_year",
                *b"2026 Oct 11 22:14:15 host app: no PRI\n<14>next\n",
                false,
                &[b"2026 Oct 11 22:14:15 host app: no PRI", b"<14>next"],
                0,
            ),
            case(
                "line_opens_with_a_sequence_number",
                *b"123456: Mar  1 18:46:11.407: %SYS-5-CONFIG_I\n<14>next\n",
                false,
                &[b"123456: Mar  1 18:46:11.407: %SYS-5-CONFIG_I", b"<14>next"],
                0,
            ),
            case(
                "digits_space_no_pri",
                *b"11 hello world\n<14>x\n",
                false,
                &[b"11 hello world", b"<14>x"],
                0,
            ),
            case(
                "digits_two_spaces",
                *b"11  <14>hello\n",
                false,
                &[b"11  <14>hello"],
                0,
            ),
            case("zero_count", *b"0 <14>x\n", false, &[b"0 <14>x"], 0),
            // Oversize messages cost themselves and nothing after them.
            case(
                "oversize_octet_is_skipped",
                format!("{big_octet}11 <14>hello w"),
                false,
                &[b"<14>hello w"],
                1,
            ),
            case(
                "oversize_line_is_skipped",
                format!("{big_line}<14>ok\n"),
                false,
                &[b"<14>ok"],
                1,
            ),
            case(
                "two_oversize_lines",
                format!("{big_line}{big_line}<14>ok\n"),
                false,
                &[b"<14>ok"],
                2,
            ),
            // A count past the frame limit is read as a line.
            case(
                "untrusted_count_is_a_line",
                *b"9999999 <14>hello\n<14>next\n",
                false,
                &[b"9999999 <14>hello", b"<14>next"],
                0,
            ),
            case(
                "overflowing_count_is_a_line",
                *b"99999999999999999999999 <14>x\n",
                false,
                &[b"99999999999999999999999 <14>x"],
                0,
            ),
            // At the end of the stream a last line with no LF is a frame, and a
            // partial octet-counted frame is not.
            case(
                "last_line_no_lf",
                *b"<14>one\n<14>last",
                true,
                &[b"<14>one", b"<14>last"],
                0,
            ),
            case(
                "last_line_no_pri",
                *b"2026-10-31T10:00:00Z host app: no PRI",
                true,
                &[b"2026-10-31T10:00:00Z host app: no PRI"],
                0,
            ),
            case(
                "partial_octet_at_eof",
                *b"<14>one\n20 <14>cut",
                true,
                &[b"<14>one"],
                0,
            ),
            case(
                "trailing_whitespace_at_eof",
                *b"<14>one\n\n \t",
                true,
                &[b"<14>one"],
                0,
            ),
            case(
                "lone_number_at_eof",
                *b"<14>one\n2026",
                true,
                &[b"<14>one", b"2026"],
                0,
            ),
            case(
                "oversize_line_cut_by_eof",
                format!("<14>{}", "c".repeat(70_000)),
                true,
                &[],
                1,
            ),
        ]
    }

    /// Each case decodes the same whether its bytes arrive at once, in small
    /// pieces or one at a time.
    #[test]
    fn frames_survive_oversize_neighbours_digit_led_lines_and_split_reads() {
        let mut failures = Vec::new();
        for c in cases() {
            for chunk in [c.data.len(), 7, 1] {
                let mut decoder = SyslogFrameDecoder::default();
                let (frames, discarded) = decode_chunked(&mut decoder, &c.data, chunk, c.eof);
                let frames: Vec<&[u8]> = frames.iter().map(|f| &f[..]).collect();
                if frames != c.frames || discarded != c.discarded {
                    failures.push(format!(
                        "{} (chunk {chunk}): got {} frames {:?}, discarded {discarded}",
                        c.name,
                        frames.len(),
                        frames
                            .iter()
                            .map(|f| String::from_utf8_lossy(&f[..f.len().min(40)]))
                            .collect::<Vec<_>>(),
                    ));
                }
            }
        }
        assert!(failures.is_empty(), "\n{}", failures.join("\n"));
    }

    /// The frame limit is never below the message limit, so every count a
    /// message may carry is trusted.
    #[test]
    fn frame_limit_is_raised_to_the_message_limit() {
        let mut decoder = SyslogFrameDecoder::with_max_frame(100, 10);
        let msg = format!("<14>{}", "x".repeat(96));
        let data = format!("100 {msg}");
        let (frames, discarded) = decode_chunked(&mut decoder, data.as_bytes(), 3, false);
        assert_eq!(frames, vec![Bytes::from(msg)]);
        assert_eq!(discarded, 0);
    }

    #[test]
    fn test_empty_input() {
        let mut decoder = SyslogFrameDecoder::default();
        let mut buf = BytesMut::new();
        assert!(decoder.decode(&mut buf).unwrap().is_none());
    }
}
