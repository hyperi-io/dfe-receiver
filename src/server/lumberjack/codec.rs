// Project:   dfe-receiver
// File:      src/server/lumberjack/codec.rs
// Purpose:   Lumberjack v2 wire protocol frame parser and ACK encoder
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Lumberjack v2 wire protocol codec.
//!
//! Implements frame parsing for the Lumberjack v2 protocol used by
//! Elastic Beats agents (Filebeat, Winlogbeat, etc.).
//!
//! Wire format: `[version: u8 = 0x32] [frame_type: u8] [payload...]`
//!
//! Reference: <https://github.com/elastic/go-lumber/tree/master/protocol/v2>

use std::io::Cursor;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Protocol constants
// ---------------------------------------------------------------------------

/// Lumberjack protocol version 2.
pub const PROTOCOL_VERSION: u8 = b'2';

/// Window size frame — declares max unacknowledged events.
pub const FRAME_WINDOW: u8 = b'W';

/// JSON data frame — carries a JSON payload with sequence number.
pub const FRAME_JSON_DATA: u8 = b'J';

/// Compressed frame — zlib-compressed envelope of complete inner frames.
pub const FRAME_COMPRESSED: u8 = b'C';

/// Acknowledgement frame — sent by server to client.
pub const FRAME_ACK: u8 = b'A';

// ---------------------------------------------------------------------------
// Safety limits
// ---------------------------------------------------------------------------

/// Maximum JSON payload size per frame (16 MiB).
const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024;

/// Maximum compressed frame size (32 MiB).
const MAX_COMPRESSED_SIZE: u32 = 32 * 1024 * 1024;

/// Maximum decompressed size (64 MiB).
const MAX_DECOMPRESSED_SIZE: u64 = 64 * 1024 * 1024;

/// Maximum window size (prevent absurd ack delays).
const MAX_WINDOW_SIZE: u32 = 100_000;

// ---------------------------------------------------------------------------
// Frame types
// ---------------------------------------------------------------------------

/// A parsed Lumberjack v2 frame.
#[derive(Debug)]
pub enum Frame {
    /// Window size declaration from client.
    Window { size: u32 },

    /// JSON data frame with sequence number.
    JsonData { sequence: u32, payload: Bytes },

    /// Compressed frame containing inner frames.
    Compressed { data: Vec<u8> },
}

// ---------------------------------------------------------------------------
// Async frame reading (from TCP stream)
// ---------------------------------------------------------------------------

/// Read a single frame from an async stream.
///
/// Returns `Ok(None)` on clean EOF (connection closed).
/// Returns `Err` on protocol violations or I/O errors.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Frame>> {
    // Read 2-byte header: version + frame type
    let mut header = [0u8; 2];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(Error::Io(e)),
    }

    let version = header[0];
    let frame_type = header[1];

    if version != PROTOCOL_VERSION {
        return Err(Error::Validation(format!(
            "lumberjack: unsupported protocol version 0x{version:02x}, expected 0x{PROTOCOL_VERSION:02x}"
        )));
    }

    match frame_type {
        FRAME_WINDOW => read_window_frame(reader).await.map(Some),
        FRAME_JSON_DATA => read_json_data_frame(reader).await.map(Some),
        FRAME_COMPRESSED => read_compressed_frame(reader).await.map(Some),
        _ => Err(Error::Validation(format!(
            "lumberjack: unknown frame type 0x{frame_type:02x}"
        ))),
    }
}

/// Read a window frame payload: 4-byte BE u32 window size.
async fn read_window_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Frame> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).await?;
    let size = u32::from_be_bytes(buf);

    if size > MAX_WINDOW_SIZE {
        return Err(Error::Validation(format!(
            "lumberjack: window size {size} exceeds maximum {MAX_WINDOW_SIZE}"
        )));
    }

    Ok(Frame::Window { size })
}

/// Read a JSON data frame: 4-byte seq + 4-byte len + JSON payload.
async fn read_json_data_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Frame> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).await?;

    let sequence = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let payload_len = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);

    if payload_len > MAX_PAYLOAD_SIZE {
        return Err(Error::Validation(format!(
            "lumberjack: JSON payload size {payload_len} exceeds maximum {MAX_PAYLOAD_SIZE}"
        )));
    }

    let mut payload = vec![0u8; payload_len as usize];
    reader.read_exact(&mut payload).await?;

    Ok(Frame::JsonData {
        sequence,
        payload: Bytes::from(payload),
    })
}

/// Read a compressed frame: 4-byte len + zlib-compressed data.
async fn read_compressed_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Frame> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).await?;
    let compressed_len = u32::from_be_bytes(buf);

    if compressed_len > MAX_COMPRESSED_SIZE {
        return Err(Error::Validation(format!(
            "lumberjack: compressed frame size {compressed_len} exceeds maximum {MAX_COMPRESSED_SIZE}"
        )));
    }

    let mut data = vec![0u8; compressed_len as usize];
    reader.read_exact(&mut data).await?;

    Ok(Frame::Compressed { data })
}

// ---------------------------------------------------------------------------
// Synchronous frame parsing (for decompressed buffers)
// ---------------------------------------------------------------------------

/// Parse all frames from a decompressed buffer.
///
/// Used after decompressing a `Compressed` frame. The buffer must contain
/// only complete frames (no partial frames allowed per protocol spec).
pub fn parse_frames_from_buf(buf: &[u8]) -> Result<Vec<Frame>> {
    let mut cursor = Cursor::new(buf);
    let mut frames = Vec::new();

    while (cursor.position() as usize) < buf.len() {
        let frame = parse_frame_sync(&mut cursor)?;
        frames.push(frame);
    }

    Ok(frames)
}

/// Parse a single frame synchronously from a cursor.
///
/// Uses `std::io::Read` explicitly to avoid ambiguity with `AsyncReadExt`.
fn parse_frame_sync(cursor: &mut Cursor<&[u8]>) -> Result<Frame> {
    use std::io::Read;

    let mut header = [0u8; 2];
    Read::read_exact(cursor, &mut header).map_err(|e| {
        Error::Validation(format!(
            "lumberjack: truncated frame in compressed block: {e}"
        ))
    })?;

    let version = header[0];
    let frame_type = header[1];

    if version != PROTOCOL_VERSION {
        return Err(Error::Validation(format!(
            "lumberjack: unsupported version 0x{version:02x} in compressed block"
        )));
    }

    match frame_type {
        FRAME_WINDOW => {
            let mut buf = [0u8; 4];
            Read::read_exact(cursor, &mut buf).map_err(|e| {
                Error::Validation(format!("lumberjack: truncated window frame: {e}"))
            })?;
            let size = u32::from_be_bytes(buf);
            if size > MAX_WINDOW_SIZE {
                return Err(Error::Validation(format!(
                    "lumberjack: window size {size} exceeds maximum {MAX_WINDOW_SIZE}"
                )));
            }
            Ok(Frame::Window { size })
        }
        FRAME_JSON_DATA => {
            let mut buf = [0u8; 8];
            Read::read_exact(cursor, &mut buf).map_err(|e| {
                Error::Validation(format!("lumberjack: truncated JSON data frame: {e}"))
            })?;
            let sequence = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
            let payload_len = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);

            if payload_len > MAX_PAYLOAD_SIZE {
                return Err(Error::Validation(format!(
                    "lumberjack: JSON payload size {payload_len} exceeds maximum {MAX_PAYLOAD_SIZE}"
                )));
            }

            let mut payload = vec![0u8; payload_len as usize];
            Read::read_exact(cursor, &mut payload).map_err(|e| {
                Error::Validation(format!("lumberjack: truncated JSON payload: {e}"))
            })?;

            Ok(Frame::JsonData {
                sequence,
                payload: Bytes::from(payload),
            })
        }
        _ => Err(Error::Validation(format!(
            "lumberjack: unexpected frame type 0x{frame_type:02x} in compressed block"
        ))),
    }
}

// ---------------------------------------------------------------------------
// ACK encoding
// ---------------------------------------------------------------------------

/// Encode an ACK frame: version(1) + type(1) + sequence(4) = 6 bytes.
pub fn encode_ack(sequence: u32) -> [u8; 6] {
    let mut buf = [0u8; 6];
    buf[0] = PROTOCOL_VERSION;
    buf[1] = FRAME_ACK;
    buf[2..6].copy_from_slice(&sequence.to_be_bytes());
    buf
}

// ---------------------------------------------------------------------------
// Decompression
// ---------------------------------------------------------------------------

/// Decompress a zlib-compressed frame and parse the contained inner frames.
pub fn decompress_and_parse(compressed: &[u8]) -> Result<Vec<Frame>> {
    use flate2::read::ZlibDecoder;
    use std::io::Read;

    let mut decoder = ZlibDecoder::new(compressed).take(MAX_DECOMPRESSED_SIZE);
    let mut decompressed = Vec::new();
    Read::read_to_end(&mut decoder, &mut decompressed)
        .map_err(|e| Error::Validation(format!("lumberjack: zlib decompression failed: {e}")))?;

    parse_frames_from_buf(&decompressed)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Build a window frame: version + 'W' + 4-byte BE size.
    fn build_window_frame(size: u32) -> Vec<u8> {
        let mut buf = vec![PROTOCOL_VERSION, FRAME_WINDOW];
        buf.extend_from_slice(&size.to_be_bytes());
        buf
    }

    /// Build a JSON data frame: version + 'J' + 4-byte seq + 4-byte len + payload.
    fn build_json_data_frame(sequence: u32, payload: &[u8]) -> Vec<u8> {
        let mut buf = vec![PROTOCOL_VERSION, FRAME_JSON_DATA];
        buf.extend_from_slice(&sequence.to_be_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    /// Build a compressed frame: version + 'C' + 4-byte len + zlib data.
    fn build_compressed_frame(inner_frames: &[u8]) -> Vec<u8> {
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(inner_frames).unwrap();
        let compressed = encoder.finish().unwrap();

        let mut buf = vec![PROTOCOL_VERSION, FRAME_COMPRESSED];
        buf.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
        buf.extend_from_slice(&compressed);
        buf
    }

    #[tokio::test]
    async fn test_parse_window_frame() {
        let data = build_window_frame(50);
        let mut cursor = Cursor::new(data.as_slice());
        let frame = read_frame(&mut cursor).await.unwrap().unwrap();
        match frame {
            Frame::Window { size } => assert_eq!(size, 50),
            _ => panic!("expected Window frame"),
        }
    }

    #[tokio::test]
    async fn test_parse_json_data_frame() {
        let json = br#"{"message":"hello"}"#;
        let data = build_json_data_frame(1, json);
        let mut cursor = Cursor::new(data.as_slice());
        let frame = read_frame(&mut cursor).await.unwrap().unwrap();
        match frame {
            Frame::JsonData { sequence, payload } => {
                assert_eq!(sequence, 1);
                assert_eq!(payload.as_ref(), json);
            }
            _ => panic!("expected JsonData frame"),
        }
    }

    #[tokio::test]
    async fn test_parse_compressed_frame() {
        let json = br#"{"key":"val"}"#;
        let inner = build_json_data_frame(1, json);
        let data = build_compressed_frame(&inner);

        let mut cursor = Cursor::new(data.as_slice());
        let frame = read_frame(&mut cursor).await.unwrap().unwrap();
        match frame {
            Frame::Compressed { data } => {
                let frames = decompress_and_parse(&data).unwrap();
                assert_eq!(frames.len(), 1);
                match &frames[0] {
                    Frame::JsonData { sequence, payload } => {
                        assert_eq!(*sequence, 1);
                        assert_eq!(payload.as_ref(), json);
                    }
                    _ => panic!("expected JsonData in compressed"),
                }
            }
            _ => panic!("expected Compressed frame"),
        }
    }

    #[test]
    fn test_encode_ack() {
        let ack = encode_ack(42);
        assert_eq!(ack[0], PROTOCOL_VERSION);
        assert_eq!(ack[1], FRAME_ACK);
        assert_eq!(u32::from_be_bytes([ack[2], ack[3], ack[4], ack[5]]), 42);
    }

    #[test]
    fn test_encode_ack_roundtrip() {
        for seq in [0, 1, 255, 65535, u32::MAX] {
            let ack = encode_ack(seq);
            assert_eq!(ack[0], PROTOCOL_VERSION);
            assert_eq!(ack[1], FRAME_ACK);
            let decoded = u32::from_be_bytes([ack[2], ack[3], ack[4], ack[5]]);
            assert_eq!(decoded, seq);
        }
    }

    #[tokio::test]
    async fn test_reject_invalid_version() {
        let data = vec![0x01, FRAME_WINDOW, 0, 0, 0, 10];
        let mut cursor = Cursor::new(data.as_slice());
        let result = read_frame(&mut cursor).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unsupported protocol version")
        );
    }

    #[tokio::test]
    async fn test_reject_unknown_frame_type() {
        let data = vec![PROTOCOL_VERSION, 0xFF, 0, 0, 0, 10];
        let mut cursor = Cursor::new(data.as_slice());
        let result = read_frame(&mut cursor).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown frame type")
        );
    }

    #[tokio::test]
    async fn test_eof_returns_none() {
        let data: Vec<u8> = vec![];
        let mut cursor = Cursor::new(data.as_slice());
        let result = read_frame(&mut cursor).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_reject_oversized_window() {
        let data = build_window_frame(MAX_WINDOW_SIZE + 1);
        let mut cursor = Cursor::new(data.as_slice());
        let result = read_frame(&mut cursor).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }

    #[tokio::test]
    async fn test_reject_oversized_payload() {
        // Build header with payload size exceeding limit (don't need actual payload bytes)
        let mut data = vec![PROTOCOL_VERSION, FRAME_JSON_DATA];
        data.extend_from_slice(&1u32.to_be_bytes()); // sequence
        data.extend_from_slice(&(MAX_PAYLOAD_SIZE + 1).to_be_bytes()); // oversized len
        let mut cursor = Cursor::new(data.as_slice());
        let result = read_frame(&mut cursor).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }

    #[tokio::test]
    async fn test_multiple_frames_in_sequence() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_window_frame(3));
        data.extend_from_slice(&build_json_data_frame(1, b"{}"));
        data.extend_from_slice(&build_json_data_frame(2, b"{}"));
        data.extend_from_slice(&build_json_data_frame(3, b"{}"));

        let mut cursor = Cursor::new(data.as_slice());

        // Window
        let frame = read_frame(&mut cursor).await.unwrap().unwrap();
        assert!(matches!(frame, Frame::Window { size: 3 }));

        // 3 JSON frames
        for seq in 1..=3 {
            let frame = read_frame(&mut cursor).await.unwrap().unwrap();
            match frame {
                Frame::JsonData { sequence, .. } => assert_eq!(sequence, seq),
                _ => panic!("expected JsonData"),
            }
        }

        // EOF
        assert!(read_frame(&mut cursor).await.unwrap().is_none());
    }

    #[test]
    fn test_parse_frames_from_buf() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&build_json_data_frame(1, b"{\"a\":1}"));
        buf.extend_from_slice(&build_json_data_frame(2, b"{\"b\":2}"));

        let frames = parse_frames_from_buf(&buf).unwrap();
        assert_eq!(frames.len(), 2);

        match &frames[0] {
            Frame::JsonData { sequence, .. } => assert_eq!(*sequence, 1),
            _ => panic!("expected JsonData"),
        }
        match &frames[1] {
            Frame::JsonData { sequence, .. } => assert_eq!(*sequence, 2),
            _ => panic!("expected JsonData"),
        }
    }

    #[test]
    fn test_compressed_multiple_inner_frames() {
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        let mut inner = Vec::new();
        inner.extend_from_slice(&build_json_data_frame(1, b"{\"x\":1}"));
        inner.extend_from_slice(&build_json_data_frame(2, b"{\"y\":2}"));
        inner.extend_from_slice(&build_json_data_frame(3, b"{\"z\":3}"));

        // Compress the inner frames
        let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&inner).unwrap();
        let compressed = encoder.finish().unwrap();

        let frames = decompress_and_parse(&compressed).unwrap();
        assert_eq!(frames.len(), 3);
    }
}
