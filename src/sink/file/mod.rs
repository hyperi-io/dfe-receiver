// Project:   dfe-receiver
// File:      src/sink/file/mod.rs
// Purpose:   Debug file sink for local message inspection
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Debug file sink.
//!
//! Writes all processed messages as NDJSON to a configurable file path.
//! Intended for development and debugging inside dfe-docker — not for
//! production use. Enabled via `file_sink.enabled = true` in config.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use tracing::debug;

use crate::error::{Error, Result};
use crate::sink::Sink;

/// Debug file sink — writes received messages as NDJSON.
pub struct FileSink {
    writer: Mutex<BufWriter<File>>,
}

impl FileSink {
    /// Create a new file sink writing to the given path.
    ///
    /// Creates parent directories as needed. Appends to the file if it exists.
    pub fn new(path: &str) -> Result<Self> {
        let path = PathBuf::from(path);

        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        tracing::info!(path = %path.display(), "File sink initialised");

        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
        })
    }
}

#[async_trait]
impl Sink for FileSink {
    /// Write the payload as a single NDJSON line.
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
        let mut w = self.writer.lock();
        w.write_all(&payload)
            .map_err(|e| Error::Transport(format!("file sink write failed: {e}")))?;
        w.write_all(b"\n")
            .map_err(|e| Error::Transport(format!("file sink write failed: {e}")))?;
        debug!(topic = %topic, bytes = payload.len(), "Written to file sink");
        Ok(())
    }

    /// Flush the write buffer to disk.
    async fn flush(&self) -> Result<()> {
        self.writer
            .lock()
            .flush()
            .map_err(|e| Error::Transport(format!("file sink flush failed: {e}")))
    }

    /// File sink is always considered healthy.
    fn is_healthy(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_sink_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.ndjson");
        let path_str = path.to_str().unwrap();

        let sink = FileSink::new(path_str).unwrap();
        assert!(sink.is_healthy());

        let payload = Bytes::from(r#"{"event":"test"}"#);
        sink.send("topic", payload).await.unwrap();
        sink.flush().await.unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "{\"event\":\"test\"}\n");
    }

    #[tokio::test]
    async fn test_file_sink_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("append.ndjson");
        let path_str = path.to_str().unwrap();

        let sink = FileSink::new(path_str).unwrap();
        sink.send("t", Bytes::from(r#"{"a":1}"#)).await.unwrap();
        sink.send("t", Bytes::from(r#"{"b":2}"#)).await.unwrap();
        sink.flush().await.unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "{\"a\":1}\n{\"b\":2}\n");
    }
}
