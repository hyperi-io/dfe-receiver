// Project:   dfe-receiver
// File:      src/sink/file/mod.rs
// Purpose:   Debug file sink for local message inspection
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Debug file sink.
//!
//! Writes all processed messages as NDJSON to a configurable file path.
//! Intended for development and debugging inside dfe-docker — not for
//! production use. Enabled via `file_sink.enabled = true` in config.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use tracing::debug;

use crate::error::{Error, Result};
use crate::sink::Sink;

/// Debug file sink: writes received messages as NDJSON.
///
/// Writes run on Tokio's blocking pool, so slow storage never holds a runtime
/// worker.
pub struct FileSink {
    writer: Arc<Mutex<BufWriter<File>>>,
}

impl FileSink {
    /// Create a new file sink writing to the given path.
    ///
    /// Creates parent directories as needed. Appends to the file if it exists.
    pub fn new(path: &str) -> Result<Self> {
        let path = PathBuf::from(path);

        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        tracing::info!(path = %path.display(), "File sink initialised");

        Ok(Self {
            writer: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }
}

impl FileSink {
    /// Run `op` on the writer from Tokio's blocking pool.
    async fn blocking(
        &self,
        what: &'static str,
        op: impl FnOnce(&mut BufWriter<File>) -> std::io::Result<()> + Send + 'static,
    ) -> Result<()> {
        let writer = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || op(&mut writer.lock()))
            .await
            .map_err(|e| Error::Transport(format!("file sink {what} did not finish: {e}")))?
            .map_err(|e| Error::Transport(format!("file sink {what} failed: {e}")))
    }
}

#[async_trait]
impl Sink for FileSink {
    /// Write the payload as a single NDJSON line.
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
        let bytes = payload.len();
        self.blocking("write", move |w| {
            w.write_all(&payload)?;
            w.write_all(b"\n")
        })
        .await?;
        debug!(topic = %topic, bytes, "Written to file sink");
        Ok(())
    }

    /// Flush the write buffer to disk.
    async fn flush(&self) -> Result<()> {
        self.blocking("flush", |w| w.flush()).await
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
    /// A write the file cannot take yet leaves the runtime free: another task
    /// on the same thread runs while it waits.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_blocked_write_leaves_the_runtime_free() {
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("slow.ndjson");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        let (go, wait) = std::sync::mpsc::channel::<()>();
        let reader_path = fifo.clone();
        let reader = std::thread::spawn(move || {
            let mut pipe = File::open(reader_path).unwrap();
            let signalled = wait.recv_timeout(std::time::Duration::from_secs(5)).is_ok();
            std::io::copy(&mut pipe, &mut std::io::sink()).unwrap();
            signalled
        });
        let sink = FileSink::new(fifo.to_str().unwrap()).unwrap();

        tokio::spawn(async move { go.send(()).unwrap() });
        // Past the pipe's buffer, so the write waits on the reader.
        sink.send("t", Bytes::from(vec![b'x'; 1 << 20]))
            .await
            .unwrap();
        drop(sink);

        assert!(
            reader.join().unwrap(),
            "no other task ran while the write waited"
        );
    }
}
