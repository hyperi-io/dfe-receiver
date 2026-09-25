// Project:   dfe-receiver
// File:      src/server/gelf/mod.rs
// Purpose:   GELF protocol handler (TCP, null-byte delimited)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! GELF (Graylog Extended Log Format) protocol handler.
//!
//! Accepts GELF messages over TCP using null-byte (`\0`) delimiters.
//! Standard port: 12201

pub mod convert;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::{GelfConfig, RawCapture};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::{Acks, PipelineState};
use crate::server::hold::hold_until_settled;
use crate::server::ip_filter::IpFilter;
use crate::server::traits::{BoundAddr, ProtocolHandler};
use convert::gelf_to_json;

/// TLS handshake timeout.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Null-byte framing codec
// ---------------------------------------------------------------------------

/// GELF TCP framing decoder -- splits on null bytes (`\0`).
pub struct GelfFrameDecoder {
    max_length: usize,
}

impl GelfFrameDecoder {
    pub fn new(max_length: usize) -> Self {
        Self { max_length }
    }
}

impl Decoder for GelfFrameDecoder {
    type Item = Vec<u8>;
    type Error = io::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        // Find null-byte delimiter
        let null_pos = match src.iter().position(|&b| b == b'\0') {
            Some(pos) => pos,
            None => {
                if src.len() > self.max_length {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "GELF message too large: {} bytes without null delimiter (max {})",
                            src.len(),
                            self.max_length
                        ),
                    ));
                }
                return Ok(None);
            }
        };

        if null_pos > self.max_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "GELF message too large: {null_pos} bytes (max {})",
                    self.max_length
                ),
            ));
        }

        // Extract message up to null byte, consume the null byte
        let frame = src.split_to(null_pos + 1);
        let msg_bytes = &frame[..null_pos];

        if msg_bytes.is_empty() {
            return Ok(None);
        }

        Ok(Some(msg_bytes.to_vec()))
    }
}

// ---------------------------------------------------------------------------
// TCP per-connection handler
// ---------------------------------------------------------------------------

/// Handle a single GELF TCP connection.
///
/// A message the pipeline cannot take is held, and the socket is not read,
/// until it can: null-delimited GELF has no acknowledgement to ask for a retry.
async fn handle_tcp_connection<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    peer_addr: SocketAddr,
    max_message_size: usize,
    raw_capture: RawCapture,
) {
    use tokio_stream::StreamExt;

    let mut framed = FramedRead::new(stream, GelfFrameDecoder::new(max_message_size));

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                debug!(peer = %peer_addr, "GELF TCP connection closing (shutdown)");
                break;
            }
            result = StreamExt::next(&mut framed) => {
                match result {
                    Some(Ok(raw)) => {
                        metrics.inc_requests_total("gelf");
                        metrics.add_bytes_received("gelf", raw.len() as u64);

                        match gelf_to_json(&raw, raw_capture) {
                            Ok(payload) => {
                                let held = [payload];
                                if !hold_until_settled(&pipeline, &held, &metrics, "gelf", &shutdown, &Acks::at_enqueue()).await {
                                    debug!(peer = %peer_addr, "GELF TCP connection closing (shutdown during a hold)");
                                    break;
                                }
                            }
                            Err(e) => {
                                debug!(peer = %peer_addr, error = %e, "GELF parse error");
                                metrics.inc_parse_failure("gelf");
                                metrics.inc_requests_error("gelf");
                            }
                        }
                    }
                    Some(Err(e)) => {
                        warn!(peer = %peer_addr, error = %e, "GELF TCP framing error");
                        break;
                    }
                    None => {
                        debug!(peer = %peer_addr, "GELF TCP client disconnected");
                        break;
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TCP listener
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_tcp(
    bind_addr: SocketAddr,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    max_message_size: usize,
    raw_capture: RawCapture,
    ip_filter: IpFilter,
    bound: BoundAddr,
) -> Result<()> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| Error::Server(format!("failed to bind GELF TCP listener: {e}")))?;
    let _serving = bound.publish(&listener.local_addr());

    let tls_enabled = tls_acceptor.is_some();
    info!(addr = %bind_addr, tls = tls_enabled, "GELF TCP listener started");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("GELF TCP listener stopping");
                break;
            }
            result = listener.accept() => {
                let (stream, peer_addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!(error = %e, "Failed to accept GELF TCP connection");
                        continue;
                    }
                };

                // Reject before the TLS handshake and before any framing.
                if !ip_filter.admits(peer_addr) {
                    drop(stream);
                    continue;
                }

                let pipeline = pipeline.clone();
                let metrics = metrics.clone();
                let conn_shutdown = shutdown.clone();

                if let Some(ref acceptor) = tls_acceptor {
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        let tls_result = timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await;
                        let tls_stream = match tls_result {
                            Ok(Ok(s)) => s,
                            Ok(Err(e)) => {
                                metrics.inc_tls_handshake_failure();
                                debug!(peer = %peer_addr, error = %e, "GELF TLS handshake failed");
                                return;
                            }
                            Err(_) => {
                                metrics.inc_tls_handshake_failure();
                                warn!(peer = %peer_addr, "GELF TLS handshake timeout");
                                return;
                            }
                        };

                        handle_tcp_connection(
                            tls_stream, pipeline, metrics, conn_shutdown, peer_addr,
                            max_message_size, raw_capture,
                        ).await;
                    });
                } else {
                    tokio::spawn(async move {
                        handle_tcp_connection(
                            stream, pipeline, metrics, conn_shutdown, peer_addr,
                            max_message_size, raw_capture,
                        ).await;
                    });
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Protocol handler
// ---------------------------------------------------------------------------

/// GELF protocol handler.
pub struct GelfHandler {
    config: GelfConfig,
    /// Raw capture already resolved against the common `raw_capture` block.
    raw_capture: RawCapture,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    bound: BoundAddr,
}

impl GelfHandler {
    pub fn new(
        config: GelfConfig,
        raw_capture: RawCapture,
        pipeline: Arc<PipelineState>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            config,
            raw_capture,
            pipeline,
            metrics,
            bound: BoundAddr::default(),
        }
    }

    /// The address the listener bound, once [`ProtocolHandler::start`] binds it.
    #[must_use]
    pub fn bound_addr(&self) -> BoundAddr {
        self.bound.clone()
    }
}

#[async_trait::async_trait]
impl ProtocolHandler for GelfHandler {
    fn name(&self) -> &'static str {
        "gelf"
    }

    fn bind_address(&self) -> &str {
        &self.config.bind_address
    }

    fn listeners(&self) -> Vec<BoundAddr> {
        vec![self.bound.clone()]
    }

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        let addr: SocketAddr = self
            .config
            .bind_address
            .parse()
            .map_err(|e| Error::Config(format!("invalid GELF bind address: {e}")))?;

        let max_msg = self.config.max_message_size;

        // Build TLS acceptor if enabled
        let tls_acceptor = if self.config.tls.enabled {
            super::tls::build_tls_acceptor_async(&self.config.tls).await?
        } else {
            None
        };

        // GELF carries no credential, so the IP filter and the TLS handshake
        // are the whole admission surface on this port.
        let ip_filter = IpFilter::from_config(&self.pipeline.config().server.ip_filter);

        run_tcp(
            addr,
            self.pipeline.clone(),
            self.metrics.clone(),
            shutdown,
            tls_acceptor,
            max_msg,
            self.raw_capture,
            ip_filter,
            self.bound.clone(),
        )
        .await?;

        info!("GELF server stopped");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GelfConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.bind_address, "0.0.0.0:12201");
        assert_eq!(config.max_message_size, 1024 * 1024);
        assert!(!config.tls.enabled);
    }

    #[test]
    fn test_frame_decoder_single() {
        let mut decoder = GelfFrameDecoder::new(1024);
        let mut buf = BytesMut::from(&br#"{"version":"1.1","host":"h","short_message":"m"}"#[..]);
        buf.extend_from_slice(b"\0");
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["host"], "h");
    }

    #[test]
    fn test_frame_decoder_multiple() {
        let mut decoder = GelfFrameDecoder::new(1024);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(br#"{"version":"1.1","host":"a","short_message":"1"}"#);
        buf.extend_from_slice(b"\0");
        buf.extend_from_slice(br#"{"version":"1.1","host":"b","short_message":"2"}"#);
        buf.extend_from_slice(b"\0");

        let msg1 = decoder.decode(&mut buf).unwrap().unwrap();
        let msg2 = decoder.decode(&mut buf).unwrap().unwrap();
        let j1: serde_json::Value = serde_json::from_slice(&msg1).unwrap();
        let j2: serde_json::Value = serde_json::from_slice(&msg2).unwrap();
        assert_eq!(j1["host"], "a");
        assert_eq!(j2["host"], "b");
    }

    #[test]
    fn test_frame_decoder_partial() {
        let mut decoder = GelfFrameDecoder::new(1024);
        let mut buf = BytesMut::from(&b"partial data"[..]);
        assert!(decoder.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_frame_decoder_oversized() {
        let mut decoder = GelfFrameDecoder::new(10);
        let mut buf = BytesMut::from(&b"this is too long for the limit\0"[..]);
        assert!(decoder.decode(&mut buf).is_err());
    }

    #[test]
    fn test_frame_decoder_empty_message() {
        let mut decoder = GelfFrameDecoder::new(1024);
        let mut buf = BytesMut::from(&b"\0"[..]);
        // Empty message between null bytes should return None
        assert!(decoder.decode(&mut buf).unwrap().is_none());
    }
}
