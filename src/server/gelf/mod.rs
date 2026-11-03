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

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::GelfConfig;
use crate::error::{Error, Result};
use crate::metrics::{DropReason, Metrics};
use crate::pipeline::{Acks, PipelineState};
use crate::server::frame_clock::Clocked;
use crate::server::hold::hold_until_settled;
use crate::server::http::{Accept, ConnectionLimits};
use crate::server::ip_filter::IpFilter;
use crate::server::traits::{BoundAddr, ProtocolHandler};
use crate::tenancy::{Ingest, Principal};
use convert::gelf_to_json;

/// The transport label the GELF listener counts under.
const TRANSPORT: &str = "gelf";

// ---------------------------------------------------------------------------
// Null-byte framing codec
// ---------------------------------------------------------------------------

/// GELF TCP framing decoder -- splits on null bytes (`\0`).
///
/// A message over the size limit costs that message and never the connection:
/// it is discarded up to its delimiter and counted for
/// [`GelfFrameDecoder::take_discarded`].
pub struct GelfFrameDecoder {
    max_length: usize,
    discarding: bool,
    discarded: u64,
}

impl GelfFrameDecoder {
    pub fn new(max_length: usize) -> Self {
        Self {
            max_length,
            discarding: false,
            discarded: 0,
        }
    }

    /// Messages discarded for their size since the last call.
    pub fn take_discarded(&mut self) -> u64 {
        std::mem::take(&mut self.discarded)
    }
}

impl crate::server::frame_clock::Framing for GelfFrameDecoder {
    fn mid_frame(&self) -> bool {
        self.discarding
    }
}

impl Decoder for GelfFrameDecoder {
    type Item = Bytes;
    type Error = io::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        loop {
            if self.discarding {
                let Some(null_pos) = memchr::memchr(b'\0', src) else {
                    src.clear();
                    return Ok(None);
                };
                src.advance(null_pos + 1);
                self.discarding = false;
            }

            match memchr::memchr(b'\0', src) {
                // An empty message between two delimiters is skipped.
                Some(0) => src.advance(1),
                Some(null_pos) if null_pos <= self.max_length => {
                    let frame = src.split_to(null_pos + 1).freeze();
                    return Ok(Some(frame.slice(..null_pos)));
                }
                Some(null_pos) => {
                    self.discarded += 1;
                    src.advance(null_pos + 1);
                }
                None if src.len() > self.max_length => {
                    self.discarded += 1;
                    src.clear();
                    self.discarding = true;
                    return Ok(None);
                }
                None => return Ok(None),
            }
        }
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
    stream: Clocked<S>,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    peer_addr: SocketAddr,
    max_message_size: usize,
) {
    use tokio_stream::StreamExt;

    let decoder = stream.decoder(GelfFrameDecoder::new(max_message_size));
    let mut framed = FramedRead::new(stream, decoder);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                debug!(peer = %peer_addr, "GELF TCP connection closing (shutdown)");
                break;
            }
            result = StreamExt::next(&mut framed) => {
                let oversize = framed.decoder_mut().get_mut().take_discarded();
                metrics.add_records_dropped(TRANSPORT, DropReason::Oversize, oversize);
                match result {
                    Some(Ok(raw)) => {
                        metrics.inc_requests_total("gelf");
                        metrics.add_bytes_received("gelf", raw.len() as u64);

                        match gelf_to_json(&raw) {
                            Ok(payload) => {
                                let held = [payload];
                                if !hold_until_settled(&pipeline, &held, &metrics, "gelf", &shutdown, &Acks::at_enqueue(), &Ingest::built(Principal::Listener(TRANSPORT), convert::SOURCE)).await {
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
                        warn!(peer = %peer_addr, error = %e, "GELF TCP read error");
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
    let oversize = framed.decoder_mut().get_mut().take_discarded();
    metrics.add_records_dropped(TRANSPORT, DropReason::Oversize, oversize);
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
    ip_filter: IpFilter,
    bound: BoundAddr,
) -> Result<()> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| Error::Server(format!("failed to bind GELF TCP listener: {e}")))?;
    let _serving = bound.publish(&listener.local_addr());

    let tls_enabled = tls_acceptor.is_some();
    info!(addr = %bind_addr, tls = tls_enabled, "GELF TCP listener started");

    let accept = Accept {
        ip_filter,
        metrics: metrics.clone(),
        transport: TRANSPORT,
        limits: ConnectionLimits::new(&pipeline.config().server.connections),
    };

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
                let Some(open) = accept.admit(peer_addr) else {
                    drop(stream);
                    continue;
                };

                let pipeline = pipeline.clone();
                let metrics = metrics.clone();
                let conn_shutdown = shutdown.clone();
                let accept = accept.clone();

                if let Some(ref acceptor) = tls_acceptor {
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        let _open = open;
                        if !accept.first_byte(&stream).await {
                            return;
                        }
                        let tls_result = timeout(accept.limits.tls_handshake, acceptor.accept(stream)).await;
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
                            accept.clock(tls_stream), pipeline, metrics, conn_shutdown, peer_addr,
                            max_message_size,
                        ).await;
                    });
                } else {
                    tokio::spawn(async move {
                        let _open = open;
                        if !accept.first_byte(&stream).await {
                            return;
                        }
                        handle_tcp_connection(
                            accept.clock(stream), pipeline, metrics, conn_shutdown, peer_addr,
                            max_message_size,
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
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    bound: BoundAddr,
}

impl GelfHandler {
    pub fn new(config: GelfConfig, pipeline: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        Self {
            config,
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
    use crate::server::frame_clock::Framing;

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

    /// An oversize message costs itself and nothing after it, whether its
    /// delimiter arrives in the same read or several reads later. The decoder
    /// says it is mid-frame while it discards, so the frame timeout keeps running.
    #[test]
    fn an_oversize_message_is_discarded_and_counted() {
        struct Case {
            name: &'static str,
            chunks: &'static [&'static [u8]],
            frames: &'static [&'static str],
            discarded: u64,
            mid_frame: bool,
        }
        let cases = [
            Case {
                name: "oversize with its delimiter in the same read",
                chunks: &[b"a\0this is too long\0b\0"],
                frames: &["a", "b"],
                discarded: 1,
                mid_frame: false,
            },
            Case {
                name: "oversize split across reads",
                chunks: &[b"a\0this is too", b" long and still going", b" on\0b\0"],
                frames: &["a", "b"],
                discarded: 1,
                mid_frame: false,
            },
            Case {
                name: "oversize still arriving",
                chunks: &[b"a\0this is too long", b" and no delimiter yet"],
                frames: &["a"],
                discarded: 1,
                mid_frame: true,
            },
            Case {
                name: "two oversize messages back to back",
                chunks: &[b"this is too long\0and so is this\0b\0"],
                frames: &["b"],
                discarded: 2,
                mid_frame: false,
            },
            Case {
                name: "exactly the limit is kept",
                chunks: &[b"0123456789\0"],
                frames: &["0123456789"],
                discarded: 0,
                mid_frame: false,
            },
            Case {
                name: "empty messages are skipped",
                chunks: &[b"\0\0a\0", b"\0b\0"],
                frames: &["a", "b"],
                discarded: 0,
                mid_frame: false,
            },
            Case {
                name: "a partial message waits",
                chunks: &[b"a\0par"],
                frames: &["a"],
                discarded: 0,
                mid_frame: false,
            },
        ];
        let mut failed = Vec::new();
        for case in cases {
            let mut decoder = GelfFrameDecoder::new(10);
            let mut buf = BytesMut::new();
            let mut frames = Vec::new();
            for chunk in case.chunks {
                buf.extend_from_slice(chunk);
                while let Some(frame) = decoder.decode(&mut buf).unwrap() {
                    frames.push(String::from_utf8_lossy(&frame).into_owned());
                }
            }
            let got = (frames, decoder.take_discarded(), decoder.mid_frame());
            let want = (
                case.frames
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                case.discarded,
                case.mid_frame,
            );
            if got != want {
                failed.push(format!("{}: got {got:?}, want {want:?}", case.name));
            }
        }
        assert!(failed.is_empty(), "{}", failed.join("\n"));
    }
}
