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

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::GelfConfig;
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::{Acks, PipelineState};
use crate::server::frame_clock::Clocked;
use crate::server::hold::hold_until_settled;
use crate::server::http::{Accept, ConnectionLimits};
use crate::server::ip_filter::IpFilter;
use crate::server::oversize::{self, Frame, Oversize, Skipping};
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
/// it is skipped up to its delimiter and handed over as [`Frame::Oversize`],
/// with its first bytes and its length, for the listener to dead-letter.
pub struct GelfFrameDecoder {
    max_length: usize,
    discarding: bool,
    /// The head and length of the oversize message being skipped.
    skipping: Skipping,
}

impl GelfFrameDecoder {
    pub fn new(max_length: usize) -> Self {
        Self {
            max_length,
            discarding: false,
            skipping: Skipping::unmeasured(),
        }
    }

    /// The oversize message being skipped, as far as it arrived, for a stream
    /// that ends or fails inside it.
    pub fn abandon(&mut self) -> Option<Oversize> {
        std::mem::take(&mut self.discarding).then(|| self.skipping.finish())
    }
}

impl crate::server::frame_clock::Framing for GelfFrameDecoder {
    fn mid_frame(&self) -> bool {
        self.discarding
    }
}

impl Decoder for GelfFrameDecoder {
    type Item = Frame;
    type Error = io::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        loop {
            if self.discarding {
                let Some(null_pos) = memchr::memchr(b'\0', src) else {
                    self.skipping.grow(src);
                    src.clear();
                    return Ok(None);
                };
                self.skipping.grow(&src[..null_pos]);
                src.advance(null_pos + 1);
                self.discarding = false;
                return Ok(Some(Frame::Oversize(self.skipping.finish())));
            }

            match memchr::memchr(b'\0', src) {
                // An empty message between two delimiters is skipped.
                Some(0) => src.advance(1),
                Some(null_pos) if null_pos <= self.max_length => {
                    let frame = src.split_to(null_pos + 1).freeze();
                    return Ok(Some(Frame::Message(frame.slice(..null_pos))));
                }
                Some(null_pos) => {
                    let frame = oversize::whole(&src[..null_pos], null_pos);
                    src.advance(null_pos + 1);
                    return Ok(Some(Frame::Oversize(frame)));
                }
                None if src.len() > self.max_length => {
                    self.skipping = Skipping::unmeasured();
                    self.skipping.grow(src);
                    src.clear();
                    self.discarding = true;
                    return Ok(None);
                }
                None => return Ok(None),
            }
        }
    }

    /// An oversize message the stream ended inside is handed over as far as it
    /// arrived, and any other partial message is an error.
    fn decode_eof(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        if let Some(frame) = self.decode(src)? {
            return Ok(Some(frame));
        }
        if let Some(skipped) = self.abandon() {
            return Ok(Some(Frame::Oversize(skipped)));
        }
        if src.is_empty() {
            Ok(None)
        } else {
            Err(io::Error::other("bytes remaining on stream"))
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
                match result {
                    Some(Ok(Frame::Oversize(skipped))) => {
                        if !dead_letter_oversize(vec![skipped], &pipeline, &metrics, max_message_size, &shutdown).await {
                            debug!(peer = %peer_addr, "GELF TCP connection closing (shutdown during a DLQ write)");
                            break;
                        }
                    }
                    Some(Ok(Frame::Message(raw))) => {
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
                        let skipped: Vec<Oversize> = framed.decoder_mut().get_mut().abandon().into_iter().collect();
                        dead_letter_oversize(skipped, &pipeline, &metrics, max_message_size, &shutdown).await;
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

/// Dead-letter the messages the framing skipped for their size. Returns false
/// when shutdown came first, and the caller must stop reading.
async fn dead_letter_oversize(
    mut skipped: Vec<Oversize>,
    pipeline: &PipelineState,
    metrics: &Metrics,
    limit: usize,
    shutdown: &CancellationToken,
) -> bool {
    oversize::dead_letter(
        &mut skipped,
        pipeline,
        metrics,
        TRANSPORT,
        TRANSPORT,
        limit,
        shutdown,
    )
    .await
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
    use bytes::Bytes;

    use super::*;
    use crate::server::frame_clock::Framing;

    /// The message a frame holds.
    fn message(frame: Frame) -> Bytes {
        match frame {
            Frame::Message(message) => message,
            Frame::Oversize(skipped) => panic!("a message was skipped: {skipped:?}"),
        }
    }

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
        let result = message(decoder.decode(&mut buf).unwrap().unwrap());
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

        let msg1 = message(decoder.decode(&mut buf).unwrap().unwrap());
        let msg2 = message(decoder.decode(&mut buf).unwrap().unwrap());
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

    /// What a decoder given `chunks` yields, and whether it is mid-frame after.
    fn decode_chunks(
        decoder: &mut GelfFrameDecoder,
        chunks: &[&[u8]],
    ) -> (Vec<String>, Vec<Oversize>, bool) {
        let mut buf = BytesMut::new();
        let (mut messages, mut skipped) = (Vec::new(), Vec::new());
        for chunk in chunks {
            buf.extend_from_slice(chunk);
            while let Some(frame) = decoder.decode(&mut buf).unwrap() {
                match frame {
                    Frame::Message(message) => {
                        messages.push(String::from_utf8_lossy(&message).into_owned());
                    }
                    Frame::Oversize(frame) => skipped.push(frame),
                }
            }
        }
        (messages, skipped, decoder.mid_frame())
    }

    /// An oversize message costs itself and nothing after it, whether its
    /// delimiter arrives in the same read or several reads later, and is handed
    /// over once its delimiter has arrived. The decoder says it is mid-frame
    /// while it skips, so the frame timeout keeps running.
    #[test]
    fn an_oversize_message_is_skipped_and_handed_over() {
        struct Case {
            name: &'static str,
            chunks: &'static [&'static [u8]],
            frames: &'static [&'static str],
            /// The head and length of each oversize message handed over.
            skipped: &'static [(&'static str, usize)],
            mid_frame: bool,
        }
        let cases = [
            Case {
                name: "oversize with its delimiter in the same read",
                chunks: &[b"a\0this is too long\0b\0"],
                frames: &["a", "b"],
                skipped: &[("this is too long", 16)],
                mid_frame: false,
            },
            Case {
                name: "oversize split across reads",
                chunks: &[b"a\0this is too", b" long and still going", b" on\0b\0"],
                frames: &["a", "b"],
                skipped: &[("this is too long and still going on", 35)],
                mid_frame: false,
            },
            Case {
                name: "oversize still arriving",
                chunks: &[b"a\0this is too long", b" and no delimiter yet"],
                frames: &["a"],
                skipped: &[],
                mid_frame: true,
            },
            Case {
                name: "two oversize messages back to back",
                chunks: &[b"this is too long\0and so is this\0b\0"],
                frames: &["b"],
                skipped: &[("this is too long", 16), ("and so is this", 14)],
                mid_frame: false,
            },
            Case {
                name: "exactly the limit is kept",
                chunks: &[b"0123456789\0"],
                frames: &["0123456789"],
                skipped: &[],
                mid_frame: false,
            },
            Case {
                name: "empty messages are skipped",
                chunks: &[b"\0\0a\0", b"\0b\0"],
                frames: &["a", "b"],
                skipped: &[],
                mid_frame: false,
            },
            Case {
                name: "a partial message waits",
                chunks: &[b"a\0par"],
                frames: &["a"],
                skipped: &[],
                mid_frame: false,
            },
        ];
        let mut failed = Vec::new();
        for case in cases {
            let (frames, skipped, mid_frame) =
                decode_chunks(&mut GelfFrameDecoder::new(10), case.chunks);
            let got = (
                frames,
                skipped
                    .iter()
                    .map(|s| {
                        (
                            String::from_utf8_lossy(&s.head).into_owned(),
                            s.original_bytes,
                        )
                    })
                    .collect::<Vec<_>>(),
                mid_frame,
            );
            let want = (
                case.frames
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                case.skipped
                    .iter()
                    .map(|(head, len)| ((*head).to_string(), *len))
                    .collect::<Vec<_>>(),
                case.mid_frame,
            );
            if got != want {
                failed.push(format!("{}: got {got:?}, want {want:?}", case.name));
            }
        }
        assert!(failed.is_empty(), "{}", failed.join("\n"));
    }

    /// A stream that ends or fails inside an oversize message still hands over
    /// the part that arrived, and a long one keeps only its head.
    #[test]
    fn an_oversize_message_cut_by_the_stream_is_handed_over_as_far_as_it_arrived() {
        let mut decoder = GelfFrameDecoder::new(10);
        let mut buf = BytesMut::from(&b"this is too long and no delimiter"[..]);
        assert!(decoder.decode(&mut buf).unwrap().is_none());
        let Some(Frame::Oversize(skipped)) = decoder.decode_eof(&mut buf).unwrap() else {
            panic!("the oversize message was not handed over");
        };
        assert_eq!(skipped.original_bytes, 33);
        assert_eq!(&skipped.head[..], b"this is too long and no delimiter");
        assert!(decoder.decode_eof(&mut buf).unwrap().is_none());

        let mut decoder = GelfFrameDecoder::new(10);
        let chunk = vec![b'x'; oversize::HEAD_BYTES];
        let mut buf = BytesMut::new();
        for _ in 0..3 {
            buf.extend_from_slice(&chunk);
            assert!(decoder.decode(&mut buf).unwrap().is_none());
        }
        let skipped = decoder.abandon().unwrap();
        assert_eq!(skipped.original_bytes, 3 * oversize::HEAD_BYTES);
        assert_eq!(skipped.head.len(), oversize::HEAD_BYTES);
        assert!(decoder.abandon().is_none());
    }
}
