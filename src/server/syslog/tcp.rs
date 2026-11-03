// Project:   dfe-receiver
// File:      src/server/syslog/tcp.rs
// Purpose:   Syslog TCP and TLS listeners: batched settle, keepalive, idle close
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The syslog TCP and TLS listeners.
//!
//! Every frame one read produced is settled in a single hold: the socket is
//! not read again until the pipeline has taken the batch, since RFC 6587 has
//! no acknowledgement to ask the sender to retry. Each accepted socket gets
//! TCP keepalive (`syslog.tcp_keepalive_secs`), so a peer that has gone frees
//! its place, and a connection with no frame for `syslog.idle_timeout_ms` is
//! closed.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Instant, timeout};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::convert::Converter;
use super::framing::SyslogFrameDecoder;
use super::{TRANSPORT, decode_lossy, listener_ingest};
use crate::error::{Error, Result};
use crate::metrics::{DropReason, Metrics};
use crate::pipeline::{Acks, PipelineState};
use crate::server::frame_clock::{Clocked, Framing};
use crate::server::hold::{Held, hold};
use crate::server::http::{Accept, ConnectionLimits};
use crate::server::ip_filter::IpFilter;
use crate::server::traits::BoundAddr;

/// The idle timer's period with idle closing off, when it is never polled.
const NEVER: Duration = Duration::from_hours(24);

/// How every connection of one listener reads and settles its frames.
#[derive(Clone, Copy)]
pub(super) struct Connection {
    pub(super) converter: Converter,
    pub(super) max_message_size: usize,
    pub(super) max_frame_size: usize,
    pub(super) keepalive: Option<Duration>,
    pub(super) idle: Option<Duration>,
}

/// One TCP or TLS listener.
pub(super) struct TcpListenerTask {
    pub(super) bind_addr: SocketAddr,
    pub(super) pipeline: Arc<PipelineState>,
    pub(super) metrics: Arc<Metrics>,
    pub(super) tls: Option<tokio_rustls::TlsAcceptor>,
    pub(super) connection: Connection,
    pub(super) label: &'static str,
    pub(super) ip_filter: IpFilter,
    pub(super) bound: BoundAddr,
}

/// Turn on keepalive for `stream`: probes start after `idle` with no traffic
/// and repeat every `idle`.
pub(super) fn keep_alive(stream: &TcpStream, idle: Duration) -> std::io::Result<()> {
    let params = TcpKeepalive::new().with_time(idle);
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    let params = params.with_interval(idle);
    SockRef::from(stream).set_tcp_keepalive(&params)
}

impl TcpListenerTask {
    /// Accept connections until `shutdown`.
    pub(super) async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let label = self.label;
        let listener = TcpListener::bind(self.bind_addr)
            .await
            .map_err(|e| Error::Server(format!("failed to bind syslog {label} listener: {e}")))?;
        let _serving = self.bound.publish(&listener.local_addr());
        info!(addr = %self.bind_addr, tls = self.tls.is_some(), "Syslog {label} listener started");

        let accept = Accept {
            ip_filter: self.ip_filter.clone(),
            metrics: self.metrics.clone(),
            transport: TRANSPORT,
            limits: ConnectionLimits::new(&self.pipeline.config().server.connections),
        };

        loop {
            let (stream, peer_addr) = tokio::select! {
                () = shutdown.cancelled() => break,
                result = listener.accept() => match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!(error = %e, "Failed to accept syslog {label} connection");
                        continue;
                    }
                },
            };

            // Reject before the TLS handshake and before any framing.
            let Some(open) = accept.admit(peer_addr) else {
                drop(stream);
                continue;
            };
            if let Some(idle) = self.connection.keepalive
                && let Err(e) = keep_alive(&stream, idle)
            {
                debug!(peer = %peer_addr, error = %e, "Syslog {label}: keepalive not set");
            }

            let reader = Reader {
                pipeline: self.pipeline.clone(),
                metrics: self.metrics.clone(),
                shutdown: shutdown.clone(),
                peer_addr,
                connection: self.connection,
            };
            let accept = accept.clone();
            let tls = self.tls.clone();
            tokio::spawn(async move {
                let _open = open;
                if !accept.first_byte(&stream).await {
                    return;
                }
                let Some(acceptor) = tls else {
                    reader.read(accept.clock(stream)).await;
                    return;
                };
                let handshake = timeout(accept.limits.tls_handshake, acceptor.accept(stream)).await;
                let tls_stream = match handshake {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => {
                        reader.metrics.inc_tls_handshake_failure();
                        debug!(peer = %peer_addr, error = %e, "Syslog TLS handshake failed");
                        return;
                    }
                    Err(_) => {
                        reader.metrics.inc_tls_handshake_failure();
                        warn!(peer = %peer_addr, "Syslog TLS handshake timeout");
                        return;
                    }
                };
                debug!(peer = %peer_addr, "Syslog TLS connection established");
                reader.read(accept.clock(tls_stream)).await;
            });
        }

        info!("Syslog {label} listener stopping");
        Ok(())
    }
}

/// Why a connection stops being read after its current batch.
enum End {
    Closed,
    Failed(std::io::Error),
}

/// One connection's reader.
struct Reader {
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    peer_addr: SocketAddr,
    connection: Connection,
}

impl Reader {
    /// Read and settle frames until the peer closes, it goes idle, or shutdown.
    async fn read<S: AsyncRead + AsyncWrite + Unpin>(self, stream: Clocked<S>) {
        let c = self.connection;
        let decoder = stream.decoder(SyslogFrameDecoder::with_max_frame(
            c.max_message_size,
            c.max_frame_size,
        ));
        let mut framed = FramedRead::new(stream, decoder);
        let mut payloads: Vec<Bytes> = Vec::new();
        let idle_for = c.idle.unwrap_or(NEVER);
        let idle = tokio::time::sleep(idle_for);
        tokio::pin!(idle);

        loop {
            let first = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => {
                    debug!(peer = %self.peer_addr, "Syslog TCP connection closing (shutdown)");
                    return;
                }
                () = &mut idle, if c.idle.is_some() => {
                    // A frame part way in is the frame timeout's to close.
                    if framed.read_buffer().is_empty() && !framed.decoder().get_ref().mid_frame() {
                        let oversize = framed.decoder_mut().get_mut().take_discarded();
                        self.metrics
                            .add_records_dropped(TRANSPORT, DropReason::Oversize, oversize);
                        self.metrics.inc_connection_closed(TRANSPORT, "idle");
                        debug!(peer = %self.peer_addr, "Syslog TCP connection closing (idle)");
                        return;
                    }
                    idle.as_mut().reset(Instant::now() + idle_for);
                    continue;
                }
                next = framed.next() => next,
            };

            let end = match first {
                Some(Ok(frame)) => {
                    self.push(&frame, &mut payloads);
                    self.drain_buffered(&mut framed, &mut payloads)
                }
                Some(Err(e)) => Some(End::Failed(e)),
                None => Some(End::Closed),
            };

            let oversize = framed.decoder_mut().get_mut().take_discarded();
            self.metrics
                .add_records_dropped(TRANSPORT, DropReason::Oversize, oversize);
            if !payloads.is_empty() {
                let held = hold(
                    &self.pipeline,
                    &payloads,
                    &self.metrics,
                    TRANSPORT,
                    &self.shutdown,
                    &Acks::at_enqueue(),
                    &listener_ingest(),
                )
                .await;
                // A frame is a request, as a datagram is on UDP.
                let (taken, refused) = match held {
                    Held::Settled { rejected } => (payloads.len() - rejected, rejected),
                    Held::Shutdown => (0, payloads.len()),
                };
                for _ in 0..taken {
                    self.metrics.inc_requests_success(TRANSPORT);
                }
                for _ in 0..refused {
                    self.metrics.inc_requests_error(TRANSPORT);
                }
                payloads.clear();
                if held == Held::Shutdown {
                    debug!(peer = %self.peer_addr, "Syslog TCP connection closing (shutdown during a hold)");
                    return;
                }
            }
            match end {
                None => idle.as_mut().reset(Instant::now() + idle_for),
                Some(End::Closed) => {
                    debug!(peer = %self.peer_addr, "Syslog TCP client disconnected");
                    return;
                }
                Some(End::Failed(e)) => {
                    warn!(peer = %self.peer_addr, error = %e, "Syslog TCP read error");
                    return;
                }
            }
        }
    }

    /// Add every further frame the last read already brought in.
    fn drain_buffered<S, D>(
        &self,
        framed: &mut FramedRead<S, D>,
        payloads: &mut Vec<Bytes>,
    ) -> Option<End>
    where
        D: Decoder<Item = Bytes, Error = std::io::Error>,
    {
        // FramedRead lends its buffer and its decoder one at a time, so the
        // buffer comes out for the drain and goes back after it.
        let mut buffer = std::mem::take(framed.read_buffer_mut());
        let end = loop {
            match framed.decoder_mut().decode(&mut buffer) {
                Ok(Some(frame)) => self.push(&frame, payloads),
                Ok(None) => break None,
                Err(e) => break Some(End::Failed(e)),
            }
        };
        *framed.read_buffer_mut() = buffer;
        end
    }

    /// Count one frame and add its record to the batch.
    fn push(&self, frame: &[u8], payloads: &mut Vec<Bytes>) {
        self.metrics.inc_requests_total(TRANSPORT);
        self.metrics
            .add_bytes_received(TRANSPORT, frame.len() as u64);
        let raw = decode_lossy(frame, &self.metrics);
        match self.connection.converter.convert(&raw) {
            Ok(payload) => payloads.push(payload),
            Err(e) => {
                debug!(peer = %self.peer_addr, error = %e, "Syslog TCP parse error");
                self.metrics.inc_parse_failure(TRANSPORT);
                self.metrics.inc_requests_error(TRANSPORT);
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn keepalive_is_set_with_its_idle_time() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        keep_alive(&server, Duration::from_secs(45)).unwrap();
        let sock = SockRef::from(&server);
        assert!(sock.keepalive().unwrap());
        assert_eq!(sock.tcp_keepalive_time().unwrap(), Duration::from_secs(45));
        assert_eq!(
            sock.tcp_keepalive_interval().unwrap(),
            Duration::from_secs(45)
        );
        drop(client);
    }
}
