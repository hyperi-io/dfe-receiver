// Project:   dfe-receiver
// File:      src/server/lumberjack/mod.rs
// Purpose:   Lumberjack v2 (Beats) protocol handler
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Lumberjack v2 (Beats) protocol handler.
//!
//! Accepts data from Elastic Beats agents (Filebeat, Winlogbeat, etc.)
//! over TCP/TLS using the Lumberjack v2 wire protocol.
//!
//! Standard port: 5044

pub mod codec;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Sampled counter for Lumberjack event processing errors (log 1 in 100).
static LUMBERJACK_ERRORS: AtomicU64 = AtomicU64::new(0);

use crate::config::LumberjackConfig;
use crate::error::{Error, Result};
use crate::metrics::{DropReason, Metrics};
use crate::pipeline::{Acks, PipelineState};
use crate::server::frame_clock::Clocked;
use crate::server::http::Accept;
use crate::server::traits::{BoundAddr, ProtocolHandler};
use crate::tenancy::{Ingest, Principal};
use codec::{CompressedFrames, Frame, encode_ack, read_frame};

/// The transport label the Lumberjack listener counts under.
const TRANSPORT: &str = "lumberjack";

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

/// Most event bytes a connection holds unsettled. A window past it is
/// settled, and ACKed, in parts: a client may announce 100 000 events of up to
/// 16 MiB each, and only what is held unsettled bypasses the pipeline's
/// memory guard.
const MAX_HELD_BYTES: usize = 16 * 1024 * 1024;

/// The events read since the last ACK, with their sequence numbers.
#[derive(Default)]
struct Window {
    /// Events the current `Window` frame announced; zero before one arrives.
    size: u32,
    /// Events of the current window read so far, settled or not.
    received: u32,
    sequences: Vec<u32>,
    payloads: Vec<bytes::Bytes>,
    /// Bytes of the unsettled events.
    held: usize,
}

impl Window {
    /// Begin a window of `size` events.
    fn open(&mut self, size: u32) {
        self.size = size;
        self.received = 0;
    }

    fn push(&mut self, sequence: u32, payload: bytes::Bytes) {
        self.received = self.received.saturating_add(1);
        self.held += payload.len();
        self.sequences.push(sequence);
        self.payloads.push(payload);
    }

    /// Whether every event the window announced has arrived.
    fn is_complete(&self) -> bool {
        self.received >= self.size
    }

    /// Whether the events held should be settled now: the window is complete,
    /// or holding more would pass [`MAX_HELD_BYTES`].
    fn is_due(&self) -> bool {
        self.is_complete() || self.held >= MAX_HELD_BYTES
    }

    fn clear(&mut self) {
        self.sequences.clear();
        self.payloads.clear();
        self.held = 0;
    }
}

/// The pipeline, and when the connection's events count as taken.
struct Intake<'a> {
    pipeline: &'a PipelineState,
    metrics: &'a Metrics,
    acks: &'a Acks,
    peer_addr: SocketAddr,
}

impl Intake<'_> {
    /// Offer the window's events to the pipeline and ACK the last one it
    /// settled. False when the connection must close, after a partial ACK
    /// for the events before the first it could not take.
    ///
    /// An event refused for good counts as settled: Lumberjack has no refusal,
    /// and a resend would be refused again.
    async fn settle<W: AsyncWrite + Unpin>(&self, window: &mut Window, writer: &mut W) -> bool {
        if window.payloads.is_empty() {
            return true;
        }
        for payload in &window.payloads {
            self.metrics.inc_requests_total("lumberjack");
            self.metrics
                .add_bytes_received("lumberjack", payload.len() as u64);
        }

        // A Beats frame carries no credential, and its events are the sender's own JSON.
        let outcome = self
            .pipeline
            .process_batch_acked(
                &window.payloads,
                self.acks,
                None,
                &Ingest::sent(Principal::Listener(TRANSPORT)),
            )
            .await;
        for _ in 0..outcome.accepted {
            self.metrics.inc_requests_success("lumberjack");
        }
        for _ in 0..outcome.rejected {
            self.metrics.inc_requests_error("lumberjack");
        }
        self.metrics.add_records_dropped(
            "lumberjack",
            DropReason::Rejected,
            outcome.rejected as u64,
        );
        let taken = outcome
            .settled()
            .checked_sub(1)
            .map_or(0, |last| window.sequences[last]);
        let events = window.payloads.len();
        window.clear();

        let Some(e) = outcome.unavailable else {
            return ack(writer, taken, self.peer_addr).await;
        };
        if scalo::logger::log_sampled(&LUMBERJACK_ERRORS, 100) {
            let total = LUMBERJACK_ERRORS.load(Ordering::Relaxed);
            warn!(peer = %self.peer_addr, events, error = %e, total_errors = total, "Lumberjack window not taken (1 in 100)");
        }
        self.metrics.inc_requests_error("lumberjack");
        self.metrics.record_backpressure();
        ack_and_close(writer, taken, self.peer_addr).await;
        false
    }
}

/// Acknowledge every event up to `sequence`. False when the write failed.
async fn ack<W: AsyncWrite + Unpin>(writer: &mut W, sequence: u32, peer_addr: SocketAddr) -> bool {
    if sequence == 0 {
        return true;
    }
    match writer.write_all(&encode_ack(sequence)).await {
        Ok(()) => true,
        Err(e) => {
            warn!(peer = %peer_addr, error = %e, "Failed to send ACK");
            false
        }
    }
}

/// Acknowledge every event up to `sequence`, then close: the client resends
/// the rest of its window on a new connection.
///
/// The go-lumber client that Beats uses reads a partial ACK as progress and a
/// closed connection as an error, on which Beats re-queues the window's
/// unacknowledged events.
async fn ack_and_close<W: AsyncWrite + Unpin>(
    writer: &mut W,
    sequence: u32,
    peer_addr: SocketAddr,
) {
    if sequence > 0
        && let Err(e) = writer.write_all(&encode_ack(sequence)).await
    {
        debug!(peer = %peer_addr, error = %e, "Failed to send partial ACK");
    }
    debug!(peer = %peer_addr, acked = sequence, "Lumberjack event not taken; closing so the client resends");
}

/// Handle a single Lumberjack v2 client connection.
///
/// Reads frames in a loop and hands each window's events to the pipeline
/// together once the window is complete, or at the end of a compressed frame,
/// then ACKs them. With `acks` holding, the ACK waits until every destination
/// confirmed the events. A window the pipeline cannot take ends the connection
/// after a partial ACK for the events before the first it could not take, so
/// the client resends the rest and nothing behind it is acknowledged.
async fn handle_connection<S: AsyncRead + AsyncWrite + Unpin>(
    mut reader: Clocked<BufReader<S>>,
    intake: Intake<'_>,
    shutdown: CancellationToken,
) {
    let peer_addr = intake.peer_addr;
    let clock = reader.clock();
    let mut window = Window::default();

    loop {
        let frame = tokio::select! {
            _ = shutdown.cancelled() => {
                debug!(peer = %peer_addr, "Lumberjack connection closing (shutdown)");
                break;
            }
            result = read_frame(&mut reader) => {
                match result {
                    Ok(Some(frame)) => {
                        clock.frame_ended();
                        frame
                    }
                    Ok(None) => {
                        debug!(peer = %peer_addr, "Lumberjack client disconnected");
                        break;
                    }
                    Err(e) => {
                        warn!(peer = %peer_addr, error = %e, "Lumberjack protocol error");
                        break;
                    }
                }
            }
        };

        match frame {
            Frame::Window { size } => {
                // Events of a window the client left short are settled before the next.
                if !intake.settle(&mut window, reader.get_mut()).await {
                    return;
                }
                window.open(size);
                debug!(peer = %peer_addr, window_size = size, "Lumberjack window started");
            }

            Frame::JsonData { sequence, payload } => {
                window.push(sequence, payload);
                if window.is_due() && !intake.settle(&mut window, reader.get_mut()).await {
                    return;
                }
            }

            Frame::Compressed { data } => {
                for inner in CompressedFrames::new(&data) {
                    let due = match inner {
                        Ok(Frame::Window { size }) => {
                            if !intake.settle(&mut window, reader.get_mut()).await {
                                return;
                            }
                            window.open(size);
                            false
                        }
                        Ok(Frame::JsonData { sequence, payload }) => {
                            window.push(sequence, payload);
                            window.held >= MAX_HELD_BYTES
                        }
                        Ok(Frame::Compressed { .. }) => {
                            warn!(peer = %peer_addr, "Nested compressed frame rejected");
                            break;
                        }
                        // The events before the bad frame were sent whole, so they are settled first.
                        Err(e) => {
                            warn!(peer = %peer_addr, error = %e, "Failed to decompress Lumberjack frame");
                            let _ = intake.settle(&mut window, reader.get_mut()).await;
                            return;
                        }
                    };
                    if due && !intake.settle(&mut window, reader.get_mut()).await {
                        return;
                    }
                }

                // ACK after processing all events in compressed batch
                if !intake.settle(&mut window, reader.get_mut()).await {
                    return;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Protocol handler
// ---------------------------------------------------------------------------

/// Lumberjack v2 protocol handler.
pub struct LumberjackHandler {
    config: LumberjackConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    bound: BoundAddr,
}

impl LumberjackHandler {
    pub fn new(
        config: LumberjackConfig,
        pipeline: Arc<PipelineState>,
        metrics: Arc<Metrics>,
    ) -> Self {
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
impl ProtocolHandler for LumberjackHandler {
    fn name(&self) -> &'static str {
        "lumberjack"
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
            .map_err(|e| Error::Config(format!("invalid Lumberjack bind address: {e}")))?;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| Error::Server(format!("failed to bind Lumberjack listener: {e}")))?;

        // A Lumberjack frame carries no credential, so the IP filter and the
        // TLS handshake are the whole admission surface on this port.
        let accept = Accept::new(
            &self.pipeline.config().server,
            self.metrics.clone(),
            TRANSPORT,
        );
        let acks = self
            .pipeline
            .acks("lumberjack", self.config.acknowledgements, None);

        // Build TLS acceptor if enabled
        let tls_acceptor = if self.config.tls.enabled {
            let acceptor = super::tls::build_tls_acceptor_async(&self.config.tls).await?;
            info!(addr = %addr, tls = true, "Lumberjack server listening");
            acceptor
        } else {
            info!(addr = %addr, tls = false, "Lumberjack server listening");
            None
        };

        // Published once TLS is ready, so a failed TLS setup never reads as serving.
        let _serving = self.bound.publish(&listener.local_addr());

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Lumberjack server stopping");
                    break;
                }
                result = listener.accept() => {
                    let (stream, peer_addr) = match result {
                        Ok(conn) => conn,
                        Err(e) => {
                            error!(error = %e, "Failed to accept Lumberjack connection");
                            continue;
                        }
                    };

                    // Reject before the TLS handshake and before any frame is read.
                    let Some(open) = accept.admit(peer_addr) else {
                        drop(stream);
                        continue;
                    };

                    let pipeline = self.pipeline.clone();
                    let metrics = self.metrics.clone();
                    let acks = acks.clone();
                    let conn_shutdown = shutdown.clone();
                    let accept = accept.clone();

                    if let Some(ref acceptor) = tls_acceptor {
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            let _open = open;
                            if !accept.first_byte(&stream).await {
                                return;
                            }
                            // TLS handshake with timeout
                            let tls_result = timeout(accept.limits.tls_handshake, acceptor.accept(stream)).await;
                            let tls_stream = match tls_result {
                                Ok(Ok(s)) => s,
                                Ok(Err(e)) => {
                                    metrics.inc_tls_handshake_failure();
                                    debug!(peer = %peer_addr, error = %e, "Lumberjack TLS handshake failed");
                                    return;
                                }
                                Err(_) => {
                                    metrics.inc_tls_handshake_failure();
                                    warn!(peer = %peer_addr, "Lumberjack TLS handshake timeout");
                                    return;
                                }
                            };

                            debug!(peer = %peer_addr, "Lumberjack TLS connection established");
                            let intake = Intake { pipeline: &pipeline, metrics: &metrics, acks: &acks, peer_addr };
                            handle_connection(accept.clock(BufReader::new(tls_stream)), intake, conn_shutdown).await;
                        });
                    } else {
                        tokio::spawn(async move {
                            let _open = open;
                            if !accept.first_byte(&stream).await {
                                return;
                            }
                            let intake = Intake { pipeline: &pipeline, metrics: &metrics, acks: &acks, peer_addr };
                            handle_connection(accept.clock(BufReader::new(stream)), intake, conn_shutdown).await;
                        });
                    }
                }
            }
        }

        info!("Lumberjack server stopped");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_default_config() {
        let config = LumberjackConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.bind_address, "0.0.0.0:5044");
        assert!(!config.tls.enabled);
    }

    /// A JSON data frame of `bytes` bytes.
    fn data_frame(sequence: u32, bytes: usize) -> Vec<u8> {
        let mut payload = vec![b' '; bytes];
        payload[0] = b'{';
        payload[bytes - 1] = b'}';
        let mut frame = vec![codec::PROTOCOL_VERSION, codec::FRAME_JSON_DATA];
        frame.extend_from_slice(&sequence.to_be_bytes());
        frame.extend_from_slice(&u32::try_from(bytes).unwrap().to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    /// A window bigger than [`MAX_HELD_BYTES`] is settled and ACKed in parts,
    /// plain or inside one compressed frame, so a client cannot make a
    /// connection hold more than that unsettled.
    #[tokio::test]
    async fn a_window_past_the_held_byte_limit_is_acked_in_parts() {
        use std::io::Write as _;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        use crate::config::{Config, SharedConfig};

        const EVENT: usize = 6 * 1024 * 1024;
        let mut config = Config::default();
        config.destinations.default = "loader".into();
        config.loader.transport = "memory".to_string();
        let pipeline = PipelineState::new(SharedConfig::new(config), CancellationToken::new())
            .await
            .unwrap();
        let metrics = Metrics::default();
        let acks = Acks::at_enqueue();

        for compressed in [false, true] {
            let mut window = vec![codec::PROTOCOL_VERSION, codec::FRAME_WINDOW];
            window.extend_from_slice(&4_u32.to_be_bytes());
            let events: Vec<u8> = (1..=4).flat_map(|seq| data_frame(seq, EVENT)).collect();
            let sent = if compressed {
                let mut encoder =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
                encoder.write_all(&events).unwrap();
                let block = encoder.finish().unwrap();
                let mut frame = vec![codec::PROTOCOL_VERSION, codec::FRAME_COMPRESSED];
                frame.extend_from_slice(&u32::try_from(block.len()).unwrap().to_be_bytes());
                frame.extend_from_slice(&block);
                [window, frame].concat()
            } else {
                [window, events].concat()
            };

            let (mut client, server) = tokio::io::duplex(1 << 20);
            let intake = Intake {
                pipeline: &pipeline,
                metrics: &metrics,
                acks: &acks,
                peer_addr: "127.0.0.1:5044".parse().unwrap(),
            };
            let shutdown = CancellationToken::new();
            let client_side = async {
                client.write_all(&sent).await.unwrap();
                let mut acked = Vec::new();
                for _ in 0..2 {
                    let mut ack = [0_u8; 6];
                    tokio::time::timeout(Duration::from_secs(10), client.read_exact(&mut ack))
                        .await
                        .unwrap()
                        .unwrap();
                    acked.push(u32::from_be_bytes([ack[2], ack[3], ack[4], ack[5]]));
                }
                shutdown.cancel();
                acked
            };
            let reader = Clocked::new(
                BufReader::new(server),
                Duration::from_secs(30),
                Arc::new(Metrics::default()),
                TRANSPORT,
            );
            let (acked, ()) = tokio::join!(
                client_side,
                handle_connection(reader, intake, shutdown.clone())
            );
            assert_eq!(acked, [3, 4], "compressed: {compressed}");
        }
    }
}
