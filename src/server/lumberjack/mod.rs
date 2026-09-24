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
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Sampled counter for Lumberjack event processing errors (log 1 in 100).
static LUMBERJACK_ERRORS: AtomicU64 = AtomicU64::new(0);

use crate::config::LumberjackConfig;
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::ip_filter::IpFilter;
use crate::server::traits::{BoundAddr, ProtocolHandler};
use codec::{Frame, decompress_and_parse, encode_ack, read_frame};

/// TLS handshake timeout (matches HTTP handler).
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

/// Handle a single Lumberjack v2 client connection.
///
/// Reads frames in a loop, processes JSON payloads through the pipeline,
/// and sends ACKs after completing each window.
async fn handle_connection<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    peer_addr: SocketAddr,
) {
    let mut reader = BufReader::new(stream);
    let mut window_size: u32 = 0;
    let mut events_in_window: u32 = 0;
    let mut last_sequence: u32 = 0;

    loop {
        let frame = tokio::select! {
            _ = shutdown.cancelled() => {
                debug!(peer = %peer_addr, "Lumberjack connection closing (shutdown)");
                break;
            }
            result = read_frame(&mut reader) => {
                match result {
                    Ok(Some(frame)) => frame,
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
                window_size = size;
                events_in_window = 0;
                debug!(peer = %peer_addr, window_size, "Lumberjack window started");
            }

            Frame::JsonData { sequence, payload } => {
                last_sequence = sequence;
                events_in_window += 1;

                metrics.inc_requests_total("lumberjack");
                metrics.add_bytes_received("lumberjack", payload.len() as u64);

                if let Err(e) = pipeline.process(payload).await {
                    if scalo::logger::log_sampled(&LUMBERJACK_ERRORS, 100) {
                        let total = LUMBERJACK_ERRORS.load(Ordering::Relaxed);
                        warn!(peer = %peer_addr, seq = sequence, error = %e, total_errors = total, "Lumberjack event error (1 in 100)");
                    }
                    metrics.inc_requests_error("lumberjack");
                } else {
                    metrics.inc_requests_success("lumberjack");
                }

                // ACK after window is complete
                if window_size > 0 && events_in_window >= window_size {
                    let ack = encode_ack(last_sequence);
                    let writer = reader.get_mut();
                    if let Err(e) = writer.write_all(&ack).await {
                        warn!(peer = %peer_addr, error = %e, "Failed to send ACK");
                        break;
                    }
                    events_in_window = 0;
                }
            }

            Frame::Compressed { data } => {
                let inner_frames = match decompress_and_parse(&data) {
                    Ok(frames) => frames,
                    Err(e) => {
                        warn!(peer = %peer_addr, error = %e, "Failed to decompress Lumberjack frame");
                        break;
                    }
                };

                for inner in inner_frames {
                    match inner {
                        Frame::Window { size } => {
                            window_size = size;
                            events_in_window = 0;
                        }
                        Frame::JsonData { sequence, payload } => {
                            last_sequence = sequence;
                            events_in_window += 1;

                            metrics.inc_requests_total("lumberjack");
                            metrics.add_bytes_received("lumberjack", payload.len() as u64);

                            if let Err(e) = pipeline.process(payload).await {
                                if scalo::logger::log_sampled(&LUMBERJACK_ERRORS, 100) {
                                    let total = LUMBERJACK_ERRORS.load(Ordering::Relaxed);
                                    warn!(peer = %peer_addr, seq = sequence, error = %e, total_errors = total, "Lumberjack event error (1 in 100)");
                                }
                                metrics.inc_requests_error("lumberjack");
                            } else {
                                metrics.inc_requests_success("lumberjack");
                            }
                        }
                        Frame::Compressed { .. } => {
                            warn!(peer = %peer_addr, "Nested compressed frame rejected");
                            break;
                        }
                    }
                }

                // ACK after processing all events in compressed batch
                if last_sequence > 0 {
                    let ack = encode_ack(last_sequence);
                    let writer = reader.get_mut();
                    if let Err(e) = writer.write_all(&ack).await {
                        warn!(peer = %peer_addr, error = %e, "Failed to send ACK");
                        break;
                    }
                    events_in_window = 0;
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
        let ip_filter = IpFilter::from_config(&self.pipeline.config().server.ip_filter);

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
                    if !ip_filter.admits(peer_addr) {
                        drop(stream);
                        continue;
                    }

                    let pipeline = self.pipeline.clone();
                    let metrics = self.metrics.clone();
                    let conn_shutdown = shutdown.clone();

                    if let Some(ref acceptor) = tls_acceptor {
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            // TLS handshake with timeout
                            let tls_result = timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await;
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
                            handle_connection(tls_stream, pipeline, metrics, conn_shutdown, peer_addr).await;
                        });
                    } else {
                        tokio::spawn(async move {
                            handle_connection(stream, pipeline, metrics, conn_shutdown, peer_addr).await;
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
    use super::*;

    #[test]
    fn test_default_config() {
        let config = LumberjackConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.bind_address, "0.0.0.0:5044");
        assert!(!config.tls.enabled);
    }
}
