// Project:   dfe-receiver
// File:      src/server/fluent/mod.rs
// Purpose:   Fluent Forward protocol handler (msgpack over TCP)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Fluent Forward protocol handler.
//!
//! Accepts data from Fluentd and Fluent Bit agents over the Forward
//! protocol (msgpack over TCP). Supports Message, Forward, and
//! PackedForward modes.
//!
//! Standard port: 24224

pub mod convert;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::{FluentConfig, RawCapture};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::ip_filter::IpFilter;
use crate::server::traits::{BoundAddr, ProtocolHandler};
use convert::{extract_chunk_id, fluent_to_json};

/// TLS handshake timeout.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// TCP per-connection handler
// ---------------------------------------------------------------------------

/// Handle a single Fluent Forward TCP connection.
///
/// Reads msgpack values from the stream, converts them to JSON, and
/// processes through the pipeline. Sends ACK responses when the client
/// includes a `chunk` option.
async fn handle_tcp_connection<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    peer_addr: SocketAddr,
    max_buffer_size: usize,
    raw_capture: RawCapture,
) {
    use tokio::io::AsyncReadExt;

    let mut buf = vec![0u8; 64 * 1024];
    let mut pending = Vec::new();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                debug!(peer = %peer_addr, "Fluent Forward connection closing (shutdown)");
                break;
            }
            result = stream.read(&mut buf) => {
                let n = match result {
                    Ok(0) => {
                        debug!(peer = %peer_addr, "Fluent Forward client disconnected");
                        break;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        warn!(peer = %peer_addr, error = %e, "Fluent Forward read error");
                        break;
                    }
                };

                pending.extend_from_slice(&buf[..n]);
                metrics.add_bytes_received("fluent", n as u64);

                // Guard against unbounded buffer growth (OOM prevention)
                if pending.len() > max_buffer_size {
                    warn!(
                        peer = %peer_addr,
                        size = pending.len(),
                        limit = max_buffer_size,
                        "Fluent Forward buffer exceeded max size, closing connection"
                    );
                    break;
                }

                // Try to decode complete msgpack values from the buffer
                loop {
                    let mut cursor = &pending[..];
                    let start_len = cursor.len();

                    let msg = match rmpv::decode::read_value(&mut cursor) {
                        Ok(val) => val,
                        Err(_) => break, // incomplete data, wait for more
                    };

                    let consumed = start_len - cursor.len();
                    metrics.inc_requests_total("fluent");

                    // Check for chunk ACK before processing
                    let chunk_id = extract_chunk_id(&msg);

                    match fluent_to_json(&msg, raw_capture) {
                        Ok(payloads) => {
                            let (success, first_err) = pipeline.process_batch(&payloads).await;
                            if first_err.is_some() {
                                let failed = payloads.len() - success;
                                debug!(peer = %peer_addr, success = success, failed = failed, "Fluent batch partially failed");
                                metrics.inc_requests_error("fluent");
                            } else {
                                metrics.inc_requests_success("fluent");
                            }
                        }
                        Err(e) => {
                            debug!(peer = %peer_addr, error = %e, "Fluent Forward parse error");
                            metrics.inc_parse_failure("fluent");
                            metrics.inc_requests_error("fluent");
                        }
                    }

                    // Send ACK response if chunk ID was provided
                    if let Some(ref chunk) = chunk_id {
                        let ack = rmpv::Value::Map(vec![(
                            rmpv::Value::String("ack".into()),
                            rmpv::Value::String(chunk.clone().into()),
                        )]);
                        let mut ack_buf = Vec::new();
                        if rmpv::encode::write_value(&mut ack_buf, &ack).is_ok()
                            && let Err(e) = stream.write_all(&ack_buf).await
                        {
                            debug!(peer = %peer_addr, error = %e, "Failed to send Fluent ACK");
                        }
                    }

                    // Remove consumed bytes
                    pending.drain(..consumed);
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
    max_buffer_size: usize,
    raw_capture: RawCapture,
    ip_filter: IpFilter,
    bound: BoundAddr,
) -> Result<()> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| Error::Server(format!("failed to bind Fluent Forward listener: {e}")))?;
    let _serving = bound.publish(&listener.local_addr());

    let tls_enabled = tls_acceptor.is_some();
    info!(addr = %bind_addr, tls = tls_enabled, "Fluent Forward listener started");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Fluent Forward listener stopping");
                break;
            }
            result = listener.accept() => {
                let (stream, peer_addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!(error = %e, "Failed to accept Fluent Forward connection");
                        continue;
                    }
                };

                // Reject before the TLS handshake and before any msgpack is read.
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
                                debug!(peer = %peer_addr, error = %e, "Fluent Forward TLS handshake failed");
                                return;
                            }
                            Err(_) => {
                                metrics.inc_tls_handshake_failure();
                                warn!(peer = %peer_addr, "Fluent Forward TLS handshake timeout");
                                return;
                            }
                        };

                        handle_tcp_connection(
                            tls_stream, pipeline, metrics, conn_shutdown, peer_addr,
                            max_buffer_size, raw_capture,
                        ).await;
                    });
                } else {
                    tokio::spawn(async move {
                        handle_tcp_connection(
                            stream, pipeline, metrics, conn_shutdown, peer_addr,
                            max_buffer_size, raw_capture,
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

/// Fluent Forward protocol handler.
pub struct FluentHandler {
    config: FluentConfig,
    /// Raw capture already resolved against the common `raw_capture` block.
    raw_capture: RawCapture,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    bound: BoundAddr,
}

impl FluentHandler {
    pub fn new(
        config: FluentConfig,
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
impl ProtocolHandler for FluentHandler {
    fn name(&self) -> &'static str {
        "fluent-forward"
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
            .map_err(|e| Error::Config(format!("invalid Fluent Forward bind address: {e}")))?;

        // Build TLS acceptor if enabled
        let tls_acceptor = if self.config.tls.enabled {
            super::tls::build_tls_acceptor_async(&self.config.tls).await?
        } else {
            None
        };

        // Forward carries no credential, so the IP filter and the TLS
        // handshake are the whole admission surface on this port.
        let ip_filter = IpFilter::from_config(&self.pipeline.config().server.ip_filter);

        run_tcp(
            addr,
            self.pipeline.clone(),
            self.metrics.clone(),
            shutdown,
            tls_acceptor,
            self.config.max_message_size,
            self.raw_capture,
            ip_filter,
            self.bound.clone(),
        )
        .await?;

        info!("Fluent Forward server stopped");
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
        let config = FluentConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.bind_address, "0.0.0.0:24224");
        assert_eq!(config.max_message_size, 32 * 1024 * 1024);
        assert!(!config.tls.enabled);
    }
}
