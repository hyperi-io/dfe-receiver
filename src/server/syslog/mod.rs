// Project:   dfe-receiver
// File:      src/server/syslog/mod.rs
// Purpose:   Syslog protocol handler (UDP + TCP + TLS/TCP)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Syslog protocol handler.
//!
//! Accepts syslog messages over UDP, TCP, and TLS/TCP.
//! Auto-detects RFC 5424 vs RFC 3164 format per message.
//!
//! Standard ports: 514 (UDP/TCP), 6514 (TLS/TCP per RFC 5425)

pub mod convert;
pub mod framing;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UdpSocket};
use tokio::time::timeout;
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Debounced timestamp for syslog UDP recv error warnings (1 per 5s).
static SYSLOG_UDP_WARN: AtomicU64 = AtomicU64::new(0);

use crate::config::{RawCapture, SyslogConfig};
use crate::error::{Error, Result};
use crate::metrics::{DropReason, Metrics};
use crate::pipeline::{Acks, PipelineState};
use crate::server::hold::hold_until_settled;
use crate::server::http::Accept;
use crate::server::ip_filter::IpFilter;
use crate::server::traits::{BoundAddr, Listeners, ProtocolHandler};
use convert::syslog_to_json;
use framing::SyslogFrameDecoder;

/// The transport label every syslog listener counts under.
const TRANSPORT: &str = "syslog";

/// TLS handshake timeout.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum UDP datagram buffer size.
const UDP_BUF_SIZE: usize = 65536;

// ---------------------------------------------------------------------------
// UDP handler
// ---------------------------------------------------------------------------

/// Run the UDP syslog listener.
async fn run_udp(
    bind_addr: SocketAddr,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    raw_capture: RawCapture,
    ip_filter: IpFilter,
    bound: BoundAddr,
) -> Result<()> {
    let socket = UdpSocket::bind(bind_addr)
        .await
        .map_err(|e| Error::Server(format!("failed to bind syslog UDP socket: {e}")))?;
    let _serving = bound.publish(&socket.local_addr());

    info!(addr = %bind_addr, protocol = "udp", "Syslog UDP listener started");

    let mut buf = vec![0u8; UDP_BUF_SIZE];

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Syslog UDP listener stopping");
                break;
            }
            result = socket.recv_from(&mut buf) => {
                let (len, peer_addr) = match result {
                    Ok(r) => r,
                    Err(e) => {
                        if scalo::logger::log_debounced(&SYSLOG_UDP_WARN, 5000) {
                            warn!(error = %e, "Syslog UDP recv error (throttled to 1/5s)");
                        }
                        continue;
                    }
                };

                // UDP has no connection to reject, so the filter runs per
                // datagram -- before the payload is read.
                if !ip_filter.admits(peer_addr, TRANSPORT, &metrics) {
                    continue;
                }

                metrics.inc_requests_total("syslog");
                metrics.add_bytes_received("syslog", len as u64);

                let raw = match std::str::from_utf8(&buf[..len]) {
                    Ok(s) => s,
                    Err(e) => {
                        debug!(peer = %peer_addr, error = %e, "Syslog UDP: invalid UTF-8");
                        metrics.inc_parse_failure("syslog");
                        metrics.inc_requests_error("syslog");
                        continue;
                    }
                };

                match syslog_to_json(raw, raw_capture) {
                    Ok(payload) => {
                        if let Err(e) = pipeline.process(payload).await {
                            debug!(peer = %peer_addr, error = %e, "Failed to process syslog UDP event");
                            metrics.inc_requests_error("syslog");
                            // A datagram has no answer to carry a retry, so the record is gone.
                            let reason = if e.is_retryable() {
                                DropReason::Unavailable
                            } else {
                                DropReason::Rejected
                            };
                            metrics.add_records_dropped("syslog", reason, 1);
                        } else {
                            metrics.inc_requests_success("syslog");
                        }
                    }
                    Err(e) => {
                        debug!(peer = %peer_addr, error = %e, "Syslog UDP parse error");
                        metrics.inc_parse_failure("syslog");
                        metrics.inc_requests_error("syslog");
                    }
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// TCP per-connection handler
// ---------------------------------------------------------------------------

/// Handle a single TCP syslog connection using the framing codec.
///
/// A frame the pipeline cannot take is held, and the socket is not read,
/// until it can: RFC 6587 has no acknowledgement to ask the sender to retry.
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

    let mut framed = FramedRead::new(stream, SyslogFrameDecoder::new(max_message_size));

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                debug!(peer = %peer_addr, "Syslog TCP connection closing (shutdown)");
                break;
            }
            result = StreamExt::next(&mut framed) => {
                match result {
                    Some(Ok(raw)) => {
                        metrics.inc_requests_total("syslog");
                        metrics.add_bytes_received("syslog", raw.len() as u64);

                        match syslog_to_json(&raw, raw_capture) {
                            Ok(payload) => {
                                let held = [payload];
                                if !hold_until_settled(&pipeline, &held, &metrics, "syslog", &shutdown, &Acks::at_enqueue()).await {
                                    debug!(peer = %peer_addr, "Syslog TCP connection closing (shutdown during a hold)");
                                    break;
                                }
                            }
                            Err(e) => {
                                debug!(peer = %peer_addr, error = %e, "Syslog TCP parse error");
                                metrics.inc_parse_failure("syslog");
                                metrics.inc_requests_error("syslog");
                            }
                        }
                    }
                    Some(Err(e)) => {
                        warn!(peer = %peer_addr, error = %e, "Syslog TCP framing error");
                        break;
                    }
                    None => {
                        debug!(peer = %peer_addr, "Syslog TCP client disconnected");
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

/// Run the TCP syslog listener (plain or TLS).
#[allow(clippy::too_many_arguments)]
async fn run_tcp(
    bind_addr: SocketAddr,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    max_message_size: usize,
    raw_capture: RawCapture,
    label: &str,
    ip_filter: IpFilter,
    bound: BoundAddr,
) -> Result<()> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| Error::Server(format!("failed to bind syslog {label} listener: {e}")))?;
    let _serving = bound.publish(&listener.local_addr());

    let tls_enabled = tls_acceptor.is_some();
    info!(addr = %bind_addr, tls = tls_enabled, "Syslog {label} listener started");

    let accept = Accept {
        ip_filter,
        metrics: metrics.clone(),
        transport: TRANSPORT,
    };

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Syslog {label} listener stopping");
                break;
            }
            result = listener.accept() => {
                let (stream, peer_addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!(error = %e, "Failed to accept syslog {label} connection");
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

                if let Some(ref acceptor) = tls_acceptor {
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        let _open = open;
                        let tls_result = timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await;
                        let tls_stream = match tls_result {
                            Ok(Ok(s)) => s,
                            Ok(Err(e)) => {
                                metrics.inc_tls_handshake_failure();
                                debug!(peer = %peer_addr, error = %e, "Syslog TLS handshake failed");
                                return;
                            }
                            Err(_) => {
                                metrics.inc_tls_handshake_failure();
                                warn!(peer = %peer_addr, "Syslog TLS handshake timeout");
                                return;
                            }
                        };

                        debug!(peer = %peer_addr, "Syslog TLS connection established");
                        handle_tcp_connection(
                            tls_stream, pipeline, metrics, conn_shutdown, peer_addr,
                            max_message_size, raw_capture,
                        ).await;
                    });
                } else {
                    tokio::spawn(async move {
                        let _open = open;
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

/// Syslog protocol handler.
pub struct SyslogHandler {
    config: SyslogConfig,
    /// Raw capture already resolved against the common `raw_capture` block.
    raw_capture: RawCapture,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    udp_bound: BoundAddr,
    tcp_bound: BoundAddr,
    tls_bound: BoundAddr,
}

impl SyslogHandler {
    pub fn new(
        config: SyslogConfig,
        raw_capture: RawCapture,
        pipeline: Arc<PipelineState>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            config,
            raw_capture,
            pipeline,
            metrics,
            udp_bound: BoundAddr::default(),
            tcp_bound: BoundAddr::default(),
            tls_bound: BoundAddr::default(),
        }
    }

    /// The address the UDP listener bound, once [`ProtocolHandler::start`] binds it.
    #[must_use]
    pub fn udp_bound_addr(&self) -> BoundAddr {
        self.udp_bound.clone()
    }

    /// The address the plain TCP listener bound, once [`ProtocolHandler::start`] binds it.
    #[must_use]
    pub fn tcp_bound_addr(&self) -> BoundAddr {
        self.tcp_bound.clone()
    }

    /// The address the TLS listener bound, once [`ProtocolHandler::start`] binds it.
    #[must_use]
    pub fn tls_bound_addr(&self) -> BoundAddr {
        self.tls_bound.clone()
    }
}

#[async_trait::async_trait]
impl ProtocolHandler for SyslogHandler {
    fn name(&self) -> &'static str {
        "syslog"
    }

    fn bind_address(&self) -> &str {
        &self.config.tcp_bind_address
    }

    fn listeners(&self) -> Vec<BoundAddr> {
        let mut listeners = vec![self.udp_bound.clone(), self.tcp_bound.clone()];
        if self.config.tls.enabled {
            listeners.push(self.tls_bound.clone());
        }
        listeners
    }

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        // Settle what can fail before any listener binds, so a failed start leaves none serving.
        let udp_addr: SocketAddr = self
            .config
            .udp_bind_address
            .parse()
            .map_err(|e| Error::Config(format!("invalid syslog UDP bind address: {e}")))?;

        let tcp_addr: SocketAddr = self
            .config
            .tcp_bind_address
            .parse()
            .map_err(|e| Error::Config(format!("invalid syslog TCP bind address: {e}")))?;

        let tls = if self.config.tls.enabled {
            let tls_addr: SocketAddr = self
                .config
                .tls_bind_address
                .parse()
                .map_err(|e| Error::Config(format!("invalid syslog TLS bind address: {e}")))?;
            // The config asks for a TLS listener, so no acceptor fails the start.
            let acceptor = super::tls::build_tls_acceptor_async(&self.config.tls)
                .await?
                .ok_or_else(|| {
                    Error::Tls("syslog TLS is enabled but no TLS acceptor was built".into())
                })?;
            Some((tls_addr, acceptor))
        } else {
            None
        };

        let max_msg = self.config.max_message_size;
        let raw_capture = self.raw_capture;

        // A syslog line carries no credential, and UDP and plain TCP have no
        // handshake either, so the IP filter is the only admission control on
        // two of these three listeners.
        let ip_filter = IpFilter::from_config(&self.pipeline.config().server.ip_filter);

        let mut listeners = Listeners::default();
        listeners.spawn(
            "syslog UDP",
            run_udp(
                udp_addr,
                self.pipeline.clone(),
                self.metrics.clone(),
                shutdown.clone(),
                raw_capture,
                ip_filter.clone(),
                self.udp_bound.clone(),
            ),
        );
        listeners.spawn(
            "syslog TCP",
            run_tcp(
                tcp_addr,
                self.pipeline.clone(),
                self.metrics.clone(),
                shutdown.clone(),
                None,
                max_msg,
                raw_capture,
                "TCP",
                ip_filter.clone(),
                self.tcp_bound.clone(),
            ),
        );
        if let Some((tls_addr, acceptor)) = tls {
            listeners.spawn(
                "syslog TLS",
                run_tcp(
                    tls_addr,
                    self.pipeline.clone(),
                    self.metrics.clone(),
                    shutdown.clone(),
                    Some(acceptor),
                    max_msg,
                    raw_capture,
                    "TLS",
                    ip_filter,
                    self.tls_bound.clone(),
                ),
            );
        }

        listeners.run(&shutdown).await?;

        info!("Syslog server stopped");
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
        let config = SyslogConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.udp_bind_address, "0.0.0.0:514");
        assert_eq!(config.tcp_bind_address, "0.0.0.0:514");
        assert_eq!(config.tls_bind_address, "0.0.0.0:6514");
        assert_eq!(config.max_message_size, 64 * 1024);
        assert!(!config.tls.enabled);
    }

    /// A datagram the pipeline cannot take is counted as dropped: UDP has no
    /// answer to carry a retry, so the counter is the only trace of it.
    #[tokio::test]
    async fn a_datagram_the_pipeline_cannot_take_is_counted_as_dropped() {
        use scalo::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};

        use crate::config::{Config, SharedConfig};

        let mut config = Config::default();
        config.destinations.default = "loader".into();
        config.loader.transport = "memory".to_string();
        let guard = MemoryGuard::with_usage_source(
            MemoryGuardConfig {
                limit_bytes: 1_000_000,
                pressure_threshold: 0.8,
                ..Default::default()
            },
            UsageSource::Reservations,
        );
        guard.add_bytes(1_000_000);
        let pipeline = Arc::new(
            PipelineState::with_governor(
                SharedConfig::new(config.clone()),
                CancellationToken::new(),
                None,
                Some(Arc::new(guard)),
            )
            .await
            .unwrap(),
        );
        let metrics = Arc::new(Metrics::default());
        let bound = BoundAddr::default();
        let shutdown = CancellationToken::new();
        let listener = tokio::spawn(run_udp(
            "127.0.0.1:0".parse().unwrap(),
            pipeline,
            Arc::clone(&metrics),
            shutdown.clone(),
            RawCapture::OFF,
            IpFilter::from_config(&config.server.ip_filter),
            bound.clone(),
        ));
        let addr = timeout(Duration::from_secs(5), bound.wait()).await.unwrap();

        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        sender
            .send_to(b"<14>Sep 25 10:00:00 host app: not taken", addr)
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while metrics.get_records_dropped() == 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        shutdown.cancel();
        listener.await.unwrap().unwrap();

        assert_eq!(metrics.get_records_dropped(), 1);
        assert_eq!(metrics.get_requests_success(), 0);
    }
}
