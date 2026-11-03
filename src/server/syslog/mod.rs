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
//! Auto-detects RFC 5424 vs RFC 3164 format per message. The listeners live in
//! `udp` and `tcp`, the parse in [`convert`] and the TCP framing in [`framing`].
//!
//! Standard ports: 514 (UDP/TCP), 6514 (TLS/TCP per RFC 5425)

pub mod convert;
pub mod framing;
mod tcp;
mod udp;

use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::{RawCapture, SyslogConfig};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::ip_filter::IpFilter;
use crate::server::traits::{BoundAddr, Listeners, ProtocolHandler};
use crate::tenancy::{Ingest, Principal};
use convert::Converter;
use tcp::{Connection, TcpListenerTask};
use udp::UdpListener;

/// The transport label every syslog listener counts under.
const TRANSPORT: &str = "syslog";

/// A syslog line carries no credential, and the listener writes its `_source`.
fn listener_ingest() -> Ingest {
    Ingest::built(Principal::Listener(TRANSPORT), convert::SOURCE)
}

/// Decode a message as text. RFC 3164 bodies are arbitrary bytes and RFC 5424
/// promises UTF-8 only behind a BOM, so a message that is not valid UTF-8 is
/// kept with each invalid sequence replaced by U+FFFD and counted. Valid input
/// is borrowed, not copied.
fn decode_lossy<'a>(bytes: &'a [u8], metrics: &Metrics) -> Cow<'a, str> {
    let text = String::from_utf8_lossy(bytes);
    if matches!(text, Cow::Owned(_)) {
        metrics.inc_lossy_decode(TRANSPORT);
    }
    text
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

        let cfg = &self.config;
        let converter = Converter::new(cfg.timezone.fixed(), self.raw_capture);
        let seconds = |s: u64| (s > 0).then(|| Duration::from_secs(s));
        let millis = |ms: u64| (ms > 0).then(|| Duration::from_millis(ms));
        let connection = Connection {
            converter,
            max_message_size: cfg.max_message_size,
            max_frame_size: cfg.max_frame_size,
            keepalive: seconds(cfg.tcp_keepalive_secs),
            idle: millis(cfg.idle_timeout_ms),
        };

        // A syslog line carries no credential, and UDP and plain TCP have no
        // handshake either, so the IP filter is the only admission control on
        // two of these three listeners.
        let ip_filter = IpFilter::from_config(&self.pipeline.config().server.ip_filter);
        let tcp_listener = |bind_addr, tls, label, bound| TcpListenerTask {
            bind_addr,
            pipeline: self.pipeline.clone(),
            metrics: self.metrics.clone(),
            tls,
            connection,
            label,
            ip_filter: ip_filter.clone(),
            bound,
        };

        let mut listeners = Listeners::default();
        let udp = UdpListener {
            bind_addr: udp_addr,
            pipeline: self.pipeline.clone(),
            metrics: self.metrics.clone(),
            converter,
            ip_filter: ip_filter.clone(),
            bound: self.udp_bound.clone(),
            recv_buffer_size: cfg.udp_recv_buffer_size,
            batch_size: cfg.udp_batch_size,
        };
        listeners.spawn("syslog UDP", udp.run(shutdown.clone()));
        let plain = tcp_listener(tcp_addr, None, "TCP", self.tcp_bound.clone());
        listeners.spawn("syslog TCP", plain.run(shutdown.clone()));
        if let Some((tls_addr, acceptor)) = tls {
            let secure = tcp_listener(tls_addr, Some(acceptor), "TLS", self.tls_bound.clone());
            listeners.spawn("syslog TLS", secure.run(shutdown.clone()));
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
    use chrono::Offset;

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

    /// Valid UTF-8 is borrowed and not counted. Invalid bytes become U+FFFD
    /// and the message is counted once, wherever the bad byte sits.
    #[test]
    fn decode_lossy_borrows_valid_input_and_counts_invalid_input() {
        let metrics = Metrics::default();

        let valid = decode_lossy("<14>caf\u{e9}".as_bytes(), &metrics);
        assert!(matches!(valid, Cow::Borrowed(_)));
        assert_eq!(metrics.get_lossy_decodes_total(), 0);

        for (bytes, expected) in [
            (&b"<14>caf\xe9"[..], "<14>caf\u{fffd}"),
            (
                &b"<14>h\xffst app: x\xc3"[..],
                "<14>h\u{fffd}st app: x\u{fffd}",
            ),
            (&b"\xff"[..], "\u{fffd}"),
        ] {
            let before = metrics.get_lossy_decodes_total();
            assert_eq!(decode_lossy(bytes, &metrics), expected);
            assert_eq!(metrics.get_lossy_decodes_total(), before + 1);
        }
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
        let udp = UdpListener {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            pipeline,
            metrics: Arc::clone(&metrics),
            converter: Converter::new(chrono::Utc.fix(), RawCapture::OFF),
            ip_filter: IpFilter::from_config(&config.server.ip_filter),
            bound: bound.clone(),
            recv_buffer_size: 256 * 1024,
            batch_size: 64,
        };
        let listener = tokio::spawn(udp.run(shutdown.clone()));
        let addr = tokio::time::timeout(Duration::from_secs(5), bound.wait())
            .await
            .unwrap();

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
