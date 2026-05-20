// Project:   dfe-receiver
// File:      src/server/flow/listener.rs
// Purpose:   Generic UDP flow listener (NetFlow/sFlow autosense)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Generic UDP flow listener. Owns optional `NetflowDecoder` + `SflowDecoder`.
//! Per-packet protocol dispatch via the 2-byte autosense (`flow/dispatch`).
//!
//! Defensive gates applied per packet, in order:
//!   1. IP filter
//!   2. Per-source rate limit (token bucket)
//!   3. Pre-decode wire-format autosense + length sanity
//!   4. Memory-pressure gate (state-transition logged)
//!
//! On Linux, drains the socket on `AsyncFd::readable()` events to amortise
//! readiness syscalls. True `recvmmsg` batching is deferred (see TODO below)
//! -- per-packet `recv_from` inside the readiness loop is functionally
//! equivalent at moderate pps and avoids `nix` API-surface fragility.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use bytes::Bytes;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::pipeline::PipelineState;
use crate::server::flow::config::{FlowListenerConfig, OutputMode};
use crate::server::flow::decoder::FlowDecoder;
use crate::server::flow::dispatch::{ProtocolKind, dispatch_protocol_kind, length_sanity_check};
use crate::server::flow::envelope::render_packet;
use crate::server::flow::metrics::FlowMetrics;
use crate::server::flow::rate_limit::PerSourceRateLimiter;
use crate::server::ip_filter::IpFilter;
use crate::server::netflow::decoder::NetflowDecoder;
use crate::server::sflow::decoder::SflowDecoder;

/// Generic UDP flow listener. Owns optional NetFlow and sFlow decoders; each
/// incoming packet is autosensed and routed to the right one.
pub struct UdpFlowListener {
    bind_addr: SocketAddr,
    netflow: Option<NetflowDecoder>,
    sflow: Option<SflowDecoder>,
    cfg: FlowListenerConfig,
    metrics: FlowMetrics,
    pipeline: Arc<PipelineState>,
    ip_filter: Arc<IpFilter>,
    rate_limiter: Option<Arc<PerSourceRateLimiter>>,
    pressure_state: Arc<AtomicBool>,
}

impl UdpFlowListener {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bind_addr: SocketAddr,
        netflow: Option<NetflowDecoder>,
        sflow: Option<SflowDecoder>,
        cfg: FlowListenerConfig,
        metrics: FlowMetrics,
        pipeline: Arc<PipelineState>,
        ip_filter: Arc<IpFilter>,
        rate_limiter: Option<Arc<PerSourceRateLimiter>>,
    ) -> Self {
        Self {
            bind_addr,
            netflow,
            sflow,
            cfg,
            metrics,
            pipeline,
            ip_filter,
            rate_limiter,
            pressure_state: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Drive the listener until `shutdown` fires.
    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        // Build the socket via socket2 so we can set SO_RCVBUF and
        // SO_REUSEPORT before binding.
        let socket = Socket::new(
            Domain::for_address(self.bind_addr),
            Type::DGRAM,
            Some(Protocol::UDP),
        )
        .map_err(|e| Error::Server(format!("flow socket create failed: {e}")))?;
        socket
            .set_recv_buffer_size(self.cfg.recv_buffer_bytes)
            .map_err(|e| Error::Server(format!("flow SO_RCVBUF failed: {e}")))?;
        let _ = socket.set_reuse_port(true); // best-effort; not all platforms
        socket
            .set_nonblocking(true)
            .map_err(|e| Error::Server(format!("flow set_nonblocking failed: {e}")))?;
        socket
            .bind(&self.bind_addr.into())
            .map_err(|e| Error::Server(format!("flow bind {} failed: {e}", self.bind_addr)))?;

        let std_sock: std::net::UdpSocket = socket.into();
        let udp = UdpSocket::from_std(std_sock)
            .map_err(|e| Error::Server(format!("flow UdpSocket::from_std failed: {e}")))?;

        tracing::info!(addr = %self.bind_addr, "flow listener started");

        #[cfg(target_os = "linux")]
        {
            self.run_linux_drain(udp, shutdown).await
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.run_recv_from(udp, shutdown).await
        }
    }

    /// Portable path: one `recv_from` per readiness, used on non-Linux.
    #[cfg(not(target_os = "linux"))]
    async fn run_recv_from(mut self, udp: UdpSocket, shutdown: CancellationToken) -> Result<()> {
        let mut buf = vec![0u8; 65_535];
        let mut json_buf: Vec<u8> = Vec::with_capacity(8192);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    tracing::info!(addr = %self.bind_addr, "flow listener shutdown");
                    return Ok(());
                }
                recv = udp.recv_from(&mut buf) => match recv {
                    Ok((n, src)) => {
                        let now = chrono::Utc::now().to_rfc3339();
                        self.process_packet(&buf[..n], src, &now, &mut json_buf).await;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "flow recv_from error");
                    }
                }
            }
        }
    }

    /// Linux path: drain the socket queue on each readiness event.
    ///
    /// TODO: wire `nix::sys::socket::recvmmsg` for batched syscall once load
    /// testing shows the per-packet recv_from is the bottleneck. The drain
    /// loop already amortises wakeups, so the gain only materialises at
    /// very high pps.
    #[cfg(target_os = "linux")]
    async fn run_linux_drain(mut self, udp: UdpSocket, shutdown: CancellationToken) -> Result<()> {
        let mut buf = vec![0u8; 65_535];
        let mut json_buf: Vec<u8> = Vec::with_capacity(8192);
        // Safety bound per readiness event to avoid starving other tasks.
        const DRAIN_BURST: usize = 256;

        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    tracing::info!(addr = %self.bind_addr, "flow listener shutdown");
                    return Ok(());
                }
                ready = udp.readable() => {
                    if let Err(e) = ready {
                        tracing::warn!(error = %e, "flow readable() error");
                        continue;
                    }
                    let now = chrono::Utc::now().to_rfc3339();
                    for _ in 0..DRAIN_BURST {
                        match udp.try_recv_from(&mut buf) {
                            Ok((n, src)) => {
                                self.process_packet(&buf[..n], src, &now, &mut json_buf).await;
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                            Err(e) => {
                                tracing::warn!(error = %e, "flow try_recv_from error");
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn process_packet(
        &mut self,
        data: &[u8],
        src: SocketAddr,
        now_rfc3339: &str,
        json_buf: &mut Vec<u8>,
    ) {
        self.metrics.recv_total.inc();
        self.metrics.recv_bytes_total.add(data.len() as u64);

        // Gate 1: IP filter.
        if !self.ip_filter.is_allowed(src.ip()) {
            self.metrics
                .drops_total
                .inc(&[("transport", "flow"), ("reason", "ip_filter")]);
            return;
        }

        // Gate 2: per-source rate limit.
        if let Some(rl) = &self.rate_limiter
            && !rl.try_acquire(src.ip())
        {
            let src_str = src.ip().to_string();
            self.metrics
                .rate_limited_total
                .inc(&[("transport", "flow"), ("src_ip", &src_str)]);
            return;
        }

        // Gate 3: pre-decode wire-format autosense + length sanity.
        let kind = dispatch_protocol_kind(data);
        match kind {
            ProtocolKind::TooShort => {
                self.metrics
                    .invalid_packet_total
                    .inc(&[("transport", "flow"), ("reason", "too_short")]);
                return;
            }
            ProtocolKind::Unknown => {
                self.metrics.unknown_version_total.inc();
                self.metrics
                    .invalid_packet_total
                    .inc(&[("transport", "flow"), ("reason", "unknown_version")]);
                return;
            }
            _ => {}
        }
        if !length_sanity_check(kind, data) {
            self.metrics.invalid_packet_total.inc(&[
                ("transport", flow_transport(kind)),
                ("reason", "length_overflow"),
            ]);
            return;
        }

        // Gate 4: memory pressure (state-transition log).
        let under_pressure = self.pipeline.memory_guard().under_pressure();
        if hyperi_rustlib::logger::log_state_change(&self.pressure_state, under_pressure) {
            if under_pressure {
                tracing::warn!(addr = %self.bind_addr, "flow listener under memory pressure");
            } else {
                tracing::info!(addr = %self.bind_addr, "flow listener memory pressure recovered");
            }
        }
        if under_pressure {
            self.metrics.drops_total.inc(&[
                ("transport", flow_transport(kind)),
                ("reason", "memory_pressure"),
            ]);
            return;
        }

        // Dispatch + decode + emit.
        if kind.is_sflow() {
            if let Some(d) = self.sflow.as_mut() {
                Self::decode_and_emit::<SflowDecoder>(
                    d,
                    data,
                    src,
                    kind,
                    now_rfc3339,
                    json_buf,
                    &self.cfg,
                    &self.metrics,
                    &self.pipeline,
                )
                .await;
            } else {
                self.metrics
                    .drops_total
                    .inc(&[("transport", "sflow"), ("reason", "decoder_disabled")]);
            }
        } else if kind.is_netflow_family() {
            if let Some(d) = self.netflow.as_mut() {
                Self::decode_and_emit::<NetflowDecoder>(
                    d,
                    data,
                    src,
                    kind,
                    now_rfc3339,
                    json_buf,
                    &self.cfg,
                    &self.metrics,
                    &self.pipeline,
                )
                .await;
            } else {
                self.metrics
                    .drops_total
                    .inc(&[("transport", "netflow"), ("reason", "decoder_disabled")]);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn decode_and_emit<D: FlowDecoder>(
        decoder: &mut D,
        data: &[u8],
        src: SocketAddr,
        _kind: ProtocolKind,
        now_rfc3339: &str,
        json_buf: &mut Vec<u8>,
        cfg: &FlowListenerConfig,
        metrics: &FlowMetrics,
        pipeline: &Arc<PipelineState>,
    ) {
        let decoded = match decoder.decode(data, src.ip(), _kind) {
            Ok(d) => d,
            Err(e) if D::is_template_miss(&e) => {
                metrics
                    .decode_err_total
                    .inc(&[("transport", D::PROTOCOL), ("reason", "template_miss")]);
                return;
            }
            Err(_e) => {
                metrics
                    .decode_err_total
                    .inc(&[("transport", D::PROTOCOL), ("reason", "parse_err")]);
                return;
            }
        };

        if decoded.records.len() > cfg.output.max_records_per_packet {
            metrics
                .invalid_packet_total
                .inc(&[("transport", D::PROTOCOL), ("reason", "oversized")]);
            return;
        }

        metrics
            .records_per_packet
            .observe(decoded.records.len() as f64);

        let ranges = match render_packet::<D>(&decoded, cfg.output.mode, now_rfc3339, json_buf) {
            Ok(r) => r,
            Err(_) => {
                metrics
                    .decode_err_total
                    .inc(&[("transport", D::PROTOCOL), ("reason", "render_err")]);
                return;
            }
        };

        let mode_label = cfg.output.mode.label();
        // For Canonical / CanonicalWithRaw modes ranges.len() == 1; for
        // Exploded it equals records.len() (one event per record).
        let is_exploded = matches!(cfg.output.mode, OutputMode::Exploded);
        for range in ranges {
            let bytes = Bytes::copy_from_slice(&json_buf[range]);
            match pipeline.process(bytes).await {
                Ok(()) => {
                    metrics
                        .records_emitted_total
                        .inc(&[("transport", D::PROTOCOL), ("mode", mode_label)]);
                }
                Err(_) => {
                    // Pipeline buffer is full / backpressured / DLQ-routed.
                    metrics
                        .drops_total
                        .inc(&[("transport", D::PROTOCOL), ("reason", "channel_full")]);
                    if is_exploded {
                        // Stop emitting the rest of this packet's records to
                        // avoid partial bursts saturating the pipeline.
                        break;
                    }
                    return;
                }
            }
        }
    }
}

#[inline]
fn flow_transport(kind: ProtocolKind) -> &'static str {
    if kind.is_sflow() {
        "sflow"
    } else if kind.is_netflow_family() {
        "netflow"
    } else {
        "flow"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_transport_label_strings() {
        assert_eq!(flow_transport(ProtocolKind::SflowV5), "sflow");
        assert_eq!(flow_transport(ProtocolKind::NetflowV5), "netflow");
        assert_eq!(flow_transport(ProtocolKind::Ipfix), "netflow");
        assert_eq!(flow_transport(ProtocolKind::Unknown), "flow");
        assert_eq!(flow_transport(ProtocolKind::TooShort), "flow");
    }
}
