// Project:   dfe-receiver
// File:      src/server/flow/listener.rs
// Purpose:   Generic UDP flow listener (NetFlow/sFlow autosense)
// Language:  Rust
//
// License:   BUSL-1.1
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
        if scalo::logger::log_state_change(&self.pressure_state, under_pressure) {
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
        kind: ProtocolKind,
        now_rfc3339: &str,
        json_buf: &mut Vec<u8>,
        cfg: &FlowListenerConfig,
        metrics: &FlowMetrics,
        pipeline: &Arc<PipelineState>,
    ) {
        let packet = match decoder.decode(data, src.ip(), kind) {
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

        if packet.records.len() > cfg.output.max_records_per_packet {
            metrics
                .invalid_packet_total
                .inc(&[("transport", D::PROTOCOL), ("reason", "oversized")]);
            return;
        }

        metrics
            .records_per_packet
            .observe(packet.records.len() as f64);

        // Template-only / options-template packets (NetFlow v9/IPFIX) and
        // empty sFlow datagrams decode successfully but carry zero data
        // records. Emitting an empty `flows:[]` envelope would be pure
        // downstream noise -- a v9 exporter refreshes templates periodically,
        // so this would fire on every refresh. Skip emission; the zero is
        // still recorded in `records_per_packet` above for observability.
        // (Exploded mode already skips via its empty-ranges guard; this makes
        // canonical and canonical_with_raw consistent.)
        if packet.records.is_empty() {
            return;
        }

        let ranges = match render_packet::<D>(&packet, cfg.output.mode, now_rfc3339, json_buf) {
            Ok(r) => r,
            Err(_) => {
                metrics
                    .decode_err_total
                    .inc(&[("transport", D::PROTOCOL), ("reason", "render_err")]);
                return;
            }
        };

        let mode_label = cfg.output.mode.label();
        let is_exploded = matches!(cfg.output.mode, OutputMode::Exploded);

        if is_exploded {
            // All-or-nothing per packet: pre-clone every rendered envelope
            // into its own `Bytes` before any send so a mid-batch failure
            // cannot leave the buffer pointing at half-consumed ranges. The
            // first send proves the pipeline has capacity; if it fails we
            // drop the WHOLE packet (no event leaves the listener). If a
            // later send fails the remaining records are charged to
            // `drops_total{reason="channel_full"}` and emission stops --
            // some events from this packet will already have been emitted
            // in that case, but we never start emission unless the first
            // event succeeds.
            let total = ranges.len();
            if total == 0 {
                return;
            }
            let events: Vec<Bytes> = ranges
                .into_iter()
                .map(|r| Bytes::copy_from_slice(&json_buf[r]))
                .collect();

            // Probe with the first event. If it fails, charge ALL N to drops
            // and return without partial emission.
            let mut iter = events.into_iter();
            let first = iter.next().unwrap_or_else(|| {
                unreachable!("events is non-empty (proven by len check at line 366)")
            });
            match pipeline.process(first).await {
                Ok(()) => {
                    metrics
                        .records_emitted_total
                        .inc(&[("transport", D::PROTOCOL), ("mode", mode_label)]);
                }
                Err(_) => {
                    for _ in 0..total {
                        metrics
                            .drops_total
                            .inc(&[("transport", D::PROTOCOL), ("reason", "channel_full")]);
                    }
                    return;
                }
            }
            // Pipeline accepted the first event; emit the rest. A later
            // failure stops emission and charges the remaining tail to
            // drops_total{reason="channel_full"}.
            let mut emitted = 1usize;
            for ev in iter {
                emitted += 1;
                match pipeline.process(ev).await {
                    Ok(()) => {
                        metrics
                            .records_emitted_total
                            .inc(&[("transport", D::PROTOCOL), ("mode", mode_label)]);
                    }
                    Err(_) => {
                        let remaining = total - emitted + 1;
                        for _ in 0..remaining {
                            metrics
                                .drops_total
                                .inc(&[("transport", D::PROTOCOL), ("reason", "channel_full")]);
                        }
                        return;
                    }
                }
            }
            return;
        }

        // Canonical / CanonicalWithRaw modes: ranges.len() == 1 so the
        // original simple loop is safe -- no partial-emit hazard.
        for range in ranges {
            let bytes = Bytes::copy_from_slice(&json_buf[range]);
            match pipeline.process(bytes).await {
                Ok(()) => {
                    metrics
                        .records_emitted_total
                        .inc(&[("transport", D::PROTOCOL), ("mode", mode_label)]);
                }
                Err(_) => {
                    metrics
                        .drops_total
                        .inc(&[("transport", D::PROTOCOL), ("reason", "channel_full")]);
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
