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

use crate::config::RawCapture;
use crate::error::{Error, Result};
use crate::pipeline::{BatchOutcome, PipelineState};
use crate::server::flow::config::FlowListenerConfig;
use crate::server::flow::decoder::FlowDecoder;
use crate::server::flow::dispatch::{ProtocolKind, dispatch_protocol_kind, length_sanity_check};
use crate::server::flow::envelope::render_packet;
use crate::server::flow::metrics::FlowMetrics;
use crate::server::flow::rate_limit::PerSourceRateLimiter;
use crate::server::ip_filter::IpFilter;
use crate::server::netflow::decoder::NetflowDecoder;
use crate::server::sflow::decoder::SflowDecoder;
use crate::server::traits::BoundAddr;
use crate::tenancy::{Ingest, Principal};

/// Generic UDP flow listener. Owns optional NetFlow and sFlow decoders; each
/// incoming packet is autosensed and routed to the right one.
pub struct UdpFlowListener {
    bind_addr: SocketAddr,
    netflow: Option<NetflowDecoder>,
    sflow: Option<SflowDecoder>,
    cfg: FlowListenerConfig,
    /// Resolved against the common `raw_capture` block by the handler.
    raw: RawCapture,
    metrics: FlowMetrics,
    pipeline: Arc<PipelineState>,
    ip_filter: Arc<IpFilter>,
    rate_limiter: Option<Arc<PerSourceRateLimiter>>,
    pressure_state: Arc<AtomicBool>,
    bound: BoundAddr,
}

impl UdpFlowListener {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bind_addr: SocketAddr,
        mut netflow: Option<NetflowDecoder>,
        mut sflow: Option<SflowDecoder>,
        cfg: FlowListenerConfig,
        raw: RawCapture,
        metrics: FlowMetrics,
        pipeline: Arc<PipelineState>,
        ip_filter: Arc<IpFilter>,
        rate_limiter: Option<Arc<PerSourceRateLimiter>>,
    ) -> Self {
        // A record's verbatim form is built only where it is sent.
        if let Some(decoder) = netflow.as_mut() {
            decoder.capture_raw(raw.enabled);
        }
        if let Some(decoder) = sflow.as_mut() {
            decoder.capture_raw(raw.enabled);
        }
        Self {
            bind_addr,
            netflow,
            sflow,
            cfg,
            raw,
            metrics,
            pipeline,
            ip_filter,
            rate_limiter,
            pressure_state: Arc::new(AtomicBool::new(false)),
            bound: BoundAddr::default(),
        }
    }

    /// Publish the address this listener binds to `bound`.
    #[must_use]
    pub(crate) fn publishing_to(mut self, bound: BoundAddr) -> Self {
        self.bound = bound;
        self
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
        let _serving = self.bound.publish(&udp.local_addr());

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
        // No source address label: a spoofed UDP source would mint a series each.
        if let Some(rl) = &self.rate_limiter
            && !rl.try_acquire(src.ip())
        {
            self.metrics
                .rate_limited_total
                .inc(&[("transport", "flow")]);
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
                    self.raw,
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
                    self.raw,
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
        raw: RawCapture,
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
        // (Exploded mode already skips via its empty-ranges guard; this keeps
        // canonical consistent with it.)
        if packet.records.is_empty() {
            return;
        }

        let ranges = match render_packet::<D>(&packet, cfg.output.mode, raw, now_rfc3339, json_buf)
        {
            Ok(r) => r,
            Err(_) => {
                metrics
                    .decode_err_total
                    .inc(&[("transport", D::PROTOCOL), ("reason", "render_err")]);
                return;
            }
        };

        let mode_label = cfg.output.mode.label();
        // A flow packet carries no credential, and the envelope writes `_source`.
        let ingest = Ingest::built(Principal::Listener(D::PROTOCOL), D::PROTOCOL);

        // Each envelope gets its own `Bytes`: `json_buf` is reused by the next packet.
        let events: Vec<Bytes> = ranges
            .into_iter()
            .map(|r| Bytes::copy_from_slice(&json_buf[r]))
            .collect();
        let outcome = pipeline.process_batch(&events, &ingest).await;
        let (emitted, dropped) = packet_tally(events.len(), &outcome);
        if emitted > 0 {
            metrics
                .records_emitted_total
                .add(&[("transport", D::PROTOCOL), ("mode", mode_label)], emitted);
        }
        if dropped > 0 {
            metrics.drops_total.add(
                &[("transport", D::PROTOCOL), ("reason", "channel_full")],
                dropped,
            );
        }
    }
}

/// The records of a packet of `total` the pipeline took, and the rest.
///
/// The batch stops at the first retryable failure, so a packet whose first
/// record found no room emits nothing.
fn packet_tally(total: usize, outcome: &BatchOutcome) -> (u64, u64) {
    let emitted = outcome.accepted.min(total);
    (emitted as u64, (total - emitted) as u64)
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

    use std::sync::atomic::{AtomicU64, Ordering};

    use scalo::memory::UsageSource;

    use crate::buffer::{MemoryGuard, MemoryGuardConfig};
    use crate::config::{Config, DestinationSpec, GrpcDestination, SharedConfig};
    use crate::server::flow::metrics::mock::{MockLabelledCounter, flow_metrics_for_test};
    use crate::sink::grpc::test_destination::TestDestination;

    /// A NetFlow v5 datagram carrying `records` flows.
    fn v5_packet(records: u16) -> Vec<u8> {
        let mut packet = vec![0u8; 24];
        packet[0..2].copy_from_slice(&5u16.to_be_bytes());
        packet[2..4].copy_from_slice(&records.to_be_bytes());
        packet[8..12].copy_from_slice(&1_700_000_000u32.to_be_bytes());
        for flow in 0..records {
            let mut record = [0u8; 48];
            record[0..4].copy_from_slice(&[10, 0, 0, 1]);
            record[4..8].copy_from_slice(&[10, 0, 0, 2]);
            record[32..34].copy_from_slice(&(1024 + flow).to_be_bytes());
            record[38] = 6;
            packet.extend_from_slice(&record);
        }
        packet
    }

    /// A pipeline sending every record to `destination` alone, its memory
    /// guard counting only what the pipeline reserves against 1 MB.
    async fn pipeline_to(destination: &TestDestination) -> Arc<PipelineState> {
        let mut config = Config::default();
        config.buffer.memory_limit = 1_000_000;
        config.loader.transport = "memory".to_string();
        config.routing.dlq.file_path = "/dev/null/dlq".to_string();
        config.destinations.default = "alpha".into();
        config.destinations.named.insert(
            "alpha".to_string(),
            DestinationSpec {
                grpc: Some(GrpcDestination {
                    endpoint: destination.endpoint().to_string(),
                    ..GrpcDestination::default()
                }),
                kafka: None,
            },
        );
        let guard = MemoryGuard::with_usage_source(
            MemoryGuardConfig {
                limit_bytes: 1_000_000,
                pressure_threshold: config.buffer.pressure_threshold,
                ..Default::default()
            },
            UsageSource::Reservations,
        );
        let shared = SharedConfig::new(config);
        let state = PipelineState::with_governor(
            shared,
            CancellationToken::new(),
            None,
            Some(Arc::new(guard)),
        );
        Arc::new(state.await.unwrap())
    }

    /// Decode and emit one packet of `records` flows in exploded mode, and
    /// return what was counted emitted and dropped.
    async fn emit(pipeline: &Arc<PipelineState>, records: u16) -> (u64, u64) {
        let emitted = Arc::new(MockLabelledCounter(AtomicU64::new(0)));
        let drops = Arc::new(MockLabelledCounter(AtomicU64::new(0)));
        let metrics = FlowMetrics {
            records_emitted_total: emitted.clone(),
            drops_total: drops.clone(),
            ..flow_metrics_for_test()
        };
        let cfg: FlowListenerConfig = scalo::yaml::from_str(
            "ports: [2055]\ntopic: flows\noutput:\n  mode: exploded\n",
            "flow.yaml",
            scalo::yaml::Profile::Config,
        )
        .unwrap();
        let mut decoder = NetflowDecoder::new(64, 64, metrics.clone());
        let mut json_buf = Vec::new();
        UdpFlowListener::decode_and_emit::<NetflowDecoder>(
            &mut decoder,
            &v5_packet(records),
            "192.0.2.10:2055".parse().unwrap(),
            ProtocolKind::NetflowV5,
            "2026-11-20T00:00:00Z",
            &mut json_buf,
            &cfg,
            RawCapture::default(),
            &metrics,
            pipeline,
        )
        .await;
        (
            emitted.0.load(Ordering::Relaxed),
            drops.0.load(Ordering::Relaxed),
        )
    }

    /// A packet's records reach a gRPC destination in one batch, and a
    /// packet whose first record the pipeline cannot take emits nothing and
    /// counts every record dropped.
    #[tokio::test]
    async fn a_packet_is_sent_as_one_batch() {
        let destination = TestDestination::start();
        let pipeline = pipeline_to(&destination).await;

        assert_eq!(emit(&pipeline, 3).await, (3, 0));
        assert_eq!(destination.batches(), 1, "one send per record");
        assert_eq!(destination.records(), 3);

        pipeline.memory_guard().add_bytes(900_000);
        assert_eq!(emit(&pipeline, 3).await, (0, 3));
        assert_eq!(destination.batches(), 1);
    }
}
