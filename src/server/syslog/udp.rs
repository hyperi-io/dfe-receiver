// Project:   dfe-receiver
// File:      src/server/syslog/udp.rs
// Purpose:   Syslog UDP listener: sized receive buffer, batched reads
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The syslog UDP listener.
//!
//! The socket's receive buffer is set from `syslog.udp_recv_buffer_size`, and
//! datagrams the kernel drops when it is full are counted from
//! `/proc/net/udp`. Each wake reads every datagram ready, up to
//! `syslog.udp_batch_size`, through `quinn-udp` (`recvmmsg` on Linux), and
//! hands them to the pipeline as one batch.

use std::io::{self, IoSliceMut};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use quinn_udp::{RecvMeta, UdpSocketState};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::Interest;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::convert::Converter;
use super::{TRANSPORT, decode_lossy, listener_ingest};
use crate::error::{Error, Result};
use crate::metrics::{DropReason, Metrics};
use crate::pipeline::PipelineState;
use crate::server::ip_filter::IpFilter;
use crate::server::kernel_drops::poll_kernel_drops;
use crate::server::traits::BoundAddr;

/// Debounced timestamp for syslog UDP recv error warnings (1 per 5s).
static SYSLOG_UDP_WARN: AtomicU64 = AtomicU64::new(0);

/// One read buffer: a whole datagram, or the kernel's 64 KiB cap on a run of
/// datagrams coalesced by receive offload.
const BUFFER_SIZE: usize = 64 * 1024;

/// One UDP listener's settings.
pub(super) struct UdpListener {
    pub(super) bind_addr: SocketAddr,
    pub(super) pipeline: Arc<PipelineState>,
    pub(super) metrics: Arc<Metrics>,
    pub(super) converter: Converter,
    pub(super) ip_filter: IpFilter,
    pub(super) bound: BoundAddr,
    pub(super) recv_buffer_size: usize,
    pub(super) batch_size: usize,
}

/// Bind `addr` with a receive buffer of `recv_buffer_size` bytes, saying when
/// the kernel granted less.
pub(super) fn bind(addr: SocketAddr, recv_buffer_size: usize) -> Result<UdpSocket> {
    let fail = |what: &str, e: io::Error| Error::Server(format!("syslog UDP {what}: {e}"));
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))
        .map_err(|e| fail("socket", e))?;
    socket
        .set_recv_buffer_size(recv_buffer_size)
        .map_err(|e| fail("SO_RCVBUF", e))?;
    // Linux reports double the payload room it grants, so a grant below the
    // ask means net.core.rmem_max capped it.
    match socket.recv_buffer_size() {
        Ok(granted) if granted < recv_buffer_size => warn!(
            asked = recv_buffer_size,
            granted,
            "Syslog UDP receive buffer is smaller than syslog.udp_recv_buffer_size; raise net.core.rmem_max"
        ),
        Ok(_) => {}
        Err(e) => debug!(error = %e, "Syslog UDP receive buffer size unreadable"),
    }
    socket
        .set_nonblocking(true)
        .map_err(|e| fail("set_nonblocking", e))?;
    socket.bind(&addr.into()).map_err(|e| fail("bind", e))?;
    UdpSocket::from_std(socket.into()).map_err(|e| fail("register", e))
}

impl UdpListener {
    /// Read datagrams until `shutdown`.
    pub(super) async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let socket = bind(self.bind_addr, self.recv_buffer_size)?;
        let mut batch = Batch::new(&socket, self.batch_size)?;
        let local = socket.local_addr();
        let _serving = self.bound.publish(&local);
        info!(addr = %self.bind_addr, protocol = "udp", "Syslog UDP listener started");

        let drops = shutdown.child_token();
        let drop_metrics = Arc::clone(&self.metrics);
        let ports = local.map(|a| vec![a.port()]).unwrap_or_default();
        let poller = tokio::spawn(poll_kernel_drops(
            ports,
            move |n| drop_metrics.add_records_dropped(TRANSPORT, DropReason::Kernel, n),
            drops.clone(),
        ));

        let mut payloads: Vec<Bytes> = Vec::with_capacity(batch.capacity());
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                ready = socket.readable() => {
                    if let Err(e) = ready {
                        self.warn_recv(&e);
                        continue;
                    }
                }
            }
            if let Err(e) = batch.receive(&socket) {
                self.warn_recv(&e);
            }
            for (data, peer) in batch.datagrams() {
                if let Some(payload) = self.convert(data, peer) {
                    payloads.push(payload);
                }
            }
            self.offer(&payloads).await;
            payloads.clear();
        }

        info!("Syslog UDP listener stopping");
        drops.cancel();
        if let Err(e) = poller.await {
            warn!(error = %e, "Syslog UDP drop poller ended abnormally");
        }
        Ok(())
    }

    fn warn_recv(&self, e: &io::Error) {
        if scalo::logger::log_debounced(&SYSLOG_UDP_WARN, 5000) {
            warn!(addr = %self.bind_addr, error = %e, "Syslog UDP recv error (throttled to 1/5s)");
        }
    }

    /// One datagram as a pipeline payload, or `None` when it is refused or
    /// does not convert.
    fn convert(&self, data: &[u8], peer: SocketAddr) -> Option<Bytes> {
        // UDP has no connection to reject, so the filter runs per datagram,
        // before the payload is read.
        if !self.ip_filter.admits(peer, TRANSPORT, &self.metrics) {
            return None;
        }
        self.metrics.inc_requests_total(TRANSPORT);
        self.metrics
            .add_bytes_received(TRANSPORT, data.len() as u64);
        let raw = decode_lossy(data, &self.metrics);
        match self.converter.convert(&raw) {
            Ok(payload) => Some(payload),
            Err(e) => {
                debug!(peer = %peer, error = %e, "Syslog UDP parse error");
                self.metrics.inc_parse_failure(TRANSPORT);
                self.metrics.inc_requests_error(TRANSPORT);
                None
            }
        }
    }

    /// Hand one batch to the pipeline. A datagram has no answer to carry a
    /// retry, so every record not taken is gone, and counted.
    async fn offer(&self, payloads: &[Bytes]) {
        if payloads.is_empty() {
            return;
        }
        let outcome = self
            .pipeline
            .process_batch(payloads, &listener_ingest())
            .await;
        let lost = payloads.len() - outcome.accepted;
        for _ in 0..outcome.accepted {
            self.metrics.inc_requests_success(TRANSPORT);
        }
        for _ in 0..lost {
            self.metrics.inc_requests_error(TRANSPORT);
        }
        let rejected = outcome.rejected as u64;
        self.metrics
            .add_records_dropped(TRANSPORT, DropReason::Rejected, rejected);
        self.metrics.add_records_dropped(
            TRANSPORT,
            DropReason::Unavailable,
            lost as u64 - rejected,
        );
        if let Some(e) = outcome.into_error() {
            debug!(error = %e, lost, "Syslog UDP batch not wholly taken");
        }
    }
}

/// Buffers for one batch of datagrams, reused across reads.
struct Batch {
    state: UdpSocketState,
    buffers: Vec<Vec<u8>>,
    meta: Vec<RecvMeta>,
    /// Each datagram the last read filled: its buffer, its span there, its sender.
    received: Vec<(usize, usize, usize, SocketAddr)>,
}

impl Batch {
    fn new(socket: &UdpSocket, capacity: usize) -> Result<Self> {
        let capacity = capacity.max(1);
        let state = UdpSocketState::new(socket.into())
            .map_err(|e| Error::Server(format!("syslog UDP socket state: {e}")))?;
        Ok(Self {
            state,
            buffers: (0..capacity).map(|_| vec![0; BUFFER_SIZE]).collect(),
            meta: vec![RecvMeta::default(); capacity],
            received: Vec::with_capacity(capacity),
        })
    }

    fn capacity(&self) -> usize {
        self.buffers.len()
    }

    /// The datagrams the last [`receive`](Self::receive) read.
    fn datagrams(&self) -> impl Iterator<Item = (&[u8], SocketAddr)> {
        self.received
            .iter()
            .map(|&(buf, start, end, peer)| (&self.buffers[buf][start..end], peer))
    }

    /// Read every datagram ready, until the buffers are full or none is left.
    ///
    /// An error after some datagrams were read keeps them.
    fn receive(&mut self, socket: &UdpSocket) -> io::Result<()> {
        let Self {
            state,
            buffers,
            meta,
            received,
        } = self;
        received.clear();
        let mut used = 0;
        while used < buffers.len() {
            let mut slices: Vec<IoSliceMut<'_>> = buffers[used..]
                .iter_mut()
                .take(quinn_udp::BATCH_SIZE)
                .map(|buf| IoSliceMut::new(buf))
                .collect();
            let metas = &mut meta[used..used + slices.len()];
            let read = socket.try_io(Interest::READABLE, || {
                state.recv(socket.into(), &mut slices, metas)
            });
            let filled = match read {
                Ok(filled) => filled,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e),
            };
            for (i, m) in metas.iter().take(filled).enumerate() {
                // Receive offload joins equal-sized datagrams from one sender,
                // each `stride` bytes but the last.
                let stride = m.stride.max(1);
                let mut start = 0;
                while start < m.len {
                    let end = (start + stride).min(m.len);
                    received.push((used + i, start, end, m.addr));
                    start = end;
                }
            }
            used += filled;
            if filled == 0 {
                return Ok(());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The receive buffer is asked for, and every datagram sent in a burst
    /// larger than one batch is read, in batches no larger than asked.
    #[tokio::test]
    async fn a_burst_is_read_in_batches_with_nothing_lost() {
        let socket = bind("127.0.0.1:0".parse().unwrap(), 256 * 1024).unwrap();
        let granted = socket2::SockRef::from(&socket).recv_buffer_size().unwrap();
        assert!(granted >= 200 * 1024, "asked 256 KiB, granted {granted}");

        let addr = socket.local_addr().unwrap();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let sent: Vec<String> = (0..150).map(|i| format!("<14>datagram {i}")).collect();
        for line in &sent {
            sender.send_to(line.as_bytes(), addr).unwrap();
        }

        let mut batch = Batch::new(&socket, 64).unwrap();
        let mut got = Vec::new();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while got.len() < sent.len() && tokio::time::Instant::now() < deadline {
            socket.readable().await.unwrap();
            batch.receive(&socket).unwrap();
            let read: Vec<String> = batch
                .datagrams()
                .map(|(data, peer)| {
                    assert_eq!(peer, sender.local_addr().unwrap());
                    String::from_utf8(data.to_vec()).unwrap()
                })
                .collect();
            assert!(
                read.len() <= 64,
                "a batch of {} over the 64 asked",
                read.len()
            );
            got.extend(read);
        }
        assert_eq!(got, sent);
    }
}
