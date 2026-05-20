//  Project:      dfe-receiver
//  File:         src/server/sflow/decoder.rs
//  Purpose:      SflowDecoder -- implements FlowDecoder for sFlow v5
//  Language:     Rust
//
//  License:      FSL-1.1-ALv2
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! sFlow v5 decoder implementing the generic `FlowDecoder` trait.
//!
//! Wraps the nom-based `parser` module: parse the datagram, then for each
//! sample translate to a `SflowRecord` (Flow or Counter). Flow samples that
//! carry a `SampledHeader` record have their first 54+ bytes of raw L2/L3/L4
//! walked by `parse_sampled_ip` to extract canonical IP/port/protocol/tcp_flags.
//!
//! Vendor-specific opaque blocks are preserved in `raw_json` but skipped by
//! canonical mapping.

use crate::server::flow::decoder::{DecodedPacket, FlowDecoder};
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::flow::schema::{CanonicalCounterRecord, CanonicalRecord};
use crate::server::sflow::parser::{self, CounterRecord, FlowRecord, Sample};
use std::io::{self, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SflowError {
    #[error("parse: {0}")]
    Parse(String),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

/// One decoded sFlow record. Flow and Counter variants carry the canonical
/// mapping `render_canonical` consumes; `raw_json` is a verbatim view of the
/// parsed sample for callers that want the untransformed form.
#[derive(Debug, Clone)]
pub enum SflowRecord {
    Flow {
        sampling_rate: u32,
        input_iface: u32,
        output_iface: u32,
        sampled_header: Option<SampledIpHeader>,
        raw_json: String,
    },
    Counter {
        generic: Option<GenericCounters>,
        raw_json: String,
    },
}

/// IP/port/protocol/tcp_flags extracted from a sampled L2/L3/L4 header.
#[derive(Debug, Clone, Default)]
pub struct SampledIpHeader {
    pub src_ip: Option<IpAddr>,
    pub dst_ip: Option<IpAddr>,
    pub src_port: Option<u16>,
    pub dst_port: Option<u16>,
    pub protocol: Option<u8>,
    pub ip_version: Option<u8>,
    pub tcp_flags: Option<u8>,
}

/// Generic interface counters condensed to the canonical fields. The
/// `if_in_packets` / `if_out_packets` values are summed across
/// ucast+multicast+broadcast so a downstream consumer sees a single packets
/// figure regardless of the sample's breakdown granularity.
#[derive(Debug, Clone)]
pub struct GenericCounters {
    pub if_index: u32,
    pub if_speed: u64,
    pub if_in_octets: u64,
    pub if_in_packets: u64,
    pub if_in_errors: u64,
    pub if_in_discards: u64,
    pub if_out_octets: u64,
    pub if_out_packets: u64,
    pub if_out_errors: u64,
    pub if_out_discards: u64,
}

pub struct SflowDecoder;

impl SflowDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SflowDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl FlowDecoder for SflowDecoder {
    type Record = SflowRecord;
    type DecodeError = SflowError;
    const PROTOCOL: &'static str = "sflow";

    fn decode(
        &mut self,
        data: &[u8],
        source: IpAddr,
        kind: ProtocolKind,
    ) -> Result<DecodedPacket<Self::Record>, Self::DecodeError> {
        if !kind.is_sflow() {
            return Err(SflowError::Parse(format!(
                "wrong protocol kind for SflowDecoder: {kind:?}"
            )));
        }
        let (_, dg) =
            parser::parse_datagram(data).map_err(|e| SflowError::Parse(format!("nom: {e:?}")))?;

        let mut records: Vec<SflowRecord> = Vec::with_capacity(dg.samples.len());
        for sample in &dg.samples {
            match sample {
                Sample::Flow(fs) => {
                    let mut sampled_header: Option<SampledIpHeader> = None;
                    for r in &fs.records {
                        if let FlowRecord::SampledHeader {
                            protocol, header, ..
                        } = r
                        {
                            sampled_header = Some(parse_sampled_ip(*protocol, header));
                            break;
                        }
                    }
                    let raw_json = serialise_flow_sample_raw(fs);
                    records.push(SflowRecord::Flow {
                        sampling_rate: fs.sampling_rate,
                        input_iface: fs.input_iface,
                        output_iface: fs.output_iface,
                        sampled_header,
                        raw_json,
                    });
                }
                Sample::Counter(cs) => {
                    let generic = cs.records.iter().find_map(|r| match r {
                        CounterRecord::Generic {
                            if_index,
                            if_speed,
                            if_in_octets,
                            if_in_ucast_pkts,
                            if_in_multicast_pkts,
                            if_in_broadcast_pkts,
                            if_in_discards,
                            if_in_errors,
                            if_out_octets,
                            if_out_ucast_pkts,
                            if_out_multicast_pkts,
                            if_out_broadcast_pkts,
                            if_out_discards,
                            if_out_errors,
                            ..
                        } => Some(GenericCounters {
                            if_index: *if_index,
                            if_speed: *if_speed,
                            if_in_octets: *if_in_octets,
                            if_in_packets: u64::from(*if_in_ucast_pkts)
                                + u64::from(*if_in_multicast_pkts)
                                + u64::from(*if_in_broadcast_pkts),
                            if_in_errors: u64::from(*if_in_errors),
                            if_in_discards: u64::from(*if_in_discards),
                            if_out_octets: *if_out_octets,
                            if_out_packets: u64::from(*if_out_ucast_pkts)
                                + u64::from(*if_out_multicast_pkts)
                                + u64::from(*if_out_broadcast_pkts),
                            if_out_errors: u64::from(*if_out_errors),
                            if_out_discards: u64::from(*if_out_discards),
                        }),
                        CounterRecord::Opaque { .. } => None,
                    });
                    let raw_json = serialise_counter_sample_raw(cs);
                    records.push(SflowRecord::Counter { generic, raw_json });
                }
                Sample::Opaque { .. } => {
                    // Vendor-specific samples are preserved in the datagram but
                    // we don't surface them as records in v1. A future pass may
                    // emit a "raw" SflowRecord variant for downstream tooling.
                }
            }
        }

        Ok(DecodedPacket {
            exporter_ip: source,
            observation_domain: dg.sub_agent_id,
            packet_seq: dg.sequence_number,
            kind: ProtocolKind::SflowV5,
            records,
        })
    }

    fn render_canonical(record: &Self::Record, buf: &mut Vec<u8>) -> io::Result<()> {
        match record {
            SflowRecord::Flow {
                sampling_rate,
                input_iface,
                output_iface,
                sampled_header,
                ..
            } => {
                let mut c = CanonicalRecord::empty();
                c.record_kind = "flow";
                c.sampling_rate = Some(*sampling_rate);
                c.input_iface = Some(*input_iface);
                c.output_iface = Some(*output_iface);
                if let Some(h) = sampled_header {
                    c.src_ip = h.src_ip;
                    c.dst_ip = h.dst_ip;
                    c.src_port = h.src_port;
                    c.dst_port = h.dst_port;
                    c.protocol = h.protocol;
                    c.ip_version = h.ip_version;
                    c.tcp_flags = h.tcp_flags;
                }
                // Hot path: SIMD writer straight into the reusable buffer.
                sonic_rs::to_writer(&mut *buf, &c).map_err(io::Error::other)
            }
            SflowRecord::Counter { generic, .. } => {
                let mut c = CanonicalCounterRecord::empty();
                if let Some(g) = generic {
                    c.if_index = Some(g.if_index);
                    c.if_speed = Some(g.if_speed);
                    c.if_in_octets = Some(g.if_in_octets);
                    c.if_in_packets = Some(g.if_in_packets);
                    c.if_in_errors = Some(g.if_in_errors);
                    c.if_in_discards = Some(g.if_in_discards);
                    c.if_out_octets = Some(g.if_out_octets);
                    c.if_out_packets = Some(g.if_out_packets);
                    c.if_out_errors = Some(g.if_out_errors);
                    c.if_out_discards = Some(g.if_out_discards);
                }
                // Hot path: SIMD writer straight into the reusable buffer.
                sonic_rs::to_writer(&mut *buf, &c).map_err(io::Error::other)
            }
        }
    }

    fn render_raw(record: &Self::Record, buf: &mut Vec<u8>) -> io::Result<()> {
        let raw = match record {
            SflowRecord::Flow { raw_json, .. } => raw_json.as_bytes(),
            SflowRecord::Counter { raw_json, .. } => raw_json.as_bytes(),
        };
        buf.write_all(raw)
    }

    fn record_kind(record: &Self::Record) -> &'static str {
        match record {
            SflowRecord::Flow { .. } => "flow",
            SflowRecord::Counter { .. } => "counter",
        }
    }
}

// ----- raw JSON serialisation -----------------------------------------------

#[derive(serde::Serialize)]
struct FlowSampleRaw {
    kind: &'static str,
    sequence_number: u32,
    sampling_rate: u32,
    sample_pool: u32,
    drops: u32,
    input_iface: u32,
    output_iface: u32,
    num_records: usize,
}

fn serialise_flow_sample_raw(fs: &parser::FlowSample) -> String {
    serde_json::to_string(&FlowSampleRaw {
        kind: "flow_sample",
        sequence_number: fs.sequence_number,
        sampling_rate: fs.sampling_rate,
        sample_pool: fs.sample_pool,
        drops: fs.drops,
        input_iface: fs.input_iface,
        output_iface: fs.output_iface,
        num_records: fs.records.len(),
    })
    .unwrap_or_else(|_| "{}".to_string())
}

#[derive(serde::Serialize)]
struct CounterSampleRaw {
    kind: &'static str,
    sequence_number: u32,
    source_id: u32,
    num_records: usize,
}

fn serialise_counter_sample_raw(cs: &parser::CounterSample) -> String {
    serde_json::to_string(&CounterSampleRaw {
        kind: "counter_sample",
        sequence_number: cs.sequence_number,
        source_id: cs.source_id,
        num_records: cs.records.len(),
    })
    .unwrap_or_else(|_| "{}".to_string())
}

// ----- sampled-header walker ------------------------------------------------

/// Walk a sampled L2/L3/L4 header and extract canonical fields.
///
/// `header_protocol` semantics per sFlow v5 spec:
///   1  = Ethernet (default)
///   11 = IPv4 (no Ethernet wrapper)
///   12 = IPv6 (no Ethernet wrapper)
///
/// 802.1Q VLAN tags are skipped transparently. Returns whatever could be
/// extracted; on a malformed/short header the unset fields stay `None`.
pub(crate) fn parse_sampled_ip(header_protocol: u32, header: &[u8]) -> SampledIpHeader {
    let mut out = SampledIpHeader::default();
    match header_protocol {
        1 => walk_ethernet(header, &mut out),
        11 => walk_ipv4(header, &mut out),
        12 => walk_ipv6(header, &mut out),
        _ => {
            // Unknown encap -- leave SampledIpHeader empty. We never panic on
            // unsupported header types; the caller still gets sampling_rate
            // and interface info from the FlowSample envelope.
        }
    }
    out
}

fn walk_ethernet(h: &[u8], out: &mut SampledIpHeader) {
    if h.len() < 14 {
        return;
    }
    let mut ethertype = u16::from_be_bytes([h[12], h[13]]);
    let mut rest = &h[14..];
    // 802.1Q VLAN tag: 4 bytes (TPID handled by ethertype check, then TCI+inner ethertype)
    if ethertype == 0x8100 && rest.len() >= 4 {
        ethertype = u16::from_be_bytes([rest[2], rest[3]]);
        rest = &rest[4..];
    }
    match ethertype {
        0x0800 => walk_ipv4(rest, out),
        0x86DD => walk_ipv6(rest, out),
        _ => {}
    }
}

fn walk_ipv4(h: &[u8], out: &mut SampledIpHeader) {
    if h.len() < 20 {
        return;
    }
    let ihl = (h[0] & 0x0F) as usize * 4;
    if ihl < 20 || h.len() < ihl {
        return;
    }
    out.ip_version = Some(4);
    out.protocol = Some(h[9]);
    out.src_ip = Some(IpAddr::V4(Ipv4Addr::new(h[12], h[13], h[14], h[15])));
    out.dst_ip = Some(IpAddr::V4(Ipv4Addr::new(h[16], h[17], h[18], h[19])));
    walk_l4(h[9], &h[ihl..], out);
}

fn walk_ipv6(h: &[u8], out: &mut SampledIpHeader) {
    if h.len() < 40 {
        return;
    }
    out.ip_version = Some(6);
    let next_hdr = h[6];
    out.protocol = Some(next_hdr);
    let mut src = [0u8; 16];
    let mut dst = [0u8; 16];
    src.copy_from_slice(&h[8..24]);
    dst.copy_from_slice(&h[24..40]);
    out.src_ip = Some(IpAddr::V6(Ipv6Addr::from(src)));
    out.dst_ip = Some(IpAddr::V6(Ipv6Addr::from(dst)));
    walk_l4(next_hdr, &h[40..], out);
}

fn walk_l4(protocol: u8, l4: &[u8], out: &mut SampledIpHeader) {
    if l4.len() < 4 {
        return;
    }
    // TCP = 6, UDP = 17 -- both have src/dst port as first 4 bytes.
    if protocol == 6 || protocol == 17 {
        out.src_port = Some(u16::from_be_bytes([l4[0], l4[1]]));
        out.dst_port = Some(u16::from_be_bytes([l4[2], l4[3]]));
        if protocol == 6 && l4.len() >= 14 {
            // TCP flags live in byte 13 of the TCP header.
            out.tcp_flags = Some(l4[13]);
        }
    }
}
