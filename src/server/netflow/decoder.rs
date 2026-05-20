//  Project:      dfe-receiver
//  File:         src/server/netflow/decoder.rs
//  Purpose:      NetflowDecoder -- NetFlow v5 (hand-rolled) + v9/IPFIX (netgauze)
//  Language:     Rust
//
//  License:      FSL-1.1-ALv2
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! `NetflowDecoder` implements `FlowDecoder` for NetFlow v5, v9, and IPFIX.
//!
//! v5 is hand-rolled because `netgauze-flow-pkt` 0.12 does not implement
//! NetFlow v5 -- its `FlowInfoCodec` returns `UnsupportedVersion(5)` for v5
//! datagrams. v5 has a fixed wire layout (24-byte header + N x 48-byte
//! records) per the original Cisco specification, so a small dedicated parser
//! is more straightforward than fighting the netgauze API.
//!
//! v9 and IPFIX share `netgauze_flow_pkt::codec::FlowInfoCodec`, instantiated
//! per exporter IP so each router's template cache is isolated. Canonical
//! field mapping for v9/IPFIX records is intentionally minimal in this task
//! -- it's expanded in a follow-up task that fills out IANA IE -> canonical
//! field translation.

use crate::server::flow::decoder::{DecodedPacket, FlowDecoder};
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::flow::schema::CanonicalRecord;
use bytes::BytesMut;
use netgauze_flow_pkt::codec::{FlowInfoCodec, FlowInfoCodecDecoderError};
use std::collections::HashMap;
use std::io::{self, Write};
use std::net::{IpAddr, Ipv4Addr};
use thiserror::Error;
use tokio_util::codec::Decoder;

/// NetFlow v5 wire constants (RFC-equivalent: Cisco NetFlow Export Datagram Format).
const V5_HEADER_BYTES: usize = 24;
const V5_RECORD_BYTES: usize = 48;

#[derive(Debug, Error)]
pub enum NetflowError {
    #[error("template not yet received for exporter (observation_domain={observation_domain})")]
    TemplateMiss { observation_domain: u32 },
    #[error("malformed packet: {0}")]
    Parse(String),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

/// Distinguish "template not yet received" from a real parse error by looking
/// at the netgauze parsing error. Template misses surface as data-set
/// references for an ID we haven't seen a template for; netgauze reports these
/// as inner parsing errors but the precise variant differs between v9 and IPFIX,
/// so we keep this heuristic conservative: anything that's not "Incomplete" is
/// treated as a hard parse error here. A follow-up task may refine this to
/// recognise the specific "no template" inner error and return `TemplateMiss`
/// so the listener can update the `dfe_flow_template_misses_total` counter.
fn map_netgauze_error(err: &FlowInfoCodecDecoderError) -> NetflowError {
    match err {
        FlowInfoCodecDecoderError::IoError(s) => NetflowError::Parse(format!("io: {s}")),
        FlowInfoCodecDecoderError::Incomplete(n) => {
            NetflowError::Parse(format!("incomplete packet, need {n:?}"))
        }
        FlowInfoCodecDecoderError::UnsupportedVersion(v) => {
            NetflowError::Parse(format!("unsupported version: {v}"))
        }
        FlowInfoCodecDecoderError::IpfixParsingError(e) => {
            NetflowError::Parse(format!("ipfix: {e:?}"))
        }
        FlowInfoCodecDecoderError::NetFlowV9ParingError(e) => {
            NetflowError::Parse(format!("netflow v9: {e:?}"))
        }
    }
}

/// One decoded NetFlow record. For v5 the `fields` are populated directly
/// from the fixed wire layout. For v9/IPFIX, `fields` is currently sparse
/// and the verbatim netgauze JSON in `raw_json` is the primary representation.
#[derive(Debug, Clone)]
pub struct NetflowRecord {
    pub raw_json: String,
    pub fields: NetflowFields,
    /// "flow" -- richer discriminators (security_event, nat_translation) come
    /// in the follow-up canonical-mapping task.
    pub kind_label: &'static str,
}

#[derive(Debug, Clone, Default)]
pub struct NetflowFields {
    pub src_ip: Option<IpAddr>,
    pub dst_ip: Option<IpAddr>,
    pub src_port: Option<u16>,
    pub dst_port: Option<u16>,
    pub protocol: Option<u8>,
    pub tcp_flags: Option<u8>,
    pub bytes: Option<u64>,
    pub packets: Option<u64>,
    pub input_iface: Option<u32>,
    pub output_iface: Option<u32>,
    pub src_as: Option<u32>,
    pub dst_as: Option<u32>,
    pub next_hop: Option<IpAddr>,
}

/// Per-exporter codec state. For v9/IPFIX this carries the template caches.
/// For v5 it's unused (v5 is templateless) but the slot still exists so we
/// can do LRU bookkeeping uniformly.
struct ExporterState {
    codec: FlowInfoCodec,
}

impl ExporterState {
    fn new() -> Self {
        Self {
            codec: FlowInfoCodec::new(),
        }
    }
}

pub struct NetflowDecoder {
    state: HashMap<IpAddr, ExporterState>,
    /// Maximum templates per exporter (enforced indirectly: netgauze's template
    /// map grows unbounded, so this is recorded here for a follow-up cap pass).
    _max_per_exporter: usize,
    max_exporters: usize,
}

impl NetflowDecoder {
    pub fn new(max_per_exporter: usize, max_exporters: usize) -> Self {
        Self {
            state: HashMap::new(),
            _max_per_exporter: max_per_exporter,
            max_exporters,
        }
    }

    fn ensure_exporter_slot(&mut self, source: IpAddr) -> &mut ExporterState {
        // Crude LRU: if we're full and `source` is new, evict the first entry
        // we find. A proper LRU goes in with the metrics pass.
        if !self.state.contains_key(&source)
            && self.state.len() >= self.max_exporters
            && let Some(evict) = self.state.keys().next().copied()
        {
            self.state.remove(&evict);
        }
        self.state.entry(source).or_insert_with(ExporterState::new)
    }
}

impl FlowDecoder for NetflowDecoder {
    type Record = NetflowRecord;
    type DecodeError = NetflowError;
    const PROTOCOL: &'static str = "netflow";

    fn decode(
        &mut self,
        data: &[u8],
        source: IpAddr,
        kind: ProtocolKind,
    ) -> Result<DecodedPacket<Self::Record>, Self::DecodeError> {
        if !kind.is_netflow_family() {
            return Err(NetflowError::Parse(format!(
                "wrong protocol kind for NetflowDecoder: {kind:?}"
            )));
        }

        match kind {
            ProtocolKind::NetflowV5 => decode_v5(data, source),
            ProtocolKind::NetflowV9 | ProtocolKind::Ipfix => {
                let exporter = self.ensure_exporter_slot(source);
                decode_v9_or_ipfix(&mut exporter.codec, data, source, kind)
            }
            ProtocolKind::NetflowV7 => {
                Err(NetflowError::Parse("NetFlow v7 not supported".to_string()))
            }
            _ => unreachable!("is_netflow_family guarded above"),
        }
    }

    fn render_canonical(record: &Self::Record, buf: &mut Vec<u8>) -> io::Result<()> {
        let mut c = CanonicalRecord::empty();
        c.record_kind = record.kind_label;
        c.src_ip = record.fields.src_ip;
        c.dst_ip = record.fields.dst_ip;
        c.src_port = record.fields.src_port;
        c.dst_port = record.fields.dst_port;
        c.protocol = record.fields.protocol;
        c.tcp_flags = record.fields.tcp_flags;
        c.bytes = record.fields.bytes;
        c.packets = record.fields.packets;
        c.input_iface = record.fields.input_iface;
        c.output_iface = record.fields.output_iface;
        c.src_as = record.fields.src_as;
        c.dst_as = record.fields.dst_as;
        c.next_hop = record.fields.next_hop;
        c.ip_version = match c.src_ip {
            Some(IpAddr::V4(_)) => Some(4),
            Some(IpAddr::V6(_)) => Some(6),
            None => None,
        };
        let json = serde_json::to_vec(&c).map_err(io::Error::other)?;
        buf.write_all(&json)
    }

    fn render_raw(record: &Self::Record, buf: &mut Vec<u8>) -> io::Result<()> {
        buf.write_all(record.raw_json.as_bytes())
    }

    fn record_kind(record: &Self::Record) -> &'static str {
        record.kind_label
    }

    fn is_template_miss(err: &Self::DecodeError) -> bool {
        matches!(err, NetflowError::TemplateMiss { .. })
    }
}

// ----- v5 hand-rolled parser ------------------------------------------------

fn decode_v5(data: &[u8], source: IpAddr) -> Result<DecodedPacket<NetflowRecord>, NetflowError> {
    if data.len() < V5_HEADER_BYTES {
        return Err(NetflowError::Parse(format!(
            "v5 datagram too short: {} bytes",
            data.len()
        )));
    }
    let version = u16::from_be_bytes([data[0], data[1]]);
    if version != 5 {
        return Err(NetflowError::Parse(format!(
            "v5 decoder invoked on version {version}"
        )));
    }
    let count = u16::from_be_bytes([data[2], data[3]]) as usize;
    let expected_len = V5_HEADER_BYTES
        .checked_add(count.checked_mul(V5_RECORD_BYTES).ok_or_else(|| {
            NetflowError::Parse(format!("v5 record count {count} would overflow"))
        })?)
        .ok_or_else(|| NetflowError::Parse("v5 length overflow".to_string()))?;
    if data.len() < expected_len {
        return Err(NetflowError::Parse(format!(
            "v5 length mismatch: declared {expected_len}, actual {}",
            data.len()
        )));
    }

    let _sys_uptime = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    let _unix_secs = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
    let _unix_nsecs = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);
    let flow_sequence = u32::from_be_bytes([data[16], data[17], data[18], data[19]]);

    let mut records = Vec::with_capacity(count);
    for i in 0..count {
        let off = V5_HEADER_BYTES + i * V5_RECORD_BYTES;
        let rec = parse_v5_record(&data[off..off + V5_RECORD_BYTES])?;
        records.push(rec);
    }

    Ok(DecodedPacket {
        exporter_ip: source,
        observation_domain: 0, // v5 has no observation domain; engine_type/engine_id are in header
        packet_seq: flow_sequence,
        kind: ProtocolKind::NetflowV5,
        records,
    })
}

fn parse_v5_record(buf: &[u8]) -> Result<NetflowRecord, NetflowError> {
    if buf.len() < V5_RECORD_BYTES {
        return Err(NetflowError::Parse(format!(
            "v5 record short: {} bytes",
            buf.len()
        )));
    }
    let src = Ipv4Addr::new(buf[0], buf[1], buf[2], buf[3]);
    let dst = Ipv4Addr::new(buf[4], buf[5], buf[6], buf[7]);
    let next_hop = Ipv4Addr::new(buf[8], buf[9], buf[10], buf[11]);
    let input = u16::from_be_bytes([buf[12], buf[13]]);
    let output = u16::from_be_bytes([buf[14], buf[15]]);
    let pkts = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);
    let octets = u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]);
    // first/last (4 each), src_port/dst_port (2 each)
    let src_port = u16::from_be_bytes([buf[32], buf[33]]);
    let dst_port = u16::from_be_bytes([buf[34], buf[35]]);
    // buf[36] is pad1
    let tcp_flags = buf[37];
    let protocol = buf[38];
    // buf[39] is tos
    let src_as = u16::from_be_bytes([buf[40], buf[41]]);
    let dst_as = u16::from_be_bytes([buf[42], buf[43]]);

    let fields = NetflowFields {
        src_ip: Some(IpAddr::V4(src)),
        dst_ip: Some(IpAddr::V4(dst)),
        src_port: Some(src_port),
        dst_port: Some(dst_port),
        protocol: Some(protocol),
        tcp_flags: Some(tcp_flags),
        bytes: Some(u64::from(octets)),
        packets: Some(u64::from(pkts)),
        input_iface: Some(u32::from(input)),
        output_iface: Some(u32::from(output)),
        src_as: Some(u32::from(src_as)),
        dst_as: Some(u32::from(dst_as)),
        next_hop: if next_hop.is_unspecified() {
            None
        } else {
            Some(IpAddr::V4(next_hop))
        },
    };

    // For v5, the verbatim JSON is a compact representation of the structured
    // fields. We don't have a netgauze object to serialise here, so we build
    // one. This keeps `render_raw` cheap and avoids an extra serde dependency.
    let raw_json = serde_json::to_string(&V5RawJson {
        version: 5,
        src_addr: src.to_string(),
        dst_addr: dst.to_string(),
        next_hop: next_hop.to_string(),
        input,
        output,
        d_pkts: pkts,
        d_octets: octets,
        src_port,
        dst_port,
        tcp_flags,
        protocol,
        src_as,
        dst_as,
    })
    .map_err(io::Error::other)?;

    Ok(NetflowRecord {
        raw_json,
        fields,
        kind_label: "flow",
    })
}

#[derive(serde::Serialize)]
struct V5RawJson {
    version: u16,
    src_addr: String,
    dst_addr: String,
    next_hop: String,
    input: u16,
    output: u16,
    d_pkts: u32,
    d_octets: u32,
    src_port: u16,
    dst_port: u16,
    tcp_flags: u8,
    protocol: u8,
    src_as: u16,
    dst_as: u16,
}

// ----- v9/IPFIX via netgauze ------------------------------------------------

fn decode_v9_or_ipfix(
    codec: &mut FlowInfoCodec,
    data: &[u8],
    source: IpAddr,
    kind: ProtocolKind,
) -> Result<DecodedPacket<NetflowRecord>, NetflowError> {
    let mut buf = BytesMut::from(data);
    let flow_info = match codec.decode(&mut buf) {
        Ok(Some(f)) => f,
        Ok(None) => {
            return Err(NetflowError::Parse(
                "incomplete v9/IPFIX datagram (codec returned None)".to_string(),
            ));
        }
        Err(e) => return Err(map_netgauze_error(&e)),
    };

    let observation_domain = flow_info.observation_domain_id();
    let packet_seq = flow_info.sequence_number();

    // For this task we produce one NetflowRecord per data record with the
    // verbatim serde JSON as `raw_json`, and leave structured `fields` empty
    // (canonical-field mapping is the follow-up task).
    let mut records: Vec<NetflowRecord> = Vec::new();
    for (_set_id, fields) in flow_info.data_record_fields() {
        let raw_json = serde_json::to_string(&fields).unwrap_or_else(|_| "[]".to_string());
        records.push(NetflowRecord {
            raw_json,
            fields: NetflowFields::default(),
            kind_label: "flow",
        });
    }
    Ok(DecodedPacket {
        exporter_ip: source,
        observation_domain,
        packet_seq,
        kind,
        records,
    })
}

/// Re-export of `netgauze_flow_pkt::FlowInfo` for downstream callers that want
/// to inspect the typed netgauze object directly. Task 10 (canonical mapping)
/// will consume this when extracting IANA IE fields.
pub use netgauze_flow_pkt::FlowInfo as NetgauzeFlowInfo;
