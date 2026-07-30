//  Project:      dfe-receiver
//  File:         src/server/netflow/decoder.rs
//  Purpose:      NetflowDecoder -- NetFlow v5 (hand-rolled) + v9/IPFIX (netgauze)
//  Language:     Rust
//
//  License:      BUSL-1.1
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
//! per exporter IP so each router's template cache is isolated. The canonical
//! mapping iterates the typed `ie::Field` variants that netgauze emits and
//! populates a `NetflowFields` struct. NSEL (firewallEvent / Cisco PEN=9
//! NF_F_FW_EVENT) and NAT44 (natEvent) discriminators flip `record_kind` to
//! "security_event" / "nat_translation".

use crate::server::flow::decoder::{DecodedPacket, FlowDecoder};
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::flow::metrics::FlowMetrics;
use crate::server::flow::schema::CanonicalRecord;
use bytes::BytesMut;
use netgauze_flow_pkt::codec::{FlowInfoCodec, FlowInfoCodecDecoderError};
use netgauze_flow_pkt::ie::Field;
use std::collections::HashMap;
use std::io::{self, Write};
use std::net::{IpAddr, Ipv4Addr};
use thiserror::Error;
use tokio_util::codec::Decoder;

/// NetFlow v5 wire constants (RFC-equivalent: Cisco NetFlow Export Datagram Format).
const V5_HEADER_BYTES: usize = 24;
const V5_RECORD_BYTES: usize = 48;

/// Cisco private-enterprise number for the ASA NSEL information elements.
const CISCO_PEN: u32 = 9;
/// Cisco NSEL: NF_F_FW_EVENT -- enterprise IE 40005, semantically equivalent
/// to IANA IE 233 firewallEvent (u8).
const CISCO_IE_FW_EVENT: u16 = 40005;
/// Cisco NSEL: NF_F_FW_EXT_EVENT (u16).
const CISCO_IE_FW_EXT_EVENT: u16 = 33002;
/// Cisco NSEL: NF_F_USERNAME (string).
const CISCO_IE_USERNAME: u16 = 40000;
/// Cisco NSEL: NF_F_INGRESS_ACL_ID (12-byte octet array, rendered as hex).
const CISCO_IE_INGRESS_ACL_ID: u16 = 33000;
/// Cisco NSEL: NF_F_EGRESS_ACL_ID (12-byte octet array, rendered as hex).
const CISCO_IE_EGRESS_ACL_ID: u16 = 33001;

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

/// One decoded NetFlow record. `fields` carries the canonical mapping that
/// `render_canonical` consumes; `raw_json` is the verbatim netgauze JSON for
/// callers that want the untransformed wire view.
#[derive(Debug, Clone)]
pub struct NetflowRecord {
    pub raw_json: String,
    pub fields: NetflowFields,
}

/// Canonical fields extracted from a NetFlow record. Each `Option` here maps
/// 1:1 to a field on `CanonicalRecord`. The `record_kind` discriminator is
/// driven by NSEL (firewallEvent / Cisco PEN=9 NF_F_FW_EVENT) and NAT44
/// (natEvent) detection in the IE-iteration code.
#[derive(Debug, Clone)]
pub struct NetflowFields {
    // Core flow
    pub src_ip: Option<IpAddr>,
    pub dst_ip: Option<IpAddr>,
    pub src_port: Option<u16>,
    pub dst_port: Option<u16>,
    pub protocol: Option<u8>,
    pub ip_version: Option<u8>,
    pub bytes: Option<u64>,
    pub packets: Option<u64>,
    pub t_flow_start: Option<String>,
    pub t_flow_end: Option<String>,
    pub duration_ms: Option<u64>,
    pub tcp_flags: Option<u8>,
    pub input_iface: Option<u32>,
    pub output_iface: Option<u32>,
    pub src_as: Option<u32>,
    pub dst_as: Option<u32>,
    pub next_hop: Option<IpAddr>,
    pub sampling_rate: Option<u32>,
    pub vlan_id: Option<u16>,
    // NSEL
    pub event_type: Option<u8>,
    pub event_subtype: Option<u16>,
    pub username: Option<String>,
    pub acl_in_id: Option<String>,
    pub acl_out_id: Option<String>,
    // NAT44
    pub nat_event_type: Option<u8>,
    pub pre_nat_src_ip: Option<IpAddr>,
    pub pre_nat_dst_ip: Option<IpAddr>,
    pub pre_nat_src_port: Option<u16>,
    pub pre_nat_dst_port: Option<u16>,
    pub nat_pool_name: Option<String>,
    // Discriminator
    pub record_kind: &'static str,
}

impl Default for NetflowFields {
    fn default() -> Self {
        Self::new()
    }
}

impl NetflowFields {
    pub fn new() -> Self {
        Self {
            src_ip: None,
            dst_ip: None,
            src_port: None,
            dst_port: None,
            protocol: None,
            ip_version: None,
            bytes: None,
            packets: None,
            t_flow_start: None,
            t_flow_end: None,
            duration_ms: None,
            tcp_flags: None,
            input_iface: None,
            output_iface: None,
            src_as: None,
            dst_as: None,
            next_hop: None,
            sampling_rate: None,
            vlan_id: None,
            event_type: None,
            event_subtype: None,
            username: None,
            acl_in_id: None,
            acl_out_id: None,
            nat_event_type: None,
            pre_nat_src_ip: None,
            pre_nat_dst_ip: None,
            pre_nat_src_port: None,
            pre_nat_dst_port: None,
            nat_pool_name: None,
            record_kind: "flow",
        }
    }
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
    // TODO: template_cache.max_per_exporter not yet enforced.
    //
    // netgauze-flow-pkt 0.12's `TemplatesMap` is internal and doesn't expose a
    // per-exporter prune API. Enforcing this cap requires either upstream
    // netgauze support or a custom wrapper that intercepts template additions
    // and tracks counts per exporter scope.
    //
    // Currently the bound on `max_exporters` (whole exporters evicted via LRU)
    // is the only template-related backstop.
    _max_per_exporter: usize,
    max_exporters: usize,
    metrics: FlowMetrics,
}

impl NetflowDecoder {
    pub fn new(max_per_exporter: usize, max_exporters: usize, metrics: FlowMetrics) -> Self {
        Self {
            state: HashMap::new(),
            _max_per_exporter: max_per_exporter,
            max_exporters,
            metrics,
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
            self.metrics.template_evicted_total.inc();
        }
        // Update the size gauge after any insert/evict. Capture the length
        // through a single mutable borrow window so the gauge call doesn't
        // conflict with the `entry().or_insert_with()` borrow we return.
        let _ = self.state.entry(source).or_insert_with(ExporterState::new);
        let len = self.state.len();
        self.metrics
            .template_cache_size
            .set(&[("transport", "netflow")], len as f64);
        self.state
            .get_mut(&source)
            .unwrap_or_else(|| unreachable!("slot was just inserted via entry().or_insert_with()"))
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
        let f = &record.fields;
        let mut c = CanonicalRecord::empty();
        c.record_kind = f.record_kind;
        c.src_ip = f.src_ip;
        c.dst_ip = f.dst_ip;
        c.src_port = f.src_port;
        c.dst_port = f.dst_port;
        c.protocol = f.protocol;
        c.bytes = f.bytes;
        c.packets = f.packets;
        c.t_flow_start = f.t_flow_start.clone();
        c.t_flow_end = f.t_flow_end.clone();
        c.duration_ms = f.duration_ms;
        c.tcp_flags = f.tcp_flags;
        c.input_iface = f.input_iface;
        c.output_iface = f.output_iface;
        c.src_as = f.src_as;
        c.dst_as = f.dst_as;
        c.next_hop = f.next_hop;
        c.sampling_rate = f.sampling_rate;
        c.vlan_id = f.vlan_id;
        // NSEL
        c.event_type = f.event_type;
        c.event_subtype = f.event_subtype;
        c.username = f.username.clone();
        c.acl_in_id = f.acl_in_id.clone();
        c.acl_out_id = f.acl_out_id.clone();
        // NAT44
        c.nat_event_type = f.nat_event_type;
        c.pre_nat_src_ip = f.pre_nat_src_ip;
        c.pre_nat_dst_ip = f.pre_nat_dst_ip;
        c.pre_nat_src_port = f.pre_nat_src_port;
        c.pre_nat_dst_port = f.pre_nat_dst_port;
        c.nat_pool_name = f.nat_pool_name.clone();
        // Derive ip_version from src_ip when the wire data didn't carry an
        // explicit IE for it.
        c.ip_version = f.ip_version.or(match c.src_ip {
            Some(IpAddr::V4(_)) => Some(4),
            Some(IpAddr::V6(_)) => Some(6),
            None => None,
        });
        // Hot path: render directly into the reusable buffer via sonic-rs's
        // SIMD writer. Avoids the per-record allocation that
        // `serde_json::to_vec(&c)` introduces.
        sonic_rs::to_writer(&mut *buf, &c).map_err(io::Error::other)
    }

    fn render_raw(record: &Self::Record, buf: &mut Vec<u8>) -> io::Result<()> {
        buf.write_all(record.raw_json.as_bytes())
    }

    fn record_kind(record: &Self::Record) -> &'static str {
        record.fields.record_kind
    }

    /// TODO: template_miss differentiation deferred.
    ///
    /// netgauze-flow-pkt 0.12 does not expose a "template not yet received"
    /// error variant distinct from generic parse errors. Currently this method
    /// returns true ONLY for our explicit `NetflowError::TemplateMiss` variant,
    /// which we never produce from the netgauze decode path -- all netgauze
    /// errors map to `Parse`. The `dfe_transport_decode_err_total{reason=
    /// "template_miss"}` label will therefore be zero until either:
    ///   1. netgauze upstream exposes the discriminator, OR
    ///   2. we add a custom error classifier that inspects netgauze error
    ///      strings (brittle, defer until needed).
    ///
    /// Known limitation, tracked rather than solved.
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

    let mut fields = NetflowFields::new();
    fields.src_ip = Some(IpAddr::V4(src));
    fields.dst_ip = Some(IpAddr::V4(dst));
    fields.src_port = Some(src_port);
    fields.dst_port = Some(dst_port);
    fields.protocol = Some(protocol);
    fields.tcp_flags = Some(tcp_flags);
    fields.bytes = Some(u64::from(octets));
    fields.packets = Some(u64::from(pkts));
    fields.input_iface = Some(u32::from(input));
    fields.output_iface = Some(u32::from(output));
    fields.src_as = Some(u32::from(src_as));
    fields.dst_as = Some(u32::from(dst_as));
    fields.next_hop = if next_hop.is_unspecified() {
        None
    } else {
        Some(IpAddr::V4(next_hop))
    };
    fields.ip_version = Some(4);

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

    Ok(NetflowRecord { raw_json, fields })
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

    let mut records: Vec<NetflowRecord> = Vec::new();
    for (_set_id, fields) in flow_info.data_record_fields() {
        let raw_json = serde_json::to_string(fields).unwrap_or_else(|_| "[]".to_string());
        let canonical = map_fields_to_canonical(fields);
        records.push(NetflowRecord {
            raw_json,
            fields: canonical,
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

/// Translate a slice of netgauze `ie::Field` into our canonical `NetflowFields`.
///
/// Cisco PEN=9 NSEL information elements (NF_F_FW_EVENT etc.) are not exposed
/// as named variants by netgauze 0.12 -- it lacks a Cisco sub-module -- so they
/// arrive via `Field::Unknown { pen: 9, id, value }`. We pattern-match those
/// by IE number and decode the raw bytes inline. The IANA `firewallEvent` IE
/// (233) is exposed as a named variant and is mapped on its own arm.
fn map_fields_to_canonical(fields: &[Field]) -> NetflowFields {
    let mut out = NetflowFields::new();

    for field in fields {
        map_single_field(field, &mut out);
    }

    // Derive ip_version from src_ip if not explicitly set elsewhere.
    if out.ip_version.is_none() {
        out.ip_version = match out.src_ip {
            Some(IpAddr::V4(_)) => Some(4),
            Some(IpAddr::V6(_)) => Some(6),
            None => None,
        };
    }

    // Derive duration_ms when both start and end timestamps are present.
    // We use ms-precision timestamps where available; sysUpTime is a u32
    // delta against the exporter boot time so a coarse subtract is fine
    // for the duration field.
    if out.duration_ms.is_none()
        && let (Some(start), Some(end)) = (&out.t_flow_start, &out.t_flow_end)
        && let (Ok(s), Ok(e)) = (
            chrono::DateTime::parse_from_rfc3339(start),
            chrono::DateTime::parse_from_rfc3339(end),
        )
    {
        let delta_ms = (e - s).num_milliseconds();
        if delta_ms >= 0 {
            out.duration_ms = Some(delta_ms as u64);
        }
    }

    out
}

/// Map a single netgauze `Field` into the canonical struct in place.
///
/// Split out of the loop body so the `match` doesn't bloat into a single
/// 200-line function and so individual arms read cleanly.
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
fn map_single_field(field: &Field, out: &mut NetflowFields) {
    match field {
        // ----- Core flow IEs -----
        Field::sourceIPv4Address(ip) => {
            out.src_ip = Some(IpAddr::V4(*ip));
        }
        Field::sourceIPv6Address(ip) => {
            out.src_ip = Some(IpAddr::V6(*ip));
            out.ip_version = Some(6);
        }
        Field::destinationIPv4Address(ip) => {
            out.dst_ip = Some(IpAddr::V4(*ip));
        }
        Field::destinationIPv6Address(ip) => {
            out.dst_ip = Some(IpAddr::V6(*ip));
        }
        Field::sourceTransportPort(p) => {
            out.src_port = Some(*p);
        }
        Field::destinationTransportPort(p) => {
            out.dst_port = Some(*p);
        }
        Field::protocolIdentifier(p) => {
            out.protocol = Some(u8::from(*p));
        }
        Field::octetDeltaCount(n) => {
            out.bytes = Some(*n);
        }
        Field::packetDeltaCount(n) => {
            out.packets = Some(*n);
        }
        Field::tcpControlBits(flags) => {
            out.tcp_flags = Some(u8::from(*flags));
        }
        Field::ingressInterface(v) => {
            out.input_iface = Some(*v);
        }
        Field::egressInterface(v) => {
            out.output_iface = Some(*v);
        }
        Field::bgpSourceAsNumber(v) => {
            out.src_as = Some(*v);
        }
        Field::bgpDestinationAsNumber(v) => {
            out.dst_as = Some(*v);
        }
        Field::ipNextHopIPv4Address(ip) if !ip.is_unspecified() => {
            out.next_hop = Some(IpAddr::V4(*ip));
        }
        Field::ipNextHopIPv6Address(ip) if !ip.is_unspecified() => {
            out.next_hop = Some(IpAddr::V6(*ip));
        }
        Field::samplingInterval(v) | Field::samplingPacketInterval(v) => {
            out.sampling_rate = Some(*v);
        }
        Field::vlanId(v) => {
            out.vlan_id = Some(*v);
        }
        Field::flowStartMilliseconds(dt) => {
            out.t_flow_start = Some(dt.to_rfc3339());
        }
        Field::flowEndMilliseconds(dt) => {
            out.t_flow_end = Some(dt.to_rfc3339());
        }
        Field::flowStartSysUpTime(ms) | Field::flowEndSysUpTime(ms) => {
            // SysUpTime IEs are u32 deltas against the exporter's boot
            // time. Without the option-template "exporterSysUpTime" anchor
            // we can't render an absolute RFC 3339 timestamp here, so we
            // contribute to duration_ms via the start/end pair. Stash the
            // raw delta on the *_sys_up_time-less fields opportunistically:
            // first writer wins (start) so we can compute end-start.
            if matches!(field, Field::flowStartSysUpTime(_)) {
                out.t_flow_start
                    .get_or_insert_with(|| format!("sysup:{ms}"));
            } else {
                out.t_flow_end.get_or_insert_with(|| format!("sysup:{ms}"));
            }
        }

        // ----- NSEL: IANA firewallEvent (IE 233) -----
        Field::firewallEvent(ev) => {
            out.record_kind = "security_event";
            out.event_type = Some(u8::from(*ev));
        }

        // ----- NAT44: natEvent (IE 230) + NAT addresses/ports + pool name -----
        Field::natEvent(ev) => {
            out.record_kind = "nat_translation";
            out.nat_event_type = Some(u8::from(*ev));
        }
        Field::postNATSourceIPv4Address(ip) => {
            out.pre_nat_src_ip = Some(IpAddr::V4(*ip));
        }
        Field::postNATDestinationIPv4Address(ip) => {
            out.pre_nat_dst_ip = Some(IpAddr::V4(*ip));
        }
        Field::postNAPTSourceTransportPort(p) => {
            out.pre_nat_src_port = Some(*p);
        }
        Field::postNAPTDestinationTransportPort(p) => {
            out.pre_nat_dst_port = Some(*p);
        }
        Field::natPoolName(s) => {
            out.nat_pool_name = Some(s.to_string());
        }

        // ----- Cisco PEN=9 (NSEL) IEs via netgauze's generic escape hatch -----
        //
        // netgauze 0.12 does not ship a Cisco vendor module, so Cisco-private
        // NSEL fields arrive as `Field::Unknown { pen: 9, id, value }`. Decode
        // the raw bytes inline per Cisco's "ASA NetFlow Implementation" guide.
        Field::Unknown { pen, id, value } if *pen == CISCO_PEN => {
            map_cisco_unknown(*id, value, out);
        }
        _ => {
            // Unmapped IE -- ignore. Includes basicList/subTemplateList, vendor
            // IEs we don't decode (Nokia, Huawei, VMware, NetGauze), and many
            // niche IANA IEs we don't yet need on the canonical record.
        }
    }
}

/// Decode a Cisco PEN=9 NSEL Information Element from its raw bytes.
fn map_cisco_unknown(id: u16, value: &[u8], out: &mut NetflowFields) {
    match id {
        // NF_F_FW_EVENT (u8): semantically identical to IANA firewallEvent.
        CISCO_IE_FW_EVENT => {
            if let Some(&b) = value.first() {
                out.record_kind = "security_event";
                out.event_type = Some(b);
            }
        }
        // NF_F_FW_EXT_EVENT (u16, big-endian).
        CISCO_IE_FW_EXT_EVENT if value.len() >= 2 => {
            out.event_subtype = Some(u16::from_be_bytes([value[0], value[1]]));
        }
        // NF_F_USERNAME -- UTF-8 string, null-terminated tolerated.
        CISCO_IE_USERNAME => {
            let s = std::str::from_utf8(value)
                .unwrap_or("")
                .trim_end_matches('\0');
            if !s.is_empty() {
                out.username = Some(s.to_string());
            }
        }
        // NF_F_INGRESS_ACL_ID / NF_F_EGRESS_ACL_ID are 12-byte tuples in
        // Cisco's docs (ACL name hash + rule index). They contain non-UTF-8
        // bytes so we hex-encode for the canonical record.
        CISCO_IE_INGRESS_ACL_ID => {
            out.acl_in_id = Some(hex_encode(value));
        }
        CISCO_IE_EGRESS_ACL_ID => {
            out.acl_out_id = Some(hex_encode(value));
        }
        _ => {
            // Unmapped Cisco IE -- silently ignored. Add new arms as needed.
        }
    }
}

/// Lowercase hex encoding without separators. Small inline helper to avoid a
/// new dep for this single use site.
fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(nybble_to_hex(b >> 4));
        out.push(nybble_to_hex(b & 0x0F));
    }
    out
}

fn nybble_to_hex(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + (n - 10)) as char,
        _ => '?',
    }
}

/// Re-export of `netgauze_flow_pkt::FlowInfo` for downstream callers that want
/// to inspect the typed netgauze object directly.
pub use netgauze_flow_pkt::FlowInfo as NetgauzeFlowInfo;
