//! Render a DecodedPacket<R> into JSON envelopes per the configured OutputMode.
//!
//! The three modes share top-level envelope fields (_source, protocol,
//! version, exporter_ip, observation_domain, packet_seq, t_collected). Modes
//! differ in whether they emit per-packet (Canonical/CanonicalWithRaw) or
//! per-record (Exploded).

use crate::server::flow::config::OutputMode;
use crate::server::flow::decoder::{DecodedPacket, FlowDecoder};
use std::io::{self, Write};
use std::ops::Range;

/// Render `decoded` into one or more JSON events written into `buf`. Returns
/// the byte ranges within `buf` corresponding to each event.
///
/// `buf.clear()` is called inside; caller passes a reusable buffer.
pub fn render_packet<D: FlowDecoder>(
    decoded: &DecodedPacket<D::Record>,
    mode: OutputMode,
    now_rfc3339: &str,
    buf: &mut Vec<u8>,
) -> io::Result<Vec<Range<usize>>> {
    buf.clear();
    let mut ranges = Vec::new();
    match mode {
        OutputMode::Canonical => {
            let start = buf.len();
            render_canonical_packet::<D>(decoded, now_rfc3339, buf)?;
            ranges.push(start..buf.len());
        }
        OutputMode::CanonicalWithRaw => {
            let start = buf.len();
            render_canonical_with_raw::<D>(decoded, now_rfc3339, buf)?;
            ranges.push(start..buf.len());
        }
        OutputMode::Exploded => {
            for (i, record) in decoded.records.iter().enumerate() {
                let start = buf.len();
                render_exploded_record::<D>(decoded, record, i, now_rfc3339, buf)?;
                ranges.push(start..buf.len());
            }
        }
    }
    Ok(ranges)
}

fn write_envelope_head<D: FlowDecoder>(
    decoded: &DecodedPacket<D::Record>,
    now_rfc3339: &str,
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    buf.extend_from_slice(br#"{"_source":""#);
    buf.extend_from_slice(D::PROTOCOL.as_bytes());
    buf.extend_from_slice(br#"","protocol":""#);
    buf.extend_from_slice(D::PROTOCOL.as_bytes());
    buf.extend_from_slice(br#"","version":""#);
    buf.extend_from_slice(decoded.kind.version_tag().as_bytes());
    buf.extend_from_slice(br#"","exporter_ip":""#);
    write!(buf, "{}", decoded.exporter_ip)?;
    buf.extend_from_slice(br#"","observation_domain":"#);
    write!(buf, "{}", decoded.observation_domain)?;
    buf.extend_from_slice(br#","packet_seq":"#);
    write!(buf, "{}", decoded.packet_seq)?;
    buf.extend_from_slice(br#","t_collected":""#);
    buf.extend_from_slice(now_rfc3339.as_bytes());
    buf.push(b'"');
    Ok(())
}

fn render_canonical_packet<D: FlowDecoder>(
    decoded: &DecodedPacket<D::Record>,
    now_rfc3339: &str,
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    write_envelope_head::<D>(decoded, now_rfc3339, buf)?;
    buf.extend_from_slice(br#","record_count":"#);
    write!(buf, "{}", decoded.records.len())?;
    buf.extend_from_slice(br#","flows":["#);
    for (i, r) in decoded.records.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        D::render_canonical(r, buf)?;
    }
    buf.extend_from_slice(b"]}");
    Ok(())
}

fn render_canonical_with_raw<D: FlowDecoder>(
    decoded: &DecodedPacket<D::Record>,
    now_rfc3339: &str,
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    write_envelope_head::<D>(decoded, now_rfc3339, buf)?;
    buf.extend_from_slice(br#","record_count":"#);
    write!(buf, "{}", decoded.records.len())?;
    buf.extend_from_slice(br#","flows":["#);
    for (i, r) in decoded.records.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        D::render_canonical(r, buf)?;
    }
    buf.extend_from_slice(br#"],"raw":["#);
    for (i, r) in decoded.records.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        D::render_raw(r, buf)?;
    }
    buf.extend_from_slice(b"]}");
    Ok(())
}

fn render_exploded_record<D: FlowDecoder>(
    decoded: &DecodedPacket<D::Record>,
    record: &D::Record,
    record_index: usize,
    now_rfc3339: &str,
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    write_envelope_head::<D>(decoded, now_rfc3339, buf)?;
    buf.extend_from_slice(br#","record_count":1,"record_index":"#);
    write!(buf, "{}", record_index)?;
    buf.extend_from_slice(br#","record_total":"#);
    write!(buf, "{}", decoded.records.len())?;
    buf.extend_from_slice(br#","flow":"#);
    D::render_canonical(record, buf)?;
    buf.push(b'}');
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::flow::decoder::DecodedPacket;
    use crate::server::flow::dispatch::ProtocolKind;
    use crate::server::sflow::decoder::{SflowDecoder, SflowRecord};

    fn fake_decoded_sflow_empty() -> DecodedPacket<SflowRecord> {
        DecodedPacket {
            exporter_ip: "10.0.0.1".parse().unwrap(),
            observation_domain: 0,
            packet_seq: 42,
            kind: ProtocolKind::SflowV5,
            records: vec![],
        }
    }

    #[test]
    fn canonical_mode_one_event_with_empty_flows_array() {
        let decoded = fake_decoded_sflow_empty();
        let mut buf = Vec::new();
        let ranges = render_packet::<SflowDecoder>(
            &decoded,
            OutputMode::Canonical,
            "2026-05-20T00:00:00Z",
            &mut buf,
        )
        .unwrap();
        assert_eq!(ranges.len(), 1);
        let json = std::str::from_utf8(&buf[ranges[0].clone()]).unwrap();
        assert!(json.contains(r#""flows":[]"#), "json: {json}");
        assert!(json.contains(r#""record_count":0"#));
        assert!(json.contains(r#""packet_seq":42"#));
        assert!(json.contains(r#""exporter_ip":"10.0.0.1""#));
        assert!(json.contains(r#""t_collected":"2026-05-20T00:00:00Z""#));
        assert!(json.contains(r#""_source":"sflow""#));
        assert!(json.contains(r#""version":"sflow_v5""#));
    }

    #[test]
    fn canonical_with_raw_includes_raw_array() {
        let decoded = fake_decoded_sflow_empty();
        let mut buf = Vec::new();
        let ranges = render_packet::<SflowDecoder>(
            &decoded,
            OutputMode::CanonicalWithRaw,
            "2026-05-20T00:00:00Z",
            &mut buf,
        )
        .unwrap();
        assert_eq!(ranges.len(), 1);
        let json = std::str::from_utf8(&buf[ranges[0].clone()]).unwrap();
        assert!(json.contains(r#""flows":[]"#));
        assert!(json.contains(r#""raw":[]"#));
    }

    #[test]
    fn exploded_mode_emits_one_event_per_record() {
        let mut decoded = fake_decoded_sflow_empty();
        decoded.records.push(SflowRecord::Counter {
            generic: None,
            raw_json: "{}".into(),
        });
        decoded.records.push(SflowRecord::Counter {
            generic: None,
            raw_json: "{}".into(),
        });
        let mut buf = Vec::new();
        let ranges = render_packet::<SflowDecoder>(
            &decoded,
            OutputMode::Exploded,
            "2026-05-20T00:00:00Z",
            &mut buf,
        )
        .unwrap();
        assert_eq!(ranges.len(), 2);
        let e0 = std::str::from_utf8(&buf[ranges[0].clone()]).unwrap();
        let e1 = std::str::from_utf8(&buf[ranges[1].clone()]).unwrap();
        assert!(e0.contains(r#""record_index":0"#));
        assert!(e1.contains(r#""record_index":1"#));
        assert!(e0.contains(r#""record_total":2"#));
        assert!(e0.contains(r#""flow":"#));
    }

    #[test]
    fn exploded_mode_with_empty_records_emits_zero_events() {
        let decoded = fake_decoded_sflow_empty();
        let mut buf = Vec::new();
        let ranges = render_packet::<SflowDecoder>(
            &decoded,
            OutputMode::Exploded,
            "2026-05-20T00:00:00Z",
            &mut buf,
        )
        .unwrap();
        assert_eq!(ranges.len(), 0);
    }
}
