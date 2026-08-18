//! Render a `DecodedPacket<R>` into JSON envelopes per the configured `OutputMode`.
//!
//! Both modes share top-level envelope fields (_source, protocol, version,
//! exporter_ip, observation_domain, packet_seq, t_collected). They differ in
//! whether they emit per-packet (Canonical) or per-record (Exploded).
//!
//! Raw retention is orthogonal to the mode: with `flow.raw_capture.enabled`,
//! each event also carries the decoder's verbatim record rendering in the
//! common-header `_raw` field -- a JSON array of every record in Canonical
//! mode, the single record in Exploded.

use crate::config::RawCapture;
use crate::server::flow::config::OutputMode;
use crate::server::flow::decoder::{DecodedPacket, FlowDecoder};
use crate::server::raw_capture;
use std::io::{self, Write};
use std::ops::Range;

/// Render `decoded` into one or more JSON events written into `buf`. Returns
/// the byte ranges within `buf` corresponding to each event.
///
/// `buf.clear()` is called inside; caller passes a reusable buffer.
pub fn render_packet<D: FlowDecoder>(
    decoded: &DecodedPacket<D::Record>,
    mode: OutputMode,
    raw: RawCapture,
    now_rfc3339: &str,
    buf: &mut Vec<u8>,
) -> io::Result<Vec<Range<usize>>> {
    buf.clear();
    let mut ranges = Vec::with_capacity(match mode {
        OutputMode::Canonical => 1,
        OutputMode::Exploded => decoded.records.len(),
    });

    // One scratch buffer for the whole packet, reused across records. Only
    // allocated when capture is on, and sized so the common case does not
    // grow it.
    let mut scratch = if raw.enabled {
        Vec::with_capacity(RAW_SCRATCH_HINT_PER_RECORD * decoded.records.len().max(1))
    } else {
        Vec::new()
    };

    match mode {
        OutputMode::Canonical => {
            let start = buf.len();
            render_canonical_packet::<D>(decoded, raw, now_rfc3339, buf, &mut scratch)?;
            ranges.push(start..buf.len());
        }
        OutputMode::Exploded => {
            for (i, record) in decoded.records.iter().enumerate() {
                let start = buf.len();
                render_exploded_record::<D>(
                    decoded,
                    record,
                    i,
                    raw,
                    now_rfc3339,
                    buf,
                    &mut scratch,
                )?;
                ranges.push(start..buf.len());
            }
        }
    }
    Ok(ranges)
}

/// Starting scratch capacity per flow record, in bytes.
const RAW_SCRATCH_HINT_PER_RECORD: usize = 256;

/// Render every record's verbatim form as a JSON array into `scratch`.
fn raw_records_array<D: FlowDecoder>(
    decoded: &DecodedPacket<D::Record>,
    scratch: &mut Vec<u8>,
) -> io::Result<()> {
    scratch.clear();
    scratch.push(b'[');
    for (i, r) in decoded.records.iter().enumerate() {
        if i > 0 {
            scratch.push(b',');
        }
        D::render_raw(r, scratch)?;
    }
    scratch.push(b']');
    Ok(())
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
    raw: RawCapture,
    now_rfc3339: &str,
    buf: &mut Vec<u8>,
    scratch: &mut Vec<u8>,
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
    buf.push(b']');
    if raw.enabled {
        raw_records_array::<D>(decoded, scratch)?;
        raw_capture::append_to_json_buf(buf, scratch, raw);
    }
    buf.push(b'}');
    Ok(())
}

fn render_exploded_record<D: FlowDecoder>(
    decoded: &DecodedPacket<D::Record>,
    record: &D::Record,
    record_index: usize,
    raw: RawCapture,
    now_rfc3339: &str,
    buf: &mut Vec<u8>,
    scratch: &mut Vec<u8>,
) -> io::Result<()> {
    write_envelope_head::<D>(decoded, now_rfc3339, buf)?;
    buf.extend_from_slice(br#","record_count":1,"record_index":"#);
    write!(buf, "{record_index}")?;
    buf.extend_from_slice(br#","record_total":"#);
    write!(buf, "{}", decoded.records.len())?;
    buf.extend_from_slice(br#","flow":"#);
    D::render_canonical(record, buf)?;
    if raw.enabled {
        scratch.clear();
        D::render_raw(record, scratch)?;
        raw_capture::append_to_json_buf(buf, scratch, raw);
    }
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
            RawCapture::OFF,
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
    fn canonical_without_capture_has_no_raw_field() {
        let mut decoded = fake_decoded_sflow_empty();
        decoded.records.push(SflowRecord::Counter {
            generic: None,
            raw_json: r#"{"kind":"counter"}"#.into(),
        });
        let mut buf = Vec::new();
        let ranges = render_packet::<SflowDecoder>(
            &decoded,
            OutputMode::Canonical,
            RawCapture::OFF,
            "2026-05-20T00:00:00Z",
            &mut buf,
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&buf[ranges[0].clone()]).unwrap();
        assert!(parsed.get("_raw").is_none());
    }

    #[test]
    fn canonical_capture_adds_raw_alongside_the_parsed_flows() {
        let mut decoded = fake_decoded_sflow_empty();
        decoded.records.push(SflowRecord::Counter {
            generic: None,
            raw_json: r#"{"kind":"counter","n":1}"#.into(),
        });
        decoded.records.push(SflowRecord::Counter {
            generic: None,
            raw_json: r#"{"kind":"counter","n":2}"#.into(),
        });
        let mut buf = Vec::new();
        let ranges = render_packet::<SflowDecoder>(
            &decoded,
            OutputMode::Canonical,
            RawCapture::on(),
            "2026-05-20T00:00:00Z",
            &mut buf,
        )
        .unwrap();
        assert_eq!(ranges.len(), 1);

        let parsed: serde_json::Value = serde_json::from_slice(&buf[ranges[0].clone()]).unwrap();
        // The parsed envelope is untouched; _raw is an addition to it.
        assert_eq!(parsed["record_count"], 2);
        assert_eq!(parsed["flows"].as_array().unwrap().len(), 2);
        assert_eq!(parsed["_source"], "sflow");

        // _raw is a STRING holding the verbatim record array, because the
        // common-header column it lands in is text, not JSON.
        let raw = parsed["_raw"].as_str().expect("_raw is a string");
        let inner: serde_json::Value = serde_json::from_str(raw).unwrap();
        assert_eq!(inner.as_array().unwrap().len(), 2);
        assert_eq!(inner[0]["n"], 1);
        assert_eq!(inner[1]["n"], 2);
    }

    #[test]
    fn exploded_capture_puts_one_record_in_each_events_raw() {
        let mut decoded = fake_decoded_sflow_empty();
        decoded.records.push(SflowRecord::Counter {
            generic: None,
            raw_json: r#"{"n":1}"#.into(),
        });
        decoded.records.push(SflowRecord::Counter {
            generic: None,
            raw_json: r#"{"n":2}"#.into(),
        });
        let mut buf = Vec::new();
        let ranges = render_packet::<SflowDecoder>(
            &decoded,
            OutputMode::Exploded,
            RawCapture::on(),
            "2026-05-20T00:00:00Z",
            &mut buf,
        )
        .unwrap();
        assert_eq!(ranges.len(), 2);

        for (i, range) in ranges.iter().enumerate() {
            let parsed: serde_json::Value = serde_json::from_slice(&buf[range.clone()]).unwrap();
            let raw = parsed["_raw"].as_str().expect("_raw is a string");
            let inner: serde_json::Value = serde_json::from_str(raw).unwrap();
            assert_eq!(inner["n"], i + 1);
        }
    }

    #[test]
    fn oversize_capture_truncates_and_flags_the_event() {
        let mut decoded = fake_decoded_sflow_empty();
        decoded.records.push(SflowRecord::Counter {
            generic: None,
            raw_json: r#"{"padding":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#.into(),
        });
        let mut buf = Vec::new();
        let ranges = render_packet::<SflowDecoder>(
            &decoded,
            OutputMode::Canonical,
            crate::config::RawCapture {
                enabled: true,
                max_bytes: 8,
                on_oversize: crate::config::OversizePolicy::Truncate,
            },
            "2026-05-20T00:00:00Z",
            &mut buf,
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&buf[ranges[0].clone()]).unwrap();
        assert_eq!(parsed["_raw_truncated"], true);
        assert_eq!(parsed["_raw"].as_str().unwrap().len(), 8);
        // Still a well-formed event even though _raw is now a JSON fragment.
        assert_eq!(parsed["record_count"], 1);
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
            RawCapture::OFF,
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
            RawCapture::OFF,
            "2026-05-20T00:00:00Z",
            &mut buf,
        )
        .unwrap();
        assert_eq!(ranges.len(), 0);
    }
}
