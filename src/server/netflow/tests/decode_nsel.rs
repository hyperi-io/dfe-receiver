//  Project:      dfe-receiver
//  File:         src/server/netflow/tests/decode_nsel.rs
//  Purpose:      NSEL (security_event) discriminator decode test
//  Language:     Rust
//
//  License:      FSL-1.1-ALv2
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! Tests the NSEL discriminator path in `NetflowDecoder`.
//!
//! When a v9/IPFIX record declares the IANA `firewallEvent` IE (233), the
//! decoder must flip `record_kind` from "flow" to "security_event" and
//! populate `event_type` from the IE's u8 value.

use crate::server::flow::decoder::FlowDecoder;
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::netflow::decoder::NetflowDecoder;
use bytes::BytesMut;
use chrono::{TimeZone, Utc};
use netgauze_flow_pkt::codec::FlowInfoCodec;
use netgauze_flow_pkt::netflow::{DataRecord, NetFlowV9Packet, Set as NfSet, TemplateRecord};
use netgauze_flow_pkt::{DataSetId, FieldSpecifier, FlowInfo, ie};
use serde_json::Value;
use std::net::{IpAddr, Ipv4Addr};
use tokio_util::codec::{Decoder, Encoder};

const TEMPLATE_ID: u16 = 500;

/// Build a v9 packet pair where the template includes `firewallEvent` and
/// the data record carries event_type = 3 (Flow Denied).
fn build_nsel_template_and_data() -> (Vec<u8>, Vec<u8>) {
    let template = NetFlowV9Packet::new(
        12_345,
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        100,
        7,
        Box::new([NfSet::Template(Box::new([TemplateRecord::new(
            TEMPLATE_ID,
            Box::new([
                FieldSpecifier::new(ie::IE::sourceIPv4Address, 4).unwrap(),
                FieldSpecifier::new(ie::IE::destinationIPv4Address, 4).unwrap(),
                FieldSpecifier::new(ie::IE::protocolIdentifier, 1).unwrap(),
                FieldSpecifier::new(ie::IE::firewallEvent, 1).unwrap(),
            ]),
        )]))]),
    );

    let data = NetFlowV9Packet::new(
        12_346,
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 1).unwrap(),
        101,
        7,
        Box::new([NfSet::Data {
            id: DataSetId::new(TEMPLATE_ID).unwrap(),
            records: Box::new([DataRecord::new(
                Box::new([]),
                Box::new([
                    ie::Field::sourceIPv4Address(Ipv4Addr::new(10, 1, 1, 1)),
                    ie::Field::destinationIPv4Address(Ipv4Addr::new(8, 8, 8, 8)),
                    ie::Field::protocolIdentifier(ie::protocolIdentifier::UDP),
                    ie::Field::firewallEvent(ie::firewallEvent::FlowDenied),
                ]),
            )]),
        }]),
    );

    let mut encoder = FlowInfoCodec::new();
    let mut tpl_buf = BytesMut::new();
    encoder
        .encode(FlowInfo::NetFlowV9(template), &mut tpl_buf)
        .expect("encode template");
    let mut decode_buf = BytesMut::from(&tpl_buf[..]);
    let _ = encoder
        .decode(&mut decode_buf)
        .expect("template decode populates encoder map");
    let mut data_buf = BytesMut::new();
    encoder
        .encode(FlowInfo::NetFlowV9(data), &mut data_buf)
        .expect("encode data");
    (tpl_buf.to_vec(), data_buf.to_vec())
}

#[test]
fn firewall_event_ie_flips_record_kind_to_security_event() {
    let (tpl, data) = build_nsel_template_and_data();
    let mut dec = NetflowDecoder::new(
        1000,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "203.0.113.1".parse().unwrap();
    dec.decode(&tpl, exporter, ProtocolKind::NetflowV9)
        .expect("template decode");
    let decoded = dec
        .decode(&data, exporter, ProtocolKind::NetflowV9)
        .expect("data decode");
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(
        NetflowDecoder::record_kind(&decoded.records[0]),
        "security_event"
    );

    let mut buf = Vec::new();
    NetflowDecoder::render_canonical(&decoded.records[0], &mut buf).expect("canonical render");
    let parsed: Value = serde_json::from_slice(&buf).expect("canonical JSON parses");

    assert_eq!(parsed["record_kind"], "security_event");
    // firewallEvent::FlowDenied = 3.
    assert_eq!(parsed["event_type"], 3);
    // Core flow fields still populate alongside the NSEL flag.
    assert_eq!(parsed["src_ip"], "10.1.1.1");
    assert_eq!(parsed["dst_ip"], "8.8.8.8");
    assert_eq!(parsed["protocol"], 17);
}
