//  Project:      dfe-receiver
//  File:         src/server/netflow/tests/decode_nat44.rs
//  Purpose:      NAT44 (nat_translation) discriminator decode test
//  Language:     Rust
//
//  License:      BUSL-1.1
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! Tests the NAT44 discriminator path in `NetflowDecoder`.
//!
//! When an IPFIX record carries the IANA `natEvent` IE (230), the decoder
//! must flip `record_kind` from "flow" to "nat_translation" and populate
//! `nat_event_type` plus the pre-NAT 5-tuple fields from
//! `postNATSourceIPv4Address`, `postNAPTSourceTransportPort`, etc.

use crate::server::flow::decoder::FlowDecoder;
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::netflow::decoder::NetflowDecoder;
use bytes::BytesMut;
use chrono::{TimeZone, Utc};
use netgauze_flow_pkt::codec::FlowInfoCodec;
use netgauze_flow_pkt::ipfix::{DataRecord, IpfixPacket, Set as IpfixSet, TemplateRecord};
use netgauze_flow_pkt::{DataSetId, FieldSpecifier, FlowInfo, ie};
use serde_json::Value;
use std::net::{IpAddr, Ipv4Addr};
use tokio_util::codec::{Decoder, Encoder};

const TEMPLATE_ID: u16 = 600;

fn build_nat44_template_and_data() -> (Vec<u8>, Vec<u8>) {
    let template = IpfixPacket::new(
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        500,
        99,
        Box::new([IpfixSet::Template(Box::new([TemplateRecord::new(
            TEMPLATE_ID,
            Box::new([
                FieldSpecifier::new(ie::IE::sourceIPv4Address, 4).unwrap(),
                FieldSpecifier::new(ie::IE::destinationIPv4Address, 4).unwrap(),
                FieldSpecifier::new(ie::IE::sourceTransportPort, 2).unwrap(),
                FieldSpecifier::new(ie::IE::destinationTransportPort, 2).unwrap(),
                FieldSpecifier::new(ie::IE::protocolIdentifier, 1).unwrap(),
                FieldSpecifier::new(ie::IE::natEvent, 1).unwrap(),
                FieldSpecifier::new(ie::IE::postNATSourceIPv4Address, 4).unwrap(),
                FieldSpecifier::new(ie::IE::postNAPTSourceTransportPort, 2).unwrap(),
            ]),
        )]))]),
    );

    let data = IpfixPacket::new(
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 1).unwrap(),
        501,
        99,
        Box::new([IpfixSet::Data {
            id: DataSetId::new(TEMPLATE_ID).unwrap(),
            records: Box::new([DataRecord::new(
                Box::new([]),
                Box::new([
                    // Outer 5-tuple as the NAT exporter saw it on the WAN side.
                    ie::Field::sourceIPv4Address(Ipv4Addr::new(203, 0, 113, 5)),
                    ie::Field::destinationIPv4Address(Ipv4Addr::new(8, 8, 8, 8)),
                    ie::Field::sourceTransportPort(60_000),
                    ie::Field::destinationTransportPort(443),
                    ie::Field::protocolIdentifier(ie::protocolIdentifier::TCP),
                    // natEvent::NAT44sessioncreate = 4
                    ie::Field::natEvent(ie::natEvent::NAT44sessioncreate),
                    // Pre-NAT (private) subscriber address + port.
                    ie::Field::postNATSourceIPv4Address(Ipv4Addr::new(10, 0, 0, 42)),
                    ie::Field::postNAPTSourceTransportPort(54_321),
                ]),
            )]),
        }]),
    );

    let mut encoder = FlowInfoCodec::new();
    let mut tpl_buf = BytesMut::new();
    encoder
        .encode(FlowInfo::IPFIX(template), &mut tpl_buf)
        .expect("encode template");
    let mut decode_buf = BytesMut::from(&tpl_buf[..]);
    let _ = encoder
        .decode(&mut decode_buf)
        .expect("template decode populates encoder map");
    let mut data_buf = BytesMut::new();
    encoder
        .encode(FlowInfo::IPFIX(data), &mut data_buf)
        .expect("encode data");
    (tpl_buf.to_vec(), data_buf.to_vec())
}

#[test]
fn nat_event_ie_flips_record_kind_to_nat_translation() {
    let (tpl, data) = build_nat44_template_and_data();
    let mut dec = NetflowDecoder::new(
        1000,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "198.51.100.99".parse().unwrap();
    dec.decode(&tpl, exporter, ProtocolKind::Ipfix)
        .expect("template decode");
    let decoded = dec
        .decode(&data, exporter, ProtocolKind::Ipfix)
        .expect("data decode");
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(
        NetflowDecoder::record_kind(&decoded.records[0]),
        "nat_translation"
    );

    let mut buf = Vec::new();
    NetflowDecoder::render_canonical(&decoded.records[0], &mut buf).expect("canonical render");
    let parsed: Value = serde_json::from_slice(&buf).expect("canonical JSON parses");

    assert_eq!(parsed["record_kind"], "nat_translation");
    // natEvent::NAT44sessioncreate = 4
    assert_eq!(parsed["nat_event_type"], 4);
    assert_eq!(parsed["pre_nat_src_ip"], "10.0.0.42");
    assert_eq!(parsed["pre_nat_src_port"], 54_321);
    // Outer NAT view stays in the core flow fields.
    assert_eq!(parsed["src_ip"], "203.0.113.5");
    assert_eq!(parsed["src_port"], 60_000);
    assert_eq!(parsed["dst_ip"], "8.8.8.8");
    assert_eq!(parsed["protocol"], 6);
}
