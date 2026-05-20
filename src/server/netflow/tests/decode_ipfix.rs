//  Project:      dfe-receiver
//  File:         src/server/netflow/tests/decode_ipfix.rs
//  Purpose:      IPFIX (NetFlow v10) decode tests via netgauze-encoded packets
//  Language:     Rust
//
//  License:      FSL-1.1-ALv2
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! Tests for `NetflowDecoder` on IPFIX (NetFlow v10) datagrams.
//!
//! Same shape as the v9 tests but exercises the IPFIX wire format. The
//! IPFIX header is 16 bytes (versus v9's 20 bytes) and the "Length" field
//! holds the whole-message length rather than v9's record count, so the
//! same canonical assertions exercise a different decode path inside
//! netgauze.

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

const TEMPLATE_ID: u16 = 400;

fn build_ipfix_template_and_data() -> (Vec<u8>, Vec<u8>) {
    let template = IpfixPacket::new(
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        1000,
        42,
        Box::new([IpfixSet::Template(Box::new([TemplateRecord::new(
            TEMPLATE_ID,
            Box::new([
                FieldSpecifier::new(ie::IE::sourceIPv4Address, 4).unwrap(),
                FieldSpecifier::new(ie::IE::destinationIPv4Address, 4).unwrap(),
                FieldSpecifier::new(ie::IE::sourceTransportPort, 2).unwrap(),
                FieldSpecifier::new(ie::IE::destinationTransportPort, 2).unwrap(),
                FieldSpecifier::new(ie::IE::protocolIdentifier, 1).unwrap(),
                FieldSpecifier::new(ie::IE::octetDeltaCount, 8).unwrap(),
                FieldSpecifier::new(ie::IE::packetDeltaCount, 8).unwrap(),
            ]),
        )]))]),
    );

    let data = IpfixPacket::new(
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 1).unwrap(),
        1001,
        42,
        Box::new([IpfixSet::Data {
            id: DataSetId::new(TEMPLATE_ID).unwrap(),
            records: Box::new([DataRecord::new(
                Box::new([]),
                Box::new([
                    ie::Field::sourceIPv4Address(Ipv4Addr::new(192, 0, 2, 100)),
                    ie::Field::destinationIPv4Address(Ipv4Addr::new(203, 0, 113, 50)),
                    ie::Field::sourceTransportPort(45_678),
                    ie::Field::destinationTransportPort(443),
                    ie::Field::protocolIdentifier(ie::protocolIdentifier::TCP),
                    ie::Field::octetDeltaCount(8192),
                    ie::Field::packetDeltaCount(12),
                ]),
            )]),
        }]),
    );

    // Encode template, then decode it through the same codec to populate the
    // codec's templates_map so the data-packet encode honours per-field lengths.
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
fn decodes_ipfix_handcrafted_template_then_data() {
    let (tpl, data) = build_ipfix_template_and_data();
    let mut dec = NetflowDecoder::new(1000, 10_000, crate::server::flow::metrics::mock::flow_metrics_for_test());
    let exporter: IpAddr = "198.51.100.5".parse().unwrap();

    let tpl_decoded = dec
        .decode(&tpl, exporter, ProtocolKind::Ipfix)
        .expect("ipfix template decode");
    assert_eq!(tpl_decoded.records.len(), 0);
    assert_eq!(tpl_decoded.kind, ProtocolKind::Ipfix);
    assert_eq!(tpl_decoded.observation_domain, 42);

    let decoded = dec
        .decode(&data, exporter, ProtocolKind::Ipfix)
        .expect("ipfix data decode");
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(decoded.packet_seq, 1001);
}

#[test]
fn ipfix_canonical_render_carries_full_5tuple_and_counters() {
    let (tpl, data) = build_ipfix_template_and_data();
    let mut dec = NetflowDecoder::new(1000, 10_000, crate::server::flow::metrics::mock::flow_metrics_for_test());
    let exporter: IpAddr = "198.51.100.5".parse().unwrap();
    dec.decode(&tpl, exporter, ProtocolKind::Ipfix)
        .expect("template");
    let decoded = dec
        .decode(&data, exporter, ProtocolKind::Ipfix)
        .expect("data");

    let mut buf = Vec::new();
    NetflowDecoder::render_canonical(&decoded.records[0], &mut buf)
        .expect("canonical render writes JSON");
    let parsed: Value = serde_json::from_slice(&buf).expect("canonical JSON parses");

    assert_eq!(parsed["record_kind"], "flow");
    assert_eq!(parsed["src_ip"], "192.0.2.100");
    assert_eq!(parsed["dst_ip"], "203.0.113.50");
    assert_eq!(parsed["src_port"], 45_678);
    assert_eq!(parsed["dst_port"], 443);
    assert_eq!(parsed["protocol"], 6);
    assert_eq!(parsed["bytes"], 8192);
    assert_eq!(parsed["packets"], 12);
    assert_eq!(parsed["ip_version"], 4);
}
