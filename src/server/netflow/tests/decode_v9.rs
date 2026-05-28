//  Project:      dfe-receiver
//  File:         src/server/netflow/tests/decode_v9.rs
//  Purpose:      NetFlow v9 decode tests using netgauze-encoded hand-crafted packets
//  Language:     Rust
//
//  License:      BUSL-1.1
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! Tests for `NetflowDecoder` on NetFlow v9 datagrams.
//!
//! v9 requires a template + data set, so each test builds the two packets
//! using netgauze's typed constructors and feeds them into the decoder in
//! order. The first packet primes the template cache; the second carries
//! the actual data record.

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

const TEMPLATE_ID: u16 = 307;

/// Build a (template_bytes, data_bytes) pair for a simple 4-field v9 flow:
/// src/dst IPv4, octets, packets.
fn build_v9_template_and_data() -> (Vec<u8>, Vec<u8>) {
    let template = NetFlowV9Packet::new(
        45_646,
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        3812,
        0,
        Box::new([NfSet::Template(Box::new([TemplateRecord::new(
            TEMPLATE_ID,
            Box::new([
                FieldSpecifier::new(ie::IE::sourceIPv4Address, 4).unwrap(),
                FieldSpecifier::new(ie::IE::destinationIPv4Address, 4).unwrap(),
                FieldSpecifier::new(ie::IE::protocolIdentifier, 1).unwrap(),
                FieldSpecifier::new(ie::IE::octetDeltaCount, 4).unwrap(),
                FieldSpecifier::new(ie::IE::packetDeltaCount, 4).unwrap(),
            ]),
        )]))]),
    );

    let data = NetFlowV9Packet::new(
        45_647,
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 1).unwrap(),
        3812,
        0,
        Box::new([NfSet::Data {
            id: DataSetId::new(TEMPLATE_ID).unwrap(),
            records: Box::new([DataRecord::new(
                Box::new([]),
                Box::new([
                    ie::Field::sourceIPv4Address(Ipv4Addr::new(10, 0, 0, 1)),
                    ie::Field::destinationIPv4Address(Ipv4Addr::new(10, 0, 0, 2)),
                    ie::Field::protocolIdentifier(ie::protocolIdentifier::TCP),
                    ie::Field::octetDeltaCount(1500),
                    ie::Field::packetDeltaCount(5),
                ]),
            )]),
        }]),
    );

    // FlowInfoCodec::encode reads its internal templates_map to honour the
    // template's per-field lengths. The map is populated by the decoder side,
    // not the encoder side, so we encode the template first then decode it
    // through the same codec instance to register the template, then encode
    // the data packet against the populated map.
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
fn decodes_v9_handcrafted_template_then_data() {
    let (tpl, data) = build_v9_template_and_data();
    let mut dec = NetflowDecoder::new(
        1000,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "192.0.2.10".parse().unwrap();

    // First packet primes the template cache; it contains no flow records.
    let tpl_decoded = dec
        .decode(&tpl, exporter, ProtocolKind::NetflowV9)
        .expect("template decode succeeds");
    assert_eq!(tpl_decoded.records.len(), 0);
    assert_eq!(tpl_decoded.kind, ProtocolKind::NetflowV9);

    // Second packet uses the cached template to materialise the record.
    let decoded = dec
        .decode(&data, exporter, ProtocolKind::NetflowV9)
        .expect("v9 data decode succeeds");
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(decoded.exporter_ip, exporter);
}

#[test]
fn v9_canonical_render_carries_core_flow_fields() {
    let (tpl, data) = build_v9_template_and_data();
    let mut dec = NetflowDecoder::new(
        1000,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "192.0.2.10".parse().unwrap();
    dec.decode(&tpl, exporter, ProtocolKind::NetflowV9)
        .expect("template decode");
    let decoded = dec
        .decode(&data, exporter, ProtocolKind::NetflowV9)
        .expect("data decode");

    let mut buf = Vec::new();
    NetflowDecoder::render_canonical(&decoded.records[0], &mut buf)
        .expect("canonical render writes JSON");
    let parsed: Value = serde_json::from_slice(&buf).expect("canonical JSON parses");

    assert_eq!(parsed["record_kind"], "flow");
    assert_eq!(parsed["src_ip"], "10.0.0.1");
    assert_eq!(parsed["dst_ip"], "10.0.0.2");
    assert_eq!(parsed["protocol"], 6);
    assert_eq!(parsed["bytes"], 1500);
    assert_eq!(parsed["packets"], 5);
    assert_eq!(parsed["ip_version"], 4);
}

#[test]
fn v9_record_kind_defaults_to_flow() {
    let (tpl, data) = build_v9_template_and_data();
    let mut dec = NetflowDecoder::new(
        1000,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "192.0.2.10".parse().unwrap();
    dec.decode(&tpl, exporter, ProtocolKind::NetflowV9)
        .expect("template decode");
    let decoded = dec
        .decode(&data, exporter, ProtocolKind::NetflowV9)
        .expect("data decode");
    assert_eq!(NetflowDecoder::record_kind(&decoded.records[0]), "flow");
}
