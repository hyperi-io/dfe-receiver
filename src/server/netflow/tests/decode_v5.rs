//  Project:      dfe-receiver
//  File:         src/server/netflow/tests/decode_v5.rs
//  Purpose:      NetFlow v5 decode tests using a hand-crafted fixture
//  Language:     Rust
//
//  License:      BUSL-1.1
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! Tests for `NetflowDecoder` on NetFlow v5 datagrams.
//!
//! Uses a hand-crafted v5 packet rather than a captured PCAP -- v5 has a
//! fixed wire layout (24-byte header + N x 48-byte records per the original
//! Cisco specification) so a hand-crafted fixture is deterministic, fast,
//! and portable across environments without Docker or live network capture.

use crate::server::flow::decoder::FlowDecoder;
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::netflow::decoder::NetflowDecoder;
use serde_json::Value;
use std::net::IpAddr;

/// Build a valid NetFlow v5 datagram with one flow record.
///
/// Header (24 bytes) + 1 record (48 bytes) = 72 bytes total.
fn build_v5_packet_with_one_flow() -> Vec<u8> {
    let mut pkt = vec![0u8; 72];

    // Header
    pkt[0..2].copy_from_slice(&5u16.to_be_bytes()); // version
    pkt[2..4].copy_from_slice(&1u16.to_be_bytes()); // count = 1
    pkt[4..8].copy_from_slice(&1000u32.to_be_bytes()); // sys_uptime ms
    pkt[8..12].copy_from_slice(&1_700_000_000u32.to_be_bytes()); // unix_secs
    pkt[12..16].copy_from_slice(&0u32.to_be_bytes()); // unix_nsecs
    pkt[16..20].copy_from_slice(&42u32.to_be_bytes()); // flow_sequence
    pkt[20] = 0; // engine_type
    pkt[21] = 0; // engine_id
    pkt[22..24].copy_from_slice(&0u16.to_be_bytes()); // sampling_interval

    // Record (offset 24, 48 bytes)
    pkt[24..28].copy_from_slice(&[10, 0, 0, 1]); // srcaddr  10.0.0.1
    pkt[28..32].copy_from_slice(&[10, 0, 0, 2]); // dstaddr  10.0.0.2
    pkt[32..36].copy_from_slice(&[0, 0, 0, 0]); // nexthop 0.0.0.0 (unspecified)
    pkt[36..38].copy_from_slice(&7u16.to_be_bytes()); // input  iface 7
    pkt[38..40].copy_from_slice(&9u16.to_be_bytes()); // output iface 9
    pkt[40..44].copy_from_slice(&5u32.to_be_bytes()); // dPkts   = 5
    pkt[44..48].copy_from_slice(&1500u32.to_be_bytes()); // dOctets = 1500
    pkt[48..52].copy_from_slice(&500u32.to_be_bytes()); // first
    pkt[52..56].copy_from_slice(&800u32.to_be_bytes()); // last
    pkt[56..58].copy_from_slice(&12345u16.to_be_bytes()); // srcport
    pkt[58..60].copy_from_slice(&80u16.to_be_bytes()); // dstport
    pkt[60] = 0; // pad1
    pkt[61] = 0x18; // tcp_flags (ACK + PSH)
    pkt[62] = 6; // protocol (TCP)
    pkt[63] = 0; // tos
    pkt[64..66].copy_from_slice(&64500u16.to_be_bytes()); // src_as
    pkt[66..68].copy_from_slice(&64501u16.to_be_bytes()); // dst_as
    pkt[68] = 24; // src_mask
    pkt[69] = 24; // dst_mask
    pkt[70..72].copy_from_slice(&0u16.to_be_bytes()); // pad2

    pkt
}

#[test]
fn decodes_v5_handcrafted_packet() {
    let pkt = build_v5_packet_with_one_flow();
    let mut dec = NetflowDecoder::new(
        1000,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let decoded = dec
        .decode(&pkt, exporter, ProtocolKind::NetflowV5)
        .expect("v5 decode succeeds");
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(decoded.kind, ProtocolKind::NetflowV5);
    assert_eq!(decoded.packet_seq, 42);
    assert_eq!(decoded.exporter_ip, exporter);
}

#[test]
fn v5_canonical_render_contains_known_fields() {
    let pkt = build_v5_packet_with_one_flow();
    let mut dec = NetflowDecoder::new(
        1000,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let decoded = dec
        .decode(&pkt, exporter, ProtocolKind::NetflowV5)
        .expect("v5 decode succeeds");
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
    assert_eq!(parsed["src_port"], 12345);
    assert_eq!(parsed["dst_port"], 80);
    assert_eq!(parsed["tcp_flags"], 0x18);
    assert_eq!(parsed["input_iface"], 7);
    assert_eq!(parsed["output_iface"], 9);
    assert_eq!(parsed["src_as"], 64500);
    assert_eq!(parsed["dst_as"], 64501);
    assert_eq!(parsed["ip_version"], 4);
    // next_hop was 0.0.0.0 (unspecified) -- canonical mapping skips it.
    assert!(parsed.get("next_hop").map_or(true, Value::is_null));
}

#[test]
fn v5_raw_render_round_trips_to_json() {
    let pkt = build_v5_packet_with_one_flow();
    let mut dec = NetflowDecoder::new(
        1000,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let decoded = dec
        .decode(&pkt, exporter, ProtocolKind::NetflowV5)
        .expect("v5 decode succeeds");
    let mut buf = Vec::new();
    NetflowDecoder::render_raw(&decoded.records[0], &mut buf).expect("raw render writes");
    let parsed: Value = serde_json::from_slice(&buf).expect("raw JSON parses");
    assert_eq!(parsed["version"], 5);
    assert_eq!(parsed["src_addr"], "10.0.0.1");
    assert_eq!(parsed["dst_addr"], "10.0.0.2");
    assert_eq!(parsed["protocol"], 6);
    assert_eq!(parsed["d_octets"], 1500);
    assert_eq!(parsed["d_pkts"], 5);
}

#[test]
fn rejects_v5_packet_shorter_than_header() {
    let pkt = vec![0x00, 0x05, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00];
    let mut dec = NetflowDecoder::new(
        1000,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let err = dec
        .decode(&pkt, exporter, ProtocolKind::NetflowV5)
        .expect_err("short packet must error");
    assert!(format!("{err}").contains("too short"));
}

#[test]
fn rejects_v5_packet_with_length_mismatch() {
    let mut pkt = build_v5_packet_with_one_flow();
    // Claim 3 flows but datagram only contains room for 1.
    pkt[2..4].copy_from_slice(&3u16.to_be_bytes());
    let mut dec = NetflowDecoder::new(
        1000,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let err = dec
        .decode(&pkt, exporter, ProtocolKind::NetflowV5)
        .expect_err("length mismatch must error");
    assert!(format!("{err}").contains("length mismatch"));
}

#[test]
fn rejects_wrong_protocol_kind() {
    let pkt = build_v5_packet_with_one_flow();
    let mut dec = NetflowDecoder::new(
        1000,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let err = dec
        .decode(&pkt, exporter, ProtocolKind::SflowV5)
        .expect_err("wrong kind must error");
    assert!(format!("{err}").contains("wrong protocol kind"));
}
