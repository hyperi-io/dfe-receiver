//  Project:      dfe-receiver
//  File:         src/server/sflow/tests/decode_flow.rs
//  Purpose:      Flow-sample decode tests using a hand-crafted sFlow v5 packet
//  Language:     Rust
//
//  License:      BUSL-1.1
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! Hand-crafted sFlow v5 flow-sample packets. We don't ship PCAPs in the test
//! tree -- the wire layout is well-specified and deterministic, so building
//! from bytes is faster and more portable than capturing live traffic.

use crate::server::flow::decoder::FlowDecoder;
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::sflow::decoder::SflowDecoder;
use serde_json::Value;
use std::net::IpAddr;

/// Ethernet + IPv4 + TCP minimal header (54 bytes) carrying recognisable
/// src/dst addresses, ports, and TCP flags for canonical-field assertions.
fn build_eth_ipv4_tcp_header() -> Vec<u8> {
    let mut hdr = Vec::with_capacity(54);
    // Ethernet (14 bytes)
    hdr.extend_from_slice(&[0xAA; 6]); // dst MAC
    hdr.extend_from_slice(&[0xBB; 6]); // src MAC
    hdr.extend_from_slice(&[0x08, 0x00]); // ethertype IPv4
    // IPv4 (20 bytes)
    hdr.push(0x45); // version=4, IHL=5
    hdr.push(0x00); // DSCP/ECN
    hdr.extend_from_slice(&40u16.to_be_bytes()); // total length
    hdr.extend_from_slice(&[0, 0, 0, 0]); // id, flags, frag
    hdr.push(64); // TTL
    hdr.push(6); // protocol = TCP
    hdr.extend_from_slice(&[0, 0]); // checksum
    hdr.extend_from_slice(&[10, 0, 0, 1]); // src ip
    hdr.extend_from_slice(&[10, 0, 0, 2]); // dst ip
    // TCP (20 bytes)
    hdr.extend_from_slice(&12345u16.to_be_bytes()); // src port
    hdr.extend_from_slice(&80u16.to_be_bytes()); // dst port
    hdr.extend_from_slice(&[0; 8]); // seq, ack
    hdr.push(0x50); // data offset
    hdr.push(0x18); // flags: PSH+ACK
    hdr.extend_from_slice(&[0; 6]); // window, checksum, urgent
    hdr
}

/// Build a complete sFlow v5 datagram containing one flow_sample with one
/// sampled_header (the eth+ipv4+tcp header).
fn build_flow_sample_packet() -> Vec<u8> {
    let hdr = build_eth_ipv4_tcp_header();

    let mut flow_record_body = Vec::new();
    flow_record_body.extend_from_slice(&1u32.to_be_bytes()); // header protocol = ethernet
    flow_record_body.extend_from_slice(&54u32.to_be_bytes()); // frame_length
    flow_record_body.extend_from_slice(&0u32.to_be_bytes()); // stripped
    flow_record_body.extend_from_slice(&(hdr.len() as u32).to_be_bytes()); // header_length
    flow_record_body.extend_from_slice(&hdr);

    let mut flow_record = Vec::new();
    flow_record.extend_from_slice(&1u32.to_be_bytes()); // format = sampled_header
    flow_record.extend_from_slice(&(flow_record_body.len() as u32).to_be_bytes());
    flow_record.extend_from_slice(&flow_record_body);

    let mut flow_sample_body = Vec::new();
    flow_sample_body.extend_from_slice(&1u32.to_be_bytes()); // sequence_number
    flow_sample_body.extend_from_slice(&0x_0100_0001u32.to_be_bytes()); // source_id
    flow_sample_body.extend_from_slice(&1000u32.to_be_bytes()); // sampling_rate
    flow_sample_body.extend_from_slice(&5000u32.to_be_bytes()); // sample_pool
    flow_sample_body.extend_from_slice(&0u32.to_be_bytes()); // drops
    flow_sample_body.extend_from_slice(&7u32.to_be_bytes()); // input_iface
    flow_sample_body.extend_from_slice(&9u32.to_be_bytes()); // output_iface
    flow_sample_body.extend_from_slice(&1u32.to_be_bytes()); // num_records
    flow_sample_body.extend_from_slice(&flow_record);

    let mut sample = Vec::new();
    sample.extend_from_slice(&1u32.to_be_bytes()); // format = flow_sample
    sample.extend_from_slice(&(flow_sample_body.len() as u32).to_be_bytes());
    sample.extend_from_slice(&flow_sample_body);

    let mut pkt = Vec::new();
    pkt.extend_from_slice(&5u32.to_be_bytes()); // version
    pkt.extend_from_slice(&1u32.to_be_bytes()); // agent_address_type = IPv4
    pkt.extend_from_slice(&[10, 0, 0, 1]); // agent ip
    pkt.extend_from_slice(&7u32.to_be_bytes()); // sub_agent_id
    pkt.extend_from_slice(&100u32.to_be_bytes()); // sequence_number
    pkt.extend_from_slice(&1_000_000u32.to_be_bytes()); // uptime
    pkt.extend_from_slice(&1u32.to_be_bytes()); // num_samples
    pkt.extend_from_slice(&sample);
    pkt
}

#[test]
fn decodes_flow_sample_to_canonical() {
    let pkt = build_flow_sample_packet();
    let mut dec = SflowDecoder::new();
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let decoded = dec
        .decode(&pkt, exporter, ProtocolKind::SflowV5)
        .expect("flow sample decode");
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(decoded.kind, ProtocolKind::SflowV5);
    assert_eq!(decoded.packet_seq, 100);
    assert_eq!(decoded.observation_domain, 7); // sub_agent_id
    assert_eq!(SflowDecoder::record_kind(&decoded.records[0]), "flow");
}

#[test]
fn flow_sample_render_canonical_json_shape() {
    let pkt = build_flow_sample_packet();
    let mut dec = SflowDecoder::new();
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let decoded = dec
        .decode(&pkt, exporter, ProtocolKind::SflowV5)
        .expect("decode");
    let mut buf = Vec::new();
    SflowDecoder::render_canonical(&decoded.records[0], &mut buf).expect("render");
    let parsed: Value = serde_json::from_slice(&buf).expect("json parses");

    assert_eq!(parsed["record_kind"], "flow");
    assert_eq!(parsed["sampling_rate"], 1000);
    assert_eq!(parsed["input_iface"], 7);
    assert_eq!(parsed["output_iface"], 9);
    assert_eq!(parsed["src_ip"], "10.0.0.1");
    assert_eq!(parsed["dst_ip"], "10.0.0.2");
    assert_eq!(parsed["src_port"], 12345);
    assert_eq!(parsed["dst_port"], 80);
    assert_eq!(parsed["protocol"], 6);
    assert_eq!(parsed["ip_version"], 4);
    assert_eq!(parsed["tcp_flags"], 0x18);
}

#[test]
fn flow_sample_render_raw_is_json() {
    let pkt = build_flow_sample_packet();
    let mut dec = SflowDecoder::new();
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let decoded = dec
        .decode(&pkt, exporter, ProtocolKind::SflowV5)
        .expect("decode");
    let mut buf = Vec::new();
    SflowDecoder::render_raw(&decoded.records[0], &mut buf).expect("render");
    let parsed: Value = serde_json::from_slice(&buf).expect("raw json parses");
    assert_eq!(parsed["kind"], "flow_sample");
    assert_eq!(parsed["sampling_rate"], 1000);
    assert_eq!(parsed["input_iface"], 7);
}

#[test]
fn rejects_wrong_protocol_kind() {
    let pkt = build_flow_sample_packet();
    let mut dec = SflowDecoder::new();
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let err = dec
        .decode(&pkt, exporter, ProtocolKind::NetflowV5)
        .expect_err("wrong kind must error");
    assert!(format!("{err}").contains("wrong protocol kind"));
}
