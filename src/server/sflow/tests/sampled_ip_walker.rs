//  Project:      dfe-receiver
//  File:         src/server/sflow/tests/sampled_ip_walker.rs
//  Purpose:      Tests for parse_sampled_ip Ethernet/IP/TCP/UDP walker
//  Language:     Rust
//
//  License:      FSL-1.1-ALv2
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! The walker translates raw L2/L3/L4 bytes from an sFlow sampled_header into
//! canonical (src_ip, dst_ip, src_port, dst_port, protocol, ip_version,
//! tcp_flags). These tests exercise the common cases: Ethernet+IPv4+TCP,
//! Ethernet+IPv4+UDP, Ethernet+IPv6+TCP, 802.1Q VLAN tag, and rejection of
//! short headers.

// We test parse_sampled_ip indirectly via the decoder's public `decode` path:
// build a packet that wraps the header we want to verify, decode it, then read
// the canonical-render fields. This keeps `parse_sampled_ip` private to the
// decoder module while still giving the walker behavioural coverage.

use crate::server::flow::decoder::FlowDecoder;
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::sflow::decoder::SflowDecoder;
use serde_json::Value;
use std::net::IpAddr;

/// Wrap a raw L2/L3/L4 header inside a one-flow-sample sFlow v5 datagram
/// and return the canonical-render JSON. `header_protocol` is the sFlow
/// sampled_header.protocol value (1 = Ethernet, 11 = IPv4, 12 = IPv6).
fn canonical_for_header(header_protocol: u32, hdr: &[u8]) -> Value {
    let mut flow_record_body = Vec::new();
    flow_record_body.extend_from_slice(&header_protocol.to_be_bytes());
    flow_record_body.extend_from_slice(&(hdr.len() as u32).to_be_bytes()); // frame_length
    flow_record_body.extend_from_slice(&0u32.to_be_bytes()); // stripped
    flow_record_body.extend_from_slice(&(hdr.len() as u32).to_be_bytes()); // header_length
    flow_record_body.extend_from_slice(hdr);

    let mut flow_record = Vec::new();
    flow_record.extend_from_slice(&1u32.to_be_bytes()); // format = sampled_header
    flow_record.extend_from_slice(&(flow_record_body.len() as u32).to_be_bytes());
    flow_record.extend_from_slice(&flow_record_body);

    let mut flow_sample_body = Vec::new();
    flow_sample_body.extend_from_slice(&1u32.to_be_bytes()); // seq
    flow_sample_body.extend_from_slice(&0u32.to_be_bytes()); // source_id
    flow_sample_body.extend_from_slice(&1000u32.to_be_bytes()); // sampling_rate
    flow_sample_body.extend_from_slice(&0u32.to_be_bytes()); // sample_pool
    flow_sample_body.extend_from_slice(&0u32.to_be_bytes()); // drops
    flow_sample_body.extend_from_slice(&1u32.to_be_bytes()); // input
    flow_sample_body.extend_from_slice(&2u32.to_be_bytes()); // output
    flow_sample_body.extend_from_slice(&1u32.to_be_bytes()); // num_records
    flow_sample_body.extend_from_slice(&flow_record);

    let mut sample = Vec::new();
    sample.extend_from_slice(&1u32.to_be_bytes());
    sample.extend_from_slice(&(flow_sample_body.len() as u32).to_be_bytes());
    sample.extend_from_slice(&flow_sample_body);

    let mut pkt = Vec::new();
    pkt.extend_from_slice(&5u32.to_be_bytes());
    pkt.extend_from_slice(&1u32.to_be_bytes()); // IPv4 agent
    pkt.extend_from_slice(&[10, 0, 0, 1]);
    pkt.extend_from_slice(&7u32.to_be_bytes());
    pkt.extend_from_slice(&100u32.to_be_bytes());
    pkt.extend_from_slice(&1_000u32.to_be_bytes());
    pkt.extend_from_slice(&1u32.to_be_bytes());
    pkt.extend_from_slice(&sample);

    let mut dec = SflowDecoder::new();
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let decoded = dec
        .decode(&pkt, exporter, ProtocolKind::SflowV5)
        .expect("decode succeeds");
    let mut buf = Vec::new();
    SflowDecoder::render_canonical(&decoded.records[0], &mut buf).expect("render");
    serde_json::from_slice(&buf).expect("json parses")
}

fn eth_header(ethertype: u16) -> Vec<u8> {
    let mut v = Vec::with_capacity(14);
    v.extend_from_slice(&[0xAA; 6]);
    v.extend_from_slice(&[0xBB; 6]);
    v.extend_from_slice(&ethertype.to_be_bytes());
    v
}

fn ipv4_header(proto: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(20);
    v.push(0x45);
    v.push(0x00);
    v.extend_from_slice(&40u16.to_be_bytes());
    v.extend_from_slice(&[0, 0, 0, 0]);
    v.push(64);
    v.push(proto);
    v.extend_from_slice(&[0, 0]); // checksum
    v.extend_from_slice(&[10, 0, 0, 1]);
    v.extend_from_slice(&[10, 0, 0, 2]);
    v
}

fn ipv6_header(next_hdr: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(40);
    v.push(0x60); // version=6
    v.extend_from_slice(&[0, 0, 0]); // traffic class, flow label
    v.extend_from_slice(&20u16.to_be_bytes()); // payload length
    v.push(next_hdr);
    v.push(64); // hop limit
    // src + dst v6
    let src: std::net::Ipv6Addr = "2001:db8::1".parse().unwrap();
    let dst: std::net::Ipv6Addr = "2001:db8::2".parse().unwrap();
    v.extend_from_slice(&src.octets());
    v.extend_from_slice(&dst.octets());
    v
}

fn tcp_header(src_port: u16, dst_port: u16, flags: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(20);
    v.extend_from_slice(&src_port.to_be_bytes());
    v.extend_from_slice(&dst_port.to_be_bytes());
    v.extend_from_slice(&[0; 8]); // seq + ack
    v.push(0x50); // data offset
    v.push(flags);
    v.extend_from_slice(&[0; 6]); // window + checksum + urgent
    v
}

fn udp_header(src_port: u16, dst_port: u16) -> Vec<u8> {
    let mut v = Vec::with_capacity(8);
    v.extend_from_slice(&src_port.to_be_bytes());
    v.extend_from_slice(&dst_port.to_be_bytes());
    v.extend_from_slice(&8u16.to_be_bytes()); // length
    v.extend_from_slice(&[0, 0]); // checksum
    v
}

#[test]
fn parses_ipv4_tcp_header() {
    let mut hdr = eth_header(0x0800);
    hdr.extend_from_slice(&ipv4_header(6));
    hdr.extend_from_slice(&tcp_header(12345, 80, 0x18));
    let j = canonical_for_header(1, &hdr);
    assert_eq!(j["src_ip"], "10.0.0.1");
    assert_eq!(j["dst_ip"], "10.0.0.2");
    assert_eq!(j["src_port"], 12345);
    assert_eq!(j["dst_port"], 80);
    assert_eq!(j["protocol"], 6);
    assert_eq!(j["ip_version"], 4);
    assert_eq!(j["tcp_flags"], 0x18);
}

#[test]
fn parses_ipv4_udp_header() {
    let mut hdr = eth_header(0x0800);
    hdr.extend_from_slice(&ipv4_header(17));
    hdr.extend_from_slice(&udp_header(53, 9999));
    let j = canonical_for_header(1, &hdr);
    assert_eq!(j["protocol"], 17);
    assert_eq!(j["src_port"], 53);
    assert_eq!(j["dst_port"], 9999);
    // tcp_flags must not be set for UDP.
    assert!(j.get("tcp_flags").map_or(true, Value::is_null));
}

#[test]
fn parses_ipv6_tcp_header() {
    let mut hdr = eth_header(0x86DD);
    hdr.extend_from_slice(&ipv6_header(6));
    hdr.extend_from_slice(&tcp_header(443, 64500, 0x02)); // SYN
    let j = canonical_for_header(1, &hdr);
    assert_eq!(j["src_ip"], "2001:db8::1");
    assert_eq!(j["dst_ip"], "2001:db8::2");
    assert_eq!(j["protocol"], 6);
    assert_eq!(j["ip_version"], 6);
    assert_eq!(j["src_port"], 443);
    assert_eq!(j["dst_port"], 64500);
    assert_eq!(j["tcp_flags"], 0x02);
}

#[test]
fn parses_8021q_vlan_then_ipv4() {
    // Eth dst+src + ethertype 0x8100 (VLAN) + TCI + inner ethertype 0x0800
    let mut hdr = Vec::new();
    hdr.extend_from_slice(&[0xAA; 6]);
    hdr.extend_from_slice(&[0xBB; 6]);
    hdr.extend_from_slice(&[0x81, 0x00]); // VLAN TPID
    hdr.extend_from_slice(&[0x00, 0x64]); // TCI (VLAN ID 100)
    hdr.extend_from_slice(&[0x08, 0x00]); // inner ethertype IPv4
    hdr.extend_from_slice(&ipv4_header(6));
    hdr.extend_from_slice(&tcp_header(1111, 2222, 0x10));
    let j = canonical_for_header(1, &hdr);
    assert_eq!(j["src_ip"], "10.0.0.1");
    assert_eq!(j["dst_ip"], "10.0.0.2");
    assert_eq!(j["src_port"], 1111);
    assert_eq!(j["dst_port"], 2222);
    assert_eq!(j["ip_version"], 4);
}

#[test]
fn rejects_too_short_header() {
    // 8 bytes is shorter than even the Ethernet header (14). The walker
    // should leave src_ip/dst_ip unset; the surrounding canonical record
    // still serialises but without those fields.
    let hdr = vec![0u8; 8];
    let j = canonical_for_header(1, &hdr);
    assert!(j.get("src_ip").map_or(true, Value::is_null));
    assert!(j.get("dst_ip").map_or(true, Value::is_null));
    // sampling_rate / input_iface / output_iface still come through.
    assert_eq!(j["sampling_rate"], 1000);
}
