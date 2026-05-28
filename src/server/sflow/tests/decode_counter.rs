//  Project:      dfe-receiver
//  File:         src/server/sflow/tests/decode_counter.rs
//  Purpose:      Counter-sample decode tests using hand-crafted sFlow v5 packet
//  Language:     Rust
//
//  License:      BUSL-1.1
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! Counter samples carry interface-level IF-MIB-style metrics. The packet
//! builder below produces one generic_interface counter record; we assert
//! the canonical mapping pulls out `if_in_octets`, summed `if_in_packets`,
//! errors, discards, and that `record_kind` flips to "counter".

use crate::server::flow::decoder::FlowDecoder;
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::sflow::decoder::SflowDecoder;
use serde_json::Value;
use std::net::IpAddr;

fn build_counter_sample_packet() -> Vec<u8> {
    // generic_interface body
    let mut counter_body = Vec::new();
    counter_body.extend_from_slice(&5u32.to_be_bytes()); // if_index
    counter_body.extend_from_slice(&6u32.to_be_bytes()); // if_type (ethernetCsmacd)
    counter_body.extend_from_slice(&1_000_000_000u64.to_be_bytes()); // if_speed
    counter_body.extend_from_slice(&1u32.to_be_bytes()); // direction
    counter_body.extend_from_slice(&3u32.to_be_bytes()); // status
    counter_body.extend_from_slice(&999_888u64.to_be_bytes()); // if_in_octets
    counter_body.extend_from_slice(&100u32.to_be_bytes()); // in ucast
    counter_body.extend_from_slice(&10u32.to_be_bytes()); // in multicast
    counter_body.extend_from_slice(&5u32.to_be_bytes()); // in broadcast
    counter_body.extend_from_slice(&2u32.to_be_bytes()); // in discards
    counter_body.extend_from_slice(&1u32.to_be_bytes()); // in errors
    counter_body.extend_from_slice(&0u32.to_be_bytes()); // in unknown_protos
    counter_body.extend_from_slice(&777_666u64.to_be_bytes()); // if_out_octets
    counter_body.extend_from_slice(&90u32.to_be_bytes()); // out ucast
    counter_body.extend_from_slice(&5u32.to_be_bytes()); // out multicast
    counter_body.extend_from_slice(&2u32.to_be_bytes()); // out broadcast
    counter_body.extend_from_slice(&3u32.to_be_bytes()); // out discards
    counter_body.extend_from_slice(&4u32.to_be_bytes()); // out errors
    counter_body.extend_from_slice(&0u32.to_be_bytes()); // promiscuous

    let mut counter_record = Vec::new();
    counter_record.extend_from_slice(&1u32.to_be_bytes()); // format = generic
    counter_record.extend_from_slice(&(counter_body.len() as u32).to_be_bytes());
    counter_record.extend_from_slice(&counter_body);

    let mut counter_sample_body = Vec::new();
    counter_sample_body.extend_from_slice(&1u32.to_be_bytes()); // sequence_number
    counter_sample_body.extend_from_slice(&0u32.to_be_bytes()); // source_id
    counter_sample_body.extend_from_slice(&1u32.to_be_bytes()); // num_records
    counter_sample_body.extend_from_slice(&counter_record);

    let mut sample = Vec::new();
    sample.extend_from_slice(&2u32.to_be_bytes()); // format = counter_sample
    sample.extend_from_slice(&(counter_sample_body.len() as u32).to_be_bytes());
    sample.extend_from_slice(&counter_sample_body);

    let mut pkt = Vec::new();
    pkt.extend_from_slice(&5u32.to_be_bytes());
    pkt.extend_from_slice(&1u32.to_be_bytes()); // IPv4
    pkt.extend_from_slice(&[10, 0, 0, 1]);
    pkt.extend_from_slice(&7u32.to_be_bytes()); // sub_agent_id
    pkt.extend_from_slice(&100u32.to_be_bytes()); // sequence
    pkt.extend_from_slice(&1_000_000u32.to_be_bytes()); // uptime
    pkt.extend_from_slice(&1u32.to_be_bytes()); // num_samples
    pkt.extend_from_slice(&sample);
    pkt
}

#[test]
fn decodes_counter_sample_to_canonical() {
    let pkt = build_counter_sample_packet();
    let mut dec = SflowDecoder::new();
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let decoded = dec
        .decode(&pkt, exporter, ProtocolKind::SflowV5)
        .expect("counter sample decode");
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(SflowDecoder::record_kind(&decoded.records[0]), "counter");
}

#[test]
fn counter_sample_render_canonical_json_shape() {
    let pkt = build_counter_sample_packet();
    let mut dec = SflowDecoder::new();
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let decoded = dec
        .decode(&pkt, exporter, ProtocolKind::SflowV5)
        .expect("decode");
    let mut buf = Vec::new();
    SflowDecoder::render_canonical(&decoded.records[0], &mut buf).expect("render");
    let parsed: Value = serde_json::from_slice(&buf).expect("json parses");

    assert_eq!(parsed["record_kind"], "counter");
    assert_eq!(parsed["if_index"], 5);
    assert_eq!(parsed["if_speed"], 1_000_000_000u64);
    assert_eq!(parsed["if_in_octets"], 999_888);
    // 100 ucast + 10 multicast + 5 broadcast
    assert_eq!(parsed["if_in_packets"], 115);
    assert_eq!(parsed["if_in_errors"], 1);
    assert_eq!(parsed["if_in_discards"], 2);
    assert_eq!(parsed["if_out_octets"], 777_666);
    // 90 + 5 + 2
    assert_eq!(parsed["if_out_packets"], 97);
    assert_eq!(parsed["if_out_errors"], 4);
    assert_eq!(parsed["if_out_discards"], 3);
}

#[test]
fn counter_sample_render_raw_is_json() {
    let pkt = build_counter_sample_packet();
    let mut dec = SflowDecoder::new();
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let decoded = dec
        .decode(&pkt, exporter, ProtocolKind::SflowV5)
        .expect("decode");
    let mut buf = Vec::new();
    SflowDecoder::render_raw(&decoded.records[0], &mut buf).expect("render");
    let parsed: Value = serde_json::from_slice(&buf).expect("raw json parses");
    assert_eq!(parsed["kind"], "counter_sample");
    assert_eq!(parsed["sequence_number"], 1);
    assert_eq!(parsed["num_records"], 1);
}
