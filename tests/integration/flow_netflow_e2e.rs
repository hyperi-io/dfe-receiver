// Project:   dfe-receiver
// File:      tests/integration/flow_netflow_e2e.rs
// Purpose:   End-to-end NetFlow v5 -> FlowHandler -> Kafka netflow_land
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! End-to-end test for the flow handler with NetFlow v5 packets.
//!
//! ## Container vs in-test UDP client
//!
//! This test uses an **in-test UDP client** that sends hand-crafted NetFlow v5
//! datagrams directly to the receiver's bind port, rather than spinning up a
//! softflowd container.
//!
//! Reasons:
//! - softflowd needs a routable interface to capture traffic from; it doesn't
//!   simply "emit packets at a target". Configuring that inside a container
//!   network is fragile across CI runners.
//! - Hand-crafted v5 datagrams are deterministic, fast, and exercise the same
//!   receive path (UDP socket -> autosense -> NetflowDecoder -> envelope ->
//!   pipeline -> Kafka). The wire format is fixed and well-specified.
//! - The receive path is what we want to validate; the upstream packet source
//!   is irrelevant to the receiver's correctness.
//!
//! Kafka is provided by a testcontainers-managed broker (or live infra when
//! TEST_MODE=live and KAFKA_BROKERS is reachable). The test skips gracefully
//! when no Kafka backend is available.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig, SourceRule};
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::flow::handler::FlowHandler;
use dfe_receiver::server::flow::metrics::mock::flow_metrics_for_test;
use dfe_receiver::server::traits::ProtocolHandler;
use tokio_util::sync::CancellationToken;

use crate::common::{kafka_backend, kafka_consume_next, kafka_consumer};
use crate::test_name;

/// Build a valid NetFlow v5 datagram with one flow record (72 bytes total).
/// Mirrors `src/server/netflow/tests/decode_v5.rs::build_v5_packet_with_one_flow`.
fn build_netflow_v5_packet() -> Vec<u8> {
    let mut pkt = vec![0u8; 72];
    // Header (24 bytes)
    pkt[0..2].copy_from_slice(&5u16.to_be_bytes()); // version
    pkt[2..4].copy_from_slice(&1u16.to_be_bytes()); // count = 1
    pkt[4..8].copy_from_slice(&1000u32.to_be_bytes()); // sys_uptime
    pkt[8..12].copy_from_slice(&1_700_000_000u32.to_be_bytes()); // unix_secs
    pkt[12..16].copy_from_slice(&0u32.to_be_bytes()); // unix_nsecs
    pkt[16..20].copy_from_slice(&42u32.to_be_bytes()); // flow_sequence
    pkt[20] = 0; // engine_type
    pkt[21] = 0; // engine_id
    pkt[22..24].copy_from_slice(&0u16.to_be_bytes()); // sampling_interval
    // Record (offset 24, 48 bytes)
    pkt[24..28].copy_from_slice(&[10, 0, 0, 1]); // srcaddr
    pkt[28..32].copy_from_slice(&[10, 0, 0, 2]); // dstaddr
    pkt[32..36].copy_from_slice(&[0, 0, 0, 0]); // nexthop
    pkt[36..38].copy_from_slice(&7u16.to_be_bytes()); // input iface
    pkt[38..40].copy_from_slice(&9u16.to_be_bytes()); // output iface
    pkt[40..44].copy_from_slice(&5u32.to_be_bytes()); // dPkts
    pkt[44..48].copy_from_slice(&1500u32.to_be_bytes()); // dOctets
    pkt[48..52].copy_from_slice(&500u32.to_be_bytes()); // first
    pkt[52..56].copy_from_slice(&800u32.to_be_bytes()); // last
    pkt[56..58].copy_from_slice(&12345u16.to_be_bytes()); // srcport
    pkt[58..60].copy_from_slice(&80u16.to_be_bytes()); // dstport
    pkt[60] = 0; // pad1
    pkt[61] = 0x18; // tcp_flags
    pkt[62] = 6; // protocol = TCP
    pkt[63] = 0; // tos
    pkt[64..66].copy_from_slice(&64500u16.to_be_bytes()); // src_as
    pkt[66..68].copy_from_slice(&64501u16.to_be_bytes()); // dst_as
    pkt[68] = 24; // src_mask
    pkt[69] = 24; // dst_mask
    pkt[70..72].copy_from_slice(&0u16.to_be_bytes()); // pad2
    pkt
}

/// Construct a Config wired for Kafka delivery with a `key_value_use`
/// source rule on `_source` so the flow envelope (`_source: "netflow"`)
/// routes to topic `netflow_land`.
fn flow_kafka_config(kf: &crate::common::KafkaTestConfig, topic_suffix: &str) -> Config {
    let mut config = Config::default();
    // The HTTP server still needs a bind address, though this test never starts it.
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();

    // Kafka destination
    config.kafka = kf.to_receiver_kafka_config();
    config.destinations.default = "kafka".into();

    // Route by the envelope's `_source` field, suffix `_land`.
    config.routing.default_source = "main".to_string();
    config.routing.topic_suffix = topic_suffix.to_string();
    config.routing.source_rules = vec![SourceRule {
        field: "_source".to_string(),
        mode: "key_value_use".to_string(),
        match_value: None,
        source: None,
    }];

    // Flow handler config -- unified mode, single port 0, NetFlow only.
    config.flow.enabled = true;
    config.flow.experimental = false; // suppress WARN log noise
    config.flow.bind_address = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    config.flow.ports = vec![0];
    // Smaller SO_RCVBUF -- system default may cap below 8MiB on some runners.
    config.flow.recv_buffer_bytes = 256 * 1024;
    // Disable sFlow on this listener (NetFlow-only).
    config.flow.sflow.enabled = false;
    config.flow.netflow.enabled = true;
    config.flow.netflow.topic = format!("netflow{topic_suffix}");

    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn netflow_v5_end_to_end_to_kafka() {
    // ------------------------------------------------------------------
    // 1. Kafka backend (live, docker, or testcontainers fallback).
    // ------------------------------------------------------------------
    let Some((_kafka_handle, kf)) = kafka_backend(test_name!()).await else {
        eprintln!("Skipping: no Kafka backend available (no live infra and Docker unavailable)");
        return;
    };

    let topic_suffix = "_land";
    let topic = format!("netflow{topic_suffix}");

    // ------------------------------------------------------------------
    // 2. Subscribe to Kafka BEFORE producing.
    // ------------------------------------------------------------------
    let consumer = kafka_consumer(&kf, &topic).expect("kafka consumer setup");
    tokio::time::sleep(Duration::from_secs(1)).await; // consumer group join

    // ------------------------------------------------------------------
    // 3. Start dfe-receiver flow handler.
    // ------------------------------------------------------------------
    let config = flow_kafka_config(&kf, topic_suffix);

    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("pipeline init"),
    );
    let handler = FlowHandler::new(
        config.flow.clone(),
        config.raw_capture_for(&config.flow.raw_capture),
        flow_metrics_for_test(),
        pipeline,
    )
    .expect("flow handler new");

    let handler_shutdown = shutdown.clone();
    let bound = handler
        .bound_addrs()
        .into_iter()
        .next()
        .expect("one listener for the one configured port");
    let mut handler_task = tokio::spawn(async move { handler.start(handler_shutdown).await });

    let flow = crate::common::bound_addr("flow", &bound, &mut handler_task).await;

    // ------------------------------------------------------------------
    // 4. Send a NetFlow v5 packet via UDP.
    // ------------------------------------------------------------------
    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind client UDP");
    let target = flow;

    // Send a few packets -- one is plenty but a small burst makes the test
    // more robust against rare UDP loss on loopback.
    let pkt = build_netflow_v5_packet();
    for _ in 0..3 {
        sock.send_to(&pkt, target).await.expect("udp send");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ------------------------------------------------------------------
    // 5. Verify a message arrived on the Kafka topic.
    // ------------------------------------------------------------------
    let received = kafka_consume_next(&consumer, Duration::from_secs(30))
        .await
        .expect("no netflow envelope arrived on Kafka topic");

    let text = String::from_utf8_lossy(&received);
    assert!(
        text.contains("\"_source\":\"netflow\""),
        "envelope missing _source=netflow: {text}"
    );
    assert!(
        text.contains("\"version\":\"netflow_v5\""),
        "envelope missing version=netflow_v5: {text}"
    );
    // Canonical mode embeds records under a "flows" array. We assert on
    // record fields populated by the v5 decoder (src_ip / dst_ip).
    assert!(
        text.contains("\"flows\":["),
        "envelope missing flows array: {text}"
    );
    assert!(
        text.contains("\"src_ip\":\"10.0.0.1\""),
        "expected src_ip 10.0.0.1 in envelope: {text}"
    );
    assert!(
        text.contains("\"dst_ip\":\"10.0.0.2\""),
        "expected dst_ip 10.0.0.2 in envelope: {text}"
    );

    // ------------------------------------------------------------------
    // 6. Shutdown.
    // ------------------------------------------------------------------
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handler_task).await;
}

// ---------------------------------------------------------------------------
// NetFlow v9 + NSEL hand-crafted packet builders. Mirror the unit-test
// builders in `src/server/netflow/tests/` so the e2e test exercises the same
// codec path. v9 requires two datagrams (template first, data second) with
// the SAME exporter IP so the decoder's per-exporter codec sees both.
// ---------------------------------------------------------------------------

use bytes::BytesMut;
use chrono::{TimeZone, Utc};
use netgauze_flow_pkt::codec::FlowInfoCodec;
use netgauze_flow_pkt::netflow::{DataRecord, NetFlowV9Packet, Set as NfSet, TemplateRecord};
use netgauze_flow_pkt::{DataSetId, FieldSpecifier, FlowInfo, ie};
use std::net::Ipv4Addr;
use tokio_util::codec::{Decoder, Encoder};

const V9_TEMPLATE_ID: u16 = 308;
const NSEL_TEMPLATE_ID: u16 = 501;

/// Build (template, data) v9 datagrams for a simple 5-field flow. Mirrors
/// `src/server/netflow/tests/decode_v9.rs::build_v9_template_and_data`.
fn build_v9_template_and_data() -> (Vec<u8>, Vec<u8>) {
    let template = NetFlowV9Packet::new(
        45_646,
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        3812,
        0,
        Box::new([NfSet::Template(Box::new([TemplateRecord::new(
            V9_TEMPLATE_ID,
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
            id: DataSetId::new(V9_TEMPLATE_ID).unwrap(),
            records: Box::new([DataRecord::new(
                Box::new([]),
                Box::new([
                    ie::Field::sourceIPv4Address(Ipv4Addr::new(172, 16, 0, 1)),
                    ie::Field::destinationIPv4Address(Ipv4Addr::new(172, 16, 0, 2)),
                    ie::Field::protocolIdentifier(ie::protocolIdentifier::TCP),
                    ie::Field::octetDeltaCount(4096),
                    ie::Field::packetDeltaCount(8),
                ]),
            )]),
        }]),
    );
    encode_pair(template, data)
}

/// Build (template, data) v9 datagrams that exercise the NSEL discriminator:
/// the template includes `firewallEvent` (IE 233); the data carries
/// event_type=3 (Flow Denied).
fn build_nsel_template_and_data() -> (Vec<u8>, Vec<u8>) {
    let template = NetFlowV9Packet::new(
        12_345,
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        100,
        7,
        Box::new([NfSet::Template(Box::new([TemplateRecord::new(
            NSEL_TEMPLATE_ID,
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
            id: DataSetId::new(NSEL_TEMPLATE_ID).unwrap(),
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
    encode_pair(template, data)
}

/// Helper: encode (template, data) using a shared `FlowInfoCodec` so the
/// encoder's internal templates_map sees the template definition before the
/// data packet is encoded against it.
fn encode_pair(template: NetFlowV9Packet, data: NetFlowV9Packet) -> (Vec<u8>, Vec<u8>) {
    let mut codec = FlowInfoCodec::new();
    let mut tpl_buf = BytesMut::new();
    codec
        .encode(FlowInfo::NetFlowV9(template), &mut tpl_buf)
        .expect("encode template");
    // Decode the template through the same codec so the encoder side knows the
    // per-field lengths when it later encodes the data packet.
    let mut decode_buf = BytesMut::from(&tpl_buf[..]);
    let _ = codec
        .decode(&mut decode_buf)
        .expect("template decode populates codec map");
    let mut data_buf = BytesMut::new();
    codec
        .encode(FlowInfo::NetFlowV9(data), &mut data_buf)
        .expect("encode data");
    (tpl_buf.to_vec(), data_buf.to_vec())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn netflow_v9_end_to_end_to_kafka() {
    let Some((_kafka_handle, kf)) = kafka_backend(test_name!()).await else {
        eprintln!("Skipping: no Kafka backend available");
        return;
    };
    let topic_suffix = "_land";
    let topic = format!("netflow{topic_suffix}");
    let consumer = kafka_consumer(&kf, &topic).expect("kafka consumer setup");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let config = flow_kafka_config(&kf, topic_suffix);

    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("pipeline init"),
    );
    let handler = FlowHandler::new(
        config.flow.clone(),
        config.raw_capture_for(&config.flow.raw_capture),
        flow_metrics_for_test(),
        pipeline,
    )
    .expect("flow handler new");
    let handler_shutdown = shutdown.clone();
    let bound = handler
        .bound_addrs()
        .into_iter()
        .next()
        .expect("one listener for the one configured port");
    let mut handler_task = tokio::spawn(async move { handler.start(handler_shutdown).await });
    let flow = crate::common::bound_addr("flow", &bound, &mut handler_task).await;

    // Send template first, then data record. Both from the same client socket
    // so the source IP (127.0.0.1) is identical -- the decoder's per-exporter
    // template cache keys on source IP.
    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind client UDP");
    let target = flow;
    let (tpl, data) = build_v9_template_and_data();
    sock.send_to(&tpl, target).await.expect("udp send template");
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Burst the data packet in case of rare loopback loss.
    for _ in 0..3 {
        sock.send_to(&data, target).await.expect("udp send data");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let received = kafka_consume_next(&consumer, Duration::from_secs(30))
        .await
        .expect("no netflow v9 envelope arrived");
    let text = String::from_utf8_lossy(&received);
    assert!(
        text.contains("\"_source\":\"netflow\""),
        "envelope missing _source=netflow: {text}"
    );
    assert!(
        text.contains("\"version\":\"netflow_v9\""),
        "envelope missing version=netflow_v9: {text}"
    );
    assert!(
        text.contains("\"flows\":["),
        "envelope missing flows array: {text}"
    );
    assert!(
        text.contains("\"src_ip\":\"172.16.0.1\""),
        "expected src_ip 172.16.0.1: {text}"
    );
    assert!(
        text.contains("\"dst_ip\":\"172.16.0.2\""),
        "expected dst_ip 172.16.0.2: {text}"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handler_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn netflow_nsel_end_to_end_to_kafka() {
    let Some((_kafka_handle, kf)) = kafka_backend(test_name!()).await else {
        eprintln!("Skipping: no Kafka backend available");
        return;
    };
    let topic_suffix = "_land";
    let topic = format!("netflow{topic_suffix}");
    let consumer = kafka_consumer(&kf, &topic).expect("kafka consumer setup");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let config = flow_kafka_config(&kf, topic_suffix);

    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("pipeline init"),
    );
    let handler = FlowHandler::new(
        config.flow.clone(),
        config.raw_capture_for(&config.flow.raw_capture),
        flow_metrics_for_test(),
        pipeline,
    )
    .expect("flow handler new");
    let handler_shutdown = shutdown.clone();
    let bound = handler
        .bound_addrs()
        .into_iter()
        .next()
        .expect("one listener for the one configured port");
    let mut handler_task = tokio::spawn(async move { handler.start(handler_shutdown).await });
    let flow = crate::common::bound_addr("flow", &bound, &mut handler_task).await;

    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind client UDP");
    let target = flow;
    let (tpl, data) = build_nsel_template_and_data();
    sock.send_to(&tpl, target).await.expect("udp send template");
    tokio::time::sleep(Duration::from_millis(50)).await;
    for _ in 0..3 {
        sock.send_to(&data, target).await.expect("udp send data");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let received = kafka_consume_next(&consumer, Duration::from_secs(30))
        .await
        .expect("no NSEL envelope arrived");
    let text = String::from_utf8_lossy(&received);
    assert!(
        text.contains("\"_source\":\"netflow\""),
        "envelope missing _source=netflow: {text}"
    );
    assert!(
        text.contains("\"record_kind\":\"security_event\""),
        "expected record_kind=security_event: {text}"
    );
    // firewallEvent::FlowDenied = 3.
    assert!(
        text.contains("\"event_type\":3"),
        "expected event_type=3 (FlowDenied): {text}"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handler_task).await;
}
