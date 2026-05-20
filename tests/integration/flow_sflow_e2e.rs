// Project:   dfe-receiver
// File:      tests/integration/flow_sflow_e2e.rs
// Purpose:   End-to-end sFlow v5 -> FlowHandler -> Kafka sflow_land
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! End-to-end test for the flow handler with sFlow v5 flow-sample packets.
//!
//! ## Container vs in-test UDP client
//!
//! This test uses an **in-test UDP client** that sends hand-crafted sFlow v5
//! datagrams directly to the receiver's bind port, rather than spinning up an
//! hsflowd container.
//!
//! Reasons:
//! - hsflowd is designed to read interface counters and emit sFlow against a
//!   configured collector; it does not produce deterministic sampled packets
//!   on demand inside a CI container without significant network plumbing.
//! - Hand-crafted v5 flow-sample datagrams are deterministic, fast, and
//!   exercise the full receive path (UDP socket -> autosense -> SflowDecoder
//!   -> envelope -> pipeline -> Kafka). The wire format is well-specified.
//! - The receive path is what we want to validate.
//!
//! Kafka is provided by a testcontainers-managed broker (or live infra when
//! TEST_MODE=live and KAFKA_BROKERS is reachable). The test skips gracefully
//! when no Kafka backend is available.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig, SourceRule};
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::flow::handler::FlowHandler;
use dfe_receiver::server::flow::metrics::{
    FlowCounter, FlowHistogram, FlowLabelledCounter, FlowLabelledGauge, FlowMetrics,
};
use dfe_receiver::server::traits::ProtocolHandler;
use tokio_util::sync::CancellationToken;

use crate::common::{kafka_backend, kafka_consume_next, kafka_consumer};

// ---------------------------------------------------------------------------
// No-op FlowMetrics for integration tests (see flow_netflow_e2e.rs for the
// rationale).
// ---------------------------------------------------------------------------

struct NoopCounter;
impl FlowCounter for NoopCounter {
    fn inc(&self) {}
    fn add(&self, _n: u64) {}
}

struct NoopLabelledCounter;
impl FlowLabelledCounter for NoopLabelledCounter {
    fn inc(&self, _labels: &[(&'static str, &str)]) {}
}

struct NoopHistogram;
impl FlowHistogram for NoopHistogram {
    fn observe(&self, _value: f64) {}
}

struct NoopLabelledGauge;
impl FlowLabelledGauge for NoopLabelledGauge {
    fn set(&self, _labels: &[(&'static str, &str)], _value: f64) {}
}

fn noop_flow_metrics() -> FlowMetrics {
    FlowMetrics {
        recv_total: Arc::new(NoopCounter),
        recv_bytes_total: Arc::new(NoopCounter),
        decode_err_total: Arc::new(NoopLabelledCounter),
        drops_total: Arc::new(NoopLabelledCounter),
        invalid_packet_total: Arc::new(NoopLabelledCounter),
        rate_limited_total: Arc::new(NoopLabelledCounter),
        records_emitted_total: Arc::new(NoopLabelledCounter),
        records_per_packet: Arc::new(NoopHistogram),
        template_cache_size: Arc::new(NoopLabelledGauge),
        template_evicted_total: Arc::new(NoopCounter),
        kernel_drops_total: Arc::new(NoopCounter),
        send_duration_seconds: Arc::new(NoopHistogram),
        unknown_version_total: Arc::new(NoopCounter),
        handler_experimental: Arc::new(NoopLabelledGauge),
    }
}

fn random_udp_port() -> u16 {
    20000 + (uuid::Uuid::new_v4().as_u128() % 30000) as u16
}

async fn wait_after_bind(_port: u16) {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

/// Minimal Ethernet + IPv4 + TCP header (54 bytes) for the sampled_header
/// payload. Mirrors `src/server/sflow/tests/decode_flow.rs::build_eth_ipv4_tcp_header`.
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
    hdr.push(0x18); // PSH+ACK
    hdr.extend_from_slice(&[0; 6]); // window, checksum, urgent
    hdr
}

/// Build a complete sFlow v5 datagram containing one flow_sample with one
/// sampled_header (eth+ipv4+tcp). Mirrors the in-tree helper in
/// `src/server/sflow/tests/decode_flow.rs`.
fn build_sflow_v5_packet() -> Vec<u8> {
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
    flow_sample_body.extend_from_slice(&7u32.to_be_bytes()); // input iface
    flow_sample_body.extend_from_slice(&9u32.to_be_bytes()); // output iface
    flow_sample_body.extend_from_slice(&1u32.to_be_bytes()); // num records
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

/// Construct a Config wired for Kafka delivery with a `key_value_use`
/// source rule on `_source` so the flow envelope (`_source: "sflow"`)
/// routes to topic `sflow_land`.
fn flow_kafka_config(
    kf: &crate::common::KafkaTestConfig,
    flow_port: u16,
    topic_suffix: &str,
) -> Config {
    let mut config = Config::default();
    config.server.bind_address = format!("127.0.0.1:{}", random_udp_port());
    config.server.auth.mode = "none".to_string();

    config.kafka = kf.to_receiver_kafka_config();
    config.destinations.default = "kafka".to_string();

    config.routing.default_source = "default".to_string();
    config.routing.topic_suffix = topic_suffix.to_string();
    config.routing.source_rules = vec![SourceRule {
        field: "_source".to_string(),
        mode: "key_value_use".to_string(),
        match_value: None,
        source: None,
    }];

    // Flow handler -- sFlow-only on a single port.
    config.flow.enabled = true;
    config.flow.experimental = false;
    config.flow.bind_address = IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1));
    config.flow.ports = vec![flow_port];
    config.flow.recv_buffer_bytes = 256 * 1024;
    config.flow.netflow.enabled = false;
    config.flow.sflow.enabled = true;
    config.flow.sflow.topic = format!("sflow{topic_suffix}");

    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sflow_v5_end_to_end_to_kafka() {
    let Some((_kafka_handle, kf)) = kafka_backend().await else {
        eprintln!("Skipping: no Kafka backend available (no live infra and Docker unavailable)");
        return;
    };

    let topic_suffix = "_land";
    let topic = format!("sflow{topic_suffix}");

    let consumer = kafka_consumer(&kf, &topic).expect("kafka consumer setup");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let flow_port = random_udp_port();
    let config = flow_kafka_config(&kf, flow_port, topic_suffix);

    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("pipeline init"),
    );
    let handler = FlowHandler::new(config.flow.clone(), noop_flow_metrics(), pipeline)
        .expect("flow handler new");

    let handler_shutdown = shutdown.clone();
    let handler_task = tokio::spawn(async move {
        let _ = handler.start(handler_shutdown).await;
    });

    wait_after_bind(flow_port).await;

    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind client UDP");
    let target = SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), flow_port);

    let pkt = build_sflow_v5_packet();
    for _ in 0..3 {
        sock.send_to(&pkt, target).await.expect("udp send");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let received = kafka_consume_next(&consumer, Duration::from_secs(30))
        .await
        .expect("no sflow envelope arrived on Kafka topic");

    let text = String::from_utf8_lossy(&received);
    assert!(
        text.contains("\"_source\":\"sflow\""),
        "envelope missing _source=sflow: {text}"
    );
    assert!(
        text.contains("\"version\":\"sflow_v5\""),
        "envelope missing version=sflow_v5: {text}"
    );
    assert!(
        text.contains("\"src_ip\":\"10.0.0.1\""),
        "expected src_ip 10.0.0.1 in envelope: {text}"
    );
    assert!(
        text.contains("\"dst_ip\":\"10.0.0.2\""),
        "expected dst_ip 10.0.0.2 in envelope: {text}"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handler_task).await;
}
