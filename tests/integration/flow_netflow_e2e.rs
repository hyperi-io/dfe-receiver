// Project:   dfe-receiver
// File:      tests/integration/flow_netflow_e2e.rs
// Purpose:   End-to-end NetFlow v5 -> FlowHandler -> Kafka netflow_land
// Language:  Rust
//
// License:   FSL-1.1-ALv2
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
// No-op FlowMetrics for integration tests.
//
// The crate's own `metrics::mock::flow_metrics_for_test` is `#[cfg(test)]`-gated
// inside the crate, so it isn't visible to integration tests. We build a tiny
// inline no-op implementation -- the test asserts on Kafka envelope content,
// not on metric counters.
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

/// Pick a random high UDP port to avoid privilege requirements and collisions
/// with other tests running in parallel on the same host.
fn random_udp_port() -> u16 {
    20000 + (uuid::Uuid::new_v4().as_u128() % 30000) as u16
}

/// Wait until UDP port is bound by sending a probe and confirming no
/// ConnectionRefused (UDP can't "connect" but a quick bind probe loop avoids
/// races on busy runners). We just poll with a short sleep loop since UDP
/// readiness is hard to observe externally.
async fn wait_after_bind(_port: u16) {
    // UDP bind is synchronous in the listener.run() but the spawn loop hasn't
    // necessarily hit the recv_from yet; give it a moment.
    tokio::time::sleep(Duration::from_millis(300)).await;
}

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
fn flow_kafka_config(
    kf: &crate::common::KafkaTestConfig,
    flow_port: u16,
    topic_suffix: &str,
) -> Config {
    let mut config = Config::default();
    // Use a high random port for the HTTP server (still needed by orchestrator).
    config.server.bind_address = format!("127.0.0.1:{}", random_udp_port());
    config.server.auth.mode = "none".to_string();

    // Kafka destination
    config.kafka = kf.to_receiver_kafka_config();
    config.destinations.default = "kafka".to_string();

    // Route by the envelope's `_source` field, suffix `_land`.
    config.routing.default_source = "default".to_string();
    config.routing.topic_suffix = topic_suffix.to_string();
    config.routing.source_rules = vec![SourceRule {
        field: "_source".to_string(),
        mode: "key_value_use".to_string(),
        match_value: None,
        source: None,
    }];

    // Flow handler config -- unified mode, single port, NetFlow only.
    config.flow.enabled = true;
    config.flow.experimental = false; // suppress WARN log noise
    config.flow.bind_address = IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1));
    config.flow.ports = vec![flow_port];
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
    let Some((_kafka_handle, kf)) = kafka_backend().await else {
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

    // ------------------------------------------------------------------
    // 4. Send a NetFlow v5 packet via UDP.
    // ------------------------------------------------------------------
    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind client UDP");
    let target = SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), flow_port);

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
    // Canonical mode embeds records under "records" array with src_ip / dst_ip.
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
