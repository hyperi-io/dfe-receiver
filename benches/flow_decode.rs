// Project:   dfe-receiver
// File:      benches/flow_decode.rs
// Purpose:   Flow decode + envelope throughput benches (NetFlow v5, sFlow v5)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Flow decode + envelope throughput benches.
//!
//! Two bench groups:
//!  - decode: raw datagram -> DecodedPacket throughput
//!  - envelope: DecodedPacket -> JSON envelope throughput across all OutputModes
//!
//! Fixtures mirror the unit-test packet builders so the bench surfaces the
//! same code path used in tests.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dfe_receiver::server::flow::config::OutputMode;
use dfe_receiver::server::flow::decoder::FlowDecoder;
use dfe_receiver::server::flow::dispatch::ProtocolKind;
use dfe_receiver::server::flow::envelope::render_packet;
use dfe_receiver::server::netflow::decoder::NetflowDecoder;
use dfe_receiver::server::sflow::decoder::SflowDecoder;
use std::hint::black_box;
use std::net::IpAddr;

/// Build a valid NetFlow v5 datagram with one flow record (matches
/// `src/server/netflow/tests/decode_v5.rs::build_v5_packet_with_one_flow`).
fn build_v5_packet() -> Vec<u8> {
    let mut pkt = vec![0u8; 72];
    // Header
    pkt[0..2].copy_from_slice(&5u16.to_be_bytes());
    pkt[2..4].copy_from_slice(&1u16.to_be_bytes());
    pkt[4..8].copy_from_slice(&1000u32.to_be_bytes());
    pkt[8..12].copy_from_slice(&1_700_000_000u32.to_be_bytes());
    pkt[12..16].copy_from_slice(&0u32.to_be_bytes());
    pkt[16..20].copy_from_slice(&42u32.to_be_bytes());
    // Record
    pkt[24..28].copy_from_slice(&[10, 0, 0, 1]);
    pkt[28..32].copy_from_slice(&[10, 0, 0, 2]);
    pkt[36..38].copy_from_slice(&7u16.to_be_bytes());
    pkt[38..40].copy_from_slice(&9u16.to_be_bytes());
    pkt[40..44].copy_from_slice(&5u32.to_be_bytes());
    pkt[44..48].copy_from_slice(&1500u32.to_be_bytes());
    pkt[48..52].copy_from_slice(&500u32.to_be_bytes());
    pkt[52..56].copy_from_slice(&800u32.to_be_bytes());
    pkt[56..58].copy_from_slice(&12345u16.to_be_bytes());
    pkt[58..60].copy_from_slice(&80u16.to_be_bytes());
    pkt[61] = 0x18;
    pkt[62] = 6;
    pkt[64..66].copy_from_slice(&64500u16.to_be_bytes());
    pkt[66..68].copy_from_slice(&64501u16.to_be_bytes());
    pkt[68] = 24;
    pkt[69] = 24;
    pkt
}

/// Build a minimal sFlow v5 datagram (header + zero samples). Useful for
/// measuring envelope-rendering cost without the parser-heavy flow sample path.
fn build_sflow_minimal_packet() -> Vec<u8> {
    let mut pkt = Vec::with_capacity(28);
    pkt.extend_from_slice(&5u32.to_be_bytes()); // version
    pkt.extend_from_slice(&1u32.to_be_bytes()); // agent_address_type = IPv4
    pkt.extend_from_slice(&[10, 0, 0, 1]); // agent ip
    pkt.extend_from_slice(&0u32.to_be_bytes()); // sub_agent_id
    pkt.extend_from_slice(&100u32.to_be_bytes()); // sequence
    pkt.extend_from_slice(&3600u32.to_be_bytes()); // uptime
    pkt.extend_from_slice(&0u32.to_be_bytes()); // num_samples = 0
    pkt
}

fn bench_decode(c: &mut Criterion) {
    let v5_pkt = build_v5_packet();
    let sf_pkt = build_sflow_minimal_packet();
    let exporter: IpAddr = "127.0.0.1".parse().expect("ipv4 literal");

    let mut g = c.benchmark_group("decode");
    g.bench_function("netflow_v5", |b| {
        let mut d = NetflowDecoder::new(1000, 10_000, dfe_receiver::server::flow::metrics::mock::flow_metrics_for_test());
        b.iter(|| {
            black_box(
                d.decode(black_box(&v5_pkt), exporter, ProtocolKind::NetflowV5)
                    .ok(),
            );
        });
    });
    g.bench_function("sflow_v5_empty", |b| {
        let mut d = SflowDecoder::new();
        b.iter(|| {
            black_box(
                d.decode(black_box(&sf_pkt), exporter, ProtocolKind::SflowV5)
                    .ok(),
            );
        });
    });
    g.finish();
}

fn bench_envelope_canonical(c: &mut Criterion) {
    let v5_pkt = build_v5_packet();
    let mut d = NetflowDecoder::new(1000, 10_000, dfe_receiver::server::flow::metrics::mock::flow_metrics_for_test());
    let exporter: IpAddr = "127.0.0.1".parse().expect("ipv4 literal");
    let decoded = d
        .decode(&v5_pkt, exporter, ProtocolKind::NetflowV5)
        .expect("v5 decodes");
    let mut buf = Vec::with_capacity(8192);
    let now = "2026-05-20T00:00:00Z";

    let mut g = c.benchmark_group("envelope");
    for mode in [
        OutputMode::Canonical,
        OutputMode::CanonicalWithRaw,
        OutputMode::Exploded,
    ] {
        g.bench_with_input(
            BenchmarkId::new("netflow_v5", format!("{mode:?}")),
            &mode,
            |b, &m| {
                b.iter(|| {
                    let _ = render_packet::<NetflowDecoder>(&decoded, m, now, &mut buf);
                });
            },
        );
    }
    g.finish();
}

criterion_group!(benches, bench_decode, bench_envelope_canonical);
criterion_main!(benches);
