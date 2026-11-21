// Project:   dfe-receiver
// File:      tests/integration/flow_exploded.rs
// Purpose:   Exploded flow events per committed fixture, and the keys dfe-schemas reads
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The `exploded` output of every committed flow fixture, replayed in order through one
//! decoder per protocol, as the receiver takes them from one exporter.
//!
//! dfe-schemas' `meta/flow/netflow` and `meta/flow/sflow` read the keys of this
//! output, and dfe-infra's `flow` acceptance case replays these files and counts the
//! rows each source's table must hold. Both numbers come from here.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::fs::File;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};

use dfe_receiver::config::RawCapture;
use dfe_receiver::server::flow::config::OutputMode;
use dfe_receiver::server::flow::decoder::FlowDecoder;
use dfe_receiver::server::flow::dispatch::{ProtocolKind, dispatch_protocol_kind};
use dfe_receiver::server::flow::envelope::render_packet;
use dfe_receiver::server::flow::metrics::mock::flow_metrics_for_test;
use dfe_receiver::server::netflow::decoder::NetflowDecoder;
use dfe_receiver::server::sflow::decoder::SflowDecoder;
use pcap_file::pcap::PcapReader;
use serde_json::Value;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/flow/external")
        .join(name)
}

/// The UDP payloads of one fixture: the file itself for a `.bin`, every packet of a `.pcap`.
fn payloads(path: &Path) -> Vec<Vec<u8>> {
    if path.extension().and_then(|ext| ext.to_str()) == Some("bin") {
        return vec![std::fs::read(path).unwrap()];
    }
    let mut reader = PcapReader::new(File::open(path).unwrap()).unwrap();
    let mut found = Vec::new();
    while let Some(Ok(packet)) = reader.next_raw_packet() {
        // Ethernet (14) + IPv4 (20) + UDP (8), which is what these captures carry.
        if let Some(payload) = packet.data.get(42..) {
            found.push(payload.to_vec());
        }
    }
    found
}

/// Every exploded event the receiver renders for one fixture, in datagram order.
///
/// The decoders are the caller's, because a template an exporter sent earlier stays in force
/// for the datagrams after it.
fn exploded(name: &str, netflow: &mut NetflowDecoder, sflow: &mut SflowDecoder) -> Vec<Value> {
    let exporter = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
    let mut buf = Vec::new();
    let mut events = Vec::new();
    for payload in payloads(&fixture(name)) {
        let kind = dispatch_protocol_kind(&payload);
        let ranges = match kind {
            ProtocolKind::NetflowV5 | ProtocolKind::NetflowV9 | ProtocolKind::Ipfix => {
                let Ok(decoded) = netflow.decode(&payload, exporter, kind) else {
                    continue;
                };
                render_packet::<NetflowDecoder>(
                    &decoded,
                    OutputMode::Exploded,
                    RawCapture::OFF,
                    "2026-11-21T00:00:00Z",
                    &mut buf,
                )
            }
            ProtocolKind::SflowV5 => {
                let Ok(decoded) = sflow.decode(&payload, exporter, kind) else {
                    continue;
                };
                render_packet::<SflowDecoder>(
                    &decoded,
                    OutputMode::Exploded,
                    RawCapture::OFF,
                    "2026-11-21T00:00:00Z",
                    &mut buf,
                )
            }
            _ => continue,
        };
        for range in ranges.unwrap() {
            events.push(serde_json::from_slice(&buf[range]).unwrap());
        }
    }
    events
}

/// Events with a source address, which an options or firewall record may lack.
fn addressed(events: &[Value]) -> usize {
    events
        .iter()
        .filter(|event| event["flow"]["src_ip"].is_string())
        .count()
}

/// The events each fixture yields, and how many carry a source address. dfe-infra's `flow`
/// acceptance case holds each table to these.
#[test]
fn each_fixture_yields_the_events_the_acceptance_case_counts() {
    let expected = [
        (
            "netgauze-pmacct-ipfix-cisco-100.pcap",
            "netflow",
            12usize,
            12usize,
        ),
        ("netgauze-pmacct-nfv9-cisco-101.pcap", "netflow", 51, 51),
        ("telegraf-netflow-v5.bin", "netflow", 8, 8),
        ("telegraf-netflow-v9.bin", "netflow", 8, 8),
        ("telegraf-netflow-v9-options.bin", "netflow", 8, 0),
        ("telegraf-ipfix-example.bin", "netflow", 0, 0),
        ("telegraf-ipfix-pen-35632.bin", "netflow", 0, 0),
        ("telegraf-sflow-v5.bin", "sflow", 1, 0),
        ("telegraf-sflow-issue-15918.bin", "sflow", 7, 6),
        ("telegraf-sflow-issue-18876.bin", "sflow", 0, 0),
    ];
    let mut netflow = NetflowDecoder::new(1000, 10_000, flow_metrics_for_test());
    let mut sflow = SflowDecoder::new();
    let measured: Vec<_> = expected
        .iter()
        .map(|(name, source, events, with_address)| {
            let found = exploded(name, &mut netflow, &mut sflow);
            (*name, *source, *events, *with_address, found)
        })
        .collect();
    for (name, source, _, _, events) in &measured {
        eprintln!(
            "MEASURED {name} {source} {} {}",
            events.len(),
            addressed(events)
        );
    }
    // With DFE_FLOW_DUMP naming a directory, each source's events are written there as
    // `flow_<source>_receiver_exploded.ndjson`, the fixtures dfe-schemas' producer contract reads.
    if let Some(dir) = std::env::var_os("DFE_FLOW_DUMP") {
        for source in ["netflow", "sflow"] {
            let mut text = String::new();
            for (_, _, _, _, events) in measured.iter().filter(|row| row.1 == source) {
                for event in events {
                    text.push_str(&event.to_string());
                    text.push('\n');
                }
            }
            let file = format!("flow_{source}_receiver_exploded.ndjson");
            std::fs::write(Path::new(&dir).join(file), text).unwrap();
        }
    }
    for (name, source, count, with_address, events) in &measured {
        assert!(
            events.iter().all(|event| event["_source"] == *source),
            "{name}: every event is labelled {source}"
        );
        assert_eq!(events.len(), *count, "{name}: events");
        assert_eq!(addressed(events), *with_address, "{name}: addressed events");
    }
}
