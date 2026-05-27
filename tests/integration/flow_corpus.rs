// Project:   dfe-receiver
// File:      tests/integration/flow_corpus.rs
// Purpose:   Real-world PCAP / UDP-payload corpus integration tests
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Real-world flow corpus integration tests.
//!
//! Iterates `tests/fixtures/flow/external/`. Two file types:
//!
//! - `*.bin` -- raw UDP-payload bytes (one datagram per file). Feed
//!   directly to the decoder.
//! - `*.pcap` -- libpcap file containing UDP datagrams; we walk the
//!   capture, strip the Ethernet+IPv4+UDP prefix heuristically, and feed
//!   the resulting bytes to the decoder.
//!
//! For each datagram we assert:
//!   - dispatch_protocol_kind recognises it as a known protocol
//!   - decoder doesn't panic
//!   - SHA256 of every file matches `sha256sums.txt` (no silent fixture rot)
//!
//! This catches real-world vendor quirks that hand-crafted unit tests miss
//! (count=0 keep-alives, padded packets, jumbo templates, vendor-specific
//! private enterprise IEs, etc.)
//!
//! Provenance + license for every fixture is documented in
//! `tests/fixtures/flow/external/README.md`. Sources: influxdata/telegraf
//! (MIT) and NetGauze (Apache-2.0) -- both license-compatible with
//! FSL-1.1-ALv2.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::fs::File;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};

use dfe_receiver::server::flow::decoder::FlowDecoder;
use dfe_receiver::server::flow::dispatch::{ProtocolKind, dispatch_protocol_kind};
use dfe_receiver::server::flow::metrics::{
    FlowCounter, FlowHistogram, FlowLabelledCounter, FlowLabelledGauge, FlowMetrics,
};
use dfe_receiver::server::netflow::decoder::NetflowDecoder;
use dfe_receiver::server::sflow::decoder::SflowDecoder;
use pcap_file::pcap::PcapReader;
use sha2::{Digest, Sha256};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// No-op FlowMetrics. NetflowDecoder requires a FlowMetrics; the corpus test
// asserts on decoded record counts, not metric counters.
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

// ---------------------------------------------------------------------------
// Fixture discovery and extraction
// ---------------------------------------------------------------------------

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/flow/external")
}

/// Walk a libpcap file and pull out the UDP payload of every packet. The
/// fixtures we ship are captured with standard Ethernet link-layer (DLT=1),
/// so the wire bytes look like:
///
/// ```text
/// Ethernet (14) + IPv4 (20+ -- usually 20) + UDP (8) = 42 bytes
/// ```
///
/// We try common prefix sizes (42 ethernet+ipv4+udp, 32 raw ipv4+udp,
/// 4 loopback null+udp, 0 raw udp) and pick the first one where the
/// resulting bytes parse as a known protocol. Heuristic but robust against
/// the small variety of link-layer headers public test PCAPs ship with.
fn extract_udp_payloads(pcap_path: &Path) -> Vec<Vec<u8>> {
    let f = File::open(pcap_path).expect("fixture exists");
    let mut reader = PcapReader::new(f).expect("valid pcap");
    let mut payloads = Vec::new();
    while let Some(Ok(packet)) = reader.next_raw_packet() {
        let data = packet.data.as_ref();
        for prefix in [42usize, 32, 4, 0] {
            if data.len() <= prefix {
                continue;
            }
            let candidate = &data[prefix..];
            let kind = dispatch_protocol_kind(candidate);
            if !matches!(kind, ProtocolKind::TooShort | ProtocolKind::Unknown) {
                payloads.push(candidate.to_vec());
                break;
            }
        }
    }
    payloads
}

/// Pick an exporter IP for the decoder. Any plausible IPv4 is fine -- the
/// decoder uses it as a HashMap key for per-exporter template state. The
/// real exporter IP from the PCAP isn't recoverable without parsing the IP
/// header here, which the extraction heuristic above intentionally skips.
fn synthetic_exporter() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn corpus_directory_populated() {
    let dir = fixtures_dir();
    assert!(dir.exists(), "external fixtures dir missing: {dir:?}");
    let count = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| {
            let path = e.path();
            let ext = path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            ext == "pcap" || ext == "pcapng" || ext == "bin"
        })
        .count();
    assert!(count > 0, "no fixture files in {dir:?}");
    // We expect at least one fixture per supported protocol family.
    assert!(
        count >= 4,
        "expected >=4 fixtures (v5, v9, ipfix, sflow); found {count}"
    );
}

/// SHA256 every fixture against the manifest. Catches silent fixture
/// rot -- if upstream replaces a file or download was corrupted, fail loudly.
#[test]
fn corpus_sha256_matches_manifest() {
    let dir = fixtures_dir();
    let manifest_path = dir.join("sha256sums.txt");
    let manifest = std::fs::read_to_string(&manifest_path).expect("sha256sums.txt missing");

    let mut expected: BTreeMap<String, String> = BTreeMap::new();
    for line in manifest.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let hash = parts.next().unwrap_or("").to_string();
        let rest = parts.next().unwrap_or("").trim();
        // sha256sum -b uses '*' prefix; strip it
        let filename = rest.trim_start_matches('*').to_string();
        assert!(
            !(hash.len() != 64 || filename.is_empty()),
            "malformed sha256sums.txt line: {line:?}"
        );
        expected.insert(filename, hash);
    }
    assert!(
        !expected.is_empty(),
        "sha256sums.txt parsed to zero entries"
    );

    for (filename, expected_hash) in &expected {
        let path = dir.join(filename);
        let bytes = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("manifest lists {filename} but cannot read: {e}"));
        let actual_hash = format!("{:x}", Sha256::digest(&bytes));
        assert_eq!(
            &actual_hash, expected_hash,
            "SHA256 mismatch for {filename}"
        );
    }
}

/// For every fixture in the corpus, exercise the appropriate decoder and
/// assert at least one record is decoded. Prints a per-file summary at the
/// end so regressions in decode coverage are visible from `cargo nextest`
/// output.
#[test]
fn corpus_decodes_real_world_datagrams() {
    let dir = fixtures_dir();
    let exporter = synthetic_exporter();
    let mut netflow_dec = NetflowDecoder::new(1000, 10_000, noop_flow_metrics());
    let mut sflow_dec = SflowDecoder::new();

    // BTreeMap so summary output is deterministic
    let mut summary: BTreeMap<String, FixtureResult> = BTreeMap::new();

    for entry in std::fs::read_dir(&dir).unwrap().filter_map(Result::ok) {
        let path = entry.path();
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        let payloads = match ext {
            "bin" => vec![std::fs::read(&path).unwrap()],
            "pcap" | "pcapng" => extract_udp_payloads(&path),
            _ => continue,
        };
        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("?")
            .to_string();

        let mut result = FixtureResult::default();
        for payload in &payloads {
            let kind = dispatch_protocol_kind(payload);
            result.datagrams += 1;
            match kind {
                ProtocolKind::NetflowV5 | ProtocolKind::NetflowV9 | ProtocolKind::Ipfix => {
                    match netflow_dec.decode(payload, exporter, kind) {
                        Ok(pkt) => result.records += pkt.records.len(),
                        Err(_) => result.decode_errors += 1,
                    }
                }
                ProtocolKind::SflowV5 => match sflow_dec.decode(payload, exporter, kind) {
                    Ok(pkt) => result.records += pkt.records.len(),
                    Err(_) => result.decode_errors += 1,
                },
                ProtocolKind::TooShort | ProtocolKind::Unknown => {
                    result.unknown_protocol += 1;
                }
                ProtocolKind::NetflowV7 => {
                    // v7 is unsupported by design; counted but not a failure
                    result.unsupported += 1;
                }
            }
        }
        summary.insert(filename, result);
    }

    // Print summary so test output documents what was exercised.
    eprintln!("\nReal-world flow corpus decode summary:");
    eprintln!(
        "  {:<48} datagrams records decode_errs unknown unsupported",
        "file"
    );
    for (file, r) in &summary {
        eprintln!(
            "  {:<48} {:>9} {:>7} {:>11} {:>7} {:>11}",
            file, r.datagrams, r.records, r.decode_errors, r.unknown_protocol, r.unsupported
        );
    }

    // Aggregate assertions. Each file is expected to produce at least one
    // datagram that the dispatcher recognises, and at least one decoded
    // record across the corpus.
    //
    // Note: per-file 0 records is legitimate -- IPFIX/NetFlow v9 first
    // packets are often template-only (no data records), and some sFlow
    // fixtures (e.g. telegraf-sflow-issue-18876) carry only counter
    // samples without flow samples. We assert corpus-wide records > 0,
    // not per-file.
    let total_records: usize = summary.values().map(|r| r.records).sum();
    let total_datagrams: usize = summary.values().map(|r| r.datagrams).sum();
    assert!(total_datagrams > 0, "corpus produced zero datagrams");
    assert!(
        total_records > 0,
        "corpus decoded zero records (decoder regression?)"
    );
    // Sanity floor: at least 50 records across the whole corpus. The
    // pcap fixtures alone deliver >60; this catches a decoder regression
    // that produces nothing from real captures.
    assert!(
        total_records >= 50,
        "corpus decoded only {total_records} records -- expected >=50 from real captures"
    );

    // Per-protocol expectations. The dispatcher version-tags each fixture
    // by name convention -- if a file is named like *netflow-v5* and the
    // dispatcher rejects it, that's a regression we want to catch.
    for (file, r) in &summary {
        let lower = file.to_ascii_lowercase();
        // ipfix uses the netflow decoder family, so it groups with netflow.
        let expected_family = if lower.contains("netflow-v5")
            || lower.contains("netflow-v9")
            || lower.contains("nfv9")
            || lower.contains("ipfix")
        {
            Some("netflow")
        } else if lower.contains("sflow") {
            Some("sflow")
        } else {
            None
        };
        if expected_family.is_some() {
            assert!(
                r.datagrams > 0,
                "{file}: expected at least one datagram, got none"
            );
            assert_eq!(
                r.unknown_protocol, 0,
                "{file}: dispatcher failed to recognise any datagram"
            );
        }
    }
}

#[derive(Default)]
struct FixtureResult {
    datagrams: usize,
    records: usize,
    decode_errors: usize,
    unknown_protocol: usize,
    unsupported: usize,
}
