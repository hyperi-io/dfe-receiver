// Project:   dfe-receiver
// File:      tests/integration/raw_capture.rs
// Purpose:   Per-transport `_raw` retention through the real config cascade
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Raw-capture coverage across every transport that carries raw data.
//!
//! The unit tests in each `convert` module cover the shape of `_raw` itself.
//! These go one layer out: they build a real `Config`, resolve the cascade the
//! way `Server::build_handlers` does, and push a payload through the
//! transport's own public conversion path. That is what proves an operator
//! setting `syslog.raw_capture.enabled: true` in YAML actually reaches the
//! bytes on the wire, rather than the converter merely honouring a struct
//! handed to it directly.
//!
//! One test per capturing transport: syslog, the Splunk HEC raw endpoint,
//! NetFlow and sFlow. The JSON transports (GELF, fluent, OTLP, Prometheus
//! Remote Write, HEC `/event`) have nothing raw to capture, since the loader
//! keeps their JSON record in `_json`.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use dfe_receiver::config::{Config, RawCapture, RawCaptureConfig};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Cascade helpers
// ---------------------------------------------------------------------------

/// A config with capture turned on for one transport only, via the
/// per-transport override -- the common block stays at its default (off).
fn config_with(transport: fn(&mut Config) -> &mut RawCaptureConfig) -> Config {
    let mut config = Config::default();
    let slot = transport(&mut config);
    slot.enabled = Some(true);
    config
}

/// The capture settings the server would hand this transport's handler.
fn resolved(config: &Config, transport: fn(&Config) -> &RawCaptureConfig) -> RawCapture {
    config.raw_capture_for(transport(config))
}

/// Parse the `_raw` string of an event as JSON. Panics if absent.
fn captured_json(event: &Value) -> Value {
    let raw = event["_raw"]
        .as_str()
        .expect("_raw present and a JSON string");
    serde_json::from_str(raw).expect("_raw holds parseable JSON")
}

// ---------------------------------------------------------------------------
// The cascade itself
// ---------------------------------------------------------------------------

#[test]
fn common_block_switches_on_every_capturing_transport() {
    let mut config = Config::default();
    config.raw_capture.enabled = Some(true);
    config.raw_capture.max_bytes = Some(4096);

    let every = [
        config.raw_capture_for(&config.syslog.raw_capture),
        config.raw_capture_for(&config.splunk_hec.raw_capture),
        config.raw_capture_for(&config.flow.raw_capture),
    ];

    for resolved in every {
        assert!(resolved.enabled);
        assert_eq!(resolved.max_bytes, 4096);
    }
}

#[test]
fn a_transport_can_opt_out_of_a_common_opt_in() {
    let mut config = Config::default();
    config.raw_capture.enabled = Some(true);
    config.splunk_hec.raw_capture.enabled = Some(false);

    assert!(config.raw_capture_for(&config.syslog.raw_capture).enabled);
    assert!(
        !config
            .raw_capture_for(&config.splunk_hec.raw_capture)
            .enabled
    );
}

#[test]
fn capture_is_off_by_default_everywhere() {
    let config = Config::default();
    assert!(!config.raw_capture_for(&config.syslog.raw_capture).enabled);
    assert!(
        !config
            .raw_capture_for(&config.splunk_hec.raw_capture)
            .enabled
    );
    assert!(!config.raw_capture_for(&config.flow.raw_capture).enabled);
}

// ---------------------------------------------------------------------------
// syslog
// ---------------------------------------------------------------------------

#[test]
fn syslog_capture_reaches_the_converter() {
    use dfe_receiver::server::syslog::convert::syslog_to_json;

    let config = config_with(|c| &mut c.syslog.raw_capture);
    let raw = resolved(&config, |c| &c.syslog.raw_capture);

    let line = "<165>1 2026-03-03T10:30:00Z web01 nginx 1234 ID47 - upstream timed out";
    let event: Value = serde_json::from_slice(&syslog_to_json(line, raw).unwrap()).unwrap();

    assert_eq!(event["message"], "upstream timed out");
    assert_eq!(event["_raw"], line);
}

// ---------------------------------------------------------------------------
// Splunk HEC -- both endpoints
// ---------------------------------------------------------------------------

#[test]
fn splunk_hec_raw_endpoint_capture_reaches_the_converter() {
    use dfe_receiver::server::splunk_hec::convert::{RawMetadata, raw_to_json};

    let config = config_with(|c| &mut c.splunk_hec.raw_capture);
    let raw = resolved(&config, |c| &c.splunk_hec.raw_capture);

    let line = b"Oct 11 22:14:15 mymachine su: authentication failure";
    let event: Value =
        serde_json::from_slice(&raw_to_json(line, &RawMetadata::default(), raw).unwrap()).unwrap();

    assert_eq!(
        event["_raw"].as_str().unwrap(),
        "Oct 11 22:14:15 mymachine su: authentication failure"
    );
}

// ---------------------------------------------------------------------------
// Flow -- NetFlow and sFlow
// ---------------------------------------------------------------------------

/// Minimal NetFlow v5 datagram carrying one flow record.
fn netflow_v5_packet() -> Vec<u8> {
    let mut pkt = vec![0u8; 72];
    pkt[0..2].copy_from_slice(&5u16.to_be_bytes()); // version
    pkt[2..4].copy_from_slice(&1u16.to_be_bytes()); // count
    pkt[4..8].copy_from_slice(&1000u32.to_be_bytes()); // sys_uptime
    pkt[8..12].copy_from_slice(&1_700_000_000u32.to_be_bytes()); // unix_secs

    let rec = &mut pkt[24..];
    rec[0..4].copy_from_slice(&[10, 0, 0, 1]); // src_addr
    rec[4..8].copy_from_slice(&[10, 0, 0, 2]); // dst_addr
    rec[16..20].copy_from_slice(&5u32.to_be_bytes()); // d_pkts
    rec[20..24].copy_from_slice(&1500u32.to_be_bytes()); // d_octets
    rec[32..34].copy_from_slice(&12345u16.to_be_bytes()); // src_port
    rec[34..36].copy_from_slice(&80u16.to_be_bytes()); // dst_port
    rec[38] = 0x18; // tcp_flags
    rec[39] = 6; // protocol
    pkt
}

#[test]
fn netflow_capture_reaches_the_envelope() {
    use dfe_receiver::config::RawCapture as Rc;
    use dfe_receiver::server::flow::config::OutputMode;
    use dfe_receiver::server::flow::decoder::FlowDecoder;
    use dfe_receiver::server::flow::dispatch::ProtocolKind;
    use dfe_receiver::server::flow::envelope::render_packet;
    use dfe_receiver::server::flow::metrics::mock::flow_metrics_for_test;
    use dfe_receiver::server::netflow::decoder::NetflowDecoder;

    let config = config_with(|c| &mut c.flow.raw_capture);
    let raw: Rc = resolved(&config, |c| &c.flow.raw_capture);

    let mut netflow = NetflowDecoder::new(1000, 10_000, flow_metrics_for_test());
    let packet = netflow
        .decode(
            &netflow_v5_packet(),
            "127.0.0.1".parse().unwrap(),
            ProtocolKind::NetflowV5,
        )
        .expect("v5 decodes");

    let mut buf = Vec::new();
    let ranges = render_packet::<NetflowDecoder>(
        &packet,
        OutputMode::Canonical,
        raw,
        "2026-05-20T00:00:00Z",
        &mut buf,
    )
    .unwrap();

    let event: Value = serde_json::from_slice(&buf[ranges[0].clone()]).unwrap();
    assert_eq!(event["flows"][0]["src_ip"], "10.0.0.1");

    let captured = captured_json(&event);
    assert_eq!(captured[0]["src_addr"], "10.0.0.1");
    assert_eq!(captured[0]["d_octets"], 1500);
}

#[test]
fn sflow_capture_reaches_the_envelope() {
    use dfe_receiver::server::flow::config::OutputMode;
    use dfe_receiver::server::flow::decoder::DecodedPacket;
    use dfe_receiver::server::flow::dispatch::ProtocolKind;
    use dfe_receiver::server::flow::envelope::render_packet;
    use dfe_receiver::server::sflow::decoder::{SflowDecoder, SflowRecord};

    let config = config_with(|c| &mut c.flow.raw_capture);
    let raw = resolved(&config, |c| &c.flow.raw_capture);

    let decoded = DecodedPacket {
        exporter_ip: "10.0.0.1".parse().unwrap(),
        observation_domain: 0,
        packet_seq: 7,
        kind: ProtocolKind::SflowV5,
        records: vec![SflowRecord::Counter {
            generic: None,
            raw_json: r#"{"kind":"counter","if_index":3}"#.into(),
        }],
    };

    let mut buf = Vec::new();
    let ranges = render_packet::<SflowDecoder>(
        &decoded,
        OutputMode::Canonical,
        raw,
        "2026-05-20T00:00:00Z",
        &mut buf,
    )
    .unwrap();

    let event: Value = serde_json::from_slice(&buf[ranges[0].clone()]).unwrap();
    assert_eq!(event["_source"], "sflow");

    let captured = captured_json(&event);
    assert_eq!(captured[0]["if_index"], 3);
}
