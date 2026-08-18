// Project:   dfe-receiver
// File:      tests/integration/raw_capture.rs
// Purpose:   Per-transport `_raw` retention through the real config cascade
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Raw-capture coverage across every transport that supports it.
//!
//! The unit tests in each `convert` module cover the shape of `_raw` itself.
//! These go one layer out: they build a real `Config`, resolve the cascade the
//! way `Server::build_handlers` does, and push a payload through the
//! transport's own public conversion path. That is what proves an operator
//! setting `syslog.raw_capture.enabled: true` in YAML actually reaches the
//! bytes on the wire, rather than the converter merely honouring a struct
//! handed to it directly.
//!
//! One test per capturing transport: syslog, GELF, Fluent Forward, Splunk HEC
//! (both endpoints), Prometheus Remote Write, OTLP (logs, traces, metrics),
//! NetFlow and sFlow.

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
        config.raw_capture_for(&config.gelf.raw_capture),
        config.raw_capture_for(&config.fluent.raw_capture),
        config.raw_capture_for(&config.splunk_hec.raw_capture),
        config.raw_capture_for(&config.prometheus_rw.raw_capture),
        config.raw_capture_for(&config.otlp.raw_capture),
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
    config.prometheus_rw.raw_capture.enabled = Some(false);

    assert!(config.raw_capture_for(&config.syslog.raw_capture).enabled);
    assert!(
        !config
            .raw_capture_for(&config.prometheus_rw.raw_capture)
            .enabled
    );
}

#[test]
fn capture_is_off_by_default_everywhere() {
    let config = Config::default();
    assert!(!config.raw_capture_for(&config.syslog.raw_capture).enabled);
    assert!(!config.raw_capture_for(&config.gelf.raw_capture).enabled);
    assert!(!config.raw_capture_for(&config.fluent.raw_capture).enabled);
    assert!(
        !config
            .raw_capture_for(&config.splunk_hec.raw_capture)
            .enabled
    );
    assert!(
        !config
            .raw_capture_for(&config.prometheus_rw.raw_capture)
            .enabled
    );
    assert!(!config.raw_capture_for(&config.otlp.raw_capture).enabled);
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
// GELF
// ---------------------------------------------------------------------------

#[test]
fn gelf_capture_reaches_the_converter() {
    use dfe_receiver::server::gelf::convert::gelf_to_json;

    let config = config_with(|c| &mut c.gelf.raw_capture);
    let raw = resolved(&config, |c| &c.gelf.raw_capture);

    let wire = br#"{"version":"1.1","host":"web01","short_message":"disk full","level":3}"#;
    let event: Value = serde_json::from_slice(&gelf_to_json(wire, raw).unwrap()).unwrap();

    assert_eq!(event["message"], "disk full");
    let captured = captured_json(&event);
    assert_eq!(captured["short_message"], "disk full");
    assert!(captured.get("severity").is_none());
}

// ---------------------------------------------------------------------------
// Fluent Forward
// ---------------------------------------------------------------------------

#[test]
fn fluent_capture_reaches_the_converter() {
    use dfe_receiver::server::fluent::convert::fluent_to_json;
    use rmpv::Value as MsgPack;

    let config = config_with(|c| &mut c.fluent.raw_capture);
    let raw = resolved(&config, |c| &c.fluent.raw_capture);

    let msg = MsgPack::Array(vec![
        MsgPack::String("app.log".into()),
        MsgPack::Integer(1_700_000_000.into()),
        MsgPack::Map(vec![(
            MsgPack::String("message".into()),
            MsgPack::String("started".into()),
        )]),
    ]);
    let payloads = fluent_to_json(&msg, raw).unwrap();
    let event: Value = serde_json::from_slice(&payloads[0]).unwrap();

    assert_eq!(event["tag"], "app.log");
    let captured = captured_json(&event);
    assert_eq!(captured["message"], "started");
    assert!(captured.get("tag").is_none());
}

// ---------------------------------------------------------------------------
// Splunk HEC -- both endpoints
// ---------------------------------------------------------------------------

#[test]
fn splunk_hec_event_capture_reaches_the_converter() {
    use dfe_receiver::server::splunk_hec::convert::{hec_event_to_json, parse_hec_events};

    let config = config_with(|c| &mut c.splunk_hec.raw_capture);
    let raw = resolved(&config, |c| &c.splunk_hec.raw_capture);

    let body = br#"{"event":{"msg":"login failed"},"host":"idp01"}"#;
    let events = parse_hec_events(body).unwrap();
    let event: Value = serde_json::from_slice(
        &hec_event_to_json(events.into_iter().next().unwrap(), raw).unwrap(),
    )
    .unwrap();

    assert_eq!(event["host"], "idp01");
    let captured = captured_json(&event);
    assert_eq!(captured["msg"], "login failed");
    assert!(captured.get("host").is_none());
}

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
// Prometheus Remote Write
// ---------------------------------------------------------------------------

#[test]
fn prometheus_rw_capture_reaches_the_converter() {
    use dfe_receiver::server::prometheus_rw::convert::{PrometheusRwMode, write_request_to_json};
    use dfe_receiver::server::prometheus_rw::proto;

    let mut config = config_with(|c| &mut c.prometheus_rw.raw_capture);
    // hyperdx mode is where _raw carries information the event lost.
    config.prometheus_rw.mode = "hyperdx".to_string();
    let raw = resolved(&config, |c| &c.prometheus_rw.raw_capture);

    let request = proto::WriteRequest {
        timeseries: vec![proto::TimeSeries {
            labels: vec![
                proto::Label {
                    name: "__name__".into(),
                    value: "node_load1".into(),
                },
                proto::Label {
                    name: "instance".into(),
                    value: "web01:9100".into(),
                },
            ],
            samples: vec![proto::Sample {
                value: 0.75,
                timestamp: 1_709_540_000_000,
            }],
            exemplars: vec![],
            histograms: vec![],
        }],
        metadata: vec![],
    };

    let events = write_request_to_json(request, PrometheusRwMode::HyperDx, raw).unwrap();
    let event: Value = serde_json::from_slice(&events[0]).unwrap();

    assert_eq!(event["MetricName"], "node_load1");
    let captured = captured_json(&event);
    assert_eq!(captured["__name__"], "node_load1");
    assert_eq!(captured["instance"], "web01:9100");
    assert_eq!(captured["value"], 0.75);
}

// ---------------------------------------------------------------------------
// OTLP -- all three signals
// ---------------------------------------------------------------------------

#[cfg(feature = "otlp")]
#[test]
fn otlp_logs_capture_reaches_the_converter() {
    use dfe_receiver::server::otlp::convert::{OtlpMode, convert_logs};
    use dfe_receiver::server::otlp::pb;

    let config = config_with(|c| &mut c.otlp.raw_capture);
    let raw = resolved(&config, |c| &c.otlp.raw_capture);

    let request = pb::collector::logs::v1::ExportLogsServiceRequest {
        resource_logs: vec![pb::logs::v1::ResourceLogs {
            resource: None,
            scope_logs: vec![pb::logs::v1::ScopeLogs {
                scope: None,
                log_records: vec![pb::logs::v1::LogRecord {
                    time_unix_nano: 1_771_459_200_000_000_000,
                    severity_number: 17,
                    severity_text: "ERROR".to_string(),
                    body: Some(pb::common::v1::AnyValue {
                        value: Some(pb::common::v1::any_value::Value::StringValue(
                            "pool exhausted".to_string(),
                        )),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let payloads = convert_logs(&request, OtlpMode::HyperDx, raw).unwrap();
    let event: Value = serde_json::from_slice(&payloads[0].json).unwrap();

    assert_eq!(event["Body"], "pool exhausted");
    let captured = captured_json(&event);
    assert_eq!(captured["_signal"], "log");
    assert_eq!(captured["body"], "pool exhausted");
}

#[cfg(feature = "otlp")]
#[test]
fn otlp_traces_capture_reaches_the_converter() {
    use dfe_receiver::server::otlp::convert::{OtlpMode, convert_traces};
    use dfe_receiver::server::otlp::pb;

    let config = config_with(|c| &mut c.otlp.raw_capture);
    let raw = resolved(&config, |c| &c.otlp.raw_capture);

    let request = pb::collector::trace::v1::ExportTraceServiceRequest {
        resource_spans: vec![pb::trace::v1::ResourceSpans {
            resource: None,
            scope_spans: vec![pb::trace::v1::ScopeSpans {
                scope: None,
                spans: vec![pb::trace::v1::Span {
                    trace_id: vec![0xab; 16],
                    span_id: vec![0xcd; 8],
                    name: "POST /login".to_string(),
                    start_time_unix_nano: 1_771_459_200_000_000_000,
                    end_time_unix_nano: 1_771_459_200_010_000_000,
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let payloads = convert_traces(&request, OtlpMode::HyperDx, raw).unwrap();
    let event: Value = serde_json::from_slice(&payloads[0].json).unwrap();

    let captured = captured_json(&event);
    assert_eq!(captured["_signal"], "trace");
    assert_eq!(captured["name"], "POST /login");
}

#[cfg(feature = "otlp")]
#[test]
fn otlp_metrics_capture_reaches_the_converter() {
    use dfe_receiver::server::otlp::convert::{OtlpMode, convert_metrics};
    use dfe_receiver::server::otlp::pb;

    let config = config_with(|c| &mut c.otlp.raw_capture);
    let raw = resolved(&config, |c| &c.otlp.raw_capture);

    let request = pb::collector::metrics::v1::ExportMetricsServiceRequest {
        resource_metrics: vec![pb::metrics::v1::ResourceMetrics {
            resource: None,
            scope_metrics: vec![pb::metrics::v1::ScopeMetrics {
                scope: None,
                metrics: vec![pb::metrics::v1::Metric {
                    name: "queue_depth".to_string(),
                    data: Some(pb::metrics::v1::metric::Data::Gauge(
                        pb::metrics::v1::Gauge {
                            data_points: vec![pb::metrics::v1::NumberDataPoint {
                                time_unix_nano: 1_771_459_200_000_000_000,
                                value: Some(pb::metrics::v1::number_data_point::Value::AsDouble(
                                    17.0,
                                )),
                                ..Default::default()
                            }],
                        },
                    )),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let payloads = convert_metrics(&request, OtlpMode::HyperDx, raw).unwrap();
    let event: Value = serde_json::from_slice(&payloads[0].json).unwrap();

    assert_eq!(event["Value"], 17.0);
    let captured = captured_json(&event);
    assert_eq!(captured["metric_name"], "queue_depth");
    assert_eq!(captured["value"], 17.0);
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
