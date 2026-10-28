// Project:   dfe-receiver
// File:      tests/integration/fluent_bit.rs
// Purpose:   Fluent Bit in Docker shipping to the receiver binary over every protocol it speaks
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One Fluent Bit container, one config with an input and an output per case,
//! against two receiver processes: one with TLS on, one in plaintext.
//!
//! Every positive case ships records tagged with its own marker and must see
//! them arrive at the destination. A negative case (wrong CA, no client
//! certificate, plaintext to a TLS port) must not deliver, and Fluent Bit must
//! log the failure. Forward, syslog, HTTP, Splunk HEC, Prometheus remote write,
//! GELF and OpenTelemetry (HTTP) all run with TLS on.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use dfe_receiver::config::Config;

use crate::common::peers::{
    Cases, FLUENT_BIT_IMAGE, PEER_DIR, Peer, PeerSpec, Pki, Receiver, Sink, free_addr,
};
use crate::{skip_if_no_docker, test_name};

/// The event time Fluent Bit stamps on the forward cases (2023-11-14T22:13:20Z).
const FIXED_TIME: u64 = 1_700_000_000;

/// How a Fluent Bit output reaches the receiver.
#[derive(Clone, Copy)]
enum Tls {
    Off,
    /// Verify the server against this CA file, and present a client certificate if named.
    On {
        ca: &'static str,
        client: Option<&'static str>,
    },
}

/// One Fluent Bit input and output pair.
struct Case {
    id: &'static str,
    /// The records that must arrive, or `None` for a case that must not deliver.
    expect: Option<usize>,
    input: String,
    output: String,
}

fn tls_lines(tls: Tls) -> String {
    match tls {
        Tls::Off => String::new(),
        Tls::On { ca, client } => {
            let mut lines = format!(
                "      tls: on\n      tls.verify: on\n      tls.vhost: localhost\n      tls.ca_file: {PEER_DIR}/{ca}.crt\n"
            );
            if let Some(client) = client {
                lines.push_str(&format!(
                    "      tls.crt_file: {PEER_DIR}/{client}.crt\n      tls.key_file: {PEER_DIR}/{client}.key\n"
                ));
            }
            lines
        }
    }
}

/// A `dummy` input emitting `samples` records carrying the case marker.
fn dummy_input(id: &str, samples: usize, fixed_time: bool) -> String {
    let time = if fixed_time {
        format!("      start_time_sec: {FIXED_TIME}\n      fixed_timestamp: on\n")
    } else {
        String::new()
    };
    format!(
        "    - name: dummy\n      tag: {id}\n      dummy: '{{\"message\":\"{id} hello\",\"case\":\"{id}\",\"source_host\":\"fbhost\",\"environment\":\"testing\"}}'\n      samples: {samples}\n      rate: {samples}\n{time}"
    )
}

fn output(name: &str, id: &str, addr: SocketAddr, tls: Tls, extra: &str) -> String {
    format!(
        "    - name: {name}\n      match: {id}\n      host: {}\n      port: {}\n      retry_limit: 3\n{extra}{}",
        addr.ip(),
        addr.port(),
        tls_lines(tls)
    )
}

fn case(id: &'static str, expect: Option<usize>, input: String, output: String) -> Case {
    Case {
        id,
        expect,
        input,
        output,
    }
}

#[tokio::test]
async fn fluent_bit_ships_to_every_listener_over_tls() {
    skip_if_no_docker!();
    let test = test_name!();
    let pki = Pki::generate().await;
    let sink = Sink::start().await;

    // Receiver with TLS: forward and syslog require a client certificate, the rest only serve one.
    let mut tls_cfg = Config::default();
    let http = free_addr();
    let forward = free_addr();
    let syslog = free_addr();
    let hec = free_addr();
    let prom = free_addr();
    let gelf = free_addr();
    let otlp_http = free_addr();
    tls_cfg.server.bind_address = http.to_string();
    tls_cfg.server.auth.mode = "none".to_string();
    tls_cfg.server.tls = pki.server_tls("none");
    tls_cfg.fluent.enabled = true;
    tls_cfg.fluent.bind_address = forward.to_string();
    tls_cfg.fluent.tls = pki.server_tls("required");
    tls_cfg.syslog.enabled = true;
    tls_cfg.syslog.udp_bind_address = free_addr().to_string();
    tls_cfg.syslog.tcp_bind_address = free_addr().to_string();
    tls_cfg.syslog.tls_bind_address = syslog.to_string();
    tls_cfg.syslog.accept_unauthenticated = true;
    tls_cfg.syslog.tls = pki.server_tls("required");
    tls_cfg.splunk_hec.enabled = true;
    tls_cfg.splunk_hec.bind_address = hec.to_string();
    tls_cfg.splunk_hec.auth.mode = "none".to_string();
    tls_cfg.splunk_hec.tls = pki.server_tls("none");
    tls_cfg.prometheus_rw.enabled = true;
    tls_cfg.prometheus_rw.bind_address = prom.to_string();
    tls_cfg.prometheus_rw.auth.mode = "none".to_string();
    tls_cfg.prometheus_rw.tls = pki.server_tls("none");
    tls_cfg.gelf.enabled = true;
    tls_cfg.gelf.bind_address = gelf.to_string();
    tls_cfg.gelf.accept_unauthenticated = true;
    tls_cfg.gelf.tls = pki.server_tls("none");
    tls_cfg.otlp.enabled = true;
    tls_cfg.otlp.grpc_bind_address = free_addr().to_string();
    tls_cfg.otlp.http_bind_address = otlp_http.to_string();
    tls_cfg.otlp.auth.mode = "none".to_string();
    tls_cfg.otlp.tls = pki.server_tls("none");
    let tls_ready = [http, forward, syslog, hec, prom, gelf, otlp_http];
    let mut tls_receiver = Receiver::start(tls_cfg, &sink, &tls_ready)
        .await
        .unwrap_or_else(|e| panic!("{e}"));

    // Receiver in plaintext, for the protocol cases that do not depend on TLS.
    let mut plain_cfg = Config::default();
    let plain_http = free_addr();
    let plain_forward = free_addr();
    let plain_gelf = free_addr();
    plain_cfg.server.bind_address = plain_http.to_string();
    plain_cfg.server.auth.mode = "none".to_string();
    plain_cfg.fluent.enabled = true;
    plain_cfg.fluent.bind_address = plain_forward.to_string();
    plain_cfg.fluent.accept_unauthenticated = true;
    plain_cfg.gelf.enabled = true;
    plain_cfg.gelf.bind_address = plain_gelf.to_string();
    plain_cfg.gelf.accept_unauthenticated = true;
    let mut plain_receiver =
        Receiver::start(plain_cfg, &sink, &[plain_http, plain_forward, plain_gelf])
            .await
            .unwrap_or_else(|e| panic!("{e}"));

    let mtls = Tls::On {
        ca: "ca",
        client: Some("client"),
    };
    let server_only = Tls::On {
        ca: "ca",
        client: None,
    };
    let gelf_extra = "      mode: tcp\n      gelf_short_message_key: message\n      gelf_host_key: source_host\n";
    let gelf_tls_extra = "      mode: tls\n      gelf_short_message_key: message\n      gelf_host_key: source_host\n";

    let cases = vec![
        case(
            "fbcase-forward-mtls",
            Some(10),
            dummy_input("fbcase-forward-mtls", 10, true),
            output("forward", "fbcase-forward-mtls", forward, mtls, ""),
        ),
        case(
            "fbcase-forward-mtls-ack",
            Some(5),
            dummy_input("fbcase-forward-mtls-ack", 5, false),
            output(
                "forward",
                "fbcase-forward-mtls-ack",
                forward,
                mtls,
                "      require_ack_response: true\n",
            ),
        ),
        case(
            "fbcase-forward-wrong-ca",
            None,
            dummy_input("fbcase-forward-wrong-ca", 3, false),
            output(
                "forward",
                "fbcase-forward-wrong-ca",
                forward,
                Tls::On {
                    ca: "rogue-ca",
                    client: Some("client"),
                },
                "",
            ),
        ),
        case(
            "fbcase-forward-no-client-cert",
            None,
            dummy_input("fbcase-forward-no-client-cert", 3, false),
            output(
                "forward",
                "fbcase-forward-no-client-cert",
                forward,
                server_only,
                "      require_ack_response: true\n",
            ),
        ),
        case(
            "fbcase-forward-plaintext-to-tls",
            None,
            dummy_input("fbcase-forward-plaintext-to-tls", 3, false),
            output(
                "forward",
                "fbcase-forward-plaintext-to-tls",
                forward,
                Tls::Off,
                "      require_ack_response: true\n",
            ),
        ),
        case(
            "fbcase-syslog-tls",
            Some(5),
            dummy_input("fbcase-syslog-tls", 5, false),
            output(
                "syslog",
                "fbcase-syslog-tls",
                syslog,
                mtls,
                "      mode: tls\n      syslog_format: rfc5424\n      syslog_message_key: message\n      syslog_severity_preset: 6\n      syslog_hostname_preset: fbhost\n      syslog_appname_preset: fb\n",
            ),
        ),
        case(
            "fbcase-http-tls",
            Some(5),
            dummy_input("fbcase-http-tls", 5, false),
            output(
                "http",
                "fbcase-http-tls",
                http,
                server_only,
                "      uri: /ingest\n      format: json_lines\n",
            ),
        ),
        case(
            "fbcase-splunk-hec-tls",
            Some(5),
            dummy_input("fbcase-splunk-hec-tls", 5, false),
            output(
                "splunk",
                "fbcase-splunk-hec-tls",
                hec,
                server_only,
                "      splunk_token: fb-test-token\n",
            ),
        ),
        case(
            "fbcase-prometheus-rw-tls",
            Some(1),
            "    - name: fluentbit_metrics\n      tag: fbcase-prometheus-rw-tls\n      scrape_interval: 1\n".to_string(),
            output(
                "prometheus_remote_write",
                "fbcase-prometheus-rw-tls",
                prom,
                server_only,
                "      uri: /api/v1/write\n      add_label: marker fbcase-prometheus-rw-tls\n",
            ),
        ),
        case(
            "fbcase-gelf-tls",
            Some(5),
            dummy_input("fbcase-gelf-tls", 5, false),
            output("gelf", "fbcase-gelf-tls", gelf, server_only, gelf_tls_extra),
        ),
        case(
            "fbcase-otlp-http-tls",
            Some(5),
            dummy_input("fbcase-otlp-http-tls", 5, false),
            output(
                "opentelemetry",
                "fbcase-otlp-http-tls",
                otlp_http,
                server_only,
                "      logs_uri: /v1/logs\n",
            ),
        ),
        case(
            "fbcase-forward-plain",
            Some(50),
            dummy_input("fbcase-forward-plain", 50, true),
            output("forward", "fbcase-forward-plain", plain_forward, Tls::Off, ""),
        ),
        case(
            "fbcase-forward-plain-ack",
            Some(5),
            dummy_input("fbcase-forward-plain-ack", 5, false),
            output(
                "forward",
                "fbcase-forward-plain-ack",
                plain_forward,
                Tls::Off,
                "      require_ack_response: true\n",
            ),
        ),
        case(
            "fbcase-gelf-plain",
            Some(5),
            dummy_input("fbcase-gelf-plain", 5, false),
            output("gelf", "fbcase-gelf-plain", plain_gelf, Tls::Off, gelf_extra),
        ),
    ];

    let mut inputs = String::new();
    let mut outputs = String::new();
    for case in &cases {
        inputs.push_str(&case.input);
        outputs.push_str(&case.output);
    }
    let yaml = format!(
        "service:\n  flush: 1\n  grace: 2\n  log_level: info\n\npipeline:\n  inputs:\n{inputs}\n  outputs:\n{outputs}"
    );
    let files = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(pki.dir()).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), files.path().join(entry.file_name())).unwrap();
    }
    std::fs::write(files.path().join("fluent-bit.yaml"), &yaml).unwrap();

    let peer = Peer::start(PeerSpec {
        test,
        service: "fluent-bit",
        image: FLUENT_BIT_IMAGE,
        entrypoint: None,
        args: vec![
            "--config".to_string(),
            format!("{PEER_DIR}/fluent-bit.yaml"),
        ],
        files: Some(files.path()),
        user: None,
        env: vec![],
        extra_files: vec![],
    })
    .await
    .unwrap_or_else(|e| panic!("the Fluent Bit peer did not start: {e}"));

    let mut results = Cases::default();
    let deadline = Instant::now() + Duration::from_secs(90);
    for case in cases.iter().filter(|c| c.expect.is_some()) {
        let want = case.expect.unwrap_or(0);
        let arrived = sink
            .wait_for_count(
                case.id,
                want,
                deadline.saturating_duration_since(Instant::now()),
            )
            .await;
        results.check(
            &format!("{}: {want} records arrive", case.id),
            arrived,
            || format!("only {} arrived", sink.count(case.id)),
        );
    }

    // Negative cases have had the whole positive run to retry in.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let logs = peer.logs().await;
    for (index, case) in cases.iter().enumerate().filter(|(_, c)| c.expect.is_none()) {
        results.check(
            &format!("{}: nothing is delivered", case.id),
            sink.count(case.id) == 0,
            || sink.find(case.id).unwrap_or_default(),
        );
        // Without this an output Fluent Bit never ran would pass as a refusal. Outputs are
        // numbered in config order.
        let plugin = case
            .output
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .trim_start_matches("- name: ");
        let tag = format!("[output:{plugin}:{plugin}.{index}]");
        results.check(
            &format!("{}: fluent bit tried and logged the failure", case.id),
            logs.lines()
                .any(|l| l.contains(&tag) && l.contains("[error]")),
            || logs.clone(),
        );
    }

    for (id, field) in [
        ("fbcase-forward-mtls", "timestamp"),
        ("fbcase-forward-plain", "timestamp"),
    ] {
        let record = sink.find(id).unwrap_or_default();
        let value: serde_json::Value = serde_json::from_str(&record).unwrap_or_default();
        results.check(
            &format!("{id}: the event time survives as {field}"),
            #[allow(clippy::cast_precision_loss)]
            (value[field].as_f64() == Some(FIXED_TIME as f64)),
            || record.clone(),
        );
    }

    results.check(
        "the TLS receiver is still running",
        tls_receiver.alive(),
        || tls_receiver.log_text(),
    );
    results.check(
        "the plain receiver is still running",
        plain_receiver.alive(),
        || plain_receiver.log_text(),
    );
    results.finish(|| {
        format!(
            "fluent bit log:\n{logs}\n\nTLS receiver log:\n{}\n\n{}",
            tls_receiver.log_text(),
            sink.dump()
        )
    });
}
