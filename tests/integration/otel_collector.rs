// Project:   dfe-receiver
// File:      tests/integration/otel_collector.rs
// Purpose:   The OpenTelemetry Collector in Docker exporting OTLP to the receiver binary over TLS
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One OpenTelemetry Collector (contrib) container with an exporter per case,
//! against three receiver processes: TLS, mutual TLS and plaintext.
//!
//! The exporters keep the Collector's default compression, which is gzip, since
//! that is what a deployed Collector sends. Logs come from a `filelog` receiver
//! and metrics from `hostmetrics`, each stamped with its case as a resource
//! attribute so the destination can tell the cases apart.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::fmt::Write;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use dfe_receiver::config::Config;

use crate::common::peers::{
    Cases, OTEL_COLLECTOR_IMAGE, PEER_DIR, Peer, PeerSpec, Pki, Receiver, Sink, free_addr,
};
use crate::{skip_if_no_docker, test_name};

/// One exporter, and whether its records must arrive.
struct Case {
    id: &'static str,
    arrives: bool,
    /// The exporter component name, such as `otlp/grpc-tls`.
    exporter: &'static str,
    /// The YAML body of the exporter.
    body: String,
    /// The signals the case exports.
    signals: &'static [&'static str],
}

fn tls_block(ca: &str, client: Option<&str>) -> String {
    let mut text = format!("    tls:\n      ca_file: {PEER_DIR}/{ca}.crt\n");
    if let Some(client) = client {
        text.push_str(&format!(
            "      cert_file: {PEER_DIR}/{client}.crt\n      key_file: {PEER_DIR}/{client}.key\n"
        ));
    }
    text
}

fn grpc(addr: SocketAddr, tls: &str) -> String {
    format!(
        "    endpoint: {addr}\n{tls}    timeout: 5s\n    retry_on_failure:\n      enabled: false\n    sending_queue:\n      enabled: false\n"
    )
}

fn http(addr: SocketAddr, scheme: &str, tls: &str) -> String {
    format!(
        "    endpoint: {scheme}://{addr}\n{tls}    timeout: 5s\n    retry_on_failure:\n      enabled: false\n    sending_queue:\n      enabled: false\n"
    )
}

fn otlp_config(pki_listener_cfg: impl FnOnce(&mut Config)) -> Config {
    let mut config = Config::default();
    config.server.bind_address = free_addr().to_string();
    config.server.auth.mode = "none".to_string();
    config.otlp.enabled = true;
    config.otlp.auth.mode = "none".to_string();
    pki_listener_cfg(&mut config);
    config
}

#[tokio::test]
async fn otel_collector_exports_to_every_otlp_listener() {
    skip_if_no_docker!();
    let test = test_name!();
    let pki = Pki::generate().await;
    let sink = Sink::start().await;

    let mut receivers = Vec::new();
    let mut addrs = Vec::new();
    for tls in [Some("none"), Some("required"), None] {
        let (receiver, pair) = Receiver::start(&sink, || {
            let (grpc_addr, http_addr) = (free_addr(), free_addr());
            let config = otlp_config(|config| {
                config.otlp.grpc_bind_address = grpc_addr.to_string();
                config.otlp.http_bind_address = http_addr.to_string();
                if let Some(mode) = tls {
                    config.otlp.tls = pki.server_tls(mode);
                }
            });
            (config, vec![grpc_addr, http_addr], (grpc_addr, http_addr))
        })
        .await
        .unwrap_or_else(|e| panic!("{e}"));
        receivers.push(receiver);
        addrs.push(pair);
    }
    let [
        (tls_grpc, tls_http),
        (mtls_grpc, mtls_http),
        (plain_grpc, plain_http),
    ] = addrs[..]
    else {
        unreachable!("three receivers were started")
    };

    let both: &'static [&'static str] = &["logs", "metrics"];
    let cases = vec![
        Case {
            id: "otelcase-grpc-tls",
            arrives: true,
            exporter: "otlp/grpc-tls",
            body: grpc(tls_grpc, &tls_block("ca", None)),
            signals: both,
        },
        Case {
            id: "otelcase-http-tls",
            arrives: true,
            exporter: "otlphttp/http-tls",
            body: http(tls_http, "https", &tls_block("ca", None)),
            signals: both,
        },
        Case {
            id: "otelcase-grpc-mtls",
            arrives: true,
            exporter: "otlp/grpc-mtls",
            body: grpc(mtls_grpc, &tls_block("ca", Some("client"))),
            signals: both,
        },
        Case {
            id: "otelcase-http-mtls",
            arrives: true,
            exporter: "otlphttp/http-mtls",
            body: http(mtls_http, "https", &tls_block("ca", Some("client"))),
            signals: both,
        },
        Case {
            id: "otelcase-grpc-plain",
            arrives: true,
            exporter: "otlp/grpc-plain",
            body: grpc(plain_grpc, "    tls:\n      insecure: true\n"),
            signals: both,
        },
        Case {
            id: "otelcase-http-plain",
            arrives: true,
            exporter: "otlphttp/http-plain",
            body: http(plain_http, "http", ""),
            signals: both,
        },
        Case {
            id: "otelcase-grpc-wrong-ca",
            arrives: false,
            exporter: "otlp/grpc-wrong-ca",
            body: grpc(tls_grpc, &tls_block("rogue-ca", None)),
            signals: &["logs"],
        },
        Case {
            id: "otelcase-http-wrong-ca",
            arrives: false,
            exporter: "otlphttp/http-wrong-ca",
            body: http(tls_http, "https", &tls_block("rogue-ca", None)),
            signals: &["logs"],
        },
        Case {
            id: "otelcase-grpc-no-client-cert",
            arrives: false,
            exporter: "otlp/grpc-no-client-cert",
            body: grpc(mtls_grpc, &tls_block("ca", None)),
            signals: &["logs"],
        },
        Case {
            id: "otelcase-http-no-client-cert",
            arrives: false,
            exporter: "otlphttp/http-no-client-cert",
            body: http(mtls_http, "https", &tls_block("ca", None)),
            signals: &["logs"],
        },
        Case {
            id: "otelcase-grpc-plaintext-to-tls",
            arrives: false,
            exporter: "otlp/grpc-plaintext-to-tls",
            body: grpc(tls_grpc, "    tls:\n      insecure: true\n"),
            signals: &["logs"],
        },
        Case {
            id: "otelcase-http-plaintext-to-tls",
            arrives: false,
            exporter: "otlphttp/http-plaintext-to-tls",
            body: http(tls_http, "http", ""),
            signals: &["logs"],
        },
    ];

    let mut exporters = String::new();
    let mut processors = String::new();
    let mut pipelines = String::new();
    for case in &cases {
        exporters.push_str(&format!("  {}:\n{}", case.exporter, case.body));
        processors.push_str(&format!(
            "  resource/{id}:\n    attributes:\n      - key: case\n        value: {id}\n        action: upsert\n",
            id = case.id
        ));
        for signal in case.signals {
            let receiver = if *signal == "logs" {
                "filelog"
            } else {
                "hostmetrics"
            };
            pipelines.push_str(&format!(
                "    {signal}/{id}:\n      receivers: [{receiver}]\n      processors: [resource/{id}]\n      exporters: [{exporter}]\n",
                id = case.id,
                exporter = case.exporter
            ));
        }
    }
    let yaml = format!(
        "receivers:\n  filelog:\n    include: [{PEER_DIR}/app.log]\n    start_at: beginning\n  hostmetrics:\n    collection_interval: 1s\n    scrapers:\n      cpu: {{}}\n      memory: {{}}\n\nprocessors:\n{processors}\nexporters:\n{exporters}\nservice:\n  telemetry:\n    metrics:\n      level: none\n  pipelines:\n{pipelines}"
    );

    let files = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(pki.dir()).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), files.path().join(entry.file_name())).unwrap();
    }
    std::fs::write(files.path().join("collector.yaml"), &yaml).unwrap();
    let mut lines = String::new();
    for i in 0..5 {
        writeln!(lines, "otel collector log line {i}").unwrap();
    }
    std::fs::write(files.path().join("app.log"), lines).unwrap();

    let peer = Peer::start(PeerSpec {
        test,
        service: "otel-collector",
        image: OTEL_COLLECTOR_IMAGE,
        entrypoint: None,
        args: vec!["--config".to_string(), format!("{PEER_DIR}/collector.yaml")],
        files: Some(files.path()),
        user: None,
        env: vec![],
        extra_files: vec![],
    })
    .await
    .unwrap_or_else(|e| panic!("the OpenTelemetry Collector peer did not start: {e}"));

    let mut results = Cases::default();
    let deadline = Instant::now() + Duration::from_secs(60);
    for case in cases.iter().filter(|c| c.arrives) {
        for signal in case.signals {
            // The case is a resource attribute on both signals. The second marker names the signal.
            let kind = if *signal == "logs" {
                "otel collector log line"
            } else {
                "system.cpu"
            };
            let present = sink
                .wait_for_all(
                    &[case.id, kind],
                    deadline.saturating_duration_since(Instant::now()),
                )
                .await;
            results.check(
                &format!("{} ({signal}): records arrive", case.id),
                present,
                || format!("no record carries both {} and {kind}", case.id),
            );
        }
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let logs = peer.logs().await;
    for case in cases.iter().filter(|c| !c.arrives) {
        results.check(
            &format!("{}: nothing is delivered", case.id),
            sink.count(case.id) == 0,
            || sink.find(case.id).unwrap_or_default(),
        );
    }
    for case in cases.iter().filter(|c| !c.arrives) {
        // Without this an exporter the collector never ran would pass as a refusal.
        results.check(
            &format!("{}: the collector tried and logged the failure", case.id),
            logs.lines()
                .any(|l| l.contains(case.exporter) && (l.contains("error") || l.contains("warn"))),
            || logs.clone(),
        );
    }
    for (index, receiver) in receivers.iter_mut().enumerate() {
        results.check(
            &format!("receiver {index} is still running"),
            receiver.alive(),
            || receiver.log_text(),
        );
    }
    results.finish(|| {
        format!(
            "collector log:\n{logs}\n\nreceiver 0 log:\n{}\n\n{}",
            receivers[0].log_text(),
            sink.dump()
        )
    });
}
