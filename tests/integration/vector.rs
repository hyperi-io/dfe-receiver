// Project:   dfe-receiver
// File:      tests/integration/vector.rs
// Purpose:   Vector in Docker shipping to the receiver binary over HTTP and gRPC, with and without TLS
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One Vector container, one sink per case, fed by a single `demo_logs` source.
//! Two receiver processes: one with TLS (HTTPS ingest, mutual-TLS gRPC) and one
//! in plaintext (HTTP ingest behind a bearer token, plain gRPC). Vector is a
//! real peer for the `vector` gRPC protocol the receiver speaks, so it is named.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use dfe_receiver::config::Config;

use crate::common::peers::{
    Cases, PEER_DIR, Peer, PeerSpec, Pki, Receiver, Sink, VECTOR_IMAGE, free_addr,
};
use crate::{skip_if_no_docker, test_name};

/// Events the source emits, and each sink must deliver.
const EVENTS: usize = 10;

/// The bearer token the plaintext receiver accepts.
const TOKEN: &str = "vector-test-token-42";

struct Case {
    id: &'static str,
    arrives: bool,
    /// The sink's YAML after its `inputs` line.
    sink: String,
}

/// The `vector` sink needs `enabled`, and the `http` sink has no such key.
fn tls_block(ca: &str, client: Option<&str>, enabled_key: bool) -> String {
    let enabled = if enabled_key {
        "      enabled: true\n"
    } else {
        ""
    };
    let mut text = format!("    tls:\n{enabled}      ca_file: {PEER_DIR}/{ca}.crt\n");
    if let Some(client) = client {
        text.push_str(&format!(
            "      crt_file: {PEER_DIR}/{client}.crt\n      key_file: {PEER_DIR}/{client}.key\n"
        ));
    }
    text
}

fn http_sink(scheme: &str, addr: SocketAddr, tls: &str, token: bool) -> String {
    let headers = if token {
        format!("    request:\n      headers:\n        Authorization: \"Bearer {TOKEN}\"\n")
    } else {
        String::new()
    };
    format!(
        "    type: http\n    uri: \"{scheme}://{addr}/ingest\"\n    encoding:\n      codec: json\n    method: post\n    batch:\n      max_events: 1\n      timeout_secs: 1\n{tls}{headers}    healthcheck:\n      enabled: false\n"
    )
}

fn grpc_sink(addr: SocketAddr, tls: &str) -> String {
    format!(
        "    type: vector\n    address: \"{addr}\"\n{tls}    healthcheck:\n      enabled: false\n"
    )
}

#[tokio::test]
async fn vector_ships_over_http_and_grpc_with_and_without_tls() {
    skip_if_no_docker!();
    let test = test_name!();
    let pki = Pki::generate().await;
    let sink = Sink::start().await;

    let (tls_http, tls_grpc) = (free_addr(), free_addr());
    let mut tls_config = Config::default();
    tls_config.server.bind_address = tls_http.to_string();
    tls_config.server.auth.mode = "none".to_string();
    tls_config.server.tls = pki.server_tls("none");
    tls_config.grpc.enabled = true;
    tls_config.grpc.bind_address = tls_grpc.to_string();
    tls_config.grpc.tls = pki.server_tls("required");
    let mut tls_receiver = Receiver::start(tls_config, &sink, &[tls_http, tls_grpc])
        .await
        .unwrap_or_else(|e| panic!("{e}"));

    let token_dir = tempfile::tempdir().unwrap();
    let token_file = token_dir.path().join("token");
    std::fs::write(&token_file, TOKEN).unwrap();
    let (plain_http, plain_grpc) = (free_addr(), free_addr());
    let mut plain_config = Config::default();
    plain_config.server.bind_address = plain_http.to_string();
    plain_config.server.auth.mode = "bearer".to_string();
    plain_config.server.auth.bearer.secret_source = Some(format!("file:{}", token_file.display()));
    plain_config.grpc.enabled = true;
    plain_config.grpc.bind_address = plain_grpc.to_string();
    let mut plain_receiver = Receiver::start(plain_config, &sink, &[plain_http, plain_grpc])
        .await
        .unwrap_or_else(|e| panic!("{e}"));

    let cases = vec![
        Case {
            id: "vectorcase-http-plain-bearer",
            arrives: true,
            sink: http_sink("http", plain_http, "", true),
        },
        Case {
            id: "vectorcase-http-plain-no-token",
            arrives: false,
            sink: http_sink("http", plain_http, "", false),
        },
        Case {
            id: "vectorcase-https",
            arrives: true,
            sink: http_sink("https", tls_http, &tls_block("ca", None, false), false),
        },
        Case {
            id: "vectorcase-https-wrong-ca",
            arrives: false,
            sink: http_sink(
                "https",
                tls_http,
                &tls_block("rogue-ca", None, false),
                false,
            ),
        },
        Case {
            id: "vectorcase-http-plaintext-to-tls",
            arrives: false,
            sink: http_sink("http", tls_http, "", false),
        },
        Case {
            id: "vectorcase-grpc-plain",
            arrives: true,
            sink: grpc_sink(plain_grpc, ""),
        },
        Case {
            id: "vectorcase-grpc-mtls",
            arrives: true,
            sink: grpc_sink(tls_grpc, &tls_block("ca", Some("client"), true)),
        },
        Case {
            id: "vectorcase-grpc-wrong-ca",
            arrives: false,
            sink: grpc_sink(tls_grpc, &tls_block("rogue-ca", Some("client"), true)),
        },
        Case {
            id: "vectorcase-grpc-no-client-cert",
            arrives: false,
            sink: grpc_sink(tls_grpc, &tls_block("ca", None, true)),
        },
        Case {
            id: "vectorcase-grpc-rogue-client-cert",
            arrives: false,
            sink: grpc_sink(tls_grpc, &tls_block("ca", Some("rogue-client"), true)),
        },
    ];

    let mut transforms = String::new();
    let mut sinks = String::new();
    for case in &cases {
        transforms.push_str(&format!(
            "  tag_{id}:\n    type: remap\n    inputs: [demo]\n    source: '.case = \"{id}\"'\n",
            id = case.id
        ));
        sinks.push_str(&format!(
            "  {id}:\n    inputs: [tag_{id}]\n{body}",
            id = case.id,
            body = case.sink
        ));
    }
    let yaml = format!(
        "data_dir: /tmp/vector-data\n\nsources:\n  demo:\n    type: demo_logs\n    format: json\n    count: {EVENTS}\n    interval: 0.1\n\ntransforms:\n{transforms}\nsinks:\n{sinks}"
    );
    let files = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(pki.dir()).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), files.path().join(entry.file_name())).unwrap();
    }
    std::fs::write(files.path().join("vector.yaml"), &yaml).unwrap();

    let peer = Peer::start(PeerSpec {
        test,
        service: "vector",
        image: VECTOR_IMAGE,
        entrypoint: None,
        args: vec!["--config".to_string(), format!("{PEER_DIR}/vector.yaml")],
        files: Some(files.path()),
        user: None,
        env: vec![("VECTOR_LOG", "info")],
        extra_files: vec![],
    })
    .await
    .unwrap_or_else(|e| panic!("the Vector peer did not start: {e}"));

    let mut results = Cases::default();
    let deadline = Instant::now() + Duration::from_secs(90);
    for case in cases.iter().filter(|c| c.arrives) {
        let arrived = sink
            .wait_for_count(
                case.id,
                EVENTS,
                deadline.saturating_duration_since(Instant::now()),
            )
            .await;
        results.check(
            &format!("{}: {EVENTS} events arrive", case.id),
            arrived,
            || format!("only {} arrived", sink.count(case.id)),
        );
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let logs = peer.logs().await;
    for case in cases.iter().filter(|c| !c.arrives) {
        results.check(
            &format!("{}: nothing is delivered", case.id),
            sink.count(case.id) == 0,
            || sink.find(case.id).unwrap_or_default(),
        );
        // Without this a sink Vector never started would pass as a refusal.
        results.check(
            &format!("{}: vector tried and logged the failure", case.id),
            logs.lines()
                .any(|l| l.contains(case.id) && (l.contains("ERROR") || l.contains("WARN"))),
            || logs.clone(),
        );
    }
    results.check(
        "vector logged the refused deliveries",
        logs.contains("ERROR") || logs.contains("WARN"),
        || logs.clone(),
    );
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
            "vector log:\n{logs}\n\nTLS receiver log:\n{}\n\n{}",
            tls_receiver.log_text(),
            sink.dump()
        )
    });
}
