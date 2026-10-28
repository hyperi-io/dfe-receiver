// Project:   dfe-receiver
// File:      tests/integration/logstash.rs
// Purpose:   Logstash in Docker shipping to the receiver binary over TLS with its lumberjack and http outputs
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One Logstash container, one pipeline per case, against two receiver
//! processes: HTTP ingest with mutual TLS, and HTTP ingest plus Lumberjack with
//! TLS. A JVM takes tens of seconds to start, so this is the slowest peer.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use dfe_receiver::config::Config;

use crate::common::peers::{
    Cases, LOGSTASH_IMAGE, PEER_DIR, Peer, PeerSpec, Pki, Receiver, Sink, free_addr,
};
use crate::{skip_if_no_docker, test_name};

/// Events each pipeline sends.
const EVENTS: usize = 5;

struct Case {
    id: &'static str,
    arrives: bool,
    output: String,
}

impl Case {
    /// The Lumberjack output connects while it registers, so it never finishes starting against a CA it refuses.
    fn starts(&self) -> bool {
        self.id != "logstashcase-lumberjack-wrong-ca"
    }
}

/// What Logstash's Lumberjack output sends: the receiver speaks Lumberjack v2 only.
const LUMBERJACK_V1_REFUSED: &str = "unsupported protocol version 0x31";

fn http_output(addr: SocketAddr, ca: &str, client: Option<&str>) -> String {
    let client = client.map_or_else(String::new, |c| {
        format!(" ssl_certificate => \"{PEER_DIR}/{c}.crt\" ssl_key => \"{PEER_DIR}/{c}.key\"")
    });
    format!(
        "http {{ url => \"https://{addr}/ingest\" http_method => \"post\" format => \"json\" automatic_retries => 1 ssl_certificate_authorities => [\"{PEER_DIR}/{ca}.crt\"]{client} }}"
    )
}

fn lumberjack_output(addr: SocketAddr, ca: &str) -> String {
    format!(
        "lumberjack {{ hosts => [\"{}\"] port => {} ssl_certificate => \"{PEER_DIR}/{ca}.crt\" }}",
        addr.ip(),
        addr.port()
    )
}

#[tokio::test]
async fn logstash_ships_over_tls_with_http_and_lumberjack_outputs() {
    skip_if_no_docker!();
    let test = test_name!();
    let pki = Pki::generate().await;
    let sink = Sink::start().await;

    let (mut mtls_receiver, mtls_http) = Receiver::start(&sink, || {
        let mtls_http = free_addr();
        let mut mtls_config = Config::default();
        mtls_config.server.bind_address = mtls_http.to_string();
        mtls_config.server.auth.mode = "none".to_string();
        mtls_config.server.tls = pki.server_tls("required");
        (mtls_config, vec![mtls_http], mtls_http)
    })
    .await
    .unwrap_or_else(|e| panic!("{e}"));

    let (mut tls_receiver, (tls_http, tls_lumberjack)) = Receiver::start(&sink, || {
        let (tls_http, tls_lumberjack) = (free_addr(), free_addr());
        let mut tls_config = Config::default();
        tls_config.server.bind_address = tls_http.to_string();
        tls_config.server.auth.mode = "none".to_string();
        tls_config.server.tls = pki.server_tls("none");
        tls_config.lumberjack.enabled = true;
        tls_config.lumberjack.bind_address = tls_lumberjack.to_string();
        tls_config.lumberjack.accept_unauthenticated = true;
        tls_config.lumberjack.tls = pki.server_tls("none");
        (
            tls_config,
            vec![tls_http, tls_lumberjack],
            (tls_http, tls_lumberjack),
        )
    })
    .await
    .unwrap_or_else(|e| panic!("{e}"));

    let cases = vec![
        Case {
            id: "logstashcase-http-mtls",
            arrives: true,
            output: http_output(mtls_http, "ca", Some("client")),
        },
        Case {
            id: "logstashcase-http-tls",
            arrives: true,
            output: http_output(tls_http, "ca", None),
        },
        Case {
            id: "logstashcase-lumberjack-tls",
            arrives: false,
            output: lumberjack_output(tls_lumberjack, "ca"),
        },
        Case {
            id: "logstashcase-http-mtls-no-client-cert",
            arrives: false,
            output: http_output(mtls_http, "ca", None),
        },
        Case {
            id: "logstashcase-http-mtls-rogue-client-cert",
            arrives: false,
            output: http_output(mtls_http, "ca", Some("rogue-client")),
        },
        Case {
            id: "logstashcase-http-wrong-ca",
            arrives: false,
            output: http_output(tls_http, "rogue-ca", None),
        },
        Case {
            id: "logstashcase-lumberjack-wrong-ca",
            arrives: false,
            output: lumberjack_output(tls_lumberjack, "rogue-ca"),
        },
    ];

    let mut pipelines = String::new();
    for case in &cases {
        pipelines.push_str(&format!(
            "- pipeline.id: {id}\n  pipeline.workers: 1\n  config.string: |\n    input {{ generator {{ count => {EVENTS} message => \"{id} hello\" }} }}\n    filter {{ mutate {{ add_field => {{ \"case\" => \"{id}\" }} }} }}\n    output {{ {output} }}\n",
            id = case.id,
            output = case.output
        ));
    }
    let files = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(pki.dir()).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), files.path().join(entry.file_name())).unwrap();
    }
    let pipelines_yml = files.path().join("pipelines.yml");
    std::fs::write(&pipelines_yml, pipelines).unwrap();
    let settings_yml = files.path().join("logstash.yml");
    std::fs::write(&settings_yml, "api.enabled: false\nlog.level: info\n").unwrap();

    let peer = Peer::start(PeerSpec {
        test,
        service: "logstash",
        image: LOGSTASH_IMAGE,
        entrypoint: None,
        args: vec![],
        files: Some(files.path()),
        user: None,
        env: vec![("LS_JAVA_OPTS", "-Xms256m -Xmx512m")],
        extra_files: vec![
            (
                PathBuf::from(&pipelines_yml),
                "/usr/share/logstash/config/pipelines.yml".to_string(),
            ),
            (
                PathBuf::from(&settings_yml),
                "/usr/share/logstash/config/logstash.yml".to_string(),
            ),
        ],
    })
    .await
    .unwrap_or_else(|e| panic!("the Logstash peer did not start: {e}"));

    let started = Instant::now();
    for case in cases.iter().filter(|c| c.starts()) {
        peer.wait_for_log(
            &format!("Pipeline started {{\"pipeline.id\" => \"{}\"}}", case.id),
            Duration::from_secs(240),
        )
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    }
    let startup = started.elapsed();
    eprintln!("logstash started its pipelines in {startup:?}");

    let mut results = Cases::default();
    let deadline = Instant::now() + Duration::from_secs(120);
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
        // Without this a pipeline Logstash never ran would pass as a refusal.
        results.check(
            &format!("{}: logstash tried and logged the failure", case.id),
            logs.lines()
                .any(|l| l.contains(case.id) && l.contains("[ERROR]")),
            || logs.clone(),
        );
    }
    // The TLS handshake with Logstash's Lumberjack client completes (the wrong-CA pipeline does not
    // get that far), then the receiver refuses the client's version 1 frames.
    results.check(
        "logstashcase-lumberjack-tls: the handshake completes and Lumberjack v1 is refused",
        tls_receiver.log_text().contains(LUMBERJACK_V1_REFUSED),
        || tls_receiver.log_text(),
    );
    results.check(
        "logstashcase-lumberjack-wrong-ca: the client refuses the certificate",
        logs.contains("certificate verify failed"),
        || logs.clone(),
    );
    results.check(
        "the mTLS receiver is still running",
        mtls_receiver.alive(),
        || mtls_receiver.log_text(),
    );
    results.check(
        "the TLS receiver is still running",
        tls_receiver.alive(),
        || tls_receiver.log_text(),
    );
    results.finish(|| {
        format!(
            "logstash log:\n{logs}\n\nTLS receiver log:\n{}\n\n{}",
            tls_receiver.log_text(),
            sink.dump()
        )
    });
}
