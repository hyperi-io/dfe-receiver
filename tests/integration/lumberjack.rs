// Project:   dfe-receiver
// File:      tests/integration/lumberjack.rs
// Purpose:   Filebeat in Docker shipping Lumberjack v2 to the receiver binary, with and without TLS
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One Filebeat container runs one Filebeat process per case, each with its own
//! output, against three receiver processes: mutual TLS, TLS and plaintext.
//!
//! Filebeat has a single output, so a case is a process. They share the
//! container and the log file they read, and each stamps its case on its events.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::fmt::Write;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use dfe_receiver::config::Config;

use crate::common::peers::{
    Cases, FILEBEAT_IMAGE, PEER_DIR, Peer, PeerSpec, Pki, Receiver, Sink, free_addr,
};
use crate::{skip_if_no_docker, test_name};

/// Events each Filebeat process sends.
const EVENTS: usize = 20;

struct Case {
    id: &'static str,
    arrives: bool,
    addr: SocketAddr,
    /// `Some((ca, client))` for TLS, `None` for plaintext.
    tls: Option<(&'static str, Option<&'static str>)>,
}

fn filebeat_config(index: usize, case: &Case) -> String {
    let ssl = case.tls.map_or_else(String::new, |(ca, client)| {
        let mut text = format!(
            "  ssl.enabled: true\n  ssl.verification_mode: full\n  ssl.certificate_authorities: [\"{PEER_DIR}/{ca}.crt\"]\n"
        );
        if let Some(client) = client {
            text.push_str(&format!(
                "  ssl.certificate: \"{PEER_DIR}/{client}.crt\"\n  ssl.key: \"{PEER_DIR}/{client}.key\"\n"
            ));
        }
        text
    });
    format!(
        r#"filebeat.inputs:
  - type: filestream
    id: case-{index}
    paths: ["{PEER_DIR}/app.log"]
    prospector:
      scanner:
        fingerprint:
          enabled: false
    file_identity.native: ~
    parsers:
      - ndjson:
          keys_under_root: true

fields:
  case: "{id}"
fields_under_root: true

output.logstash:
  hosts: ["{addr}"]
  timeout: 5s
  max_retries: 2
{ssl}
path.data: "/tmp/fb-{index}"
path.logs: "/tmp/fb-{index}"
setup.template.enabled: false
setup.ilm.enabled: false
logging.level: info
"#,
        id = case.id,
        addr = case.addr,
    )
}

async fn start(config: Config, sink: &Sink, addr: SocketAddr) -> Receiver {
    Receiver::start(config, sink, &[addr])
        .await
        .unwrap_or_else(|e| panic!("{e}"))
}

#[tokio::test]
async fn filebeat_ships_lumberjack_over_tls_and_plaintext() {
    skip_if_no_docker!();
    let test = test_name!();
    let pki = Pki::generate().await;
    let sink = Sink::start().await;

    let config_for = |client_auth: Option<&str>| -> (Config, SocketAddr) {
        let addr = free_addr();
        let mut config = Config::default();
        config.server.bind_address = free_addr().to_string();
        config.server.auth.mode = "none".to_string();
        config.lumberjack.enabled = true;
        config.lumberjack.bind_address = addr.to_string();
        match client_auth {
            Some("required") => config.lumberjack.tls = pki.server_tls("required"),
            Some(mode) => {
                config.lumberjack.tls = pki.server_tls(mode);
                config.lumberjack.accept_unauthenticated = true;
            }
            None => config.lumberjack.accept_unauthenticated = true,
        }
        (config, addr)
    };
    let (mtls_config, mtls) = config_for(Some("required"));
    let (tls_config, tls) = config_for(Some("none"));
    let (plain_config, plain) = config_for(None);
    let mut receivers = Vec::new();
    receivers.push(start(mtls_config, &sink, mtls).await);
    receivers.push(start(tls_config, &sink, tls).await);
    receivers.push(start(plain_config, &sink, plain).await);

    let cases = [
        Case {
            id: "fbeatcase-mtls",
            arrives: true,
            addr: mtls,
            tls: Some(("ca", Some("client"))),
        },
        Case {
            id: "fbeatcase-tls",
            arrives: true,
            addr: tls,
            tls: Some(("ca", None)),
        },
        Case {
            id: "fbeatcase-plain",
            arrives: true,
            addr: plain,
            tls: None,
        },
        Case {
            id: "fbeatcase-mtls-wrong-ca",
            arrives: false,
            addr: mtls,
            tls: Some(("rogue-ca", Some("client"))),
        },
        Case {
            id: "fbeatcase-mtls-no-client-cert",
            arrives: false,
            addr: mtls,
            tls: Some(("ca", None)),
        },
        Case {
            id: "fbeatcase-mtls-rogue-client-cert",
            arrives: false,
            addr: mtls,
            tls: Some(("ca", Some("rogue-client"))),
        },
        Case {
            id: "fbeatcase-tls-wrong-ca",
            arrives: false,
            addr: tls,
            tls: Some(("rogue-ca", None)),
        },
        Case {
            id: "fbeatcase-plaintext-to-tls",
            arrives: false,
            addr: tls,
            tls: None,
        },
    ];

    let files = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(pki.dir()).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), files.path().join(entry.file_name())).unwrap();
    }
    let mut lines = String::new();
    for i in 0..EVENTS {
        writeln!(
            lines,
            "{{\"message\":\"filebeat test event {i}\",\"sequence\":{i}}}"
        )
        .unwrap();
    }
    std::fs::write(files.path().join("app.log"), lines).unwrap();
    for (index, case) in cases.iter().enumerate() {
        std::fs::write(
            files.path().join(format!("fb-{index}.yml")),
            filebeat_config(index, case),
        )
        .unwrap();
    }

    let peer = Peer::start(PeerSpec {
        test,
        service: "filebeat",
        image: FILEBEAT_IMAGE,
        entrypoint: Some("sleep"),
        args: vec!["3600".to_string()],
        files: Some(files.path()),
        user: Some("0:0"),
        env: vec![],
        extra_files: vec![],
    })
    .await
    .unwrap_or_else(|e| panic!("the Filebeat peer did not start: {e}"));
    for index in 0..cases.len() {
        let command = format!(
            "filebeat -e --strict.perms=false -c {PEER_DIR}/fb-{index}.yml > /tmp/fb-{index}.log 2>&1"
        );
        peer.exec_detached(&["sh", "-c", &command]).await;
    }

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
    for case in cases.iter().filter(|c| !c.arrives) {
        results.check(
            &format!("{}: nothing is delivered", case.id),
            sink.count(case.id) == 0,
            || sink.find(case.id).unwrap_or_default(),
        );
    }
    let mut filebeat_logs = String::new();
    for (index, case) in cases.iter().enumerate() {
        let log = peer
            .exec(&["cat", &format!("/tmp/fb-{index}.log")])
            .await
            .output;
        if !case.arrives {
            results.check(
                &format!("{}: filebeat logged the failed connection", case.id),
                log.contains("ERROR") || log.contains("error"),
                || log.clone(),
            );
        }
        filebeat_logs.push_str(&format!("--- {} ---\n{log}\n", case.id));
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
            "filebeat logs:\n{filebeat_logs}\nreceiver 0 log:\n{}\n\n{}",
            receivers[0].log_text(),
            sink.dump()
        )
    });
}
