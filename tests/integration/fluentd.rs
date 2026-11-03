// Project:   dfe-receiver
// File:      tests/integration/fluentd.rs
// Purpose:   Fluentd in Docker shipping to the Forward listener through out_forward
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One Fluentd container tails one file per case and forwards it to a
//! plaintext receiver process with `out_forward`: on its defaults, with
//! `require_ack_response true`, and with `compress gzip`.
//!
//! Fluentd packs a chunk's entries as a MessagePack str (`0xdb`), not bin, and
//! resends a chunk whose ack does not come within `ack_response_timeout`. So a
//! case passes when every event arrives exactly once, and still has after the
//! ack timeout has run out: a missing ack would show as a resend.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::time::{Duration, Instant};

use dfe_receiver::config::Config;

use crate::common::peers::{
    Cases, FLUENTD_IMAGE, PEER_DIR, Peer, PeerSpec, Receiver, Sink, free_addr,
};
use crate::{skip_if_no_docker, test_name};

/// Events each case sends.
const EVENTS: usize = 10;

/// How long Fluentd waits for an ack before it resends, in the ack case.
const ACK_TIMEOUT_SECS: u64 = 5;

/// One `out_forward` configuration.
struct Case {
    id: &'static str,
    /// Extra `out_forward` parameters.
    forward: &'static str,
}

const CASES: [Case; 3] = [
    Case {
        id: "fdcase-defaults",
        forward: "",
    },
    Case {
        id: "fdcase-ack",
        forward: "require_ack_response true\n  ack_response_timeout 5",
    },
    Case {
        id: "fdcase-gzip",
        forward: "compress gzip",
    },
];

/// The line of event `n` of case `id`. The closing quote of its JSON string keeps
/// event 1 from matching event 10.
fn marker(id: &str, n: usize) -> String {
    format!("{id} event {n}\"")
}

#[tokio::test]
async fn fluentd_forwards_every_event_exactly_once() {
    skip_if_no_docker!();
    let test = test_name!();
    let sink = Sink::start().await;

    let (mut receiver, forward) = Receiver::start(&sink, || {
        let http = free_addr();
        let forward = free_addr();
        let mut config = Config::default();
        config.server.bind_address = http.to_string();
        config.server.auth.mode = "none".to_string();
        config.fluent.enabled = true;
        config.fluent.bind_address = forward.to_string();
        config.fluent.accept_unauthenticated = true;
        (config, vec![http, forward], forward)
    })
    .await
    .unwrap_or_else(|e| panic!("{e}"));

    let files = tempfile::tempdir().unwrap();
    let mut conf = String::from("<system>\n  log_level info\n</system>\n");
    for case in &CASES {
        let mut lines = String::new();
        for n in 0..EVENTS {
            lines.push_str(marker(case.id, n).trim_end_matches('"'));
            lines.push('\n');
        }
        std::fs::write(files.path().join(format!("{}.log", case.id)), lines).unwrap();
        conf.push_str(&format!(
            "<source>\n  @type tail\n  path {PEER_DIR}/{id}.log\n  pos_file /tmp/{id}.pos\n  read_from_head true\n  encoding UTF-8\n  tag {id}\n  <parse>\n    @type none\n  </parse>\n</source>\n\
             <match {id}>\n  @type forward\n  {extra}\n  <server>\n    host {ip}\n    port {port}\n  </server>\n  <buffer>\n    flush_mode interval\n    flush_interval 1s\n  </buffer>\n</match>\n",
            id = case.id,
            extra = case.forward,
            ip = forward.ip(),
            port = forward.port(),
        ));
    }
    std::fs::write(files.path().join("fluent.conf"), conf).unwrap();

    let peer = Peer::start(PeerSpec {
        test,
        service: "fluentd",
        image: FLUENTD_IMAGE,
        entrypoint: None,
        args: vec![
            "fluentd".to_string(),
            "-c".to_string(),
            format!("{PEER_DIR}/fluent.conf"),
        ],
        files: Some(files.path()),
        user: Some("0:0"),
        env: vec![],
        extra_files: vec![],
    })
    .await
    .unwrap_or_else(|e| panic!("the Fluentd peer did not start: {e}"));

    let mut results = Cases::default();
    let deadline = Instant::now() + Duration::from_secs(120);
    for case in &CASES {
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

    // Past the ack timeout a chunk that was not acked has been sent again.
    tokio::time::sleep(Duration::from_secs(ACK_TIMEOUT_SECS + 5)).await;
    let logs = peer.logs().await;
    for case in &CASES {
        results.check(
            &format!("{}: {EVENTS} events, not more", case.id),
            sink.count(case.id) == EVENTS,
            || format!("{} arrived", sink.count(case.id)),
        );
        let each_once = (0..EVENTS).all(|n| sink.count(&marker(case.id, n)) == 1);
        results.check(
            &format!("{}: every event arrives once", case.id),
            each_once,
            || sink.dump(),
        );
        let record = sink.find(case.id).unwrap_or_default();
        let value: serde_json::Value = serde_json::from_str(&record).unwrap_or_default();
        results.check(
            &format!("{}: the tag and message are on the record", case.id),
            value["tag"] == case.id && value["message"].is_string(),
            || record.clone(),
        );
    }
    results.check(
        "fluentd logged no failed flush",
        !logs.contains("failed to flush the buffer") && !logs.contains("no ack"),
        || logs.clone(),
    );
    results.check("the receiver is still running", receiver.alive(), || {
        receiver.log_text()
    });
    results.finish(|| {
        format!(
            "fluentd log:\n{logs}\n\nreceiver log:\n{}",
            receiver.log_text()
        )
    });
}
