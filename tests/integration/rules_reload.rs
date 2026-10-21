// Project:   dfe-receiver
// File:      tests/integration/rules_reload.rs
// Purpose:   Rule edits and reorders in the config file route the next record, no restart
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The config file is rewritten under a running receiver, and the next record
//! POSTed to `/ingest` is routed by the rows the file now holds.
//!
//! The reload is the one the binary runs ([`Orchestrator::watch_config`]): it
//! polls the file, validates the new config and rebuilds the routes.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::Config;
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::Orchestrator;
use dfe_receiver::server::http::HttpHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use scalo::transport::TransportReceiver;
use scalo::transport::grpc::GrpcTransport;
use tokio_util::sync::CancellationToken;

/// The longest a file edit may take to route records: a few 5 s polls, then the rebuild.
const RELOAD_WITHIN: Duration = Duration::from_secs(30);

/// How long an edit waits for a poll to see it before it is written again.
const REWRITE_AFTER: Duration = Duration::from_secs(6);

/// The receiver config, its rows given as YAML list items.
fn config_yaml(loader: &str, rows: &str) -> String {
    format!(
        r#"server:
  bind_address: "127.0.0.1:0"
  auth:
    mode: none
destinations:
  default: loader
loader:
  transport: grpc
  grpc_endpoint: "{loader}"
routing:
  dlq:
    enabled: false
rules:
  rules:
{rows}
"#
    )
}

const BY_APP: &str = "    - id: by-app\n      match: { field: app, op: equals, value: pay }\n      set: { _source: { value: payments } }\n";
const BY_HOST: &str = "    - id: by-host\n      match: { field: host, op: equals, value: h1 }\n      set: { _source: { value: hosts } }\n";
const BY_HOST_EDITED: &str = "    - id: by-host\n      match: { field: host, op: equals, value: h1 }\n      set: { _source: { value: edited } }\n";
const BAD_ROW: &str = "    - id: \"bad id!\"\n      set: { _source: { value: refused } }\n";

/// Rewrite `path` whole, so the reloader sees one complete file.
fn write_config(path: &Path, yaml: &str) {
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(yaml.as_bytes()).unwrap();
    file.sync_all().unwrap();
}

/// POST one record, and return the `_source` the loader received it with.
async fn source_of_next(addr: SocketAddr, loader: &GrpcTransport) -> String {
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .header("content-type", "application/json")
        .body(r#"{"app":"pay","host":"h1"}"#)
        .send()
        .await
        .expect("POST /ingest");
    assert!(response.status().is_success(), "{}", response.status());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if let Ok(batch) = loader.recv(1).await
            && let Some(record) = batch.records.into_iter().next()
        {
            let record: serde_json::Value = serde_json::from_slice(&record.payload).unwrap();
            return record["_source"].as_str().unwrap_or_default().to_string();
        }
    }
    panic!("the loader received nothing");
}

/// Write `yaml` to `path`, then POST records until one routes to `want`, or
/// [`RELOAD_WITHIN`] passes. The `_source` of every record is returned.
///
/// The reloader reads its baseline mtime once its task runs, so an edit that
/// landed first is taken as the baseline: the edit is written again after
/// each [`REWRITE_AFTER`] that does not route.
async fn routes_to(
    addr: SocketAddr,
    loader: &GrpcTransport,
    path: &Path,
    yaml: &str,
    want: &str,
) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + RELOAD_WITHIN;
    let mut rewrite_at = tokio::time::Instant::now();
    let mut seen = Vec::new();
    while tokio::time::Instant::now() < deadline {
        if tokio::time::Instant::now() >= rewrite_at {
            write_config(path, yaml);
            rewrite_at = tokio::time::Instant::now() + REWRITE_AFTER;
        }
        let source = source_of_next(addr, loader).await;
        let done = source == want;
        seen.push(source);
        if done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rule_edit_and_reorder_route_the_next_record_with_no_restart() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.yaml");
    write_config(
        &path,
        &config_yaml(&loader_endpoint, &format!("{BY_APP}{BY_HOST}")),
    );
    let path_str = path.display().to_string();
    let config = Config::load(Some(&path_str)).expect("the first config loads");

    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let orchestrator = Orchestrator::new(config.clone(), metrics.clone(), shutdown.clone())
        .await
        .expect("pipeline init");
    orchestrator.watch_config(Some(&path_str), 0);
    let handler = HttpHandler::new(
        config.server.bind_address.clone(),
        orchestrator.state(),
        metrics,
    );
    let bound = handler.bound_addr();
    let server_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(server_shutdown).await });
    let addr = crate::common::bound_addr("HTTP", &bound, &mut task).await;

    assert_eq!(source_of_next(addr, &loader).await, "payments");

    let reordered = config_yaml(&loader_endpoint, &format!("{BY_HOST}{BY_APP}"));
    let seen = routes_to(addr, &loader, &path, &reordered, "hosts").await;
    assert_eq!(
        seen.last().map(String::as_str),
        Some("hosts"),
        "the reorder never routed: {seen:?}"
    );

    let edited = config_yaml(&loader_endpoint, &format!("{BY_HOST_EDITED}{BY_APP}"));
    let seen = routes_to(addr, &loader, &path, &edited, "edited").await;
    assert_eq!(
        seen.last().map(String::as_str),
        Some("edited"),
        "the edit never routed: {seen:?}"
    );

    // Rows that do not compile are refused by the reload's validate step.
    let version = orchestrator.shared_config().version();
    write_config(
        &path,
        &config_yaml(&loader_endpoint, &format!("{BAD_ROW}{BY_APP}")),
    );
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert_eq!(
        orchestrator.shared_config().version(),
        version,
        "the bad rows were applied"
    );
    assert_eq!(source_of_next(addr, &loader).await, "edited");

    shutdown.cancel();
}
