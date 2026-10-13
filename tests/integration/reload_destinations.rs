// Project:   dfe-receiver
// File:      tests/integration/reload_destinations.rs
// Purpose:   Destination senders built and retired by a config reload, in-process
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A config reload builds the sender for a destination it adds and retires the
//! sender of one it removes, with no restart.
//!
//! scalo Push listeners stand in for the destinations and a file DLQ for the
//! dead-letter store. The assertions are the artefacts: which listener received
//! the record, and what the DLQ file holds.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
// The config structs put the inline PipelineState future just over clippy's
// 16 KiB threshold, as in tests/integration/source_routing.rs.
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use dfe_receiver::config::{
    Config, DestinationRule, DestinationSpec, DlqConfig, GrpcDestination, SharedConfig,
};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::http::HttpHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use scalo::transport::grpc::GrpcTransport;
use scalo::transport::{AcknowledgementsConfig, TransportReceiver};
use tokio_util::sync::CancellationToken;

fn grpc_destination(endpoint: &str) -> DestinationSpec {
    DestinationSpec {
        grpc: Some(GrpcDestination {
            endpoint: endpoint.to_string(),
            ..GrpcDestination::default()
        }),
        kafka: None,
    }
}

/// Records whose `app` is `value` go to destination `name`.
fn app_rule(value: &str, name: &str) -> DestinationRule {
    DestinationRule {
        match_field: "app".to_string(),
        match_value: value.to_string(),
        destination: name.into(),
    }
}

/// A brokerless receiver sending everything to the loader at `loader`.
fn loader_only(loader: &str) -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    config.routing.dlq.enabled = false;
    config.destinations.default = "loader".into();
    config
        .destinations
        .named
        .insert("loader".to_string(), grpc_destination(loader));
    config
}

/// `base`, plus destination `name` at `endpoint` taking records whose `app` is
/// `value`.
fn routing_app(mut base: Config, value: &str, name: &str, endpoint: &str) -> Config {
    base.destinations
        .named
        .insert(name.to_string(), grpc_destination(endpoint));
    base.destinations.rules.push(app_rule(value, name));
    base
}

async fn pipeline_for(config: Config) -> Arc<PipelineState> {
    Arc::new(
        PipelineState::new(SharedConfig::new(config), CancellationToken::new())
            .await
            .expect("pipeline"),
    )
}

/// Start the receiver's HTTP listener over `pipeline`.
async fn start_http(pipeline: Arc<PipelineState>) -> (SocketAddr, CancellationToken) {
    let bind = pipeline.config().server.bind_address;
    let handler = HttpHandler::new(bind, pipeline, Arc::new(Metrics::default()));
    let bound = handler.bound_addr();
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(server_shutdown).await });
    let addr = crate::common::bound_addr("HTTP", &bound, &mut task).await;
    (addr, shutdown)
}

/// POST `body` to `/ingest` and return the status.
async fn post(addr: SocketAddr, body: &str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("POST /ingest")
        .status()
}

/// Collect up to `want` records, or give up after `within`.
async fn drain(server: &GrpcTransport, want: usize, within: Duration) -> Vec<Bytes> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + within;
    while out.len() < want && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = server.recv(want).await {
            out.extend(batch.records.into_iter().map(|r| r.payload));
        }
    }
    out
}

/// The `id` of each record.
fn ids(records: &[Bytes]) -> Vec<u64> {
    records
        .iter()
        .map(|r| {
            let value: serde_json::Value = serde_json::from_slice(r).expect("a JSON record");
            value["id"].as_u64().expect("an id")
        })
        .collect()
}

/// A destination a reload adds takes its records at once: the POST is
/// accepted and the record reaches the new destination, with no restart.
#[tokio::test]
async fn a_destination_a_reload_adds_takes_records_without_a_restart() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let (transform_endpoint, transform) = crate::common::grpc_destination().await;
    let pipeline = pipeline_for(loader_only(&loader_endpoint)).await;
    let (addr, shutdown) = start_http(Arc::clone(&pipeline)).await;

    pipeline
        .reload_config(routing_app(
            loader_only(&loader_endpoint),
            "orders",
            "transform_orders",
            &transform_endpoint,
        ))
        .await
        .expect("the reload applies");
    let status = post(addr, r#"{"app":"orders","id":1}"#).await;
    let at_transform = drain(&transform, 1, Duration::from_secs(10)).await;
    let at_loader = drain(&loader, 1, Duration::from_millis(500)).await;
    shutdown.cancel();

    assert!(
        status.is_success(),
        "the newly routed source was answered {status}"
    );
    assert_eq!(ids(&at_transform), [1], "the new destination's records");
    assert!(at_loader.is_empty(), "the record also reached the loader");
}

/// A destination whose address a reload changes sends to the new address.
#[tokio::test]
async fn a_destination_a_reload_readdresses_sends_to_its_new_address() {
    let (old_endpoint, old) = crate::common::grpc_destination().await;
    let (new_endpoint, new) = crate::common::grpc_destination().await;
    let pipeline = pipeline_for(loader_only(&old_endpoint)).await;
    let (addr, shutdown) = start_http(Arc::clone(&pipeline)).await;

    pipeline
        .reload_config(loader_only(&new_endpoint))
        .await
        .expect("the reload applies");
    let status = post(addr, r#"{"id":2}"#).await;
    let at_new = drain(&new, 1, Duration::from_secs(10)).await;
    let at_old = drain(&old, 1, Duration::from_millis(500)).await;
    shutdown.cancel();

    assert!(status.is_success(), "answered {status}");
    assert_eq!(ids(&at_new), [2], "the new address's records");
    assert!(at_old.is_empty(), "the old address still took records");
}

/// A file-only DLQ under `dir`.
fn file_dlq(dir: &Path) -> DlqConfig {
    DlqConfig {
        enabled: true,
        mode: "file_only".to_string(),
        file_path: dir.display().to_string(),
        kafka_enabled: false,
        ..DlqConfig::default()
    }
}

/// The payload of each entry in the file DLQ under `dir`.
fn dead_letters(dir: &Path) -> Vec<Bytes> {
    let Ok(written) = std::fs::read_to_string(dir.join("receiver").join("dlq.ndjson")) else {
        return Vec::new();
    };
    written
        .lines()
        .map(|line| {
            let entry: serde_json::Value = serde_json::from_str(line).expect("a DLQ entry");
            let payload = entry["payload"].as_str().expect("a base64 payload");
            Bytes::from(
                base64::engine::general_purpose::STANDARD
                    .decode(payload)
                    .expect("base64"),
            )
        })
        .collect()
}

/// A destination a reload removes hands back what its buffer holds: records
/// answered at enqueue that it could not send land in the DLQ, every one.
#[tokio::test]
async fn a_destination_a_reload_removes_loses_no_accepted_record() {
    let dlq_dir = tempfile::tempdir().unwrap();
    let (loader_endpoint, _loader) = crate::common::grpc_destination().await;
    // Held for the whole test, so the archiver's records stay in its buffer.
    let unreachable = crate::common::ClosedPort::loopback().expect("hold a closed port");
    let with_dlq = |mut config: Config| {
        config.routing.dlq = file_dlq(dlq_dir.path());
        // The buffer exists only for a listener that answers at enqueue.
        config.server.acknowledgements = AcknowledgementsConfig::new(false);
        config
    };
    let before = with_dlq(routing_app(
        loader_only(&loader_endpoint),
        "audit",
        "archiver",
        &format!("http://{}", unreachable.addr()),
    ));
    let pipeline = pipeline_for(before).await;
    let (addr, shutdown) = start_http(Arc::clone(&pipeline)).await;

    let mut accepted = Vec::new();
    for id in 0..5_u64 {
        let status = post(addr, &format!(r#"{{"app":"audit","id":{id}}}"#)).await;
        assert!(status.is_success(), "record {id} was answered {status}");
        accepted.push(id);
    }
    pipeline
        .reload_config(with_dlq(loader_only(&loader_endpoint)))
        .await
        .expect("the reload applies");

    let mut landed = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        landed = ids(&dead_letters(dlq_dir.path()));
        if landed.len() >= accepted.len() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    shutdown.cancel();
    landed.sort_unstable();

    assert_eq!(
        landed, accepted,
        "every record the retired destination held is in the DLQ"
    );
}
