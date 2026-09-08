// Project:   dfe-receiver
// File:      tests/integration/source_routing.rs
// Purpose:   gRPC ingest -> source routing -> loader forward, in-process
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! gRPC PushEvents through the router to the loader, with no external
//! infrastructure.
//!
//! A fetcher-based DFE source is routed on the top-level `_source` dfe-fetcher
//! stamps on every record, using a `key_value_set` rule the engine compiles.
//! These tests drive the receiver's own gRPC listener with scalo's
//! `VectorCompatClient` (the client dfe-fetcher's Vector extractors and Vector
//! itself use) and assert what reaches the loader.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
// The config structs put the inline PipelineState future just over clippy's
// 16 KiB threshold, as in tests/integration/vector.rs.
#![allow(clippy::large_futures)]

use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig, SourceRule};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::{TransportReceiver, VectorCompatClient};
use tokio_util::sync::CancellationToken;

/// The rule the engine compiles for a fetcher-origin source on its own topic.
fn fetcher_source_rule(source: &str) -> SourceRule {
    SourceRule {
        field: "_source".to_string(),
        mode: "key_value_set".to_string(),
        match_value: Some(source.to_string()),
        source: Some(source.to_string()),
    }
}

/// One enriched record as dfe-fetcher emits it.
fn fetcher_record(source: &str) -> serde_json::Value {
    serde_json::json!({
        "crate": "dfe-fetcher",
        "downloads": 42,
        "_timestamp_fetcher": 1_757_000_000_000_u64,
        "_timestamp_received": 1_757_000_000_000_u64,
        "_source": source,
        "_source_fetcher": "crates_io.crates",
    })
}

/// Allocate a free loopback port.
fn random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// Poll until the port accepts a TCP connection, or fail after 15s.
async fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..300 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("nothing listening on 127.0.0.1:{port} within 15s");
}

/// Start a mock dfe-loader: a scalo gRPC server accepting Push RPCs.
async fn start_mock_loader() -> (String, GrpcTransport) {
    // The port is free when picked but can be taken before the bind lands under
    // parallel CI load, so retry on a fresh one.
    let mut last_err = String::new();
    for _ in 0..20 {
        let port = random_port();
        let config = GrpcConfig::server(&format!("127.0.0.1:{port}"));
        match GrpcTransport::new(&config).await {
            Ok(transport) => {
                wait_for_port(port).await;
                return (format!("http://127.0.0.1:{port}"), transport);
            }
            Err(e) => {
                last_err = e.to_string();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!("mock loader failed to start after 20 attempts: {last_err}");
}

/// Start the receiver's gRPC listener over a pipeline built from `config`.
async fn start_receiver_grpc(config: Config) -> (u16, CancellationToken) {
    let port: u16 = config
        .grpc
        .bind_address
        .rsplit(':')
        .next()
        .expect("bind address has a port")
        .parse()
        .expect("port parses");

    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("pipeline"),
    );

    let server_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = dfe_receiver::server::grpc::run_server(
            &config,
            pipeline,
            metrics,
            None,
            server_shutdown,
        )
        .await;
    });

    wait_for_port(port).await;
    (port, shutdown)
}

/// Collect up to `want` records from the mock loader, or give up after 10s.
async fn drain_loader(server: &GrpcTransport, want: usize) -> Vec<bytes::Bytes> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while out.len() < want && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = server.recv(want).await {
            out.extend(batch.records.into_iter().map(|r| r.payload));
        }
    }
    out
}

/// A record pushed over gRPC reaches the loader with its `_source` intact.
///
/// This is the no-Kafka forward: `destinations.default = loader` with
/// `loader.transport = grpc`. The loader maps `_source` to the table, so the
/// receiver must hand the field through untouched.
#[tokio::test]
async fn test_grpc_push_forwards_source_to_the_loader() {
    let (loader_endpoint, loader) = start_mock_loader().await;

    let mut config = Config::default();
    config.grpc.enabled = true;
    config.grpc.bind_address = format!("127.0.0.1:{}", random_port());
    config.destinations.default = "loader".to_string();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint);
    config.routing.source_rules = vec![fetcher_source_rule("crates_audit")];
    config.routing.dlq.enabled = false;

    let (port, shutdown) = start_receiver_grpc(config).await;

    let client = VectorCompatClient::connect_lazy(&format!("http://127.0.0.1:{port}"))
        .expect("vector client");
    client
        .send_events(&[fetcher_record("crates_audit")])
        .await
        .expect("push_events");

    let received = drain_loader(&loader, 1).await;
    shutdown.cancel();

    assert_eq!(
        received.len(),
        1,
        "loader should receive exactly one record"
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&received[0]).expect("loader payload is JSON");
    assert_eq!(parsed["_source"], "crates_audit");
    assert_eq!(parsed["_source_fetcher"], "crates_io.crates");
    assert_eq!(parsed["crate"], "dfe-fetcher");
    // The receiver stamps its own arrival time when include_common_header is on.
    assert!(parsed["_timestamp_receiver"].is_number());
}

/// The gRPC ingest accepts a gzip-compressed PushEvents.
///
/// `VectorCompatClient` compresses unconditionally, so a server without the
/// encoding enabled rejects every scalo-originated push.
#[tokio::test]
async fn test_grpc_push_accepts_gzip() {
    let (loader_endpoint, loader) = start_mock_loader().await;

    let mut config = Config::default();
    config.grpc.enabled = true;
    config.grpc.bind_address = format!("127.0.0.1:{}", random_port());
    config.destinations.default = "loader".to_string();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint);
    config.routing.dlq.enabled = false;

    let (port, shutdown) = start_receiver_grpc(config).await;

    let client = VectorCompatClient::connect_lazy(&format!("http://127.0.0.1:{port}"))
        .expect("vector client");
    let result = client.send_events(&[fetcher_record("default")]).await;
    let received = drain_loader(&loader, 1).await;
    shutdown.cancel();

    result.expect("gzip-compressed push_events must be accepted");
    assert_eq!(received.len(), 1);
}
