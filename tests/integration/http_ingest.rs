// Project:   dfe-receiver
// File:      tests/integration/http_ingest.rs
// Purpose:   HTTP ingest body handling end to end through the real server
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Ingest-body tests that drive the real HTTP server.
//!
//! An in-process gRPC server stands in for dfe-loader, so the records it
//! receives are the events a request actually produced. Nothing external is
//! required.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
// Test helpers build a PipelineState inline; the config structs put the future
// just over clippy's 16 KiB threshold. Mirrors the lib crate's allow (main.rs).
#![allow(clippy::large_futures)]

use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::http;
use scalo::transport::TransportReceiver;
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use tokio_util::sync::CancellationToken;

fn random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// Poll the loopback port until it accepts a connection, up to `attempts`.
async fn port_accepts(port: u16, attempts: u32) -> bool {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..attempts {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Stand up the mock loader, retrying when a picked port is taken before bind.
async fn start_mock_loader() -> (String, GrpcTransport) {
    let mut last_err = String::new();
    for _ in 0..20 {
        let port = random_port();
        let config = GrpcConfig::server(&format!("127.0.0.1:{port}"));
        match GrpcTransport::new(&config).await {
            Ok(transport) if port_accepts(port, 300).await => {
                return (format!("http://127.0.0.1:{port}"), transport);
            }
            Ok(_) => {
                last_err = format!("port {port} never accepted");
            }
            Err(e) => {
                last_err = e.to_string();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!("failed to start mock loader after 20 attempts: {last_err}");
}

/// Start the HTTP server routing every event to `loader_endpoint`.
///
/// A port picked by `random_port` can be taken again before the server binds
/// it, so a port that never accepts is retried rather than failed.
async fn start_receiver(loader_endpoint: &str) -> (String, CancellationToken) {
    for _ in 0..20 {
        let port = random_port();

        let mut config = Config::default();
        config.server.bind_address = format!("127.0.0.1:{port}");
        config.destinations.default = "loader".to_string();
        config.loader.transport = "grpc".to_string();
        config.loader.grpc_endpoint = Some(loader_endpoint.to_string());

        let shutdown = CancellationToken::new();
        let pipeline = Arc::new(
            PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
                .await
                .expect("pipeline init"),
        );

        let bind_addr = config.server.bind_address.clone();
        let server_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = http::run_server(
                &bind_addr,
                pipeline,
                Arc::new(Metrics::default()),
                server_shutdown,
            )
            .await;
        });

        if port_accepts(port, 40).await {
            return (format!("http://127.0.0.1:{port}"), shutdown);
        }
        shutdown.cancel();
    }
    panic!("receiver never accepted connections on any of 20 ports");
}

/// Collect records from the mock loader until `expected` arrive or time runs out.
async fn collect(loader: &GrpcTransport, expected: usize) -> Vec<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut records = Vec::new();
    while records.len() < expected && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = loader.recv(10).await {
            records.extend(batch.records.into_iter().map(|r| r.payload.to_vec()));
        }
    }
    records
}

async fn post(url: &str, body: &'static str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(format!("{url}/ingest"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("request failed")
        .status()
}

#[tokio::test]
async fn a_posted_json_array_becomes_one_event_per_element() {
    let (endpoint, loader) = start_mock_loader().await;
    let (url, shutdown) = start_receiver(&endpoint).await;

    let status = post(
        &url,
        r#"[{"event_category":"a"},{"event_category":"b"},{"event_category":"c"}]"#,
    )
    .await;
    assert!(status.is_success(), "batched ingest rejected: {status}");

    let records = collect(&loader, 3).await;
    assert_eq!(records.len(), 3, "expected one event per element");
    for (record, want) in records.iter().zip(["a", "b", "c"]) {
        let text = String::from_utf8_lossy(record);
        assert!(
            !text.starts_with('['),
            "the array reached the loader whole: {text}"
        );
        assert!(text.contains(want), "element out of order or lost: {text}");
    }

    shutdown.cancel();
}

#[tokio::test]
async fn a_posted_ndjson_body_becomes_one_event_per_line() {
    // The deployment contract advertises NDJSON on this endpoint; unsplit it
    // fails validation whole and lands in the DLQ, so the client sees 202 and
    // no data.
    let (endpoint, loader) = start_mock_loader().await;
    let (url, shutdown) = start_receiver(&endpoint).await;

    let status = post(
        &url,
        "{\"event_category\":\"a\"}\n{\"event_category\":\"b\"}\n{\"event_category\":\"c\"}",
    )
    .await;
    assert!(status.is_success(), "ndjson ingest rejected: {status}");

    let records = collect(&loader, 3).await;
    assert_eq!(records.len(), 3, "expected one event per line");
    for (record, want) in records.iter().zip(["a", "b", "c"]) {
        let text = String::from_utf8_lossy(record);
        assert!(
            !text.contains('\n'),
            "the ndjson body reached the loader whole: {text}"
        );
        assert!(text.contains(want), "line out of order or lost: {text}");
    }

    shutdown.cancel();
}

#[tokio::test]
async fn a_posted_object_stays_a_single_event() {
    let (endpoint, loader) = start_mock_loader().await;
    let (url, shutdown) = start_receiver(&endpoint).await;

    let status = post(&url, r#"{"event_category":"solo"}"#).await;
    assert!(status.is_success(), "ingest rejected: {status}");

    let records = collect(&loader, 1).await;
    assert_eq!(records.len(), 1, "expected exactly one event");

    shutdown.cancel();
}
