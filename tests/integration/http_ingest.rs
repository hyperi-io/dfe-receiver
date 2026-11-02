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

use dfe_receiver::config::{Config, DestinationRef, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::http::HttpHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use scalo::transport::TransportReceiver;
use scalo::transport::grpc::GrpcTransport;
use tokio_util::sync::CancellationToken;

/// Start the HTTP server routing every event to `loader_endpoint`.
fn receiver_config(loader_endpoint: &str) -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.destinations.default = DestinationRef::One("loader".to_string());
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint.to_string());
    config
}

async fn start_receiver(loader_endpoint: &str) -> (String, CancellationToken) {
    let (addr, shutdown) = start_http(receiver_config(loader_endpoint)).await;
    (format!("http://{addr}"), shutdown)
}

async fn start_http(config: Config) -> (std::net::SocketAddr, CancellationToken) {
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("pipeline init"),
    );

    let handler = HttpHandler::new(
        config.server.bind_address.clone(),
        pipeline,
        Arc::new(Metrics::default()),
    );
    let bound = handler.bound_addr();
    let server_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(server_shutdown).await });
    let addr = crate::common::bound_addr("HTTP", &bound, &mut task).await;
    (addr, shutdown)
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
    let (endpoint, loader) = crate::common::grpc_destination().await;
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
    let (endpoint, loader) = crate::common::grpc_destination().await;
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
    let (endpoint, loader) = crate::common::grpc_destination().await;
    let (url, shutdown) = start_receiver(&endpoint).await;

    let status = post(&url, r#"{"event_category":"solo"}"#).await;
    assert!(status.is_success(), "ingest rejected: {status}");

    let records = collect(&loader, 1).await;
    assert_eq!(records.len(), 1, "expected exactly one event");

    shutdown.cancel();
}

fn gzip(data: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

/// A gzip body is undone before the split, so its elements land as events
/// rather than as compressed bytes, and any other encoding is refused 415
/// with nothing stored.
#[tokio::test]
async fn ingest_reads_gzip_and_refuses_other_encodings() {
    let array = br#"[{"event_category":"a"},{"event_category":"b"}]"#;
    // (Content-Encoding, body, status, records stored)
    let cases = vec![
        (Some("gzip"), gzip(array), 202, 2),
        (None, array.to_vec(), 202, 2),
        (Some("zstd"), gzip(array), 415, 0),
        (Some("br"), array.to_vec(), 415, 0),
        (Some("gzip"), b"not gzip".to_vec(), 400, 0),
    ];
    for (encoding, body, status, stored) in cases {
        let (endpoint, loader) = crate::common::grpc_destination().await;
        let (url, shutdown) = start_receiver(&endpoint).await;
        let mut request = reqwest::Client::new()
            .post(format!("{url}/ingest"))
            .header("content-type", "application/json");
        if let Some(encoding) = encoding {
            request = request.header("content-encoding", encoding);
        }

        let resp = request.body(body).send().await.expect("request failed");

        assert_eq!(resp.status().as_u16(), status, "{encoding:?}");
        let wait = Duration::from_secs(if stored == 0 { 1 } else { 10 });
        let records = crate::common::stored_within(&loader, stored, wait).await;
        assert_eq!(records.len(), stored, "{encoding:?}: {records:?}");
        for (record, want) in records.iter().zip(["a", "b"]) {
            let text = String::from_utf8_lossy(record);
            assert!(text.contains(want), "{encoding:?}: {text}");
        }
        shutdown.cancel();
    }
}

/// A gzip body that inflates past `server.max_body_size` is refused 413.
#[tokio::test]
async fn ingest_refuses_a_gzip_bomb() {
    let (endpoint, loader) = crate::common::grpc_destination().await;
    let mut config = receiver_config(&endpoint);
    config.server.max_body_size = 2048;
    let (addr, shutdown) = start_http(config).await;
    let bomb = gzip(&vec![b' '; 1 << 20]);
    assert!(
        bomb.len() < 2048,
        "test setup: the bomb must fit the wire cap"
    );

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .header("content-encoding", "gzip")
        .body(bomb)
        .send()
        .await
        .expect("request failed");

    assert_eq!(resp.status(), 413);
    let records = crate::common::stored_within(&loader, 0, Duration::from_secs(1)).await;
    assert!(records.is_empty(), "stored {} records", records.len());
    shutdown.cancel();
}
