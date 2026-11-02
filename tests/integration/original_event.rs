// Project:   dfe-receiver
// File:      tests/integration/original_event.rs
// Purpose:   event.original and log.original are dropped beside a message, over HTTP and Lumberjack
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A Logstash event with ECS compatibility on carries the raw line in
//! `message` and again in `event.original`. Each case is sent through the real
//! HTTP and Lumberjack listeners, and the record that reaches the in-process
//! loader says whether the copy survived.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, DestinationRef, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::http::HttpHandler;
use dfe_receiver::server::lumberjack::LumberjackHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use scalo::transport::TransportReceiver;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

/// A case, and whether `event.original` is still on the record that lands.
fn cases() -> Vec<(&'static str, Value, bool)> {
    let line = "<14>Oct 31 10:00:00 host app: hello";
    vec![
        (
            "message-and-original",
            json!({"message": line, "event": {"original": line, "sequence": 0}}),
            false,
        ),
        (
            "log-original-too",
            json!({"message": line, "event": {"original": line}, "log": {"original": line}}),
            false,
        ),
        (
            "preserve-tag",
            json!({"message": line, "tags": ["preserve_original_event"], "event": {"original": line}}),
            true,
        ),
        ("no-message", json!({"event": {"original": line}}), true),
    ]
}

fn receiver_config(loader_endpoint: &str) -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.destinations.default = DestinationRef::One("loader".to_string());
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint.to_string());
    config.lumberjack.enabled = true;
    config.lumberjack.bind_address = "127.0.0.1:0".to_string();
    config.lumberjack.accept_unauthenticated = true;
    config
}

/// Every record the loader receives within the deadline, parsed, by `case`.
async fn landed(loader: &scalo::transport::grpc::GrpcTransport, expected: usize) -> Vec<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut records = Vec::new();
    while records.len() < expected && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = loader.recv(10).await {
            records.extend(
                batch
                    .records
                    .into_iter()
                    .map(|r| serde_json::from_slice::<Value>(&r.payload).unwrap()),
            );
        }
    }
    records
}

fn assert_landed(records: &[Value]) {
    let cases = cases();
    assert_eq!(records.len(), cases.len(), "every case lands: {records:?}");
    for (id, _, kept) in &cases {
        let record = records
            .iter()
            .find(|r| r["case"] == *id)
            .unwrap_or_else(|| panic!("{id} did not land in {records:?}"));
        assert_eq!(
            record["event"].get("original").is_some(),
            *kept,
            "{id}: {record}"
        );
        if *id == "log-original-too" {
            assert!(record.get("log").is_none(), "{id}: {record}");
        }
        if *id == "message-and-original" {
            assert_eq!(record["event"]["sequence"], 0, "{id}: {record}");
        }
        if !*kept {
            assert!(record["message"].is_string(), "{id}: {record}");
        }
    }
}

#[tokio::test]
async fn http_ingest_drops_the_copy_beside_a_message() {
    let (endpoint, loader) = crate::common::grpc_destination().await;
    let config = receiver_config(&endpoint);
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .unwrap(),
    );
    let handler = HttpHandler::new(
        config.server.bind_address.clone(),
        pipeline,
        Arc::new(Metrics::default()),
    );
    let bound = handler.bound_addr();
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(server_shutdown).await });
    let addr = crate::common::bound_addr("HTTP", &bound, &mut task).await;

    let body: Vec<Value> = cases()
        .into_iter()
        .map(|(id, mut record, _)| {
            record["case"] = json!(id);
            record
        })
        .collect();
    let status = reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .status();
    assert!(status.is_success(), "ingest refused: {status}");

    assert_landed(&landed(&loader, body.len()).await);
    shutdown.cancel();
}

#[tokio::test]
async fn lumberjack_drops_the_copy_beside_a_message() {
    let (endpoint, loader) = crate::common::grpc_destination().await;
    let config = receiver_config(&endpoint);
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .unwrap(),
    );
    let handler = LumberjackHandler::new(
        config.lumberjack.clone(),
        pipeline,
        Arc::new(Metrics::default()),
    );
    let bound = handler.bound_addr();
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(server_shutdown).await });
    let addr = crate::common::bound_addr("Lumberjack", &bound, &mut task).await;

    let cases = cases();
    let mut wire = vec![b'2', b'W'];
    wire.extend_from_slice(&u32::try_from(cases.len()).unwrap().to_be_bytes());
    for (sequence, (id, record, _)) in (1_u32..).zip(&cases) {
        let mut record = record.clone();
        record["case"] = json!(id);
        let payload = serde_json::to_vec(&record).unwrap();
        wire.extend_from_slice(b"2J");
        wire.extend_from_slice(&sequence.to_be_bytes());
        wire.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        wire.extend_from_slice(&payload);
    }
    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    client.write_all(&wire).await.unwrap();
    let mut ack = [0_u8; 6];
    tokio::time::timeout(Duration::from_secs(15), client.read_exact(&mut ack))
        .await
        .expect("the window is acknowledged")
        .unwrap();

    assert_landed(&landed(&loader, cases.len()).await);
    shutdown.cancel();
}
