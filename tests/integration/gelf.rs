// Project:   dfe-receiver
// File:      tests/integration/gelf.rs
// Purpose:   GELF TCP listener against a real gRPC destination
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The GELF handler runs in-process and sends to a gRPC destination standing
//! in for dfe-loader, so every record the listener takes is read back.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::gelf::GelfHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::common::{bound_addr, grpc_destination, receive_records};

/// A GELF message carrying `marker` as its `short_message`, with its delimiter.
fn gelf(marker: &str) -> Vec<u8> {
    format!("{{\"version\":\"1.1\",\"host\":\"h\",\"short_message\":\"{marker}\"}}\0").into_bytes()
}

/// An oversize message costs itself and nothing after it on the same
/// connection, whether its delimiter came in the same read or after a pause,
/// and each one is counted dropped.
#[tokio::test]
async fn gelf_carries_on_past_oversize_messages() {
    let (loader_endpoint, loader) = grpc_destination().await;
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    config.destinations.default = "loader".into();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint);
    config.routing.dlq.enabled = false;
    config.gelf.enabled = true;
    config.gelf.bind_address = "127.0.0.1:0".to_string();
    config.gelf.accept_unauthenticated = true;
    config.gelf.max_message_size = 1_024;

    let metrics = Arc::new(Metrics::default());
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("the pipeline starts"),
    );
    let handler = GelfHandler::new(config.gelf.clone(), pipeline, metrics.clone());
    let bound = handler.bound_addr();
    let shutdown = CancellationToken::new();
    let handler_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(handler_shutdown).await });
    let addr = bound_addr("GELF", &bound, &mut task).await;

    let mut wire = gelf("first");
    wire.extend(vec![b'x'; 2_000]);
    wire.push(b'\0');
    wire.extend(gelf("second"));
    wire.extend(vec![b'y'; 3_000]);
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(&wire).await.unwrap();
    tcp.flush().await.unwrap();
    // The second oversize message is part way in when the reads pause.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut rest = b"yyy\0".to_vec();
    rest.extend(gelf("third"));
    tcp.write_all(&rest).await.unwrap();
    tcp.flush().await.unwrap();

    let records = receive_records(&loader, 3, Duration::from_secs(10)).await;
    let messages: Vec<&str> = records
        .iter()
        .map(|r| r["message"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        messages,
        ["first", "second", "third"],
        "the connection carries on past both oversize messages"
    );
    assert_eq!(
        metrics.get_records_dropped(),
        2,
        "both oversize messages counted"
    );

    tcp.write_all(&gelf("fourth")).await.unwrap();
    let after = receive_records(&loader, 1, Duration::from_secs(10)).await;
    assert_eq!(after.len(), 1, "a message after the oversize ones arrives");

    drop(tcp);
    shutdown.cancel();
}
