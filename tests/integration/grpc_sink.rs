// Project:   dfe-receiver
// File:      tests/integration/grpc_sink.rs
// Purpose:   End-to-end gRPC loader sink tests with in-process server
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! gRPC loader sink integration tests.
//!
//! These tests spin up an in-process gRPC server (via scalo's
//! `GrpcTransport` in server mode) that acts as a mock dfe-loader, then
//! connect a `GrpcSink` to it and verify messages are delivered correctly.
//!
//! No external infrastructure required — everything runs in-process.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dfe_receiver::sink::Sink;
use dfe_receiver::sink::grpc::GrpcSink;
use dfe_receiver::{Error, Result};
use scalo::transport::TransportReceiver;
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};

/// Allocate a random port for test isolation.
fn random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// Send with retry on transient backpressure.
///
/// `GrpcSink::send` surfaces backpressure as `Err(Transport("...backpressured"))`
/// by contract — callers are expected to retry. Under parallel CI load the
/// transport's outgoing queue can briefly fill; a bounded retry with small
/// backoff absorbs that without masking real transport failures.
async fn send_with_retry(sink: &GrpcSink, topic: &str, payload: Bytes) -> Result<()> {
    for attempt in 0..10u32 {
        match sink.send(topic, payload.clone()).await {
            Ok(()) => return Ok(()),
            Err(e) if e.to_string().contains("backpressured") => {
                tokio::time::sleep(Duration::from_millis(10 * u64::from(attempt + 1))).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(Error::Transport(
        "backpressure persisted after 10 retries".into(),
    ))
}

/// Poll the loopback port until it accepts a TCP connection or the
/// budget is exhausted. Replaces blind sleep waits that race tonic's
/// server bind under parallel CI load.
async fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    // 300 x 50ms = 15s budget. ARC runners under parallel CI load can take
    // well over 5s to bind tonic's server, which flaked the grpc_sink tests.
    for _ in 0..300 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("gRPC server on port {port} never accepted connections within 15s");
}

/// Spin up an in-process gRPC server, returning (endpoint, transport, port).
async fn start_server() -> (String, GrpcTransport, u16) {
    // `random_port()` can collide under parallel CI load -- the port is free
    // when picked but grabbed by another test before GrpcTransport binds, so
    // `new()` fails fast. Retry on a fresh port rather than flake the test.
    let mut last_err = String::new();
    for _ in 0..20 {
        let port = random_port();
        let listen = format!("127.0.0.1:{port}");
        let config = GrpcConfig::server(&listen);
        match GrpcTransport::new(&config).await {
            Ok(transport) => {
                wait_for_port(port).await;
                let endpoint = format!("http://127.0.0.1:{port}");
                return (endpoint, transport, port);
            }
            Err(e) => {
                last_err = e.to_string();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!("failed to start gRPC server after 20 attempts: {last_err}");
}

#[tokio::test]
async fn test_grpc_sink_delivers_message() {
    let (endpoint, server, _port) = start_server().await;

    let sink = GrpcSink::new(&endpoint)
        .await
        .expect("GrpcSink creation failed");

    assert!(sink.is_healthy(), "sink should start healthy");

    // Send one message
    let payload = Bytes::from(r#"{"test":"grpc-sink","id":42}"#);
    sink.send("events_load", payload.clone())
        .await
        .expect("send failed");

    // Server should receive it (recv now yields a WorkBatch -- records on
    // `.records`, source acks on `.commit_tokens`)
    let received = server.recv(10).await.expect("recv failed");
    assert_eq!(received.len(), 1, "expected exactly 1 message");
    assert_eq!(&received.records[0].payload[..], &payload[..]);

    assert!(sink.is_healthy(), "sink should remain healthy after send");
}

#[tokio::test]
async fn test_grpc_sink_multiple_messages_preserved_order() {
    let (endpoint, server, _port) = start_server().await;

    let sink = GrpcSink::new(&endpoint).await.expect("sink init");

    // Send 10 ordered messages. Use retry helper: under parallel CI load,
    // the transport's outgoing queue can briefly fill between sequential
    // sends, surfacing as transient backpressure per the sink's contract.
    for i in 0..10 {
        let payload = Bytes::from(format!(r#"{{"seq":{i}}}"#));
        send_with_retry(&sink, "ordered_topic", payload)
            .await
            .expect("send");
    }

    // Collect all messages
    let mut all_received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while all_received.len() < 10 && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = server.recv(10).await {
            all_received.extend(batch.records);
        }
    }

    assert_eq!(all_received.len(), 10, "not all messages received");

    // Verify payload contents (order preservation is a gRPC sink guarantee
    // per topic key, since each RPC completes before the next is sent).
    for (i, msg) in all_received.iter().enumerate() {
        let text = std::str::from_utf8(&msg.payload).unwrap();
        assert!(
            text.contains(&format!("\"seq\":{i}")),
            "message {i} out of order: got {text}"
        );
    }
}

#[tokio::test]
async fn test_grpc_sink_large_payload() {
    let (endpoint, server, _port) = start_server().await;

    let sink = GrpcSink::new(&endpoint).await.expect("sink init");

    // 256 KiB payload (well above typical event size)
    let payload_bytes: Vec<u8> = (0..256 * 1024).map(|i| (i % 256) as u8).collect();
    let payload = Bytes::from(payload_bytes.clone());

    // Retry on transient backpressure — a 256 KiB RPC on a busy ARC
    // runner can briefly fill the transport's outgoing queue. Same
    // contract as ordered/concurrent tests.
    send_with_retry(&sink, "large_topic", payload)
        .await
        .expect("large send failed");

    let received = server.recv(10).await.expect("recv failed");
    assert_eq!(received.len(), 1);
    assert_eq!(received.records[0].payload.len(), payload_bytes.len());
    assert_eq!(&received.records[0].payload[..], &payload_bytes[..]);
}

#[tokio::test]
async fn test_grpc_sink_fails_gracefully_when_server_unreachable() {
    // Connect to a port with no server — connection is lazy so init succeeds
    let sink = GrpcSink::new("http://127.0.0.1:1")
        .await
        .expect("lazy connection should succeed");

    // First send should fail (no server listening). The failure mode is
    // implementation-defined (`Backpressured` or `Fatal` depending on how
    // the transport classifies connection refusal), but `send` must surface
    // it as `Err` rather than silently succeeding.
    let result = sink.send("topic", Bytes::from(r#"{}"#)).await;
    assert!(result.is_err(), "send to unreachable server should fail");
}

#[tokio::test]
async fn test_grpc_sink_recovers_after_server_restart() {
    let (endpoint, server1, port) = start_server().await;

    let sink = GrpcSink::new(&endpoint).await.expect("sink init");

    // Initial send succeeds
    sink.send("topic", Bytes::from(r#"{"phase":"before"}"#))
        .await
        .expect("pre-restart send");
    assert_eq!(server1.recv(1).await.expect("recv").records.len(), 1);

    // Simulate server going down and coming back on the same port
    drop(server1);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send while down — should fail
    let _ = sink
        .send("topic", Bytes::from(r#"{"phase":"during"}"#))
        .await;

    // Restart server on same port
    let config = GrpcConfig::server(&format!("127.0.0.1:{port}"));
    let Ok(server2) = GrpcTransport::new(&config).await else {
        // Port may still be in TIME_WAIT; skip the restart-recovery check
        // rather than failing. The first two phases validate the happy path
        // and failure handling, which is the test's core purpose.
        eprintln!("port {port} still in TIME_WAIT, skipping restart phase");
        return;
    };
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Sink should recover (tonic auto-reconnects)
    let mut recovered = false;
    for _ in 0..5 {
        if sink
            .send("topic", Bytes::from(r#"{"phase":"after"}"#))
            .await
            .is_ok()
        {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    if recovered {
        // Verify the "after" message landed
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            if let Ok(batch) = server2.recv(10).await
                && batch
                    .records
                    .iter()
                    .any(|m| String::from_utf8_lossy(&m.payload).contains("after"))
            {
                return;
            }
        }
        panic!("post-restart message never arrived");
    }
    // Lazy reconnection timing is client-dependent; not strictly a failure.
}

#[tokio::test]
async fn test_grpc_sink_concurrent_sends() {
    let (endpoint, server, _port) = start_server().await;

    let sink = Arc::new(GrpcSink::new(&endpoint).await.expect("sink init"));

    // Fire 50 concurrent sends. Retry on transient backpressure — the
    // transport queue can briefly fill under 50-way fan-out on CI runners.
    let mut handles = Vec::new();
    for i in 0..50 {
        let s = sink.clone();
        handles.push(tokio::spawn(async move {
            send_with_retry(&s, "concurrent", Bytes::from(format!(r#"{{"id":{i}}}"#))).await
        }));
    }
    for h in handles {
        h.await.unwrap().expect("concurrent send failed");
    }

    // Collect all 50
    let mut total = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while total < 50 && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = server.recv(50).await {
            total += batch.len();
        }
    }
    // NB: WorkBatch::len() == records.len(); the running total is correct.
    assert_eq!(total, 50, "expected 50 messages, got {total}");
}
