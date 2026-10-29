// Project:   dfe-receiver
// File:      tests/integration/fluent.rs
// Purpose:   The Fluent Forward listener closes a connection that streams unparseable data
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Fluent Forward abuse test over a raw TCP client. Fluent Bit shipping, with
//! and without TLS, is in `fluent_bit.rs`.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
// Test helpers build a PipelineState inline; the config structs put the future
// just over clippy's 16 KiB threshold. Mirrors the lib crate's allow (main.rs).
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::fluent::FluentHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use tokio_util::sync::CancellationToken;

fn test_config() -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    config.fluent.enabled = true;
    config.fluent.bind_address = "127.0.0.1:0".to_string();
    config.fluent.accept_unauthenticated = true;
    // The loader on its memory transport: accepted, sent nowhere, no broker.
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config
}

async fn start_fluent_handler(config: Config) -> (CancellationToken, Arc<Metrics>, SocketAddr) {
    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(
            SharedConfig::new(config.clone()),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("Failed to create pipeline"),
    );

    let handler = FluentHandler::new(config.fluent.clone(), pipeline, metrics.clone());
    let bound = handler.bound_addr();

    let handler_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(handler_shutdown).await });
    let addr = crate::common::bound_addr("Fluent Forward", &bound, &mut task).await;
    (shutdown, metrics, addr)
}

// ---------------------------------------------------------------------------
// Security: buffer size enforcement (OOM protection)
// ---------------------------------------------------------------------------

/// Verify the Fluent Forward handler closes a connection when the pending
/// buffer would exceed `max_message_size`. An attacker could otherwise
/// send a stream of incomplete msgpack data to cause unbounded growth.
///
/// This test uses a raw TCP client (no fluent-bit needed) so it always runs.
#[tokio::test]
async fn test_fluent_buffer_size_enforcement() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let mut config = test_config();
    // Small limit to make the test fast and predictable
    config.fluent.max_message_size = 64 * 1024; // 64 KiB

    let (shutdown, _metrics, addr) = start_fluent_handler(config).await;

    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect to fluent listener");

    // 0xc1 is a reserved marker in msgpack — rmpv returns an error on it,
    // which the server treats as "incomplete, wait for more data". The bytes
    // are never consumed from `pending`, so the buffer grows unboundedly
    // unless our size guard closes the connection.
    let garbage_chunk = vec![0xc1_u8; 8192]; // 8 KiB of invalid markers
    let mut total_sent = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    // Generous cutoff to account for OS TCP buffers (up to ~512 KB on Linux
    // per-direction). Without the guard, we would be able to send unlimited
    // data; with the guard, the server closes at 64 KB (plus OS buffers).
    let abort_at = 8 * 1024 * 1024; // 8 MiB hard cap
    while tokio::time::Instant::now() < deadline && total_sent < abort_at {
        match tokio::time::timeout(Duration::from_millis(500), stream.write_all(&garbage_chunk))
            .await
        {
            Ok(Ok(())) => total_sent += garbage_chunk.len(),
            Ok(Err(_)) => break, // Connection closed by server (expected)
            Err(_) => break,     // Timeout on write (server stopped reading)
        }
    }

    // Without the guard, we'd easily send the full 8 MiB. With the guard,
    // the server closes the connection once `pending` exceeds 64 KiB.
    // Some slack is needed for OS TCP send buffers (~512 KiB typical).
    assert!(
        total_sent < abort_at,
        "server accepted unlimited incomplete data: {total_sent} bytes sent (OOM guard failed)"
    );

    shutdown.cancel();
}
