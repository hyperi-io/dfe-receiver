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

// ---------------------------------------------------------------------------
// CompressedPackedForward
// ---------------------------------------------------------------------------

/// A Forward message `[tag, blob, {"compressed": codec, "chunk": chunk}]`.
fn packed_message(blob: Vec<u8>, codec: &str, chunk: &str) -> Vec<u8> {
    use rmpv::Value;

    let message = Value::Array(vec![
        Value::String("app.log".into()),
        Value::Binary(blob),
        Value::Map(vec![
            (
                Value::String("compressed".into()),
                Value::String(codec.into()),
            ),
            (Value::String("chunk".into()), Value::String(chunk.into())),
        ]),
    ]);
    let mut wire = Vec::new();
    rmpv::encode::write_value(&mut wire, &message).unwrap();
    wire
}

/// `count` PackedForward entries `{"msg": "<prefix>-<n>"}`.
fn packed_entries(prefix: &str, count: usize) -> Vec<u8> {
    use rmpv::Value;

    let mut packed = Vec::new();
    for n in 0..count {
        let entry = Value::Array(vec![
            Value::Integer(1_700_000_000.into()),
            Value::Map(vec![(
                Value::String("msg".into()),
                Value::String(format!("{prefix}-{n}").into()),
            )]),
        ]);
        rmpv::encode::write_value(&mut packed, &entry).unwrap();
    }
    packed
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write as _;

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

/// The chunk the next `{"ack": chunk}` on `stream` names, `None` if the
/// server closes the connection first.
async fn read_ack(stream: &mut tokio::net::TcpStream) -> Option<String> {
    use tokio::io::AsyncReadExt;

    let mut buf = vec![0_u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buf))
        .await
        .expect("the server neither acked nor closed")
        .unwrap_or(0);
    if n == 0 {
        return None;
    }
    let ack = rmpv::decode::read_value(&mut &buf[..n]).unwrap();
    let chunk = ack.as_map().unwrap()[0].1.as_str().unwrap().to_string();
    Some(chunk)
}

fn grpc_loader_config(endpoint: &str) -> Config {
    let mut config = test_config();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(endpoint.to_string());
    config.routing.dlq.enabled = false;
    config
}

/// Records the loader receives within the deadline.
async fn landed(loader: &scalo::transport::grpc::GrpcTransport, expected: usize) -> Vec<String> {
    use scalo::transport::TransportReceiver;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut records = Vec::new();
    while records.len() < expected && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = loader.recv(10).await {
            records.extend(batch.records.into_iter().map(|r| {
                let json: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
                json["msg"].as_str().unwrap().to_string()
            }));
        }
    }
    records.sort();
    records
}

/// A gzip PackedForward chunk, one gzip member or several, lands every entry
/// and is acknowledged once the pipeline has taken them.
#[tokio::test]
async fn a_gzip_packed_forward_chunk_lands_and_is_acked() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let (endpoint, loader) = crate::common::grpc_destination().await;
    let (shutdown, metrics, addr) = start_fluent_handler(grpc_loader_config(&endpoint)).await;

    let cases = [
        ("one", gzip(&packed_entries("one", 3))),
        (
            "several",
            [
                gzip(&packed_entries("several-a", 2)),
                gzip(&packed_entries("several-b", 2)),
            ]
            .concat(),
        ),
    ];
    let mut want = vec![
        "one-0",
        "one-1",
        "one-2",
        "several-a-0",
        "several-a-1",
        "several-b-0",
        "several-b-1",
    ];
    want.sort_unstable();
    for (name, blob) in cases {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(&packed_message(blob, "gzip", name))
            .await
            .unwrap();
        assert_eq!(read_ack(&mut stream).await.as_deref(), Some(name), "{name}");
    }

    assert_eq!(landed(&loader, want.len()).await, want);
    assert_eq!(metrics.get_requests_success(), 2);
    assert_eq!(metrics.get_requests_error(), 0);
    shutdown.cancel();
}

/// A gzip chunk that inflates past `max_message_size` is refused, whatever it
/// compresses to, with no ack, and none of its entries land.
#[tokio::test]
async fn a_gzip_chunk_past_the_size_limit_once_inflated_is_refused() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let (endpoint, loader) = crate::common::grpc_destination().await;
    let mut config = grpc_loader_config(&endpoint);
    config.fluent.max_message_size = 64 * 1024;
    let (shutdown, metrics, addr) = start_fluent_handler(config).await;

    let packed = packed_entries("bomb", 20_000);
    assert!(
        packed.len() > 64 * 1024,
        "the entries inflate past the limit"
    );
    let blob = gzip(&packed);
    assert!(
        blob.len() < 64 * 1024,
        "the blob fits the frame limit: {}",
        blob.len()
    );
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(&packed_message(blob, "gzip", "bomb"))
        .await
        .unwrap();
    assert_eq!(read_ack(&mut stream).await, None, "a gzip bomb was acked");

    assert_eq!(landed(&loader, 1).await, Vec::<String>::new());
    assert_eq!(metrics.get_requests_error(), 1);
    assert_eq!(metrics.get_requests_success(), 0);
    shutdown.cancel();
}

/// A chunk compressed with a codec the receiver cannot read gets no ack and
/// the connection closes, so the sender keeps the chunk and resends.
#[tokio::test]
async fn a_chunk_compressed_with_zstd_gets_no_ack() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let (endpoint, loader) = crate::common::grpc_destination().await;
    let (shutdown, metrics, addr) = start_fluent_handler(grpc_loader_config(&endpoint)).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(&packed_message(
            packed_entries("zstd", 2),
            "zstd",
            "zstd-chunk",
        ))
        .await
        .unwrap();
    assert_eq!(read_ack(&mut stream).await, None, "a zstd chunk was acked");

    assert_eq!(landed(&loader, 1).await, Vec::<String>::new());
    assert_eq!(metrics.get_requests_error(), 1);
    assert_eq!(metrics.get_requests_success(), 0);
    shutdown.cancel();
}

/// A chunk of valid entries past the entry budget is not malformed, so it gets
/// no ack and the connection closes, and the sender resends or fails visibly.
#[tokio::test]
async fn a_chunk_past_the_entry_budget_gets_no_ack() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let (endpoint, loader) = crate::common::grpc_destination().await;
    let (shutdown, metrics, addr) = start_fluent_handler(grpc_loader_config(&endpoint)).await;

    // 100 001 entries of `[0, {}]`, one past the entry budget.
    let packed = [0x92, 0x00, 0x80].repeat(100_001);
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(&packed_message(packed, "text", "big"))
        .await
        .unwrap();
    assert_eq!(
        read_ack(&mut stream).await,
        None,
        "an over-budget chunk was acked"
    );

    assert_eq!(landed(&loader, 1).await, Vec::<String>::new());
    assert_eq!(metrics.get_requests_error(), 1);
    shutdown.cancel();
}

/// A chunk that is not msgpack at all is acknowledged: it fails the same way
/// every time it is sent.
#[tokio::test]
async fn a_chunk_that_is_not_msgpack_is_acked() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let (endpoint, _loader) = crate::common::grpc_destination().await;
    let (shutdown, metrics, addr) = start_fluent_handler(grpc_loader_config(&endpoint)).await;

    // Entries that are not an array of `[time, record]` pairs: a lone reserved marker.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(&packed_message(vec![0xc1], "text", "junk"))
        .await
        .unwrap();
    assert_eq!(read_ack(&mut stream).await.as_deref(), Some("junk"));
    assert_eq!(metrics.get_requests_error(), 1);
    shutdown.cancel();
}

/// Fluentd's out_forward sends the packed entries as a msgpack str, not bin,
/// and every entry lands and the chunk is acked.
#[tokio::test]
async fn a_packed_forward_chunk_sent_as_a_str_lands_every_entry() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let (endpoint, loader) = crate::common::grpc_destination().await;
    let (shutdown, metrics, addr) = start_fluent_handler(grpc_loader_config(&endpoint)).await;

    let packed = packed_entries("str", 3);
    let mut wire = vec![0x93, 0xa7];
    wire.extend_from_slice(b"app.log");
    wire.push(0xdb);
    wire.extend_from_slice(&u32::try_from(packed.len()).unwrap().to_be_bytes());
    wire.extend_from_slice(&packed);
    rmpv::encode::write_value(
        &mut wire,
        &rmpv::Value::Map(vec![(
            rmpv::Value::String("chunk".into()),
            rmpv::Value::String("fluentd".into()),
        )]),
    )
    .unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(&wire).await.unwrap();
    assert_eq!(read_ack(&mut stream).await.as_deref(), Some("fluentd"));

    assert_eq!(landed(&loader, 3).await, ["str-0", "str-1", "str-2"]);
    assert_eq!(metrics.get_requests_success(), 1);
    shutdown.cancel();
}
