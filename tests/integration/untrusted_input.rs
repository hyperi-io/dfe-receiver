// Project:   dfe-receiver
// File:      tests/integration/untrusted_input.rs
// Purpose:   Requests and connections a sender shapes to cost the receiver binary more than they send
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Each test starts the built binary and sends what an untrusted sender can:
//! a request that decodes into far more than its bytes, or connections that
//! send nothing and stay. The receiver must refuse or close them and stay small.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes};
use dfe_receiver::config::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::common::peers::{Cases, Receiver, Sink, free_addr};

/// The most a receiver may hold resident across one of these tests.
const PEAK_RSS_KIB: u64 = 256 * 1024;

/// A config with only the HTTP listener, on a free port.
fn base_config() -> Config {
    let mut config = Config::default();
    config.server.bind_address = free_addr().to_string();
    config.server.auth.mode = "none".to_string();
    config
}

/// Whether the receiver closes `stream` within `within`, reading and
/// discarding anything it sends first.
async fn closed_within(stream: &mut TcpStream, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    let mut buf = [0_u8; 4096];
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match tokio::time::timeout(left, stream.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) => return true,
            Ok(Ok(_)) => {}
            Err(_) => return false,
        }
    }
    false
}

/// A gRPC codec that sends and takes raw message bytes.
#[derive(Clone, Copy, Default)]
struct Raw;

impl tonic::codec::Codec for Raw {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = Raw;
    type Decoder = Raw;

    fn encoder(&mut self) -> Raw {
        Raw
    }

    fn decoder(&mut self) -> Raw {
        Raw
    }
}

impl tonic::codec::Encoder for Raw {
    type Item = Bytes;
    type Error = tonic::Status;

    fn encode(
        &mut self,
        item: Bytes,
        buf: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), tonic::Status> {
        buf.put_slice(&item);
        Ok(())
    }
}

impl tonic::codec::Decoder for Raw {
    type Item = Bytes;
    type Error = tonic::Status;

    fn decode(
        &mut self,
        buf: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Bytes>, tonic::Status> {
        Ok(Some(buf.copy_to_bytes(buf.remaining())))
    }
}

/// Eight million empty events are 16 MB on the wire, inside the gRPC
/// listener's message limit, and each decodes into a struct of its own. The
/// push is refused before it decodes and the receiver stays small.
#[tokio::test]
async fn a_vector_push_past_the_record_budget_is_refused_before_it_decodes() {
    let sink = Sink::start().await;
    let (receiver, grpc) = Receiver::start(&sink, || {
        let grpc = free_addr();
        let mut config = base_config();
        config.grpc.enabled = true;
        config.grpc.bind_address = grpc.to_string();
        (config, vec![grpc], grpc)
    })
    .await
    .unwrap();

    let body = Bytes::from([0x0a_u8, 0x00].repeat(8_000_000));
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{grpc}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let pushed = tokio::time::timeout(
        Duration::from_secs(60),
        client.unary(
            tonic::Request::new(body),
            http::uri::PathAndQuery::from_static("/vector.Vector/PushEvents"),
            Raw,
        ),
    )
    .await
    .expect("the push was answered");

    let status = pushed.expect_err("an over-budget push is refused");
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    let peak = receiver.peak_rss_kib().unwrap();
    assert!(peak < PEAK_RSS_KIB, "the receiver peaked at {peak} KiB");
}

/// One Forward message of ten million empty entries is 30 MB, inside the
/// listener's 32 MiB buffer. It is refused once its value count passes the
/// budget, without re-parsing everything received on every read, and the
/// connection closes.
#[tokio::test]
async fn a_fluent_message_past_the_value_budget_closes_the_connection() {
    let sink = Sink::start().await;
    let (receiver, fluent) = Receiver::start(&sink, || {
        let fluent = free_addr();
        let mut config = base_config();
        config.fluent.enabled = true;
        config.fluent.bind_address = fluent.to_string();
        config.fluent.accept_unauthenticated = true;
        (config, vec![fluent], fluent)
    })
    .await
    .unwrap();

    let entries: u32 = 10_000_000;
    let mut message = vec![0x92, 0xa1, b't', 0xdd];
    message.extend_from_slice(&entries.to_be_bytes());
    message.extend_from_slice(&[0x92, 0x00, 0x80].repeat(entries as usize));

    let mut stream = TcpStream::connect(fluent).await.unwrap();
    let (mut read, mut write) = stream.split();
    let send = async {
        // The receiver may close before the whole message is written.
        let _ = write.write_all(&message).await;
    };
    let mut buf = [0_u8; 64];
    let closed = async {
        matches!(
            tokio::time::timeout(Duration::from_secs(20), read.read(&mut buf)).await,
            Ok(Ok(0) | Err(_))
        )
    };
    let ((), closed) = tokio::join!(send, closed);
    assert!(closed, "the connection was still open after 20s");
    let peak = receiver.peak_rss_kib().unwrap();
    assert!(peak < PEAK_RSS_KIB, "the receiver peaked at {peak} KiB");
}

/// The HTTP/2 connection preface and an empty SETTINGS frame.
const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00";

/// A plaintext connection that never sends a byte is closed after the first
/// byte timeout, on HTTP and on a raw TCP listener. An HTTP/2 connection that
/// opens no stream is closed after the idle timeout. A connection past the
/// per-listener cap is closed at once, and one under it is served.
#[tokio::test]
async fn silent_idle_and_surplus_connections_are_closed() {
    let sink = Sink::start().await;
    let (receiver, (http, syslog)) = Receiver::start(&sink, || {
        let http = free_addr();
        let syslog = free_addr();
        let mut config = base_config();
        config.server.bind_address = http.to_string();
        config.server.connections.first_byte_timeout_ms = 1_000;
        config.server.connections.idle_timeout_ms = 2_000;
        config.server.connections.max_per_listener = 3;
        config.syslog.enabled = true;
        config.syslog.udp_bind_address = free_addr().to_string();
        config.syslog.tcp_bind_address = syslog.to_string();
        config.syslog.accept_unauthenticated = true;
        (config, vec![http, syslog], (http, syslog))
    })
    .await
    .unwrap();
    let within = Duration::from_secs(6);

    let mut silent_http = TcpStream::connect(http).await.unwrap();
    let mut silent_syslog = TcpStream::connect(syslog).await.unwrap();
    let mut h2 = TcpStream::connect(http).await.unwrap();
    h2.write_all(H2_PREFACE).await.unwrap();
    let (http_closed, syslog_closed, h2_closed) = tokio::join!(
        closed_within(&mut silent_http, within),
        closed_within(&mut silent_syslog, within),
        closed_within(&mut h2, within),
    );
    assert!(http_closed, "a silent HTTP connection stayed open");
    assert!(syslog_closed, "a silent syslog connection stayed open");
    assert!(h2_closed, "an idle HTTP/2 connection stayed open");

    let mut held = Vec::new();
    for _ in 0..3 {
        let mut stream = TcpStream::connect(http).await.unwrap();
        stream
            .write_all(b"GET /livez HTTP/1.1\r\nHost: r\r\n\r\n")
            .await
            .unwrap();
        let mut answer = [0_u8; 12];
        stream.read_exact(&mut answer).await.unwrap();
        assert_eq!(
            &answer, b"HTTP/1.1 200",
            "a connection under the cap was not served"
        );
        held.push(stream);
    }
    let mut surplus = TcpStream::connect(http).await.unwrap();
    assert!(
        closed_within(&mut surplus, Duration::from_millis(500)).await,
        "a connection past the cap was kept"
    );
    drop(held);
    drop(receiver);
}

/// A raw TCP listener, how it frames one record, and the first byte of a frame.
struct RawListener {
    name: &'static str,
    addr: SocketAddr,
    /// A whole frame carrying `marker`, with `sequence` where the protocol numbers events.
    frame: fn(marker: &str, sequence: u32) -> Vec<u8>,
    start: &'static [u8],
}

fn syslog_frame(marker: &str, _: u32) -> Vec<u8> {
    format!("<14>Oct 31 10:00:00 host app: {marker}\n").into_bytes()
}

fn gelf_frame(marker: &str, _: u32) -> Vec<u8> {
    format!("{{\"version\":\"1.1\",\"host\":\"h\",\"short_message\":\"{marker}\"}}\0").into_bytes()
}

fn fluent_frame(marker: &str, _: u32) -> Vec<u8> {
    let message = rmpv::Value::Array(vec![
        rmpv::Value::from("t"),
        rmpv::Value::Integer(1_700_000_000.into()),
        rmpv::Value::Map(vec![(
            rmpv::Value::from("message"),
            rmpv::Value::from(marker),
        )]),
    ]);
    let mut frame = Vec::new();
    rmpv::encode::write_value(&mut frame, &message).unwrap();
    frame
}

/// A Lumberjack window of one event.
fn lumberjack_frame(marker: &str, sequence: u32) -> Vec<u8> {
    let payload = format!("{{\"message\":\"{marker}\"}}");
    let mut frame = b"2W".to_vec();
    frame.extend_from_slice(&1_u32.to_be_bytes());
    frame.extend_from_slice(b"2J");
    frame.extend_from_slice(&sequence.to_be_bytes());
    frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(payload.as_bytes());
    frame
}

/// A connection `addr` admitted. A listener at its cap closes a new one at
/// once, and one under it waits for the client to speak.
async fn admitted(addr: SocketAddr, within: Duration) -> Option<TcpStream> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let mut stream = TcpStream::connect(addr).await.ok()?;
        let mut byte = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut byte)).await;
        if read.is_err() {
            return Some(stream);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

/// The stalled and the idle client on one listener, each case named with its verdict.
async fn stalled_and_idle(sink: &Sink, listener: &RawListener) -> Vec<(String, bool, String)> {
    let name = listener.name;
    let mut cases = Vec::new();

    let Some(mut stalled) = admitted(listener.addr, Duration::from_secs(5)).await else {
        cases.push((
            format!("{name}: first client"),
            false,
            "never admitted".into(),
        ));
        return cases;
    };
    let stalled_marker = format!("stalled-{name}");
    let mut sent = (listener.frame)(&stalled_marker, 1);
    sent.extend_from_slice(listener.start);
    stalled.write_all(&sent).await.unwrap();
    let landed = sink
        .wait_for(&stalled_marker, Duration::from_secs(10))
        .await;
    cases.push((
        format!("{name}: the frame before the stall lands"),
        landed,
        sink.dump(),
    ));
    let closed = closed_within(&mut stalled, Duration::from_secs(5)).await;
    cases.push((
        format!("{name}: a stalled frame closes its connection"),
        closed,
        "still open 5s after the stall".into(),
    ));

    let Some(mut idle) = admitted(listener.addr, Duration::from_secs(5)).await else {
        cases.push((
            format!("{name}: the cap recovers"),
            false,
            "a new client was not admitted".into(),
        ));
        return cases;
    };
    let before = format!("before-idle-{name}");
    let after = format!("after-idle-{name}");
    idle.write_all(&(listener.frame)(&before, 1)).await.unwrap();
    let first = sink.wait_for(&before, Duration::from_secs(10)).await;
    cases.push((format!("{name}: the cap recovers"), first, sink.dump()));
    // Three frame timeouts with no frame in progress.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let wrote = idle.write_all(&(listener.frame)(&after, 2)).await.is_ok();
    let next = wrote && sink.wait_for(&after, Duration::from_secs(10)).await;
    cases.push((
        format!("{name}: an idle connection keeps its next frame"),
        next,
        sink.dump(),
    ));
    cases
}

/// A client that sends a whole frame and the first byte of the next, then
/// stops, is closed once the frame timeout passes, which frees the only place
/// under the cap. A client idle between whole frames for longer than that
/// keeps its connection, and its next frame lands.
#[tokio::test]
async fn a_stalled_frame_is_closed_and_an_idle_connection_is_kept() {
    let sink = Sink::start().await;
    let (receiver, addrs) = Receiver::start(&sink, || {
        let addrs = [free_addr(), free_addr(), free_addr(), free_addr()];
        let [syslog, gelf, fluent, lumberjack] = addrs;
        let mut config = base_config();
        config.server.connections.max_per_listener = 1;
        config.server.connections.frame_timeout_ms = 1_000;
        config.syslog.enabled = true;
        config.syslog.udp_bind_address = free_addr().to_string();
        config.syslog.tcp_bind_address = syslog.to_string();
        config.syslog.accept_unauthenticated = true;
        config.gelf.enabled = true;
        config.gelf.bind_address = gelf.to_string();
        config.gelf.accept_unauthenticated = true;
        config.fluent.enabled = true;
        config.fluent.bind_address = fluent.to_string();
        config.fluent.accept_unauthenticated = true;
        config.lumberjack.enabled = true;
        config.lumberjack.bind_address = lumberjack.to_string();
        config.lumberjack.accept_unauthenticated = true;
        (config, addrs.to_vec(), addrs)
    })
    .await
    .unwrap();

    let [syslog, gelf, fluent, lumberjack] = addrs;
    let listeners = [
        RawListener {
            name: "syslog",
            addr: syslog,
            frame: syslog_frame,
            start: b"<",
        },
        RawListener {
            name: "gelf",
            addr: gelf,
            frame: gelf_frame,
            start: b"{",
        },
        RawListener {
            name: "fluent",
            addr: fluent,
            frame: fluent_frame,
            start: b"\x93",
        },
        RawListener {
            name: "lumberjack",
            addr: lumberjack,
            frame: lumberjack_frame,
            start: b"2",
        },
    ];
    let (a, b, c, d) = tokio::join!(
        stalled_and_idle(&sink, &listeners[0]),
        stalled_and_idle(&sink, &listeners[1]),
        stalled_and_idle(&sink, &listeners[2]),
        stalled_and_idle(&sink, &listeners[3]),
    );
    let mut cases = Cases::default();
    for (name, ok, detail) in [a, b, c, d].into_iter().flatten() {
        cases.check(&name, ok, || detail);
    }
    cases.finish(|| receiver.log_text());
}
