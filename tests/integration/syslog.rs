// Project:   dfe-receiver
// File:      tests/integration_syslog.rs
// Purpose:   Integration tests using logger binary (syslog protocol)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests that use the `logger` binary (util-linux / bsdutils).
//!
//! These tests start a dfe-receiver syslog handler and run `logger` as a
//! subprocess to send syslog messages over UDP and TCP.
//!
//! Run with: `cargo test --test integration_syslog`
//!
//! Requirements:
//! - `logger` binary (bsdutils — Essential package on Ubuntu/Debian, built-in on macOS)

// Allow unwrap/expect in tests - they're the idiomatic way to fail fast
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
// Test helpers build a PipelineState inline; the config structs put the future
// just over clippy's 16 KiB threshold. Mirrors the lib crate's allow (main.rs).
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::syslog::SyslogHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use tokio_util::sync::CancellationToken;

use crate::common::receive_records;

/// Check if logger binary is available and supports --server flag.
fn has_logger() -> bool {
    Command::new("logger")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Create a minimal config for testing with syslog enabled.
fn test_config() -> Config {
    let mut config = Config::default();
    // HTTP server still needs a bind address (always enabled)
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    // Port 0, so each listener takes a free port at bind time
    config.syslog.enabled = true;
    config.syslog.udp_bind_address = "127.0.0.1:0".to_string();
    config.syslog.tcp_bind_address = "127.0.0.1:0".to_string();
    // Disable TLS for tests
    config.syslog.tls.enabled = false;
    // The loader on its memory transport: accepted, sent nowhere, no broker.
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config
}

/// A running syslog handler and the addresses its plaintext listeners bound.
struct Started {
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
    udp: SocketAddr,
    tcp: SocketAddr,
}

/// Start the syslog handler and wait for its UDP and TCP listeners to bind.
async fn start_syslog_handler(config: Config) -> Started {
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

    let handler = SyslogHandler::new(
        config.syslog.clone(),
        config.raw_capture_for(&config.syslog.raw_capture),
        pipeline,
        metrics.clone(),
    );
    let udp_bound = handler.udp_bound_addr();
    let tcp_bound = handler.tcp_bound_addr();

    let handler_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(handler_shutdown).await });
    let udp = crate::common::bound_addr("syslog UDP", &udp_bound, &mut task).await;
    let tcp = crate::common::bound_addr("syslog TCP", &tcp_bound, &mut task).await;

    Started {
        shutdown,
        metrics,
        udp,
        tcp,
    }
}

/// Get requests_total from metrics.
fn requests_total(metrics: &Metrics) -> u64 {
    metrics.get_requests_total()
}

/// Get requests_success from rendered metrics.
fn requests_success(metrics: &Metrics) -> u64 {
    metrics.get_requests_success()
}

/// Get bytes_received_total from rendered metrics.
fn bytes_received(metrics: &Metrics) -> u64 {
    metrics.get_bytes_received()
}

// ---------------------------------------------------------------------------
// UDP Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_syslog_udp_rfc5424() {
    if !has_logger() {
        eprintln!("SKIP: logger binary not found");
        return;
    }

    let syslog = start_syslog_handler(test_config()).await;

    // Send an RFC 5424 message via UDP
    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &syslog.udp.port().to_string(),
            "--udp",
            "--rfc5424",
            "--tag",
            "integration-test",
            "--priority",
            "user.info",
            "--msgid",
            "TEST001",
            "Hello from syslog UDP RFC5424",
        ])
        .output()
        .expect("Failed to run logger");

    assert!(output.status.success(), "logger failed: {:?}", output);

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    let total = requests_total(&syslog.metrics);
    assert!(total >= 1, "Expected at least 1 request, got {total}");

    let success = requests_success(&syslog.metrics);
    assert!(
        success >= 1,
        "Expected at least 1 successful request, got {success}"
    );

    syslog.shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_syslog_udp_rfc3164() {
    if !has_logger() {
        eprintln!("SKIP: logger binary not found");
        return;
    }

    let syslog = start_syslog_handler(test_config()).await;

    // Send an RFC 3164 message via UDP
    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &syslog.udp.port().to_string(),
            "--udp",
            "--rfc3164",
            "--tag",
            "integration-test",
            "--priority",
            "daemon.warning",
            "Hello from syslog UDP RFC3164",
        ])
        .output()
        .expect("Failed to run logger");

    assert!(output.status.success(), "logger failed: {:?}", output);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let total = requests_total(&syslog.metrics);
    assert!(total >= 1, "Expected at least 1 request, got {total}");

    syslog.shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_syslog_udp_multiple_messages() {
    if !has_logger() {
        eprintln!("SKIP: logger binary not found");
        return;
    }

    let syslog = start_syslog_handler(test_config()).await;

    // Send 5 messages
    for i in 0..5 {
        let output = Command::new("logger")
            .args([
                "--server",
                "127.0.0.1",
                "--port",
                &syslog.udp.port().to_string(),
                "--udp",
                "--rfc5424",
                "--tag",
                "batch-test",
                &format!("Message {i} of 5"),
            ])
            .output()
            .expect("Failed to run logger");

        assert!(output.status.success(), "logger {i} failed: {:?}", output);
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    let total = requests_total(&syslog.metrics);
    assert!(total >= 5, "Expected at least 5 requests, got {total}");

    let success = requests_success(&syslog.metrics);
    assert!(
        success >= 5,
        "Expected at least 5 successful requests, got {success}"
    );

    syslog.shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

// ---------------------------------------------------------------------------
// TCP Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_syslog_tcp_rfc5424() {
    if !has_logger() {
        eprintln!("SKIP: logger binary not found");
        return;
    }

    let syslog = start_syslog_handler(test_config()).await;

    // Send an RFC 5424 message via TCP
    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &syslog.tcp.port().to_string(),
            "--tcp",
            "--rfc5424",
            "--tag",
            "tcp-test",
            "--priority",
            "local0.info",
            "Hello from syslog TCP RFC5424",
        ])
        .output()
        .expect("Failed to run logger");

    assert!(output.status.success(), "logger failed: {:?}", output);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let total = requests_total(&syslog.metrics);
    assert!(total >= 1, "Expected at least 1 request, got {total}");

    let success = requests_success(&syslog.metrics);
    assert!(
        success >= 1,
        "Expected at least 1 successful request, got {success}"
    );

    syslog.shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_syslog_tcp_octet_counting() {
    if !has_logger() {
        eprintln!("SKIP: logger binary not found");
        return;
    }

    let syslog = start_syslog_handler(test_config()).await;

    // Send an RFC 5424 message with octet counting via TCP
    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &syslog.tcp.port().to_string(),
            "--tcp",
            "--rfc5424",
            "--octet-count",
            "--tag",
            "octet-test",
            "Hello from octet-counted syslog",
        ])
        .output()
        .expect("Failed to run logger");

    assert!(output.status.success(), "logger failed: {:?}", output);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let total = requests_total(&syslog.metrics);
    assert!(total >= 1, "Expected at least 1 request, got {total}");

    syslog.shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_syslog_tcp_rfc3164() {
    if !has_logger() {
        eprintln!("SKIP: logger binary not found");
        return;
    }

    let syslog = start_syslog_handler(test_config()).await;

    // Send an RFC 3164 message via TCP
    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &syslog.tcp.port().to_string(),
            "--tcp",
            "--rfc3164",
            "--tag",
            "tcp3164-test",
            "Hello from syslog TCP RFC3164",
        ])
        .output()
        .expect("Failed to run logger");

    assert!(output.status.success(), "logger failed: {:?}", output);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let total = requests_total(&syslog.metrics);
    assert!(total >= 1, "Expected at least 1 request, got {total}");

    syslog.shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

// ---------------------------------------------------------------------------
// Structured Data Tests (RFC 5424)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_syslog_structured_data() {
    if !has_logger() {
        eprintln!("SKIP: logger binary not found");
        return;
    }

    let syslog = start_syslog_handler(test_config()).await;

    // Send RFC 5424 message with structured data via UDP
    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &syslog.udp.port().to_string(),
            "--udp",
            "--rfc5424",
            "--tag",
            "sd-test",
            "--sd-id",
            "mySDID@12345",
            "--sd-param",
            "eventSource=\"myApp\"",
            "--sd-param",
            "eventID=\"1234\"",
            "Structured data test message",
        ])
        .output()
        .expect("Failed to run logger");

    assert!(output.status.success(), "logger failed: {:?}", output);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let total = requests_total(&syslog.metrics);
    assert!(total >= 1, "Expected at least 1 request, got {total}");

    let success = requests_success(&syslog.metrics);
    assert!(
        success >= 1,
        "Expected at least 1 successful request, got {success}"
    );

    syslog.shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

// ---------------------------------------------------------------------------
// Bytes Tracking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_syslog_bytes_received_tracked() {
    if !has_logger() {
        eprintln!("SKIP: logger binary not found");
        return;
    }

    let syslog = start_syslog_handler(test_config()).await;

    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &syslog.udp.port().to_string(),
            "--udp",
            "--rfc5424",
            "--tag",
            "bytes-test",
            "Test message for byte counting",
        ])
        .output()
        .expect("Failed to run logger");

    assert!(output.status.success(), "logger failed: {:?}", output);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let bytes = bytes_received(&syslog.metrics);
    assert!(bytes > 0, "Expected bytes_received > 0, got {bytes}");

    syslog.shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

// ---------------------------------------------------------------------------
// Admission counters
// ---------------------------------------------------------------------------

/// An open syslog TCP connection counts in the active-connection gauge until it closes.
#[tokio::test]
async fn a_syslog_tcp_connection_is_counted_until_it_closes() {
    let syslog = start_syslog_handler(test_config()).await;
    let wait = Duration::from_secs(5);

    let connection = tokio::net::TcpStream::connect(syslog.tcp).await.unwrap();
    assert!(
        crate::common::eventually(wait, || syslog.metrics.get_active_connections() == 1).await,
        "one open connection, counted {}",
        syslog.metrics.get_active_connections()
    );

    drop(connection);
    assert!(
        crate::common::eventually(wait, || syslog.metrics.get_active_connections() == 0).await,
        "closed, counted {}",
        syslog.metrics.get_active_connections()
    );
    syslog.shutdown.cancel();
}

/// A line that is not UTF-8 is taken and counted as a lossy decode: the
/// connection stays open, the next line on it is taken, and a last line the
/// sender closes without a newline is taken too.
#[tokio::test]
async fn a_line_that_is_not_utf8_is_taken_and_counted() {
    use tokio::io::AsyncWriteExt;

    let syslog = start_syslog_handler(test_config()).await;
    let wait = Duration::from_secs(5);

    let mut connection = tokio::net::TcpStream::connect(syslog.tcp).await.unwrap();
    connection
        .write_all(b"<14>Oct 31 10:00:00 host app: caf\xe9\n<14>Oct 31 10:00:01 host app: next\n")
        .await
        .unwrap();
    assert!(
        crate::common::eventually(wait, || requests_success(&syslog.metrics) == 2).await,
        "both lines should be taken: {} taken, {} failed",
        requests_success(&syslog.metrics),
        syslog.metrics.get_requests_error()
    );
    assert_eq!(syslog.metrics.get_requests_error(), 0);
    assert_eq!(syslog.metrics.get_lossy_decodes_total(), 1);
    assert_eq!(
        syslog.metrics.get_active_connections(),
        1,
        "the connection closed"
    );

    connection
        .write_all(b"<14>Oct 31 10:00:02 host app: last, no newline")
        .await
        .unwrap();
    drop(connection);
    assert!(
        crate::common::eventually(wait, || requests_success(&syslog.metrics) == 3).await,
        "the unterminated last line was not taken"
    );
    assert_eq!(syslog.metrics.get_lossy_decodes_total(), 1);
    syslog.shutdown.cancel();
}

/// A datagram and a connection the IP filter refuses each count, and neither
/// reaches the pipeline.
#[tokio::test]
async fn a_syslog_ip_filter_refusal_is_counted() {
    let mut config = test_config();
    config.server.ip_filter.mode = "denylist".to_string();
    config.server.ip_filter.cidrs = vec!["127.0.0.0/8".to_string()];
    let syslog = start_syslog_handler(config).await;

    let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender
        .send_to(b"<14>Sep 25 10:00:00 host app: refused", syslog.udp)
        .await
        .unwrap();
    let _refused = tokio::net::TcpStream::connect(syslog.tcp).await.unwrap();

    assert!(
        crate::common::eventually(Duration::from_secs(5), || {
            syslog.metrics.get_ip_filter_rejected_total() == 2
        })
        .await,
        "a datagram and a connection refused, counted {}",
        syslog.metrics.get_ip_filter_rejected_total()
    );
    syslog.shutdown.cancel();

    assert_eq!(requests_total(&syslog.metrics), 0);
    assert_eq!(syslog.metrics.get_active_connections(), 0);
}

/// An unbound syslog listener delivers every line as before, over UDP and TCP,
/// each stamped `_source: syslog` and the default org. TLS reads through the
/// same connection handler as plain TCP.
#[tokio::test]
async fn an_unbound_syslog_listener_delivers_into_the_default_org() {
    use scalo::transport::TransportReceiver;
    use tokio::io::AsyncWriteExt;

    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let mut config = test_config();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint);
    config.routing.dlq.enabled = false;
    let syslog = start_syslog_handler(config).await;

    let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender
        .send_to(b"<14>Sep 25 10:00:00 host app: over udp", syslog.udp)
        .await
        .unwrap();
    let mut tcp = tokio::net::TcpStream::connect(syslog.tcp).await.unwrap();
    tcp.write_all(b"<14>Sep 25 10:00:01 host app: over tcp\n")
        .await
        .unwrap();
    tcp.flush().await.unwrap();

    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while received.len() < 2 && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = loader.recv(2).await {
            received.extend(batch.records.into_iter().map(|r| r.payload));
        }
    }
    drop(tcp);
    syslog.shutdown.cancel();

    assert_eq!(received.len(), 2, "both lines reach the loader");
    for record in &received {
        let record: serde_json::Value = serde_json::from_slice(record).unwrap();
        assert_eq!(record["_source"], "syslog", "{record}");
        assert_eq!(record["_org_id"], "default", "{record}");
        assert!(record.get("_source_original").is_none(), "{record}");
    }
}

/// A message with an invalid byte in its header or its body lands over UDP and
/// TCP, parsed, with the byte replaced by U+FFFD. TLS reads through the same
/// connection handler as plain TCP.
#[tokio::test]
async fn a_message_that_is_not_utf8_lands_parsed_over_udp_and_tcp() {
    use scalo::transport::TransportReceiver;
    use tokio::io::AsyncWriteExt;

    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let mut config = test_config();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint);
    config.routing.dlq.enabled = false;
    let syslog = start_syslog_handler(config).await;

    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut tcp = tokio::net::TcpStream::connect(syslog.tcp).await.unwrap();

    // (transport, wire bytes, field the replacement character must land in, expected value)
    let cases: [(&str, &[u8], &str, &str); 4] = [
        (
            "udp",
            b"<14>Oct 31 10:00:00 ho\xffst udpheader: body",
            "hostname",
            "ho\u{fffd}st",
        ),
        (
            "udp",
            b"<14>Oct 31 10:00:00 udpbody app: caf\xe9 au lait",
            "message",
            "caf\u{fffd} au lait",
        ),
        (
            "tcp",
            b"<14>Oct 31 10:00:00 ho\xffst tcpheader: body\n",
            "hostname",
            "ho\u{fffd}st",
        ),
        (
            "tcp",
            b"<14>Oct 31 10:00:00 tcpbody app: caf\xe9 au lait\n",
            "message",
            "caf\u{fffd} au lait",
        ),
    ];
    for (transport, wire, _, _) in &cases {
        if *transport == "udp" {
            udp.send_to(wire, syslog.udp).await.unwrap();
        } else {
            tcp.write_all(wire).await.unwrap();
        }
    }
    tcp.flush().await.unwrap();

    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while received.len() < cases.len() && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = loader.recv(cases.len()).await {
            received.extend(batch.records.into_iter().map(|r| r.payload));
        }
    }
    drop(tcp);
    syslog.shutdown.cancel();

    assert_eq!(
        received.len(),
        cases.len(),
        "every message reaches the loader"
    );
    let records: Vec<serde_json::Value> = received
        .iter()
        .map(|payload| serde_json::from_slice(payload).unwrap())
        .collect();
    for (transport, _, field, expected) in &cases {
        let found = records.iter().any(|record| record[field] == *expected);
        assert!(
            found,
            "{transport}: no record with {field} = {expected:?} in {records:?}"
        );
    }
    assert_eq!(syslog.metrics.get_lossy_decodes_total(), 4);
    assert_eq!(syslog.metrics.get_requests_error(), 0);
}

/// Over TCP an oversize message costs itself and nothing after it, a line
/// opening with digits is a line, and a stamp with no zone takes
/// `syslog.timezone`. Over UDP a burst larger than a batch arrives whole.
#[tokio::test]
async fn syslog_carries_on_past_oversize_messages_and_reads_bursts_whole() {
    use tokio::io::AsyncWriteExt;

    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let mut config = test_config();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint);
    config.routing.dlq.enabled = false;
    config.syslog.timezone = "+10:00".parse().unwrap();
    config.syslog.udp_batch_size = 16;
    let syslog = start_syslog_handler(config).await;

    let second = "<14>Oct 31 10:00:00 h app: second";
    let mut wire = String::from("<14>Oct 31 10:00:00 h app: first\n");
    wire.push_str(&format!("70000 <14>{}", "a".repeat(70_000 - 4)));
    wire.push_str(&format!("{} {second}", second.len()));
    wire.push_str("2026 Oct 31 10:00:00 h app: third\n");
    wire.push_str(&format!("<14>{}\n", "b".repeat(70_000)));
    wire.push_str("<14>Oct 31 10:00:00 h app: fourth\n");
    let mut tcp = tokio::net::TcpStream::connect(syslog.tcp).await.unwrap();
    tcp.write_all(wire.as_bytes()).await.unwrap();
    tcp.flush().await.unwrap();

    let records = receive_records(&loader, 4, Duration::from_secs(10)).await;
    let messages: Vec<&str> = records
        .iter()
        .map(|r| r["message"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        messages,
        [
            "first",
            "second",
            "2026 Oct 31 10:00:00 h app: third",
            "fourth"
        ],
        "the TCP connection carries on past both oversize messages"
    );
    let stamp = records[0]["timestamp"].as_str().unwrap_or_default();
    assert!(
        stamp.ends_with("-10-31T10:00:00+10:00"),
        "timestamp {stamp}"
    );
    assert_eq!(
        syslog.metrics.get_records_dropped(),
        2,
        "both oversize messages counted"
    );

    // The connection is still open: another line still lands.
    tcp.write_all(b"<14>Oct 31 10:00:00 h app: fifth\n")
        .await
        .unwrap();
    let after = receive_records(&loader, 1, Duration::from_secs(10)).await;
    assert_eq!(after.len(), 1, "a line after the oversize ones arrives");

    let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let burst = 200;
    for i in 0..burst {
        udp.send_to(
            format!("<14>Oct 31 10:00:00 h app: udp {i}").as_bytes(),
            syslog.udp,
        )
        .unwrap();
    }
    let datagrams = receive_records(&loader, burst, Duration::from_secs(10)).await;
    assert_eq!(
        datagrams.len(),
        burst,
        "every datagram of the burst arrives"
    );

    drop(tcp);
    syslog.shutdown.cancel();
}

/// A connection that sends nothing for `syslog.idle_timeout_ms` is closed.
#[tokio::test]
async fn an_idle_syslog_tcp_connection_is_closed() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut config = test_config();
    config.syslog.idle_timeout_ms = 300;
    let syslog = start_syslog_handler(config).await;

    let mut tcp = tokio::net::TcpStream::connect(syslog.tcp).await.unwrap();
    tcp.write_all(b"<14>Oct 31 10:00:00 h app: one\n")
        .await
        .unwrap();
    let mut buf = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(5), tcp.read(&mut buf)).await;
    assert!(
        matches!(read, Ok(Ok(0))),
        "the receiver closes an idle connection, got {read:?}"
    );
    syslog.shutdown.cancel();
}
