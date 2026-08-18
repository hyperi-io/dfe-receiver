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

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::syslog::SyslogHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use tokio_util::sync::CancellationToken;

/// Check if logger binary is available and supports --server flag.
fn has_logger() -> bool {
    Command::new("logger")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Get a random port for testing (use high ports to avoid privilege issues).
fn random_port() -> u16 {
    10000 + (uuid::Uuid::new_v4().as_u128() % 10000) as u16
}

/// Create a minimal config for testing with syslog enabled.
fn test_config(udp_port: u16, tcp_port: u16) -> Config {
    let mut config = Config::default();
    // HTTP server still needs a bind address (always enabled)
    let http_port = random_port();
    config.server.bind_address = format!("127.0.0.1:{http_port}");
    config.server.auth.mode = "none".to_string();
    // Enable syslog on high ports
    config.syslog.enabled = true;
    config.syslog.udp_bind_address = format!("127.0.0.1:{udp_port}");
    config.syslog.tcp_bind_address = format!("127.0.0.1:{tcp_port}");
    // Disable TLS for tests
    config.syslog.tls.enabled = false;
    // Use loader destination to avoid Kafka dependency
    config.destinations.default = "loader".to_string();
    config
}

/// Start the syslog handler and return (shutdown_token, metrics).
async fn start_syslog_handler(config: Config) -> (CancellationToken, Arc<Metrics>) {
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

    let handler_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = handler.start(handler_shutdown).await;
    });

    // Wait for handler to start listening
    tokio::time::sleep(Duration::from_millis(500)).await;
    (shutdown, metrics)
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

    let udp_port = random_port();
    let tcp_port = random_port();
    let config = test_config(udp_port, tcp_port);
    let (shutdown, metrics) = start_syslog_handler(config).await;

    // Send an RFC 5424 message via UDP
    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &udp_port.to_string(),
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

    let total = requests_total(&metrics);
    assert!(total >= 1, "Expected at least 1 request, got {total}");

    let success = requests_success(&metrics);
    assert!(
        success >= 1,
        "Expected at least 1 successful request, got {success}"
    );

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_syslog_udp_rfc3164() {
    if !has_logger() {
        eprintln!("SKIP: logger binary not found");
        return;
    }

    let udp_port = random_port();
    let tcp_port = random_port();
    let config = test_config(udp_port, tcp_port);
    let (shutdown, metrics) = start_syslog_handler(config).await;

    // Send an RFC 3164 message via UDP
    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &udp_port.to_string(),
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

    let total = requests_total(&metrics);
    assert!(total >= 1, "Expected at least 1 request, got {total}");

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_syslog_udp_multiple_messages() {
    if !has_logger() {
        eprintln!("SKIP: logger binary not found");
        return;
    }

    let udp_port = random_port();
    let tcp_port = random_port();
    let config = test_config(udp_port, tcp_port);
    let (shutdown, metrics) = start_syslog_handler(config).await;

    // Send 5 messages
    for i in 0..5 {
        let output = Command::new("logger")
            .args([
                "--server",
                "127.0.0.1",
                "--port",
                &udp_port.to_string(),
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

    let total = requests_total(&metrics);
    assert!(total >= 5, "Expected at least 5 requests, got {total}");

    let success = requests_success(&metrics);
    assert!(
        success >= 5,
        "Expected at least 5 successful requests, got {success}"
    );

    shutdown.cancel();
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

    let udp_port = random_port();
    let tcp_port = random_port();
    let config = test_config(udp_port, tcp_port);
    let (shutdown, metrics) = start_syslog_handler(config).await;

    // Send an RFC 5424 message via TCP
    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &tcp_port.to_string(),
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

    let total = requests_total(&metrics);
    assert!(total >= 1, "Expected at least 1 request, got {total}");

    let success = requests_success(&metrics);
    assert!(
        success >= 1,
        "Expected at least 1 successful request, got {success}"
    );

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_syslog_tcp_octet_counting() {
    if !has_logger() {
        eprintln!("SKIP: logger binary not found");
        return;
    }

    let udp_port = random_port();
    let tcp_port = random_port();
    let config = test_config(udp_port, tcp_port);
    let (shutdown, metrics) = start_syslog_handler(config).await;

    // Send an RFC 5424 message with octet counting via TCP
    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &tcp_port.to_string(),
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

    let total = requests_total(&metrics);
    assert!(total >= 1, "Expected at least 1 request, got {total}");

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_syslog_tcp_rfc3164() {
    if !has_logger() {
        eprintln!("SKIP: logger binary not found");
        return;
    }

    let udp_port = random_port();
    let tcp_port = random_port();
    let config = test_config(udp_port, tcp_port);
    let (shutdown, metrics) = start_syslog_handler(config).await;

    // Send an RFC 3164 message via TCP
    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &tcp_port.to_string(),
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

    let total = requests_total(&metrics);
    assert!(total >= 1, "Expected at least 1 request, got {total}");

    shutdown.cancel();
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

    let udp_port = random_port();
    let tcp_port = random_port();
    let config = test_config(udp_port, tcp_port);
    let (shutdown, metrics) = start_syslog_handler(config).await;

    // Send RFC 5424 message with structured data via UDP
    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &udp_port.to_string(),
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

    let total = requests_total(&metrics);
    assert!(total >= 1, "Expected at least 1 request, got {total}");

    let success = requests_success(&metrics);
    assert!(
        success >= 1,
        "Expected at least 1 successful request, got {success}"
    );

    shutdown.cancel();
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

    let udp_port = random_port();
    let tcp_port = random_port();
    let config = test_config(udp_port, tcp_port);
    let (shutdown, metrics) = start_syslog_handler(config).await;

    let output = Command::new("logger")
        .args([
            "--server",
            "127.0.0.1",
            "--port",
            &udp_port.to_string(),
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

    let bytes = bytes_received(&metrics);
    assert!(bytes > 0, "Expected bytes_received > 0, got {bytes}");

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
}
