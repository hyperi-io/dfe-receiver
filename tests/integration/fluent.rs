// Project:   dfe-receiver
// File:      tests/integration_fluent.rs
// Purpose:   Integration tests using Fluent Bit binary (Forward protocol)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::collapsible_if)]

//! Integration tests that use the Fluent Bit binary.
//!
//! These tests start a dfe-receiver Fluent Forward handler and run Fluent Bit
//! as a subprocess configured to send events via the Forward protocol
//! (msgpack over TCP). The Fluent Bit binary is auto-downloaded and cached
//! by `scripts/fetch-fluent-bit.sh` on first run.
//!
//! Run with: `cargo test --test integration_fluent`
//!
//! Requirements:
//! - `gh` or `jq` + `curl` (for auto-downloading Fluent Bit)
//! - `dpkg-deb` (for extracting the .deb package)

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::fluent::FluentHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use tokio_util::sync::CancellationToken;

/// Resolve the path to the Fluent Bit binary (cached via fetch-fluent-bit.sh or system PATH).
fn fluent_bit_binary_path() -> Option<&'static PathBuf> {
    static FLUENT_BIT_BIN: OnceLock<Option<PathBuf>> = OnceLock::new();

    FLUENT_BIT_BIN
        .get_or_init(|| {
            let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
            let fetch_script = repo_root.join("scripts/fetch-fluent-bit.sh");

            if fetch_script.exists() {
                if let Ok(output) = Command::new("bash").arg(&fetch_script).output() {
                    if output.status.success() {
                        let path = String::from_utf8_lossy(&output.stdout)
                            .trim()
                            .lines()
                            .last()
                            .unwrap_or("")
                            .to_string();
                        let binary = PathBuf::from(&path);
                        if binary.exists() {
                            return Some(binary);
                        }
                    } else {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        eprintln!("fetch-fluent-bit.sh failed: {stderr}");
                    }
                }
            }

            // Fall back to system PATH
            Command::new("fluent-bit")
                .arg("--version")
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|_| PathBuf::from("fluent-bit"))
        })
        .as_ref()
}

fn random_port() -> u16 {
    10000 + (uuid::Uuid::new_v4().as_u128() % 10000) as u16
}

fn test_config(fluent_port: u16) -> Config {
    let mut config = Config::default();
    let http_port = random_port();
    config.server.bind_address = format!("127.0.0.1:{http_port}");
    config.server.auth.mode = "none".to_string();
    config.fluent.enabled = true;
    config.fluent.bind_address = format!("127.0.0.1:{fluent_port}");
    config.fluent.tls.enabled = false;
    config.destinations.default = "loader".to_string();
    config
}

async fn start_fluent_handler(config: Config) -> (CancellationToken, Arc<Metrics>) {
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

    let handler_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = handler.start(handler_shutdown).await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
    (shutdown, metrics)
}

fn requests_total(metrics: &Metrics) -> u64 {
    metrics.get_requests_total()
}

fn requests_success(metrics: &Metrics) -> u64 {
    metrics.get_requests_success()
}

fn bytes_received(metrics: &Metrics) -> u64 {
    metrics.get_bytes_received()
}

/// Write a Fluent Bit config to a file.
fn write_fluent_bit_config(path: &Path, config: &str) {
    let mut file = std::fs::File::create(path).expect("Failed to create Fluent Bit config file");
    file.write_all(config.as_bytes())
        .expect("Failed to write Fluent Bit config");
    file.flush().expect("Failed to flush Fluent Bit config");
}

/// Spawn Fluent Bit, wait for data to arrive at the receiver, then kill it.
///
/// Fluent Bit does not auto-exit after dummy samples are sent (unlike Vector),
/// so we poll the metrics and kill the process once data is confirmed received.
/// Returns (stderr_output).
async fn run_fluent_bit_and_wait(
    fluent_bit_bin: &Path,
    config_path: &Path,
    metrics: &Metrics,
    timeout_secs: u64,
) -> String {
    let mut child = tokio::process::Command::new(fluent_bit_bin)
        .arg("--config")
        .arg(config_path)
        .arg("--quiet")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to spawn fluent-bit binary");

    // Poll until we see data received or timeout
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if tokio::time::Instant::now() > deadline {
            let _ = child.kill().await;
            panic!("Fluent Bit: no data received within {timeout_secs}s");
        }

        if requests_total(metrics) >= 1 {
            // Data received — give a brief grace period for remaining in-flight data
            tokio::time::sleep(Duration::from_millis(500)).await;
            break;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Kill fluent-bit gracefully (SIGTERM)
    let _ = child.kill().await;
    let output = child
        .wait_with_output()
        .await
        .expect("Failed to collect fluent-bit output");
    String::from_utf8_lossy(&output.stderr).to_string()
}

/// Assert no error lines appear in Fluent Bit's stderr output.
fn assert_no_fluent_bit_errors(stderr: &str) {
    let error_lines: Vec<&str> = stderr
        .lines()
        .filter(|l| l.contains("[error]") || l.contains("[ error]"))
        .collect();

    assert!(
        error_lines.is_empty(),
        "Fluent Bit produced errors:\n{}",
        error_lines.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test Fluent Bit sending events via Forward protocol.
///
/// Starts a dfe-receiver Forward handler, runs Fluent Bit with `dummy` input
/// and `forward` output, and verifies events are received without errors.
#[tokio::test]
async fn test_fluent_bit_forward_output() {
    let fluent_bit_bin = if let Some(path) = fluent_bit_binary_path() {
        path
    } else {
        eprintln!("Skipping test: fluent-bit binary not available");
        return;
    };

    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics) = start_fluent_handler(config).await;

    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = tmp_dir.path().join("fluent-bit.yaml");

    let fb_config = format!(
        r#"
service:
  flush: 1
  grace: 2
  daemon: off
  log_level: warn

pipeline:
  inputs:
    - name: dummy
      tag: test.forward
      dummy: '{{"message":"hello from fluent-bit","level":"info"}}'
      samples: 10
      rate: 10

  outputs:
    - name: forward
      match: "*"
      host: 127.0.0.1
      port: {port}
"#
    );

    write_fluent_bit_config(&config_path, &fb_config);

    let stderr = run_fluent_bit_and_wait(fluent_bit_bin, &config_path, &metrics, 15).await;

    assert_no_fluent_bit_errors(&stderr);
    assert!(
        requests_total(&metrics) >= 1,
        "should have received at least 1 request, got {}",
        requests_total(&metrics)
    );
    assert!(
        requests_success(&metrics) >= 1,
        "should have at least 1 success, got {}",
        requests_success(&metrics)
    );
    assert!(
        bytes_received(&metrics) > 0,
        "should have tracked bytes received"
    );

    shutdown.cancel();
}

/// Test Fluent Bit sending multiple batches via Forward protocol.
///
/// Uses a higher sample count to verify multi-batch delivery works correctly.
#[tokio::test]
async fn test_fluent_bit_forward_multiple_batches() {
    let fluent_bit_bin = if let Some(path) = fluent_bit_binary_path() {
        path
    } else {
        eprintln!("Skipping test: fluent-bit binary not available");
        return;
    };

    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics) = start_fluent_handler(config).await;

    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = tmp_dir.path().join("fluent-bit.yaml");

    let fb_config = format!(
        r#"
service:
  flush: 1
  grace: 2
  daemon: off
  log_level: warn

pipeline:
  inputs:
    - name: dummy
      tag: test.batch
      dummy: '{{"msg":"batch test","count":1}}'
      samples: 50
      rate: 50

  outputs:
    - name: forward
      match: "*"
      host: 127.0.0.1
      port: {port}
"#
    );

    write_fluent_bit_config(&config_path, &fb_config);

    let stderr = run_fluent_bit_and_wait(fluent_bit_bin, &config_path, &metrics, 15).await;

    assert_no_fluent_bit_errors(&stderr);
    assert!(
        requests_total(&metrics) >= 1,
        "should have received requests, got {}",
        requests_total(&metrics)
    );

    shutdown.cancel();
}

/// Test Fluent Bit Forward with chunk ACK enabled (require_ack_response).
///
/// Verifies the receiver correctly responds to Fluent Forward ACK requests.
#[tokio::test]
async fn test_fluent_bit_forward_with_ack() {
    let fluent_bit_bin = if let Some(path) = fluent_bit_binary_path() {
        path
    } else {
        eprintln!("Skipping test: fluent-bit binary not available");
        return;
    };

    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics) = start_fluent_handler(config).await;

    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = tmp_dir.path().join("fluent-bit.yaml");

    let fb_config = format!(
        r#"
service:
  flush: 1
  grace: 2
  daemon: off
  log_level: warn

pipeline:
  inputs:
    - name: dummy
      tag: test.ack
      dummy: '{{"message":"ack test"}}'
      samples: 5
      rate: 5

  outputs:
    - name: forward
      match: "*"
      host: 127.0.0.1
      port: {port}
      require_ack_response: true
"#
    );

    write_fluent_bit_config(&config_path, &fb_config);

    let stderr = run_fluent_bit_and_wait(fluent_bit_bin, &config_path, &metrics, 15).await;

    // If ACK handling is broken, fluent-bit would report errors or retry
    assert_no_fluent_bit_errors(&stderr);
    assert!(
        requests_success(&metrics) >= 1,
        "should have successful requests with ACK, got {}",
        requests_success(&metrics)
    );

    shutdown.cancel();
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

    let fluent_port = random_port();
    let mut config = test_config(fluent_port);
    // Small limit to make the test fast and predictable
    config.fluent.max_message_size = 64 * 1024; // 64 KiB

    let (shutdown, _metrics) = start_fluent_handler(config).await;

    let mut stream = TcpStream::connect(("127.0.0.1", fluent_port))
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
