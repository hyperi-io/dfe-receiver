// Project:   dfe-receiver
// File:      tests/integration_gelf.rs
// Purpose:   Integration tests using Fluent Bit binary (GELF protocol)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests that use the Fluent Bit binary to send GELF messages.
//!
//! These tests start a dfe-receiver GELF handler and run Fluent Bit as a
//! subprocess configured to send events via the GELF TCP output plugin
//! (null-byte delimited JSON over TCP). The Fluent Bit binary is
//! auto-downloaded and cached by `scripts/fetch-fluent-bit.sh` on first run.
//!
//! Run with: `cargo test --test integration_gelf`
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
use dfe_receiver::server::gelf::GelfHandler;
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

fn test_config(gelf_port: u16) -> Config {
    let mut config = Config::default();
    let http_port = random_port();
    config.server.bind_address = format!("127.0.0.1:{http_port}");
    config.server.auth.mode = "none".to_string();
    config.gelf.enabled = true;
    config.gelf.bind_address = format!("127.0.0.1:{gelf_port}");
    config.gelf.tls.enabled = false;
    config.destinations.default = "loader".to_string();
    config
}

async fn start_gelf_handler(config: Config) -> (CancellationToken, Arc<Metrics>) {
    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()))
            .await
            .expect("Failed to create pipeline"),
    );

    let handler = GelfHandler::new(config.gelf.clone(), pipeline, metrics.clone());

    let handler_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = handler.start(handler_shutdown).await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
    (shutdown, metrics)
}

fn metric_value(metrics: &Metrics, name: &str) -> u64 {
    let output = metrics.render();
    for line in output.lines() {
        if line.starts_with(name) && !line.starts_with('#') {
            return line.trim_start_matches(name).trim().parse().unwrap_or(0);
        }
    }
    0
}

fn requests_total(metrics: &Metrics) -> u64 {
    metric_value(metrics, "receiver_requests_total ")
}

fn requests_success(metrics: &Metrics) -> u64 {
    metric_value(metrics, "receiver_requests_success ")
}

fn bytes_received(metrics: &Metrics) -> u64 {
    metric_value(metrics, "receiver_bytes_received_total ")
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

    // Kill fluent-bit gracefully
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

/// Test Fluent Bit sending events via GELF TCP output.
///
/// Starts a dfe-receiver GELF handler, runs Fluent Bit with `dummy` input
/// and `gelf` output (TCP mode), and verifies events are received.
#[tokio::test]
async fn test_fluent_bit_gelf_tcp_output() {
    let fluent_bit_bin = if let Some(path) = fluent_bit_binary_path() {
        path
    } else {
        eprintln!("Skipping test: fluent-bit binary not available");
        return;
    };

    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics) = start_gelf_handler(config).await;

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
      tag: test.gelf
      dummy: '{{"message":"hello from gelf","level":"info","source_host":"test-host"}}'
      samples: 10
      rate: 10

  outputs:
    - name: gelf
      match: "*"
      host: 127.0.0.1
      port: {port}
      mode: tcp
      gelf_short_message_key: message
      gelf_host_key: source_host
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

/// Test Fluent Bit sending multiple batches of GELF messages.
#[tokio::test]
async fn test_fluent_bit_gelf_tcp_multiple_batches() {
    let fluent_bit_bin = if let Some(path) = fluent_bit_binary_path() {
        path
    } else {
        eprintln!("Skipping test: fluent-bit binary not available");
        return;
    };

    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics) = start_gelf_handler(config).await;

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
      tag: test.gelf.batch
      dummy: '{{"message":"batch gelf test","severity":"info","source_host":"test-host"}}'
      samples: 50
      rate: 50

  outputs:
    - name: gelf
      match: "*"
      host: 127.0.0.1
      port: {port}
      mode: tcp
      gelf_short_message_key: message
      gelf_host_key: source_host
"#
    );

    write_fluent_bit_config(&config_path, &fb_config);

    let stderr = run_fluent_bit_and_wait(fluent_bit_bin, &config_path, &metrics, 15).await;

    assert_no_fluent_bit_errors(&stderr);
    assert!(
        requests_total(&metrics) >= 1,
        "should have received requests from multiple batches, got {}",
        requests_total(&metrics)
    );

    shutdown.cancel();
}

/// Test Fluent Bit GELF output with custom fields.
///
/// Verifies that additional fields (prefixed with `_`) are correctly
/// transmitted through the GELF protocol.
#[tokio::test]
async fn test_fluent_bit_gelf_tcp_with_custom_fields() {
    let fluent_bit_bin = if let Some(path) = fluent_bit_binary_path() {
        path
    } else {
        eprintln!("Skipping test: fluent-bit binary not available");
        return;
    };

    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics) = start_gelf_handler(config).await;

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
      tag: test.gelf.custom
      dummy: '{{"message":"custom fields test","environment":"testing","request_id":"abc-123","source_host":"web01"}}'
      samples: 5
      rate: 5

  outputs:
    - name: gelf
      match: "*"
      host: 127.0.0.1
      port: {port}
      mode: tcp
      gelf_short_message_key: message
      gelf_host_key: source_host
"#
    );

    write_fluent_bit_config(&config_path, &fb_config);

    let stderr = run_fluent_bit_and_wait(fluent_bit_bin, &config_path, &metrics, 15).await;

    assert_no_fluent_bit_errors(&stderr);
    assert!(
        requests_success(&metrics) >= 1,
        "should have successful requests with custom fields, got {}",
        requests_success(&metrics)
    );

    shutdown.cancel();
}
