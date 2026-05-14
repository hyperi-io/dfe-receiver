// Project:   dfe-receiver
// File:      tests/integration_lumberjack.rs
// Purpose:   Integration tests using Filebeat binary (Lumberjack v2 protocol)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::collapsible_if)]
#![allow(clippy::similar_names)]

//! Integration tests that use the Filebeat binary.
//!
//! These tests start a dfe-receiver Lumberjack handler and run Filebeat as a
//! subprocess configured to send events over the Lumberjack v2 protocol.
//! The Filebeat binary is auto-downloaded and cached by `scripts/fetch-filebeat.sh`.
//!
//! Run with: `cargo test --test integration_lumberjack`
//!
//! Requirements:
//! - `gh` or `jq` + `curl` (for auto-downloading Filebeat)

// Allow unwrap/expect in tests - they're the idiomatic way to fail fast
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
use dfe_receiver::server::lumberjack::LumberjackHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use tokio_util::sync::CancellationToken;

/// Resolve the path to the Filebeat binary (cached via fetch-filebeat.sh or system PATH).
///
/// Runs the fetch script once per test binary via `OnceLock`. If the script fails
/// (offline, no `jq`, etc.), falls back to `filebeat` in PATH.
fn filebeat_binary_path() -> Option<&'static PathBuf> {
    static FILEBEAT_BIN: OnceLock<Option<PathBuf>> = OnceLock::new();

    FILEBEAT_BIN
        .get_or_init(|| {
            let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
            let fetch_script = repo_root.join("scripts/fetch-filebeat.sh");

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
                        eprintln!("fetch-filebeat.sh failed: {stderr}");
                    }
                }
            }

            // Fall back to system PATH
            Command::new("filebeat")
                .arg("version")
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|_| PathBuf::from("filebeat"))
        })
        .as_ref()
}

/// Get a random port for testing.
fn random_port() -> u16 {
    10000 + (uuid::Uuid::new_v4().as_u128() % 10000) as u16
}

/// Create a minimal config for testing with Lumberjack enabled.
fn test_config(lumberjack_port: u16) -> Config {
    let mut config = Config::default();
    // HTTP server still needs a bind address (always enabled)
    let http_port = random_port();
    config.server.bind_address = format!("127.0.0.1:{http_port}");
    config.server.auth.mode = "none".to_string();
    // Enable Lumberjack
    config.lumberjack.enabled = true;
    config.lumberjack.bind_address = format!("127.0.0.1:{lumberjack_port}");
    // Use loader destination to avoid Kafka dependency
    config.destinations.default = "loader".to_string();
    config
}

/// Start the Lumberjack handler and return (shutdown_token, metrics).
async fn start_lumberjack_handler(config: Config) -> (CancellationToken, Arc<Metrics>) {
    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), tokio_util::sync::CancellationToken::new())
            .await
            .expect("Failed to create pipeline"),
    );

    let handler = LumberjackHandler::new(config.lumberjack.clone(), pipeline, metrics.clone());

    let handler_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = handler.start(handler_shutdown).await;
    });

    // Wait for handler to start listening
    tokio::time::sleep(Duration::from_millis(300)).await;
    (shutdown, metrics)
}

/// Write a Filebeat YAML config to a file.
///
/// Filebeat requires config files to be owner-writable only (chmod go-w).
fn write_filebeat_config(path: &Path, config_yaml: &str) {
    let mut file = std::fs::File::create(path).expect("Failed to create Filebeat config file");
    file.write_all(config_yaml.as_bytes())
        .expect("Failed to write Filebeat config");
    file.flush().expect("Failed to flush Filebeat config");

    // Filebeat refuses to load configs writable by group/others
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("Failed to set config file permissions");
    }
}

/// Write test log lines to a file.
fn write_test_log_file(path: &Path, count: usize) {
    let mut file = std::fs::File::create(path).expect("Failed to create test log file");
    for i in 0..count {
        writeln!(
            file,
            r#"{{"message":"test event {i}","sequence":{i},"source":"integration_test"}}"#
        )
        .expect("Failed to write test event");
    }
    file.flush().expect("Failed to flush test log file");
}

/// Run Filebeat as an async subprocess with timeout.
///
/// Filebeat doesn't cleanly exit after sending all events from a file,
/// so we use a timeout and treat timeout-after-success as acceptable.
/// Returns (completed_normally, stderr_output).
async fn run_filebeat_async(
    filebeat_bin: &Path,
    config_path: &Path,
    timeout_secs: u64,
) -> (bool, String) {
    let child = tokio::process::Command::new(filebeat_bin)
        .arg("-c")
        .arg(config_path)
        .arg("-e")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to spawn filebeat binary");

    let child_id = child.id();

    let result =
        tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait_with_output()).await;

    match result {
        Ok(Ok(output)) => {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            (output.status.success(), stderr)
        }
        Ok(Err(e)) => {
            panic!("Failed to wait for filebeat process: {e}");
        }
        Err(_) => {
            // Timeout — Filebeat often doesn't exit after --once with file input.
            // Kill the process and treat this as acceptable if events were sent.
            if let Some(pid) = child_id {
                let _ = Command::new("kill").arg(pid.to_string()).output();
                // Give it a moment to clean up
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            // Return true since timeout after processing is expected behavior
            (true, String::from("(timed out — expected for file input)"))
        }
    }
}

/// Get total events received from metrics.
fn events_received(metrics: &Metrics) -> u64 {
    metrics.get_requests_total()
}

// =============================================================================
// Filebeat → Lumberjack Plaintext Test
// =============================================================================

/// Test Filebeat sending events to the receiver over plain TCP (Lumberjack v2).
///
/// Starts a dfe-receiver Lumberjack handler, writes test log lines to a file,
/// runs Filebeat configured to read the file and output to our handler,
/// and verifies events were received.
#[tokio::test]
async fn test_filebeat_lumberjack_plaintext() {
    let filebeat_bin = if let Some(path) = filebeat_binary_path() {
        path
    } else {
        eprintln!("Skipping test: filebeat binary not available");
        return;
    };

    let port = random_port();
    let config = test_config(port);
    let (shutdown, metrics) = start_lumberjack_handler(config).await;

    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let log_file = tmp_dir.path().join("test.log");
    let config_path = tmp_dir.path().join("filebeat.yml");
    let data_dir = tmp_dir.path().join("filebeat-data");
    let data_dir_str = data_dir.to_str().unwrap();
    let log_file_str = log_file.to_str().unwrap();

    // Write test log lines (enough to exceed Filebeat's 1024-byte fingerprint threshold)
    write_test_log_file(&log_file, 20);

    let filebeat_config = format!(
        r#"
filebeat.inputs:
  - type: filestream
    id: test-input
    enabled: true
    paths:
      - "{log_file_str}"
    prospector:
      scanner:
        fingerprint:
          enabled: false
    close:
      on_state_change:
        inactive: 2s
    parsers:
      - ndjson:
          keys_under_root: true

output.logstash:
  hosts: ["127.0.0.1:{port}"]

path.data: "{data_dir_str}"
path.logs: "{data_dir_str}"

# Disable unnecessary features for testing
setup.template.enabled: false
setup.ilm.enabled: false
logging.level: warning
"#
    );

    write_filebeat_config(&config_path, &filebeat_config);

    // Run Filebeat in the background — it won't exit on its own with filestream input.
    // We poll metrics until events arrive, then kill Filebeat.
    let (filebeat_success, stderr) = run_filebeat_async(filebeat_bin, &config_path, 15).await;

    let received = events_received(&metrics);
    eprintln!("Events received by handler: {received}");

    if !filebeat_success {
        // Check for critical errors (ignore harmless warnings)
        let critical_errors: Vec<&str> = stderr
            .lines()
            .filter(|l| {
                l.contains("ERR")
                    && !l.contains("module")
                    && !l.contains("connection refused")
                    && !l.contains("ingest pipeline")
            })
            .collect();

        if !critical_errors.is_empty() {
            eprintln!("Filebeat critical errors:\n{}", critical_errors.join("\n"));
        }
    }

    // We should have received events (Filebeat sends them as Lumberjack v2 frames)
    assert!(
        received > 0,
        "Expected to receive events from Filebeat, but got 0. Stderr:\n{stderr}"
    );

    shutdown.cancel();
}
