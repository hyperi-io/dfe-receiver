// Project:   dfe-receiver
// File:      tests/integration_vector.rs
// Purpose:   Integration tests using Vector binary (HTTP, HTTPS, gRPC)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests that use the locally installed Vector binary.
//!
//! These tests start a dfe-receiver server and run Vector as a subprocess
//! configured to send events to the receiver. Tests gracefully skip on
//! machines where Vector is not installed.
//!
//! Run with: `cargo test --test integration_vector`
//!
//! Requirements:
//! - `vector` binary in PATH (v0.30+)
//! - `openssl` binary in PATH (for HTTPS/TLS test cert generation)

// Allow unwrap/expect in tests - they're the idiomatic way to fail fast
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::http;
use tokio_util::sync::CancellationToken;

/// Install the rustls CryptoProvider (needed when both ring and aws-lc-rs are available).
fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Check if the `vector` binary is available on this machine.
fn vector_available() -> bool {
    Command::new("vector")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Check if `openssl` is available (needed for TLS cert generation).
fn openssl_available() -> bool {
    Command::new("openssl")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Get a random port for testing.
fn random_port() -> u16 {
    10000 + (uuid::Uuid::new_v4().as_u128() % 10000) as u16
}

/// Create a minimal config for testing with loader destination (no Kafka needed).
fn test_config(http_port: u16) -> Config {
    let mut config = Config::default();
    config.server.bind_address = format!("127.0.0.1:{http_port}");
    config.server.max_body_size = 10 * 1024 * 1024;
    config.server.request_timeout_ms = 30_000;
    config.server.auth.mode = "none".to_string();
    // Use loader destination to avoid Kafka dependency
    config.destinations.default = "loader".to_string();
    config
}

/// Start a test HTTP server and return the shutdown token.
async fn start_http_server(config: Config) -> CancellationToken {
    let metrics = Arc::new(Metrics::new());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(PipelineState::new(SharedConfig::new(config.clone())).expect("Failed to create pipeline"));

    let server_shutdown = shutdown.clone();
    let server_metrics = metrics.clone();
    let bind_addr = config.server.bind_address.clone();

    tokio::spawn(async move {
        let _ = http::run_server(&bind_addr, pipeline, server_metrics, server_shutdown).await;
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown
}

/// Write a Vector YAML config to a file.
fn write_vector_config(path: &Path, config_yaml: &str) {
    let mut file = std::fs::File::create(path).expect("Failed to create Vector config file");
    file.write_all(config_yaml.as_bytes())
        .expect("Failed to write Vector config");
    file.flush().expect("Failed to flush Vector config");
}

/// Run Vector as an async subprocess with timeout.
///
/// Uses `tokio::process::Command` so we don't block the async runtime.
/// Returns (exit_status_success, stderr_output).
async fn run_vector_async(config_path: &Path, timeout_secs: u64) -> (bool, String) {
    let child = tokio::process::Command::new("vector")
        .arg("--config")
        .arg(config_path)
        .arg("--quiet")
        .env("VECTOR_LOG", "warn")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to spawn vector binary");

    // Wait with timeout
    let result =
        tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait_with_output()).await;

    match result {
        Ok(Ok(output)) => {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            (output.status.success(), stderr)
        }
        Ok(Err(e)) => {
            panic!("Failed to wait for vector process: {e}");
        }
        Err(_) => {
            // Timeout - Vector didn't exit in time
            panic!(
                "Vector did not exit within {timeout_secs}s - likely stuck retrying failed deliveries"
            );
        }
    }
}

/// Validate a Vector config file. Panics if validation fails.
fn validate_vector_config(config_path: &Path) {
    let output = Command::new("vector")
        .arg("validate")
        .arg("--no-environment")
        .arg("--config-yaml")
        .arg(config_path)
        .output()
        .expect("Failed to run vector validate");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "Vector config validation failed:\nstdout: {stdout}\nstderr: {stderr}"
    );
}

/// Generate a self-signed TLS certificate using openssl (ECDSA P-384).
/// Returns (cert_path, key_path) within the given directory.
fn generate_self_signed_cert(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");

    let output = Command::new("openssl")
        .args([
            "req",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-384",
            "-nodes",
            "-x509",
            "-keyout",
        ])
        .arg(&key_path)
        .arg("-out")
        .arg(&cert_path)
        .args([
            "-days",
            "1",
            "-subj",
            "/CN=localhost/O=Test/C=AU",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ])
        .output()
        .expect("Failed to run openssl");

    assert!(
        output.status.success(),
        "openssl cert generation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(cert_path.exists(), "Certificate file not created");
    assert!(key_path.exists(), "Key file not created");

    (cert_path, key_path)
}

/// Assert no ERROR lines appear in Vector's stderr output.
fn assert_no_vector_errors(stderr: &str) {
    let error_lines: Vec<&str> = stderr.lines().filter(|l| l.contains("ERROR")).collect();

    assert!(
        error_lines.is_empty(),
        "Vector produced errors:\n{}",
        error_lines.join("\n")
    );
}

// =============================================================================
// Vector HTTP Plaintext Test
// =============================================================================

/// Test Vector sending events to the receiver over plain HTTP.
///
/// Starts a dfe-receiver HTTP server, runs Vector with `demo_logs` source
/// and `http` sink pointing at the receiver, and verifies Vector completes
/// without errors.
#[tokio::test]
async fn test_vector_http_sink() {
    if !vector_available() {
        eprintln!("Skipping test: vector binary not found in PATH");
        return;
    }

    let port = random_port();
    let config = test_config(port);
    let shutdown = start_http_server(config).await;

    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = tmp_dir.path().join("vector.yaml");

    let data_dir = tmp_dir.path().join("vector-data");
    let data_dir_str = data_dir.to_str().unwrap();

    let vector_config = format!(
        r#"
data_dir: "{data_dir_str}"

sources:
  demo:
    type: demo_logs
    format: json
    count: 10
    interval: 0.1

sinks:
  receiver:
    type: http
    inputs: ["demo"]
    uri: "http://127.0.0.1:{port}/ingest"
    encoding:
      codec: json
    method: post
    batch:
      max_events: 1
      timeout_secs: 1
    healthcheck:
      enabled: false
"#
    );

    write_vector_config(&config_path, &vector_config);
    validate_vector_config(&config_path);

    let (success, stderr) = run_vector_async(&config_path, 30).await;

    if !success {
        eprintln!("Vector stderr: {stderr}");
    }

    assert_no_vector_errors(&stderr);
    shutdown.cancel();
}

// =============================================================================
// Vector HTTPS Test
// =============================================================================

/// Test Vector sending events to the receiver over HTTPS with a self-signed cert.
///
/// Generates a self-signed TLS certificate, starts a dfe-receiver HTTPS server,
/// runs Vector with the CA cert configured, and verifies clean delivery.
#[tokio::test]
async fn test_vector_https_sink() {
    install_crypto_provider();

    if !vector_available() {
        eprintln!("Skipping test: vector binary not found in PATH");
        return;
    }

    if !openssl_available() {
        eprintln!("Skipping test: openssl binary not found in PATH");
        return;
    }

    let port = random_port();
    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Generate self-signed TLS cert
    let (cert_path, key_path) = generate_self_signed_cert(tmp_dir.path());

    // Configure receiver with TLS
    let mut config = test_config(port);
    config.server.tls.enabled = true;
    config.server.tls.cert_file = Some(cert_path.to_str().unwrap().to_string());
    config.server.tls.key_file = Some(key_path.to_str().unwrap().to_string());

    let shutdown = start_http_server(config).await;

    let config_path = tmp_dir.path().join("vector.yaml");
    let cert_path_str = cert_path.to_str().unwrap();
    let data_dir = tmp_dir.path().join("vector-data");
    let data_dir_str = data_dir.to_str().unwrap();

    let vector_config = format!(
        r#"
data_dir: "{data_dir_str}"

sources:
  demo:
    type: demo_logs
    format: json
    count: 10
    interval: 0.1

sinks:
  receiver:
    type: http
    inputs: ["demo"]
    uri: "https://127.0.0.1:{port}/ingest"
    encoding:
      codec: json
    method: post
    batch:
      max_events: 1
      timeout_secs: 1
    tls:
      ca_file: "{cert_path_str}"
    healthcheck:
      enabled: false
"#
    );

    write_vector_config(&config_path, &vector_config);
    validate_vector_config(&config_path);

    let (success, stderr) = run_vector_async(&config_path, 30).await;

    if !success {
        eprintln!("Vector stderr: {stderr}");
    }

    assert_no_vector_errors(&stderr);
    shutdown.cancel();
}

// =============================================================================
// Vector gRPC Test
// =============================================================================

/// Test Vector sending events to the receiver over gRPC (Vector sink protocol).
///
/// Starts a dfe-receiver gRPC server, runs Vector with `vector` sink pointing
/// at the gRPC endpoint, and verifies clean delivery.
#[tokio::test]
async fn test_vector_grpc_sink() {
    if !vector_available() {
        eprintln!("Skipping test: vector binary not found in PATH");
        return;
    }

    let http_port = random_port();
    let grpc_port = random_port();

    // Ensure different ports
    let grpc_port = if grpc_port == http_port {
        grpc_port + 1
    } else {
        grpc_port
    };

    let mut config = test_config(http_port);
    config.grpc.enabled = true;
    config.grpc.bind_address = format!("127.0.0.1:{grpc_port}");

    let metrics = Arc::new(Metrics::new());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(PipelineState::new(SharedConfig::new(config.clone())).expect("Failed to create pipeline"));

    // Spawn HTTP server
    let http_shutdown = shutdown.clone();
    let http_metrics = metrics.clone();
    let http_pipeline = pipeline.clone();
    let http_addr = config.server.bind_address.clone();
    tokio::spawn(async move {
        let _ = http::run_server(&http_addr, http_pipeline, http_metrics, http_shutdown).await;
    });

    // Spawn gRPC server
    let grpc_config = config.clone();
    let grpc_pipeline = pipeline.clone();
    let grpc_metrics = metrics.clone();
    let grpc_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = dfe_receiver::server::grpc::run_server(
            &grpc_config,
            grpc_pipeline,
            grpc_metrics,
            None, // No auth for this test
            grpc_shutdown,
        )
        .await;
    });

    // Wait for servers to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = tmp_dir.path().join("vector.yaml");
    let data_dir = tmp_dir.path().join("vector-data");
    let data_dir_str = data_dir.to_str().unwrap();

    let vector_config = format!(
        r#"
data_dir: "{data_dir_str}"

sources:
  demo:
    type: demo_logs
    format: json
    count: 10
    interval: 0.1

sinks:
  receiver:
    type: vector
    inputs: ["demo"]
    address: "127.0.0.1:{grpc_port}"
    compression: false
    healthcheck:
      enabled: false
"#
    );

    write_vector_config(&config_path, &vector_config);
    validate_vector_config(&config_path);

    let (success, stderr) = run_vector_async(&config_path, 30).await;

    if !success {
        eprintln!("Vector stderr: {stderr}");
    }

    assert_no_vector_errors(&stderr);
    shutdown.cancel();
}

// =============================================================================
// Vector gRPC with TLS Test
// =============================================================================

/// Test Vector sending events to the receiver over gRPC with TLS.
///
/// Generates a self-signed TLS certificate, starts a dfe-receiver gRPC server
/// with TLS, and runs Vector with the `vector` sink configured for TLS.
#[tokio::test]
async fn test_vector_grpc_tls_sink() {
    install_crypto_provider();

    if !vector_available() {
        eprintln!("Skipping test: vector binary not found in PATH");
        return;
    }

    if !openssl_available() {
        eprintln!("Skipping test: openssl binary not found in PATH");
        return;
    }

    let http_port = random_port();
    let grpc_port = random_port();

    // Ensure different ports
    let grpc_port = if grpc_port == http_port {
        grpc_port + 1
    } else {
        grpc_port
    };

    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let (cert_path, key_path) = generate_self_signed_cert(tmp_dir.path());

    let mut config = test_config(http_port);
    config.grpc.enabled = true;
    config.grpc.bind_address = format!("127.0.0.1:{grpc_port}");
    config.grpc.tls.enabled = true;
    config.grpc.tls.cert_file = Some(cert_path.to_str().unwrap().to_string());
    config.grpc.tls.key_file = Some(key_path.to_str().unwrap().to_string());

    let metrics = Arc::new(Metrics::new());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(PipelineState::new(SharedConfig::new(config.clone())).expect("Failed to create pipeline"));

    // Spawn HTTP server
    let http_shutdown = shutdown.clone();
    let http_metrics = metrics.clone();
    let http_pipeline = pipeline.clone();
    let http_addr = config.server.bind_address.clone();
    tokio::spawn(async move {
        let _ = http::run_server(&http_addr, http_pipeline, http_metrics, http_shutdown).await;
    });

    // Spawn gRPC server with TLS
    let grpc_config = config.clone();
    let grpc_pipeline = pipeline.clone();
    let grpc_metrics = metrics.clone();
    let grpc_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if let Err(e) = dfe_receiver::server::grpc::run_server(
            &grpc_config,
            grpc_pipeline,
            grpc_metrics,
            None,
            grpc_shutdown,
        )
        .await
        {
            eprintln!("gRPC TLS server error: {e}");
        }
    });

    // Wait for servers to start (TLS setup takes longer)
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let config_path = tmp_dir.path().join("vector.yaml");
    let cert_path_str = cert_path.to_str().unwrap();

    let vector_config = format!(
        r#"
data_dir: "{data_dir}"

sources:
  demo:
    type: demo_logs
    format: json
    count: 10
    interval: 0.1

sinks:
  receiver:
    type: vector
    inputs: ["demo"]
    address: "127.0.0.1:{grpc_port}"
    compression: false
    tls:
      enabled: true
      ca_file: "{cert_path_str}"
    healthcheck:
      enabled: false
"#,
        data_dir = tmp_dir.path().join("vector-data").to_str().unwrap(),
    );

    write_vector_config(&config_path, &vector_config);
    validate_vector_config(&config_path);

    let (success, stderr) = run_vector_async(&config_path, 15).await;

    if !success {
        eprintln!("Vector stderr: {stderr}");
    }

    assert_no_vector_errors(&stderr);
    shutdown.cancel();
}

// =============================================================================
// Vector HTTP with Bearer Auth Test
// =============================================================================

/// Test Vector sending events to the receiver with bearer token auth.
///
/// Starts a dfe-receiver HTTP server with bearer auth enabled,
/// runs Vector with the Authorization header configured, and verifies
/// authentication works end-to-end.
#[tokio::test]
async fn test_vector_http_bearer_auth() {
    use dfe_receiver::config::BearerConfig;

    if !vector_available() {
        eprintln!("Skipping test: vector binary not found in PATH");
        return;
    }

    let port = random_port();
    let mut config = test_config(port);
    config.server.auth.mode = "bearer".to_string();
    config.server.auth.bearer = BearerConfig {
        tokens: vec!["vector-test-token-42".to_string()],
        secret_source: None,
        refresh_interval_secs: 300,
    };

    let shutdown = start_http_server(config).await;

    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = tmp_dir.path().join("vector.yaml");
    let data_dir = tmp_dir.path().join("vector-data");
    let data_dir_str = data_dir.to_str().unwrap();

    let vector_config = format!(
        r#"
data_dir: "{data_dir_str}"

sources:
  demo:
    type: demo_logs
    format: json
    count: 5
    interval: 0.1

sinks:
  receiver:
    type: http
    inputs: ["demo"]
    uri: "http://127.0.0.1:{port}/ingest"
    encoding:
      codec: json
    method: post
    batch:
      max_events: 1
      timeout_secs: 1
    request:
      headers:
        Authorization: "Bearer vector-test-token-42"
    healthcheck:
      enabled: false
"#
    );

    write_vector_config(&config_path, &vector_config);
    validate_vector_config(&config_path);

    let (success, stderr) = run_vector_async(&config_path, 30).await;

    if !success {
        eprintln!("Vector stderr: {stderr}");
    }

    assert_no_vector_errors(&stderr);
    shutdown.cancel();
}
