// Project:   dfe-receiver
// File:      tests/orchestrator_lifecycle.rs
// Purpose:   The orchestrator's background tasks must run while serving
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Drives the built binary rather than the library, because the defect this
//! guards is the order of two calls in `main.rs`: the server blocked until
//! shutdown and the orchestrator only started afterwards, so every drain task
//! and the KEDA scaling feed ran for the shutdown window alone. A test that
//! calls `Orchestrator::start` itself cannot catch that order coming back.

use std::fs::File;
use std::io::Write;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::time::Instant;

/// Kills the receiver on the way out so a failed assertion cannot leave it
/// holding its ports.
struct ServerProcess(Child);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Take a port from the OS, then release it for the receiver's metrics server.
///
/// scalo's service runtime binds `--metrics-addr` itself and reports no bound
/// address, so this listener cannot be given port 0 and read back.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// The orchestrator must be running while the listeners serve, not only once
/// they stop.
///
/// `receiver_events_per_second` is set only by the orchestrator's
/// once-per-second metrics feed (`PipelineState::update_metrics`), so finding
/// it on `/metrics` while the process is still serving is the proof that the
/// feed is live. On the unfixed ordering the endpoint answers and the gauge
/// never appears.
#[tokio::test]
async fn orchestrator_runs_while_the_listeners_serve() {
    let metrics_port = free_port();

    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("receiver.yaml");
    let log_path = dir.path().join("receiver.log");

    let mut config = File::create(&config_path).unwrap();
    write!(
        config,
        // The broker is never reached: rdkafka resolves it in the background, so
        // the process serves without one and this test needs no container.
        r#"server:
  bind_address: "127.0.0.1:0"
metrics:
  enabled: true
  address: "127.0.0.1:{metrics_port}"
kafka:
  brokers:
    - "127.0.0.1:9092"
dlq:
  enabled: false
"#
    )
    .unwrap();
    config.flush().unwrap();

    let log = File::create(&log_path).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_dfe-receiver"))
        .arg("--config")
        .arg(&config_path)
        .arg("--metrics-addr")
        .arg(format!("127.0.0.1:{metrics_port}"))
        .arg("run")
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("receiver binary should start");
    let mut guard = ServerProcess(child);

    let url = format!("http://127.0.0.1:{metrics_port}/metrics");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut body = String::new();

    while Instant::now() < deadline {
        if let Some(status) = guard.0.try_wait().unwrap() {
            let log = std::fs::read_to_string(&log_path).unwrap_or_default();
            panic!("receiver exited early with {status}:\n{log}");
        }

        if let Ok(response) = reqwest::get(&url).await {
            body = response.text().await.unwrap_or_default();
            if body.contains("receiver_events_per_second") {
                return;
            }
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let log = std::fs::read_to_string(&log_path).unwrap_or_default();
    panic!(
        "receiver_events_per_second absent from /metrics after 30s while the \
         listeners were serving, so the orchestrator never started.\n\
         --- /metrics ---\n{body}\n--- stderr ---\n{log}"
    );
}
