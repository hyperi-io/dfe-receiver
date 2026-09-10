// Project:   dfe-receiver
// File:      tests/smoke_startup.rs
// Purpose:   Smoke test for full startup lifecycle (catches init panics)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::unwrap_used, clippy::expect_used)]
// Test builds a PipelineState/Orchestrator inline; the config structs put the
// future just over clippy's 16 KiB threshold. Mirrors the lib's main.rs allow.
#![allow(clippy::large_futures)]

//! Smoke test that exercises the full startup path:
//! config → metrics init → pipeline build → server bind → shutdown.
//!
//! This must be in its own integration test file because `MetricsManager::new()`
//! installs a global Prometheus recorder (once per process). cargo-nextest runs
//! each test binary in its own process, so this won't conflict with other tests.

use bytes::Bytes;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use dfe_receiver::config::Config;
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::Orchestrator;

/// Full startup lifecycle: metrics init + pipeline build + process + shutdown.
///
/// This is the exact path that panicked in production (issue #19) when
/// `MetricsManager` was created twice. The test verifies that
/// `Metrics::with_dfe_metrics()` returns a reusable manager and that
/// the full init sequence completes without panic.
#[tokio::test]
async fn test_full_startup_lifecycle() {
    // Step 1: Load config (same as main.rs)
    let mut config = Config::default();
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();

    // Step 2: Create metrics with DFE groups — this installs the global recorder.
    // If this panics with SetRecorderError, the bug is back.
    let (metrics_instance, metrics_manager) =
        Metrics::with_dfe_metrics(Arc::new(config.scaling.build_pressure()));
    let metrics = Arc::new(metrics_instance);

    // Step 3: Verify the MetricsManager is usable (can set readiness check)
    let mut manager = metrics_manager;
    manager.set_readiness_check(|| true);

    // Step 4: Create orchestrator (pipeline + sinks)
    let shutdown = CancellationToken::new();
    let orchestrator = Orchestrator::new(config, metrics.clone(), shutdown.clone())
        .await
        .expect("orchestrator should initialise");

    // Step 5: Pipeline should be ready
    let state = orchestrator.state();
    assert!(state.is_ready(), "pipeline should be ready after init");

    // Step 6: Process a message through the pipeline
    let payload = Bytes::from(r#"{"test": "startup_smoke"}"#);
    state
        .process(payload)
        .await
        .expect("should process message");

    // Step 7: Verify metrics infrastructure works (counter via metrics crate)
    metrics.inc_requests_total("test");
    assert_eq!(metrics.get_requests_total(), 1);

    // Step 8: Graceful shutdown
    shutdown.cancel();
}

/// Verify that creating Metrics without DFE groups (test mode) still works.
/// This is the path used by all unit tests.
#[tokio::test]
async fn test_metrics_default_no_recorder_panic() {
    let metrics = Metrics::default();
    metrics.inc_requests_total("test");
    metrics.inc_requests_success("test");
    metrics.inc_requests_error("test");
    metrics.add_bytes_received("test", 100);
    assert_eq!(metrics.get_requests_total(), 1);
}
