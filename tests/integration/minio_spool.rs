// Project:   dfe-receiver
// File:      tests/integration/minio_spool.rs
// Purpose:   MinIO / S3-compatible storage integration tests
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! MinIO / S3-compatible storage tests.
//!
//! These tests spin up a fresh MinIO container per test via testcontainers-rs
//! and verify S3-compatible HTTP endpoints work end-to-end. The MinIO
//! emulator is a drop-in replacement for S3 for local testing.
//!
//! Note: dfe-receiver itself uses disk spillover via scalo's `TieredSink`
//! (not S3). These tests document the MinIO test-container pattern for
//! downstream archival/spillover features that may be added in future.
//! The MinIO container is stopped automatically when the test completes.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::time::Duration;

use crate::common::start_minio_container;
use crate::skip_if_no_docker;

/// Basic MinIO health check — ensures the container starts and serves the
/// S3 health endpoint. This is the minimum required to validate that
/// downstream code targeting S3-compatible storage can be exercised.
#[tokio::test]
async fn test_minio_container_starts_and_responds() {
    skip_if_no_docker!();

    let (_container, endpoint, _access_key, _secret_key) = match start_minio_container().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // MinIO's OWN liveness endpoint, not ours. It is upstream's spelling and we
    // do not get to pick it -- leave it alone when the fleet renames its probes.
    let health_url = format!("{endpoint}/minio/health/live");
    let client = reqwest::Client::new();

    // Give MinIO a moment to fully initialise
    tokio::time::sleep(Duration::from_millis(500)).await;

    let resp = tokio::time::timeout(Duration::from_secs(10), client.get(&health_url).send())
        .await
        .expect("MinIO health check timed out")
        .expect("MinIO health check request failed");

    assert!(
        resp.status().is_success() || resp.status().as_u16() == 200,
        "MinIO health returned {}",
        resp.status()
    );
}

/// Verify MinIO container accepts S3-compatible authenticated requests.
///
/// This test uses `reqwest` with MinIO's anonymous bucket listing to
/// confirm the S3 API endpoint is live. Full bucket/object CRUD would
/// require an `aws-sdk-s3` client dependency, which is not worth adding
/// for this smoke test. When dfe-receiver gains S3 spool support, this
/// test file becomes the seed for end-to-end spool tests.
#[tokio::test]
async fn test_minio_s3_endpoint_reachable() {
    skip_if_no_docker!();

    let (_container, endpoint, _access_key, _secret_key) = match start_minio_container().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Hit the S3 endpoint root — should return an XML error (no auth)
    // not a connection refusal. This proves the S3 emulator is listening.
    let client = reqwest::Client::new();
    let resp = tokio::time::timeout(Duration::from_secs(10), client.get(&endpoint).send())
        .await
        .expect("S3 endpoint timeout")
        .expect("S3 endpoint request failed");

    // Unauthenticated GET to MinIO root returns 403 (forbidden) with XML body —
    // this is the correct S3-compatible behaviour. 200 is also acceptable on
    // some MinIO versions depending on default permission config.
    let status = resp.status().as_u16();
    assert!(
        matches!(status, 200 | 400 | 403),
        "expected S3-style status code from MinIO, got {status}"
    );
}
