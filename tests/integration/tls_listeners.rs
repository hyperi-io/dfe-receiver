// Project:   dfe-receiver
// File:      tests/integration/tls_listeners.rs
// Purpose:   Every TLS listener starts through its handler and completes a handshake
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Each TLS listener starts through its real handler and completes a handshake,
//! with no rustls provider installed by the test. The test binary compiles both
//! ring and aws-lc-rs, so a listener that leaves the choice to rustls panics.
//! Syslog TLS and HTTPS ingest carry an event end to end in their own files.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use dfe_receiver::config::{Config, SharedConfig, TlsConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::grpc::GrpcVectorHandler;
use dfe_receiver::server::lumberjack::LumberjackHandler;
#[cfg(feature = "otlp")]
use dfe_receiver::server::otlp::OtlpHandler;
use dfe_receiver::server::traits::{BoundAddr, ProtocolHandler};
use tokio_util::sync::CancellationToken;

/// A config whose only destination is the loader on its memory transport.
fn base_config() -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config
}

/// TLS on, from a throwaway certificate that stays on disk while `dir` lives.
fn tls_on(dir: &tempfile::TempDir) -> (TlsConfig, PathBuf) {
    let (cert, key) = crate::common::self_signed_cert(dir.path());
    let tls = TlsConfig {
        enabled: true,
        cert_file: Some(cert.to_string_lossy().into_owned()),
        key_file: Some(key.to_string_lossy().into_owned()),
        ..TlsConfig::default()
    };
    (tls, cert)
}

async fn pipeline_for(config: &Config) -> Arc<PipelineState> {
    Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("build pipeline"),
    )
}

/// Start `handler`, wait for each of `listeners` to bind, and handshake with it.
async fn assert_handshakes(
    handler: Box<dyn ProtocolHandler>,
    listeners: &[(&str, BoundAddr)],
    cert: &std::path::Path,
) {
    let shutdown = CancellationToken::new();
    let handler_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(handler_shutdown).await });
    for (name, bound) in listeners {
        let addr: SocketAddr = crate::common::bound_addr(name, bound, &mut task).await;
        drop(crate::common::tls_connect(addr, cert).await);
    }
    shutdown.cancel();
}

#[tokio::test]
async fn grpc_over_tls_handshakes() {
    let dir = tempfile::tempdir().unwrap();
    let (tls, cert) = tls_on(&dir);
    let mut config = base_config();
    config.grpc.enabled = true;
    config.grpc.bind_address = "127.0.0.1:0".to_string();
    config.grpc.tls = tls;

    let handler = GrpcVectorHandler::new(
        config.clone(),
        pipeline_for(&config).await,
        Arc::new(Metrics::default()),
    );
    let bound = handler.bound_addr();
    assert_handshakes(Box::new(handler), &[("gRPC", bound)], &cert).await;
}

#[cfg(feature = "otlp")]
#[tokio::test]
async fn otlp_grpc_and_http_over_tls_handshake() {
    let dir = tempfile::tempdir().unwrap();
    let (tls, cert) = tls_on(&dir);
    let mut config = base_config();
    config.otlp.enabled = true;
    config.otlp.grpc_bind_address = "127.0.0.1:0".to_string();
    config.otlp.http_bind_address = "127.0.0.1:0".to_string();
    config.otlp.auth.mode = "none".to_string();
    config.otlp.tls = tls;

    let handler = OtlpHandler::new(
        config.otlp.clone(),
        config.raw_capture_for(&config.otlp.raw_capture),
        pipeline_for(&config).await,
        Arc::new(Metrics::default()),
    );
    let listeners = [
        ("OTLP gRPC", handler.grpc_bound_addr()),
        ("OTLP HTTP", handler.http_bound_addr()),
    ];
    assert_handshakes(Box::new(handler), &listeners, &cert).await;
}

#[tokio::test]
async fn lumberjack_over_tls_handshakes() {
    let dir = tempfile::tempdir().unwrap();
    let (tls, cert) = tls_on(&dir);
    let mut config = base_config();
    config.lumberjack.enabled = true;
    config.lumberjack.bind_address = "127.0.0.1:0".to_string();
    config.lumberjack.tls = tls;

    let handler = LumberjackHandler::new(
        config.lumberjack.clone(),
        pipeline_for(&config).await,
        Arc::new(Metrics::default()),
    );
    let bound = handler.bound_addr();
    assert_handshakes(Box::new(handler), &[("Lumberjack", bound)], &cert).await;
}
