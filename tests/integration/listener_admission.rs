// Project:   dfe-receiver
// File:      tests/integration/listener_admission.rs
// Purpose:   server.ip_filter and server.rate_limit reach every listener
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `server.ip_filter` and `server.rate_limit` read as controls on the
//! receiver's ingest surface, so every listener has to honour them.
//!
//! The per-protocol suites all run with the filter disabled, which is the
//! control: these tests set it and assert the listener stops admitting.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
// Test helpers build a PipelineState inline; the config structs put the future
// just over clippy's 16 KiB threshold. Mirrors the lib crate's allow (main.rs).
#![allow(clippy::large_futures)]

use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::fluent::FluentHandler;
use dfe_receiver::server::gelf::GelfHandler;
use dfe_receiver::server::lumberjack::LumberjackHandler;
#[cfg(feature = "otlp")]
use dfe_receiver::server::otlp::OtlpHandler;
use dfe_receiver::server::prometheus_rw::PrometheusRwHandler;
use dfe_receiver::server::splunk_hec::SplunkHecHandler;
use dfe_receiver::server::syslog::SyslogHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpStream, UdpSocket};
use tokio_util::sync::CancellationToken;

/// An OS-assigned ephemeral port, released before the handler binds it.
fn random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// A config that starts, with nothing but the always-on HTTP listener enabled.
fn base_config() -> Config {
    let mut config = Config::default();
    config.server.bind_address = format!("127.0.0.1:{}", random_port());
    config.server.auth.mode = "none".to_string();
    // The loader on its memory transport: accepted, sent nowhere, no broker.
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config
}

/// Bar every loopback source, which is where the test client connects from.
fn deny_loopback(config: &mut Config) {
    config.server.ip_filter.mode = "denylist".to_string();
    config.server.ip_filter.cidrs = vec!["127.0.0.0/8".to_string(), "::1/128".to_string()];
}

/// One request per second with no burst, so a handful in parallel overruns it.
fn throttle_hard(config: &mut Config) {
    config.server.rate_limit.enabled = true;
    config.server.rate_limit.requests_per_second = 1;
    config.server.rate_limit.burst = 1;
}

async fn pipeline_for(config: &Config) -> Arc<PipelineState> {
    Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("build pipeline"),
    )
}

/// Spawn a handler and give it time to bind.
async fn spawn(handler: Box<dyn ProtocolHandler>) -> CancellationToken {
    let shutdown = CancellationToken::new();
    let handler_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = handler.start(handler_shutdown).await;
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown
}

/// Whether a barred peer sees the connection close with nothing sent.
///
/// A listener that admits the connection holds it open waiting for the client
/// to speak first, so the read blocks until the timeout; one that rejects it
/// drops the stream in the accept loop and the peer reads EOF straight away.
async fn connection_closed_immediately(port: u16) -> bool {
    let mut stream = match TcpStream::connect(("127.0.0.1", port)).await {
        Ok(s) => s,
        // A RST during connect is the same verdict: no session was granted.
        Err(_) => return true,
    };
    let mut buf = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
    // EOF or a reset both say the listener let go of the connection.
    matches!(read, Ok(Ok(0) | Err(_)))
}

/// Fire requests in parallel from one apparent IP and report the statuses.
///
/// `SmartIpKeyExtractor` prefers `x-forwarded-for`, so the header keys every
/// request to the same bucket however the test client actually connects.
async fn parallel_post(url: String, count: usize) -> Vec<u16> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let mut handles = Vec::with_capacity(count);
    for _ in 0..count {
        let client = client.clone();
        let url = url.clone();
        handles.push(tokio::spawn(async move {
            client
                .post(url)
                .header("x-forwarded-for", "192.0.2.10")
                .body("{}")
                .send()
                .await
                .expect("request")
                .status()
                .as_u16()
        }));
    }

    let mut statuses = Vec::with_capacity(count);
    for handle in handles {
        statuses.push(handle.await.expect("task"));
    }
    statuses
}

// ---------------------------------------------------------------------------
// Splunk HEC
// ---------------------------------------------------------------------------

async fn start_hec(config: Config) -> CancellationToken {
    let pipeline = pipeline_for(&config).await;
    spawn(Box::new(SplunkHecHandler::new(
        config.splunk_hec.clone(),
        config.raw_capture_for(&config.splunk_hec.raw_capture),
        pipeline,
        Arc::new(Metrics::default()),
    )))
    .await
}

fn hec_config(port: u16) -> Config {
    let mut config = base_config();
    config.splunk_hec.enabled = true;
    config.splunk_hec.bind_address = format!("127.0.0.1:{port}");
    config.splunk_hec.auth.mode = "none".to_string();
    config
}

#[tokio::test]
async fn hec_applies_the_configured_ip_filter() {
    let port = random_port();
    let mut config = hec_config(port);
    deny_loopback(&mut config);

    let shutdown = start_hec(config).await;
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
        .post(format!("http://127.0.0.1:{port}/services/collector/event"))
        .body(r#"{"event":"x"}"#)
        .send()
        .await;

    assert!(
        response.is_err(),
        "a denied peer must not reach HEC, got: {:?}",
        response.ok().map(|r| r.status())
    );
    shutdown.cancel();
}

#[tokio::test]
async fn hec_applies_the_configured_rate_limit() {
    let port = random_port();
    let mut config = hec_config(port);
    throttle_hard(&mut config);

    let shutdown = start_hec(config).await;
    let statuses = parallel_post(
        format!("http://127.0.0.1:{port}/services/collector/event"),
        20,
    )
    .await;

    assert!(
        statuses.contains(&429),
        "HEC must throttle a source over the configured rate, got: {statuses:?}"
    );
    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Prometheus remote write
// ---------------------------------------------------------------------------

async fn start_rw(config: Config) -> CancellationToken {
    let pipeline = pipeline_for(&config).await;
    spawn(Box::new(PrometheusRwHandler::new(
        config.prometheus_rw.clone(),
        config.raw_capture_for(&config.prometheus_rw.raw_capture),
        pipeline,
        Arc::new(Metrics::default()),
    )))
    .await
}

fn rw_config(port: u16) -> Config {
    let mut config = base_config();
    config.prometheus_rw.enabled = true;
    config.prometheus_rw.bind_address = format!("127.0.0.1:{port}");
    config.prometheus_rw.auth.mode = "none".to_string();
    config
}

#[tokio::test]
async fn prometheus_rw_applies_the_configured_ip_filter() {
    let port = random_port();
    let mut config = rw_config(port);
    deny_loopback(&mut config);

    let shutdown = start_rw(config).await;
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
        .post(format!("http://127.0.0.1:{port}/api/v1/write"))
        .body("x")
        .send()
        .await;

    assert!(
        response.is_err(),
        "a denied peer must not reach remote write, got: {:?}",
        response.ok().map(|r| r.status())
    );
    shutdown.cancel();
}

#[tokio::test]
async fn prometheus_rw_applies_the_configured_rate_limit() {
    let port = random_port();
    let mut config = rw_config(port);
    throttle_hard(&mut config);

    let shutdown = start_rw(config).await;
    let statuses = parallel_post(format!("http://127.0.0.1:{port}/api/v1/write"), 20).await;

    assert!(
        statuses.contains(&429),
        "remote write must throttle a source over the configured rate, got: {statuses:?}"
    );
    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// OTLP HTTP
// ---------------------------------------------------------------------------

#[cfg(feature = "otlp")]
async fn start_otlp(config: Config) -> CancellationToken {
    let pipeline = pipeline_for(&config).await;
    spawn(Box::new(OtlpHandler::new(
        config.otlp.clone(),
        config.raw_capture_for(&config.otlp.raw_capture),
        pipeline,
        Arc::new(Metrics::default()),
    )))
    .await
}

#[cfg(feature = "otlp")]
fn otlp_config(http_port: u16) -> Config {
    let mut config = base_config();
    config.otlp.enabled = true;
    config.otlp.grpc_bind_address = format!("127.0.0.1:{}", random_port());
    config.otlp.http_bind_address = format!("127.0.0.1:{http_port}");
    config.otlp.auth.mode = "none".to_string();
    config
}

#[cfg(feature = "otlp")]
#[tokio::test]
async fn otlp_http_applies_the_configured_ip_filter() {
    let port = random_port();
    let mut config = otlp_config(port);
    deny_loopback(&mut config);

    let shutdown = start_otlp(config).await;
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
        .post(format!("http://127.0.0.1:{port}/v1/logs"))
        .header("content-type", "application/x-protobuf")
        .body(Vec::new())
        .send()
        .await;

    assert!(
        response.is_err(),
        "a denied peer must not reach OTLP HTTP, got: {:?}",
        response.ok().map(|r| r.status())
    );
    shutdown.cancel();
}

#[cfg(feature = "otlp")]
#[tokio::test]
async fn otlp_http_applies_the_configured_rate_limit() {
    let port = random_port();
    let mut config = otlp_config(port);
    throttle_hard(&mut config);

    let shutdown = start_otlp(config).await;
    let statuses = parallel_post(format!("http://127.0.0.1:{port}/v1/logs"), 20).await;

    assert!(
        statuses.contains(&429),
        "OTLP HTTP must throttle a source over the configured rate, got: {statuses:?}"
    );
    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Raw TCP listeners
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fluent_applies_the_configured_ip_filter() {
    let port = random_port();
    let mut config = base_config();
    config.fluent.enabled = true;
    config.fluent.bind_address = format!("127.0.0.1:{port}");
    deny_loopback(&mut config);

    let pipeline = pipeline_for(&config).await;
    let shutdown = spawn(Box::new(FluentHandler::new(
        config.fluent.clone(),
        config.raw_capture_for(&config.fluent.raw_capture),
        pipeline,
        Arc::new(Metrics::default()),
    )))
    .await;

    assert!(
        connection_closed_immediately(port).await,
        "a denied peer must not hold a Fluent Forward session"
    );
    shutdown.cancel();
}

#[tokio::test]
async fn gelf_applies_the_configured_ip_filter() {
    let port = random_port();
    let mut config = base_config();
    config.gelf.enabled = true;
    config.gelf.bind_address = format!("127.0.0.1:{port}");
    deny_loopback(&mut config);

    let pipeline = pipeline_for(&config).await;
    let shutdown = spawn(Box::new(GelfHandler::new(
        config.gelf.clone(),
        config.raw_capture_for(&config.gelf.raw_capture),
        pipeline,
        Arc::new(Metrics::default()),
    )))
    .await;

    assert!(
        connection_closed_immediately(port).await,
        "a denied peer must not hold a GELF session"
    );
    shutdown.cancel();
}

#[tokio::test]
async fn lumberjack_applies_the_configured_ip_filter() {
    let port = random_port();
    let mut config = base_config();
    config.lumberjack.enabled = true;
    config.lumberjack.bind_address = format!("127.0.0.1:{port}");
    deny_loopback(&mut config);

    let pipeline = pipeline_for(&config).await;
    let shutdown = spawn(Box::new(LumberjackHandler::new(
        config.lumberjack.clone(),
        pipeline,
        Arc::new(Metrics::default()),
    )))
    .await;

    assert!(
        connection_closed_immediately(port).await,
        "a denied peer must not hold a Lumberjack session"
    );
    shutdown.cancel();
}

#[tokio::test]
async fn syslog_applies_the_configured_ip_filter_on_tcp() {
    let udp_port = random_port();
    let tcp_port = random_port();
    let mut config = base_config();
    config.syslog.enabled = true;
    config.syslog.udp_bind_address = format!("127.0.0.1:{udp_port}");
    config.syslog.tcp_bind_address = format!("127.0.0.1:{tcp_port}");
    deny_loopback(&mut config);

    let pipeline = pipeline_for(&config).await;
    let shutdown = spawn(Box::new(SyslogHandler::new(
        config.syslog.clone(),
        config.raw_capture_for(&config.syslog.raw_capture),
        pipeline,
        Arc::new(Metrics::default()),
    )))
    .await;

    assert!(
        connection_closed_immediately(tcp_port).await,
        "a denied peer must not hold a syslog TCP session"
    );
    shutdown.cancel();
}

#[tokio::test]
async fn syslog_applies_the_configured_ip_filter_per_udp_datagram() {
    let udp_port = random_port();
    let tcp_port = random_port();
    let mut config = base_config();
    config.syslog.enabled = true;
    config.syslog.udp_bind_address = format!("127.0.0.1:{udp_port}");
    config.syslog.tcp_bind_address = format!("127.0.0.1:{tcp_port}");
    deny_loopback(&mut config);

    let metrics = Arc::new(Metrics::default());
    let pipeline = pipeline_for(&config).await;
    let shutdown = spawn(Box::new(SyslogHandler::new(
        config.syslog.clone(),
        config.raw_capture_for(&config.syslog.raw_capture),
        pipeline,
        metrics.clone(),
    )))
    .await;

    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind sender");
    socket
        .send_to(
            b"<34>1 2026-01-01T00:00:00Z host app - - - denied",
            ("127.0.0.1", udp_port),
        )
        .await
        .expect("send datagram");
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        metrics.get_requests_total(),
        0,
        "a denied datagram must not be counted or parsed"
    );
    shutdown.cancel();
}
