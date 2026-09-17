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
//! Every filter test here runs its listener twice: once with the peer admitted,
//! which proves the listener bound and answers, and once with it barred. The
//! first half is what tells a rejection apart from a listener that never
//! started -- both refuse a connection, and the assertion on its own cannot see
//! the difference.

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
use tokio::task::JoinHandle;
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

/// Admit one range and nothing else.
fn allow_only(config: &mut Config, cidr: &str) {
    config.server.ip_filter.mode = "allowlist".to_string();
    config.server.ip_filter.cidrs = vec![cidr.to_string()];
}

/// Four requests a second with a burst of two, so a handful in parallel
/// overruns it.
///
/// Deliberately not one per second: that is the single value where reading
/// `requests_per_second` as a rate and reading it as an interval agree, so a
/// test set there cannot tell the two apart.
fn throttle_hard(config: &mut Config) {
    config.server.rate_limit.enabled = true;
    config.server.rate_limit.requests_per_second = 4;
    config.server.rate_limit.burst = 2;
}

async fn pipeline_for(config: &Config) -> Arc<PipelineState> {
    Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("build pipeline"),
    )
}

/// Spawn a handler and wait for its port to accept a connection.
async fn spawn(handler: Box<dyn ProtocolHandler>, port: u16) -> CancellationToken {
    let shutdown = CancellationToken::new();
    let handler_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(handler_shutdown).await });
    wait_for_port(port, &mut task).await;
    shutdown
}

/// Poll the port until the handler accepts, or fail loudly.
///
/// A handler that cannot bind returns straight away and takes its error with
/// it, so a fixed sleep leaves the test unable to tell a listener that rejected
/// the peer from one that never started. Watching the task means a failed bind
/// fails the test instead of satisfying it.
async fn wait_for_port(port: u16, task: &mut JoinHandle<Result<(), dfe_receiver::error::Error>>) {
    let addr = format!("127.0.0.1:{port}");
    // 100 x 50ms = 5s, generous because it guards a race rather than measuring.
    for _ in 0..100 {
        if task.is_finished() {
            let outcome = task.await;
            panic!("the listener on port {port} exited before serving: {outcome:?}");
        }
        if TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the listener on port {port} never accepted a connection within 5s");
}

/// Whether an admitted peer keeps its session.
///
/// A listener that admits the connection waits for the client to speak first,
/// so the read blocks until the timeout. One that rejected it has already
/// dropped the stream and the read returns EOF.
async fn session_held(port: u16) -> bool {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("the listener must be bound");
    let mut buf = [0u8; 1];
    tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf))
        .await
        .is_err()
}

/// Whether a barred peer sees the connection close with nothing sent.
///
/// Connecting must succeed: the kernel completes the handshake for a bound
/// port whatever the accept loop then does with the stream, so a refused
/// connection means no listener, not a rejection.
async fn connection_closed_immediately(port: u16) -> bool {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("the listener must be bound -- a refused connection is not a rejection");
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

/// One POST, with the transport error kept rather than unwrapped.
async fn post_once(url: &str, content_type: &str, body: &'static [u8]) -> reqwest::Result<u16> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
        .post(url)
        .header("content-type", content_type)
        .body(body)
        .send()
        .await
        .map(|r| r.status().as_u16())
}

/// Build a protocol handler over a config and the pipeline behind it.
type Build = dyn Fn(&Config, Arc<PipelineState>) -> Box<dyn ProtocolHandler>;

/// Start one handler on `port` and wait for it to accept.
async fn start(config: &Config, build: &Build, port: u16) -> CancellationToken {
    let pipeline = pipeline_for(config).await;
    spawn(build(config, pipeline), port).await
}

/// The ingest endpoint of one HTTP listener.
struct Endpoint {
    protocol: &'static str,
    path: &'static str,
    content_type: &'static str,
    body: &'static [u8],
}

/// A raw TCP listener holds a session for an admitted peer and drops a barred
/// one.
async fn tcp_honours_the_shared_ip_filter(
    protocol: &str,
    configure: &dyn Fn(&mut Config, u16),
    build: &Build,
) {
    // Positive control: no filter, so the peer is admitted.
    let port = random_port();
    let mut config = base_config();
    configure(&mut config, port);
    let shutdown = start(&config, build, port).await;
    assert!(
        session_held(port).await,
        "{protocol} must hold a session for an admitted peer"
    );
    shutdown.cancel();

    // Same listener, loopback barred.
    let port = random_port();
    let mut config = base_config();
    configure(&mut config, port);
    deny_loopback(&mut config);
    let shutdown = start(&config, build, port).await;
    assert!(
        connection_closed_immediately(port).await,
        "a denied peer must not hold a {protocol} session"
    );
    shutdown.cancel();
}

/// An HTTP listener answers an admitted peer and drops a barred one.
async fn http_honours_the_shared_ip_filter(
    endpoint: &Endpoint,
    configure: &dyn Fn(&mut Config, u16),
    build: &Build,
) {
    let Endpoint {
        protocol,
        path,
        content_type,
        body,
    } = *endpoint;

    // Positive control: no filter, so the request is served.
    let port = random_port();
    let mut config = base_config();
    configure(&mut config, port);
    let shutdown = start(&config, build, port).await;
    let answered = post_once(
        &format!("http://127.0.0.1:{port}{path}"),
        content_type,
        body,
    )
    .await;
    assert!(
        answered.is_ok(),
        "{protocol} must answer an admitted peer, got: {:?}",
        answered.err()
    );
    shutdown.cancel();

    // Same listener, loopback barred.
    let port = random_port();
    let mut config = base_config();
    configure(&mut config, port);
    deny_loopback(&mut config);
    let shutdown = start(&config, build, port).await;
    let response = post_once(
        &format!("http://127.0.0.1:{port}{path}"),
        content_type,
        body,
    )
    .await;
    assert!(
        response.is_err(),
        "a denied peer must not reach {protocol}, got: {:?}",
        response.ok()
    );
    shutdown.cancel();
}

/// An HTTP listener serves inside the configured rate and answers 429 over it.
async fn http_honours_the_shared_rate_limit(
    endpoint: &Endpoint,
    configure: &dyn Fn(&mut Config, u16),
    build: &Build,
) {
    let port = random_port();
    let mut config = base_config();
    configure(&mut config, port);
    throttle_hard(&mut config);

    let shutdown = start(&config, build, port).await;
    let statuses = parallel_post(format!("http://127.0.0.1:{port}{}", endpoint.path), 20).await;

    assert!(
        statuses.iter().any(|status| *status != 429),
        "{} must serve the requests inside the burst, got: {statuses:?}",
        endpoint.protocol
    );
    assert!(
        statuses.contains(&429),
        "{} must throttle a source over the configured rate, got: {statuses:?}",
        endpoint.protocol
    );
    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Splunk HEC
// ---------------------------------------------------------------------------

const HEC: Endpoint = Endpoint {
    protocol: "HEC",
    path: "/services/collector/event",
    content_type: "application/json",
    body: br#"{"event":"x"}"#,
};

fn configure_hec(config: &mut Config, port: u16) {
    config.splunk_hec.enabled = true;
    config.splunk_hec.bind_address = format!("127.0.0.1:{port}");
    config.splunk_hec.auth.mode = "none".to_string();
}

fn build_hec(config: &Config, pipeline: Arc<PipelineState>) -> Box<dyn ProtocolHandler> {
    Box::new(SplunkHecHandler::new(
        config.splunk_hec.clone(),
        config.raw_capture_for(&config.splunk_hec.raw_capture),
        pipeline,
        Arc::new(Metrics::default()),
    ))
}

#[tokio::test]
async fn hec_applies_the_configured_ip_filter() {
    http_honours_the_shared_ip_filter(&HEC, &configure_hec, &build_hec).await;
}

#[tokio::test]
async fn hec_applies_the_configured_rate_limit() {
    http_honours_the_shared_rate_limit(&HEC, &configure_hec, &build_hec).await;
}

// ---------------------------------------------------------------------------
// Prometheus remote write
// ---------------------------------------------------------------------------

const PROMETHEUS_RW: Endpoint = Endpoint {
    protocol: "remote write",
    path: "/api/v1/write",
    content_type: "application/x-protobuf",
    body: b"x",
};

fn configure_rw(config: &mut Config, port: u16) {
    config.prometheus_rw.enabled = true;
    config.prometheus_rw.bind_address = format!("127.0.0.1:{port}");
    config.prometheus_rw.auth.mode = "none".to_string();
}

fn build_rw(config: &Config, pipeline: Arc<PipelineState>) -> Box<dyn ProtocolHandler> {
    Box::new(PrometheusRwHandler::new(
        config.prometheus_rw.clone(),
        config.raw_capture_for(&config.prometheus_rw.raw_capture),
        pipeline,
        Arc::new(Metrics::default()),
    ))
}

#[tokio::test]
async fn prometheus_rw_applies_the_configured_ip_filter() {
    http_honours_the_shared_ip_filter(&PROMETHEUS_RW, &configure_rw, &build_rw).await;
}

#[tokio::test]
async fn prometheus_rw_applies_the_configured_rate_limit() {
    http_honours_the_shared_rate_limit(&PROMETHEUS_RW, &configure_rw, &build_rw).await;
}

// ---------------------------------------------------------------------------
// OTLP HTTP
// ---------------------------------------------------------------------------

#[cfg(feature = "otlp")]
const OTLP_HTTP: Endpoint = Endpoint {
    protocol: "OTLP HTTP",
    path: "/v1/logs",
    content_type: "application/x-protobuf",
    body: b"",
};

#[cfg(feature = "otlp")]
fn configure_otlp(config: &mut Config, http_port: u16) {
    config.otlp.enabled = true;
    config.otlp.grpc_bind_address = format!("127.0.0.1:{}", random_port());
    config.otlp.http_bind_address = format!("127.0.0.1:{http_port}");
    config.otlp.auth.mode = "none".to_string();
}

#[cfg(feature = "otlp")]
fn build_otlp(config: &Config, pipeline: Arc<PipelineState>) -> Box<dyn ProtocolHandler> {
    Box::new(OtlpHandler::new(
        config.otlp.clone(),
        config.raw_capture_for(&config.otlp.raw_capture),
        pipeline,
        Arc::new(Metrics::default()),
    ))
}

#[cfg(feature = "otlp")]
#[tokio::test]
async fn otlp_http_applies_the_configured_ip_filter() {
    http_honours_the_shared_ip_filter(&OTLP_HTTP, &configure_otlp, &build_otlp).await;
}

#[cfg(feature = "otlp")]
#[tokio::test]
async fn otlp_http_applies_the_configured_rate_limit() {
    http_honours_the_shared_rate_limit(&OTLP_HTTP, &configure_otlp, &build_otlp).await;
}

// ---------------------------------------------------------------------------
// Raw TCP listeners
// ---------------------------------------------------------------------------

fn configure_fluent(config: &mut Config, port: u16) {
    config.fluent.enabled = true;
    config.fluent.bind_address = format!("127.0.0.1:{port}");
}

fn build_fluent(config: &Config, pipeline: Arc<PipelineState>) -> Box<dyn ProtocolHandler> {
    Box::new(FluentHandler::new(
        config.fluent.clone(),
        config.raw_capture_for(&config.fluent.raw_capture),
        pipeline,
        Arc::new(Metrics::default()),
    ))
}

fn configure_gelf(config: &mut Config, port: u16) {
    config.gelf.enabled = true;
    config.gelf.bind_address = format!("127.0.0.1:{port}");
}

fn build_gelf(config: &Config, pipeline: Arc<PipelineState>) -> Box<dyn ProtocolHandler> {
    Box::new(GelfHandler::new(
        config.gelf.clone(),
        config.raw_capture_for(&config.gelf.raw_capture),
        pipeline,
        Arc::new(Metrics::default()),
    ))
}

fn configure_lumberjack(config: &mut Config, port: u16) {
    config.lumberjack.enabled = true;
    config.lumberjack.bind_address = format!("127.0.0.1:{port}");
}

fn build_lumberjack(config: &Config, pipeline: Arc<PipelineState>) -> Box<dyn ProtocolHandler> {
    Box::new(LumberjackHandler::new(
        config.lumberjack.clone(),
        pipeline,
        Arc::new(Metrics::default()),
    ))
}

fn configure_syslog(config: &mut Config, tcp_port: u16) {
    config.syslog.enabled = true;
    config.syslog.udp_bind_address = format!("127.0.0.1:{}", random_port());
    config.syslog.tcp_bind_address = format!("127.0.0.1:{tcp_port}");
}

fn build_syslog(config: &Config, pipeline: Arc<PipelineState>) -> Box<dyn ProtocolHandler> {
    Box::new(SyslogHandler::new(
        config.syslog.clone(),
        config.raw_capture_for(&config.syslog.raw_capture),
        pipeline,
        Arc::new(Metrics::default()),
    ))
}

#[tokio::test]
async fn fluent_applies_the_configured_ip_filter() {
    tcp_honours_the_shared_ip_filter("Fluent Forward", &configure_fluent, &build_fluent).await;
}

#[tokio::test]
async fn gelf_applies_the_configured_ip_filter() {
    tcp_honours_the_shared_ip_filter("GELF", &configure_gelf, &build_gelf).await;
}

#[tokio::test]
async fn lumberjack_applies_the_configured_ip_filter() {
    tcp_honours_the_shared_ip_filter("Lumberjack", &configure_lumberjack, &build_lumberjack).await;
}

#[tokio::test]
async fn syslog_applies_the_configured_ip_filter_on_tcp() {
    tcp_honours_the_shared_ip_filter("syslog TCP", &configure_syslog, &build_syslog).await;
}

#[tokio::test]
async fn an_allowlist_admits_only_the_sources_it_names() {
    // The other arm of the filter enum, and the one an operator reaches for
    // first. A denylist test never runs it: an allowlist bars every source it
    // does not name, which is the opposite default.
    let port = random_port();
    let mut config = base_config();
    configure_gelf(&mut config, port);
    allow_only(&mut config, "127.0.0.0/8");
    let shutdown = start(&config, &build_gelf, port).await;
    assert!(
        session_held(port).await,
        "an allowlisted peer must hold a session"
    );
    shutdown.cancel();

    let port = random_port();
    let mut config = base_config();
    configure_gelf(&mut config, port);
    allow_only(&mut config, "10.0.0.0/8");
    let shutdown = start(&config, &build_gelf, port).await;
    assert!(
        connection_closed_immediately(port).await,
        "a peer outside the allowlist must be dropped"
    );
    shutdown.cancel();
}

#[tokio::test]
async fn syslog_applies_the_configured_ip_filter_per_udp_datagram() {
    // Positive control: no filter, so the datagram is counted and parsed.
    let udp_port = random_port();
    let tcp_port = random_port();
    let mut config = base_config();
    config.syslog.enabled = true;
    config.syslog.udp_bind_address = format!("127.0.0.1:{udp_port}");
    config.syslog.tcp_bind_address = format!("127.0.0.1:{tcp_port}");

    let metrics = Arc::new(Metrics::default());
    let pipeline = pipeline_for(&config).await;
    let shutdown = spawn(
        Box::new(SyslogHandler::new(
            config.syslog.clone(),
            config.raw_capture_for(&config.syslog.raw_capture),
            pipeline,
            metrics.clone(),
        )),
        tcp_port,
    )
    .await;
    send_syslog_datagram(udp_port).await;
    assert_eq!(
        counted_requests(&metrics).await,
        1,
        "an admitted datagram must be counted"
    );
    shutdown.cancel();

    // Same listener, loopback barred.
    let udp_port = random_port();
    let tcp_port = random_port();
    let mut config = base_config();
    config.syslog.enabled = true;
    config.syslog.udp_bind_address = format!("127.0.0.1:{udp_port}");
    config.syslog.tcp_bind_address = format!("127.0.0.1:{tcp_port}");
    deny_loopback(&mut config);

    let metrics = Arc::new(Metrics::default());
    let pipeline = pipeline_for(&config).await;
    let shutdown = spawn(
        Box::new(SyslogHandler::new(
            config.syslog.clone(),
            config.raw_capture_for(&config.syslog.raw_capture),
            pipeline,
            metrics.clone(),
        )),
        tcp_port,
    )
    .await;
    send_syslog_datagram(udp_port).await;
    assert_eq!(
        counted_requests(&metrics).await,
        0,
        "a denied datagram must not be counted or parsed"
    );
    shutdown.cancel();
}

/// Send one well-formed syslog line to the UDP listener.
async fn send_syslog_datagram(udp_port: u16) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind sender");
    socket
        .send_to(
            b"<34>1 2026-01-01T00:00:00Z host app - - - hello",
            ("127.0.0.1", udp_port),
        )
        .await
        .expect("send datagram");
}

/// Requests the handler has counted, once it has had the chance to count one.
///
/// Returns as soon as a request lands, so the admitted half is quick; the
/// barred half spends the whole budget proving nothing arrives.
async fn counted_requests(metrics: &Metrics) -> u64 {
    // 40 x 50ms = 2s, long enough that a slow loopback datagram is not read as
    // a rejection.
    for _ in 0..40 {
        let seen = metrics.get_requests_total();
        if seen > 0 {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    metrics.get_requests_total()
}
