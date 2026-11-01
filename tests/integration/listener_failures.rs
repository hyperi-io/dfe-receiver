// Project:   dfe-receiver
// File:      tests/integration/listener_failures.rs
// Purpose:   A listener that cannot bind fails its handler and holds the pod not ready
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A handler whose listener cannot bind must fail its `start()`, and the pod
//! must not report ready while any enabled listener is missing.
//!
//! Each failure test holds the port with another socket before the handler
//! starts, so the bind fails the way it does when another process owns the port.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
// Test helpers build a PipelineState inline; the config structs put the future
// just over clippy's 16 KiB threshold. Mirrors the lib crate's allow (main.rs).
#![allow(clippy::large_futures)]

use std::io::Write;
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, SharedConfig, SpilloverConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::Server;
use dfe_receiver::server::flow::handler::FlowHandler;
use dfe_receiver::server::flow::metrics::mock::flow_metrics_for_test;
use dfe_receiver::server::fluent::FluentHandler;
use dfe_receiver::server::gelf::GelfHandler;
#[cfg(feature = "otlp")]
use dfe_receiver::server::otlp::OtlpHandler;
use dfe_receiver::server::syslog::SyslogHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use scalo::transport::AcknowledgementsConfig;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::common::peers::{Cases, free_addr, free_port};
use crate::common::{ClosedPort, HeldUdpPort};

/// How long a handler gets to report that a listener could not bind.
const FAILURE_DEADLINE: Duration = Duration::from_secs(10);

/// How long readiness gets to settle before a test gives up on it.
const READY_DEADLINE: Duration = Duration::from_secs(15);

/// A config that starts, with nothing but the always-on HTTP listener enabled.
fn base_config() -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    // The loader on its memory transport: accepted, sent nowhere, no broker.
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config
}

async fn pipeline_for(config: &Config) -> Arc<PipelineState> {
    Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("build pipeline"),
    )
}

fn metrics() -> Arc<Metrics> {
    Arc::new(Metrics::default())
}

/// Start `handler` and return what `start()` gave back.
///
/// # Panics
///
/// When `start()` is still running at [`FAILURE_DEADLINE`]: a handler with a
/// listener missing must not carry on serving as though it started.
async fn start_outcome(handler: Box<dyn ProtocolHandler>) -> dfe_receiver::Result<()> {
    let name = handler.name();
    let shutdown = CancellationToken::new();
    let outcome = tokio::time::timeout(FAILURE_DEADLINE, handler.start(shutdown.clone())).await;
    shutdown.cancel();
    outcome.unwrap_or_else(|_| {
        panic!("{name} kept running for {FAILURE_DEADLINE:?} with a listener that could not bind")
    })
}

/// Assert `handler` fails to start, with an error that names `listener`.
async fn assert_fails_naming(handler: Box<dyn ProtocolHandler>, listener: &str) {
    let name = handler.name();
    let Err(error) = start_outcome(handler).await else {
        panic!("{name} returned Ok with its {listener} listener unable to bind");
    };
    let message = error.to_string();
    assert!(
        message.contains(listener),
        "the {name} error must name the {listener} listener, got: {message}"
    );
}

/// Poll `condition` until it holds, or [`READY_DEADLINE`] passes.
async fn eventually(condition: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + READY_DEADLINE;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    condition()
}

// ---------------------------------------------------------------------------
// Fluent Forward and GELF
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fluent_fails_when_its_port_is_taken() {
    let held = ClosedPort::loopback().expect("hold a TCP port");
    let mut config = base_config();
    config.fluent.enabled = true;
    config.fluent.bind_address = held.addr().to_string();

    let handler = FluentHandler::new(
        config.fluent.clone(),
        pipeline_for(&config).await,
        metrics(),
    );
    assert_fails_naming(Box::new(handler), "Fluent Forward").await;
}

#[tokio::test]
async fn gelf_fails_when_its_port_is_taken() {
    let held = ClosedPort::loopback().expect("hold a TCP port");
    let mut config = base_config();
    config.gelf.enabled = true;
    config.gelf.bind_address = held.addr().to_string();

    let handler = GelfHandler::new(config.gelf.clone(), pipeline_for(&config).await, metrics());
    assert_fails_naming(Box::new(handler), "GELF").await;
}

// ---------------------------------------------------------------------------
// Syslog: UDP, TCP and TLS
// ---------------------------------------------------------------------------

fn syslog_config() -> Config {
    let mut config = base_config();
    config.syslog.enabled = true;
    config.syslog.udp_bind_address = "127.0.0.1:0".to_string();
    config.syslog.tcp_bind_address = "127.0.0.1:0".to_string();
    config.syslog.tls_bind_address = "127.0.0.1:0".to_string();
    config
}

async fn syslog_handler(config: &Config) -> SyslogHandler {
    SyslogHandler::new(
        config.syslog.clone(),
        config.raw_capture_for(&config.syslog.raw_capture),
        pipeline_for(config).await,
        metrics(),
    )
}

#[tokio::test]
async fn syslog_fails_when_its_udp_port_is_taken() {
    let held = HeldUdpPort::loopback().expect("hold a UDP port");
    let mut config = syslog_config();
    config.syslog.udp_bind_address = held.addr().to_string();

    let handler = syslog_handler(&config).await;
    assert_fails_naming(Box::new(handler), "syslog UDP").await;
}

#[tokio::test]
async fn syslog_fails_when_its_tcp_port_is_taken() {
    let held = ClosedPort::loopback().expect("hold a TCP port");
    let mut config = syslog_config();
    config.syslog.tcp_bind_address = held.addr().to_string();

    let handler = syslog_handler(&config).await;
    assert_fails_naming(Box::new(handler), "syslog TCP").await;
}

#[tokio::test]
async fn syslog_fails_when_its_tls_port_is_taken() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (cert, key) = crate::common::self_signed_cert(dir.path());

    let held = ClosedPort::loopback().expect("hold a TCP port");
    let mut config = syslog_config();
    config.syslog.tls.enabled = true;
    config.syslog.tls.cert_file = Some(cert.to_string_lossy().into_owned());
    config.syslog.tls.key_file = Some(key.to_string_lossy().into_owned());
    config.syslog.tls_bind_address = held.addr().to_string();

    let handler = syslog_handler(&config).await;
    assert_fails_naming(Box::new(handler), "syslog TLS").await;
}

/// TLS switched on with no certificate is a startup error, and it must stop the
/// whole handler: UDP and plain TCP must not come up without the TLS listener
/// the config asks for.
#[tokio::test]
async fn syslog_with_tls_it_cannot_build_starts_no_listener() {
    let mut config = syslog_config();
    config.syslog.tls.enabled = true;

    let handler = syslog_handler(&config).await;
    let udp = handler.udp_bound_addr();
    let tcp = handler.tcp_bound_addr();

    let outcome = start_outcome(Box::new(handler)).await;
    assert!(
        outcome.is_err(),
        "syslog with TLS enabled and no certificate must fail to start"
    );

    let came_up = tokio::select! {
        addr = udp.wait() => Some(("UDP", addr)),
        addr = tcp.wait() => Some(("TCP", addr)),
        () = tokio::time::sleep(Duration::from_secs(1)) => None,
    };
    assert!(
        came_up.is_none(),
        "a syslog handler that failed its TLS setup left a listener serving: {came_up:?}"
    );
}

// ---------------------------------------------------------------------------
// OTLP: gRPC and HTTP
// ---------------------------------------------------------------------------

#[cfg(feature = "otlp")]
async fn otlp_handler(grpc: &str, http: &str) -> OtlpHandler {
    let mut config = base_config();
    config.otlp.enabled = true;
    config.otlp.grpc_bind_address = grpc.to_string();
    config.otlp.http_bind_address = http.to_string();
    config.otlp.auth.mode = "none".to_string();
    OtlpHandler::new(config.otlp.clone(), pipeline_for(&config).await, metrics())
}

#[cfg(feature = "otlp")]
#[tokio::test]
async fn otlp_fails_when_its_grpc_port_is_taken() {
    let held = ClosedPort::loopback().expect("hold a TCP port");
    let handler = otlp_handler(&held.addr().to_string(), "127.0.0.1:0").await;
    assert_fails_naming(Box::new(handler), "OTLP gRPC").await;
}

#[cfg(feature = "otlp")]
#[tokio::test]
async fn otlp_fails_when_its_http_port_is_taken() {
    let held = ClosedPort::loopback().expect("hold a TCP port");
    let handler = otlp_handler("127.0.0.1:0", &held.addr().to_string()).await;
    assert_fails_naming(Box::new(handler), "OTLP HTTP").await;
}

// ---------------------------------------------------------------------------
// Flow: unified and split
// ---------------------------------------------------------------------------

async fn flow_handler(config: &Config) -> FlowHandler {
    FlowHandler::new(
        config.flow.clone(),
        config.raw_capture_for(&config.flow.raw_capture),
        flow_metrics_for_test(),
        pipeline_for(config).await,
    )
    .expect("a valid flow config")
}

fn flow_config() -> Config {
    let mut config = base_config();
    config.flow.experimental = false;
    config.flow.bind_address = std::net::Ipv4Addr::LOCALHOST.into();
    // The system default can cap SO_RCVBUF below the 8 MiB default.
    config.flow.recv_buffer_bytes = 256 * 1024;
    config
}

/// A split config with NetFlow on `netflow_port` and sFlow on `sflow_port`.
fn split_flow_config(netflow_port: u16, sflow_port: u16) -> Config {
    let mut config = flow_config();
    config.flow.split = Some(
        scalo::yaml::from_str(
            &format!(
                "netflow:\n  bind_address: 127.0.0.1\n  ports: [{netflow_port}]\n  \
             recv_buffer_bytes: 262144\n  topic: netflow_land\n\
             sflow:\n  bind_address: 127.0.0.1\n  ports: [{sflow_port}]\n  \
             recv_buffer_bytes: 262144\n  topic: sflow_land\n"
            ),
            "config.yaml",
            scalo::yaml::Profile::Config,
        )
        .expect("split flow config"),
    );
    config
}

#[tokio::test]
async fn flow_fails_when_one_of_its_ports_is_taken() {
    let held = HeldUdpPort::loopback().expect("hold a UDP port");
    let mut config = flow_config();
    config.flow.enabled = true;
    config.flow.ports = vec![0, held.addr().port()];

    let handler = flow_handler(&config).await;
    assert_fails_naming(Box::new(handler), &held.addr().to_string()).await;
}

#[tokio::test]
async fn split_flow_fails_when_its_netflow_port_is_taken() {
    let held = HeldUdpPort::loopback().expect("hold a UDP port");
    let config = split_flow_config(held.addr().port(), 0);

    let handler = flow_handler(&config).await;
    assert_fails_naming(Box::new(handler), &held.addr().to_string()).await;
}

#[tokio::test]
async fn split_flow_fails_when_its_sflow_port_is_taken() {
    let held = HeldUdpPort::loopback().expect("hold a UDP port");
    let config = split_flow_config(0, held.addr().port());

    let handler = flow_handler(&config).await;
    assert_fails_naming(Box::new(handler), &held.addr().to_string()).await;
}

// ---------------------------------------------------------------------------
// Readiness
// ---------------------------------------------------------------------------

/// Run `server` until `shutdown`, the way `main` does.
fn run(server: Server, shutdown: &CancellationToken) -> tokio::task::JoinHandle<()> {
    let shutdown = shutdown.clone();
    tokio::spawn(async move {
        server.run(shutdown).await.expect("the server returns Ok");
    })
}

/// Readiness waits for every listener the server enables, and drops once they
/// stop.
#[tokio::test]
async fn readiness_waits_for_every_enabled_listener() {
    let mut config = base_config();
    config.fluent.enabled = true;
    config.fluent.bind_address = "127.0.0.1:0".to_string();
    config.gelf.enabled = true;
    config.gelf.bind_address = "127.0.0.1:0".to_string();
    let pipeline = pipeline_for(&config).await;

    assert!(
        !pipeline.probe_ready(),
        "the pod must not be ready before any listener has bound"
    );

    let shutdown = CancellationToken::new();
    let server = run(Server::new(pipeline.clone(), metrics()), &shutdown);
    assert!(
        eventually(|| pipeline.probe_ready()).await,
        "the pod must be ready once HTTP, Fluent Forward and GELF have bound"
    );

    shutdown.cancel();
    assert!(
        eventually(|| !pipeline.probe_ready()).await,
        "the pod must not stay ready once its listeners have stopped"
    );
    server.await.expect("server task");
}

/// Run `server` for two seconds and assert the pod never reports ready.
///
/// The positive control is `readiness_waits_for_every_enabled_listener`: the
/// same server path reports ready within [`READY_DEADLINE`] when every
/// listener comes up.
async fn assert_never_ready(server: Server, pipeline: &PipelineState, missing: &str) {
    let shutdown = CancellationToken::new();
    let task = run(server, &shutdown);
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        assert!(
            !pipeline.probe_ready(),
            "the pod reported ready with {missing}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.cancel();
    task.await.expect("server task");
}

/// A listener that cannot bind keeps the pod not ready, while HTTP serves.
#[tokio::test]
async fn a_listener_that_cannot_bind_keeps_the_pod_not_ready() {
    let held = ClosedPort::loopback().expect("hold a TCP port");
    let mut config = base_config();
    config.fluent.enabled = true;
    config.fluent.bind_address = held.addr().to_string();
    let pipeline = pipeline_for(&config).await;

    let server = Server::new(pipeline.clone(), metrics());
    assert_never_ready(server, &pipeline, "Fluent Forward unable to bind").await;
}

/// A flow handler whose config it refuses keeps the pod not ready.
#[tokio::test]
async fn a_flow_handler_that_cannot_build_keeps_the_pod_not_ready() {
    // Unified and split at once, which FlowHandler::new refuses.
    let mut config = split_flow_config(0, 0);
    config.flow.enabled = true;
    let pipeline = pipeline_for(&config).await;

    let server = Server::with_flow_metrics(pipeline.clone(), metrics(), flow_metrics_for_test());
    assert_never_ready(server, &pipeline, "the flow handler unable to build").await;
}

/// Flow enabled with no port binds no socket, so it must keep the pod not ready.
#[tokio::test]
async fn flow_enabled_with_no_ports_keeps_the_pod_not_ready() {
    let mut config = flow_config();
    config.flow.enabled = true;
    config.flow.ports = Vec::new();
    let pipeline = pipeline_for(&config).await;

    let server = Server::with_flow_metrics(pipeline.clone(), metrics(), flow_metrics_for_test());
    assert_never_ready(server, &pipeline, "flow enabled and no flow port").await;
}

/// Flow enabled on a server built without flow metrics keeps the pod not ready.
#[tokio::test]
async fn flow_enabled_without_flow_metrics_keeps_the_pod_not_ready() {
    let mut config = flow_config();
    config.flow.enabled = true;
    config.flow.ports = vec![0];
    let pipeline = pipeline_for(&config).await;

    let server = Server::new(pipeline.clone(), metrics());
    assert_never_ready(server, &pipeline, "the flow handler never built").await;
}

// ---------------------------------------------------------------------------
// The ingest port's /readyz
// ---------------------------------------------------------------------------

/// The first status the ingest port's `/readyz` answers.
///
/// # Panics
///
/// When nothing answers within [`READY_DEADLINE`].
async fn first_readyz(http: SocketAddr) -> u16 {
    let url = format!("http://{http}/readyz");
    let deadline = Instant::now() + READY_DEADLINE;
    while Instant::now() < deadline {
        if let Some(status) = probe(&url).await {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("nothing answered {url} within {READY_DEADLINE:?}");
}

/// A destination outage is shared by every replica, so the ingest port's
/// `/readyz` must not fail on it while every listener serves.
#[tokio::test]
async fn the_ingest_readyz_passes_with_an_unhealthy_sink() {
    let spool = tempfile::tempdir().expect("spool dir");
    let destination = ClosedPort::loopback().expect("hold the destination's port");
    let http = ClosedPort::loopback()
        .expect("reserve the HTTP port")
        .release();

    let mut config = base_config();
    config.server.bind_address = http.to_string();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(format!("http://{}", destination.addr()));
    // A spool opens only for a listener that answers at enqueue.
    config.server.acknowledgements = AcknowledgementsConfig::new(false);
    // A usage ceiling no real filesystem is under reports the spool full, so the
    // destination's sink reads unhealthy on the first disk poll.
    config.buffer.spillover = SpilloverConfig {
        enabled: true,
        path: spool.path().to_path_buf(),
        max_usage_percent: 0.000_001,
        poll_interval_secs: 1,
        // On Kubernetes a spool outside a writable volume needs a cap.
        max_spool_bytes: Some(64 * 1024 * 1024),
    };
    let pipeline = pipeline_for(&config).await;

    let shutdown = CancellationToken::new();
    let server = run(Server::new(pipeline.clone(), metrics()), &shutdown);
    assert!(
        eventually(|| pipeline.probe_ready() && !pipeline.is_ready()).await,
        "the test needs every listener serving and the sink unhealthy"
    );

    assert_eq!(
        first_readyz(http).await,
        200,
        "the ingest /readyz failed on a sink outage every replica shares"
    );
    shutdown.cancel();
    server.await.expect("server task");
}

/// The ingest port's `/readyz` fails while an enabled listener is missing,
/// matching the kubelet probe.
#[tokio::test]
async fn the_ingest_readyz_fails_while_a_listener_is_missing() {
    let held = ClosedPort::loopback().expect("hold a TCP port");
    let http = ClosedPort::loopback()
        .expect("reserve the HTTP port")
        .release();

    let mut config = base_config();
    config.server.bind_address = http.to_string();
    config.fluent.enabled = true;
    config.fluent.bind_address = held.addr().to_string();
    let pipeline = pipeline_for(&config).await;

    let shutdown = CancellationToken::new();
    let server = run(Server::new(pipeline, metrics()), &shutdown);
    assert_eq!(
        first_readyz(http).await,
        503,
        "the ingest /readyz passed with Fluent Forward unable to bind"
    );
    shutdown.cancel();
    server.await.expect("server task");
}

// ---------------------------------------------------------------------------
// The binary: what a kubelet and an operator see
// ---------------------------------------------------------------------------

/// Kills the receiver on the way out so a failed assertion cannot leave it
/// holding its ports.
struct ServerProcess(Child);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The listeners of one binary run, each on the loopback address named.
struct RunPorts {
    ingest: SocketAddr,
    fluent: SocketAddr,
    gelf: SocketAddr,
    syslog_tcp: SocketAddr,
}

/// Start the receiver binary with Fluent Forward, GELF and syslog on `ports`,
/// logging to a file in `dir`. Returns the process, the metrics base URL and the
/// log path.
fn spawn_receiver(
    dir: &std::path::Path,
    ports: &RunPorts,
) -> (ServerProcess, String, std::path::PathBuf) {
    let metrics_port = free_port();
    let config_path = dir.join("receiver.yaml");
    let log_path = dir.join("receiver.log");
    let RunPorts {
        ingest,
        fluent,
        gelf,
        syslog_tcp,
    } = ports;

    let mut config = std::fs::File::create(&config_path).unwrap();
    write!(
        config,
        // The broker is never reached: rdkafka resolves it in the background, so
        // the process serves without one and this test needs no container.
        r#"server:
  bind_address: "{ingest}"
fluent:
  enabled: true
  bind_address: "{fluent}"
  accept_unauthenticated: true
gelf:
  enabled: true
  bind_address: "{gelf}"
  accept_unauthenticated: true
syslog:
  enabled: true
  udp_bind_address: "127.0.0.1:0"
  tcp_bind_address: "{syslog_tcp}"
  accept_unauthenticated: true
kafka:
  brokers:
    - "127.0.0.1:9092"
"#
    )
    .unwrap();
    config.flush().unwrap();

    let log = std::fs::File::create(&log_path).unwrap();
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
    (
        ServerProcess(child),
        format!("http://127.0.0.1:{metrics_port}"),
        log_path,
    )
}

/// The status and body a probe path answers, or None while nothing is listening.
async fn probe_body(url: &str) -> Option<(u16, String)> {
    let response = reqwest::get(url).await.ok()?;
    let status = response.status().as_u16();
    Some((status, response.text().await.unwrap_or_default()))
}

/// The status a probe path answers, or None while nothing is listening.
async fn probe(url: &str) -> Option<u16> {
    probe_body(url).await.map(|(status, _)| status)
}

/// Which listener's port a row holds before the receiver starts.
#[derive(Clone, Copy, PartialEq)]
enum Held {
    Fluent,
    Gelf,
    SyslogTcp,
}

impl Held {
    /// The handler name the ingest `/readyz` 503 lists.
    fn handler(self) -> &'static str {
        match self {
            Self::Fluent => "fluent-forward",
            Self::Gelf => "gelf",
            Self::SyslogTcp => "syslog",
        }
    }

    /// How the handler's error names the listener in the log.
    fn listener(self) -> &'static str {
        match self {
            Self::Fluent => "Fluent Forward",
            Self::Gelf => "GELF",
            Self::SyslogTcp => "syslog TCP",
        }
    }
}

/// Run the binary on one row and check what a kubelet and an operator see.
///
/// With a listener held the row waits for the failure to be logged, then expects
/// both `/readyz` endpoints to answer 503 with the ingest port naming the
/// handler and nothing else, the log holding the listener and the bind error,
/// the process live, and every other handler's port still accepting. With nothing held it expects both `/readyz` endpoints to answer 200.
async fn run_readiness_row(held: Option<Held>) -> Result<(), String> {
    let held_port = match held {
        Some(_) => Some(ClosedPort::loopback().map_err(|e| format!("hold a TCP port: {e}"))?),
        None => None,
    };
    let pick = |which: Held| match (&held_port, held) {
        (Some(port), Some(victim)) if victim == which => port.addr(),
        _ => free_addr(),
    };
    let ports = RunPorts {
        ingest: free_addr(),
        fluent: pick(Held::Fluent),
        gelf: pick(Held::Gelf),
        syslog_tcp: pick(Held::SyslogTcp),
    };
    let dir = tempfile::tempdir().map_err(|e| format!("temp dir: {e}"))?;
    let (mut receiver, base, log_path) = spawn_receiver(dir.path(), &ports);
    let log = || std::fs::read_to_string(&log_path).unwrap_or_default();
    let readyz = format!("{base}/readyz");
    let ingest_readyz = format!("http://{}/readyz", ports.ingest);

    let Some(victim) = held else {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = receiver.0.try_wait().unwrap() {
                return Err(format!("receiver exited with {status}:\n{}", log()));
            }
            let ready =
                probe(&readyz).await == Some(200) && probe(&ingest_readyz).await == Some(200);
            if ready {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "/readyz never passed with every port free:\n{}",
                    log()
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };

    let deadline = Instant::now() + Duration::from_secs(30);
    while !log().contains("Protocol handler failed") {
        if let Some(status) = receiver.0.try_wait().unwrap() {
            return Err(format!("receiver exited with {status}:\n{}", log()));
        }
        if probe(&readyz).await == Some(200) {
            return Err(format!(
                "/readyz passed with {} unable to bind:\n{}",
                victim.listener(),
                log()
            ));
        }
        if Instant::now() > deadline {
            return Err(format!(
                "no \"Protocol handler failed\" line in 30s:\n{}",
                log()
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The handlers run side by side, so the others bind when their turn comes.
    let bystanders = [
        (Held::Fluent, ports.fluent),
        (Held::Gelf, ports.gelf),
        (Held::SyslogTcp, ports.syslog_tcp),
    ]
    .into_iter()
    .filter(|(which, _)| *which != victim);
    for (_, addr) in bystanders {
        let deadline = Instant::now() + READY_DEADLINE;
        while tokio::net::TcpStream::connect(addr).await.is_err() {
            if Instant::now() > deadline {
                return Err(format!(
                    "{addr} stopped serving because {} failed:\n{}",
                    victim.listener(),
                    log()
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let kubelet = probe(&readyz).await;
    if kubelet != Some(503) {
        return Err(format!(
            "the metrics /readyz answered {kubelet:?}:\n{}",
            log()
        ));
    }
    let livez = probe(&format!("{base}/livez")).await;
    if livez != Some(200) {
        return Err(format!(
            "the process must stay live, /livez answered {livez:?}:\n{}",
            log()
        ));
    }

    let (status, body) = probe_body(&ingest_readyz)
        .await
        .ok_or_else(|| format!("nothing answered the ingest /readyz:\n{}", log()))?;
    if status != 503 {
        return Err(format!(
            "the ingest /readyz answered {status}: {body}\n{}",
            log()
        ));
    }
    let detail: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| format!("the 503 body is not JSON ({e}): {body}"))?;
    let handlers: Vec<&serde_json::Value> = detail["listeners"]
        .as_array()
        .map(|failed| failed.iter().map(|failure| &failure["handler"]).collect())
        .unwrap_or_default();
    if handlers != [victim.handler()] {
        return Err(format!(
            "the ingest /readyz 503 must list only {}: {body}",
            victim.handler()
        ));
    }
    // The probe is unauthenticated on an internet-facing port, so the bind error stays in the log.
    if body.contains("Address already in use") || body.contains(victim.listener()) {
        return Err(format!("the ingest /readyz 503 leaks the error: {body}"));
    }
    let failed_line = log()
        .lines()
        .find(|line| line.contains("Protocol handler failed"))
        .unwrap_or_default()
        .to_string();
    if failed_line.contains(victim.listener()) && failed_line.contains("Address already in use") {
        Ok(())
    } else {
        Err(format!(
            "the log line must name {} and its bind error: {failed_line}",
            victim.listener()
        ))
    }
}

/// `/readyz` stays 503 while an enabled listener cannot bind, the ingest port's
/// 503 names the handler, the log carries the listener and the bind error, and
/// the handlers that did bind keep serving. The last row is the control: every port free, both endpoints 200.
#[tokio::test]
async fn readiness_names_the_listener_that_cannot_bind() {
    let mut cases = Cases::default();
    for (case, held) in [
        ("fluent port taken", Some(Held::Fluent)),
        ("gelf port taken", Some(Held::Gelf)),
        ("syslog tcp port taken", Some(Held::SyslogTcp)),
        ("every port free", None),
    ] {
        let outcome = run_readiness_row(held).await;
        cases.check(case, outcome.is_ok(), || {
            outcome.clone().err().unwrap_or_default()
        });
    }
    cases.finish(String::new);
}

// ---------------------------------------------------------------------------
// The binary: configs that read as closed and are not refuse to start
// ---------------------------------------------------------------------------

/// Run `dfe-receiver config-check` on a config of `extra` YAML plus a broker,
/// and return whether it passed and what it printed.
fn config_check(extra: &str) -> (bool, String) {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("receiver.yaml");
    std::fs::write(
        &config_path,
        format!("kafka:\n  brokers:\n    - \"127.0.0.1:9092\"\n{extra}"),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_dfe-receiver"))
        .arg("--config")
        .arg(&config_path)
        .arg("config-check")
        .output()
        .expect("receiver binary should run");
    let printed = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), printed)
}

/// Each config is refused with a message naming the key that fixes it.
#[test]
fn a_config_that_reads_as_closed_and_is_not_refuses_to_start() {
    for (extra, names) in [
        (
            "server:\n  auth:\n    mode: bearrer\n",
            "server.auth.mode is 'bearrer'",
        ),
        (
            "server:\n  ip_filter:\n    mode: allowlist\n    cidrs: []\n",
            "server.ip_filter",
        ),
        (
            "server:\n  ip_filter:\n    mode: allowlist\n    cidrs: [\"10.0.0/8\"]\n",
            "'10.0.0/8'",
        ),
        (
            "server:\n  trusted_proxies: [\"proxy.internal\"]\n",
            "server.trusted_proxies",
        ),
        (
            "gelf:\n  enabled: true\n",
            "gelf.accept_unauthenticated: true",
        ),
        (
            "syslog:\n  enabled: true\n",
            "syslog.accept_unauthenticated: true",
        ),
    ] {
        let (passed, printed) = config_check(extra);
        assert!(!passed, "config-check passed:\n{extra}\n{printed}");
        assert!(
            printed.contains(names),
            "{names} is missing from:\n{printed}"
        );
    }
}

/// The positive control: the same listeners start once the opt-out is written.
#[test]
fn a_credential_less_listener_starts_with_the_opt_out() {
    let (passed, printed) = config_check(
        "gelf:\n  enabled: true\n  accept_unauthenticated: true\n\
         syslog:\n  enabled: true\n  accept_unauthenticated: true\n",
    );
    assert!(passed, "config-check refused the opt-out:\n{printed}");
}
