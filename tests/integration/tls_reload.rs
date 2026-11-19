// Project:   dfe-receiver
// File:      tests/integration/tls_reload.rs
// Purpose:   A rotated certificate reaches every TLS listener family with no restart
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A certificate rotated on disk, or behind a `file:` secret, is served from the
//! next handshake on every TLS listener, with no restart.
//!
//! Each case starts one handler with `tls.refresh_interval_secs: 1`, reads the
//! certificate a real TLS client is served, replaces the certificate and key,
//! and waits for the client to be served the new one. The three families run
//! their own accept path: the hyper listeners (`/ingest`, webhook, Splunk HEC,
//! remote write), the raw TCP listeners (syslog, Fluent Forward, GELF,
//! Lumberjack) and the tonic listeners (Vector gRPC, OTLP). The tonic listeners
//! also answer a request over the handshake the receiver runs for them.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{
    Config, SharedConfig, TlsConfig, WebhookAuthConfig, WebhookAuthMode, WebhookCallerConfig,
};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::fluent::FluentHandler;
use dfe_receiver::server::gelf::GelfHandler;
use dfe_receiver::server::grpc::{GrpcVectorHandler, pb as grpc_pb};
use dfe_receiver::server::http::HttpHandler;
use dfe_receiver::server::lumberjack::LumberjackHandler;
use dfe_receiver::server::prometheus_rw::PrometheusRwHandler;
use dfe_receiver::server::splunk_hec::SplunkHecHandler;
use dfe_receiver::server::syslog::SyslogHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use dfe_receiver::server::webhook::WebhookHandler;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_util::sync::CancellationToken;

use crate::common::bound_addr;

/// How long a rotated certificate gets to reach a listener, which re-reads
/// every second.
const ROTATION_DEADLINE: Duration = Duration::from_secs(20);

/// A throwaway CA that signs the certificates a case rotates between.
struct Ca {
    issuer: CertifiedIssuer<'static, KeyPair>,
}

/// A certificate and its key, signed by a [`Ca`].
struct Leaf {
    cert_pem: String,
    key_pem: String,
    der: CertificateDer<'static>,
}

impl Ca {
    fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params
            .distinguished_name
            .push(DnType::CommonName, "tls reload test ca");
        let issuer = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
        Self { issuer }
    }

    /// A `localhost` server certificate named `name`.
    fn server(&self, name: &str) -> Leaf {
        let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        params.distinguished_name.push(DnType::CommonName, name);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        Leaf {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            der: cert.der().clone(),
        }
    }

    /// A TLS client that trusts only this CA.
    fn client(&self) -> TlsConnector {
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(self.issuer.pem().as_bytes()) {
            roots.add(cert.unwrap()).unwrap();
        }
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        TlsConnector::from(Arc::new(config))
    }
}

/// Replace `path` in one rename, so a reader never sees half a file.
fn write(path: &Path, contents: &str) {
    let staged = path.with_extension("staged");
    std::fs::write(&staged, contents).unwrap();
    std::fs::rename(&staged, path).unwrap();
}

/// Where a listener's certificate and key come from.
#[derive(Debug, Clone, Copy)]
enum Source {
    File,
    /// A `file:` secret spec, read through the same resolver as `vault:`.
    Secret,
}

/// The certificate and key a case rotates, in a directory.
struct Files {
    dir: TempDir,
}

impl Files {
    fn new(leaf: &Leaf) -> Self {
        let files = Self {
            dir: TempDir::new().unwrap(),
        };
        files.serve(leaf);
        files
    }

    fn cert(&self) -> PathBuf {
        self.dir.path().join("cert.pem")
    }

    fn key(&self) -> PathBuf {
        self.dir.path().join("key.pem")
    }

    fn serve(&self, leaf: &Leaf) {
        write(&self.cert(), &leaf.cert_pem);
        write(&self.key(), &leaf.key_pem);
    }

    /// A `tls:` block reading these files from `source`, re-read every second.
    fn tls(&self, source: Source) -> TlsConfig {
        let tls = TlsConfig {
            enabled: true,
            refresh_interval_secs: Some(1),
            ..TlsConfig::default()
        };
        match source {
            Source::File => TlsConfig {
                cert_file: Some(self.cert().display().to_string()),
                key_file: Some(self.key().display().to_string()),
                ..tls
            },
            Source::Secret => TlsConfig {
                cert_secret: Some(format!("file:{}", self.cert().display())),
                key_secret: Some(format!("file:{}", self.key().display())),
                ..tls
            },
        }
    }
}

/// One TLS listener under test.
#[derive(Debug, Clone, Copy)]
enum Listener {
    Http,
    Webhook,
    SplunkHec,
    PrometheusRw,
    Syslog,
    Fluent,
    Gelf,
    Lumberjack,
    Grpc,
    #[cfg(feature = "otlp")]
    Otlp,
}

/// A handler started on free ports, and what stops it.
struct Running {
    /// Each TLS listener it bound, by name.
    tls: Vec<(&'static str, SocketAddr)>,
    shutdown: CancellationToken,
    /// Holds the webhook caller's secret for as long as it runs.
    _scratch: TempDir,
}

fn base_config() -> Config {
    let mut config = Config::default();
    // The loader on its memory transport: accepted, sent nowhere, no broker.
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    config
}

impl Listener {
    /// Start the listener with `tls` and wait for every listener it binds.
    async fn start(self, tls: TlsConfig) -> Running {
        let any = || "127.0.0.1:0".to_string();
        let scratch = TempDir::new().unwrap();
        let mut config = base_config();
        match self {
            Self::Http => config.server.tls = tls,
            Self::Webhook => {
                let secret = scratch.path().join("webhook-secret");
                std::fs::write(&secret, "probe-secret").unwrap();
                // A caller delivers to a Kafka topic, so the config names a broker.
                config.kafka.brokers = vec!["127.0.0.1:1".to_string()];
                config.webhook.enabled = true;
                config.webhook.bind_address = Some(any());
                config.webhook.callers = vec![WebhookCallerConfig {
                    name: "probe".to_string(),
                    topic: "probe".to_string(),
                    auth: WebhookAuthConfig {
                        mode: WebhookAuthMode::Header,
                        secret_source: format!("file:{}", secret.display()),
                        header: "x-probe-secret".to_string(),
                        ..WebhookAuthConfig::default()
                    },
                    ..WebhookCallerConfig::default()
                }];
                config.webhook.tls = tls;
            }
            Self::SplunkHec => {
                config.splunk_hec.enabled = true;
                config.splunk_hec.bind_address = any();
                config.splunk_hec.auth.mode = "none".to_string();
                config.splunk_hec.tls = tls;
            }
            Self::PrometheusRw => {
                config.prometheus_rw.enabled = true;
                config.prometheus_rw.bind_address = any();
                config.prometheus_rw.auth.mode = "none".to_string();
                config.prometheus_rw.tls = tls;
            }
            Self::Syslog => {
                config.syslog.enabled = true;
                config.syslog.udp_bind_address = any();
                config.syslog.tcp_bind_address = any();
                config.syslog.tls_bind_address = any();
                config.syslog.accept_unauthenticated = true;
                config.syslog.tls = tls;
            }
            Self::Fluent => {
                config.fluent.enabled = true;
                config.fluent.bind_address = any();
                config.fluent.accept_unauthenticated = true;
                config.fluent.tls = tls;
            }
            Self::Gelf => {
                config.gelf.enabled = true;
                config.gelf.bind_address = any();
                config.gelf.accept_unauthenticated = true;
                config.gelf.tls = tls;
            }
            Self::Lumberjack => {
                config.lumberjack.enabled = true;
                config.lumberjack.bind_address = any();
                config.lumberjack.accept_unauthenticated = true;
                config.lumberjack.tls = tls;
            }
            Self::Grpc => {
                config.grpc.enabled = true;
                config.grpc.bind_address = any();
                config.grpc.tls = tls;
            }
            #[cfg(feature = "otlp")]
            Self::Otlp => {
                config.otlp.enabled = true;
                config.otlp.grpc_bind_address = any();
                config.otlp.http_bind_address = any();
                config.otlp.auth.mode = "none".to_string();
                config.otlp.tls = tls;
            }
        }
        let pipeline = Arc::new(
            PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
                .await
                .expect("build pipeline"),
        );
        let metrics = Arc::new(Metrics::default());

        // The handler, and the name and index of each of its TLS listeners.
        let (handler, tls_listeners): (Box<dyn ProtocolHandler>, Vec<(&'static str, usize)>) =
            match self {
                Self::Http => (
                    Box::new(HttpHandler::new(
                        config.server.bind_address.clone(),
                        pipeline,
                        metrics,
                    )),
                    vec![("http", 0)],
                ),
                Self::Webhook => (
                    Box::new(WebhookHandler::new(config.clone(), pipeline, metrics)),
                    vec![("webhook", 0)],
                ),
                Self::SplunkHec => (
                    Box::new(SplunkHecHandler::new(
                        config.splunk_hec.clone(),
                        config.raw_capture_for(&config.splunk_hec.raw_capture),
                        pipeline,
                        metrics,
                    )),
                    vec![("splunk_hec", 0)],
                ),
                Self::PrometheusRw => (
                    Box::new(PrometheusRwHandler::new(
                        config.prometheus_rw.clone(),
                        pipeline,
                        metrics,
                    )),
                    vec![("prometheus_rw", 0)],
                ),
                // UDP and plain TCP bind first, the TLS listener last.
                Self::Syslog => (
                    Box::new(SyslogHandler::new(
                        config.syslog.clone(),
                        config.raw_capture_for(&config.syslog.raw_capture),
                        pipeline,
                        metrics,
                    )),
                    vec![("syslog tls", 2)],
                ),
                Self::Fluent => (
                    Box::new(FluentHandler::new(config.fluent.clone(), pipeline, metrics)),
                    vec![("fluent", 0)],
                ),
                Self::Gelf => (
                    Box::new(GelfHandler::new(config.gelf.clone(), pipeline, metrics)),
                    vec![("gelf", 0)],
                ),
                Self::Lumberjack => (
                    Box::new(LumberjackHandler::new(
                        config.lumberjack.clone(),
                        pipeline,
                        metrics,
                    )),
                    vec![("lumberjack", 0)],
                ),
                Self::Grpc => (
                    Box::new(GrpcVectorHandler::new(config.clone(), pipeline, metrics)),
                    vec![("grpc", 0)],
                ),
                #[cfg(feature = "otlp")]
                Self::Otlp => (
                    Box::new(dfe_receiver::server::otlp::OtlpHandler::new(
                        config.otlp.clone(),
                        pipeline,
                        metrics,
                    )),
                    vec![("otlp grpc", 0), ("otlp http", 1)],
                ),
            };

        let name = handler.name();
        let bounds = handler.listeners();
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let mut task = tokio::spawn(async move { handler.start(token).await });
        let mut addrs = Vec::with_capacity(bounds.len());
        for bound in &bounds {
            addrs.push(bound_addr(name, bound, &mut task).await);
        }
        Running {
            tls: tls_listeners
                .into_iter()
                .map(|(label, index)| (label, addrs[index]))
                .collect(),
            shutdown,
            _scratch: scratch,
        }
    }
}

/// The certificate `addr` serves a client that trusts `ca`, `None` when the
/// handshake does not complete.
async fn served(client: &TlsConnector, addr: SocketAddr) -> Option<CertificateDer<'static>> {
    let stream = TcpStream::connect(addr).await.ok()?;
    let secured = client
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .ok()?;
    let (_, connection) = secured.get_ref();
    connection.peer_certificates()?.first().cloned()
}

/// Whether `addr` serves `expected` within [`ROTATION_DEADLINE`].
async fn serves_within_deadline(
    client: &TlsConnector,
    addr: SocketAddr,
    expected: &CertificateDer<'static>,
) -> bool {
    let deadline = tokio::time::Instant::now() + ROTATION_DEADLINE;
    loop {
        if served(client, addr).await.as_ref() == Some(expected) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Start `listener` on `source`, check it serves the first certificate, rotate,
/// and check it serves the second without a restart.
async fn rotation_reaches(listener: Listener, source: Source) {
    let ca = Ca::new();
    let (first, second) = (ca.server("first"), ca.server("second"));
    let files = Files::new(&first);
    let client = ca.client();

    let running = listener.start(files.tls(source)).await;
    for (name, addr) in &running.tls {
        assert!(
            serves_within_deadline(&client, *addr, &first.der).await,
            "{listener:?} {source:?}: {name} does not serve the first certificate"
        );
    }

    files.serve(&second);
    for (name, addr) in &running.tls {
        assert!(
            serves_within_deadline(&client, *addr, &second.der).await,
            "{listener:?} {source:?}: {name} still serves the retired certificate"
        );
    }
    running.shutdown.cancel();
}

/// Every listener of `family`, on a file and on a secret, at once.
async fn family_rotates(family: &[Listener]) {
    let mut cases = tokio::task::JoinSet::new();
    for &listener in family {
        for source in [Source::File, Source::Secret] {
            cases.spawn(rotation_reaches(listener, source));
        }
    }
    while let Some(outcome) = cases.join_next().await {
        outcome.expect("a rotation case panicked");
    }
}

#[tokio::test]
async fn the_hyper_listeners_serve_a_rotated_certificate() {
    family_rotates(&[
        Listener::Http,
        Listener::Webhook,
        Listener::SplunkHec,
        Listener::PrometheusRw,
    ])
    .await;
}

#[tokio::test]
async fn the_raw_tcp_listeners_serve_a_rotated_certificate() {
    family_rotates(&[
        Listener::Syslog,
        Listener::Fluent,
        Listener::Gelf,
        Listener::Lumberjack,
    ])
    .await;
}

#[tokio::test]
async fn the_tonic_listeners_serve_a_rotated_certificate() {
    #[cfg(feature = "otlp")]
    family_rotates(&[Listener::Grpc, Listener::Otlp]).await;
    #[cfg(not(feature = "otlp"))]
    family_rotates(&[Listener::Grpc]).await;
}

/// A tonic channel to `addr` that trusts `ca_pem`, speaking TLS to `localhost`.
async fn grpc_channel(ca_pem: &str, addr: SocketAddr) -> tonic::transport::Channel {
    let tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(ca_pem))
        .domain_name("localhost");
    tonic::transport::Channel::from_shared(format!("https://{addr}"))
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect()
        .await
        .expect("a gRPC client completes the TLS handshake")
}

/// The handshake is run by the receiver rather than tonic, so a gRPC client
/// has to negotiate `h2` and get an answer back over the stream it hands tonic.
#[tokio::test]
async fn grpc_answers_requests_over_the_receivers_handshake() {
    use grpc_pb::vector::{HealthCheckRequest, HealthCheckResponse};

    let ca = Ca::new();
    let files = Files::new(&ca.server("grpc"));
    let running = Listener::Grpc.start(files.tls(Source::File)).await;
    let (_, addr) = running.tls[0];

    let channel = grpc_channel(&ca.issuer.pem(), addr).await;
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let codec = tonic_prost::ProstCodec::<HealthCheckRequest, HealthCheckResponse>::default();
    let path = http::uri::PathAndQuery::from_static("/vector.Vector/HealthCheck");
    client
        .unary(
            tonic::Request::new(HealthCheckRequest::default()),
            path,
            codec,
        )
        .await
        .expect("the health check is answered over TLS");
    running.shutdown.cancel();
}

#[cfg(feature = "otlp")]
#[tokio::test]
async fn otlp_grpc_answers_requests_over_the_receivers_handshake() {
    use dfe_receiver::server::otlp::pb::collector::logs::v1::ExportLogsServiceRequest;
    use dfe_receiver::server::otlp::pb::collector::logs::v1::logs_service_client::LogsServiceClient;

    let ca = Ca::new();
    let files = Files::new(&ca.server("otlp"));
    let running = Listener::Otlp.start(files.tls(Source::File)).await;
    let (_, addr) = running.tls[0];

    let mut client = LogsServiceClient::new(grpc_channel(&ca.issuer.pem(), addr).await);
    client
        .export(ExportLogsServiceRequest::default())
        .await
        .expect("an empty export is accepted over TLS");
    running.shutdown.cancel();
}
