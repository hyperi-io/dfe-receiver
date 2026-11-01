// Project:   dfe-receiver
// File:      tests/integration/auth_reload.rs
// Purpose:   A reloaded credential takes effect on every authenticating listener
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A config reload that revokes a credential stops it working, and one that
//! adds a credential starts it working, on every listener that authenticates,
//! with no restart.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{AcceptedHeader, AuthConfig, Config, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::grpc::{GrpcVectorHandler, pb as grpc_pb};
use dfe_receiver::server::http::HttpHandler;
use dfe_receiver::server::prometheus_rw::PrometheusRwHandler;
use dfe_receiver::server::splunk_hec::SplunkHecHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use tokio_util::sync::CancellationToken;

/// How long a reload may take to reach a listener before the test fails.
const RELOAD_DEADLINE: Duration = Duration::from_secs(5);

/// A listener that authenticates.
#[derive(Debug, Clone, Copy)]
enum Listener {
    Http,
    SplunkHec,
    PrometheusRw,
    #[cfg(feature = "otlp")]
    OtlpHttp,
    #[cfg(feature = "otlp")]
    OtlpGrpc,
    Grpc,
}

impl Listener {
    /// The auth section of `config` this listener reads.
    fn auth(self, config: &mut Config) -> &mut AuthConfig {
        match self {
            Self::Http => &mut config.server.auth,
            Self::SplunkHec => &mut config.splunk_hec.auth,
            Self::PrometheusRw => &mut config.prometheus_rw.auth,
            #[cfg(feature = "otlp")]
            Self::OtlpHttp | Self::OtlpGrpc => &mut config.otlp.auth,
            Self::Grpc => &mut config.grpc.auth,
        }
    }

    /// Enable this listener in `config` on a free port.
    fn enable(self, config: &mut Config) {
        let any = "127.0.0.1:0".to_string();
        match self {
            Self::Http => config.server.bind_address = any,
            Self::SplunkHec => {
                config.splunk_hec.enabled = true;
                config.splunk_hec.bind_address = any;
            }
            Self::PrometheusRw => {
                config.prometheus_rw.enabled = true;
                config.prometheus_rw.bind_address = any;
            }
            #[cfg(feature = "otlp")]
            Self::OtlpHttp | Self::OtlpGrpc => {
                config.otlp.enabled = true;
                config.otlp.grpc_bind_address.clone_from(&any);
                config.otlp.http_bind_address = any;
            }
            Self::Grpc => {
                config.grpc.enabled = true;
                config.grpc.bind_address = any;
            }
        }
    }

    /// Start the listener over `pipeline` and return its address.
    async fn start(
        self,
        config: &Config,
        pipeline: Arc<PipelineState>,
    ) -> (SocketAddr, CancellationToken) {
        let metrics = Arc::new(Metrics::default());
        match self {
            Self::Http => {
                let handler =
                    HttpHandler::new(config.server.bind_address.clone(), pipeline, metrics);
                serve(handler, 0).await
            }
            Self::SplunkHec => {
                let handler = SplunkHecHandler::new(
                    config.splunk_hec.clone(),
                    config.raw_capture_for(&config.splunk_hec.raw_capture),
                    pipeline,
                    metrics,
                );
                serve(handler, 0).await
            }
            Self::PrometheusRw => {
                let handler =
                    PrometheusRwHandler::new(config.prometheus_rw.clone(), pipeline, metrics);
                serve(handler, 0).await
            }
            #[cfg(feature = "otlp")]
            Self::OtlpGrpc | Self::OtlpHttp => {
                let handler = dfe_receiver::server::otlp::OtlpHandler::new(
                    config.otlp.clone(),
                    pipeline,
                    metrics,
                );
                let index = usize::from(matches!(self, Self::OtlpHttp));
                serve(handler, index).await
            }
            Self::Grpc => serve(GrpcVectorHandler::new(config.clone(), pipeline, metrics), 0).await,
        }
    }

    /// Whether the listener at `addr` lets a request carrying `header: value`
    /// past authentication. What it then makes of the request does not matter.
    async fn admits(self, addr: SocketAddr, header: &str, value: &str) -> bool {
        let path = match self {
            Self::Http => "/ingest",
            Self::SplunkHec => "/services/collector/event",
            Self::PrometheusRw => "/api/v1/write",
            #[cfg(feature = "otlp")]
            Self::OtlpHttp => "/v1/logs",
            #[cfg(feature = "otlp")]
            Self::OtlpGrpc => return otlp_grpc_admits(addr, header, value).await,
            Self::Grpc => return grpc_admits(addr, header, value).await,
        };
        let status = reqwest::Client::new()
            .post(format!("http://{addr}{path}"))
            .header(header, value)
            .body("{}")
            .send()
            .await
            .expect("request")
            .status();
        status != reqwest::StatusCode::UNAUTHORIZED
    }
}

/// Start `handler` and return the address of its `index`th listener.
async fn serve<H: ProtocolHandler + 'static>(
    handler: H,
    index: usize,
) -> (SocketAddr, CancellationToken) {
    let name = handler.name();
    let listeners = handler.listeners();
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(token).await });
    let mut addrs = Vec::with_capacity(listeners.len());
    for bound in &listeners {
        addrs.push(crate::common::bound_addr(name, bound, &mut task).await);
    }
    (addrs[index], shutdown)
}

/// A gRPC request carrying `header: value` as metadata.
fn with_metadata<T>(message: T, header: &str, value: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(
        tonic::metadata::MetadataKey::from_bytes(header.as_bytes()).unwrap(),
        value.parse().unwrap(),
    );
    request
}

async fn grpc_admits(addr: SocketAddr, header: &str, value: &str) -> bool {
    use grpc_pb::vector::{PushEventsRequest, PushEventsResponse};

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let codec = tonic_prost::ProstCodec::<PushEventsRequest, PushEventsResponse>::default();
    let path = http::uri::PathAndQuery::from_static("/vector.Vector/PushEvents");
    let answer = client
        .unary(
            with_metadata(PushEventsRequest { events: vec![] }, header, value),
            path,
            codec,
        )
        .await;
    !matches!(answer, Err(ref status) if status.code() == tonic::Code::Unauthenticated)
}

#[cfg(feature = "otlp")]
async fn otlp_grpc_admits(addr: SocketAddr, header: &str, value: &str) -> bool {
    use dfe_receiver::server::otlp::pb::collector::logs::v1::ExportLogsServiceRequest;
    use dfe_receiver::server::otlp::pb::collector::logs::v1::logs_service_client::LogsServiceClient;

    let mut client = LogsServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let request = with_metadata(ExportLogsServiceRequest::default(), header, value);
    let answer = client.export(request).await;
    !matches!(answer, Err(ref status) if status.code() == tonic::Code::Unauthenticated)
}

/// The credential a reload revokes, the one it adds, and how each is sent.
struct Credentials {
    mode: &'static str,
    header: &'static str,
    old: &'static str,
    new: &'static str,
    set: fn(&mut AuthConfig, &str),
}

const BEARER: Credentials = Credentials {
    mode: "bearer",
    header: "authorization",
    old: "Bearer old-token",
    new: "Bearer new-token",
    set: |auth, value| {
        auth.bearer.tokens = vec![value.trim_start_matches("Bearer ").into()];
    },
};

const HEADER: Credentials = Credentials {
    mode: "header",
    header: "x-ingest-key",
    old: "old-key",
    new: "new-key",
    set: |auth, value| {
        auth.accepted_headers = vec![AcceptedHeader {
            name: "x-ingest-key".to_string(),
            values: vec![value.into()],
        }];
    },
};

/// A reload revoking `credentials.old` and adding `credentials.new` reaches
/// `listener` within [`RELOAD_DEADLINE`], with no restart.
async fn revocation(listener: Listener, credentials: &Credentials) {
    let mut config = Config::default();
    listener.enable(&mut config);
    let auth = listener.auth(&mut config);
    auth.mode = credentials.mode.to_string();
    (credentials.set)(auth, credentials.old);
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("pipeline init"),
    );
    let (addr, shutdown) = listener.start(&config, Arc::clone(&pipeline)).await;
    let header = credentials.header;
    assert!(
        listener.admits(addr, header, credentials.old).await,
        "{listener:?}: the configured credential is refused"
    );
    assert!(
        !listener.admits(addr, header, credentials.new).await,
        "{listener:?}: an unconfigured credential is admitted"
    );

    let mut reloaded = config.clone();
    (credentials.set)(listener.auth(&mut reloaded), credentials.new);
    pipeline.shared_config().update(reloaded);

    let deadline = tokio::time::Instant::now() + RELOAD_DEADLINE;
    let mut revoked = false;
    let mut added = false;
    while tokio::time::Instant::now() < deadline {
        revoked = !listener.admits(addr, header, credentials.old).await;
        added = listener.admits(addr, header, credentials.new).await;
        if revoked && added {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    shutdown.cancel();
    assert!(revoked, "{listener:?}: the revoked credential still works");
    assert!(added, "{listener:?}: the added credential does not work");
}

#[tokio::test]
async fn a_reloaded_bearer_token_reaches_http() {
    revocation(Listener::Http, &BEARER).await;
}

#[tokio::test]
async fn a_reloaded_header_credential_reaches_http() {
    revocation(Listener::Http, &HEADER).await;
}

#[tokio::test]
async fn a_reloaded_bearer_token_reaches_splunk_hec() {
    revocation(Listener::SplunkHec, &BEARER).await;
}

#[tokio::test]
async fn a_reloaded_bearer_token_reaches_prometheus_remote_write() {
    revocation(Listener::PrometheusRw, &BEARER).await;
}

#[cfg(feature = "otlp")]
#[tokio::test]
async fn a_reloaded_bearer_token_reaches_otlp_http() {
    revocation(Listener::OtlpHttp, &BEARER).await;
}

#[cfg(feature = "otlp")]
#[tokio::test]
async fn a_reloaded_bearer_token_reaches_otlp_grpc() {
    revocation(Listener::OtlpGrpc, &BEARER).await;
}

#[tokio::test]
async fn a_reloaded_bearer_token_reaches_grpc() {
    revocation(Listener::Grpc, &BEARER).await;
}
