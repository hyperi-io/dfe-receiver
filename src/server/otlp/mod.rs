// Project:   dfe-receiver
// File:      src/server/otlp/mod.rs
// Purpose:   OTLP gRPC + HTTP receiver (OpenTelemetry Protocol)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! OTLP (OpenTelemetry Protocol) receiver.
//!
//! Accepts logs, metrics, and traces via:
//! - gRPC on port 4317 (standard OTLP gRPC port)
//! - HTTP on port 4318 (standard OTLP HTTP port)
//!
//! Converts OTLP protobuf messages to JSON and feeds them into the
//! processing pipeline. Supports two conversion modes:
//!
//! - **`hyperdx`**: JSON matching the OTel ClickHouse exporter schema
//!   for direct HyperDX compatibility
//! - **`generic`**: Normalised JSON envelope for custom routing

pub mod budget;
pub mod convert;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use scalo::transport::grpc::sender_deadline;
use tokio_util::sync::CancellationToken;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use crate::config::OtlpConfig;
use crate::error::{Error, Result, unavailable_response};
use crate::metrics::Metrics;
use crate::pipeline::{Acks, BatchOutcome, PipelineState};
use crate::server::auth::{AuthMode, TokenAuth, grpc_auth_interceptor, principal_or_listener};
use crate::server::grpc::{count_connections, count_oversize};
use crate::server::http::ConnectionLimits;
use crate::server::http::{Accept, create_auth_state};
use crate::server::tls::{TlsCertProvider, build_tls_acceptor, uses_secrets};
use crate::server::traits::{BoundAddr, Listeners, ProtocolHandler};
use crate::tenancy::{Ingest, Principal};
use convert::OtlpMode;

/// The transport label both OTLP listeners count under.
const TRANSPORT: &str = "otlp";

// ---------------------------------------------------------------------------
// Generated OTLP proto types
// ---------------------------------------------------------------------------
// Vendored from https://github.com/open-telemetry/opentelemetry-proto v1.5.0

#[doc(hidden)]
pub mod pb {
    // Generated prost/tonic types carry the upstream OpenTelemetry proto doc
    // comments verbatim (e.g. `schema: .../<version>`, `rejected_<signal>`),
    // which rustdoc parses as unclosed HTML tags. The code is generated into
    // `OUT_DIR` and regenerated every build, so the comments cannot be edited
    // at the source; allow the lint at the module boundary instead.
    #![allow(rustdoc::invalid_html_tags)]

    pub mod common {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.common.v1");
        }
    }
    pub mod resource {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.resource.v1");
        }
    }
    pub mod logs {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.logs.v1");
        }
    }
    pub mod metrics {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.metrics.v1");
        }
    }
    pub mod trace {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.trace.v1");
        }
    }
    pub mod collector {
        pub mod logs {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.collector.logs.v1");
            }
        }
        pub mod metrics {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.collector.metrics.v1");
            }
        }
        pub mod trace {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.collector.trace.v1");
            }
        }
    }
}

use pb::collector::logs::v1::logs_service_server::{LogsService, LogsServiceServer};
use pb::collector::metrics::v1::metrics_service_server::{MetricsService, MetricsServiceServer};
use pb::collector::trace::v1::trace_service_server::{TraceService, TraceServiceServer};

// ---------------------------------------------------------------------------
// OTLP gRPC service implementation
// ---------------------------------------------------------------------------

/// OTLP gRPC service handling logs, metrics, and traces.
#[derive(Clone)]
pub struct OtlpService {
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    mode: OtlpMode,
    acks: Acks,
}

impl OtlpService {
    fn new(
        pipeline: Arc<PipelineState>,
        metrics: Arc<Metrics>,
        mode: OtlpMode,
        acks: Acks,
    ) -> Self {
        Self {
            pipeline,
            metrics,
            mode,
            acks,
        }
    }

    /// Process converted payloads through the pipeline and settle the gRPC
    /// answer: `UNAVAILABLE` when a record could not be taken or, held, was
    /// not confirmed within the exporter's own deadline, `INVALID_ARGUMENT`
    /// when every record was refused for good, and otherwise OK with any
    /// refusals as a partial success.
    async fn process_payloads(
        &self,
        payloads: Vec<convert::ConvertedPayload>,
        deadline: Option<std::time::Duration>,
        ingest: &Ingest,
    ) -> std::result::Result<Option<Rejected>, Status> {
        let total_bytes: u64 = payloads.iter().map(|p| p.json.len() as u64).sum();
        self.metrics.add_bytes_received("otlp", total_bytes);

        let jsons: Vec<bytes::Bytes> = payloads.into_iter().map(|p| p.json).collect();
        let outcome = self
            .pipeline
            .process_batch_acked(&jsons, &self.acks, deadline, ingest)
            .await;
        let settled = settle(&self.metrics, jsons.len(), outcome);
        match settled {
            Settled::Taken(rejected) => Ok(rejected),
            Settled::Unavailable => Err(Status::unavailable(OVERLOADED)),
            Settled::AllRejected(message) => Err(Status::invalid_argument(message)),
        }
    }
}

/// Records of one export refused for good, reported as a partial success.
#[derive(Debug)]
struct Rejected {
    count: i64,
    message: String,
}

/// What an export came to, ahead of its wire answer.
#[derive(Debug)]
enum Settled {
    /// Every record settled, some refused for good or none.
    Taken(Option<Rejected>),
    /// A record could not be taken, so the sender must retry the export.
    Unavailable,
    /// Every record was refused for good.
    AllRejected(String),
}

/// The message on a retryable OTLP answer.
const OVERLOADED: &str = "server is overloaded, retry later";

/// Who sent an export: its token's credential, else the listener.
fn sender(principal: Option<&Principal>) -> Ingest {
    Ingest::sent(principal_or_listener(principal, TRANSPORT))
}

/// Settle a batch outcome into an OTLP answer, counting the request.
///
/// The OTLP specification makes `UNAVAILABLE` / HTTP 503 retryable and
/// `INTERNAL` / HTTP 500 final, and a client must not retry an export answered
/// with a populated `partial_success`.
fn settle(metrics: &Metrics, records: usize, outcome: BatchOutcome) -> Settled {
    // Debug, not warn: the pipeline logs the pressure edge once, and a line
    // per refused export would flood the log for as long as it lasts.
    if let Some(e) = outcome.unavailable {
        debug!(
            records,
            accepted = outcome.accepted,
            error = %e,
            "OTLP export not fully taken; answering retryable"
        );
        metrics.inc_requests_error("otlp");
        metrics.record_backpressure();
        return Settled::Unavailable;
    }
    let Some(e) = outcome.first_rejection else {
        metrics.inc_requests_success("otlp");
        return Settled::Taken(None);
    };
    metrics.inc_requests_error("otlp");
    if outcome.accepted == 0 {
        return Settled::AllRejected(e.public_message());
    }
    Settled::Taken(Some(Rejected {
        count: i64::try_from(outcome.rejected).unwrap_or(i64::MAX),
        message: e.public_message(),
    }))
}

#[tonic::async_trait]
impl LogsService for OtlpService {
    async fn export(
        &self,
        request: Request<pb::collector::logs::v1::ExportLogsServiceRequest>,
    ) -> std::result::Result<Response<pb::collector::logs::v1::ExportLogsServiceResponse>, Status>
    {
        self.metrics.inc_requests_total("otlp");
        let deadline = sender_deadline(request.metadata());
        let ingest = sender(request.extensions().get());
        let req = request.into_inner();

        let payloads = convert::convert_logs(&req, self.mode)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let rejected = self.process_payloads(payloads, deadline, &ingest).await?;
        Ok(Response::new(logs_response(rejected)))
    }
}

#[tonic::async_trait]
impl TraceService for OtlpService {
    async fn export(
        &self,
        request: Request<pb::collector::trace::v1::ExportTraceServiceRequest>,
    ) -> std::result::Result<Response<pb::collector::trace::v1::ExportTraceServiceResponse>, Status>
    {
        self.metrics.inc_requests_total("otlp");
        let deadline = sender_deadline(request.metadata());
        let ingest = sender(request.extensions().get());
        let req = request.into_inner();

        let payloads = convert::convert_traces(&req, self.mode)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let rejected = self.process_payloads(payloads, deadline, &ingest).await?;
        Ok(Response::new(traces_response(rejected)))
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpService {
    async fn export(
        &self,
        request: Request<pb::collector::metrics::v1::ExportMetricsServiceRequest>,
    ) -> std::result::Result<
        Response<pb::collector::metrics::v1::ExportMetricsServiceResponse>,
        Status,
    > {
        self.metrics.inc_requests_total("otlp");
        let deadline = sender_deadline(request.metadata());
        let ingest = sender(request.extensions().get());
        let req = request.into_inner();

        let payloads = convert::convert_metrics(&req, self.mode)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let rejected = self.process_payloads(payloads, deadline, &ingest).await?;
        Ok(Response::new(metrics_response(rejected)))
    }
}

/// A logs export response, with the refused log records as a partial success.
fn logs_response(rejected: Option<Rejected>) -> pb::collector::logs::v1::ExportLogsServiceResponse {
    pb::collector::logs::v1::ExportLogsServiceResponse {
        partial_success: rejected.map(|r| pb::collector::logs::v1::ExportLogsPartialSuccess {
            rejected_log_records: r.count,
            error_message: r.message,
        }),
    }
}

/// A traces export response, with the refused spans as a partial success.
fn traces_response(
    rejected: Option<Rejected>,
) -> pb::collector::trace::v1::ExportTraceServiceResponse {
    pb::collector::trace::v1::ExportTraceServiceResponse {
        partial_success: rejected.map(|r| pb::collector::trace::v1::ExportTracePartialSuccess {
            rejected_spans: r.count,
            error_message: r.message,
        }),
    }
}

/// A metrics export response, with the refused data points as a partial
/// success.
fn metrics_response(
    rejected: Option<Rejected>,
) -> pb::collector::metrics::v1::ExportMetricsServiceResponse {
    pb::collector::metrics::v1::ExportMetricsServiceResponse {
        partial_success: rejected.map(|r| {
            pb::collector::metrics::v1::ExportMetricsPartialSuccess {
                rejected_data_points: r.count,
                error_message: r.message,
            }
        }),
    }
}

// ---------------------------------------------------------------------------
// gRPC server runner
// ---------------------------------------------------------------------------

/// Run the OTLP gRPC server on the specified address.
async fn run_grpc_server(
    config: &OtlpConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    acks: Acks,
    shutdown: CancellationToken,
    bound: &BoundAddr,
) -> Result<()> {
    let addr: SocketAddr = config
        .grpc_bind_address
        .parse()
        .map_err(|e| Error::Config(format!("invalid OTLP gRPC bind address: {e}")))?;

    let mode = OtlpMode::from_str(&config.mode);
    let pipeline_server = pipeline.config().server;
    let service = OtlpService::new(pipeline, metrics.clone(), mode, acks);

    // Build TLS config if enabled
    let tls_config = if config.tls.enabled {
        let identity = super::tls::build_grpc_tls_config(&config.tls).await?;
        Some(identity)
    } else {
        None
    };

    // Build auth interceptor if configured.
    //
    // A failure here is fatal, not a downgrade: `auth_state: None` registers
    // the logs/traces/metrics services with no interceptor, so degrading would
    // serve three wide-open OTLP endpoints and report a successful start.
    let auth_state = if AuthMode::parse(&config.auth.mode) != Some(AuthMode::None) {
        Some(create_auth_state(&config.auth).await?)
    } else {
        None
    };

    let limits = ConnectionLimits::new(&pipeline_server.connections);
    let mut builder = crate::server::grpc::keep_alive(tonic::transport::Server::builder(), &limits);

    if let Some(tls) = tls_config {
        builder = builder
            .tls_config(tls)
            .map_err(|e| Error::Tls(format!("OTLP gRPC TLS error: {e}")))?;
    }
    let mut builder = builder.layer(count_oversize(metrics.clone(), TRANSPORT));

    // Register all three OTLP services on the same server, sharing one held
    // answer ceiling. tonic requires separate service instances for each trait
    // impl.
    let limit = config.max_message_size;
    // Gzip in both directions: the OpenTelemetry Collector's exporters compress with it by default,
    // and a server without the encoding enabled refuses the RPC.
    let gzip = tonic::codec::CompressionEncoding::Gzip;
    let logs_svc = LogsServiceServer::new(service.clone())
        .accept_compressed(gzip)
        .send_compressed(gzip)
        .max_decoding_message_size(limit);
    let traces_svc = TraceServiceServer::new(service.clone())
        .accept_compressed(gzip)
        .send_compressed(gzip)
        .max_decoding_message_size(limit);
    let metrics_svc = MetricsServiceServer::new(service)
        .accept_compressed(gzip)
        .send_compressed(gzip)
        .max_decoding_message_size(limit);

    let router = if let Some(auth) = auth_state {
        let interceptor = grpc_auth_interceptor(auth, metrics.clone(), TRANSPORT);
        builder
            .add_service(InterceptedService::new(logs_svc, interceptor.clone()))
            .add_service(InterceptedService::new(traces_svc, interceptor.clone()))
            .add_service(InterceptedService::new(metrics_svc, interceptor))
    } else {
        builder
            .add_service(logs_svc)
            .add_service(traces_svc)
            .add_service(metrics_svc)
    };

    // Bound here rather than inside tonic, which keeps the address it took to itself.
    // serve_with_incoming_shutdown drops the builder's TCP settings, so they go on the TcpIncoming.
    let incoming = TcpIncoming::bind(addr)
        .map_err(|e| Error::Server(format!("OTLP gRPC server error: {e}")))?
        .with_nodelay(Some(true));
    let _serving = bound.publish(&incoming.local_addr());

    info!(addr = %addr, mode = ?mode, "OTLP gRPC server listening");

    router
        .serve_with_incoming_shutdown(
            count_connections(incoming, metrics, TRANSPORT, limits),
            shutdown.cancelled_owned(),
        )
        .await
        .map_err(|e| Error::Server(format!("OTLP gRPC server error: {e}")))?;

    info!("OTLP gRPC server stopped");
    Ok(())
}

// ---------------------------------------------------------------------------
// HTTP server runner (OTLP HTTP on port 4318)
// ---------------------------------------------------------------------------

/// What each OTLP HTTP handler needs to take a request.
#[derive(Clone)]
struct OtlpHttpState {
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    mode: OtlpMode,
    acks: Acks,
    /// Bound on a request body after `Content-Encoding` is undone.
    max_body_size: usize,
}

/// Run the OTLP HTTP server for `/v1/logs`, `/v1/metrics`, `/v1/traces`.
async fn run_http_server(
    config: &OtlpConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    acks: Acks,
    shutdown: CancellationToken,
    bound: &BoundAddr,
) -> Result<()> {
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::routing::post;
    use axum::{Extension, Router};
    use tokio::net::TcpListener;

    let addr: SocketAddr = config
        .http_bind_address
        .parse()
        .map_err(|e| Error::Config(format!("invalid OTLP HTTP bind address: {e}")))?;

    let mode = OtlpMode::from_str(&config.mode);

    // `server.ip_filter` and `server.rate_limit` govern the ingest surface, not
    // one port of it. The gRPC endpoint on 4317 gets neither: tonic owns its
    // accept loop, so there is no place to run either control there.
    let server = pipeline.config().server;

    let state = OtlpHttpState {
        pipeline,
        metrics: metrics.clone(),
        mode,
        acks,
        max_body_size: server.max_body_size,
    };

    // Handler for OTLP HTTP logs
    async fn logs_handler(
        State(state): State<OtlpHttpState>,
        principal: Option<Extension<Principal>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> std::result::Result<axum::response::Response, Error> {
        state.metrics.inc_requests_total("otlp");
        state.metrics.add_bytes_received("otlp", body.len() as u64);

        let request = decode_otlp_request::<pb::collector::logs::v1::ExportLogsServiceRequest>(
            &headers,
            &body,
            state.max_body_size,
        )?;

        let payloads = convert::convert_logs(&request, state.mode)?;
        Ok(export_over_http(
            &state.pipeline,
            &state.metrics,
            &state.acks,
            payloads,
            &sender(principal.as_ref().map(|Extension(p)| p)),
            logs_response,
        )
        .await)
    }

    // Handler for OTLP HTTP traces
    async fn traces_handler(
        State(state): State<OtlpHttpState>,
        principal: Option<Extension<Principal>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> std::result::Result<axum::response::Response, Error> {
        state.metrics.inc_requests_total("otlp");
        state.metrics.add_bytes_received("otlp", body.len() as u64);

        let request = decode_otlp_request::<pb::collector::trace::v1::ExportTraceServiceRequest>(
            &headers,
            &body,
            state.max_body_size,
        )?;

        let payloads = convert::convert_traces(&request, state.mode)?;
        Ok(export_over_http(
            &state.pipeline,
            &state.metrics,
            &state.acks,
            payloads,
            &sender(principal.as_ref().map(|Extension(p)| p)),
            traces_response,
        )
        .await)
    }

    // Handler for OTLP HTTP metrics
    async fn metrics_handler(
        State(state): State<OtlpHttpState>,
        principal: Option<Extension<Principal>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> std::result::Result<axum::response::Response, Error> {
        state.metrics.inc_requests_total("otlp");
        state.metrics.add_bytes_received("otlp", body.len() as u64);

        let request = decode_otlp_request::<pb::collector::metrics::v1::ExportMetricsServiceRequest>(
            &headers,
            &body,
            state.max_body_size,
        )?;

        let payloads = convert::convert_metrics(&request, state.mode)?;
        Ok(export_over_http(
            &state.pipeline,
            &state.metrics,
            &state.acks,
            payloads,
            &sender(principal.as_ref().map(|Extension(p)| p)),
            metrics_response,
        )
        .await)
    }

    // `otlp.auth` and `otlp.tls` describe the OTLP receiver, not one half of
    // it. Both endpoints carry the same signals, so applying either to the gRPC
    // server alone leaves 4318 as an unauthenticated plaintext door beside a
    // closed 4317 -- with the config, and the startup log, reading as though
    // both were shut.
    let auth_state = create_auth_state(&config.auth).await?;

    let app = Router::new()
        .route("/v1/logs", post(logs_handler))
        .route("/v1/traces", post(traces_handler))
        .route("/v1/metrics", post(metrics_handler))
        .layer(axum::middleware::from_fn_with_state(
            TokenAuth::new(auth_state, metrics.clone(), TRANSPORT),
            crate::server::auth::token_auth_middleware,
        ))
        .with_state(state);

    // OtlpConfig has no body or time limit of its own, so the server-wide ones apply.
    let app = crate::server::http::apply_request_limits(
        app,
        server.max_body_size,
        Duration::from_millis(server.request_timeout_ms),
        metrics.clone(),
        TRANSPORT,
    );
    let app = crate::server::http::apply_server_limits(app, &server, metrics.clone(), TRANSPORT)?;

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Server(format!("failed to bind OTLP HTTP: {e}")))?;

    // Same TLS wiring as the Splunk HEC listener, over the same `tls:` block
    // the gRPC endpoint uses.
    let tls_provider = if config.tls.enabled && uses_secrets(&config.tls) {
        let provider = TlsCertProvider::new(config.tls.clone()).await?;
        provider.start_refresh_task();
        Some(provider)
    } else {
        None
    };
    let tls_acceptor = if tls_provider.is_some() {
        None
    } else {
        build_tls_acceptor(&config.tls)?
    };

    let accept = Accept::new(&server, metrics, TRANSPORT);

    let acceptor_handle = if let Some(ref provider) = tls_provider {
        Some(provider.acceptor_handle())
    } else {
        tls_acceptor.map(|a| Arc::new(parking_lot::RwLock::new(a)))
    };

    // Published once TLS is ready, so a failed TLS setup never reads as serving.
    let _serving = bound.publish(&listener.local_addr());

    if let Some(handle) = acceptor_handle {
        info!(addr = %addr, mode = ?mode, tls = true, "OTLP HTTP server listening");
        crate::server::http::run_tls_server(listener, app, handle, accept, shutdown).await?;
    } else {
        info!(addr = %addr, mode = ?mode, tls = false, "OTLP HTTP server listening");
        // The shared accept loop, not `axum::serve`: it runs the IP filter and
        // puts the peer address on each request for the rate limiter.
        crate::server::http::run_plain_server(listener, app, accept, shutdown).await?;
    }

    info!("OTLP HTTP server stopped");
    Ok(())
}

/// Run an OTLP/HTTP export through the pipeline and answer it: 503 with
/// `Retry-After` when a record could not be taken, or, held, was not
/// confirmed, 400 when every record was refused for good, otherwise 200 with
/// the protobuf export response, carrying any refusals as a partial success.
async fn export_over_http<R: prost::Message>(
    pipeline: &PipelineState,
    metrics: &Metrics,
    acks: &Acks,
    payloads: Vec<convert::ConvertedPayload>,
    ingest: &Ingest,
    response: fn(Option<Rejected>) -> R,
) -> axum::response::Response {
    use axum::http::{HeaderValue, StatusCode, header};
    use axum::response::IntoResponse;

    let jsons: Vec<bytes::Bytes> = payloads.into_iter().map(|p| p.json).collect();
    let outcome = pipeline
        .process_batch_acked(&jsons, acks, None, ingest)
        .await;
    match settle(metrics, jsons.len(), outcome) {
        Settled::Unavailable => unavailable_response(OVERLOADED),
        Settled::AllRejected(message) => (StatusCode::BAD_REQUEST, message).into_response(),
        Settled::Taken(rejected) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            )],
            response(rejected).encode_to_vec(),
        )
            .into_response(),
    }
}

/// Undo the request's `Content-Encoding`, keeping the decoded body within `limit` bytes.
///
/// `identity` and gzip are supported. The decoder stops one byte past `limit`, so a
/// small compressed body cannot expand into memory beyond it.
fn decode_content_encoding<'a>(
    headers: &axum::http::HeaderMap,
    body: &'a [u8],
    limit: usize,
) -> Result<std::borrow::Cow<'a, [u8]>> {
    use std::io::Read;

    let encoding = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map_or("identity", str::trim);
    if encoding.eq_ignore_ascii_case("identity") {
        return Ok(std::borrow::Cow::Borrowed(body));
    }
    if !encoding.eq_ignore_ascii_case("gzip") {
        return Err(Error::Validation(format!(
            "OTLP Content-Encoding '{encoding}' is not supported; use gzip or none"
        )));
    }
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(body)
        .take(limit as u64 + 1)
        .read_to_end(&mut decoded)
        .map_err(|e| Error::Validation(format!("OTLP gzip body is not valid: {e}")))?;
    if decoded.len() > limit {
        return Err(Error::Validation(format!(
            "OTLP body is over the {limit} byte limit once decompressed"
        )));
    }
    Ok(std::borrow::Cow::Owned(decoded))
}

/// Decode an OTLP HTTP request body (protobuf), gzip-compressed or not.
///
/// Supports `application/x-protobuf` (default) and `application/grpc`.
/// OTLP/JSON (`application/json`) support requires serde integration with
/// prost types and can be added when needed.
fn decode_otlp_request<T: prost::Message + Default + crate::server::budget::Budgeted>(
    headers: &axum::http::HeaderMap,
    body: &[u8],
    limit: usize,
) -> Result<T> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/x-protobuf");

    if content_type.contains("json") {
        // OTLP/JSON not yet supported -- requires serde derives on prost types
        Err(Error::Validation(
            "OTLP/JSON content-type not yet supported; use application/x-protobuf".into(),
        ))
    } else {
        // OTLP/protobuf (default)
        let body = decode_content_encoding(headers, body, limit)?;
        budget::admit::<T>(&body)?;
        <T as prost::Message>::decode(&*body)
            .map_err(|e| Error::Validation(format!("OTLP protobuf decode failed: {e}")))
    }
}

// ---------------------------------------------------------------------------
// ProtocolHandler implementation
// ---------------------------------------------------------------------------

/// OTLP protocol handler -- runs gRPC (4317) and HTTP (4318) servers.
pub struct OtlpHandler {
    config: OtlpConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    grpc_bound: BoundAddr,
    http_bound: BoundAddr,
}

impl OtlpHandler {
    pub fn new(config: OtlpConfig, pipeline: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        Self {
            config,
            pipeline,
            metrics,
            grpc_bound: BoundAddr::default(),
            http_bound: BoundAddr::default(),
        }
    }

    /// The address the gRPC listener bound, once [`ProtocolHandler::start`] binds it.
    #[must_use]
    pub fn grpc_bound_addr(&self) -> BoundAddr {
        self.grpc_bound.clone()
    }

    /// The address the HTTP listener bound, once [`ProtocolHandler::start`] binds it.
    #[must_use]
    pub fn http_bound_addr(&self) -> BoundAddr {
        self.http_bound.clone()
    }
}

#[async_trait::async_trait]
impl ProtocolHandler for OtlpHandler {
    fn name(&self) -> &'static str {
        "otlp"
    }

    fn bind_address(&self) -> &str {
        &self.config.grpc_bind_address
    }

    fn listeners(&self) -> Vec<BoundAddr> {
        vec![self.grpc_bound.clone(), self.http_bound.clone()]
    }

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        let mut listeners = Listeners::default();
        // One setting and one held-byte ceiling for both endpoints.
        let acks = self
            .pipeline
            .acks("otlp", self.config.acknowledgements, None);

        let grpc_config = self.config.clone();
        let grpc_pipeline = self.pipeline.clone();
        let grpc_metrics = self.metrics.clone();
        let grpc_acks = acks.clone();
        let grpc_shutdown = shutdown.clone();
        let grpc_bound = self.grpc_bound.clone();
        listeners.spawn("OTLP gRPC", async move {
            run_grpc_server(
                &grpc_config,
                grpc_pipeline,
                grpc_metrics,
                grpc_acks,
                grpc_shutdown,
                &grpc_bound,
            )
            .await
        });

        let http_config = self.config.clone();
        let http_pipeline = self.pipeline.clone();
        let http_metrics = self.metrics.clone();
        let http_shutdown = shutdown.clone();
        let http_bound = self.http_bound.clone();
        // An HTTP exporter sends no deadline, so its hold is configured.
        let http_acks = acks.holding_at_most(Duration::from_millis(self.config.http_max_hold_ms));
        listeners.spawn("OTLP HTTP", async move {
            run_http_server(
                &http_config,
                http_pipeline,
                http_metrics,
                http_acks,
                http_shutdown,
                &http_bound,
            )
            .await
        });

        listeners.run(&shutdown).await?;

        info!("OTLP handler stopped");
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io::Write;

    use axum::http::{HeaderMap, HeaderValue, header::CONTENT_ENCODING};

    use super::decode_content_encoding;

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn headers(encoding: Option<&'static str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(encoding) = encoding {
            headers.insert(CONTENT_ENCODING, HeaderValue::from_static(encoding));
        }
        headers
    }

    /// One request: its `Content-Encoding`, body and limit, and the decoded length or the error text expected.
    struct Case<'a> {
        encoding: Option<&'static str>,
        payload: &'a [u8],
        limit: usize,
        want: Result<usize, &'static str>,
    }

    #[test]
    fn content_encoding_cases() {
        let body = b"0123456789".repeat(100);
        let packed = gzip(&body);
        let bomb = gzip(&vec![0_u8; 1 << 20]);
        let case = |encoding, payload, limit, want| Case {
            encoding,
            payload,
            limit,
            want,
        };

        let cases = vec![
            case(None, &body[..], 1 << 20, Ok(body.len())),
            case(Some("identity"), &body[..], 1 << 20, Ok(body.len())),
            case(Some("gzip"), &packed[..], 1 << 20, Ok(body.len())),
            case(Some("GZIP"), &packed[..], 1 << 20, Ok(body.len())),
            case(Some("gzip"), &packed[..], body.len(), Ok(body.len())),
            case(Some("gzip"), &packed[..], body.len() - 1, Err("over the")),
            case(Some("gzip"), &bomb[..], 1024, Err("over the")),
            case(Some("gzip"), b"not gzip at all", 1 << 20, Err("not valid")),
            case(Some("zstd"), &packed[..], 1 << 20, Err("not supported")),
        ];
        for Case {
            encoding,
            payload,
            limit,
            want,
        } in cases
        {
            let got = decode_content_encoding(&headers(encoding), payload, limit);
            match want {
                Ok(len) => assert_eq!(got.unwrap().len(), len, "{encoding:?} limit {limit}"),
                Err(text) => {
                    let message = got.unwrap_err().to_string();
                    assert!(message.contains(text), "{encoding:?}: {message}");
                }
            }
        }
    }
}
