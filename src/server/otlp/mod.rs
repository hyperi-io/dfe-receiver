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

pub mod convert;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::config::OtlpConfig;
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::auth::{AuthMode, AuthState, validate_bearer_auth};
use crate::server::http::create_auth_state;
use crate::server::traits::ProtocolHandler;
use convert::OtlpMode;

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
pub struct OtlpService {
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    mode: OtlpMode,
}

impl OtlpService {
    fn new(pipeline: Arc<PipelineState>, metrics: Arc<Metrics>, mode: OtlpMode) -> Self {
        Self {
            pipeline,
            metrics,
            mode,
        }
    }

    /// Process converted payloads through the pipeline.
    async fn process_payloads(
        &self,
        payloads: Vec<convert::ConvertedPayload>,
    ) -> std::result::Result<(), Status> {
        let total_bytes: u64 = payloads.iter().map(|p| p.json.len() as u64).sum();
        self.metrics.add_bytes_received("otlp", total_bytes);

        let jsons: Vec<bytes::Bytes> = payloads.iter().map(|p| p.json.clone()).collect();
        let (success, first_err) = self.pipeline.process_batch(&jsons).await;

        if let Some(e) = first_err {
            let failed = payloads.len() - success;
            warn!(
                success = success,
                failed = failed,
                error = %e,
                "OTLP batch partially failed"
            );
            if success == 0 {
                self.metrics.inc_requests_error("otlp");
                return Err(Status::internal(e.to_string()));
            }
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl LogsService for OtlpService {
    async fn export(
        &self,
        request: Request<pb::collector::logs::v1::ExportLogsServiceRequest>,
    ) -> std::result::Result<Response<pb::collector::logs::v1::ExportLogsServiceResponse>, Status>
    {
        self.metrics.inc_requests_total("otlp");
        let req = request.into_inner();

        let payloads = convert::convert_logs(&req, self.mode)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        self.process_payloads(payloads).await?;
        self.metrics.inc_requests_success("otlp");

        Ok(Response::new(
            pb::collector::logs::v1::ExportLogsServiceResponse {
                partial_success: None,
            },
        ))
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
        let req = request.into_inner();

        let payloads = convert::convert_traces(&req, self.mode)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        self.process_payloads(payloads).await?;
        self.metrics.inc_requests_success("otlp");

        Ok(Response::new(
            pb::collector::trace::v1::ExportTraceServiceResponse {
                partial_success: None,
            },
        ))
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
        let req = request.into_inner();

        let payloads = convert::convert_metrics(&req, self.mode)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        self.process_payloads(payloads).await?;
        self.metrics.inc_requests_success("otlp");

        Ok(Response::new(
            pb::collector::metrics::v1::ExportMetricsServiceResponse {
                partial_success: None,
            },
        ))
    }
}

// ---------------------------------------------------------------------------
// Auth interceptor (reuse pattern from gRPC/Vector)
// ---------------------------------------------------------------------------

fn make_auth_interceptor(
    auth: AuthState,
) -> impl Fn(Request<()>) -> std::result::Result<Request<()>, Status> + Clone {
    move |req: Request<()>| {
        let mut headers = axum::http::HeaderMap::new();
        if let Some(auth_value) = req.metadata().get("authorization")
            && let Ok(s) = auth_value.to_str()
            && let Ok(hv) = axum::http::HeaderValue::from_str(s)
        {
            headers.insert("authorization", hv);
        }

        if let Some(err) = validate_bearer_auth(&auth, &headers) {
            return Err(Status::unauthenticated(err.message));
        }

        Ok(req)
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
    shutdown: CancellationToken,
) -> Result<()> {
    let addr: SocketAddr = config
        .grpc_bind_address
        .parse()
        .map_err(|e| Error::Config(format!("invalid OTLP gRPC bind address: {e}")))?;

    let mode = OtlpMode::from_str(&config.mode);
    let service = Arc::new(OtlpService::new(pipeline, metrics, mode));

    // Build TLS config if enabled
    let tls_config = if config.tls.enabled {
        let identity = super::tls::build_grpc_tls_config(&config.tls).await?;
        Some(identity)
    } else {
        None
    };

    // Build auth interceptor if configured
    let auth_state = if AuthMode::from_str(&config.auth.mode) != AuthMode::None {
        match create_auth_state(&config.auth).await {
            Ok(auth) => Some(auth),
            Err(e) => {
                warn!(error = %e, "Failed to create OTLP auth state, running without auth");
                None
            }
        }
    } else {
        None
    };

    let mut builder = tonic::transport::Server::builder();

    if let Some(tls) = tls_config {
        builder = builder
            .tls_config(tls)
            .map_err(|e| Error::Tls(format!("OTLP gRPC TLS error: {e}")))?;
    }

    // Register all three OTLP services on the same server.
    // tonic requires separate service instances for each trait impl.
    let logs_svc = OtlpService::new(service.pipeline.clone(), service.metrics.clone(), mode);
    let traces_svc = OtlpService::new(service.pipeline.clone(), service.metrics.clone(), mode);
    let metrics_svc = OtlpService::new(service.pipeline.clone(), service.metrics.clone(), mode);

    let router = if let Some(auth) = auth_state {
        let interceptor = make_auth_interceptor(auth);
        builder
            .add_service(LogsServiceServer::with_interceptor(
                logs_svc,
                interceptor.clone(),
            ))
            .add_service(TraceServiceServer::with_interceptor(
                traces_svc,
                interceptor.clone(),
            ))
            .add_service(MetricsServiceServer::with_interceptor(
                metrics_svc,
                interceptor,
            ))
    } else {
        builder
            .add_service(LogsServiceServer::new(logs_svc))
            .add_service(TraceServiceServer::new(traces_svc))
            .add_service(MetricsServiceServer::new(metrics_svc))
    };

    info!(addr = %addr, mode = ?mode, "OTLP gRPC server listening");

    router
        .serve_with_shutdown(addr, shutdown.cancelled_owned())
        .await
        .map_err(|e| Error::Server(format!("OTLP gRPC server error: {e}")))?;

    info!("OTLP gRPC server stopped");
    Ok(())
}

// ---------------------------------------------------------------------------
// HTTP server runner (OTLP HTTP on port 4318)
// ---------------------------------------------------------------------------

/// Run the OTLP HTTP server for `/v1/logs`, `/v1/metrics`, `/v1/traces`.
async fn run_http_server(
    config: &OtlpConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<()> {
    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use tokio::net::TcpListener;

    #[derive(Clone)]
    struct OtlpHttpState {
        pipeline: Arc<PipelineState>,
        metrics: Arc<Metrics>,
        mode: OtlpMode,
    }

    let addr: SocketAddr = config
        .http_bind_address
        .parse()
        .map_err(|e| Error::Config(format!("invalid OTLP HTTP bind address: {e}")))?;

    let mode = OtlpMode::from_str(&config.mode);

    let state = OtlpHttpState {
        pipeline,
        metrics,
        mode,
    };

    // Handler for OTLP HTTP logs
    async fn logs_handler(
        State(state): State<OtlpHttpState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> std::result::Result<StatusCode, Error> {
        state.metrics.inc_requests_total("otlp");
        state.metrics.add_bytes_received("otlp", body.len() as u64);

        let request = decode_otlp_request::<pb::collector::logs::v1::ExportLogsServiceRequest>(
            &headers, &body,
        )?;

        let payloads = convert::convert_logs(&request, state.mode)?;
        let jsons: Vec<Bytes> = payloads.into_iter().map(|p| p.json).collect();
        let (_, first_err) = state.pipeline.process_batch(&jsons).await;
        if let Some(e) = first_err {
            return Err(e);
        }

        state.metrics.inc_requests_success("otlp");
        Ok(StatusCode::OK)
    }

    // Handler for OTLP HTTP traces
    async fn traces_handler(
        State(state): State<OtlpHttpState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> std::result::Result<StatusCode, Error> {
        state.metrics.inc_requests_total("otlp");
        state.metrics.add_bytes_received("otlp", body.len() as u64);

        let request = decode_otlp_request::<pb::collector::trace::v1::ExportTraceServiceRequest>(
            &headers, &body,
        )?;

        let payloads = convert::convert_traces(&request, state.mode)?;
        let jsons: Vec<Bytes> = payloads.into_iter().map(|p| p.json).collect();
        let (_, first_err) = state.pipeline.process_batch(&jsons).await;
        if let Some(e) = first_err {
            return Err(e);
        }

        state.metrics.inc_requests_success("otlp");
        Ok(StatusCode::OK)
    }

    // Handler for OTLP HTTP metrics
    async fn metrics_handler(
        State(state): State<OtlpHttpState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> std::result::Result<StatusCode, Error> {
        state.metrics.inc_requests_total("otlp");
        state.metrics.add_bytes_received("otlp", body.len() as u64);

        let request = decode_otlp_request::<pb::collector::metrics::v1::ExportMetricsServiceRequest>(
            &headers, &body,
        )?;

        let payloads = convert::convert_metrics(&request, state.mode)?;
        let jsons: Vec<Bytes> = payloads.into_iter().map(|p| p.json).collect();
        let (_, first_err) = state.pipeline.process_batch(&jsons).await;
        if let Some(e) = first_err {
            return Err(e);
        }

        state.metrics.inc_requests_success("otlp");
        Ok(StatusCode::OK)
    }

    let app = Router::new()
        .route("/v1/logs", post(logs_handler))
        .route("/v1/traces", post(traces_handler))
        .route("/v1/metrics", post(metrics_handler))
        .with_state(state);

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Server(format!("failed to bind OTLP HTTP: {e}")))?;

    info!(addr = %addr, mode = ?mode, "OTLP HTTP server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
        .map_err(|e| Error::Server(format!("OTLP HTTP server error: {e}")))?;

    info!("OTLP HTTP server stopped");
    Ok(())
}

/// Decode an OTLP HTTP request body (protobuf).
///
/// Supports `application/x-protobuf` (default) and `application/grpc`.
/// OTLP/JSON (`application/json`) support requires serde integration with
/// prost types and can be added when needed.
fn decode_otlp_request<T: prost::Message + Default>(
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> Result<T> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/x-protobuf");

    if content_type.contains("json") {
        // OTLP/JSON not yet supported — requires serde derives on prost types
        Err(Error::Validation(
            "OTLP/JSON content-type not yet supported; use application/x-protobuf".into(),
        ))
    } else {
        // OTLP/protobuf (default)
        <T as prost::Message>::decode(body)
            .map_err(|e| Error::Validation(format!("OTLP protobuf decode failed: {e}")))
    }
}

// ---------------------------------------------------------------------------
// ProtocolHandler implementation
// ---------------------------------------------------------------------------

/// OTLP protocol handler — runs gRPC (4317) and HTTP (4318) servers.
pub struct OtlpHandler {
    config: OtlpConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
}

impl OtlpHandler {
    pub fn new(config: OtlpConfig, pipeline: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        Self {
            config,
            pipeline,
            metrics,
        }
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

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        // Spawn gRPC and HTTP servers concurrently
        let grpc_config = self.config.clone();
        let grpc_pipeline = self.pipeline.clone();
        let grpc_metrics = self.metrics.clone();
        let grpc_shutdown = shutdown.clone();

        let grpc_handle = tokio::spawn(async move {
            run_grpc_server(&grpc_config, grpc_pipeline, grpc_metrics, grpc_shutdown).await
        });

        let http_config = self.config.clone();
        let http_pipeline = self.pipeline.clone();
        let http_metrics = self.metrics.clone();
        let http_shutdown = shutdown.clone();

        let http_handle = tokio::spawn(async move {
            run_http_server(&http_config, http_pipeline, http_metrics, http_shutdown).await
        });

        // Wait for shutdown
        shutdown.cancelled().await;

        let _ = grpc_handle.await;
        let _ = http_handle.await;

        info!("OTLP handler stopped");
        Ok(())
    }
}
