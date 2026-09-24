// Project:   dfe-receiver
// File:      src/server/grpc/mod.rs
// Purpose:   gRPC server using tonic (Vector-compatible protocol)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! gRPC server implementation using tonic.
//!
//! Implements Vector's gRPC sink protocol for receiving events.
//! The protocol uses unary RPCs (not streaming) matching Vector's upstream
//! definition at `proto/vector/vector.proto`.
//!
//! Events arrive as protobuf `EventWrapper` messages and are converted
//! to JSON bytes for the processing pipeline.

pub mod convert;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};
use tracing::{debug, info, trace, warn};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::auth::{AuthMode, AuthState, validate_bearer_auth};
use crate::server::http::create_auth_state;
use crate::server::traits::{BoundAddr, ProtocolHandler};

// Include generated proto code.
// The `event` package types and `vector` package service.
pub mod pb {
    pub mod event {
        tonic::include_proto!("event");
    }
    pub mod vector {
        tonic::include_proto!("vector");
    }
}

use pb::vector::vector_server::{Vector, VectorServer};
use pb::vector::{HealthCheckRequest, HealthCheckResponse, PushEventsRequest, PushEventsResponse};

/// gRPC service implementation.
pub struct VectorService {
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
}

impl VectorService {
    /// Create a new Vector service.
    pub fn new(pipeline: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        Self { pipeline, metrics }
    }
}

#[tonic::async_trait]
impl Vector for VectorService {
    /// Handle unary push of events from Vector.
    ///
    /// Receives a batch of events, converts each from protobuf to JSON,
    /// and processes through the pipeline.
    async fn push_events(
        &self,
        request: Request<PushEventsRequest>,
    ) -> std::result::Result<Response<PushEventsResponse>, Status> {
        // Shed load when pipeline is not ready (memory pressure, sink down, draining)
        if !self.pipeline.is_ready() {
            debug!(
                transport = "grpc",
                "gRPC push_events rejected — pipeline not ready (backpressure)"
            );
            self.metrics.inc_requests_total("grpc");
            self.metrics.inc_requests_error("grpc");
            self.metrics.record_backpressure();
            return Err(Status::unavailable("server is overloaded"));
        }

        let req = request.into_inner();
        let event_count = req.events.len();
        debug!(
            transport = "grpc",
            events = event_count,
            "gRPC push_events received"
        );

        self.metrics.inc_requests_total("grpc");

        let start = std::time::Instant::now();
        for event in &req.events {
            let json_bytes = convert::event_wrapper_to_json(event)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            trace!(
                transport = "grpc",
                bytes = json_bytes.len(),
                "Dispatching gRPC event to pipeline"
            );

            self.metrics
                .add_bytes_received("grpc", json_bytes.len() as u64);

            if let Err(e) = self.pipeline.process(json_bytes).await {
                warn!(error = %e, transport = "grpc", "Failed to process gRPC event");
                self.metrics.inc_requests_error("grpc");
                self.metrics
                    .record_request_duration("grpc", start.elapsed().as_secs_f64());
                return Err(Status::internal(e.to_string()));
            }
        }

        let elapsed = start.elapsed();
        debug!(
            transport = "grpc",
            events = event_count,
            duration_us = elapsed.as_micros(),
            "gRPC push_events completed"
        );
        self.metrics
            .record_request_duration("grpc", elapsed.as_secs_f64());
        self.metrics.inc_requests_success("grpc");
        Ok(Response::new(PushEventsResponse {}))
    }

    /// Health check endpoint.
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> std::result::Result<Response<HealthCheckResponse>, Status> {
        let status = if self.pipeline.is_ready() {
            pb::vector::ServingStatus::Serving
        } else {
            pb::vector::ServingStatus::NotServing
        };

        Ok(Response::new(HealthCheckResponse {
            status: status.into(),
        }))
    }
}

/// Create a tonic auth interceptor from the shared `AuthState`.
///
/// Extracts the `authorization` metadata key from gRPC requests and
/// validates against the bearer token provider.
fn make_auth_interceptor(
    auth: AuthState,
) -> impl Fn(Request<()>) -> std::result::Result<Request<()>, Status> + Clone {
    move |req: Request<()>| {
        // Build an HTTP header map from gRPC metadata for reuse of validate_bearer_auth
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

/// gRPC/Vector protocol handler wrapping the existing tonic server.
pub struct GrpcVectorHandler {
    config: Config,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    bound: BoundAddr,
}

impl GrpcVectorHandler {
    /// Create a new gRPC/Vector handler.
    pub fn new(config: Config, pipeline: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        Self {
            config,
            pipeline,
            metrics,
            bound: BoundAddr::default(),
        }
    }

    /// The address the listener bound, once [`ProtocolHandler::start`] binds it.
    #[must_use]
    pub fn bound_addr(&self) -> BoundAddr {
        self.bound.clone()
    }
}

#[async_trait::async_trait]
impl ProtocolHandler for GrpcVectorHandler {
    fn name(&self) -> &'static str {
        "grpc-vector"
    }

    fn bind_address(&self) -> &str {
        &self.config.grpc.bind_address
    }

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        // Create auth state for gRPC if auth is configured.
        //
        // A failure here is fatal, not a downgrade: `run_server` reads
        // `auth_state: None` as "register the service with no interceptor", so
        // degrading would serve a wide-open gRPC port and report a successful
        // start.
        let auth_state = if AuthMode::from_str(&self.config.grpc.auth.mode) != AuthMode::None {
            Some(create_auth_state(&self.config.grpc.auth).await?)
        } else {
            None
        };

        serve(
            &self.config,
            self.pipeline.clone(),
            self.metrics.clone(),
            auth_state,
            shutdown,
            &self.bound,
        )
        .await
    }
}

/// Run the gRPC server with optional TLS and auth.
pub async fn run_server(
    config: &Config,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    auth_state: Option<AuthState>,
    shutdown: CancellationToken,
) -> Result<()> {
    serve(
        config,
        pipeline,
        metrics,
        auth_state,
        shutdown,
        &BoundAddr::default(),
    )
    .await
}

/// Run the gRPC server, publishing the address it binds to `bound`.
async fn serve(
    config: &Config,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    auth_state: Option<AuthState>,
    shutdown: CancellationToken,
    bound: &BoundAddr,
) -> Result<()> {
    let addr: SocketAddr = config
        .grpc
        .bind_address
        .parse()
        .map_err(|e| Error::Config(format!("invalid gRPC bind address: {e}")))?;

    let service = VectorService::new(pipeline, metrics);

    // Build TLS config if enabled
    let tls_config = if config.grpc.tls.enabled {
        let identity = super::tls::build_grpc_tls_config(&config.grpc.tls).await?;
        Some(identity)
    } else {
        None
    };

    let mut builder = tonic::transport::Server::builder();

    // Apply TLS
    if let Some(tls) = tls_config {
        builder = builder
            .tls_config(tls)
            .map_err(|e| Error::Tls(format!("gRPC TLS config error: {e}")))?;
    }

    // Gzip on both directions: scalo's VectorCompatClient compresses
    // unconditionally, and a server without the encoding enabled rejects the RPC.
    let vector_server = VectorServer::new(service)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .send_compressed(tonic::codec::CompressionEncoding::Gzip);

    let router = if let Some(auth) = auth_state {
        let interceptor = make_auth_interceptor(auth);
        builder.add_service(InterceptedService::new(vector_server, interceptor))
    } else {
        builder.add_service(vector_server)
    };

    // Bound here rather than inside tonic, which keeps the address it took to itself.
    // tonic's own bind also sets TCP_NODELAY, which a hand-bound stream must be given.
    let incoming = TcpIncoming::bind(addr)
        .map_err(|e| Error::Server(format!("gRPC server error: {e}")))?
        .with_nodelay(Some(true));
    bound.publish(&incoming.local_addr());

    info!(addr = %addr, "gRPC server listening");

    router
        .serve_with_incoming_shutdown(incoming, shutdown.cancelled_owned())
        .await
        .map_err(|e| Error::Server(format!("gRPC server error: {e}")))?;

    info!("gRPC server stopped");
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_serving_status_values() {
        assert_eq!(pb::vector::ServingStatus::Serving as i32, 0);
        assert_eq!(pb::vector::ServingStatus::NotServing as i32, 1);
    }
}
