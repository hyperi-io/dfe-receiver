// Project:   dfe-receiver
// File:      src/server/grpc/mod.rs
// Purpose:   gRPC server using tonic (Vector-compatible protocol)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
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
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::auth::{validate_bearer_auth, AuthState};

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
        let req = request.into_inner();
        self.metrics.inc_requests_total();

        for event in &req.events {
            let json_bytes = convert::event_wrapper_to_json(event)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            self.metrics.add_bytes_received(json_bytes.len() as u64);

            if let Err(e) = self.pipeline.process(json_bytes).await {
                warn!(error = %e, "Failed to process gRPC event");
                self.metrics.inc_requests_error();
                return Err(Status::internal(e.to_string()));
            }
        }

        self.metrics.inc_requests_success();
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
        if let Some(auth_value) = req.metadata().get("authorization") {
            if let Ok(s) = auth_value.to_str() {
                if let Ok(hv) = axum::http::HeaderValue::from_str(s) {
                    headers.insert("authorization", hv);
                }
            }
        }

        if let Some(err) = validate_bearer_auth(&auth, &headers) {
            return Err(Status::unauthenticated(err.message));
        }

        Ok(req)
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

    // Apply auth interceptor or use plain service
    let router = if let Some(auth) = auth_state {
        let interceptor = make_auth_interceptor(auth);
        builder.add_service(VectorServer::with_interceptor(service, interceptor))
    } else {
        builder.add_service(VectorServer::new(service))
    };

    info!(addr = %addr, "gRPC server listening");

    router
        .serve_with_shutdown(addr, shutdown.cancelled_owned())
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
