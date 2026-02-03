// Project:   dfe-receiver
// File:      src/server/grpc/mod.rs
// Purpose:   gRPC server using tonic
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

//! gRPC server implementation using tonic.
//!
//! Handles the Vector gRPC sink protocol for receiving events.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures_core::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;

// Include generated proto code
pub mod pb {
    tonic::include_proto!("vector");
}

use pb::vector_server::{Vector, VectorServer};
use pb::{
    EventWrapper, HealthCheckRequest, HealthCheckResponse, PushEventsRequest, PushEventsResponse,
    PushStatus, ServingStatus,
};

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

    /// Process a single event (reserved for future use).
    #[allow(dead_code)]
    async fn process_event(&self, event: &EventWrapper) -> std::result::Result<(), Status> {
        // Prefer log data, fall back to metric/trace
        let data = if !event.log.is_empty() {
            &event.log
        } else if !event.metric.is_empty() {
            &event.metric
        } else if !event.trace.is_empty() {
            &event.trace
        } else {
            return Err(Status::invalid_argument("empty event"));
        };

        let payload = Bytes::copy_from_slice(data);

        self.pipeline
            .process(payload)
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }
}

#[tonic::async_trait]
impl Vector for VectorService {
    type PushEventsStream =
        Pin<Box<dyn Stream<Item = std::result::Result<PushEventsResponse, Status>> + Send>>;

    /// Handle streaming push of events from Vector.
    async fn push_events(
        &self,
        request: Request<Streaming<PushEventsRequest>>,
    ) -> std::result::Result<Response<Self::PushEventsStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(128);

        let pipeline = self.pipeline.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            while let Ok(Some(req)) = stream.message().await {
                let mut events_received = 0u64;
                let mut status = PushStatus::Ok;

                metrics.inc_requests_total();

                for event in &req.events {
                    // Get event data
                    let data = if !event.log.is_empty() {
                        &event.log
                    } else if !event.metric.is_empty() {
                        &event.metric
                    } else if !event.trace.is_empty() {
                        &event.trace
                    } else {
                        continue;
                    };

                    metrics.add_bytes_received(data.len() as u64);

                    let payload = Bytes::copy_from_slice(data);

                    match pipeline.process(payload).await {
                        Ok(()) => {
                            events_received += 1;
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to process gRPC event");
                            status = PushStatus::Rejected;
                            metrics.inc_requests_error();
                        }
                    }
                }

                if events_received > 0 {
                    metrics.inc_requests_success();
                }

                let response = PushEventsResponse {
                    status: status.into(),
                    events_received,
                };

                if tx.send(Ok(response)).await.is_err() {
                    debug!("gRPC client disconnected");
                    break;
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream)))
    }

    /// Health check endpoint.
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> std::result::Result<Response<HealthCheckResponse>, Status> {
        let status = if self.pipeline.is_ready() {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };

        Ok(Response::new(HealthCheckResponse {
            status: status.into(),
        }))
    }
}

/// Run the gRPC server.
pub async fn run_server(
    addr: &str,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<()> {
    let addr: SocketAddr = addr
        .parse()
        .map_err(|e| Error::Config(format!("invalid gRPC bind address: {e}")))?;

    let service = VectorService::new(pipeline, metrics);

    info!(addr = %addr, "gRPC server listening");

    tonic::transport::Server::builder()
        .add_service(VectorServer::new(service))
        .serve_with_shutdown(addr, shutdown.cancelled_owned())
        .await
        .map_err(|e| Error::Server(format!("gRPC server error: {e}")))?;

    info!("gRPC server stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_status_values() {
        // Verify proto enum values
        assert_eq!(PushStatus::Unspecified as i32, 0);
        assert_eq!(PushStatus::Ok as i32, 1);
        assert_eq!(PushStatus::Rejected as i32, 2);
        assert_eq!(PushStatus::Unavailable as i32, 3);
    }

    #[test]
    fn test_serving_status_values() {
        assert_eq!(ServingStatus::Unspecified as i32, 0);
        assert_eq!(ServingStatus::Serving as i32, 1);
        assert_eq!(ServingStatus::NotServing as i32, 2);
    }
}
