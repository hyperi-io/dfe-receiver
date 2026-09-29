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

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use scalo::transport::grpc::sender_deadline;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::{Connected, TcpConnectInfo, TcpIncoming};
use tonic::{Code, Request, Response, Status};
use tracing::{debug, info, trace};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::metrics::{ConnectionGuard, Metrics};
use crate::pipeline::{Acks, BatchOutcome, PipelineState};
use crate::server::auth::{AuthMode, AuthState, grpc_auth_interceptor};
use crate::server::http::create_auth_state;
use crate::server::traits::{BoundAddr, ProtocolHandler};

/// The transport label the Vector gRPC listener counts under.
const TRANSPORT: &str = "grpc";

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
    acks: Acks,
}

impl VectorService {
    /// Create a new Vector service, answering as `acks` says.
    pub fn new(pipeline: Arc<PipelineState>, metrics: Arc<Metrics>, acks: Acks) -> Self {
        Self {
            pipeline,
            metrics,
            acks,
        }
    }
}

#[tonic::async_trait]
impl Vector for VectorService {
    /// Handle unary push of events from Vector.
    ///
    /// Receives a batch of events, converts each from protobuf to JSON,
    /// and processes through the pipeline. An event the pipeline could not
    /// take answers `UNAVAILABLE`, which the peer retries; one refused for good
    /// answers `INVALID_ARGUMENT`, which it does not.
    ///
    /// With `grpc.acknowledgements` on, OK waits until every destination
    /// confirmed the events, within the peer's own `grpc-timeout`.
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
            return Err(Status::unavailable(OVERLOADED));
        }

        let deadline = sender_deadline(request.metadata());
        let req = request.into_inner();
        let event_count = req.events.len();
        debug!(
            transport = "grpc",
            events = event_count,
            "gRPC push_events received"
        );

        self.metrics.inc_requests_total("grpc");

        let start = std::time::Instant::now();
        // An event that does not convert is refused for good, and the rest go on.
        let mut refused = BatchOutcome::default();
        let mut jsons = Vec::with_capacity(event_count);
        for event in &req.events {
            match convert::event_wrapper_to_json(event) {
                Ok(json_bytes) => {
                    trace!(
                        transport = "grpc",
                        bytes = json_bytes.len(),
                        "Dispatching gRPC event to pipeline"
                    );
                    self.metrics
                        .add_bytes_received("grpc", json_bytes.len() as u64);
                    jsons.push(json_bytes);
                }
                Err(e) => {
                    let _ = refused.record(Err(e));
                }
            }
        }
        let mut outcome = self
            .pipeline
            .process_batch_acked(&jsons, &self.acks, deadline)
            .await;
        outcome.rejected += refused.rejected;
        outcome.first_rejection = outcome.first_rejection.or(refused.first_rejection);
        outcome.unavailable = outcome.unavailable.or(refused.unavailable);

        let elapsed = start.elapsed();
        self.metrics
            .record_request_duration("grpc", elapsed.as_secs_f64());
        debug!(
            transport = "grpc",
            events = event_count,
            accepted = outcome.accepted,
            duration_us = elapsed.as_micros(),
            "gRPC push_events completed"
        );
        push_answer(&self.metrics, outcome)
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

/// The message on a retryable push answer.
const OVERLOADED: &str = "server is overloaded, retry later";

/// The gRPC answer to a push, by the same classification every listener uses.
///
/// A record not taken answers `UNAVAILABLE`, even when others landed: the
/// resend duplicates those, where OK would lose the rest. A record refused for
/// good answers `INVALID_ARGUMENT`, which the peer drops rather than retries --
/// `INTERNAL`, which it retries, would resend a record that can never land.
fn push_answer(
    metrics: &Metrics,
    outcome: BatchOutcome,
) -> std::result::Result<Response<PushEventsResponse>, Status> {
    if let Some(e) = outcome.unavailable {
        debug!(transport = "grpc", error = %e, "gRPC push not fully taken; answering UNAVAILABLE");
        metrics.inc_requests_error("grpc");
        metrics.record_backpressure();
        return Err(Status::unavailable(OVERLOADED));
    }
    if let Some(e) = outcome.first_rejection {
        debug!(transport = "grpc", error = %e, rejected = outcome.rejected, "gRPC push carried records refused for good");
        metrics.inc_requests_error("grpc");
        return Err(Status::invalid_argument(e.public_message()));
    }
    metrics.inc_requests_success("grpc");
    Ok(Response::new(PushEventsResponse {}))
}

/// An accepted gRPC connection, counted open until it drops.
pub(crate) struct CountedConnection {
    stream: TcpStream,
    _open: ConnectionGuard,
}

impl Connected for CountedConnection {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.stream.connect_info()
    }
}

impl AsyncRead for CountedConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for CountedConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

/// `incoming` with each connection counted open on `metrics` under `transport`.
pub(crate) fn count_connections(
    incoming: TcpIncoming,
    metrics: Arc<Metrics>,
    transport: &'static str,
) -> impl Stream<Item = io::Result<CountedConnection>> {
    tokio_stream::StreamExt::map(incoming, move |conn| {
        conn.map(|stream| CountedConnection {
            stream,
            _open: metrics.open_connection(transport),
        })
    })
}

/// A tonic server layer counting requests refused for their size.
///
/// tonic answers an oversized message `OUT_OF_RANGE`, and one that inflates
/// past the limit `RESOURCE_EXHAUSTED`, before any handler runs, so this is
/// the only place either is seen. The handlers answer neither code.
pub(crate) fn count_oversize(
    metrics: Arc<Metrics>,
    transport: &'static str,
) -> tower::util::MapResponseLayer<
    impl Fn(http::Response<tonic::body::Body>) -> http::Response<tonic::body::Body> + Clone,
> {
    tower::util::MapResponseLayer::new(move |response: http::Response<tonic::body::Body>| {
        let code = response
            .headers()
            .get("grpc-status")
            .map(|status| Code::from_bytes(status.as_bytes()));
        if matches!(code, Some(Code::OutOfRange | Code::ResourceExhausted)) {
            metrics.inc_body_size_rejected(transport);
        }
        response
    })
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

    fn listeners(&self) -> Vec<BoundAddr> {
        vec![self.bound.clone()]
    }

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        // Create auth state for gRPC if auth is configured.
        //
        // A failure here is fatal, not a downgrade: `run_server` reads
        // `auth_state: None` as "register the service with no interceptor", so
        // degrading would serve a wide-open gRPC port and report a successful
        // start.
        let auth_state = if AuthMode::parse(&self.config.grpc.auth.mode) != Some(AuthMode::None) {
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

    let acks = pipeline.acks(TRANSPORT, config.grpc.acknowledgements, None);
    let service = VectorService::new(pipeline, metrics.clone(), acks);

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
    let mut builder = builder.layer(count_oversize(metrics.clone(), TRANSPORT));

    // Gzip on both directions: scalo's VectorCompatClient compresses
    // unconditionally, and a server without the encoding enabled rejects the RPC.
    let vector_server = VectorServer::new(service)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .send_compressed(tonic::codec::CompressionEncoding::Gzip)
        .max_decoding_message_size(config.grpc.max_message_size);

    let router = if let Some(auth) = auth_state {
        let interceptor = grpc_auth_interceptor(auth, metrics.clone(), TRANSPORT);
        builder.add_service(InterceptedService::new(vector_server, interceptor))
    } else {
        builder.add_service(vector_server)
    };

    // Bound here rather than inside tonic, which keeps the address it took to itself.
    // serve_with_incoming_shutdown drops the builder's TCP settings, so they go on the TcpIncoming.
    let incoming = TcpIncoming::bind(addr)
        .map_err(|e| Error::Server(format!("gRPC server error: {e}")))?
        .with_nodelay(Some(true));
    let _serving = bound.publish(&incoming.local_addr());

    info!(addr = %addr, "gRPC server listening");

    router
        .serve_with_incoming_shutdown(
            count_connections(incoming, metrics, TRANSPORT),
            shutdown.cancelled_owned(),
        )
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
