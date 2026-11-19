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

mod budget;
pub mod convert;

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use scalo::tls::ServerTls;
use scalo::transport::grpc::sender_deadline;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;
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
use crate::server::auth::{AuthMode, AuthState, grpc_auth_interceptor, principal_or_listener};
use crate::server::http::{ConnectionLimits, Slot, create_auth_state};
use crate::server::tls::{Handshake, grpc_options, handshake, load_server_tls};
use crate::server::traits::{BoundAddr, ProtocolHandler};
use crate::tenancy::Ingest;

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
        // Shed load under memory pressure.
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
        let ingest = Ingest::sent(principal_or_listener(request.extensions().get(), TRANSPORT));
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
            .process_batch_acked(&jsons, &self.acks, deadline, &ingest)
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
///
/// `S` is the TCP stream, or the TLS stream once its handshake is done.
pub(crate) struct CountedConnection<S> {
    stream: S,
    info: TcpConnectInfo,
    _open: ConnectionGuard,
    _slot: Slot,
}

impl<S> Connected for CountedConnection<S> {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for CountedConnection<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CountedConnection<S> {
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

/// `incoming` with each connection counted open on `metrics` under
/// `transport`, and one past the listener's cap closed as it is accepted.
pub(crate) fn count_connections(
    incoming: TcpIncoming,
    metrics: Arc<Metrics>,
    transport: &'static str,
    limits: ConnectionLimits,
) -> impl Stream<Item = io::Result<CountedConnection<TcpStream>>> {
    tokio_stream::StreamExt::filter_map(incoming, move |conn| match conn {
        Ok(stream) => {
            let Some(slot) = limits.slot() else {
                metrics.inc_connection_closed(transport, "over_limit");
                return None;
            };
            Some(Ok(CountedConnection {
                info: stream.connect_info(),
                stream,
                _open: metrics.open_connection(transport),
                _slot: slot,
            }))
        }
        Err(e) => Some(Err(e)),
    })
}

/// How many handshaken connections wait for tonic to take them.
const HANDSHAKED_BACKLOG: usize = 1024;

/// `connections` after their TLS handshake on `tls`.
///
/// Each handshake runs in its own task, bounded by the handshake timeout, so a
/// slow client holds up nobody else. A connection is counted open and holds its
/// place under the listener's cap from before its handshake, and one whose
/// handshake fails is dropped and counted. Accepting stops once tonic drops
/// the stream.
pub(crate) fn handshake_connections(
    connections: impl Stream<Item = io::Result<CountedConnection<TcpStream>>> + Send + 'static,
    tls: ServerTls,
    metrics: Arc<Metrics>,
) -> impl Stream<Item = io::Result<CountedConnection<TlsStream<TcpStream>>>> {
    let (sender, receiver) = mpsc::channel(HANDSHAKED_BACKLOG);
    tokio::spawn(async move {
        let mut connections = std::pin::pin!(connections);
        loop {
            let next = tokio::select! {
                () = sender.closed() => break,
                next = tokio_stream::StreamExt::next(&mut connections) => next,
            };
            match next {
                None => break,
                Some(Err(e)) => {
                    if sender.send(Err(e)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(connection)) => {
                    let (tls, metrics, sender) = (tls.clone(), metrics.clone(), sender.clone());
                    tokio::spawn(async move {
                        let secured = connection.handshake(&tls, &metrics).await;
                        if let Some(secured) = secured {
                            let _ = sender.send(Ok(secured)).await;
                        }
                    });
                }
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(receiver)
}

impl CountedConnection<TcpStream> {
    /// This connection once its TLS handshake on `tls` is done, `None` when
    /// the handshake failed.
    async fn handshake(
        self,
        tls: &ServerTls,
        metrics: &Metrics,
    ) -> Option<CountedConnection<TlsStream<TcpStream>>> {
        let Self {
            stream,
            info,
            _open: open,
            _slot: slot,
        } = self;
        let peer = info.remote_addr()?;
        let stream = handshake(tls, stream, peer, metrics).await?;
        Some(CountedConnection {
            stream,
            info,
            _open: open,
            _slot: slot,
        })
    }
}

/// The HTTP/2 keep-alive a tonic listener pings with, from `server.connections`.
pub(crate) fn keep_alive(
    builder: tonic::transport::Server,
    limits: &ConnectionLimits,
) -> tonic::transport::Server {
    builder
        .http2_keepalive_interval(Some(limits.http2_keep_alive))
        .http2_keepalive_timeout(Some(crate::server::http::KEEP_ALIVE_PING_TIMEOUT))
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

/// A tonic server layer counting requests that name a `grpc-encoding` the
/// server does not decode.
///
/// tonic answers those `UNIMPLEMENTED` with `grpc-accept-encoding` listing what
/// it does read (gzip), before any handler runs, so a request-side check is the
/// only place the refusal is seen.
#[cfg(feature = "otlp")]
pub(crate) fn count_unsupported_encoding<B>(
    metrics: Arc<Metrics>,
    transport: &'static str,
) -> tower::util::MapRequestLayer<impl Fn(http::Request<B>) -> http::Request<B> + Clone> {
    tower::util::MapRequestLayer::new(move |request: http::Request<B>| {
        let encoding = request
            .headers()
            .get("grpc-encoding")
            .and_then(|v| v.to_str().ok());
        if encoding
            .is_some_and(|e| !e.eq_ignore_ascii_case("gzip") && !e.eq_ignore_ascii_case("identity"))
        {
            metrics.inc_unsupported_encoding(transport);
        }
        request
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
        let handshake = Handshake::of(&self.config.grpc.tls);
        let auth_state = if AuthMode::parse(&self.config.grpc.auth.mode) != Some(AuthMode::None) {
            Some(create_auth_state(&self.config.grpc.auth, handshake).await?)
        } else {
            None
        };
        // A service registered with no interceptor takes no reload: turning
        // auth on or off on this port is a restart.
        if let Some(auth_state) = &auth_state {
            crate::server::http::spawn_auth_reload_watcher(
                self.pipeline.shared_config(),
                auth_state.clone(),
                |config| &config.grpc.auth,
                handshake,
                TRANSPORT,
                shutdown.clone(),
            );
        }

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

    let tls = load_server_tls(&config.grpc.tls, &config.server.connections, grpc_options()).await?;

    let limits = ConnectionLimits::new(&config.server.connections);
    let mut builder = keep_alive(tonic::transport::Server::builder(), &limits)
        .layer(count_oversize(metrics.clone(), TRANSPORT));

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

    info!(addr = %addr, tls = tls.is_some(), "gRPC server listening");

    let connections = count_connections(incoming, metrics.clone(), TRANSPORT, limits);
    let served = match tls {
        Some(tls) => {
            router
                .serve_with_incoming_shutdown(
                    handshake_connections(connections, tls, metrics),
                    shutdown.cancelled_owned(),
                )
                .await
        }
        None => {
            router
                .serve_with_incoming_shutdown(connections, shutdown.cancelled_owned())
                .await
        }
    };
    served.map_err(|e| Error::Server(format!("gRPC server error: {e}")))?;

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
