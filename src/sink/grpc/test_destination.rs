// Project:   dfe-receiver
// File:      src/sink/grpc/test_destination.rs
// Purpose:   A gRPC destination for tests that counts the batches it is sent
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A scalo Push listener on loopback that counts each `RouteBatch` call and
//! can refuse every send with a status the sender reads as fatal.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use scalo::transport::grpc::proto::transport_server::{Transport, TransportServer};
use scalo::transport::grpc::proto::{
    Batch, BatchAck, HealthCheckRequest, HealthCheckResponse, PushRequest, PushResponse,
};
use tokio::task::JoinHandle;
use tonic::codec::CompressionEncoding;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
struct Counts {
    batches: AtomicUsize,
    records: AtomicUsize,
    refusing: AtomicBool,
}

struct Service(Arc<Counts>);

#[tonic::async_trait]
impl Transport for Service {
    async fn push(&self, _: Request<PushRequest>) -> Result<Response<PushResponse>, Status> {
        Err(Status::unimplemented(
            "this destination takes RouteBatch only",
        ))
    }

    async fn route_batch(&self, request: Request<Batch>) -> Result<Response<BatchAck>, Status> {
        self.0.batches.fetch_add(1, Ordering::Relaxed);
        if self.0.refusing.load(Ordering::Relaxed) {
            return Err(Status::internal("refused"));
        }
        let records = request.into_inner().records.len();
        self.0.records.fetch_add(records, Ordering::Relaxed);
        Ok(Response::new(BatchAck {
            accepted: records as u64,
        }))
    }

    async fn health_check(
        &self,
        _: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse::default()))
    }
}

/// A destination serving until dropped.
pub(crate) struct TestDestination {
    endpoint: String,
    counts: Arc<Counts>,
    server: JoinHandle<()>,
}

impl TestDestination {
    /// Serve on a free loopback port.
    ///
    /// # Panics
    ///
    /// When no loopback port can be bound.
    #[allow(clippy::expect_used)]
    pub(crate) fn start() -> Self {
        let incoming = TcpIncoming::bind(([127, 0, 0, 1], 0).into())
            .expect("invariant: a loopback port is free");
        let addr = incoming
            .local_addr()
            .expect("invariant: a bound listener has an address");
        let counts = Arc::new(Counts::default());
        let service = TransportServer::new(Service(Arc::clone(&counts)))
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip);
        let server = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await;
        });
        Self {
            endpoint: format!("http://{addr}"),
            counts,
            server,
        }
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// `RouteBatch` calls received, refused ones included.
    pub(crate) fn batches(&self) -> usize {
        self.counts.batches.load(Ordering::Relaxed)
    }

    /// Records taken across every batch not refused.
    pub(crate) fn records(&self) -> usize {
        self.counts.records.load(Ordering::Relaxed)
    }

    /// Answer every later send `INTERNAL`.
    pub(crate) fn refuse_every_send(&self) {
        self.counts.refusing.store(true, Ordering::Relaxed);
    }
}

impl Drop for TestDestination {
    fn drop(&mut self) {
        self.server.abort();
    }
}
