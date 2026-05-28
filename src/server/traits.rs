// Project:   dfe-receiver
// File:      src/server/traits.rs
// Purpose:   Protocol handler trait for pluggable ingestion protocols
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Protocol handler trait for pluggable ingestion protocols.
//!
//! All ingestion protocols (HTTP/JSON, gRPC/Vector, OTLP, Prometheus Remote
//! Write, etc.) implement [`ProtocolHandler`]. The server orchestration layer
//! starts all enabled handlers in parallel and monitors their health.

use tokio_util::sync::CancellationToken;

use crate::error::Result;

/// Trait for pluggable protocol handlers.
///
/// Each protocol implements this trait. The server collects all enabled
/// handlers and spawns them concurrently. Handlers run until the
/// cancellation token is triggered, then return.
#[async_trait::async_trait]
pub trait ProtocolHandler: Send + Sync {
    /// Human-readable handler name (e.g. "http", "grpc-vector", "otlp-grpc").
    fn name(&self) -> &'static str;

    /// Address this handler listens on (for logging/health).
    fn bind_address(&self) -> &str;

    /// Start the handler. Blocks until shutdown is signalled.
    async fn start(&self, shutdown: CancellationToken) -> Result<()>;

    /// Check if the handler is healthy and accepting traffic.
    fn is_healthy(&self) -> bool {
        true
    }
}
