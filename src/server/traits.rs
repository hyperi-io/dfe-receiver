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

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::SetOnce;
use tokio_util::sync::CancellationToken;

use crate::error::Result;

/// The address one listener bound, set the moment its bind succeeds.
///
/// A handler binds inside [`ProtocolHandler::start`], which runs until
/// shutdown, so a caller that configured port 0 learns the port the OS
/// assigned by waiting on this. Clones share one cell.
#[derive(Clone, Debug, Default)]
pub struct BoundAddr(Arc<SetOnce<SocketAddr>>);

impl BoundAddr {
    /// Record the bound socket's `local_addr()`. A handler instance binds
    /// each listener once, so a second call is ignored.
    pub(crate) fn publish(&self, local: &std::io::Result<SocketAddr>) {
        // A socket that cannot name its address leaves the cell empty, and the listener still serves.
        if let Ok(addr) = local {
            let _ = self.0.set(*addr);
        }
    }

    /// Wait for the bind and return the address it took.
    ///
    /// Never resolves when the bind fails, so bound the wait with a timeout.
    pub async fn wait(&self) -> SocketAddr {
        *self.0.wait().await
    }
}

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
