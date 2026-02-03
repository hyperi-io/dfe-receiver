// Project:   dfe-receiver
// File:      src/server/mod.rs
// Purpose:   HTTP and gRPC server management
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

//! HTTP and gRPC server management.
//!
//! Provides the main server that handles incoming requests via HTTP (axum)
//! and optionally gRPC (tonic) for Vector sink protocol.

pub mod auth;
pub mod grpc;
pub mod http;
pub mod tls;

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::error::Result;
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;

/// Main server that manages HTTP and gRPC endpoints.
pub struct Server {
    state: Arc<PipelineState>,
    metrics: Arc<Metrics>,
}

impl Server {
    /// Create a new server instance.
    pub fn new(state: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        Self { state, metrics }
    }

    /// Run the server until shutdown is signalled.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<()> {
        let config = self.state.config();

        // Start HTTP server
        let http_addr = config.server.bind_address.clone();
        let http_state = self.state.clone();
        let http_metrics = self.metrics.clone();
        let http_shutdown = shutdown.clone();

        let http_handle = tokio::spawn(async move {
            http::run_server(&http_addr, http_state, http_metrics, http_shutdown).await
        });

        // Start gRPC server if enabled
        let grpc_handle = if config.grpc.enabled {
            let grpc_addr = config.grpc.bind_address.clone();
            let grpc_state = self.state.clone();
            let grpc_metrics = self.metrics.clone();
            let grpc_shutdown = shutdown.clone();

            Some(tokio::spawn(async move {
                grpc::run_server(&grpc_addr, grpc_state, grpc_metrics, grpc_shutdown).await
            }))
        } else {
            None
        };

        // Wait for shutdown
        shutdown.cancelled().await;

        // Wait for servers to finish
        let _ = http_handle.await;
        if let Some(handle) = grpc_handle {
            let _ = handle.await;
        }

        info!("Server shutdown complete");
        Ok(())
    }
}
