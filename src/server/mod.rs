// Project:   dfe-receiver
// File:      src/server/mod.rs
// Purpose:   Protocol handler orchestration
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Protocol handler orchestration.
//!
//! Manages pluggable protocol handlers (HTTP, gRPC/Vector, OTLP, etc.).
//! All enabled handlers are spawned in parallel and monitored for health.

pub mod auth;
pub mod grpc;
pub mod http;
pub mod lumberjack;
pub mod otlp;
#[cfg(feature = "plugins")]
pub mod plugins;
pub mod splunk_hec;
pub mod tls;
pub mod traits;

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::error::Result;
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::grpc::GrpcVectorHandler;
use crate::server::http::HttpHandler;
use crate::server::lumberjack::LumberjackHandler;
use crate::server::otlp::OtlpHandler;
use crate::server::splunk_hec::SplunkHecHandler;
use crate::server::traits::ProtocolHandler;

/// Main server that manages protocol handlers.
pub struct Server {
    state: Arc<PipelineState>,
    metrics: Arc<Metrics>,
}

impl Server {
    /// Create a new server instance.
    pub fn new(state: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        Self { state, metrics }
    }

    /// Collect all enabled protocol handlers based on configuration.
    fn build_handlers(&self) -> Vec<Box<dyn ProtocolHandler>> {
        let config = self.state.config();
        let mut handlers: Vec<Box<dyn ProtocolHandler>> = Vec::new();

        // HTTP handler (always enabled)
        handlers.push(Box::new(HttpHandler::new(
            config.server.bind_address.clone(),
            self.state.clone(),
            self.metrics.clone(),
        )));

        // gRPC/Vector handler (if enabled)
        if config.grpc.enabled {
            handlers.push(Box::new(GrpcVectorHandler::new(
                config.clone(),
                self.state.clone(),
                self.metrics.clone(),
            )));
        }

        // OTLP handler (if enabled)
        if config.otlp.enabled {
            handlers.push(Box::new(OtlpHandler::new(
                config.otlp.clone(),
                self.state.clone(),
                self.metrics.clone(),
            )));
        }

        // Lumberjack/Beats handler (if enabled)
        if config.lumberjack.enabled {
            handlers.push(Box::new(LumberjackHandler::new(
                config.lumberjack.clone(),
                self.state.clone(),
                self.metrics.clone(),
            )));
        }

        // Splunk HEC handler (if enabled)
        if config.splunk_hec.enabled {
            handlers.push(Box::new(SplunkHecHandler::new(
                config.splunk_hec.clone(),
                self.state.clone(),
                self.metrics.clone(),
            )));
        }

        // External plugins (if plugins feature enabled)
        #[cfg(feature = "plugins")]
        {
            if !config.plugins.plugins.is_empty() || config.plugins.directory.is_some() {
                match plugins::load_plugins(&config.plugins, &self.state, &self.metrics) {
                    Ok(plugin_handlers) => {
                        for handler in plugin_handlers {
                            handlers.push(handler);
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to load plugins");
                        // Don't fail startup -- core protocols still work
                    }
                }
            }
        }

        handlers
    }

    /// Run all enabled protocol handlers until shutdown is signalled.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<()> {
        let handlers = self.build_handlers();

        // Log enabled handlers
        for handler in &handlers {
            info!(
                handler = handler.name(),
                bind = handler.bind_address(),
                "Protocol handler enabled"
            );
        }

        // Spawn all handlers concurrently
        let mut handles = Vec::with_capacity(handlers.len());
        for handler in handlers {
            let handler_shutdown = shutdown.clone();
            let name = handler.name();

            let handle = tokio::spawn(async move {
                if let Err(e) = handler.start(handler_shutdown).await {
                    error!(handler = name, error = %e, "Protocol handler failed");
                }
            });

            handles.push(handle);
        }

        // Wait for shutdown
        shutdown.cancelled().await;

        // Wait for all handlers to finish
        for handle in handles {
            let _ = handle.await;
        }

        info!("Server shutdown complete");
        Ok(())
    }
}
