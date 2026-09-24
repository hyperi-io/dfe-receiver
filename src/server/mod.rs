// Project:   dfe-receiver
// File:      src/server/mod.rs
// Purpose:   Protocol handler orchestration
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Protocol handler orchestration.
//!
//! Manages pluggable protocol handlers (HTTP, gRPC/Vector, OTLP, etc.).
//! All enabled handlers are spawned in parallel and monitored for health.

pub mod auth;
pub mod flow;
pub mod fluent;
pub mod gelf;
pub mod grpc;
pub mod http;
pub mod ip_filter;
pub mod lumberjack;
pub mod netflow;
#[cfg(feature = "otlp")]
pub mod otlp;
pub mod prometheus_rw;
pub mod raw_capture;
pub mod sflow;
pub mod splunk_hec;
pub mod syslog;
pub mod tls;
pub mod traits;
pub mod webhook;

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::error::Result;
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::flow::FlowHandler;
use crate::server::flow::metrics::FlowMetrics;
use crate::server::fluent::FluentHandler;
use crate::server::gelf::GelfHandler;
use crate::server::grpc::GrpcVectorHandler;
use crate::server::http::HttpHandler;
use crate::server::lumberjack::LumberjackHandler;
#[cfg(feature = "otlp")]
use crate::server::otlp::OtlpHandler;
use crate::server::prometheus_rw::PrometheusRwHandler;
use crate::server::splunk_hec::SplunkHecHandler;
use crate::server::syslog::SyslogHandler;
use crate::server::traits::ProtocolHandler;
use crate::server::webhook::WebhookHandler;

/// Main server that manages protocol handlers.
pub struct Server {
    state: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    /// Pre-registered FlowMetrics handles. `None` skips the flow handler
    /// even if `config.flow.enabled` is true; this is the legacy
    /// `Server::new` path used by tests that don't bring up a global recorder.
    flow_metrics: Option<FlowMetrics>,
}

impl Server {
    /// Create a new server instance (no flow handler).
    pub fn new(state: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        Self {
            state,
            metrics,
            flow_metrics: None,
        }
    }

    /// Create a new server instance with FlowMetrics registered against the
    /// global recorder. Required when `config.flow.enabled` or
    /// `config.flow.split` is set.
    pub fn with_flow_metrics(
        state: Arc<PipelineState>,
        metrics: Arc<Metrics>,
        flow_metrics: FlowMetrics,
    ) -> Self {
        Self {
            state,
            metrics,
            flow_metrics: Some(flow_metrics),
        }
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

        // OTLP handler (if enabled and compiled with otlp feature)
        #[cfg(feature = "otlp")]
        if config.otlp.enabled {
            handlers.push(Box::new(OtlpHandler::new(
                config.otlp.clone(),
                config.raw_capture_for(&config.otlp.raw_capture),
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
                config.raw_capture_for(&config.splunk_hec.raw_capture),
                self.state.clone(),
                self.metrics.clone(),
            )));
        }

        // Prometheus Remote Write handler (if enabled)
        if config.prometheus_rw.enabled {
            handlers.push(Box::new(PrometheusRwHandler::new(
                config.prometheus_rw.clone(),
                config.raw_capture_for(&config.prometheus_rw.raw_capture),
                self.state.clone(),
                self.metrics.clone(),
            )));
        }

        // Syslog handler (if enabled)
        if config.syslog.enabled {
            handlers.push(Box::new(SyslogHandler::new(
                config.syslog.clone(),
                config.raw_capture_for(&config.syslog.raw_capture),
                self.state.clone(),
                self.metrics.clone(),
            )));
        }

        // Fluent Forward handler (if enabled)
        if config.fluent.enabled {
            handlers.push(Box::new(FluentHandler::new(
                config.fluent.clone(),
                config.raw_capture_for(&config.fluent.raw_capture),
                self.state.clone(),
                self.metrics.clone(),
            )));
        }

        // GELF handler (if enabled)
        if config.gelf.enabled {
            handlers.push(Box::new(GelfHandler::new(
                config.gelf.clone(),
                config.raw_capture_for(&config.gelf.raw_capture),
                self.state.clone(),
                self.metrics.clone(),
            )));
        }

        // Webhook intake on its own listener. With no bind_address the routes
        // ride the HTTP handler's listener instead (see http::run_server).
        if config.webhook.enabled && config.webhook.bind_address.is_some() {
            handlers.push(Box::new(WebhookHandler::new(
                config.clone(),
                self.state.clone(),
                self.metrics.clone(),
            )));
        }

        // Flow (NetFlow + sFlow) handler -- enabled in unified or split mode.
        // Requires FlowMetrics to be pre-registered via Server::with_flow_metrics.
        if config.flow.enabled || config.flow.split.is_some() {
            match self.flow_metrics.clone() {
                Some(flow_metrics) => {
                    match FlowHandler::new(
                        config.flow.clone(),
                        config.raw_capture_for(&config.flow.raw_capture),
                        flow_metrics,
                        self.state.clone(),
                    ) {
                        Ok(handler) => handlers.push(Box::new(handler)),
                        Err(e) => {
                            error!(error = %e, "Flow handler config invalid; skipping");
                        }
                    }
                }
                None => {
                    warn!(
                        "Flow handler enabled but no FlowMetrics registered; skipping. \
                         Use Server::with_flow_metrics to enable flow."
                    );
                }
            }
        }

        handlers
    }

    /// Run all enabled protocol handlers until shutdown is signalled.
    ///
    /// A handler that fails is logged and stays down, and readiness stays
    /// false for as long as any of its listeners is not serving.
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

        self.state.watch_listeners(
            handlers
                .iter()
                .flat_map(|handler| handler.listeners())
                .collect(),
        );

        // Spawn all handlers concurrently
        let mut join_handles = Vec::with_capacity(handlers.len());
        for handler in handlers {
            let handler_shutdown = shutdown.clone();
            let name = handler.name();

            let handle = tokio::spawn(async move {
                match handler.start(handler_shutdown.clone()).await {
                    Err(e) => error!(handler = name, error = %e, "Protocol handler failed"),
                    Ok(()) if !handler_shutdown.is_cancelled() => {
                        error!(handler = name, "Protocol handler stopped before shutdown");
                    }
                    Ok(()) => {}
                }
            });

            join_handles.push((name, handle));
        }

        // Wait for shutdown
        shutdown.cancelled().await;

        // Wait for all handlers to finish
        for (name, handle) in join_handles {
            if let Err(e) = handle.await {
                error!(handler = name, error = %e, "Protocol handler panicked");
            }
        }

        info!("Server shutdown complete");
        Ok(())
    }
}
