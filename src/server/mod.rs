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
//! All enabled handlers are spawned in parallel, and readiness waits on every
//! listener they bind.

pub mod auth;
pub mod flow;
pub mod fluent;
pub mod gelf;
pub mod grpc;
mod hold;
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
use tracing::{error, info};

use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::flow::FlowHandler;
use crate::server::flow::handler::render_bind_summary;
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
use crate::server::traits::{BoundAddr, ProtocolHandler};
use crate::server::webhook::WebhookHandler;

/// Main server that manages protocol handlers.
pub struct Server {
    state: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    /// Pre-registered FlowMetrics handles. With `None` and `config.flow`
    /// enabled, the flow handler fails to start and holds readiness down.
    flow_metrics: Option<FlowMetrics>,
}

/// An enabled handler that could not be built.
///
/// It stands in for the missing handler, so the orchestrator reports the
/// failure as it reports a bind failure, and its listener never binds, so
/// readiness stays down.
struct UnbuiltHandler {
    name: &'static str,
    bind_address: String,
    reason: String,
    listener: BoundAddr,
}

#[async_trait::async_trait]
impl ProtocolHandler for UnbuiltHandler {
    fn name(&self) -> &'static str {
        self.name
    }

    fn bind_address(&self) -> &str {
        &self.bind_address
    }

    fn listeners(&self) -> Vec<BoundAddr> {
        vec![self.listener.clone()]
    }

    async fn start(&self, _shutdown: CancellationToken) -> Result<()> {
        Err(Error::Server(format!("not built: {}", self.reason)))
    }
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
            let built = match self.flow_metrics.clone() {
                Some(flow_metrics) => FlowHandler::new(
                    config.flow.clone(),
                    config.raw_capture_for(&config.flow.raw_capture),
                    flow_metrics,
                    self.state.clone(),
                ),
                None => Err(Error::Config(
                    "flow is enabled but the server has no FlowMetrics \
                     (build it with Server::with_flow_metrics)"
                        .into(),
                )),
            };
            match built {
                Ok(handler) => handlers.push(Box::new(handler)),
                Err(e) => handlers.push(Box::new(UnbuiltHandler {
                    name: "flow",
                    bind_address: render_bind_summary(&config.flow),
                    reason: e.to_string(),
                    listener: BoundAddr::default(),
                })),
            }
        }

        for (listener, enabled) in [
            ("syslog", config.syslog.enabled),
            ("gelf", config.gelf.enabled),
            ("flow", config.flow.enabled || config.flow.split.is_some()),
        ] {
            if enabled {
                crate::pipeline::acks::publish_unacknowledged_listener(listener);
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
            // Dev and test builds only: release sets panic = "abort", so a handler panic ends the process.
            if let Err(e) = handle.await {
                error!(handler = name, error = %e, "Protocol handler panicked");
            }
        }

        info!("Server shutdown complete");
        Ok(())
    }
}
