// Project:   dfe-receiver
// File:      src/main.rs
// Purpose:   CLI entry point and runtime initialisation
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-receiver CLI entry point.
//!
//! Aligned with dfe-loader's pattern: `command: Option<StandardCommand>`
//! delegates the standard subcommand surface (run, version, config-check,
//! generate-artefacts, metrics-manifest) to scalo's `run_app()`. The
//! `--emit-helm` / `--emit-dockerfile` flags exist for one-off local
//! diagnostics; the canonical artefact production path is
//! `dfe-receiver generate-artefacts --output-dir ci/`.

#![forbid(unsafe_code)]
#![allow(clippy::large_futures)]

// =============================================================================
// Global Allocator — DFE policy: jemalloc only.
// =============================================================================
// hyperi-ci's release-track build adds `--features jemalloc` automatically on
// every channel. For local builds, opt in with: `cargo build --features jemalloc`.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use scalo::cli::{CliError, CommonArgs, ServiceApp, StandardCommand, VersionInfo, run_app};
use scalo::config::reloader::{ConfigReloader, ReloaderConfig};
use tracing::{debug, error, info};

use dfe_receiver::config::{Config, reload_config};
use dfe_receiver::deployment;
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::Orchestrator;
use dfe_receiver::server::Server;

/// dfe-receiver: high-performance HTTP/gRPC receiver for data ingestion.
#[derive(Parser, Debug)]
#[command(name = "dfe-receiver")]
#[command(about = "High-performance HTTP/gRPC receiver for PB/s scale data ingestion")]
#[command(version)]
struct App {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<StandardCommand>,

    /// Generate Helm chart from deployment contract and exit (default output: ./chart).
    /// Quick local-dev shortcut; the canonical CI path is `generate-artefacts`.
    #[arg(long, value_name = "DIR", default_missing_value = "chart", num_args = 0..=1)]
    emit_helm: Option<PathBuf>,

    /// Generate Dockerfile from deployment contract and exit (default output: ./Dockerfile).
    /// Quick local-dev shortcut; the canonical CI path is `generate-artefacts`.
    #[arg(long, value_name = "FILE", default_missing_value = "Dockerfile", num_args = 0..=1)]
    emit_dockerfile: Option<PathBuf>,
}

impl ServiceApp for App {
    type Config = Config;

    fn name(&self) -> &str {
        "dfe-receiver"
    }

    fn env_prefix(&self) -> &str {
        "DFE_RECEIVER"
    }

    fn version_info(&self) -> VersionInfo {
        VersionInfo::new("dfe-receiver", env!("CARGO_PKG_VERSION"))
    }

    fn common_args(&self) -> &CommonArgs {
        &self.common
    }

    fn command(&self) -> Option<&StandardCommand> {
        self.command.as_ref()
    }

    fn load_config(&self, path: Option<&str>) -> Result<Self::Config, CliError> {
        let config = Config::load(path).map_err(|e| CliError::Config(e.to_string()))?;
        config
            .validate()
            .map_err(|e| CliError::Config(e.to_string()))?;
        Ok(config)
    }

    async fn run_service(
        &self,
        config: Self::Config,
        mut runtime: scalo::cli::ServiceRuntime,
    ) -> Result<(), CliError> {
        info!(version = env!("CARGO_PKG_VERSION"), "Starting dfe-receiver");

        // Log resolved config at debug level (sensitive fields already redacted by SensitiveString)
        debug!(
            bind_address = %config.server.bind_address,
            grpc_enabled = config.grpc.enabled,
            kafka_brokers = ?config.kafka.brokers,
            routing_default_source = %config.routing.default_source,
            routing_default_destination = %config.destinations.default,
            buffer_memory_limit = config.buffer.memory_limit,
            config_reload_secs = config.config_reload_secs,
            "Config resolved"
        );

        // Fire-and-forget startup version check; no-op unless the cascade
        // sets version_check.enabled + api_url.
        {
            use scalo::version_check::{VersionCheck, VersionCheckConfig};
            let checker = VersionCheck::new(VersionCheckConfig::from_cascade(
                "dfe-receiver",
                env!("CARGO_PKG_VERSION"),
            ));
            checker.check_on_startup();
        }

        // Register receiver-specific metric groups on the runtime's existing manager.
        // ServiceRuntime already installed the global recorder and ServiceMetrics.
        //
        // Share the runtime's ScalingPressure engine (registered via
        // `scaling_components` above and served at `/scaling/pressure` to KEDA)
        // so the once-per-second `update_scaling` feed drives the gauge KEDA
        // scales on. If the `scaling` feature/section is off the runtime hands
        // back None; fall back to a standalone engine (byte-identical to the
        // pre-share path).
        let scaling = runtime
            .scaling
            .clone()
            .unwrap_or_else(|| Arc::new(config.scaling.build_pressure()));
        let metrics = Arc::new(Metrics::register_on(scaling, &runtime.metrics));

        // Use runtime's shutdown token (signal handler + K8s pre-stop delay)
        let shutdown_token = runtime.shutdown.clone();

        // Create and run the pipeline orchestrator.
        //
        // Originator self-regulation: thread the runtime's self-regulation
        // governor (built default-ON before transports) into the pipeline so
        // the HTTP/gRPC ingest handlers shed (503 / UNAVAILABLE) under the
        // governor's UnifiedPressure latch -- the inbound brake for a push
        // source. Ingest byte tracking lands on the guard that latch watches.
        // When self_regulation.enabled = false the governor is None and the
        // brake falls back to the bespoke memory-guard threshold check.
        // Horizontal scaling pressure is driven by the once-per-second
        // `update_scaling` feed on the shared engine above (connections,
        // queue_depth, request_rate, memory, spill, circuit) -- KEDA scales on
        // the resulting `/scaling/pressure` gauge -- so the pipeline no longer
        // pushes a separate per-pod signal feed.
        let orchestrator = Orchestrator::with_governor(
            config.clone(),
            metrics.clone(),
            shutdown_token.clone(),
            runtime.governor.as_ref(),
            Some(runtime.memory_guard.clone()),
        )
        .await
        .map_err(|e| CliError::Service(e.to_string()))?;

        // Start config hot-reload (SIGHUP + periodic + file polling via scalo ConfigReloader)
        {
            let config_path_str = config.config_path.clone();
            let shared_config = orchestrator.shared_config();
            let reload_state = orchestrator.state();

            let reloader_config = ReloaderConfig {
                config_path: config.config_path.as_ref().map(PathBuf::from),
                poll_interval: Duration::from_secs(config.config_reload_secs.max(5)),
                periodic_interval: if config.config_reload_secs > 0 {
                    Duration::from_secs(config.config_reload_secs)
                } else {
                    Duration::ZERO
                },
                debounce: Duration::from_millis(500),
                enable_sighup: true,
            };

            let reloader = ConfigReloader::new(
                reloader_config,
                shared_config.clone(),
                move || reload_config_from_path(config_path_str.as_deref()),
                |cfg| {
                    cfg.validate()
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                },
            );

            let _handle = reloader.start();

            // Subscribe to config changes and rebuild pipeline components
            let mut config_rx = shared_config.subscribe();
            tokio::spawn(async move {
                while config_rx.changed().await.is_ok() {
                    let new_config = reload_state.config();
                    reload_state.rebuild_components(&new_config);
                    info!(
                        version = *config_rx.borrow(),
                        "Pipeline components rebuilt after config reload"
                    );
                }
            });

            if config.config_reload_secs > 0 {
                info!(
                    interval_secs = config.config_reload_secs,
                    "Config hot-reload enabled (SIGHUP + periodic + file polling)"
                );
            } else {
                info!("Config hot-reload enabled (SIGHUP + file polling)");
            }
        }

        // Create HTTP/gRPC server. If flow is enabled (unified or split), build
        // FlowMetrics against the global recorder; otherwise skip.
        let server = if config.flow.enabled || config.flow.split.is_some() {
            match dfe_receiver::server::flow::metrics::FlowMetrics::register(&runtime.metrics) {
                Ok(flow_metrics) => {
                    Server::with_flow_metrics(orchestrator.state(), metrics.clone(), flow_metrics)
                }
                Err(e) => {
                    error!(error = %e, "FlowMetrics::register failed; flow handler will be disabled");
                    Server::new(orchestrator.state(), metrics.clone())
                }
            }
        } else {
            Server::new(orchestrator.state(), metrics.clone())
        };

        // Set readiness check on runtime's metrics manager (already serving)
        let pipeline_for_ready = orchestrator.state();
        runtime.set_readiness_check(move || pipeline_for_ready.is_ready());

        // Run main server (blocks until shutdown)
        if let Err(e) = server.run(shutdown_token.clone()).await {
            error!(error = %e, "Server error");
            return Err(CliError::Service(e.to_string()));
        }

        // Run pipeline orchestrator
        if let Err(e) = orchestrator.run().await {
            error!(error = %e, "Pipeline error");
            return Err(CliError::Service(e.to_string()));
        }

        info!("Shutdown complete");
        Ok(())
    }

    fn scaling_components(&self, config: &Self::Config) -> Vec<scalo::scaling::ScalingComponent> {
        // Register the receiver's weighted KEDA components on the runtime's
        // ScalingPressure -- the engine `/scaling/pressure` serves -- so the
        // once-per-second `update_scaling` feed drives the gauge KEDA scales on.
        config.scaling.components()
    }

    fn deployment_contract(&self) -> Option<scalo::deployment::DeploymentContract> {
        Some(crate::deployment::contract())
    }
}

#[tokio::main]
async fn main() {
    let app = App::parse();

    if let Some(output) = &app.emit_helm {
        let contract = deployment::contract();
        // scalo's generate_chart takes a third `Option<&ContractIdentity>`
        // parameter. Passing None preserves the plain chart (no contract
        // identity annotations). The canonical generate-artefacts path stamps
        // identity via the CI-orchestrated invocation in scripts/, not via this
        // one-off --emit-helm flag.
        if let Err(e) = scalo::deployment::generate_chart(&contract, output, None) {
            eprintln!("fatal: {e}");
            std::process::exit(1);
        }
        eprintln!("Helm chart written to {}/", output.display());
        return;
    }

    if let Some(output) = &app.emit_dockerfile {
        // Single source of truth: deployment::emit_dockerfile() is what the
        // checked_in_dockerfile_matches_emit_dockerfile drift guard asserts
        // against, so the CLI emit and the test can never disagree.
        let content = deployment::emit_dockerfile();
        if let Err(e) = std::fs::write(output, &content) {
            eprintln!("fatal: could not write Dockerfile: {e}");
            std::process::exit(1);
        }
        eprintln!("Dockerfile written to {}", output.display());
        return;
    }

    if let Err(e) = run_app(app).await {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

/// Reload configuration from the original config path.
fn reload_config_from_path(
    config_path: Option<&str>,
) -> std::result::Result<Config, Box<dyn std::error::Error + Send + Sync>> {
    let placeholder = Config {
        config_path: config_path.map(String::from),
        ..Config::default()
    };
    reload_config(&placeholder).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
}
