// Project:   dfe-receiver
// File:      src/main.rs
// Purpose:   CLI entry point and runtime initialisation
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-receiver CLI entry point.
//!
//! Uses rustlib's `DfeApp` trait for the standard lifecycle:
//! parse → log → config → dispatch.

#![forbid(unsafe_code)]
#![allow(clippy::large_futures)]

// Jemalloc takes priority when enabled (including when both features are enabled via --all-features)
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Mimalloc only when jemalloc is not enabled
#[cfg(all(feature = "mimalloc", not(feature = "jemalloc")))]
#[global_allocator]
static GLOBAL_MIMALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use hyperi_rustlib::cli::{CliError, CommonArgs, DfeApp, StandardCommand, VersionInfo, run_app};
use hyperi_rustlib::config::reloader::{ConfigReloader, ReloaderConfig};
use hyperi_rustlib::deployment::{generate_chart, generate_compose_fragment, generate_dockerfile};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use dfe_receiver::config::{Config, reload_config};
use dfe_receiver::deployment;
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::Orchestrator;
use dfe_receiver::server::Server;

/// dfe-receiver: High-performance HTTP/gRPC receiver for data ingestion.
#[derive(Parser, Debug)]
#[command(name = "dfe-receiver")]
#[command(about = "High-performance HTTP/gRPC receiver for PB/s scale data ingestion")]
#[command(version)]
struct App {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<AppCommand>,
}

/// CLI subcommands.
#[derive(Subcommand, Clone, Debug)]
enum AppCommand {
    /// Start the service (default if no subcommand given).
    Run,

    /// Print version information and exit.
    Version,

    /// Validate configuration and exit.
    #[command(name = "config-check")]
    ConfigCheck,

    /// Emit generated Dockerfile to stdout.
    #[command(name = "emit-dockerfile")]
    EmitDockerfile,

    /// Generate Helm chart directory.
    #[command(name = "emit-chart")]
    EmitChart {
        /// Output directory for the Helm chart.
        #[arg(default_value = "chart")]
        dir: String,
    },

    /// Emit Docker Compose fragment to stdout.
    #[command(name = "emit-compose")]
    EmitCompose,

    /// Emit deployment contract as JSON to stdout.
    #[command(name = "emit-contract")]
    EmitContract,
}

impl DfeApp for App {
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
        match &self.command {
            Some(AppCommand::Version) => {
                // Use a const to return a stable reference
                const VERSION: StandardCommand = StandardCommand::Version;
                Some(&VERSION)
            }
            Some(AppCommand::ConfigCheck) => {
                const CONFIG_CHECK: StandardCommand = StandardCommand::ConfigCheck;
                Some(&CONFIG_CHECK)
            }
            _ => None,
        }
    }

    fn load_config(&self, path: Option<&str>) -> Result<Self::Config, CliError> {
        let config = Config::load(path).map_err(|e| CliError::Config(e.to_string()))?;
        config
            .validate()
            .map_err(|e| CliError::Config(e.to_string()))?;
        Ok(config)
    }

    async fn run_service(&self, config: Self::Config) -> Result<(), CliError> {
        info!(version = env!("CARGO_PKG_VERSION"), "Starting dfe-receiver");

        // Initialise metrics with scaling pressure engine + standard DFE metrics.
        // Returns the MetricsManager for reuse — creating a second one would panic
        // (global Prometheus recorder can only be installed once).
        let (metrics_instance, metrics_manager) =
            Metrics::with_dfe_metrics(config.scaling.build_pressure());
        let metrics = Arc::new(metrics_instance);

        // Create cancellation token for coordinated shutdown
        let shutdown_token = CancellationToken::new();

        // Spawn signal handler for graceful shutdown
        let signal_token = shutdown_token.clone();
        tokio::spawn(async move {
            if let Err(e) = signal::ctrl_c().await {
                warn!(error = %e, "Failed to listen for SIGINT");
                return;
            }
            info!("Received SIGINT, initiating shutdown");
            signal_token.cancel();
        });

        // Parse metrics server address
        let metrics_addr: std::net::SocketAddr =
            self.common.metrics_addr.parse().unwrap_or_else(|_| {
                warn!(addr = %self.common.metrics_addr, "Invalid metrics address, using default");
                std::net::SocketAddr::from(([0, 0, 0, 0], 9090))
            });

        // Create and run the pipeline orchestrator
        let orchestrator =
            Orchestrator::new(config.clone(), metrics.clone(), shutdown_token.clone())
                .await
                .map_err(|e| CliError::Service(e.to_string()))?;

        // Start config hot-reload (SIGHUP + periodic + file polling via rustlib ConfigReloader)
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

        // Create HTTP/gRPC server
        let server = Server::new(orchestrator.state(), metrics.clone());

        // Start metrics server (reuses the MetricsManager from Metrics::with_dfe_metrics)
        let pipeline_for_ready = orchestrator.state();
        let mut metrics_manager = metrics_manager;
        metrics_manager.set_readiness_check(move || pipeline_for_ready.is_ready());
        if let Err(e) = metrics_manager
            .start_server(&metrics_addr.to_string())
            .await
        {
            error!(error = %e, "Metrics server error");
        }

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
}

#[tokio::main]
async fn main() {
    let app = App::parse();

    // Handle deployment commands before the DfeApp lifecycle
    if let Some(ref cmd) = app.command {
        match cmd {
            AppCommand::EmitDockerfile => {
                println!("{}", generate_dockerfile(&deployment::contract()));
                return;
            }
            AppCommand::EmitChart { dir } => {
                if let Err(e) = generate_chart(&deployment::contract(), dir) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
                eprintln!("Helm chart written to {dir}/");
                return;
            }
            AppCommand::EmitCompose => {
                println!("{}", generate_compose_fragment(&deployment::contract()));
                return;
            }
            AppCommand::EmitContract => {
                println!("{}", deployment::contract().to_json());
                return;
            }
            _ => {}
        }
    }

    // Delegate to standard DfeApp lifecycle
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
