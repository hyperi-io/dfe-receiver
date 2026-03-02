// Project:   dfe-receiver
// File:      src/server/plugins/mod.rs
// Purpose:   External plugin loading and lifecycle management
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! External protocol plugin loading and lifecycle management.
//!
//! Loads `.so` plugins at startup and wraps them as [`ProtocolHandler`]
//! implementations so they integrate seamlessly with the built-in handlers.
//!
//! All unsafe FFI code lives in the `dfe-plugin-loader` crate. This module
//! only calls safe APIs.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::{error, info, warn};

use dfe_plugin_loader::{HostCallbacks, PluginCallbackContext, PluginHandle};

use crate::config::PluginsConfig;
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;
use crate::server::traits::ProtocolHandler;

/// Host-side callbacks that plugins use to submit data and log messages.
///
/// Implements `dfe_plugin_loader::HostCallbacks` to bridge plugin calls
/// into the receiver's pipeline and tracing system.
struct ReceiverCallbacks {
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    runtime: tokio::runtime::Handle,
}

impl HostCallbacks for ReceiverCallbacks {
    fn submit(&self, data: &[u8], topic: Option<&str>) -> i32 {
        let bytes = bytes::Bytes::copy_from_slice(data);

        self.metrics.inc_requests_total();
        self.metrics.add_bytes_received(data.len() as u64);

        let result = if let Some(topic) = topic {
            self.runtime
                .block_on(self.pipeline.process_to_topic(bytes, topic))
        } else {
            self.runtime.block_on(self.pipeline.process(bytes))
        };

        match result {
            Ok(()) => {
                self.metrics.inc_requests_success();
                dfe_protocol_sdk::abi::DFE_OK
            }
            Err(_) => {
                self.metrics.inc_requests_error();
                dfe_protocol_sdk::abi::DFE_ERR_SUBMIT_REJECTED
            }
        }
    }

    fn log(&self, level: u32, plugin_name: &str, message: &str) {
        match level {
            dfe_protocol_sdk::abi::DFE_LOG_ERROR => {
                tracing::error!(plugin = %plugin_name, "{message}");
            }
            dfe_protocol_sdk::abi::DFE_LOG_WARN => {
                tracing::warn!(plugin = %plugin_name, "{message}");
            }
            dfe_protocol_sdk::abi::DFE_LOG_INFO => {
                tracing::info!(plugin = %plugin_name, "{message}");
            }
            dfe_protocol_sdk::abi::DFE_LOG_DEBUG => {
                tracing::debug!(plugin = %plugin_name, "{message}");
            }
            _ => {
                tracing::trace!(plugin = %plugin_name, "{message}");
            }
        }
    }
}

/// Load all configured plugins and return them as `ProtocolHandler` trait objects.
pub fn load_plugins(
    config: &PluginsConfig,
    pipeline: &Arc<PipelineState>,
    metrics: &Arc<Metrics>,
) -> Result<Vec<Box<dyn ProtocolHandler>>> {
    let mut handlers: Vec<Box<dyn ProtocolHandler>> = Vec::new();

    // Collect named plugins from config
    let mut named_plugins: Vec<(String, crate::config::PluginEntry)> = config
        .plugins
        .iter()
        .map(|(name, entry)| (name.clone(), entry.clone()))
        .collect();

    // Discover plugins from directory (if configured)
    if let Some(ref dir) = config.directory {
        let dir_path = Path::new(dir);
        if dir_path.is_dir() {
            if let Ok(readdir) = std::fs::read_dir(dir_path) {
                for entry in readdir.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("so") {
                        // Derive name from filename (e.g. libdfe_receiver_plugin_syslog.so → syslog)
                        let stem = path
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .unwrap_or("unknown");
                        let name = stem
                            .strip_prefix("libdfe_receiver_plugin_")
                            .or_else(|| stem.strip_prefix("lib"))
                            .unwrap_or(stem)
                            .to_string();

                        // Skip if already configured by name
                        if config.plugins.contains_key(&name) {
                            continue;
                        }

                        info!(path = %path.display(), name = %name, "discovered plugin in directory");
                        named_plugins.push((
                            name,
                            crate::config::PluginEntry {
                                path: path.to_string_lossy().into_owned(),
                                config: serde_json::Map::new(),
                            },
                        ));
                    }
                }
            } else {
                warn!(directory = %dir, "failed to read plugin directory");
            }
        } else {
            warn!(directory = %dir, "plugin directory does not exist");
        }
    }

    // Load each plugin
    for (logical_name, entry) in &named_plugins {
        let plugin_path = PathBuf::from(&entry.path);

        if !plugin_path.exists() {
            error!(plugin = %logical_name, path = %entry.path, "plugin file not found, skipping");
            continue;
        }

        // Serialise the passthrough config fields as JSON for the plugin
        let config_json = serde_json::to_string(&entry.config).unwrap_or_else(|_| "{}".into());

        // Create callback context via safe API from dfe-plugin-loader.
        // The HostCallbacks trait bridges plugin FFI calls into our pipeline.
        let callbacks = Arc::new(ReceiverCallbacks {
            pipeline: pipeline.clone(),
            metrics: metrics.clone(),
            runtime: tokio::runtime::Handle::current(),
        });

        let cb_ctx = PluginCallbackContext::new(callbacks, logical_name.clone());

        match PluginHandle::load(
            &plugin_path,
            &config_json,
            cb_ctx.submit_fn(),
            cb_ctx.submit_ctx(),
            cb_ctx.log_fn(),
            cb_ctx.log_ctx(),
        ) {
            Ok(handle) => {
                let meta = handle.metadata();

                info!(
                    plugin = %logical_name,
                    sdk_name = %meta.name,
                    version = %meta.version,
                    path = %entry.path,
                    "loaded external plugin"
                );

                let name = logical_name.clone();
                handlers.push(Box::new(PluginHandler {
                    handle: Arc::new(handle),
                    _cb_ctx: cb_ctx,
                    name: Box::leak(name.into_boxed_str()),
                    bind_address: "plugin".to_string(),
                }));
            }
            Err(e) => {
                error!(
                    plugin = %logical_name,
                    path = %entry.path,
                    error = %e,
                    "failed to load plugin, skipping"
                );
                // cb_ctx drops here, cleaning up the callback wrapper
            }
        }
    }

    Ok(handlers)
}

/// Adapter that wraps a loaded plugin as a [`ProtocolHandler`].
struct PluginHandler {
    handle: Arc<PluginHandle>,
    /// Callback context -- must be kept alive while the plugin is alive.
    _cb_ctx: PluginCallbackContext,
    name: &'static str,
    bind_address: String,
}

#[async_trait::async_trait]
impl ProtocolHandler for PluginHandler {
    fn name(&self) -> &'static str {
        self.name
    }

    fn bind_address(&self) -> &str {
        &self.bind_address
    }

    async fn start(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> crate::error::Result<()> {
        let handle = self.handle.clone();
        let plugin_name = self.name.to_string();

        // Spawn the plugin on a dedicated OS thread (not tokio) because
        // the plugin's start() is blocking
        let thread_handle = std::thread::Builder::new()
            .name(format!("plugin-{plugin_name}"))
            .spawn(move || handle.start())
            .map_err(|e| Error::Plugin(format!("failed to spawn plugin thread: {e}")))?;

        // Wait for shutdown signal
        shutdown.cancelled().await;

        // Signal the plugin to stop
        self.handle.stop();

        // Wait for the plugin thread to finish (via spawn_blocking to avoid
        // blocking the tokio runtime)
        let name = self.name.to_string();
        let join_result = tokio::task::spawn_blocking(move || thread_handle.join())
            .await
            .map_err(|e| Error::Plugin(format!("join task error: {e}")))?;

        // Handle thread panic
        let plugin_result =
            join_result.map_err(|_| Error::Plugin(format!("plugin '{name}' thread panicked")))?;

        // Handle plugin start error
        plugin_result.map_err(|e| Error::Plugin(format!("plugin '{name}' failed: {e}")))?;

        Ok(())
    }

    fn is_healthy(&self) -> bool {
        self.handle.is_healthy()
    }
}
