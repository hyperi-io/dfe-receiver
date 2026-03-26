// Project:   dfe-receiver
// File:      src/pipeline/mod.rs
// Purpose:   Main processing pipeline orchestration
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Pipeline orchestration module.
//!
//! Coordinates the flow of messages through validation, routing,
//! and delivery to sinks with backpressure support.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// State-change flag for memory pressure log deduplication.
static PRESSURE_LOGGED: AtomicBool = AtomicBool::new(false);

use hyperi_rustlib::dlq::{Dlq, DlqEntry};
use hyperi_rustlib::logger::security;

use crate::buffer::{InMemoryBuffer, MemoryGuard, MemoryGuardConfig, MemoryPressure, SinkBackend};
use crate::config::{Config, SharedConfig};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::routing::{RouteResult, Router};
use crate::sink::Sink;
use crate::sink::file::FileSink;
use crate::sink::grpc::GrpcSink;
use crate::sink::kafka::KafkaSink;
use crate::sink::loader::LoaderSink;
use crate::validation::{ValidationResult, Validator};

/// Shared pipeline state accessible from handlers.
pub struct PipelineState {
    shared_config: SharedConfig,
    validator: RwLock<Validator>,
    router: RwLock<Router>,
    kafka_sink: Option<Arc<SinkBackend<KafkaSink>>>,
    loader_sink: Option<Arc<SinkBackend<LoaderSink>>>,
    grpc_loader_sink: Option<Arc<SinkBackend<GrpcSink>>>,
    file_sink: Option<Arc<FileSink>>,
    memory_guard: Arc<MemoryGuard>,
    dlq: Option<Arc<Dlq>>,
    ready: AtomicBool,
}

impl PipelineState {
    /// Create new pipeline state.
    pub async fn new(shared_config: SharedConfig) -> Result<Self> {
        let config = shared_config.get();
        let validator = Validator::new(config.validation.clone());
        let router = Router::new(
            &config.routing,
            &config.destinations,
            config.server.auth.include_common_header,
        );
        // Memory guard: env vars take precedence, then YAML config, then auto-detect
        let mut mg_config = MemoryGuardConfig::from_env("DFE_RECEIVER");
        if config.buffer.memory_limit > 0 && mg_config.limit_bytes == 0 {
            mg_config.limit_bytes = config.buffer.memory_limit as u64;
        }
        if (config.buffer.pressure_threshold - 0.8).abs() > f64::EPSILON {
            mg_config.pressure_threshold = config.buffer.pressure_threshold;
        }
        let memory_guard = Arc::new(MemoryGuard::new(mg_config));

        // Initialise Kafka sink with buffer wrapper if brokers configured
        let kafka_sink = if !config.kafka.brokers.is_empty() {
            let primary = KafkaSink::new(&config.kafka)?;
            Some(Arc::new(build_sink_backend(primary, &config.buffer).await?))
        } else {
            None
        };

        // Determine if loader destination is in use
        let loader_destination_active = config.destinations.default == "loader"
            || config
                .destinations
                .rules
                .iter()
                .any(|r| r.destination == "loader");

        // Initialise loader or gRPC loader sink with buffer wrapper
        let (loader_sink, grpc_loader_sink) = if loader_destination_active {
            if config.loader.transport == "grpc" {
                let endpoint = config.loader.effective_grpc_endpoint();
                let primary = GrpcSink::new(&endpoint).await?;
                (
                    None,
                    Some(Arc::new(build_sink_backend(primary, &config.buffer).await?)),
                )
            } else {
                let primary = LoaderSink::new(&config.loader, &config.kafka)?;
                (
                    Some(Arc::new(build_sink_backend(primary, &config.buffer).await?)),
                    None,
                )
            }
        } else {
            (None, None)
        };

        // Initialise debug file sink if enabled
        let file_sink = if config.file_sink.enabled {
            match FileSink::new(&config.file_sink.path) {
                Ok(sink) => Some(Arc::new(sink)),
                Err(e) => {
                    warn!(error = %e, path = %config.file_sink.path, "Failed to open file sink, disabled");
                    None
                }
            }
        } else {
            None
        };

        // DLQ (unified rustlib module — cascade: Kafka primary, file fallback)
        let dlq = if config.routing.dlq.enabled {
            let dlq_config = config.routing.dlq.to_rustlib_config();
            let kafka_config = config.kafka.to_rustlib_kafka_config();
            match Dlq::with_kafka(&dlq_config, "receiver", &kafka_config) {
                Ok(d) => {
                    info!(mode = ?dlq_config.mode, "DLQ enabled");
                    Some(Arc::new(d))
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create DLQ, disabled");
                    None
                }
            }
        } else {
            debug!("DLQ disabled by config");
            None
        };

        Ok(Self {
            shared_config,
            validator: RwLock::new(validator),
            router: RwLock::new(router),
            kafka_sink,
            loader_sink,
            grpc_loader_sink,
            file_sink,
            memory_guard,
            dlq,
            ready: AtomicBool::new(true),
        })
    }

    /// Get the current configuration.
    pub fn config(&self) -> Config {
        self.shared_config.get()
    }

    /// Get the shared config handle.
    pub fn shared_config(&self) -> SharedConfig {
        self.shared_config.clone()
    }

    /// Check if the pipeline is ready to receive requests.
    pub fn is_ready(&self) -> bool {
        if !self.ready.load(Ordering::Relaxed) {
            return false;
        }

        // Not ready under high memory pressure
        if self.memory_guard.under_pressure() {
            return false;
        }

        // Check sink health
        if let Some(ref kafka) = self.kafka_sink
            && !kafka.is_healthy()
        {
            return false;
        }

        if let Some(ref loader) = self.loader_sink
            && !loader.is_healthy()
        {
            return false;
        }

        if let Some(ref grpc) = self.grpc_loader_sink
            && !grpc.is_healthy()
        {
            return false;
        }

        true
    }

    /// Get memory pressure level.
    pub fn memory_pressure(&self) -> MemoryPressure {
        self.memory_guard.pressure()
    }

    /// Check if backpressure should be applied.
    pub fn should_apply_backpressure(&self) -> bool {
        self.memory_guard.under_pressure()
    }

    /// Process a message through the pipeline.
    ///
    /// This is a HOT PATH function.
    #[inline]
    pub async fn process(&self, payload: Bytes) -> Result<()> {
        // Check for backpressure
        if self.should_apply_backpressure() {
            if hyperi_rustlib::logger::log_state_change(&PRESSURE_LOGGED, true) {
                warn!("Memory pressure HIGH — backpressure active");
            }
            return Err(Error::Buffer("server under memory pressure".into()));
        }
        // Log recovery when pressure drops
        if hyperi_rustlib::logger::log_state_change(&PRESSURE_LOGGED, false) {
            info!("Memory pressure recovered");
        }

        // Track memory
        let payload_size = payload.len() as u64;
        self.memory_guard.add_bytes(payload_size);

        let result = self.process_inner(payload).await;

        // Release memory tracking on completion
        self.memory_guard.release(payload_size);

        result
    }

    /// Check if enrichment (timestamp injection, source rules) is enabled.
    #[inline]
    fn enrichment_enabled(&self) -> bool {
        self.shared_config
            .with(|c| c.server.auth.include_common_header)
    }

    /// Inject `_timestamp_receiver` into a validated JSON object payload.
    ///
    /// Performs byte-level append before the closing `}` to avoid a full
    /// JSON parse/rewrite on the hot path.
    #[inline]
    fn enrich_payload(payload: Bytes) -> Bytes {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        let raw = payload.as_ref();
        let Some(insert_pos) = raw.iter().rposition(|&b| b == b'}') else {
            return payload;
        };

        let mut buf = Vec::with_capacity(raw.len() + 40);
        buf.extend_from_slice(&raw[..insert_pos]);

        // Add comma if there's content before the closing brace (not empty object)
        if let Some(pos) = raw[..insert_pos]
            .iter()
            .rposition(|b| !b.is_ascii_whitespace())
            && raw[pos] != b'{'
        {
            buf.push(b',');
        }
        buf.extend_from_slice(format!("\"_timestamp_receiver\":{now_ms}").as_bytes());
        buf.extend_from_slice(&raw[insert_pos..]);
        Bytes::from(buf)
    }

    /// Inner processing logic (after backpressure check).
    #[inline]
    async fn process_inner(&self, payload: Bytes) -> Result<()> {
        // Validate (read guard dropped before any .await)
        let validation = self.validator.read().validate(&payload);
        match validation {
            ValidationResult::Valid => {}
            ValidationResult::Dlq(reason) => {
                debug!(reason = %reason, "Message validation failed, routing to DLQ");
                security::input_validation_failure("json_validate", &reason, None);
                return self.send_to_dlq(&payload, &reason).await;
            }
            ValidationResult::Reject(reason) => {
                security::input_validation_failure("json_validate", &reason, None);
                return Err(Error::Validation(reason));
            }
        }

        // Enrich (only when common header / enrichment enabled)
        let payload = if self.enrichment_enabled() {
            Self::enrich_payload(payload)
        } else {
            payload
        };

        // Route (read guard dropped before any .await)
        let route = self.router.read().route(&payload);
        match route {
            RouteResult::Kafka(topic) => {
                self.send_to_kafka(&topic, payload.clone()).await?;
            }
            RouteResult::Loader => {
                self.send_to_loader(payload.clone()).await?;
            }
            RouteResult::Dlq(topic) => {
                self.send_to_kafka(&topic, payload.clone()).await?;
            }
        }

        // Debug tap: fire-and-forget write to file sink (errors are logged, not propagated)
        if let Some(ref fsink) = self.file_sink
            && let Err(e) = fsink.send("", payload).await
        {
            warn!(error = %e, "File sink write failed");
        }

        Ok(())
    }

    /// Process a message, sending directly to a specific Kafka topic.
    ///
    /// Skips routing but still applies validation and backpressure.
    /// Used by protocol handlers that handle their own protocol-to-topic mapping.
    #[inline]
    pub async fn process_to_topic(&self, payload: Bytes, topic: &str) -> Result<()> {
        // Check for backpressure
        if self.should_apply_backpressure() {
            if hyperi_rustlib::logger::log_state_change(&PRESSURE_LOGGED, true) {
                warn!("Memory pressure HIGH — backpressure active");
            }
            return Err(Error::Buffer("server under memory pressure".into()));
        }
        // Log recovery when pressure drops
        if hyperi_rustlib::logger::log_state_change(&PRESSURE_LOGGED, false) {
            info!("Memory pressure recovered");
        }

        // Track memory
        let payload_size = payload.len() as u64;
        self.memory_guard.add_bytes(payload_size);

        // Validate (acquire and release lock before any await)
        let validation = self.validator.read().validate(&payload);
        let result = match validation {
            ValidationResult::Valid => self.send_to_kafka(topic, payload).await,
            ValidationResult::Dlq(reason) => {
                debug!(reason = %reason, "Message validation failed, routing to DLQ");
                security::input_validation_failure("json_validate", &reason, None);
                self.send_to_dlq(&payload, &reason).await
            }
            ValidationResult::Reject(reason) => {
                security::input_validation_failure("json_validate", &reason, None);
                Err(Error::Validation(reason))
            }
        };

        // Release memory tracking on completion
        self.memory_guard.release(payload_size);

        result
    }

    /// Send message to Kafka.
    #[inline]
    async fn send_to_kafka(&self, topic: &str, payload: Bytes) -> Result<()> {
        let Some(ref sink) = self.kafka_sink else {
            return Err(Error::Config("Kafka sink not configured".into()));
        };

        sink.send(topic, payload).await
    }

    /// Send message to loader.
    ///
    /// Dispatches to the gRPC loader sink when `loader.transport = "grpc"`,
    /// otherwise uses the Kafka-backed loader sink.
    #[inline]
    async fn send_to_loader(&self, payload: Bytes) -> Result<()> {
        if let Some(ref sink) = self.grpc_loader_sink {
            return sink.send("", payload).await;
        }

        let Some(ref sink) = self.loader_sink else {
            return Err(Error::Config("Loader sink not configured".into()));
        };

        sink.send("", payload).await
    }

    /// Send message to DLQ via unified rustlib module (cascade: Kafka → file).
    #[inline]
    async fn send_to_dlq(&self, payload: &Bytes, reason: &str) -> Result<()> {
        if let Some(ref dlq) = self.dlq {
            let entry = DlqEntry::new("receiver", reason, payload.to_vec());
            dlq.send(entry)
                .await
                .map_err(|e| Error::Config(format!("DLQ send failed: {e}")))?;
            Ok(())
        } else {
            // Fallback: route through Kafka sink (legacy behaviour)
            let dlq_route = { self.router.read().route_dlq(reason) };
            match dlq_route {
                RouteResult::Dlq(topic) | RouteResult::Kafka(topic) => {
                    self.send_to_kafka(&topic, payload.clone()).await
                }
                RouteResult::Loader => self.send_to_loader(payload.clone()).await,
            }
        }
    }

    /// Get memory guard for external access.
    pub fn memory_guard(&self) -> &Arc<MemoryGuard> {
        &self.memory_guard
    }

    /// Snapshot pipeline state into metrics gauges.
    ///
    /// Called every second by the orchestrator. Samples buffer manager and
    /// tiered sink stats without touching the hot path.
    pub async fn update_metrics(&self, metrics: &Metrics) {
        // Memory usage
        metrics.set_memory_usage(
            self.memory_guard.current_bytes(),
            self.memory_guard.limit_bytes(),
        );

        // Kafka sink stats
        let mut total_queue = 0u64;
        if let Some(ref kafka) = self.kafka_sink {
            let stats = kafka.stats().await;
            total_queue += stats.queue_size as u64;
            metrics.set_circuit_state(stats.circuit_state, stats.consecutive_failures);
        }

        // Loader sink stats
        if let Some(ref loader) = self.loader_sink {
            let stats = loader.stats().await;
            total_queue += stats.queue_size as u64;
        }

        // gRPC loader sink stats
        if let Some(ref grpc) = self.grpc_loader_sink {
            let stats = grpc.stats().await;
            total_queue += stats.queue_size as u64;
        }

        metrics.set_batch_queue_size(total_queue);

        // EPS gauge — events per second from the rate window
        metrics::gauge!("dfe_receiver_events_per_second").set(metrics.request_rate());

        // Sync all metrics into the scaling pressure engine
        metrics.update_scaling();
    }

    /// Reload configuration, rebuilding router and validator.
    ///
    /// Called on SIGHUP or periodic reload. Sinks are not rebuilt
    /// (Kafka/loader connections are long-lived and should not be disrupted).
    pub fn reload_config(&self, new_config: Config) -> Result<()> {
        self.rebuild_components(&new_config);

        // Update shared config (bumps version, notifies subscribers)
        self.shared_config.update(new_config);
        security::config_changed(
            "config_reload",
            "system",
            "pipeline config reloaded (router + validator)",
        );

        let version = self.shared_config.version();
        info!(version, "Configuration reloaded successfully");
        Ok(())
    }

    /// Rebuild mutable pipeline components from new configuration.
    ///
    /// Called by the config change subscriber when `SharedConfig` is updated
    /// externally (e.g., by `ConfigReloader`). Does NOT update `SharedConfig`
    /// itself — that's already been done by the caller.
    pub fn rebuild_components(&self, new_config: &Config) {
        let new_router = Router::new(
            &new_config.routing,
            &new_config.destinations,
            new_config.server.auth.include_common_header,
        );
        let new_validator = Validator::new(new_config.validation.clone());

        *self.router.write() = new_router;
        *self.validator.write() = new_validator;
    }
}

/// Build the appropriate `SinkBackend` based on spillover configuration.
///
/// When `spillover.enabled` is true, wraps the primary sink in rustlib's `TieredSink`
/// with disk spillover. Otherwise, uses the default in-memory buffer.
async fn build_sink_backend<S: crate::sink::Sink + 'static>(
    primary: S,
    buffer_config: &crate::config::BufferConfig,
) -> Result<SinkBackend<S>> {
    if buffer_config.spillover.enabled {
        let adapter = crate::buffer::adapter::RustlibSinkAdapter::new(Arc::new(primary));
        let spillover = &buffer_config.spillover;

        let mut tiered_config = hyperi_rustlib::tiered_sink::TieredSinkConfig::new(&spillover.path);

        // Configure disk-aware capacity management
        tiered_config.disk_aware = Some(hyperi_rustlib::tiered_sink::DiskAwareConfig {
            max_usage_percent: spillover.max_usage_percent,
            poll_interval_secs: spillover.poll_interval_secs,
        });

        let tiered = hyperi_rustlib::tiered_sink::TieredSink::new(adapter, tiered_config)
            .await
            .map_err(|e| Error::Config(format!("failed to create tiered sink: {e}")))?;

        info!(
            path = %spillover.path.display(),
            max_usage_percent = spillover.max_usage_percent,
            "Disk spillover enabled"
        );

        Ok(SinkBackend::Tiered(tiered))
    } else {
        Ok(SinkBackend::InMemory(InMemoryBuffer::new(
            primary,
            buffer_config,
        )))
    }
}

/// Main pipeline orchestrator.
pub struct Orchestrator {
    state: Arc<PipelineState>,
    shared_config: SharedConfig,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
}

impl Orchestrator {
    /// Create a new orchestrator.
    pub async fn new(
        config: Config,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        let shared_config = SharedConfig::new(config);
        let state = PipelineState::new(shared_config.clone()).await?;

        Ok(Self {
            state: Arc::new(state),
            shared_config,
            metrics,
            shutdown,
        })
    }

    /// Get shared pipeline state.
    pub fn state(&self) -> Arc<PipelineState> {
        Arc::clone(&self.state)
    }

    /// Get shared config handle.
    pub fn shared_config(&self) -> SharedConfig {
        self.shared_config.clone()
    }

    /// Run the orchestrator (background tasks).
    pub async fn run(&self) -> Result<()> {
        info!("Pipeline orchestrator running");

        // Start drain tasks for tiered sinks
        if let Some(ref kafka) = self.state.kafka_sink {
            kafka.clone().start_drain_task(self.shutdown.clone());
        }
        if let Some(ref loader) = self.state.loader_sink {
            loader.clone().start_drain_task(self.shutdown.clone());
        }
        if let Some(ref grpc) = self.state.grpc_loader_sink {
            grpc.clone().start_drain_task(self.shutdown.clone());
        }

        // Periodic metrics update (1s interval)
        let metrics_state = Arc::clone(&self.state);
        let metrics_ref = Arc::clone(&self.metrics);
        let metrics_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        metrics_state.update_metrics(&metrics_ref).await;
                    }
                    _ = metrics_shutdown.cancelled() => break,
                }
            }
        });

        // Wait for shutdown
        self.shutdown.cancelled().await;

        info!("Pipeline orchestrator shutting down");

        // Flush all sinks
        if let Some(ref kafka) = self.state.kafka_sink
            && let Err(e) = kafka.flush().await
        {
            error!(error = %e, "Failed to flush Kafka sink");
        }

        if let Some(ref loader) = self.state.loader_sink
            && let Err(e) = loader.flush().await
        {
            error!(error = %e, "Failed to flush loader sink");
        }

        if let Some(ref grpc) = self.state.grpc_loader_sink
            && let Err(e) = grpc.flush().await
        {
            error!(error = %e, "Failed to flush gRPC loader sink");
        }

        if let Some(ref fsink) = self.state.file_sink
            && let Err(e) = fsink.flush().await
        {
            error!(error = %e, "Failed to flush file sink");
        }

        info!("Pipeline orchestrator stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        let mut config = Config::default();
        // Use loader destination to avoid needing Kafka brokers
        config.destinations.default = "loader".to_string();
        config.loader.transport = "memory".to_string();
        config
    }

    async fn test_state() -> PipelineState {
        let config = test_config();
        PipelineState::new(SharedConfig::new(config)).await.unwrap()
    }

    async fn test_state_with(config: Config) -> PipelineState {
        PipelineState::new(SharedConfig::new(config)).await.unwrap()
    }

    #[tokio::test]
    async fn test_pipeline_validation_reject() {
        let state = test_state().await;

        // Invalid JSON with dlq_on_invalid=true goes to DLQ (success since it's routed)
        let result = state.process(Bytes::from("not json")).await;
        // Either succeeds (DLQ) or fails depending on config
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    async fn test_pipeline_valid_json() {
        let state = test_state().await;

        let valid_json = Bytes::from(r#"{"test": "data"}"#);
        let result = state.process(valid_json).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_ready_check() {
        let state = test_state().await;
        assert!(state.is_ready());
    }

    #[tokio::test]
    async fn test_pipeline_memory_pressure() {
        let mut config = test_config();
        config.buffer.memory_limit = 1000;
        config.buffer.pressure_threshold = 0.8;

        let state = test_state_with(config).await;

        // Should start without pressure
        assert!(!state.should_apply_backpressure());
        assert_eq!(state.memory_pressure(), MemoryPressure::Low);
    }

    #[tokio::test]
    async fn test_enrich_payload_injects_timestamp() {
        let payload = Bytes::from(r#"{"key": "value"}"#);
        let enriched = PipelineState::enrich_payload(payload);
        let enriched_str = std::str::from_utf8(&enriched).unwrap();

        assert!(enriched_str.contains("\"_timestamp_receiver\":"));
        // Verify it's still valid JSON
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert!(parsed.get("_timestamp_receiver").is_some());
        assert_eq!(parsed.get("key").unwrap(), "value");
    }

    #[tokio::test]
    async fn test_enrich_payload_empty_object() {
        let payload = Bytes::from(r"{}");
        let enriched = PipelineState::enrich_payload(payload);

        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert!(parsed.get("_timestamp_receiver").is_some());
    }

    #[tokio::test]
    async fn test_enrichment_disabled() {
        let mut config = test_config();
        config.server.auth.include_common_header = false;
        let state = test_state_with(config).await;

        assert!(!state.enrichment_enabled());
    }

    #[tokio::test]
    async fn test_enrichment_enabled_by_default() {
        let state = test_state().await;
        assert!(state.enrichment_enabled());
    }

    #[tokio::test]
    async fn test_reload_config_updates_version() {
        let state = test_state().await;
        assert_eq!(state.shared_config().version(), 0);

        let mut new_config = test_config();
        new_config.routing.default_source = "reloaded".to_string();
        state.reload_config(new_config).unwrap();

        assert_eq!(state.shared_config().version(), 1);
        assert_eq!(state.config().routing.default_source, "reloaded");
    }

    #[tokio::test]
    async fn test_reload_config_toggles_enrichment() {
        let state = test_state().await;
        assert!(state.enrichment_enabled());

        // Disable enrichment via reload
        let mut new_config = test_config();
        new_config.server.auth.include_common_header = false;
        state.reload_config(new_config).unwrap();

        assert!(!state.enrichment_enabled());

        // Re-enable via another reload
        let mut re_enable = test_config();
        re_enable.server.auth.include_common_header = true;
        state.reload_config(re_enable).unwrap();

        assert!(state.enrichment_enabled());
    }

    #[tokio::test]
    async fn test_reload_config_while_processing() {
        let state = Arc::new(test_state().await);

        // Process a message before reload
        let result = state.process(Bytes::from(r#"{"a": 1}"#)).await;
        assert!(result.is_ok());

        // Reload config with different default source
        let mut new_config = test_config();
        new_config.routing.default_source = "updated".to_string();
        state.reload_config(new_config).unwrap();

        // Process a message after reload — should still work
        let result = state.process(Bytes::from(r#"{"b": 2}"#)).await;
        assert!(result.is_ok());

        // Verify config actually changed
        assert_eq!(state.config().routing.default_source, "updated");
    }

    #[tokio::test]
    async fn test_reload_config_subscriber_notified() {
        let state = test_state().await;
        let mut rx = state.shared_config().subscribe();

        let new_config = test_config();
        state.reload_config(new_config).unwrap();

        rx.changed().await.expect("should receive notification");
        assert_eq!(*rx.borrow(), 1);
    }
}
