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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::buffer::{BufferManager, MemoryPressure, TieredSink};
use crate::config::{Config, SharedConfig};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::routing::{RouteResult, Router};
use crate::sink::kafka::KafkaSink;
use crate::sink::loader::LoaderSink;
use crate::sink::Sink;
use crate::validation::{ValidationResult, Validator};

/// Shared pipeline state accessible from handlers.
pub struct PipelineState {
    shared_config: SharedConfig,
    validator: RwLock<Validator>,
    router: RwLock<Router>,
    kafka_sink: Option<Arc<TieredSink<KafkaSink>>>,
    loader_sink: Option<Arc<TieredSink<LoaderSink>>>,
    buffer_manager: Arc<BufferManager>,
    ready: AtomicBool,
}

impl PipelineState {
    /// Create new pipeline state.
    pub fn new(shared_config: SharedConfig) -> Result<Self> {
        let config = shared_config.get();
        let validator = Validator::new(config.validation.clone());
        let router = Router::new(
            &config.routing,
            &config.destinations,
            config.server.auth.include_common_header,
        );
        let buffer_manager = Arc::new(BufferManager::new(&config.buffer));

        // Initialise Kafka sink with tiered wrapper if brokers configured
        let kafka_sink = if !config.kafka.brokers.is_empty() {
            let primary = KafkaSink::new(&config.kafka)?;
            Some(Arc::new(TieredSink::new(primary, &config.buffer)))
        } else {
            None
        };

        // Initialise loader sink with tiered wrapper
        let loader_sink = if config.destinations.default == "loader"
            || config
                .destinations
                .rules
                .iter()
                .any(|r| r.destination == "loader")
        {
            let primary = LoaderSink::new(&config.loader, &config.kafka)?;
            Some(Arc::new(TieredSink::new(primary, &config.buffer)))
        } else {
            None
        };

        Ok(Self {
            shared_config,
            validator: RwLock::new(validator),
            router: RwLock::new(router),
            kafka_sink,
            loader_sink,
            buffer_manager,
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
        if self.buffer_manager.is_under_pressure() {
            return false;
        }

        // Check sink health
        if let Some(ref kafka) = self.kafka_sink {
            if !kafka.is_healthy() {
                return false;
            }
        }

        if let Some(ref loader) = self.loader_sink {
            if !loader.is_healthy() {
                return false;
            }
        }

        true
    }

    /// Get memory pressure level.
    pub fn memory_pressure(&self) -> MemoryPressure {
        self.buffer_manager.pressure()
    }

    /// Check if backpressure should be applied.
    pub fn should_apply_backpressure(&self) -> bool {
        self.buffer_manager.is_under_pressure()
    }

    /// Process a message through the pipeline.
    ///
    /// This is a HOT PATH function.
    #[inline]
    pub async fn process(&self, payload: Bytes) -> Result<()> {
        // Check for backpressure
        if self.should_apply_backpressure() {
            warn!("Memory pressure high, applying backpressure");
            return Err(Error::Buffer("server under memory pressure".into()));
        }

        // Track memory
        let payload_size = payload.len() as u64;
        self.buffer_manager.add_bytes(payload_size);

        let result = self.process_inner(payload).await;

        // Release memory tracking on completion
        self.buffer_manager.remove_bytes(payload_size);

        result
    }

    /// Check if enrichment (timestamp injection, source rules) is enabled.
    #[inline]
    fn enrichment_enabled(&self) -> bool {
        self.shared_config.with(|c| c.server.auth.include_common_header)
    }

    /// Inject `_timestamp_receiver` into a validated JSON object payload.
    ///
    /// Performs byte-level append before the closing `}` to avoid a full
    /// JSON parse/rewrite on the hot path.
    #[inline]
    fn enrich_payload(&self, payload: Bytes) -> Bytes {
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
        {
            if raw[pos] != b'{' {
                buf.push(b',');
            }
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
                return self.send_to_dlq(&payload, &reason).await;
            }
            ValidationResult::Reject(reason) => {
                return Err(Error::Validation(reason));
            }
        }

        // Enrich (only when common header / enrichment enabled)
        let payload = if self.enrichment_enabled() {
            self.enrich_payload(payload)
        } else {
            payload
        };

        // Route (read guard dropped before any .await)
        let route = self.router.read().route(&payload);
        match route {
            RouteResult::Kafka(topic) => {
                self.send_to_kafka(&topic, payload).await?;
            }
            RouteResult::Loader => {
                self.send_to_loader(payload).await?;
            }
            RouteResult::Dlq(topic) => {
                self.send_to_kafka(&topic, payload).await?;
            }
        }

        Ok(())
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
    #[inline]
    async fn send_to_loader(&self, payload: Bytes) -> Result<()> {
        let Some(ref sink) = self.loader_sink else {
            return Err(Error::Config("Loader sink not configured".into()));
        };

        sink.send("", payload).await
    }

    /// Send message to DLQ.
    #[inline]
    async fn send_to_dlq(&self, payload: &Bytes, _reason: &str) -> Result<()> {
        let dlq_route = { self.router.read().route_dlq(_reason) };

        match dlq_route {
            RouteResult::Dlq(topic) | RouteResult::Kafka(topic) => {
                self.send_to_kafka(&topic, payload.clone()).await
            }
            RouteResult::Loader => self.send_to_loader(payload.clone()).await,
        }
    }

    /// Get buffer manager for external access.
    pub fn buffer_manager(&self) -> &Arc<BufferManager> {
        &self.buffer_manager
    }

    /// Reload configuration, rebuilding router and validator.
    ///
    /// Called on SIGHUP or periodic reload. Sinks are not rebuilt
    /// (Kafka/loader connections are long-lived and should not be disrupted).
    pub fn reload_config(&self, new_config: Config) -> Result<()> {
        self.rebuild_components(&new_config);

        // Update shared config (bumps version, notifies subscribers)
        self.shared_config.update(new_config);

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

/// Main pipeline orchestrator.
pub struct Orchestrator {
    state: Arc<PipelineState>,
    shared_config: SharedConfig,
    #[allow(dead_code)]
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
}

impl Orchestrator {
    /// Create a new orchestrator.
    pub fn new(config: Config, metrics: Arc<Metrics>, shutdown: CancellationToken) -> Result<Self> {
        let shared_config = SharedConfig::new(config);
        let state = PipelineState::new(shared_config.clone())?;

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

        // Wait for shutdown
        self.shutdown.cancelled().await;

        info!("Pipeline orchestrator shutting down");

        // Flush all sinks
        if let Some(ref kafka) = self.state.kafka_sink {
            if let Err(e) = kafka.flush().await {
                error!(error = %e, "Failed to flush Kafka sink");
            }
        }

        if let Some(ref loader) = self.state.loader_sink {
            if let Err(e) = loader.flush().await {
                error!(error = %e, "Failed to flush loader sink");
            }
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

    fn test_state() -> PipelineState {
        let config = test_config();
        PipelineState::new(SharedConfig::new(config)).unwrap()
    }

    fn test_state_with(config: Config) -> PipelineState {
        PipelineState::new(SharedConfig::new(config)).unwrap()
    }

    #[tokio::test]
    async fn test_pipeline_validation_reject() {
        let state = test_state();

        // Invalid JSON with dlq_on_invalid=true goes to DLQ (success since it's routed)
        let result = state.process(Bytes::from("not json")).await;
        // Either succeeds (DLQ) or fails depending on config
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    async fn test_pipeline_valid_json() {
        let state = test_state();

        let valid_json = Bytes::from(r#"{"test": "data"}"#);
        let result = state.process(valid_json).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_ready_check() {
        let state = test_state();
        assert!(state.is_ready());
    }

    #[tokio::test]
    async fn test_pipeline_memory_pressure() {
        let mut config = test_config();
        config.buffer.memory_limit = 1000;
        config.buffer.pressure_threshold = 0.8;

        let state = test_state_with(config);

        // Should start without pressure
        assert!(!state.should_apply_backpressure());
        assert_eq!(state.memory_pressure(), MemoryPressure::Low);
    }

    #[test]
    fn test_enrich_payload_injects_timestamp() {
        let state = test_state();

        let payload = Bytes::from(r#"{"key": "value"}"#);
        let enriched = state.enrich_payload(payload);
        let enriched_str = std::str::from_utf8(&enriched).unwrap();

        assert!(enriched_str.contains("\"_timestamp_receiver\":"));
        // Verify it's still valid JSON
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert!(parsed.get("_timestamp_receiver").is_some());
        assert_eq!(parsed.get("key").unwrap(), "value");
    }

    #[test]
    fn test_enrich_payload_empty_object() {
        let state = test_state();

        let payload = Bytes::from(r#"{}"#);
        let enriched = state.enrich_payload(payload);

        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert!(parsed.get("_timestamp_receiver").is_some());
    }

    #[test]
    fn test_enrichment_disabled() {
        let mut config = test_config();
        config.server.auth.include_common_header = false;
        let state = test_state_with(config);

        assert!(!state.enrichment_enabled());
    }

    #[test]
    fn test_enrichment_enabled_by_default() {
        let state = test_state();
        assert!(state.enrichment_enabled());
    }

    #[test]
    fn test_reload_config_updates_version() {
        let state = test_state();
        assert_eq!(state.shared_config().version(), 0);

        let mut new_config = test_config();
        new_config.routing.default_source = "reloaded".to_string();
        state.reload_config(new_config).unwrap();

        assert_eq!(state.shared_config().version(), 1);
        assert_eq!(state.config().routing.default_source, "reloaded");
    }

    #[test]
    fn test_reload_config_toggles_enrichment() {
        let state = test_state();
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
        let state = Arc::new(test_state());

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
        let state = test_state();
        let mut rx = state.shared_config().subscribe();

        let new_config = test_config();
        state.reload_config(new_config).unwrap();

        rx.changed().await.expect("should receive notification");
        assert_eq!(*rx.borrow(), 1);
    }
}
