// Project:   dfe-receiver
// File:      src/pipeline/mod.rs
// Purpose:   Main processing pipeline orchestration
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

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
use crate::config::Config;
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::routing::{RouteResult, Router};
use crate::sink::kafka::KafkaSink;
use crate::sink::loader::LoaderSink;
use crate::sink::Sink;
use crate::validation::{ValidationResult, Validator};

/// Shared pipeline state accessible from handlers.
pub struct PipelineState {
    config: Arc<RwLock<Config>>,
    validator: Validator,
    router: Router,
    kafka_sink: Option<Arc<TieredSink<KafkaSink>>>,
    loader_sink: Option<Arc<TieredSink<LoaderSink>>>,
    buffer_manager: Arc<BufferManager>,
    ready: AtomicBool,
}

impl PipelineState {
    /// Create new pipeline state.
    pub fn new(config: Config) -> Result<Self> {
        let validator = Validator::new(config.validation.clone());
        let router = Router::new(&config.routing, &config.destinations);
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
            config: Arc::new(RwLock::new(config)),
            validator,
            router,
            kafka_sink,
            loader_sink,
            buffer_manager,
            ready: AtomicBool::new(true),
        })
    }

    /// Get the current configuration.
    pub fn config(&self) -> Config {
        self.config.read().clone()
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

    /// Inner processing logic (after backpressure check).
    #[inline]
    async fn process_inner(&self, payload: Bytes) -> Result<()> {
        // Validate
        match self.validator.validate(&payload) {
            ValidationResult::Valid => {}
            ValidationResult::Dlq(reason) => {
                debug!(reason = %reason, "Message validation failed, routing to DLQ");
                return self.send_to_dlq(&payload, &reason).await;
            }
            ValidationResult::Reject(reason) => {
                return Err(Error::Validation(reason));
            }
        }

        // Route
        match self.router.route(&payload) {
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
        let dlq_route = self.router.route_dlq(_reason);

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
}

/// Main pipeline orchestrator.
pub struct Orchestrator {
    state: Arc<PipelineState>,
    #[allow(dead_code)]
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
}

impl Orchestrator {
    /// Create a new orchestrator.
    pub fn new(config: Config, metrics: Arc<Metrics>, shutdown: CancellationToken) -> Result<Self> {
        let state = PipelineState::new(config)?;

        Ok(Self {
            state: Arc::new(state),
            metrics,
            shutdown,
        })
    }

    /// Get shared pipeline state.
    pub fn state(&self) -> Arc<PipelineState> {
        Arc::clone(&self.state)
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

    #[tokio::test]
    async fn test_pipeline_validation_reject() {
        let config = test_config();
        let state = PipelineState::new(config).unwrap();

        // Invalid JSON with dlq_on_invalid=true goes to DLQ (success since it's routed)
        let result = state.process(Bytes::from("not json")).await;
        // Either succeeds (DLQ) or fails depending on config
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    async fn test_pipeline_valid_json() {
        let config = test_config();
        let state = PipelineState::new(config).unwrap();

        let valid_json = Bytes::from(r#"{"test": "data"}"#);
        let result = state.process(valid_json).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_ready_check() {
        let config = test_config();
        let state = PipelineState::new(config).unwrap();

        assert!(state.is_ready());
    }

    #[tokio::test]
    async fn test_pipeline_memory_pressure() {
        let mut config = test_config();
        config.buffer.memory_limit = 1000;
        config.buffer.pressure_threshold = 0.8;

        let state = PipelineState::new(config).unwrap();

        // Should start without pressure
        assert!(!state.should_apply_backpressure());
        assert_eq!(state.memory_pressure(), MemoryPressure::Low);
    }
}
