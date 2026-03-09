// Project:   dfe-receiver
// File:      src/sink/loader/mod.rs
// Purpose:   dfe-loader transport sink
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-loader transport sink.
//!
//! Sends messages directly to dfe-loader's Kafka input topic using
//! rustlib KafkaProducer.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use hyperi_rustlib::transport::kafka::{KafkaProducer, ProducerProfile};
use tracing::{debug, error, info};

use crate::config::{KafkaConfig, LoaderConfig};
use crate::error::{Error, Result};
use crate::sink::Sink;

const LOADER_TOPIC: &str = "dfe-loader-input";

/// dfe-loader sink that sends directly to a loader's Kafka input topic.
pub struct LoaderSink {
    producer: Option<KafkaProducer>,
    topic: String,
    healthy: AtomicBool,
    pending_count: AtomicU64,
}

impl LoaderSink {
    /// Create a new loader sink.
    pub fn new(config: &LoaderConfig, kafka_config: &KafkaConfig) -> Result<Self> {
        let producer = if config.transport == "kafka" && !kafka_config.brokers.is_empty() {
            let mut rustlib_config = kafka_config.to_rustlib_kafka_config_for_producer();
            rustlib_config.client_id = format!("{}-loader", kafka_config.client_id);

            let producer = KafkaProducer::new(&rustlib_config, ProducerProfile::HighThroughput)
                .map_err(|e| Error::Transport(format!("failed to create loader producer: {e}")))?;

            Some(producer)
        } else {
            None
        };

        info!(
            address = %config.address,
            transport = %config.transport,
            topic = LOADER_TOPIC,
            "Loader sink initialised"
        );

        Ok(Self {
            producer,
            topic: LOADER_TOPIC.to_string(),
            healthy: AtomicBool::new(true),
            pending_count: AtomicU64::new(0),
        })
    }

    /// Create a loader sink for memory/testing transport.
    pub fn new_memory() -> Self {
        info!("Loader sink initialised (memory transport)");
        Self {
            producer: None,
            topic: LOADER_TOPIC.to_string(),
            healthy: AtomicBool::new(true),
            pending_count: AtomicU64::new(0),
        }
    }

    /// Get pending message count.
    pub fn pending_count(&self) -> u64 {
        self.pending_count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Sink for LoaderSink {
    async fn send(&self, _topic: &str, payload: Bytes) -> Result<()> {
        self.pending_count.fetch_add(1, Ordering::Relaxed);

        let result = if let Some(ref producer) = self.producer {
            match producer.send(&self.topic, None, &payload) {
                Ok(()) => {
                    debug!(topic = %self.topic, "Sent to loader");
                    self.healthy.store(true, Ordering::Relaxed);
                    Ok(())
                }
                Err(e) => {
                    error!(error = %e, topic = %self.topic, "Loader send failed");
                    self.healthy.store(false, Ordering::Relaxed);
                    Err(Error::Transport(format!("loader send failed: {e}")))
                }
            }
        } else {
            debug!(bytes = payload.len(), "Memory transport: message received");
            Ok(())
        };

        self.pending_count.fetch_sub(1, Ordering::Relaxed);
        result
    }

    async fn flush(&self) -> Result<()> {
        if let Some(ref producer) = self.producer {
            use std::time::Duration;
            let remaining = producer.flush(Duration::from_secs(30));
            if remaining > 0 {
                error!(remaining = remaining, "Loader flush timed out");
            }
            debug!("Loader sink flush complete");
        }
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_transport() {
        let sink = LoaderSink::new_memory();
        assert!(sink.is_healthy());
        assert_eq!(sink.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_memory_send() {
        let sink = LoaderSink::new_memory();
        let payload = Bytes::from(r#"{"test": "data"}"#);

        let result = sink.send("test", payload).await;
        assert!(result.is_ok());
        assert!(sink.is_healthy());
    }

    #[tokio::test]
    async fn test_memory_flush() {
        let sink = LoaderSink::new_memory();
        let result = sink.flush().await;
        assert!(result.is_ok());
    }
}
