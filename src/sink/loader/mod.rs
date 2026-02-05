// Project:   dfe-receiver
// File:      src/sink/loader/mod.rs
// Purpose:   dfe-loader transport sink
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-loader transport sink.
//!
//! Sends messages directly to dfe-loader via hs-rustlib Kafka transport.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use tracing::{debug, error, info};

use crate::config::{KafkaConfig, LoaderConfig};
use crate::error::{Error, Result};
use crate::sink::Sink;

/// dfe-loader sink that sends directly to a loader's Kafka input topic.
pub struct LoaderSink {
    producer: Option<FutureProducer>,
    topic: String,
    healthy: AtomicBool,
    pending_count: RwLock<u64>,
}

impl LoaderSink {
    /// Create a new loader sink.
    ///
    /// Uses Kafka transport to communicate with dfe-loader.
    pub fn new(config: &LoaderConfig, kafka_config: &KafkaConfig) -> Result<Self> {
        // Only create producer if transport is kafka
        let producer = if config.transport == "kafka" && !kafka_config.brokers.is_empty() {
            let mut client_config = ClientConfig::new();

            client_config.set("bootstrap.servers", kafka_config.brokers.join(","));
            client_config.set("client.id", format!("{}-loader", kafka_config.client_id));
            client_config.set("acks", "all");
            client_config.set("retries", "3");
            client_config.set("compression.type", "lz4");

            // TLS settings
            if kafka_config.tls.enabled {
                client_config.set("security.protocol", "ssl");
                if let Some(ref ca) = kafka_config.tls.ca_file {
                    client_config.set("ssl.ca.location", ca);
                }
                if let Some(ref cert) = kafka_config.tls.cert_file {
                    client_config.set("ssl.certificate.location", cert);
                }
                if let Some(ref key) = kafka_config.tls.key_file {
                    client_config.set("ssl.key.location", key);
                }
            }

            let producer: FutureProducer = client_config.create().map_err(Error::Kafka)?;

            Some(producer)
        } else {
            None
        };

        info!(
            address = %config.address,
            transport = %config.transport,
            topic = "dfe-loader-input",
            "Loader sink initialised"
        );

        Ok(Self {
            producer,
            topic: "dfe-loader-input".to_string(),
            healthy: AtomicBool::new(true),
            pending_count: RwLock::new(0),
        })
    }

    /// Create a loader sink for memory/testing transport.
    pub fn new_memory() -> Self {
        info!("Loader sink initialised (memory transport)");
        Self {
            producer: None,
            topic: "dfe-loader-input".to_string(),
            healthy: AtomicBool::new(true),
            pending_count: RwLock::new(0),
        }
    }

    /// Get pending message count.
    pub fn pending_count(&self) -> u64 {
        *self.pending_count.read()
    }
}

#[async_trait]
impl Sink for LoaderSink {
    /// Send a message to the loader.
    async fn send(&self, _topic: &str, payload: Bytes) -> Result<()> {
        // Track pending
        {
            let mut count = self.pending_count.write();
            *count += 1;
        }

        if let Some(ref producer) = self.producer {
            let record: FutureRecord<'_, str, [u8]> =
                FutureRecord::to(&self.topic).payload(&payload[..]);

            match producer.send(record, Duration::from_secs(5)).await {
                Ok(_) => {
                    debug!(topic = %self.topic, "Sent to loader");
                    self.healthy.store(true, Ordering::Relaxed);
                }
                Err((e, _)) => {
                    error!(error = %e, topic = %self.topic, "Loader send failed");
                    self.healthy.store(false, Ordering::Relaxed);
                    return Err(Error::Transport(format!("loader send failed: {e}")));
                }
            }
        } else {
            // Memory transport - just log
            debug!(bytes = payload.len(), "Memory transport: message received");
        }

        // Track completion
        {
            let mut count = self.pending_count.write();
            *count = count.saturating_sub(1);
        }

        Ok(())
    }

    /// Flush pending messages.
    async fn flush(&self) -> Result<()> {
        // rdkafka FutureProducer doesn't have explicit flush, messages are sent immediately
        // The flush semantic is satisfied by awaiting sends
        if self.producer.is_some() {
            debug!("Loader sink flush complete");
        }
        Ok(())
    }

    /// Check if the sink is healthy.
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
