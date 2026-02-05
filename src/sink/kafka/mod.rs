// Project:   dfe-receiver
// File:      src/sink/kafka/mod.rs
// Purpose:   Kafka producer with batching
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka sink with per-topic batching.
//!
//! Implements optimised batching (10K messages / 8MiB / 20ms) before
//! sending to Kafka with zstd compression.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use tracing::{debug, error, info};

use crate::config::KafkaConfig;
use crate::error::{Error, Result};
use crate::sink::Sink;

/// Kafka sink with per-topic batching.
pub struct KafkaSink {
    producer: FutureProducer,
    batches: RwLock<HashMap<String, TopicBatch>>,
    config: BatchConfig,
    healthy: AtomicBool,
}

/// Configuration for batching.
#[derive(Debug, Clone)]
struct BatchConfig {
    max_bytes: usize,
    max_messages: usize,
    max_age: Duration,
}

/// Per-topic batch accumulator.
struct TopicBatch {
    messages: Vec<Bytes>,
    bytes: usize,
    created_at: Instant,
}

impl TopicBatch {
    fn new() -> Self {
        Self {
            messages: Vec::with_capacity(10_000),
            bytes: 0,
            created_at: Instant::now(),
        }
    }

    fn should_flush(&self, config: &BatchConfig) -> bool {
        self.bytes >= config.max_bytes
            || self.messages.len() >= config.max_messages
            || self.created_at.elapsed() >= config.max_age
    }
}

impl KafkaSink {
    /// Create a new Kafka sink.
    pub fn new(config: &KafkaConfig) -> Result<Self> {
        let mut client_config = ClientConfig::new();

        // Set brokers
        client_config.set("bootstrap.servers", config.brokers.join(","));

        // Set client ID
        client_config.set("client.id", &config.client_id);

        // Producer settings
        client_config.set("batch.size", config.producer.batch_size.to_string());
        client_config.set("linger.ms", config.producer.linger_ms.to_string());
        client_config.set("compression.type", &config.producer.compression);
        client_config.set("acks", &config.producer.acks);
        client_config.set("retries", config.producer.retries.to_string());

        // Message size
        client_config.set("message.max.bytes", "8388608"); // 8MiB

        // TLS settings
        if config.tls.enabled {
            client_config.set("security.protocol", "ssl");
            if let Some(ref ca) = config.tls.ca_file {
                client_config.set("ssl.ca.location", ca);
            }
            if let Some(ref cert) = config.tls.cert_file {
                client_config.set("ssl.certificate.location", cert);
            }
            if let Some(ref key) = config.tls.key_file {
                client_config.set("ssl.key.location", key);
            }
        }

        // SASL settings
        if let Some(ref sasl) = config.sasl {
            if sasl.enabled {
                let protocol = if config.tls.enabled {
                    "sasl_ssl"
                } else {
                    "sasl_plaintext"
                };
                client_config.set("security.protocol", protocol);
                client_config.set("sasl.mechanism", &sasl.mechanism.to_uppercase());
                client_config.set("sasl.username", &sasl.username);
                client_config.set("sasl.password", &sasl.password);
            }
        }

        let producer: FutureProducer = client_config.create().map_err(Error::Kafka)?;

        let batch_config = BatchConfig {
            max_bytes: config.producer.batch_size,
            max_messages: config.producer.batch_messages,
            max_age: Duration::from_millis(config.producer.linger_ms as u64),
        };

        info!(
            brokers = ?config.brokers,
            batch_size = config.producer.batch_size,
            batch_messages = config.producer.batch_messages,
            linger_ms = config.producer.linger_ms,
            "Kafka producer initialised"
        );

        Ok(Self {
            producer,
            batches: RwLock::new(HashMap::new()),
            config: batch_config,
            healthy: AtomicBool::new(true),
        })
    }

    /// Flush a specific topic's batch.
    async fn flush_topic(&self, topic: &str) -> Result<()> {
        let batch = {
            let mut batches = self.batches.write();
            batches.remove(topic)
        };

        let Some(batch) = batch else {
            return Ok(());
        };

        if batch.messages.is_empty() {
            return Ok(());
        }

        debug!(
            topic = topic,
            messages = batch.messages.len(),
            bytes = batch.bytes,
            "Flushing Kafka batch"
        );

        // Send messages and await immediately to avoid lifetime issues
        for msg in batch.messages {
            let record: FutureRecord<'_, str, [u8]> = FutureRecord::to(topic).payload(&msg[..]);
            match self.producer.send(record, Duration::from_secs(5)).await {
                Ok(_) => {}
                Err((e, _)) => {
                    error!(error = %e, topic = topic, "Kafka send failed");
                    self.healthy.store(false, Ordering::Relaxed);
                    return Err(Error::Kafka(e));
                }
            }
        }

        self.healthy.store(true, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl Sink for KafkaSink {
    /// Add a message to the batch for a topic.
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
        let should_flush = {
            let mut batches = self.batches.write();
            let batch = batches
                .entry(topic.to_string())
                .or_insert_with(TopicBatch::new);

            batch.bytes += payload.len();
            batch.messages.push(payload);

            batch.should_flush(&self.config)
        };

        if should_flush {
            self.flush_topic(topic).await?;
        }

        Ok(())
    }

    /// Flush all pending batches.
    async fn flush(&self) -> Result<()> {
        let topics: Vec<String> = {
            let batches = self.batches.read();
            batches.keys().cloned().collect()
        };

        for topic in topics {
            self.flush_topic(&topic).await?;
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
    // Integration tests would require a running Kafka broker
}
