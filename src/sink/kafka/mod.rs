// Project:   dfe-receiver
// File:      src/sink/kafka/mod.rs
// Purpose:   Kafka producer sink using rustlib KafkaProducer
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka sink using hyperi-rustlib KafkaProducer.
//!
//! Delegates all batching and compression to librdkafka via
//! `KafkaProducer::HighThroughput` profile (256KB batches, 100ms linger, LZ4).

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use hyperi_rustlib::transport::kafka::{KafkaProducer, ProducerProfile};
use tracing::{error, info};

use crate::config::KafkaConfig;
use crate::error::{Error, Result};
use crate::sink::Sink;

/// Kafka sink backed by rustlib KafkaProducer.
pub struct KafkaSink {
    producer: KafkaProducer,
    healthy: AtomicBool,
}

impl KafkaSink {
    /// Create a new Kafka sink.
    pub fn new(config: &KafkaConfig) -> Result<Self> {
        let rustlib_config = config.to_rustlib_kafka_config_for_producer();
        let producer = KafkaProducer::new(&rustlib_config, ProducerProfile::HighThroughput)
            .map_err(|e| Error::Transport(format!("failed to create Kafka producer: {e}")))?;

        info!(
            brokers = ?config.brokers,
            profile = "high_throughput",
            "Kafka producer initialised"
        );

        Ok(Self {
            producer,
            healthy: AtomicBool::new(true),
        })
    }
}

#[async_trait]
impl Sink for KafkaSink {
    /// Send a message to a topic (non-blocking, librdkafka batches internally).
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
        match self.producer.send(topic, None, &payload) {
            Ok(()) => {
                self.healthy.store(true, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                error!(error = %e, topic = topic, "Kafka send failed");
                self.healthy.store(false, Ordering::Relaxed);
                Err(Error::Transport(format!("kafka send failed: {e}")))
            }
        }
    }

    /// Flush all queued messages (blocks until delivered or timeout).
    async fn flush(&self) -> Result<()> {
        use std::time::Duration;
        let remaining = self.producer.flush(Duration::from_secs(30));
        if remaining > 0 {
            error!(
                remaining = remaining,
                "Kafka flush timed out with messages in flight"
            );
        }
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    // Integration tests require a running Kafka broker — see tests/integration_kafka.rs
}
