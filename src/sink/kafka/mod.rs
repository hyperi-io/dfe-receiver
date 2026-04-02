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
//! Emits per-send duration histogram and send/error counters via the global
//! metrics recorder.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use hyperi_rustlib::transport::kafka::{KafkaProducer, ProducerProfile};
use tracing::{debug, error, info, trace};

use crate::config::KafkaConfig;
use crate::error::{Error, Result};
use crate::sink::Sink;

/// Sampled counter for Kafka send errors (log 1 in 1000).
static KAFKA_ERRORS: AtomicU64 = AtomicU64::new(0);

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
        let start = Instant::now();
        let bytes = payload.len() as u64;

        trace!(topic, bytes, "Kafka produce enqueue");

        match self.producer.send(topic, None, &payload) {
            Ok(()) => {
                let elapsed = start.elapsed();
                let elapsed_secs = elapsed.as_secs_f64();
                metrics::histogram!("dfe_receiver_kafka_send_duration_seconds")
                    .record(elapsed_secs);
                metrics::counter!("dfe_receiver_kafka_sends_total").increment(1);
                metrics::counter!("dfe_receiver_kafka_bytes_sent_total").increment(bytes);
                debug!(
                    topic,
                    bytes,
                    duration_us = elapsed.as_micros(),
                    "Kafka message enqueued"
                );
                self.healthy.store(true, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                metrics::counter!("dfe_receiver_kafka_send_errors_total").increment(1);
                if hyperi_rustlib::logger::log_sampled(&KAFKA_ERRORS, 1000) {
                    let total = KAFKA_ERRORS.load(Ordering::Relaxed);
                    error!(error = %e, topic, total_errors = total, "Kafka send failed (1 in 1000)");
                }
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
