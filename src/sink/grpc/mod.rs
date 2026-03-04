// Project:   dfe-receiver
// File:      src/sink/grpc/mod.rs
// Purpose:   dfe-loader gRPC transport sink
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-loader gRPC transport sink.
//!
//! Sends messages directly to dfe-loader using the DFE native gRPC protocol
//! (hyperi-rustlib `GrpcTransport`, client-only mode). Used when
//! `loader.transport = "grpc"`, enabling receiver→loader communication
//! without Kafka (e.g., inside dfe-docker).

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use hyperi_rustlib::transport::{GrpcConfig, GrpcTransport, SendResult};
use hyperi_rustlib::Transport;
use tracing::{debug, error, info};

use crate::error::{Error, Result};
use crate::sink::Sink;

/// dfe-loader sink using DFE native gRPC transport.
pub struct GrpcSink {
    transport: GrpcTransport,
    healthy: AtomicBool,
}

impl GrpcSink {
    /// Create a new gRPC loader sink connecting to the given endpoint.
    ///
    /// Uses lazy connection — does not fail until the first RPC.
    pub async fn new(endpoint: &str) -> Result<Self> {
        let config = GrpcConfig::client(endpoint);
        let transport = GrpcTransport::new(&config)
            .await
            .map_err(|e| Error::Transport(format!("gRPC sink init failed: {e}")))?;

        info!(endpoint = %endpoint, "gRPC loader sink initialised");

        Ok(Self {
            transport,
            healthy: AtomicBool::new(true),
        })
    }
}

#[async_trait]
impl Sink for GrpcSink {
    /// Send a message to dfe-loader via gRPC.
    ///
    /// The topic is passed as the `key` to the DFE Push RPC, where dfe-loader
    /// uses it for routing.
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
        match self.transport.send(topic, &payload).await {
            SendResult::Ok => {
                debug!(topic = %topic, bytes = payload.len(), "Sent to loader via gRPC");
                self.healthy.store(true, Ordering::Relaxed);
                Ok(())
            }
            SendResult::Backpressured => {
                // Signal backpressure so TieredSink can buffer
                Err(Error::Transport("gRPC loader backpressured".into()))
            }
            SendResult::Fatal(e) => {
                error!(error = %e, topic = %topic, "gRPC loader send failed");
                self.healthy.store(false, Ordering::Relaxed);
                Err(Error::Transport(format!("gRPC loader send failed: {e}")))
            }
        }
    }

    /// Flush pending messages.
    ///
    /// No-op for gRPC — acknowledgement is implicit in the Push RPC response.
    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// Check if the sink is healthy.
    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed) && self.transport.is_healthy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_grpc_sink_lazy_connect() {
        // Lazy connection — init succeeds even with no server at the endpoint
        let sink = GrpcSink::new("http://localhost:19999").await.unwrap();
        assert!(sink.is_healthy());
    }

    #[tokio::test]
    async fn test_grpc_sink_send_fails_no_server() {
        let sink = GrpcSink::new("http://localhost:19998").await.unwrap();
        let payload = Bytes::from(r#"{"test": "data"}"#);

        // Send will fail (no server) — error is expected
        let result = sink.send("test-topic", payload).await;
        assert!(result.is_err());
    }
}
