// Project:   dfe-receiver
// File:      src/sink/grpc/mod.rs
// Purpose:   dfe-loader gRPC transport sink
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-loader gRPC transport sink.
//!
//! Sends messages directly to dfe-loader using the DFE native gRPC protocol
//! (scalo `GrpcTransport`, client-only mode). Used when
//! `loader.transport = "grpc"`, enabling receiver→loader communication
//! without Kafka (e.g., inside dfe-docker).

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use scalo::transport::{GrpcConfig, GrpcTransport, SendResult};
use scalo::transport::{TransportBase, TransportSender};
use tracing::{debug, error, info};

use crate::error::{Error, Result};
use crate::sink::Sink;

/// The client config a sink dials with; `None` keeps scalo's own deadline.
fn client_config(endpoint: &str, send_timeout_ms: Option<u64>) -> GrpcConfig {
    let mut config = GrpcConfig::client(endpoint);
    if let Some(timeout) = send_timeout_ms {
        config.send_timeout_ms = timeout;
    }
    config
}

/// dfe-loader sink using DFE native gRPC transport.
pub struct GrpcSink {
    transport: GrpcTransport,
    healthy: AtomicBool,
}

impl GrpcSink {
    /// Create a new gRPC loader sink connecting to the given endpoint.
    ///
    /// `send_timeout_ms` bounds a single Push RPC, so a loader that accepts the
    /// connection and then stops answering cannot hold a sender task forever;
    /// `None` keeps scalo's 30s default.
    ///
    /// Uses lazy connection — does not fail until the first RPC.
    pub async fn new(endpoint: &str, send_timeout_ms: Option<u64>) -> Result<Self> {
        let config = client_config(endpoint, send_timeout_ms);
        let transport = GrpcTransport::new(&config)
            .await
            .map_err(|e| Error::Transport(format!("gRPC sink init failed: {e}")))?;

        info!(
            endpoint = %endpoint,
            send_timeout_ms = config.send_timeout_ms,
            "gRPC loader sink initialised"
        );

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
        // scalo: TransportSender::send takes owned `Bytes`
        // (reqwest/tonic bodies are zero-copy from Bytes). The clone is a
        // refcount bump, not a payload copy.
        let bytes = payload.len();
        match self.transport.send(topic, payload).await {
            SendResult::Ok | SendResult::FilteredDlq => {
                debug!(topic = %topic, bytes, "Sent to loader via gRPC");
                self.healthy.store(true, Ordering::Relaxed);
                Ok(())
            }
            SendResult::Backpressured => {
                // Signal backpressure so buffer backend can handle
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
        let sink = GrpcSink::new("http://localhost:19999", None).await.unwrap();
        assert!(sink.is_healthy());
    }

    #[test]
    fn a_configured_deadline_bounds_the_push_rpc() {
        let config = client_config("http://loader:6000", Some(1_500));
        assert_eq!(config.send_timeout_ms, 1_500);
    }

    #[test]
    fn no_configured_deadline_keeps_scalos_own() {
        let default = GrpcConfig::client("http://loader:6000").send_timeout_ms;
        let config = client_config("http://loader:6000", None);
        assert_eq!(config.send_timeout_ms, default);
    }

    #[tokio::test]
    async fn test_grpc_sink_send_fails_no_server() {
        let sink = GrpcSink::new("http://localhost:19998", None).await.unwrap();
        let payload = Bytes::from(r#"{"test": "data"}"#);

        // Send will fail (no server) — error is expected
        let result = sink.send("test-topic", payload).await;
        assert!(result.is_err());
    }
}
