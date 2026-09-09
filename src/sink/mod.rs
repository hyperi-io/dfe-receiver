// Project:   dfe-receiver
// File:      src/sink/mod.rs
// Purpose:   Sink trait and destination dispatching
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Sink module for message delivery to destinations.
//!
//! Provides the `Sink` trait and implementations for the bus, a gRPC listener
//! and a debug file.

pub mod file;
pub mod grpc;
pub mod kafka;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;

/// Trait for message sinks (Kafka, gRPC, file).
#[async_trait]
pub trait Sink: Send + Sync {
    /// Send a message to the sink.
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()>;

    /// Flush pending messages.
    async fn flush(&self) -> Result<()>;

    /// Check if the sink is healthy.
    fn is_healthy(&self) -> bool;
}
