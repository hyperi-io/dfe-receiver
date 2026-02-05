// Project:   dfe-receiver
// File:      src/sink/mod.rs
// Purpose:   Sink trait and destination dispatching
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Sink module for message delivery to destinations.
//!
//! Provides the `Sink` trait and implementations for Kafka and dfe-loader.

pub mod kafka;
pub mod loader;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;

/// Trait for message sinks (Kafka, loader, etc.).
#[async_trait]
pub trait Sink: Send + Sync {
    /// Send a message to the sink.
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()>;

    /// Flush pending messages.
    async fn flush(&self) -> Result<()>;

    /// Check if the sink is healthy.
    fn is_healthy(&self) -> bool;
}
