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

use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::{RETRY_AFTER_SECS, Result};

/// Trait for message sinks (Kafka, gRPC, file).
#[async_trait]
pub trait Sink: Send + Sync {
    /// Send a message to the sink.
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()>;

    /// Send `payloads` to `topic` together. The default sends each in turn and
    /// stops at the first failure.
    async fn send_batch(&self, topic: &str, payloads: &[Bytes]) -> Result<()> {
        for payload in payloads {
            self.send(topic, payload.clone()).await?;
        }
        Ok(())
    }

    /// Why this sink refuses `payload` for good, known before it is sent. The
    /// default refuses nothing up front.
    fn refuses(&self, payload: &Bytes) -> Option<String> {
        let _ = payload;
        None
    }

    /// Flush pending messages.
    async fn flush(&self) -> Result<()>;

    /// Check if the sink is healthy.
    fn is_healthy(&self) -> bool;
}

/// How long a sink that failed is reported unhealthy before traffic is let
/// through to try it again: the wait a refused sender is asked for.
pub const SINK_RETRY_AFTER: Duration = Duration::from_secs(RETRY_AFTER_SECS);

/// Process start, the zero of [`FailureLatch`]'s clock.
static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// A sink's last failure, for a health flag that only traffic can restore.
///
/// Readiness sheds every request while a sink is unhealthy, so a flag set by a
/// failure and cleared only by a later success would never clear. Once
/// [`SINK_RETRY_AFTER`] has passed since the last failure the latch admits
/// traffic again, and the next result sets or clears it.
#[derive(Debug, Default)]
pub struct FailureLatch {
    /// Milliseconds since [`EPOCH`] of the last failure, plus one; zero is
    /// clear.
    failed_at: AtomicU64,
}

impl FailureLatch {
    /// Record a failure now.
    pub fn trip(&self) {
        let now = u64::try_from(EPOCH.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.failed_at
            .store(now.saturating_add(1), Ordering::Relaxed);
    }

    /// Record a success.
    pub fn clear(&self) {
        self.failed_at.store(0, Ordering::Relaxed);
    }

    /// Whether the last result recorded was a failure.
    #[must_use]
    pub fn is_tripped(&self) -> bool {
        self.failed_at.load(Ordering::Relaxed) != 0
    }

    /// Whether traffic may go through: no failure since the last success, or
    /// `retry_after` since the last failure.
    #[must_use]
    pub fn admits(&self, retry_after: Duration) -> bool {
        let failed_at = self.failed_at.load(Ordering::Relaxed);
        if failed_at == 0 {
            return true;
        }
        let now = u64::try_from(EPOCH.elapsed().as_millis()).unwrap_or(u64::MAX);
        let waited = now.saturating_add(1).saturating_sub(failed_at);
        u128::from(waited) >= retry_after.as_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tripped_latch_admits_again_after_the_retry_wait() {
        let latch = FailureLatch::default();
        assert!(latch.admits(Duration::from_secs(60)));
        assert!(!latch.is_tripped());

        latch.trip();
        assert!(latch.is_tripped());
        assert!(
            !latch.admits(Duration::from_secs(60)),
            "refused while fresh"
        );
        assert!(
            latch.admits(Duration::ZERO),
            "admitted once the wait has passed"
        );

        latch.clear();
        assert!(!latch.is_tripped());
        assert!(latch.admits(Duration::from_secs(60)));
    }
}
