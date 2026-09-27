// Project:   dfe-receiver
// File:      src/server/hold.rs
// Purpose:   Hold records on a stream that carries no acknowledgement
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Holding records on a stream that cannot tell its sender to retry.
//!
//! Syslog over TCP (RFC 6587 defines framing and nothing else), GELF over TCP,
//! and Fluent Forward without the `chunk` option carry no acknowledgement, so
//! TCP flow control is the only signal the sender reads. A listener that cannot
//! hand a record to the pipeline keeps it and stops reading the socket: the
//! kernel buffers fill, the sender's writes block, and the record is offered
//! again until the pipeline takes it. A listener that holds its answers reads
//! on only once every destination confirmed the records.

use std::time::Duration;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::metrics::{DropReason, Metrics};
use crate::pipeline::{Acks, PipelineState};

/// Wait before the first re-offer.
const FIRST_RETRY: Duration = Duration::from_millis(100);

/// Longest wait between re-offers, so a pipeline that recovers is found within it.
const LAST_RETRY: Duration = Duration::from_secs(2);

/// Offer `payloads` to the pipeline until every one is settled -- taken, or
/// refused for good -- holding the caller, and the socket it reads, until then.
///
/// Counts the request once, and every record dropped. Returns false when
/// shutdown came first: the records not yet taken are dropped, and the caller
/// must stop reading.
pub(crate) async fn hold_until_settled(
    pipeline: &PipelineState,
    payloads: &[Bytes],
    metrics: &Metrics,
    transport: &str,
    shutdown: &CancellationToken,
    acks: &Acks,
) -> bool {
    let mut pending = payloads;
    let mut rejected = 0;
    let mut wait = FIRST_RETRY;
    loop {
        let outcome = pipeline.process_batch_acked(pending, acks, None).await;
        rejected += outcome.rejected;
        let settled = outcome.settled();
        let Some(e) = outcome.unavailable else {
            break;
        };
        pending = &pending[settled..];
        metrics.record_backpressure();
        debug!(
            transport,
            error = %e,
            held = pending.len(),
            "Pipeline could not take the records; holding the connection"
        );
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                metrics.inc_requests_error(transport);
                metrics.add_records_dropped(transport, DropReason::Rejected, rejected as u64);
                metrics.add_records_dropped(transport, DropReason::Shutdown, pending.len() as u64);
                return false;
            }
            () = tokio::time::sleep(wait) => {}
        }
        wait = (wait * 2).min(LAST_RETRY);
    }

    // Nothing in these protocols can carry a refusal back to the sender.
    metrics.add_records_dropped(transport, DropReason::Rejected, rejected as u64);
    if rejected == 0 {
        metrics.inc_requests_success(transport);
    } else {
        metrics.inc_requests_error(transport);
    }
    true
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use scalo::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};

    use super::*;
    use crate::config::{Config, SharedConfig};

    /// A pipeline on the memory transport whose guard counts only its own
    /// reservations, so a test can put it under pressure and take it off.
    async fn pipeline() -> Arc<PipelineState> {
        let mut config = Config::default();
        config.destinations.default = "loader".into();
        config.loader.transport = "memory".to_string();
        let guard = MemoryGuard::with_usage_source(
            MemoryGuardConfig {
                limit_bytes: 1_000_000,
                pressure_threshold: 0.8,
                ..Default::default()
            },
            UsageSource::Reservations,
        );
        Arc::new(
            PipelineState::with_governor(
                SharedConfig::new(config),
                CancellationToken::new(),
                None,
                Some(Arc::new(guard)),
            )
            .await
            .unwrap(),
        )
    }

    /// A record the pipeline cannot take is held, then taken once it can.
    #[tokio::test]
    async fn a_held_record_is_taken_once_the_pipeline_recovers() {
        let pipeline = pipeline().await;
        let metrics = Arc::new(Metrics::default());
        pipeline.memory_guard().add_bytes(900_000);

        let held = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            let metrics = Arc::clone(&metrics);
            async move {
                hold_until_settled(
                    &pipeline,
                    &[Bytes::from_static(br#"{"held":true}"#)],
                    &metrics,
                    "test",
                    &CancellationToken::new(),
                    &Acks::at_enqueue(),
                )
                .await
            }
        });

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!held.is_finished(), "the record was let go under pressure");
        assert_eq!(metrics.get_requests_success(), 0);

        pipeline.memory_guard().release(900_000);
        let kept_reading = tokio::time::timeout(Duration::from_secs(5), held)
            .await
            .unwrap()
            .unwrap();

        assert!(kept_reading);
        assert_eq!(metrics.get_requests_success(), 1);
        assert_eq!(metrics.get_records_dropped(), 0);
    }

    /// Shutdown during a hold drops the record and counts it.
    #[tokio::test]
    async fn shutdown_during_a_hold_drops_and_counts_the_record() {
        let pipeline = pipeline().await;
        let metrics = Metrics::default();
        pipeline.memory_guard().add_bytes(900_000);
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let kept_reading = hold_until_settled(
            &pipeline,
            &[Bytes::from_static(br#"{"held":true}"#)],
            &metrics,
            "test",
            &shutdown,
            &Acks::at_enqueue(),
        )
        .await;

        assert!(!kept_reading);
        assert_eq!(metrics.get_records_dropped(), 1);
        assert_eq!(metrics.get_requests_success(), 0);
    }

    /// A record refused for good is not retried: it is counted as dropped.
    #[tokio::test]
    async fn a_rejected_record_is_not_held() {
        let pipeline = pipeline().await;
        let metrics = Metrics::default();
        let mut config = pipeline.config();
        config.validation.dlq_on_invalid = false;
        pipeline.rebuild_components(&config);

        let kept_reading = tokio::time::timeout(
            Duration::from_secs(1),
            hold_until_settled(
                &pipeline,
                &[Bytes::from_static(b"not json")],
                &metrics,
                "test",
                &CancellationToken::new(),
                &Acks::at_enqueue(),
            ),
        )
        .await
        .unwrap();

        assert!(kept_reading);
        assert_eq!(metrics.get_records_dropped(), 1);
        assert_eq!(metrics.get_requests_error(), 1);
    }

    /// Holding, the socket is read on only once the records are confirmed.
    #[tokio::test]
    async fn a_held_listener_reads_on_once_the_records_are_confirmed() {
        let pipeline = pipeline().await;
        let metrics = Metrics::default();
        let acks = pipeline.acks(
            "test",
            scalo::transport::AcknowledgementsConfig::default(),
            None,
        );

        let kept_reading = tokio::time::timeout(
            Duration::from_secs(5),
            hold_until_settled(
                &pipeline,
                &[Bytes::from_static(br#"{"held":true}"#)],
                &metrics,
                "test",
                &CancellationToken::new(),
                &acks,
            ),
        )
        .await
        .unwrap();

        assert!(kept_reading);
        assert_eq!(metrics.get_requests_success(), 1);
    }
}
