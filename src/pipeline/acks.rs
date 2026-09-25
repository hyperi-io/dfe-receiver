// Project:   dfe-receiver
// File:      src/pipeline/acks.rs
// Purpose:   Hold a listener's answer until every destination confirmed delivery
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Held answers.
//!
//! A listener with `acknowledgements.enabled` (the default) answers its sender
//! only once every destination confirmed the request's records: a Kafka
//! delivery report, a gRPC destination's answer, or a dead-letter write the DLQ
//! confirmed. A failure, or a hold that runs out, answers the protocol's
//! retryable refusal, and the sender keeps its copy. That refusal means "not
//! confirmed", not "not written": a record can land after its hold ran out, so a
//! retry can deliver it twice.
//!
//! Admission, the held-byte ceiling and the `transport_ack_*` metrics are
//! scalo's [`Tickets`]. The pipeline's own pressure brake and memory lease stay
//! in front of them.

use std::time::{Duration, Instant};

use scalo::transport::AcknowledgementsConfig;
use scalo::transport::ack::{
    AckControl, AckKind, EffectiveGuarantee, HeldAcks, Refused, SinkConfirmation, Ticket, Tickets,
};
use scalo::transport::grpc::hold_budget;

/// The longest a listener holds an answer.
pub const MAX_HOLD: Duration = Duration::from_secs(25);

/// Deadline for one send to the next gRPC hop, inside [`MAX_HOLD`].
pub const NEXT_HOP_DEADLINE_MS: u64 = 20_000;

/// librdkafka's `message.timeout.ms` while answers are held: inside
/// [`MAX_HOLD`], so a delivery report settles a request before its hold ends.
pub const HELD_MESSAGE_TIMEOUT: Duration = Duration::from_secs(20);

/// Bytes one listener may hold when the memory guard reports no limit.
const DEFAULT_MAX_HELD_BYTES: u64 = 256 * 1024 * 1024;

/// Bytes one listener may hold unanswered: a quarter of the memory limit.
#[must_use]
pub fn max_held_bytes(memory_limit: u64) -> u64 {
    if memory_limit == 0 {
        DEFAULT_MAX_HELD_BYTES
    } else {
        memory_limit / 4
    }
}

/// A listener's acknowledgement setting, and the admission for the requests it
/// holds.
///
/// Clones share the held-byte ceiling.
#[derive(Clone, Debug)]
pub struct Acks {
    tickets: Option<Tickets>,
    max_hold: Duration,
}

impl Acks {
    /// Answer every request once its records are queued.
    #[must_use]
    pub fn at_enqueue() -> Self {
        Self {
            tickets: None,
            max_hold: MAX_HOLD,
        }
    }

    /// The setting of listener `transport` (the metric label), whose own
    /// request timeout, where it has one, bounds the hold.
    #[must_use]
    pub fn new(
        transport: &'static str,
        config: AcknowledgementsConfig,
        request_timeout: Option<Duration>,
        max_held_bytes: u64,
    ) -> Self {
        let acks = Self {
            tickets: config
                .enabled
                .then(|| Tickets::new(transport, max_held_bytes)),
            max_hold: hold_budget(MAX_HOLD, request_timeout),
        };
        EffectiveGuarantee::of(
            Some(&ListenerAcks(config.enabled)),
            SinkConfirmation::Remote,
        )
        .publish();
        acks
    }

    /// The same setting and held-byte ceiling, holding an answer no longer
    /// than `max_hold`.
    #[must_use]
    pub fn holding_at_most(&self, max_hold: Duration) -> Self {
        Self {
            tickets: self.tickets.clone(),
            max_hold: self.max_hold.min(max_hold),
        }
    }

    /// Whether this listener holds its answers.
    #[must_use]
    pub fn holds(&self) -> bool {
        self.tickets.is_some()
    }

    /// Admit a request of `bytes` whose sender allows `sender_deadline`, or
    /// `None` when this listener answers at enqueue.
    pub(crate) fn admit(
        &self,
        bytes: u64,
        sender_deadline: Option<Duration>,
    ) -> Option<Result<Ticket, Refused>> {
        let tickets = self.tickets.as_ref()?;
        let deadline = Instant::now() + hold_budget(self.max_hold, sender_deadline);
        Some(tickets.admit(bytes, deadline))
    }
}

/// Publish the guarantee of a listener whose protocol carries no
/// acknowledgement: syslog, GELF, flow.
pub fn publish_unacknowledged_listener() {
    EffectiveGuarantee::of(None, SinkConfirmation::Remote).publish();
}

/// A listener's setting in the shape scalo's guarantee metric reads.
struct ListenerAcks(bool);

impl AckControl for ListenerAcks {
    fn enabled(&self) -> bool {
        self.0
    }

    fn arm(&self) {}

    fn is_armed(&self) -> bool {
        true
    }

    fn kind(&self) -> AckKind {
        AckKind::Push
    }

    fn held(&self) -> HeldAcks {
        HeldAcks::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// How long `acks` holds an answer for a sender that sets no deadline.
    fn held_for(acks: &Acks) -> Duration {
        let ticket = acks.admit(10, None).expect("holding").expect("admitted");
        ticket.deadline().saturating_duration_since(Instant::now())
    }

    /// The Kafka report and the next gRPC hop both settle inside the hold.
    #[test]
    fn downstream_deadlines_sit_inside_the_hold() {
        assert!(HELD_MESSAGE_TIMEOUT < MAX_HOLD);
        assert!(Duration::from_millis(NEXT_HOP_DEADLINE_MS) < MAX_HOLD);
    }

    /// A listener bounded by its own request timeout holds past the Kafka
    /// message timeout and the next gRPC hop at its default, so a slow
    /// delivery is confirmed rather than answered retry.
    #[test]
    fn every_default_request_timeout_holds_past_the_downstream_deadlines() {
        let config = crate::config::Config::default();
        for (listener, timeout_ms) in [
            ("server", config.server.request_timeout_ms),
            ("splunk_hec", config.splunk_hec.request_timeout_ms),
            ("prometheus_rw", config.prometheus_rw.request_timeout_ms),
            ("webhook", config.webhook.request_timeout_ms),
        ] {
            let acks = Acks::new(
                "test",
                AcknowledgementsConfig::default(),
                Some(Duration::from_millis(timeout_ms)),
                1_000,
            );
            let hold = held_for(&acks);
            assert!(hold > HELD_MESSAGE_TIMEOUT, "{listener} holds {hold:?}");
            assert!(
                hold > Duration::from_millis(NEXT_HOP_DEADLINE_MS),
                "{listener} holds {hold:?}"
            );
        }
    }

    /// A cap shortens the hold and keeps the listener's held-byte ceiling.
    #[test]
    fn a_capped_hold_answers_by_its_cap() {
        let acks = Acks::new("test", AcknowledgementsConfig::default(), None, 1_000);
        let capped = acks.holding_at_most(Duration::from_secs(9));
        let hold = held_for(&capped);
        assert!(
            hold <= Duration::from_secs(9) && hold > Duration::from_secs(8),
            "held {hold:?}"
        );
        assert!(
            held_for(&acks.holding_at_most(Duration::from_secs(60))) > Duration::from_secs(24),
            "a cap above the hold leaves it at {MAX_HOLD:?}"
        );

        let _held = capped.admit(990, None).expect("holding").expect("admitted");
        assert!(
            matches!(acks.admit(100, None), Some(Err(_))),
            "the capped copy's held bytes count against the listener's ceiling"
        );
    }

    #[test]
    fn a_listener_holds_a_quarter_of_the_memory_limit() {
        assert_eq!(max_held_bytes(4_000), 1_000);
        assert_eq!(max_held_bytes(0), DEFAULT_MAX_HELD_BYTES);
    }

    #[test]
    fn only_an_enabled_listener_admits() {
        assert!(Acks::at_enqueue().admit(10, None).is_none());
        let off = Acks::new("test", AcknowledgementsConfig::new(false), None, 1_000);
        assert!(!off.holds());
        assert!(off.admit(10, None).is_none());

        let on = Acks::new("test", AcknowledgementsConfig::default(), None, 1_000);
        assert!(on.holds());
        let ticket = on.admit(10, None).expect("holding").expect("admitted");
        let remaining = ticket.deadline().saturating_duration_since(Instant::now());
        assert!(
            remaining <= MAX_HOLD && remaining > MAX_HOLD.saturating_sub(Duration::from_secs(1))
        );
    }
}
