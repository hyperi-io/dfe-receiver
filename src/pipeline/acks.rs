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

/// The longest a listener holds an answer.
pub const MAX_HOLD: Duration = Duration::from_secs(25);

/// Deadline for one send to the next gRPC hop, inside [`MAX_HOLD`].
pub const NEXT_HOP_DEADLINE_MS: u64 = 20_000;

/// librdkafka's `message.timeout.ms` while answers are held: inside
/// [`MAX_HOLD`], so a delivery report settles a request before its hold ends.
pub const HELD_MESSAGE_TIMEOUT: Duration = Duration::from_secs(20);

/// The least headroom kept below a sender's deadline.
const MIN_MARGIN: Duration = Duration::from_secs(1);

/// Bytes one listener may hold when the memory guard reports no limit.
const DEFAULT_MAX_HELD_BYTES: u64 = 256 * 1024 * 1024;

/// How long an answer may be held for a sender that allows `deadline`: up to
/// `max_hold`, and a margin of a tenth of the deadline (at least a second)
/// short of it, so the answer reaches the sender in time.
#[must_use]
pub fn hold_budget(max_hold: Duration, deadline: Option<Duration>) -> Duration {
    let Some(deadline) = deadline else {
        return max_hold;
    };
    let margin = (deadline / 10).max(MIN_MARGIN).min(deadline / 2);
    max_hold.min(deadline.saturating_sub(margin))
}

/// The deadline a gRPC sender set in its `grpc-timeout` header: at most eight
/// digits and a unit of `H`, `M`, `S`, `m`, `u` or `n`, as tonic reads it.
#[must_use]
pub fn sender_deadline(metadata: &tonic::metadata::MetadataMap) -> Option<Duration> {
    let value = metadata.get("grpc-timeout")?.to_str().ok()?;
    let (digits, unit) = value.split_at(value.len().checked_sub(1)?);
    if digits.is_empty() || digits.len() > 8 {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n * 3_600),
        "M" => Duration::from_secs(n * 60),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    })
}

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

    #[test]
    fn the_hold_leaves_the_sender_a_margin() {
        assert_eq!(hold_budget(MAX_HOLD, None), MAX_HOLD);
        assert_eq!(
            hold_budget(MAX_HOLD, Some(Duration::from_secs(30))),
            MAX_HOLD,
            "a 30s request timeout holds for the full 25s"
        );
        assert_eq!(
            hold_budget(MAX_HOLD, Some(Duration::from_secs(10))),
            Duration::from_secs(9)
        );
        assert_eq!(
            hold_budget(MAX_HOLD, Some(Duration::from_secs(2))),
            Duration::from_secs(1),
            "never less than a second short of the deadline"
        );
        assert_eq!(
            hold_budget(MAX_HOLD, Some(Duration::from_millis(500))),
            Duration::from_millis(250),
            "and never more than half of it"
        );
    }

    /// The Kafka report and the next gRPC hop both settle inside the hold.
    #[test]
    fn downstream_deadlines_sit_inside_the_hold() {
        assert!(HELD_MESSAGE_TIMEOUT < MAX_HOLD);
        assert!(Duration::from_millis(NEXT_HOP_DEADLINE_MS) < MAX_HOLD);
    }

    #[test]
    fn a_grpc_timeout_header_is_read_as_tonic_reads_it() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        assert_eq!(sender_deadline(&metadata), None);
        metadata.insert("grpc-timeout", "20S".parse().unwrap());
        assert_eq!(sender_deadline(&metadata), Some(Duration::from_secs(20)));
        metadata.insert("grpc-timeout", "1500m".parse().unwrap());
        assert_eq!(
            sender_deadline(&metadata),
            Some(Duration::from_millis(1500))
        );
        metadata.insert("grpc-timeout", "123456789S".parse().unwrap());
        assert_eq!(sender_deadline(&metadata), None, "nine digits");
        metadata.insert("grpc-timeout", "20x".parse().unwrap());
        assert_eq!(sender_deadline(&metadata), None, "no such unit");
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
