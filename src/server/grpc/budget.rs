// Project:   dfe-receiver
// File:      src/server/grpc/budget.rs
// Purpose:   The decode budget of a Vector push, before it is decoded
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The decode budget of a Vector `PushEvents` request, as
//! [`crate::server::budget`] walks it. Each event is a record, and every
//! repeated message an event can hold counts against the message budget.

use super::pb;
use crate::server::budget::{Budgeted, Shape};

static EMPTY: Shape = Shape::of(&[]);
static VALUE: Shape = Shape::of(&[(7, &VALUE_MAP), (8, &VALUE_ARRAY)]);
static VALUE_ENTRY: Shape = Shape::of(&[(2, &VALUE)]);
static VALUE_MAP: Shape = Shape::of(&[(1, &VALUE_ENTRY)]);
static VALUE_ARRAY: Shape = Shape::of(&[(1, &VALUE)]);
static SECRETS: Shape = Shape::of(&[(1, &EMPTY)]);
static METADATA: Shape = Shape::of(&[(1, &VALUE), (6, &SECRETS)]);
static LOG: Shape = Shape::of(&[(1, &VALUE_ENTRY), (2, &VALUE), (3, &VALUE), (4, &METADATA)]);
static TRACE: Shape = Shape::of(&[(1, &VALUE_ENTRY), (2, &VALUE), (3, &METADATA)]);
static TAG_VALUES: Shape = Shape::of(&[(1, &EMPTY)]);
static TAGS_V2_ENTRY: Shape = Shape::of(&[(2, &TAG_VALUES)]);
// Set values, distribution samples, histogram buckets and summary quantiles.
static ITEMS: Shape = Shape::of(&[(1, &EMPTY)]);
static METRIC: Shape = Shape::of(&[
    (3, &EMPTY),
    (7, &ITEMS),
    (12, &ITEMS),
    (13, &ITEMS),
    (14, &ITEMS),
    (16, &ITEMS),
    (17, &ITEMS),
    (19, &VALUE),
    (20, &TAGS_V2_ENTRY),
    (21, &METADATA),
]);
static EVENT: Shape = Shape::record(&[(1, &LOG), (2, &METRIC), (3, &TRACE)]);
static PUSH: Shape = Shape::of(&[(1, &EVENT)]);

impl Budgeted for pb::vector::PushEventsRequest {
    const WHAT: &'static str = "Vector push";

    fn shape() -> &'static Shape {
        &PUSH
    }
}

impl Budgeted for pb::vector::PushEventsResponse {
    const WHAT: &'static str = "Vector push response";

    fn shape() -> &'static Shape {
        &EMPTY
    }
}

impl Budgeted for pb::vector::HealthCheckRequest {
    const WHAT: &'static str = "Vector health check";

    fn shape() -> &'static Shape {
        &EMPTY
    }
}

impl Budgeted for pb::vector::HealthCheckResponse {
    const WHAT: &'static str = "Vector health check response";

    fn shape() -> &'static Shape {
        &EMPTY
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::server::budget::{MAX_MESSAGES, MAX_RECORDS, admit};
    use pb::event::{EventWrapper, Log, Value, ValueArray, event_wrapper, value};

    fn push(events: Vec<EventWrapper>) -> Vec<u8> {
        pb::vector::PushEventsRequest { events }.encode_to_vec()
    }

    /// A push past the record or message budget is refused, and a real one is admitted.
    #[test]
    fn a_push_over_its_decode_budget_is_refused() {
        let log = |value: Option<Value>| EventWrapper {
            event: Some(event_wrapper::Event::Log(Log {
                value,
                ..Default::default()
            })),
        };
        let array = Value {
            kind: Some(value::Kind::Array(ValueArray {
                items: vec![Value::default(); MAX_MESSAGES],
            })),
        };
        // The schema deprecates field 19 and prost still decodes it, so it is written on the wire.
        let mut metric = Vec::new();
        prost::encoding::message::encode(19, &array, &mut metric);
        let mut event = Vec::new();
        prost::encoding::bytes::encode(2, &metric, &mut event);
        let mut metadata_push = Vec::new();
        prost::encoding::bytes::encode(1, &event, &mut metadata_push);
        for (name, body, refused) in [
            ("a real push", push(vec![log(None); 100]), None),
            (
                "events past the cap",
                push(vec![EventWrapper::default(); MAX_RECORDS + 1]),
                Some("records"),
            ),
            (
                "one event, values past the cap",
                push(vec![log(Some(array))]),
                Some("messages"),
            ),
            (
                "one metric, deprecated metadata past the cap",
                metadata_push,
                Some("messages"),
            ),
        ] {
            match (admit(&body, &PUSH, "Vector push"), refused) {
                (Ok(()), None) => {}
                (Err(e), Some(text)) => assert!(e.to_string().contains(text), "{name}: {e}"),
                (got, want) => panic!("{name}: got {got:?}, want refusal {want:?}"),
            }
        }
    }
}
