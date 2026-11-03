// Project:   dfe-receiver
// File:      src/routing/original.rs
// Purpose:   Drop the copy of the raw line Logstash and Beats add beside `message`
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `event.original` and `log.original`.
//!
//! With ECS compatibility on, which is Logstash 8's default, a Beats input or
//! plain codec stores the raw line in `[event][original]` as well as in
//! `message`. Left in, every row carries the line twice. The copy goes when the
//! record keeps its `message` and the sender did not ask to keep it with the
//! `preserve_original_event` tag. A record with no `message` keeps it, as the
//! only copy. Raw capture never reaches here: it writes `_raw` on the syslog,
//! HEC `/raw` and flow records the receiver builds, none of which carry these
//! keys.

use bytes::Bytes;
use serde_json::{Map, Value};

/// The tag a sender sets to keep `event.original` and `log.original`.
pub const PRESERVE_TAG: &str = "preserve_original_event";

/// The objects that hold an `original` copy, and the key itself.
const PARENTS: [&str; 2] = ["event", "log"];
const KEY: &str = "original";

/// Whether `raw` spells a key `"original"` anywhere, from one pass with no parse.
/// A record that does not is returned without being parsed. An `original`
/// spelled with a JSON escape is not found, and its copy stays.
fn names_original(raw: &[u8]) -> bool {
    raw.windows(2)
        .enumerate()
        .any(|(at, pair)| pair == b"\"o" && raw[at + 1..].starts_with(b"original\""))
}

/// Whether the record asks to keep the copy.
fn preserves(record: &Map<String, Value>) -> bool {
    match record.get("tags") {
        Some(Value::Array(tags)) => tags.iter().any(|tag| tag.as_str() == Some(PRESERVE_TAG)),
        Some(Value::String(tag)) => tag == PRESERVE_TAG,
        _ => false,
    }
}

/// Whether the record keeps text in `message`, so the copy is a second one.
fn has_message(record: &Map<String, Value>) -> bool {
    record
        .get("message")
        .and_then(Value::as_str)
        .is_some_and(|message| !message.is_empty())
}

/// Remove `event.original` and `log.original` from a record that also carries
/// its `message` and no `preserve_original_event` tag.
///
/// A parent object left empty goes too. A record that holds neither key, the
/// common case, is returned untouched after one pass over its bytes, and so is
/// one the rules above keep or that does not parse.
#[must_use]
pub fn strip_original(payload: Bytes) -> Bytes {
    if !names_original(&payload) {
        return payload;
    }
    let Ok(mut value) = serde_json::from_slice::<Value>(&payload) else {
        return payload;
    };
    let Some(record) = value.as_object_mut() else {
        return payload;
    };
    if !has_message(record) || preserves(record) {
        return payload;
    }
    let mut stripped = false;
    for parent in PARENTS {
        let Some(Value::Object(fields)) = record.get_mut(parent) else {
            continue;
        };
        // Logstash sends the copy as a string or, from some pipelines, an
        // array of them. Only an object is a field of the sender's own.
        if matches!(fields.get(KEY), None | Some(Value::Object(_))) {
            continue;
        }
        fields.remove(KEY);
        stripped = true;
        if fields.is_empty() {
            record.remove(parent);
        }
    }
    if !stripped {
        return payload;
    }
    match serde_json::to_vec(&value) {
        Ok(rewritten) => {
            metrics::counter!("receiver_original_stripped_total").increment(1);
            Bytes::from(rewritten)
        }
        Err(_) => payload,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::metrics::testing::CounterTotals;

    fn stripped(record: &str) -> (Value, u64) {
        let totals = CounterTotals::default();
        let _recorder = metrics::set_default_local_recorder(&totals);
        let out = strip_original(Bytes::from(record.to_owned()));
        (
            serde_json::from_slice(&out).unwrap(),
            totals.total("receiver_original_stripped_total"),
        )
    }

    /// Each row is a record, the record it becomes, and whether it counts.
    #[test]
    fn the_copy_goes_only_beside_a_message_with_no_preserve_tag() {
        let cases = [
            (
                "event.original only, so the event object goes with it",
                r#"{"message":"a","event":{"original":"a"}}"#,
                r#"{"message":"a"}"#,
                1,
            ),
            (
                "event keeps its other fields",
                r#"{"message":"a","event":{"original":"a","dataset":"x"}}"#,
                r#"{"message":"a","event":{"dataset":"x"}}"#,
                1,
            ),
            (
                "log.original goes too, one record counted once",
                r#"{"message":"a","event":{"original":"a"},"log":{"original":"a","offset":3}}"#,
                r#"{"message":"a","log":{"offset":3}}"#,
                1,
            ),
            (
                "tags without the preserve tag",
                r#"{"message":"a","tags":["beats_input_codec_plain_applied"],"event":{"original":"a"}}"#,
                r#"{"message":"a","tags":["beats_input_codec_plain_applied"]}"#,
                1,
            ),
            (
                "the preserve tag keeps it",
                r#"{"message":"a","tags":["x","preserve_original_event"],"event":{"original":"a"}}"#,
                r#"{"message":"a","tags":["x","preserve_original_event"],"event":{"original":"a"}}"#,
                0,
            ),
            (
                "a lone preserve tag string keeps it",
                r#"{"message":"a","tags":"preserve_original_event","log":{"original":"a"}}"#,
                r#"{"message":"a","tags":"preserve_original_event","log":{"original":"a"}}"#,
                0,
            ),
            (
                "no message, so it is the only copy",
                r#"{"event":{"original":"a"},"log":{"original":"a"}}"#,
                r#"{"event":{"original":"a"},"log":{"original":"a"}}"#,
                0,
            ),
            (
                "an empty message is no message",
                r#"{"message":"","event":{"original":"a"}}"#,
                r#"{"message":"","event":{"original":"a"}}"#,
                0,
            ),
            (
                "a message that is not text is no message",
                r#"{"message":{"a":1},"event":{"original":"a"}}"#,
                r#"{"message":{"a":1},"event":{"original":"a"}}"#,
                0,
            ),
            (
                "an array of copies goes, as Logstash sends it",
                r#"{"message":"a","event":{"original":["a","a"],"dataset":"x"}}"#,
                r#"{"message":"a","event":{"dataset":"x"}}"#,
                1,
            ),
            (
                "a scalar copy goes",
                r#"{"message":"a","log":{"original":7}}"#,
                r#"{"message":"a"}"#,
                1,
            ),
            (
                "an original that is an object belongs to the sender",
                r#"{"message":"a","event":{"original":{"x":1}}}"#,
                r#"{"message":"a","event":{"original":{"x":1}}}"#,
                0,
            ),
            (
                "an original under another parent is left",
                r#"{"message":"a","other":{"original":"a"},"original":"a"}"#,
                r#"{"message":"a","other":{"original":"a"},"original":"a"}"#,
                0,
            ),
        ];
        for (name, record, want, count) in cases {
            let (got, counted) = stripped(record);
            assert_eq!(got, serde_json::from_str::<Value>(want).unwrap(), "{name}");
            assert_eq!(counted, count, "{name}");
        }
    }

    #[test]
    fn a_record_with_nothing_to_strip_is_returned_as_it_came() {
        for record in [
            &br#"{"message":"a",  "event":{"dataset":"x"}}"#[..],
            b"not json at all original",
            br#"["original"]"#,
            br#"{"message":"original","event":{"original":"a"#,
        ] {
            let payload = Bytes::copy_from_slice(record);
            assert_eq!(strip_original(payload.clone()), payload);
        }
    }
}
