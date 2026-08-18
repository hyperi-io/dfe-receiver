// Project:   dfe-receiver
// File:      src/server/raw_capture.rs
// Purpose:   Write the captured original payload into the `_raw` field
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `_raw` field writer, shared by every capturing transport.
//!
//! Two entry points, matching the two shapes the transports already build:
//!
//! - [`attach_to_map`] for the handlers that assemble a `serde_json::Map`
//!   (syslog, GELF, fluent, Splunk HEC, OTLP, Prometheus RW).
//! - [`append_to_json_buf`] for the byte-buffer writers (flow), which append
//!   `,"_raw":"..."` just before the closing brace.
//!
//! Both go through [`prepare`], so truncation, invalid UTF-8 and the omit
//! policy behave identically everywhere.
//!
//! Two markers travel with the value, and both exist so a reader can never
//! mistake a mangled capture for a faithful one:
//!
//! - `_raw_truncated: true` -- the payload exceeded `max_bytes` and the tail
//!   was dropped.
//! - `_raw_lossy: true` -- the payload was not valid UTF-8 and invalid
//!   sequences became U+FFFD. `_raw` is a JSON string and then a ClickHouse
//!   text column, so arbitrary bytes cannot survive verbatim; saying so is
//!   better than a silently altered payload.

use std::borrow::Cow;

use crate::config::raw_capture::{OversizePolicy, RawCapture};

/// JSON field holding the captured payload. Matches the dfe-schemas common
/// header column, so dfe-loader's own `_raw` rename sees it already present
/// and leaves it alone.
pub const RAW_FIELD: &str = "_raw";

/// Set when the tail was dropped to respect `max_bytes`.
pub const RAW_TRUNCATED_FIELD: &str = "_raw_truncated";

/// Set when invalid UTF-8 was replaced with U+FFFD.
pub const RAW_LOSSY_FIELD: &str = "_raw_lossy";

/// A payload ready to be written into `_raw`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedRaw {
    /// The capture, already truncated and UTF-8 clean.
    pub text: String,
    /// The tail was dropped.
    pub truncated: bool,
    /// Invalid UTF-8 was replaced.
    pub lossy: bool,
}

/// Largest byte length not exceeding `limit` that lands on a char boundary.
///
/// Truncating mid-codepoint would panic on `String::truncate` and would emit
/// broken UTF-8 anyway, so a multi-byte character straddling the cap is
/// dropped whole.
fn floor_char_boundary(s: &str, limit: usize) -> usize {
    if limit >= s.len() {
        return s.len();
    }
    let mut end = limit;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    end
}

/// Turn raw wire bytes into the value for `_raw`.
///
/// Returns `None` when capture is disabled, when the payload is empty, or
/// when it is oversized under [`OversizePolicy::Omit`].
pub fn prepare(raw: &[u8], cfg: RawCapture) -> Option<PreparedRaw> {
    if !cfg.enabled || raw.is_empty() {
        return None;
    }

    // Cow::Owned is exactly the "a replacement happened" signal.
    let cow = String::from_utf8_lossy(raw);
    let lossy = matches!(cow, Cow::Owned(_));
    let mut text = cow.into_owned();

    // max_bytes is a cap on the CAPTURED text. Lossy replacement can grow the
    // byte length (a stray 0x80 becomes a 3-byte U+FFFD), so measure after
    // the conversion rather than on the input.
    let mut truncated = false;
    if cfg.max_bytes > 0 && text.len() > cfg.max_bytes {
        match cfg.on_oversize {
            OversizePolicy::Omit => return None,
            OversizePolicy::Truncate => {
                let end = floor_char_boundary(&text, cfg.max_bytes);
                text.truncate(end);
                truncated = true;
            }
        }
    }

    Some(PreparedRaw {
        text,
        truncated,
        lossy,
    })
}

/// Same as [`prepare`] for a payload that is already a `str`.
pub fn prepare_str(raw: &str, cfg: RawCapture) -> Option<PreparedRaw> {
    if !cfg.enabled || raw.is_empty() {
        return None;
    }

    let mut truncated = false;
    let text = if cfg.max_bytes > 0 && raw.len() > cfg.max_bytes {
        match cfg.on_oversize {
            OversizePolicy::Omit => return None,
            OversizePolicy::Truncate => {
                truncated = true;
                raw[..floor_char_boundary(raw, cfg.max_bytes)].to_string()
            }
        }
    } else {
        raw.to_string()
    };

    Some(PreparedRaw {
        text,
        truncated,
        lossy: false,
    })
}

/// [`prepare`] for a value that must be serialised to JSON first.
///
/// The transports whose wire form is protobuf or msgpack cannot put the bytes
/// themselves in a text field, so `_raw` carries their least-shaped JSON
/// decode instead. Deferred rather than attached, because those call sites
/// snapshot the decode before the surrounding envelope consumes it.
pub fn prepare_serialised<T: serde::Serialize + ?Sized>(
    source: &T,
    cfg: RawCapture,
) -> Result<Option<PreparedRaw>, serde_json::Error> {
    if !cfg.enabled {
        return Ok(None);
    }
    let verbatim = serde_json::to_vec(source)?;
    Ok(prepare(&verbatim, cfg))
}

/// Write `_raw` (and its markers) into a JSON object under construction.
///
/// A no-op when capture is off or the payload is omitted. An `_raw` the
/// source itself supplied is left alone -- the event's own field wins, the
/// same precedence dfe-loader applies.
pub fn attach_to_map(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    raw: &[u8],
    cfg: RawCapture,
) {
    if let Some(prepared) = prepare(raw, cfg) {
        attach_prepared_to_map(obj, prepared);
    }
}

/// [`attach_to_map`] for a payload that is already a `str`.
pub fn attach_str_to_map(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    raw: &str,
    cfg: RawCapture,
) {
    if let Some(prepared) = prepare_str(raw, cfg) {
        attach_prepared_to_map(obj, prepared);
    }
}

/// Write an already-prepared capture into a JSON object.
///
/// Used directly by the transports whose "raw" is a re-serialised decode
/// (OTLP, Prometheus RW, fluent) rather than wire bytes.
pub fn attach_prepared_to_map(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    prepared: PreparedRaw,
) {
    if obj.contains_key(RAW_FIELD) {
        return;
    }
    if prepared.truncated {
        obj.insert(
            RAW_TRUNCATED_FIELD.to_string(),
            serde_json::Value::Bool(true),
        );
    }
    if prepared.lossy {
        obj.insert(RAW_LOSSY_FIELD.to_string(), serde_json::Value::Bool(true));
    }
    obj.insert(
        RAW_FIELD.to_string(),
        serde_json::Value::String(prepared.text),
    );
}

/// Append `,"_raw":"..."` to a JSON object being written into a byte buffer.
///
/// The caller must have written the object's fields but NOT the closing
/// brace. A no-op when capture is off or the payload is omitted.
pub fn append_to_json_buf(buf: &mut Vec<u8>, raw: &[u8], cfg: RawCapture) {
    let Some(prepared) = prepare(raw, cfg) else {
        return;
    };
    if prepared.truncated {
        buf.extend_from_slice(br#","_raw_truncated":true"#);
    }
    if prepared.lossy {
        buf.extend_from_slice(br#","_raw_lossy":true"#);
    }
    buf.extend_from_slice(br#","_raw":"#);
    write_json_string(buf, &prepared.text);
}

/// Write `s` as a JSON string literal (quotes included) into `buf`.
fn write_json_string(buf: &mut Vec<u8>, s: &str) {
    // serde_json owns the escaping rules; going through it keeps this
    // identical to every other string the receiver emits.
    match serde_json::to_writer(&mut *buf, s) {
        Ok(()) => {}
        Err(_) => {
            // to_writer over a Vec only fails on a serialisation error, which
            // a &str cannot produce. Emit an empty string rather than leave
            // the buffer holding a half-written object.
            buf.extend_from_slice(b"\"\"");
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn capped(max_bytes: usize, on_oversize: OversizePolicy) -> RawCapture {
        RawCapture {
            enabled: true,
            max_bytes,
            on_oversize,
        }
    }

    #[test]
    fn disabled_captures_nothing() {
        assert!(prepare(b"hello", RawCapture::OFF).is_none());
        let mut obj = serde_json::Map::new();
        attach_to_map(&mut obj, b"hello", RawCapture::OFF);
        assert!(obj.is_empty());
    }

    #[test]
    fn empty_payload_captures_nothing() {
        assert!(prepare(b"", RawCapture::on()).is_none());
        assert!(prepare_str("", RawCapture::on()).is_none());
    }

    #[test]
    fn clean_payload_round_trips_verbatim() {
        let prepared = prepare(b"<34>Oct 11 22:14:15 host su: failed", RawCapture::on()).unwrap();
        assert_eq!(prepared.text, "<34>Oct 11 22:14:15 host su: failed");
        assert!(!prepared.truncated);
        assert!(!prepared.lossy);
    }

    #[test]
    fn oversize_truncates_and_flags() {
        let prepared = prepare(b"abcdefghij", capped(4, OversizePolicy::Truncate)).unwrap();
        assert_eq!(prepared.text, "abcd");
        assert!(prepared.truncated);
    }

    #[test]
    fn oversize_omits_entirely_under_omit_policy() {
        assert!(prepare(b"abcdefghij", capped(4, OversizePolicy::Omit)).is_none());
    }

    #[test]
    fn zero_max_bytes_means_unlimited() {
        let long = "x".repeat(10_000);
        let prepared = prepare(long.as_bytes(), capped(0, OversizePolicy::Truncate)).unwrap();
        assert_eq!(prepared.text.len(), 10_000);
        assert!(!prepared.truncated);
    }

    #[test]
    fn truncation_never_splits_a_codepoint() {
        // 4 x 3-byte chars; a 4-byte cap must drop back to one whole char.
        let prepared = prepare(
            "\u{4f60}\u{597d}\u{4e16}\u{754c}".as_bytes(),
            capped(4, OversizePolicy::Truncate),
        )
        .unwrap();
        assert_eq!(prepared.text, "\u{4f60}");
        assert!(prepared.truncated);
        // The real assertion: it is still valid UTF-8 and did not panic.
        assert_eq!(prepared.text.len(), 3);
    }

    #[test]
    fn invalid_utf8_is_replaced_and_flagged() {
        // Latin-1 high byte, which syslog senders emit routinely.
        let prepared = prepare(b"caf\xe9 log", RawCapture::on()).unwrap();
        assert!(prepared.lossy);
        assert!(prepared.text.contains('\u{fffd}'));
        assert!(!prepared.truncated);
    }

    #[test]
    fn lossy_growth_is_measured_after_replacement() {
        // Three invalid bytes become three 3-byte U+FFFD = 9 bytes, over a
        // cap of 4. Measuring the input (3 bytes) would have missed it.
        let prepared = prepare(b"\xe9\xe9\xe9", capped(4, OversizePolicy::Truncate)).unwrap();
        assert!(prepared.lossy);
        assert!(prepared.truncated);
        assert_eq!(prepared.text, "\u{fffd}");
    }

    #[test]
    fn markers_only_appear_when_they_apply() {
        let mut obj = serde_json::Map::new();
        attach_to_map(&mut obj, b"clean", RawCapture::on());
        assert_eq!(obj[RAW_FIELD], "clean");
        assert!(!obj.contains_key(RAW_TRUNCATED_FIELD));
        assert!(!obj.contains_key(RAW_LOSSY_FIELD));
    }

    #[test]
    fn both_markers_can_apply_at_once() {
        let mut obj = serde_json::Map::new();
        attach_to_map(
            &mut obj,
            b"\xe9\xe9\xe9",
            capped(4, OversizePolicy::Truncate),
        );
        assert_eq!(obj[RAW_TRUNCATED_FIELD], true);
        assert_eq!(obj[RAW_LOSSY_FIELD], true);
    }

    #[test]
    fn a_source_supplied_raw_is_not_overwritten() {
        let mut obj = serde_json::Map::new();
        obj.insert(RAW_FIELD.to_string(), serde_json::json!("from the source"));
        attach_to_map(&mut obj, b"from the wire", RawCapture::on());
        assert_eq!(obj[RAW_FIELD], "from the source");
    }

    #[test]
    fn prepare_str_truncates_on_a_char_boundary_too() {
        let prepared =
            prepare_str("\u{4f60}\u{597d}", capped(4, OversizePolicy::Truncate)).unwrap();
        assert_eq!(prepared.text, "\u{4f60}");
        assert!(prepared.truncated);
        assert!(!prepared.lossy);
    }

    #[test]
    fn buf_append_produces_parseable_json() {
        let mut buf = Vec::from(br#"{"message":"hi""#.as_slice());
        append_to_json_buf(&mut buf, b"raw \"quoted\" line\n", RawCapture::on());
        buf.push(b'}');

        let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(parsed["message"], "hi");
        assert_eq!(parsed[RAW_FIELD], "raw \"quoted\" line\n");
    }

    #[test]
    fn buf_append_is_a_no_op_when_disabled() {
        let mut buf = Vec::from(br#"{"a":1"#.as_slice());
        append_to_json_buf(&mut buf, b"payload", RawCapture::OFF);
        buf.push(b'}');
        assert_eq!(buf, br#"{"a":1}"#);
    }

    #[test]
    fn buf_append_escapes_control_characters() {
        let mut buf = Vec::from(br#"{"a":1"#.as_slice());
        append_to_json_buf(&mut buf, b"tab\there\x01", RawCapture::on());
        buf.push(b'}');
        let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(parsed[RAW_FIELD], "tab\there\u{1}");
    }

    #[test]
    fn buf_append_writes_markers_before_the_value() {
        let mut buf = Vec::from(br#"{"a":1"#.as_slice());
        append_to_json_buf(&mut buf, b"abcdefghij", capped(4, OversizePolicy::Truncate));
        buf.push(b'}');
        let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(parsed[RAW_TRUNCATED_FIELD], true);
        assert_eq!(parsed[RAW_FIELD], "abcd");
    }
}
