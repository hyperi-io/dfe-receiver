// Project:   dfe-receiver
// File:      src/routing/header.rs
// Purpose:   Write a record's header fields back into its JSON bytes
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The header fields routing decided, written into the record in one pass.
//!
//! A record that lacks every field it needs takes them by a byte append before
//! its closing brace, with no parse. One that already carries a field holding
//! another value, or spells its key twice or with an escape, is rewritten
//! through a parse, because a second top-level key of the same name makes the
//! loader's ClickHouse JSON column reject the whole record.

use bytes::Bytes;
use scalo::rules::Lookup;
use sonic_rs::get_from_slice;

use super::{ORG_FIELD, RECEIVED_FIELD, SOURCE_FIELD};
use crate::error::{Error, Result};
use crate::validation::NOT_AN_OBJECT;

/// Room for the keys and punctuation an append adds, beside the values.
const APPEND_RESERVE: usize = 80;

/// What one header field needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write<'a> {
    /// The record holds this value already, or the field is not this pass's.
    Keep,
    /// The record lacks the field.
    Append(&'a str),
    /// The record holds another value, kept as `<field>_original` when `park`.
    Replace {
        /// The value the field takes.
        value: &'a str,
        /// Whether the value it held is kept beside it.
        park: bool,
    },
}

impl<'a> Write<'a> {
    /// The write that gives a field holding `found` the value `value`.
    ///
    /// `once` says the field's key is spelled once in the bytes and with no
    /// escape. Without it a record that already holds `value` is rewritten
    /// anyway, because a lookup reads only the first of two keys of one name.
    #[must_use]
    pub fn of(found: &Lookup<'_>, once: bool, value: &'a str, park: bool) -> Self {
        match found {
            Lookup::Missing => Self::Append(value),
            Lookup::Text(text) if once && text.as_ref() == value => Self::Keep,
            _ => Self::Replace { value, park },
        }
    }

    fn appended(self) -> Option<&'a str> {
        match self {
            Self::Append(value) => Some(value),
            Self::Keep | Self::Replace { .. } => None,
        }
    }
}

/// What `_timestamp_received` needs, in epoch milliseconds.
///
/// The receiver is the first hop, so it writes its own receive time on every
/// record and keeps one the sender wrote as `_timestamp_received_original`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Received {
    /// Not this pass's field.
    Skip,
    /// The record carries none.
    Append(u128),
    /// The record carries one, which is kept beside this.
    Replace(u128),
    /// As `Replace`, for a record that spells the key once with no escape and
    /// holds no `_timestamp_received_original`: the value at byte span `at`
    /// is swapped in the bytes with no parse.
    Swap {
        /// The receive time written.
        ms: u128,
        /// Where the sender's value sits in the record.
        at: (usize, usize),
    },
}

impl Received {
    /// The write that stamps `now_ms` on a record that `holds` one or not.
    #[must_use]
    pub fn of(holds: bool, now_ms: u128) -> Self {
        if holds {
            Self::Replace(now_ms)
        } else {
            Self::Append(now_ms)
        }
    }

    fn ms(self) -> Option<u128> {
        match self {
            Self::Skip => None,
            Self::Append(ms) | Self::Replace(ms) | Self::Swap { ms, .. } => Some(ms),
        }
    }
}

/// Where a sender's `_timestamp_received` is kept.
pub const RECEIVED_ORIGINAL: &str = "_timestamp_received_original";

/// The header fields one record needs written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header<'a> {
    /// `_timestamp_received`.
    pub received: Received,
    /// `_source`.
    pub source: Write<'a>,
    /// `_org_id`.
    pub org: Write<'a>,
}

impl Header<'_> {
    /// `payload` with these fields written. A record that needs nothing comes
    /// back as it was, uncopied.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when a field must be replaced and the record does
    /// not parse as a JSON object.
    pub fn write(&self, payload: Bytes) -> Result<Bytes> {
        let replaces = |write: Write<'_>| matches!(write, Write::Replace { .. });
        if replaces(self.source)
            || replaces(self.org)
            || matches!(self.received, Received::Replace(_))
        {
            return rewrite(&payload, self);
        }
        if let Received::Swap { ms, at } = self.received {
            return swap(&payload, self, ms, at);
        }
        Ok(append(payload, self))
    }
}

/// Where the top-level `field`'s value sits in `raw`, `None` when it has none.
#[must_use]
pub(super) fn value_span(raw: &[u8], field: &str) -> Option<(usize, usize)> {
    let value = get_from_slice(raw, [field].as_slice()).ok()?;
    let part = value.as_raw_str();
    let start = (part.as_ptr() as usize).checked_sub(raw.as_ptr() as usize)?;
    let end = start.checked_add(part.len())?;
    (raw.get(start..end) == Some(part.as_bytes())).then_some((start, end))
}

/// Swap the value at `at` for `ms` in the record's bytes, keep the value it
/// held as `_timestamp_received_original`, and append the rest. Falls back to
/// the parse when `at` is not a value inside the record's braces.
fn swap(raw: &[u8], header: &Header<'_>, ms: u128, (start, end): (usize, usize)) -> Result<Bytes> {
    let close = raw.iter().rposition(|&b| b == b'}');
    let (Some(close), Some(held)) = (close, raw.get(start..end)) else {
        return rewrite(raw, header);
    };
    if start == 0 || end > close {
        return rewrite(raw, header);
    }
    let mut own = Vec::with_capacity(39);
    push_decimal(&mut own, ms);
    let mut out = Appender {
        buf: Vec::with_capacity(raw.len() + held.len() + APPEND_RESERVE * 2),
        comma: true,
    };
    out.buf.extend_from_slice(&raw[..start]);
    out.buf.extend_from_slice(&own);
    out.buf.extend_from_slice(&raw[end..close]);
    if held != own.as_slice() {
        out.key(RECEIVED_ORIGINAL);
        out.buf.extend_from_slice(held);
    }
    if let Some(source) = header.source.appended() {
        out.key(SOURCE_FIELD);
        push_json_string(&mut out.buf, source);
    }
    if let Some(org) = header.org.appended() {
        out.key(ORG_FIELD);
        push_json_string(&mut out.buf, org);
    }
    out.buf.extend_from_slice(&raw[close..]);
    Ok(Bytes::from(out.buf))
}

/// Append every field `header` adds before the record's last `}`.
fn append(payload: Bytes, header: &Header<'_>) -> Bytes {
    let source = header.source.appended();
    let org = header.org.appended();
    let received = match header.received {
        Received::Append(ms) => Some(ms),
        Received::Skip | Received::Replace(_) | Received::Swap { .. } => None,
    };
    if received.is_none() && source.is_none() && org.is_none() {
        return payload;
    }
    let raw = payload.as_ref();
    let Some(close) = raw.iter().rposition(|&b| b == b'}') else {
        return payload;
    };
    let values = source.map_or(0, str::len) + org.map_or(0, str::len);
    let mut out = Appender {
        buf: Vec::with_capacity(raw.len() + values + APPEND_RESERVE),
        // No comma after `{`, and trailing whitespace before the brace is not content.
        comma: raw[..close]
            .iter()
            .rposition(|b| !b.is_ascii_whitespace())
            .is_some_and(|pos| raw[pos] != b'{'),
    };
    out.buf.extend_from_slice(&raw[..close]);
    if let Some(ms) = received {
        out.key(RECEIVED_FIELD);
        push_decimal(&mut out.buf, ms);
    }
    if let Some(source) = source {
        out.key(SOURCE_FIELD);
        push_json_string(&mut out.buf, source);
    }
    if let Some(org) = org {
        out.key(ORG_FIELD);
        push_json_string(&mut out.buf, org);
    }
    out.buf.extend_from_slice(&raw[close..]);
    Bytes::from(out.buf)
}

/// A record being appended to, and whether the next key needs a comma.
struct Appender {
    buf: Vec<u8>,
    comma: bool,
}

impl Appender {
    fn key(&mut self, name: &str) {
        if self.comma {
            self.buf.push(b',');
        }
        self.comma = true;
        self.buf.push(b'"');
        self.buf.extend_from_slice(name.as_bytes());
        self.buf.extend_from_slice(b"\":");
    }
}

/// Rewrite the record through a parse, for a field it holds with another value.
fn rewrite(payload: &[u8], header: &Header<'_>) -> Result<Bytes> {
    let mut value: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|e| Error::Validation(format!("record header cannot be rewritten: {e}")))?;
    let Some(map) = value.as_object_mut() else {
        return Err(Error::Validation(NOT_AN_OBJECT.into()));
    };
    if let Some(ms) = header.received.ms() {
        let own = serde_json::Value::from(u64::try_from(ms).unwrap_or(u64::MAX));
        let held = map.insert(RECEIVED_FIELD.to_string(), own.clone());
        if let Some(held) = held
            && held != own
        {
            park_original(map, RECEIVED_FIELD, held);
        }
    }
    apply(map, SOURCE_FIELD, header.source);
    apply(map, ORG_FIELD, header.org);
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|e| Error::Validation(format!("record header cannot be rewritten: {e}")))
}

fn apply(map: &mut serde_json::Map<String, serde_json::Value>, field: &str, write: Write<'_>) {
    match write {
        Write::Keep => {}
        Write::Append(value) => {
            map.insert(field.to_string(), serde_json::Value::from(value));
        }
        Write::Replace { value, park } => {
            let held = map.insert(field.to_string(), serde_json::Value::from(value));
            // A second copy of the same value is collapsed, not parked.
            if park
                && let Some(held) = held
                && held.as_str() != Some(value)
            {
                park_original(map, field, held);
            }
        }
    }
}

/// Keep a replaced value as `<field>_original`, or the first free
/// `<field>_original_<n>` from 2 when the record already carries that name.
fn park_original(
    map: &mut serde_json::Map<String, serde_json::Value>,
    field: &str,
    held: serde_json::Value,
) {
    let preferred = format!("{field}_original");
    if !map.contains_key(&preferred) {
        map.insert(preferred, held);
        return;
    }
    let mut n = 2u32;
    loop {
        let numbered = format!("{preferred}_{n}");
        if !map.contains_key(&numbered) {
            map.insert(numbered, held);
            return;
        }
        n += 1;
    }
}

/// Append `n` in decimal, with no allocation.
fn push_decimal(buf: &mut Vec<u8>, mut n: u128) {
    let mut digits = [0u8; 39];
    let mut at = digits.len();
    loop {
        at -= 1;
        digits[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    buf.extend_from_slice(&digits[at..]);
}

/// Append `s` as a quoted JSON string literal.
///
/// The common value needs no escape and is written straight through. A quote,
/// backslash or control byte would break the record, so those go through serde.
fn push_json_string(buf: &mut Vec<u8>, s: &str) {
    if s.bytes().any(|b| b == b'"' || b == b'\\' || b < 0x20) {
        let escaped = serde_json::Value::String(s.to_owned()).to_string();
        buf.extend_from_slice(escaped.as_bytes());
        return;
    }
    buf.push(b'"');
    buf.extend_from_slice(s.as_bytes());
    buf.push(b'"');
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;

    fn header(source: Write<'static>, org: Write<'static>) -> Header<'static> {
        Header {
            received: Received::Append(1700),
            source,
            org,
        }
    }

    fn written(record: &'static str, header: &Header<'_>) -> String {
        String::from_utf8(header.write(Bytes::from(record)).unwrap().to_vec()).unwrap()
    }

    #[test]
    fn a_record_lacking_every_field_takes_them_by_append() {
        let h = header(Write::Append("web"), Write::Append("acme"));
        assert_eq!(
            written(r#"{"a":1}"#, &h),
            r#"{"a":1,"_timestamp_received":1700,"_source":"web","_org_id":"acme"}"#
        );
        assert_eq!(
            written("{ }", &h),
            r#"{ "_timestamp_received":1700,"_source":"web","_org_id":"acme"}"#
        );
    }

    #[test]
    fn an_empty_object_takes_no_leading_comma() {
        let h = header(Write::Keep, Write::Append("default"));
        assert_eq!(
            written("{}", &h),
            r#"{"_timestamp_received":1700,"_org_id":"default"}"#
        );
    }

    #[test]
    fn a_record_that_needs_nothing_is_not_copied() {
        let h = Header {
            received: Received::Skip,
            source: Write::Keep,
            org: Write::Keep,
        };
        let payload = Bytes::from(r#"{"a":1}"#);
        let out = h.write(payload.clone()).unwrap();
        assert_eq!(out.as_ptr(), payload.as_ptr(), "the record was copied");
    }

    #[test]
    fn a_replaced_source_is_parked_and_a_replaced_org_is_not() {
        let h = header(
            Write::Replace {
                value: "web",
                park: true,
            },
            Write::Replace {
                value: "acme",
                park: false,
            },
        );
        let out = written(r#"{"_source":"spoof","_org_id":"victim","a":1}"#, &h);
        let value: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(value["_source"], "web");
        assert_eq!(value["_source_original"], "spoof");
        assert_eq!(value["_org_id"], "acme");
        assert!(value.get("_org_id_original").is_none());
        assert_eq!(value["_timestamp_received"], 1700);
        assert_eq!(out.matches("\"_source\"").count(), 1, "{out}");
        assert_eq!(out.matches("\"_org_id\"").count(), 1, "{out}");
    }

    /// A sender's receive time, of any JSON type, is kept beside the receiver's
    /// own, by a parse or by a swap in the bytes alike.
    #[test]
    fn a_sender_receive_time_is_parked_beside_the_receivers_own() {
        for swaps in [false, true] {
            for (record, held) in [
                (r#"{"_timestamp_received":5,"a":1}"#, serde_json::json!(5)),
                (
                    r#"{"a":1, "_timestamp_received" : "soon" }"#,
                    serde_json::json!("soon"),
                ),
                (
                    r#"{"_timestamp_received":{"t":[1,2]}}"#,
                    serde_json::json!({"t": [1, 2]}),
                ),
            ] {
                let received = if swaps {
                    swap_at(record, 1700)
                } else {
                    Received::Replace(1700)
                };
                let h = Header {
                    received,
                    source: Write::Keep,
                    org: Write::Append("acme"),
                };
                let out = written(record, &h);
                let value: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(value["_timestamp_received"], 1700, "{received:?}: {out}");
                assert_eq!(value[RECEIVED_ORIGINAL], held, "{received:?}: {out}");
                assert_eq!(value["_org_id"], "acme", "{received:?}: {out}");
                assert_eq!(out.matches("\"_timestamp_received\"").count(), 1, "{out}");
            }
        }
        assert_eq!(Received::of(true, 9), Received::Replace(9));
        assert_eq!(Received::of(false, 9), Received::Append(9));
        assert_eq!(RECEIVED_ORIGINAL, format!("{RECEIVED_FIELD}_original"));
    }

    /// A swap leaves every byte around the value it replaces as it was, and
    /// parks nothing when the sender's value is the receiver's own.
    #[test]
    fn a_swap_leaves_every_other_byte_as_it_was() {
        let swapped = |record: &'static str| {
            let h = Header {
                received: swap_at(record, 1700),
                source: Write::Keep,
                org: Write::Keep,
            };
            written(record, &h)
        };
        assert_eq!(
            swapped(r#"{"b": 2, "_timestamp_received":5 ,"a":[1]}"#),
            r#"{"b": 2, "_timestamp_received":1700 ,"a":[1],"_timestamp_received_original":5}"#
        );
        assert_eq!(
            swapped(r#"{"_timestamp_received":1700}"#),
            r#"{"_timestamp_received":1700}"#
        );
    }

    /// A span that is not a value inside the braces falls back to the parse.
    #[test]
    fn a_swap_at_no_value_falls_back_to_the_parse() {
        let record = r#"{"_timestamp_received":5}"#;
        for at in [(0, 1), (24, 25), (23, 99), (99, 100)] {
            let h = Header {
                received: Received::Swap { ms: 1700, at },
                source: Write::Keep,
                org: Write::Keep,
            };
            let value: serde_json::Value = serde_json::from_str(&written(record, &h)).unwrap();
            assert_eq!(value["_timestamp_received"], 1700, "{at:?}");
            assert_eq!(value[RECEIVED_ORIGINAL], 5, "{at:?}");
        }
        assert_eq!(value_span(br#"{"a":1}"#, RECEIVED_FIELD), None);
    }

    /// The swap of `record`'s own `_timestamp_received` for `ms`.
    fn swap_at(record: &str, ms: u128) -> Received {
        let at = value_span(record.as_bytes(), RECEIVED_FIELD).expect("a receive time");
        Received::Swap { ms, at }
    }

    #[test]
    fn a_park_never_overwrites_an_earlier_original() {
        let h = header(
            Write::Replace {
                value: "web",
                park: true,
            },
            Write::Keep,
        );
        let out = written(
            r#"{"_source":"spoof","_source_original":"first","_source_original_2":"second"}"#,
            &h,
        );
        let value: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(value["_source_original"], "first");
        assert_eq!(value["_source_original_2"], "second");
        assert_eq!(value["_source_original_3"], "spoof");
    }

    #[test]
    fn a_value_with_quotes_is_escaped() {
        let h = header(Write::Append(r#"we"ird\name"#), Write::Keep);
        let value: serde_json::Value = serde_json::from_str(&written(r#"{"a":1}"#, &h)).unwrap();
        assert_eq!(value["_source"], r#"we"ird\name"#);
    }

    #[test]
    fn a_payload_with_no_closing_brace_is_left_alone() {
        let h = header(Write::Append("web"), Write::Append("acme"));
        assert_eq!(written("not json", &h), "not json");
    }

    #[test]
    fn a_rewrite_of_a_record_that_is_not_an_object_is_refused() {
        let h = header(
            Write::Replace {
                value: "web",
                park: true,
            },
            Write::Keep,
        );
        assert!(h.write(Bytes::from("[1}")).is_err());
    }

    #[test]
    fn the_write_follows_what_the_record_holds() {
        let replace = Write::Replace {
            value: "a",
            park: true,
        };
        assert_eq!(
            Write::of(&Lookup::Missing, true, "a", true),
            Write::Append("a")
        );
        assert_eq!(
            Write::of(&Lookup::Text(Cow::Borrowed("a")), true, "a", true),
            Write::Keep
        );
        assert_eq!(
            Write::of(&Lookup::Text(Cow::Borrowed("a")), false, "a", true),
            replace,
            "a key spelled twice or with an escape is rewritten even when the first copy matches"
        );
        for held in [
            Lookup::Text(Cow::Borrowed("b")),
            Lookup::Null,
            Lookup::Composite,
        ] {
            assert_eq!(Write::of(&held, true, "a", true), replace, "{held:?}");
        }
    }

    #[test]
    fn a_duplicate_key_collapses_to_one_and_an_equal_copy_is_not_parked() {
        let h = Header {
            received: Received::Skip,
            source: Write::Replace {
                value: "web",
                park: true,
            },
            org: Write::Replace {
                value: "acme",
                park: false,
            },
        };
        let out = written(
            r#"{"_source":"web","_source":"web","_org_id":"acme","_org_id":"beta"}"#,
            &h,
        );
        assert_eq!(out.matches("\"_source\"").count(), 1, "{out}");
        assert_eq!(out.matches("\"_org_id\"").count(), 1, "{out}");
        let value: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(value["_org_id"], "acme");
        assert!(value.get("_source_original").is_none(), "{out}");
    }

    #[test]
    fn the_received_time_is_written_in_decimal() {
        let mut buf = Vec::new();
        push_decimal(&mut buf, 0);
        buf.push(b' ');
        push_decimal(&mut buf, 1_760_000_000_123);
        assert_eq!(buf, b"0 1760000000123");
    }
}
