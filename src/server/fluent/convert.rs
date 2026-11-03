// Project:   dfe-receiver
// File:      src/server/fluent/convert.rs
// Purpose:   Fluent Forward msgpack to JSON conversion
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Fluent Forward protocol message conversion.
//!
//! Converts msgpack-encoded Forward protocol messages to pipeline-ready JSON.
//! Supports Message, Forward, PackedForward and gzip CompressedPackedForward modes.

use std::borrow::Cow;
use std::io::Read;

use bytes::Bytes;
use rmpv::Value;

use crate::error::{Error, Result};
use crate::server::budget::{MAX_COPIED_BYTES, MAX_RECORDS};
use crate::server::fluent::scan::{Scan, Scanner};

/// The `_source` this listener stamps on every record it builds.
pub const SOURCE: &str = "fluent";

/// Extract a unix timestamp (seconds) from a msgpack value.
///
/// Handles both integer timestamps and EventTime extension type 0
/// (8-byte payload: 4 bytes seconds + 4 bytes nanoseconds). A Forward v1 entry
/// that carries metadata, as Fluent Bit sends it, holds the time as the first
/// element of `[time, metadata]`.
fn extract_timestamp(val: &Value) -> f64 {
    match val {
        Value::Array(parts) if parts.len() == 2 => extract_scalar_timestamp(&parts[0]),
        _ => extract_scalar_timestamp(val),
    }
}

fn extract_scalar_timestamp(val: &Value) -> f64 {
    match val {
        Value::Integer(i) => i.as_f64().unwrap_or(0.0),
        Value::F32(f) => f64::from(*f),
        Value::F64(f) => *f,
        Value::Ext(0, data) if data.len() == 8 => {
            let secs = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            let nanos = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
            f64::from(secs) + f64::from(nanos) / 1_000_000_000.0
        }
        _ => 0.0,
    }
}

/// Why a message did not convert, which decides whether the listener
/// acknowledges the chunk.
#[derive(Debug, thiserror::Error)]
pub enum Refused {
    /// Valid data past a limit. A resend of the same chunk meets the same
    /// limit, so the chunk is not acknowledged: the sender sees the failure and
    /// can send it smaller.
    #[error("{0}")]
    TooBig(String),
    /// Not a Forward message. It fails the same way every time it is sent, so
    /// the chunk is acknowledged.
    #[error(transparent)]
    Malformed(#[from] Error),
}

/// A msgpack string as text. Bytes that are not UTF-8 become U+FFFD and set `lossy`.
fn text<'a>(s: &'a rmpv::Utf8String, lossy: &mut bool) -> Cow<'a, str> {
    s.as_str().map_or_else(
        || {
            *lossy = true;
            String::from_utf8_lossy(s.as_bytes())
        },
        Cow::Borrowed,
    )
}

/// Convert a msgpack Value to a serde_json Value. `lossy` is set when a string
/// in it was not UTF-8.
fn msgpack_to_json(val: &Value, lossy: &mut bool) -> serde_json::Value {
    match val {
        Value::Nil => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => {
            if let Some(u) = i.as_u64() {
                serde_json::Value::Number(u.into())
            } else if let Some(s) = i.as_i64() {
                serde_json::Value::Number(s.into())
            } else {
                serde_json::Value::Number(
                    serde_json::Number::from_f64(i.as_f64().unwrap_or(0.0))
                        .unwrap_or_else(|| 0.into()),
                )
            }
        }
        Value::F32(f) => serde_json::Number::from_f64(f64::from(*f))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::F64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(text(s, lossy).into_owned()),
        Value::Binary(b) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
        Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(|v| msgpack_to_json(v, lossy)).collect())
        }
        Value::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| {
                    let key = match k {
                        Value::String(s) => text(s, lossy).into_owned(),
                        _ => format!("{k}"),
                    };
                    (key, msgpack_to_json(v, lossy))
                })
                .collect();
            serde_json::Value::Object(obj)
        }
        Value::Ext(_, data) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            data,
        )),
    }
}

/// Convert a single entry (timestamp + record) to pipeline JSON.
fn entry_to_json(tag: &str, timestamp: f64, record: &Value) -> Result<Bytes> {
    let mut lossy = false;
    let mut obj = match msgpack_to_json(record, &mut lossy) {
        serde_json::Value::Object(map) => map,
        _ => {
            return Err(Error::Validation(
                "Fluent Forward record is not a map".into(),
            ));
        }
    };

    if lossy {
        crate::metrics::count_lossy_decode("fluent");
    }

    obj.insert(
        "tag".to_string(),
        serde_json::Value::String(tag.to_string()),
    );

    if timestamp > 0.0 {
        obj.insert(
            "timestamp".to_string(),
            serde_json::Number::from_f64(timestamp)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
        );
    }

    // Source tag for routing
    obj.insert(
        "_source".to_string(),
        serde_json::Value::String(SOURCE.to_string()),
    );

    serde_json::to_vec(&obj)
        .map(Bytes::from)
        .map_err(|e| Error::Validation(format!("Fluent JSON serialisation failed: {e}")))
}

/// How a PackedForward blob is compressed, from the `compressed` option.
#[derive(Debug, PartialEq, Eq)]
enum Compression {
    /// No option, or `text`.
    None,
    Gzip,
    /// Any other codec, named as the sender wrote it.
    Other(String),
}

/// The `compressed` option of a PackedForward message `[tag, blob, options?]`.
fn compression(arr: &[Value]) -> Compression {
    let Some(Value::Map(options)) = arr.get(2) else {
        return Compression::None;
    };
    let Some((_, codec)) = options
        .iter()
        .find(|(k, _)| k.as_str() == Some("compressed"))
    else {
        return Compression::None;
    };
    match codec.as_str() {
        Some("text") => Compression::None,
        Some("gzip") => Compression::Gzip,
        // The name is the sender's, so it is cut short before it reaches a log line.
        Some(other) => Compression::Other(other.chars().take(32).collect()),
        None => Compression::Other(format!("{codec}").chars().take(32).collect()),
    }
}

/// The codec of a PackedForward message compressed with one the receiver cannot
/// read, so the listener can refuse the chunk instead of acknowledging it.
pub fn unsupported_compression(msg: &Value) -> Option<String> {
    let arr = msg.as_array()?;
    if !matches!(arr.get(1), Some(Value::Binary(_) | Value::String(_))) {
        return None;
    }
    match compression(arr) {
        Compression::Other(codec) => Some(codec),
        Compression::None | Compression::Gzip => None,
    }
}

/// Inflate a gzip PackedForward blob, which may hold several concatenated
/// gzip members, refusing one that inflates past `limit` bytes.
fn inflate(blob: &[u8], limit: usize) -> std::result::Result<Vec<u8>, Refused> {
    let mut packed = Vec::new();
    flate2::read::MultiGzDecoder::new(blob)
        .take(limit as u64 + 1)
        .read_to_end(&mut packed)
        .map_err(|e| Error::Validation(format!("Fluent PackedForward gzip is not valid: {e}")))?;
    if packed.len() > limit {
        return Err(Refused::TooBig(format!(
            "Fluent PackedForward entries are past {limit} bytes once decompressed"
        )));
    }
    Ok(packed)
}

/// The packed entries of a PackedForward message's second element. Fluent Bit
/// sends them as MessagePack bin, Fluentd's out_forward as a str that is not
/// valid UTF-8, so both are the bytes of the blob.
fn blob_bytes(value: &Value) -> &[u8] {
    match value {
        Value::Binary(bytes) => bytes,
        Value::String(s) => s.as_bytes(),
        _ => &[],
    }
}

/// Convert a Fluent Forward protocol message to pipeline JSON payloads.
///
/// Returns one JSON payload per entry. Supports:
/// - **Message mode**: `[tag, time, record, option?]`
/// - **Forward mode**: `[tag, [[time, record], ...], option?]`
/// - **PackedForward mode**: `[tag, packed_msgpack_bytes, option?]`
/// - **CompressedPackedForward mode**: PackedForward with the option
///   `compressed: "gzip"`, whose blob inflates to at most `max_decompressed`
///   bytes and then meets the same entry budget as an uncompressed one.
///
/// # Errors
///
/// [`Refused::TooBig`] for valid entries past the entry, tag-copy or
/// decompressed-size budget, and [`Refused::Malformed`] for anything that is
/// not a Forward message.
pub fn fluent_to_json(
    msg: &Value,
    max_decompressed: usize,
) -> std::result::Result<Vec<Bytes>, Refused> {
    let arr = msg
        .as_array()
        .ok_or_else(|| Error::Validation("Fluent Forward message is not an array".into()))?;

    if arr.len() < 2 {
        return Err(
            Error::Validation("Fluent Forward message has fewer than 2 elements".into()).into(),
        );
    }

    let Value::String(tag) = &arr[0] else {
        return Err(Error::Validation("Fluent Forward tag is not a string".into()).into());
    };
    let mut lossy_tag = false;
    let tag = text(tag, &mut lossy_tag);
    if lossy_tag {
        crate::metrics::count_lossy_decode("fluent");
    }
    let tag = &*tag;

    // Detect mode based on second element type
    match &arr[1] {
        // Forward mode: second element is an array of [time, record] entries, none at all
        // being a message of no records, not a Message-mode record.
        Value::Array(entries) if entries.is_empty() || entries[0].is_array() => {
            within_budget(tag, entries.len())?;
            let mut payloads = Vec::with_capacity(entries.len());
            for entry in entries {
                let entry_arr = entry.as_array().ok_or_else(|| {
                    Error::Validation("Fluent Forward entry is not an array".into())
                })?;
                if entry_arr.len() < 2 {
                    continue;
                }
                let ts = extract_timestamp(&entry_arr[0]);
                payloads.push(entry_to_json(tag, ts, &entry_arr[1])?);
            }
            Ok(payloads)
        }

        // PackedForward mode: second element is the packed msgpack entries, bin or str
        blob @ (Value::Binary(_) | Value::String(_)) => {
            let blob = blob_bytes(blob);
            let packed = match compression(arr) {
                Compression::None => Cow::Borrowed(blob),
                Compression::Gzip => Cow::Owned(inflate(blob, max_decompressed)?),
                Compression::Other(codec) => {
                    return Err(Error::Validation(format!(
                        "Fluent PackedForward compressed with {codec:?} is not supported"
                    ))
                    .into());
                }
            };
            within_budget(tag, packed_entries(&packed)?)?;
            let mut payloads = Vec::new();
            let mut cursor = &packed[..];
            while !cursor.is_empty() {
                let entry = rmpv::decode::read_value(&mut cursor).map_err(|e| {
                    Error::Validation(format!("Fluent PackedForward decode failed: {e}"))
                })?;
                if let Some(entry_arr) = entry.as_array()
                    && entry_arr.len() >= 2
                {
                    let ts = extract_timestamp(&entry_arr[0]);
                    payloads.push(entry_to_json(tag, ts, &entry_arr[1])?);
                }
            }
            Ok(payloads)
        }

        // Message mode: [tag, time, record, option?]
        _ => {
            if arr.len() < 3 {
                return Err(Error::Validation(
                    "Fluent Message mode requires at least 3 elements".into(),
                )
                .into());
            }
            let ts = extract_timestamp(&arr[1]);
            let payload = entry_to_json(tag, ts, &arr[2])?;
            Ok(vec![payload])
        }
    }
}

/// Refuse a message of `entries` entries past the decode budget: too many
/// records, or a tag copied into each that adds up past the shared-byte cap.
fn within_budget(tag: &str, entries: usize) -> std::result::Result<(), Refused> {
    if entries > MAX_RECORDS {
        return Err(Refused::TooBig(format!(
            "Fluent Forward message carries {entries} entries, past {MAX_RECORDS}"
        )));
    }
    if tag.len().saturating_mul(entries) > MAX_COPIED_BYTES {
        return Err(Refused::TooBig(format!(
            "Fluent Forward tag of {} bytes copied into {entries} entries is past {MAX_COPIED_BYTES} bytes",
            tag.len()
        )));
    }
    Ok(())
}

/// The entries a PackedForward blob holds, counted by the scanner so a blob
/// past the value budget is refused before it decodes.
fn packed_entries(packed: &[u8]) -> std::result::Result<usize, Refused> {
    Scanner::count(packed).map_err(|stopped| match stopped {
        Scan::OverBudget => {
            Refused::TooBig("Fluent PackedForward entries are past the value budget".into())
        }
        other => Refused::Malformed(Error::Validation(format!(
            "Fluent PackedForward entries refused: {other:?}"
        ))),
    })
}

/// Extract the chunk ID from the options map (for ACK response).
pub fn extract_chunk_id(msg: &Value) -> Option<String> {
    let arr = msg.as_array()?;
    let options = arr.last()?;
    let map = options.as_map()?;
    for (k, v) in map {
        if k.as_str() == Some("chunk") {
            return v.as_str().map(String::from);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A decompressed-size limit no test message reaches.
    const TEST_LIMIT: usize = 1 << 20;

    fn make_record(key: &str, val: &str) -> Value {
        Value::Map(vec![(Value::String(key.into()), Value::String(val.into()))])
    }

    #[test]
    fn test_message_mode() {
        let msg = Value::Array(vec![
            Value::String("app.log".into()),
            Value::Integer(1_700_000_000.into()),
            make_record("message", "hello world"),
        ]);
        let payloads = fluent_to_json(&msg, TEST_LIMIT).unwrap();
        assert_eq!(payloads.len(), 1);

        let json: serde_json::Value = serde_json::from_slice(&payloads[0]).unwrap();
        assert_eq!(json["tag"], "app.log");
        assert_eq!(json["message"], "hello world");
        assert_eq!(json["_source"], "fluent");
        assert!(json["timestamp"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn test_forward_mode() {
        let msg = Value::Array(vec![
            Value::String("app.log".into()),
            Value::Array(vec![
                Value::Array(vec![
                    Value::Integer(1_700_000_000.into()),
                    make_record("msg", "one"),
                ]),
                Value::Array(vec![
                    Value::Integer(1_700_000_001.into()),
                    make_record("msg", "two"),
                ]),
            ]),
        ]);
        let payloads = fluent_to_json(&msg, TEST_LIMIT).unwrap();
        assert_eq!(payloads.len(), 2);

        let j1: serde_json::Value = serde_json::from_slice(&payloads[0]).unwrap();
        let j2: serde_json::Value = serde_json::from_slice(&payloads[1]).unwrap();
        assert_eq!(j1["msg"], "one");
        assert_eq!(j2["msg"], "two");
        assert_eq!(j1["tag"], "app.log");
    }

    #[test]
    fn test_packed_forward_mode() {
        // Encode two entries as packed msgpack
        let entry1 = Value::Array(vec![
            Value::Integer(1_700_000_000.into()),
            make_record("msg", "packed1"),
        ]);
        let entry2 = Value::Array(vec![
            Value::Integer(1_700_000_001.into()),
            make_record("msg", "packed2"),
        ]);
        let mut packed = Vec::new();
        rmpv::encode::write_value(&mut packed, &entry1).unwrap();
        rmpv::encode::write_value(&mut packed, &entry2).unwrap();

        let msg = Value::Array(vec![Value::String("app.log".into()), Value::Binary(packed)]);
        let payloads = fluent_to_json(&msg, TEST_LIMIT).unwrap();
        assert_eq!(payloads.len(), 2);

        let j1: serde_json::Value = serde_json::from_slice(&payloads[0]).unwrap();
        let j2: serde_json::Value = serde_json::from_slice(&payloads[1]).unwrap();
        assert_eq!(j1["msg"], "packed1");
        assert_eq!(j2["msg"], "packed2");
    }

    fn packed_entries_of(messages: &[&str]) -> Vec<u8> {
        let mut packed = Vec::new();
        for (i, message) in messages.iter().enumerate() {
            let entry = Value::Array(vec![
                Value::Integer((1_700_000_000 + i as i64).into()),
                make_record("msg", message),
            ]);
            rmpv::encode::write_value(&mut packed, &entry).unwrap();
        }
        packed
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        use std::io::Write as _;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn with_options(blob: Vec<u8>, codec: Value) -> Value {
        Value::Array(vec![
            Value::String("app.log".into()),
            Value::Binary(blob),
            Value::Map(vec![(Value::String("compressed".into()), codec)]),
        ])
    }

    /// A PackedForward blob compressed with gzip lands every entry, whether it
    /// is one gzip member or several concatenated.
    #[test]
    fn a_gzip_packed_forward_blob_lands_every_entry() {
        let one = gzip(&packed_entries_of(&["a", "b", "c"]));
        let several = [
            gzip(&packed_entries_of(&["a"])),
            gzip(&packed_entries_of(&["b", "c"])),
        ]
        .concat();
        for (name, blob) in [("one member", one), ("several members", several)] {
            let msg = with_options(blob, Value::String("gzip".into()));
            let payloads = fluent_to_json(&msg, TEST_LIMIT).unwrap();
            let messages: Vec<String> = payloads
                .iter()
                .map(|p| {
                    let json: serde_json::Value = serde_json::from_slice(p).unwrap();
                    json["msg"].as_str().unwrap().to_string()
                })
                .collect();
            assert_eq!(messages, ["a", "b", "c"], "{name}");
        }
    }

    /// A blob that inflates past the limit is refused whatever it compresses
    /// to, and a blob that is not gzip is refused, never read as msgpack.
    #[test]
    fn a_gzip_blob_is_held_to_its_decompressed_size() {
        let packed = packed_entries_of(&["x"; 64]);
        let msg = with_options(gzip(&packed), Value::String("gzip".into()));
        assert_eq!(fluent_to_json(&msg, packed.len()).unwrap().len(), 64);
        assert!(matches!(
            fluent_to_json(&msg, packed.len() - 1),
            Err(Refused::TooBig(_))
        ));

        let not_gzip = with_options(packed, Value::String("gzip".into()));
        assert!(matches!(
            fluent_to_json(&not_gzip, TEST_LIMIT),
            Err(Refused::Malformed(_))
        ));
    }

    /// Only `gzip` and `text` are read. Another codec is named so the listener
    /// can refuse the chunk, and a record that happens to carry a `compressed`
    /// key is not mistaken for the option.
    #[test]
    fn a_codec_other_than_gzip_is_unsupported() {
        let packed = packed_entries_of(&["a"]);
        for (codec, want) in [
            (Value::String("gzip".into()), None),
            (Value::String("text".into()), None),
            (Value::String("zstd".into()), Some("zstd")),
            (Value::Integer(1.into()), Some("1")),
        ] {
            let msg = with_options(packed.clone(), codec.clone());
            assert_eq!(unsupported_compression(&msg).as_deref(), want, "{codec}");
            if want.is_some() {
                assert!(fluent_to_json(&msg, TEST_LIMIT).is_err(), "{codec}");
            }
        }
        let text = with_options(packed, Value::String("text".into()));
        assert_eq!(fluent_to_json(&text, TEST_LIMIT).unwrap().len(), 1);

        let message_mode = Value::Array(vec![
            Value::String("t".into()),
            Value::Integer(0.into()),
            Value::Map(vec![(
                Value::String("compressed".into()),
                Value::String("zstd".into()),
            )]),
        ]);
        assert_eq!(unsupported_compression(&message_mode), None);
        assert_eq!(fluent_to_json(&message_mode, TEST_LIMIT).unwrap().len(), 1);
    }

    /// The wire bytes of `[tag, <blob as str32>, options?]`: Fluentd's out_forward
    /// sends the packed entries as a MessagePack string, not as bin.
    fn str32_message(blob: &[u8], options: Option<Value>) -> Value {
        let mut wire = vec![if options.is_some() { 0x93 } else { 0x92 }, 0xa7];
        wire.extend_from_slice(b"app.log");
        wire.push(0xdb);
        wire.extend_from_slice(&u32::try_from(blob.len()).unwrap().to_be_bytes());
        wire.extend_from_slice(blob);
        if let Some(options) = options {
            rmpv::encode::write_value(&mut wire, &options).unwrap();
        }
        rmpv::decode::read_value(&mut &wire[..]).unwrap()
    }

    fn messages(payloads: &[Bytes]) -> Vec<String> {
        payloads
            .iter()
            .map(|p| {
                let json: serde_json::Value = serde_json::from_slice(p).unwrap();
                json["msg"].as_str().unwrap_or("<none>").to_string()
            })
            .collect()
    }

    /// Fluentd sends the packed entries as a str32, plain or gzipped, and every
    /// entry lands as it does from a bin.
    #[test]
    fn packed_entries_sent_as_a_msgpack_string_land_every_entry() {
        let packed = packed_entries_of(&["a", "b"]);
        let gzipped = gzip(&packed);
        let compressed = Value::Map(vec![(
            Value::String("compressed".into()),
            Value::String("gzip".into()),
        )]);
        for (name, msg) in [
            ("plain", str32_message(&packed, None)),
            ("gzip", str32_message(&gzipped, Some(compressed))),
        ] {
            assert!(matches!(msg[1], Value::String(_)), "{name}: not a string");
            let payloads = fluent_to_json(&msg, TEST_LIMIT).unwrap();
            assert_eq!(messages(&payloads), ["a", "b"], "{name}");
        }
    }

    /// A Forward message with no entries holds no records, and the options
    /// map after it is not one.
    #[test]
    fn a_forward_message_with_no_entries_holds_no_records() {
        let options = Value::Map(vec![(
            Value::String("chunk".into()),
            Value::String("c".into()),
        )]);
        let msg = Value::Array(vec![
            Value::String("app.log".into()),
            Value::Array(vec![]),
            options,
        ]);
        assert_eq!(fluent_to_json(&msg, TEST_LIMIT).unwrap().len(), 0);
    }

    /// Valid data past a limit is `TooBig`, which the listener does not
    /// acknowledge, and what is not Forward at all is `Malformed`, which it does.
    #[test]
    fn a_refusal_says_whether_the_data_was_valid_but_too_big() {
        let entries = |n: usize| {
            Value::Array(vec![
                Value::String("t".into()),
                Value::Array(vec![
                    Value::Array(vec![
                        Value::Integer(1.into()),
                        Value::Map(vec![])
                    ]);
                    n
                ]),
            ])
        };
        assert_eq!(
            fluent_to_json(&entries(MAX_RECORDS), TEST_LIMIT)
                .unwrap()
                .len(),
            MAX_RECORDS
        );
        let packed = [0x92, 0x00, 0x80].repeat(MAX_RECORDS + 1);
        let long_tag = Value::Array(vec![
            Value::String("t".repeat(MAX_COPIED_BYTES / 2).into()),
            Value::Binary([0x92, 0x00, 0x80].repeat(3)),
        ]);
        let too_big = [
            ("forward entries", entries(MAX_RECORDS + 1)),
            (
                "packed entries",
                Value::Array(vec![Value::String("t".into()), Value::Binary(packed)]),
            ),
            ("tag copies", long_tag),
        ];
        for (name, msg) in too_big {
            assert!(
                matches!(fluent_to_json(&msg, TEST_LIMIT), Err(Refused::TooBig(_))),
                "{name}"
            );
        }

        let malformed = [
            ("not an array", Value::String("bad".into())),
            (
                "no entries array or bytes",
                Value::Array(vec![Value::String("t".into())]),
            ),
            (
                "entries that are not msgpack",
                Value::Array(vec![Value::String("t".into()), Value::Binary(vec![0xc1])]),
            ),
        ];
        for (name, msg) in malformed {
            assert!(
                matches!(fluent_to_json(&msg, TEST_LIMIT), Err(Refused::Malformed(_))),
                "{name}"
            );
        }
    }

    /// A string that is not UTF-8 keeps its other bytes, in a key or a value.
    #[test]
    fn a_string_that_is_not_utf8_is_kept_with_replacement_characters() {
        let totals = crate::metrics::testing::CounterTotals::default();
        let _recorder = metrics::set_default_local_recorder(&totals);
        // fixstr of two bytes: 0x61 'a', 0xff
        let bad = |wire: &[u8]| rmpv::decode::read_value(&mut &wire[..]).unwrap();
        let record = Value::Map(vec![
            (bad(&[0xa2, b'k', 0xff]), bad(&[0xa2, b'v', 0xe9])),
            (Value::String("msg".into()), bad(&[0xa3, b'c', b'a', 0xe9])),
        ]);
        let msg = Value::Array(vec![
            bad(&[0xa3, b'a', b'p', 0xe9]),
            Value::Integer(1_700_000_000.into()),
            record,
        ]);
        let payloads = fluent_to_json(&msg, TEST_LIMIT).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&payloads[0]).unwrap();
        assert_eq!(json["k\u{fffd}"], "v\u{fffd}");
        assert_eq!(json["msg"], "ca\u{fffd}");
        assert_eq!(json["tag"], "ap\u{fffd}");
        // The tag once, and the record once however many of its strings were bad.
        assert_eq!(totals.total("receiver_lossy_decodes_total"), 2);
    }

    #[test]
    fn test_eventtime_extension() {
        // EventTime: ext type 0, 8 bytes (4 secs + 4 nanos)
        let secs: u32 = 1_700_000_000;
        let nanos: u32 = 500_000_000;
        let mut data = Vec::with_capacity(8);
        data.extend_from_slice(&secs.to_be_bytes());
        data.extend_from_slice(&nanos.to_be_bytes());

        let msg = Value::Array(vec![
            Value::String("app.log".into()),
            Value::Ext(0, data),
            make_record("msg", "with eventtime"),
        ]);
        let payloads = fluent_to_json(&msg, TEST_LIMIT).unwrap();
        assert_eq!(payloads.len(), 1);

        let json: serde_json::Value = serde_json::from_slice(&payloads[0]).unwrap();
        let ts = json["timestamp"].as_f64().unwrap();
        assert!((ts - 1_700_000_000.5).abs() < 0.001);
    }

    #[test]
    fn test_eventtime_with_metadata_entry() {
        // Fluent Bit 5.x: each Forward entry is `[[EventTime, metadata], record]`.
        let mut data = Vec::with_capacity(8);
        data.extend_from_slice(&1_700_000_000_u32.to_be_bytes());
        data.extend_from_slice(&0_u32.to_be_bytes());
        let entry = Value::Array(vec![
            Value::Array(vec![Value::Ext(0, data), Value::Map(vec![])]),
            make_record("msg", "with metadata"),
        ]);
        let msg = Value::Array(vec![
            Value::String("app.log".into()),
            Value::Array(vec![entry]),
        ]);
        let payloads = fluent_to_json(&msg, TEST_LIMIT).unwrap();

        let json: serde_json::Value = serde_json::from_slice(&payloads[0]).unwrap();
        assert!((json["timestamp"].as_f64().unwrap() - 1_700_000_000.0).abs() < 0.001);
    }

    #[test]
    fn test_source_always_set() {
        let msg = Value::Array(vec![
            Value::String("t".into()),
            Value::Integer(0.into()),
            make_record("k", "v"),
        ]);
        let payloads = fluent_to_json(&msg, TEST_LIMIT).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&payloads[0]).unwrap();
        assert_eq!(json["_source"], "fluent");
    }

    #[test]
    fn test_not_array() {
        let msg = Value::String("bad".into());
        assert!(fluent_to_json(&msg, TEST_LIMIT).is_err());
    }

    #[test]
    fn test_too_few_elements() {
        let msg = Value::Array(vec![Value::String("tag".into())]);
        assert!(fluent_to_json(&msg, TEST_LIMIT).is_err());
    }

    #[test]
    fn test_extract_chunk_id() {
        let msg = Value::Array(vec![
            Value::String("tag".into()),
            Value::Integer(0.into()),
            make_record("k", "v"),
            Value::Map(vec![(
                Value::String("chunk".into()),
                Value::String("abc123".into()),
            )]),
        ]);
        assert_eq!(extract_chunk_id(&msg), Some("abc123".to_string()));
    }

    #[test]
    fn test_extract_chunk_id_missing() {
        let msg = Value::Array(vec![
            Value::String("tag".into()),
            Value::Integer(0.into()),
            make_record("k", "v"),
        ]);
        assert_eq!(extract_chunk_id(&msg), None);
    }

    // -----------------------------------------------------------------------
    // No `_raw`
    // -----------------------------------------------------------------------

    /// The record is msgpack on the wire and JSON in the event. The loader
    /// keeps the whole JSON record in `_json`, so a second copy in `_raw` would
    /// be stored and text-indexed twice.
    #[test]
    fn the_event_carries_no_raw_field() {
        let msg = Value::Array(vec![
            Value::String("app.log".into()),
            Value::Integer(1_700_000_000.into()),
            make_record("message", "hello"),
        ]);
        let payloads = fluent_to_json(&msg, TEST_LIMIT).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&payloads[0]).unwrap();
        assert!(json.get("_raw").is_none());
        assert_eq!(json["message"], "hello");
        assert_eq!(json["tag"], "app.log");
        assert_eq!(json["_source"], "fluent");
    }
}
