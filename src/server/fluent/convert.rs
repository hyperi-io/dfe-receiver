// Project:   dfe-receiver
// File:      src/server/fluent/convert.rs
// Purpose:   Fluent Forward msgpack to JSON conversion
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Fluent Forward protocol message conversion.
//!
//! Converts msgpack-encoded Forward protocol messages to pipeline-ready JSON.
//! Supports Message, Forward, and PackedForward modes.

use bytes::Bytes;
use rmpv::Value;

use crate::error::{Error, Result};

/// Extract a unix timestamp (seconds) from a msgpack value.
///
/// Handles both integer timestamps and EventTime extension type 0
/// (8-byte payload: 4 bytes seconds + 4 bytes nanoseconds).
fn extract_timestamp(val: &Value) -> f64 {
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

/// Convert a msgpack Value to a serde_json Value.
fn msgpack_to_json(val: &Value) -> serde_json::Value {
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
        Value::String(s) => serde_json::Value::String(s.as_str().unwrap_or("").to_string()),
        Value::Binary(b) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(msgpack_to_json).collect()),
        Value::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| {
                    let key = match k {
                        Value::String(s) => s.as_str().unwrap_or("").to_string(),
                        _ => format!("{k}"),
                    };
                    (key, msgpack_to_json(v))
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
    let mut obj = match msgpack_to_json(record) {
        serde_json::Value::Object(map) => map,
        _ => {
            return Err(Error::Validation(
                "Fluent Forward record is not a map".into(),
            ));
        }
    };

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
        serde_json::Value::String("fluent".to_string()),
    );

    serde_json::to_vec(&obj)
        .map(Bytes::from)
        .map_err(|e| Error::Validation(format!("Fluent JSON serialisation failed: {e}")))
}

/// Convert a Fluent Forward protocol message to pipeline JSON payloads.
///
/// Returns one JSON payload per entry. Supports:
/// - **Message mode**: `[tag, time, record, option?]`
/// - **Forward mode**: `[tag, [[time, record], ...], option?]`
/// - **PackedForward mode**: `[tag, packed_msgpack_bytes, option?]`
pub fn fluent_to_json(msg: &Value) -> Result<Vec<Bytes>> {
    let arr = msg
        .as_array()
        .ok_or_else(|| Error::Validation("Fluent Forward message is not an array".into()))?;

    if arr.len() < 2 {
        return Err(Error::Validation(
            "Fluent Forward message has fewer than 2 elements".into(),
        ));
    }

    let tag = arr[0]
        .as_str()
        .ok_or_else(|| Error::Validation("Fluent Forward tag is not a string".into()))?;

    // Detect mode based on second element type
    match &arr[1] {
        // Forward mode: second element is an array of [time, record] entries
        Value::Array(entries) if !entries.is_empty() && entries[0].is_array() => {
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

        // PackedForward mode: second element is binary (packed msgpack entries)
        Value::Binary(packed) => {
            let mut payloads = Vec::new();
            let mut cursor = &packed[..];
            while !cursor.is_empty() {
                let entry = rmpv::decode::read_value(&mut cursor).map_err(|e| {
                    Error::Validation(format!("Fluent PackedForward decode failed: {e}"))
                })?;
                if let Some(entry_arr) = entry.as_array() && entry_arr.len() >= 2 {
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
                ));
            }
            let ts = extract_timestamp(&arr[1]);
            let payload = entry_to_json(tag, ts, &arr[2])?;
            Ok(vec![payload])
        }
    }
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
        let payloads = fluent_to_json(&msg).unwrap();
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
        let payloads = fluent_to_json(&msg).unwrap();
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
        let payloads = fluent_to_json(&msg).unwrap();
        assert_eq!(payloads.len(), 2);

        let j1: serde_json::Value = serde_json::from_slice(&payloads[0]).unwrap();
        let j2: serde_json::Value = serde_json::from_slice(&payloads[1]).unwrap();
        assert_eq!(j1["msg"], "packed1");
        assert_eq!(j2["msg"], "packed2");
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
        let payloads = fluent_to_json(&msg).unwrap();
        assert_eq!(payloads.len(), 1);

        let json: serde_json::Value = serde_json::from_slice(&payloads[0]).unwrap();
        let ts = json["timestamp"].as_f64().unwrap();
        assert!((ts - 1_700_000_000.5).abs() < 0.001);
    }

    #[test]
    fn test_source_always_set() {
        let msg = Value::Array(vec![
            Value::String("t".into()),
            Value::Integer(0.into()),
            make_record("k", "v"),
        ]);
        let payloads = fluent_to_json(&msg).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&payloads[0]).unwrap();
        assert_eq!(json["_source"], "fluent");
    }

    #[test]
    fn test_not_array() {
        let msg = Value::String("bad".into());
        assert!(fluent_to_json(&msg).is_err());
    }

    #[test]
    fn test_too_few_elements() {
        let msg = Value::Array(vec![Value::String("tag".into())]);
        assert!(fluent_to_json(&msg).is_err());
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
}
