// Project:   dfe-receiver
// File:      src/server/splunk_hec/convert.rs
// Purpose:   Splunk HEC event parsing and conversion to pipeline JSON
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Splunk HEC event format conversion.
//!
//! Parses HEC JSON events (NDJSON format) and converts them to pipeline-ready
//! JSON payloads. Handles both `/services/collector/event` (structured) and
//! `/services/collector/raw` (plain text) formats.

use bytes::Bytes;
use serde::Deserialize;

use crate::config::RawCapture;
use crate::error::{Error, Result};
use crate::server::raw_capture;

/// A single Splunk HEC event as sent to `/services/collector/event`.
///
/// The body may contain multiple of these concatenated (NDJSON).
#[derive(Debug, Deserialize)]
pub struct HecEvent {
    /// Event payload — can be a JSON object, string, or number.
    pub event: serde_json::Value,

    /// Epoch timestamp with optional millisecond precision (e.g. 1447828325.123).
    #[serde(default)]
    pub time: Option<f64>,

    /// Hostname of the event source.
    #[serde(default)]
    pub host: Option<String>,

    /// Data source identifier.
    #[serde(default)]
    pub source: Option<String>,

    /// Event classification for parsing rules.
    #[serde(default)]
    pub sourcetype: Option<String>,

    /// Target index name.
    #[serde(default)]
    pub index: Option<String>,

    /// Extra indexed fields (flat key-value map).
    #[serde(default)]
    pub fields: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Parse an HEC request body into individual events.
///
/// The body is NDJSON (concatenated JSON objects, not a JSON array).
/// Uses `serde_json::StreamDeserializer` which handles objects with or
/// without newline separators.
pub fn parse_hec_events(body: &[u8]) -> Result<Vec<HecEvent>> {
    let stream = serde_json::Deserializer::from_slice(body).into_iter::<HecEvent>();

    let mut events = Vec::new();
    for result in stream {
        let event = result.map_err(|e| Error::Validation(format!("HEC event parse error: {e}")))?;

        // The `event` field is required and must not be null
        if event.event.is_null() {
            return Err(Error::Validation("event field cannot be blank".to_string()));
        }

        events.push(event);
    }

    if events.is_empty() {
        return Err(Error::Validation("no data".to_string()));
    }

    Ok(events)
}

/// Convert a single HEC event to pipeline-ready JSON bytes.
///
/// If the event payload is a JSON object, metadata fields are injected into it.
/// If it's a string or number, it's wrapped as `{"message": <value>, ...metadata}`.
///
/// With capture enabled, `_raw` holds the submitted `event` value as the
/// sender wrote it, before the HEC metadata below is merged in.
pub fn hec_event_to_json(event: HecEvent, raw_capture: RawCapture) -> Result<Bytes> {
    let prepared = raw_capture::prepare_serialised(&event.event, raw_capture)
        .map_err(|e| Error::Server(format!("HEC raw capture failed: {e}")))?;

    let mut obj = match event.event {
        serde_json::Value::Object(map) => map,
        serde_json::Value::String(s) => {
            let mut map = serde_json::Map::new();
            map.insert("message".into(), serde_json::Value::String(s));
            map
        }
        serde_json::Value::Number(n) => {
            let mut map = serde_json::Map::new();
            map.insert("message".into(), serde_json::Value::Number(n));
            map
        }
        other => {
            let mut map = serde_json::Map::new();
            map.insert("message".into(), other);
            map
        }
    };

    // Inject HEC metadata
    if let Some(time) = event.time {
        obj.entry("_time").or_insert(serde_json::json!(time));
    }
    if let Some(host) = event.host {
        obj.entry("host").or_insert(serde_json::Value::String(host));
    }
    if let Some(source) = event.source {
        obj.entry("source")
            .or_insert(serde_json::Value::String(source));
    }
    if let Some(sourcetype) = event.sourcetype {
        obj.entry("sourcetype")
            .or_insert(serde_json::Value::String(sourcetype));
    }
    if let Some(index) = event.index {
        obj.entry("index")
            .or_insert(serde_json::Value::String(index));
    }

    // Merge extra fields (don't overwrite existing keys)
    if let Some(fields) = event.fields {
        for (k, v) in fields {
            obj.entry(k).or_insert(v);
        }
    }

    if let Some(prepared) = prepared {
        raw_capture::attach_prepared_to_map(&mut obj, prepared);
    }

    let json =
        serde_json::to_vec(&obj).map_err(|e| Error::Server(format!("JSON serialize: {e}")))?;

    Ok(Bytes::from(json))
}

/// Metadata extracted from query parameters or headers for raw events.
#[derive(Debug, Default)]
pub struct RawMetadata {
    pub host: Option<String>,
    pub source: Option<String>,
    pub sourcetype: Option<String>,
    pub index: Option<String>,
}

/// Convert a raw text line to pipeline-ready JSON bytes.
///
/// With capture enabled, `_raw` holds the line's bytes. `message` already
/// carries the same text, but only `_raw` states when the bytes were not
/// valid UTF-8 or exceeded the cap.
pub fn raw_to_json(line: &[u8], metadata: &RawMetadata, raw_capture: RawCapture) -> Result<Bytes> {
    let message = String::from_utf8_lossy(line);

    let mut obj = serde_json::Map::new();
    obj.insert(
        "message".into(),
        serde_json::Value::String(message.into_owned()),
    );

    if let Some(ref host) = metadata.host {
        obj.insert("host".into(), serde_json::Value::String(host.clone()));
    }
    if let Some(ref source) = metadata.source {
        obj.insert("source".into(), serde_json::Value::String(source.clone()));
    }
    if let Some(ref sourcetype) = metadata.sourcetype {
        obj.insert(
            "sourcetype".into(),
            serde_json::Value::String(sourcetype.clone()),
        );
    }
    if let Some(ref index) = metadata.index {
        obj.insert("index".into(), serde_json::Value::String(index.clone()));
    }

    raw_capture::attach_to_map(&mut obj, line, raw_capture);

    let json =
        serde_json::to_vec(&obj).map_err(|e| Error::Server(format!("JSON serialize: {e}")))?;
    Ok(Bytes::from(json))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_event() {
        let body = br#"{"event":"hello world"}"#;
        let events = parse_hec_events(body).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, serde_json::json!("hello world"));
    }

    #[test]
    fn test_parse_ndjson_batch() {
        let body = br#"{"event":"one"}
{"event":"two"}
{"event":"three"}"#;
        let events = parse_hec_events(body).unwrap();
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn test_parse_concatenated_no_newlines() {
        let body = br#"{"event":"a"}{"event":"b"}"#;
        let events = parse_hec_events(body).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_event_object_payload() {
        let body = br#"{"event":{"msg":"hello","level":"info"}}"#;
        let events = parse_hec_events(body).unwrap();
        let json = hec_event_to_json(events.into_iter().next().unwrap(), RawCapture::OFF).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["msg"], "hello");
        assert_eq!(parsed["level"], "info");
    }

    #[test]
    fn test_event_string_payload() {
        let body = br#"{"event":"just a string"}"#;
        let events = parse_hec_events(body).unwrap();
        let json = hec_event_to_json(events.into_iter().next().unwrap(), RawCapture::OFF).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["message"], "just a string");
    }

    #[test]
    fn test_event_number_payload() {
        let body = br#"{"event":42}"#;
        let events = parse_hec_events(body).unwrap();
        let json = hec_event_to_json(events.into_iter().next().unwrap(), RawCapture::OFF).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["message"], 42);
    }

    #[test]
    fn test_metadata_injection() {
        let body = br#"{"event":{"msg":"test"},"time":1447828325.5,"host":"web01","source":"app","sourcetype":"json","index":"main"}"#;
        let events = parse_hec_events(body).unwrap();
        let json = hec_event_to_json(events.into_iter().next().unwrap(), RawCapture::OFF).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["msg"], "test");
        assert_eq!(parsed["_time"], 1_447_828_325.5);
        assert_eq!(parsed["host"], "web01");
        assert_eq!(parsed["source"], "app");
        assert_eq!(parsed["sourcetype"], "json");
        assert_eq!(parsed["index"], "main");
    }

    #[test]
    fn test_metadata_does_not_overwrite_event_fields() {
        let body = br#"{"event":{"host":"from-event"},"host":"from-metadata"}"#;
        let events = parse_hec_events(body).unwrap();
        let json = hec_event_to_json(events.into_iter().next().unwrap(), RawCapture::OFF).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        // Event field takes precedence
        assert_eq!(parsed["host"], "from-event");
    }

    #[test]
    fn test_fields_merge() {
        let body = br#"{"event":{"msg":"test"},"fields":{"env":"prod","region":"us-east"}}"#;
        let events = parse_hec_events(body).unwrap();
        let json = hec_event_to_json(events.into_iter().next().unwrap(), RawCapture::OFF).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["env"], "prod");
        assert_eq!(parsed["region"], "us-east");
    }

    #[test]
    fn test_empty_body_rejected() {
        let err = parse_hec_events(b"").unwrap_err();
        assert!(err.to_string().contains("no data"));
    }

    #[test]
    fn test_null_event_rejected() {
        let err = parse_hec_events(br#"{"event":null}"#).unwrap_err();
        assert!(err.to_string().contains("blank"));
    }

    #[test]
    fn test_missing_event_field() {
        let err = parse_hec_events(br#"{"host":"web01"}"#).unwrap_err();
        assert!(err.to_string().contains("parse error"));
    }

    #[test]
    fn test_invalid_json() {
        let err = parse_hec_events(b"not json at all").unwrap_err();
        assert!(err.to_string().contains("parse error"));
    }

    #[test]
    fn test_raw_to_json_with_metadata() {
        let metadata = RawMetadata {
            host: Some("web01".into()),
            source: Some("syslog".into()),
            sourcetype: Some("syslog".into()),
            index: None,
        };
        let json = raw_to_json(b"hello world", &metadata, RawCapture::OFF).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["message"], "hello world");
        assert_eq!(parsed["host"], "web01");
        assert_eq!(parsed["source"], "syslog");
        assert_eq!(parsed["sourcetype"], "syslog");
        assert!(parsed.get("index").is_none());
    }

    #[test]
    fn test_raw_to_json_no_metadata() {
        let metadata = RawMetadata::default();
        let json = raw_to_json(b"plain text event", &metadata, RawCapture::OFF).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["message"], "plain text event");
        assert_eq!(parsed.as_object().unwrap().len(), 1);
    }

    // -----------------------------------------------------------------------
    // Raw capture
    // -----------------------------------------------------------------------

    #[test]
    fn capture_off_emits_no_raw_field() {
        let events = parse_hec_events(br#"{"event":{"msg":"hello"}}"#).unwrap();
        let json = hec_event_to_json(events.into_iter().next().unwrap(), RawCapture::OFF).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert!(parsed.get("_raw").is_none());
    }

    #[test]
    fn event_capture_holds_the_payload_before_metadata_merge() {
        let body = br#"{"event":{"msg":"hello"},"host":"web01","fields":{"env":"prod"}}"#;
        let events = parse_hec_events(body).unwrap();
        let json = hec_event_to_json(events.into_iter().next().unwrap(), RawCapture::on()).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();

        // Metadata is merged into the event as before.
        assert_eq!(parsed["msg"], "hello");
        assert_eq!(parsed["host"], "web01");
        assert_eq!(parsed["env"], "prod");

        // _raw is what the sender put in `event`, and nothing else.
        let captured: serde_json::Value =
            serde_json::from_str(parsed["_raw"].as_str().unwrap()).unwrap();
        assert_eq!(captured["msg"], "hello");
        assert!(captured.get("host").is_none());
        assert!(captured.get("env").is_none());
    }

    #[test]
    fn event_capture_works_for_a_bare_string_payload() {
        let events = parse_hec_events(br#"{"event":"just a string"}"#).unwrap();
        let json = hec_event_to_json(events.into_iter().next().unwrap(), RawCapture::on()).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();

        assert_eq!(parsed["message"], "just a string");
        // The event value was a JSON string, so _raw holds it quoted.
        assert_eq!(parsed["_raw"], r#""just a string""#);
    }

    #[test]
    fn raw_endpoint_capture_keeps_the_line_bytes() {
        let metadata = RawMetadata::default();
        let json = raw_to_json(b"hello world", &metadata, RawCapture::on()).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["message"], "hello world");
        assert_eq!(parsed["_raw"], "hello world");
    }

    #[test]
    fn raw_endpoint_capture_flags_invalid_utf8() {
        // `message` silently replaces the bad byte; only _raw_lossy says so.
        let metadata = RawMetadata::default();
        let json = raw_to_json(b"caf\xe9", &metadata, RawCapture::on()).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["_raw_lossy"], true);
        assert!(parsed["_raw"].as_str().unwrap().contains('\u{fffd}'));
    }
}
