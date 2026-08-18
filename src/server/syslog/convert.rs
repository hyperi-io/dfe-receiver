// Project:   dfe-receiver
// File:      src/server/syslog/convert.rs
// Purpose:   Syslog message parsing and conversion to pipeline JSON
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Syslog message format conversion.
//!
//! Parses syslog messages (RFC 5424 + RFC 3164 auto-detect) using `syslog_loose`
//! and converts them to pipeline-ready JSON payloads.

use bytes::Bytes;
use serde_json::Map;

use crate::config::RawCapture;
use crate::error::{Error, Result};
use crate::server::raw_capture;

/// Convert a raw syslog message string to pipeline-ready JSON bytes.
///
/// Uses `syslog_loose::parse_message` which auto-detects RFC 5424 vs RFC 3164.
/// The `_source` field is set to `"syslog"` for routing.
///
/// With `raw` capture enabled the parsed fields are joined by `_raw` holding
/// the wire line verbatim -- the PRI, the header and the original spacing,
/// none of which survives the parse. It matters most on the lines that parse
/// badly: `syslog_loose` never fails, so a malformed line silently lands its
/// whole text in `message` and is indistinguishable from a clean parse.
pub fn syslog_to_json(raw: &str, raw_capture: RawCapture) -> Result<Bytes> {
    let msg = syslog_loose::parse_message(raw, syslog_loose::Variant::Either);

    let mut obj = Map::new();

    // Message body (always present)
    obj.insert(
        "message".to_string(),
        serde_json::Value::String(msg.msg.to_string()),
    );

    // Facility
    if let Some(facility) = msg.facility {
        obj.insert(
            "facility".to_string(),
            serde_json::Value::String(facility.as_str().to_string()),
        );
    }

    // Severity
    if let Some(severity) = msg.severity {
        obj.insert(
            "severity".to_string(),
            serde_json::Value::String(severity.as_str().to_string()),
        );
    }

    // Timestamp (RFC 3339 string)
    if let Some(ts) = msg.timestamp {
        obj.insert(
            "timestamp".to_string(),
            serde_json::Value::String(ts.to_rfc3339()),
        );
    }

    // Hostname
    if let Some(hostname) = msg.hostname {
        obj.insert(
            "hostname".to_string(),
            serde_json::Value::String(hostname.to_string()),
        );
    }

    // Application name
    if let Some(appname) = msg.appname {
        obj.insert(
            "appname".to_string(),
            serde_json::Value::String(appname.to_string()),
        );
    }

    // Process ID
    if let Some(ref procid) = msg.procid {
        obj.insert(
            "procid".to_string(),
            serde_json::Value::String(procid.to_string()),
        );
    }

    // Message ID (RFC 5424)
    if let Some(msgid) = msg.msgid {
        obj.insert(
            "msgid".to_string(),
            serde_json::Value::String(msgid.to_string()),
        );
    }

    // Structured data (RFC 5424)
    if !msg.structured_data.is_empty() {
        let sd: Vec<serde_json::Value> = msg
            .structured_data
            .iter()
            .map(|elem| {
                let mut sd_obj = Map::new();
                sd_obj.insert(
                    "id".to_string(),
                    serde_json::Value::String(elem.id.to_string()),
                );
                let params: Map<String, serde_json::Value> = elem
                    .params
                    .iter()
                    .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
                    .collect();
                sd_obj.insert("params".to_string(), serde_json::Value::Object(params));
                serde_json::Value::Object(sd_obj)
            })
            .collect();
        obj.insert("structured_data".to_string(), serde_json::Value::Array(sd));
    }

    // Source tag for routing
    obj.insert(
        "_source".to_string(),
        serde_json::Value::String("syslog".to_string()),
    );

    raw_capture::attach_str_to_map(&mut obj, raw, raw_capture);

    serde_json::to_vec(&obj)
        .map(Bytes::from)
        .map_err(|e| Error::Validation(format!("syslog JSON serialisation failed: {e}")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_rfc5424_with_structured_data() {
        let raw = r#"<165>1 2026-03-03T10:30:00.123+11:00 web01 nginx 1234 ID47 [exampleSDID@32473 iut="3" eventSource="Application"] This is a test message"#;
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(json["message"], "This is a test message");
        assert_eq!(json["hostname"], "web01");
        assert_eq!(json["appname"], "nginx");
        assert_eq!(json["procid"], "1234");
        assert_eq!(json["msgid"], "ID47");
        assert_eq!(json["_source"], "syslog");

        // Facility 20 (local4) and severity 5 (notice)
        assert!(json["facility"].is_string());
        assert!(json["severity"].is_string());

        // Structured data
        let sd = json["structured_data"].as_array().unwrap();
        assert_eq!(sd.len(), 1);
        assert_eq!(sd[0]["id"], "exampleSDID@32473");
        assert_eq!(sd[0]["params"]["iut"], "3");
        assert_eq!(sd[0]["params"]["eventSource"], "Application");
    }

    #[test]
    fn test_rfc3164_bsd_format() {
        let raw = "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(json["hostname"], "mymachine");
        assert_eq!(json["appname"], "su");
        assert_eq!(json["_source"], "syslog");
        // RFC 3164 has no structured data
        assert!(json.get("structured_data").is_none());
        // RFC 3164 has no msgid
        assert!(json.get("msgid").is_none());
    }

    #[test]
    fn test_minimal_rfc3164() {
        // Minimal RFC 3164 with timestamp, hostname, and app
        let raw = "<13>Mar  3 10:00:00 myhost myapp: Hello world";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert!(json["message"].as_str().unwrap().contains("Hello world"));
        assert_eq!(json["hostname"], "myhost");
        assert_eq!(json["_source"], "syslog");
        // Facility 1 (user), severity 5 (notice) — PRI 13
        assert_eq!(json["facility"], "user");
        assert_eq!(json["severity"], "notice");
    }

    #[test]
    fn test_no_priority() {
        // Message without priority tag
        let raw = "Just a plain log message";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert!(
            json["message"]
                .as_str()
                .unwrap()
                .contains("Just a plain log message")
        );
        assert_eq!(json["_source"], "syslog");
        // No facility/severity without PRI
        assert!(json.get("facility").is_none());
        assert!(json.get("severity").is_none());
    }

    #[test]
    fn test_rfc5424_no_structured_data() {
        let raw = "<14>1 2026-01-15T12:00:00Z myhost myapp 5678 - - Application started";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(json["hostname"], "myhost");
        assert_eq!(json["appname"], "myapp");
        assert_eq!(json["procid"], "5678");
        assert!(json.get("structured_data").is_none());
    }

    #[test]
    fn test_source_field_always_set() {
        let raw = "<0>test";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["_source"], "syslog");
    }

    #[test]
    fn test_timestamp_is_rfc3339() {
        let raw = "<14>1 2026-03-03T10:30:00+11:00 host app - - - test";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        let ts = json["timestamp"].as_str().unwrap();
        // Should parse as valid chrono DateTime
        assert!(chrono::DateTime::parse_from_rfc3339(ts).is_ok());
    }

    // -----------------------------------------------------------------------
    // Raw capture
    // -----------------------------------------------------------------------

    #[test]
    fn capture_off_emits_no_raw_field() {
        let raw = "<34>Oct 11 22:14:15 mymachine su: failed";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!(json.get("_raw").is_none());
    }

    #[test]
    fn capture_adds_raw_without_disturbing_the_parsed_fields() {
        let raw =
            r#"<165>1 2026-03-03T10:30:00.123+11:00 web01 nginx 1234 ID47 [ex@1 a="b"] hello"#;
        let json: serde_json::Value =
            serde_json::from_slice(&syslog_to_json(raw, RawCapture::on()).unwrap()).unwrap();

        // Every parsed field is still exactly what it was without capture.
        assert_eq!(json["message"], "hello");
        assert_eq!(json["hostname"], "web01");
        assert_eq!(json["appname"], "nginx");
        assert_eq!(json["procid"], "1234");
        assert_eq!(json["msgid"], "ID47");
        assert_eq!(json["_source"], "syslog");
        // _raw is an addition alongside them, byte-for-byte off the wire.
        assert_eq!(json["_raw"], raw);
    }

    #[test]
    fn raw_keeps_the_pri_that_the_parse_discards() {
        // The PRI number itself appears nowhere in the parsed envelope --
        // only the decoded facility/severity names do. Recovering "<165>"
        // is the point of capturing.
        let raw = "<165>1 2026-03-03T10:30:00Z web01 nginx - - - hello";
        let json: serde_json::Value =
            serde_json::from_slice(&syslog_to_json(raw, RawCapture::on()).unwrap()).unwrap();

        assert_eq!(json["facility"], "local4");
        assert_eq!(json["severity"], "notice");
        assert!(json["_raw"].as_str().unwrap().starts_with("<165>"));
    }

    #[test]
    fn raw_distinguishes_a_malformed_line_from_a_clean_parse() {
        // syslog_loose never errors: this lands whole in `message`, which is
        // indistinguishable from a well-formed message body. _raw is what
        // tells the two apart downstream.
        let raw = "<999 not really syslog at all";
        let json: serde_json::Value =
            serde_json::from_slice(&syslog_to_json(raw, RawCapture::on()).unwrap()).unwrap();

        assert!(json.get("facility").is_none());
        assert_eq!(json["_raw"], raw);
    }

    #[test]
    fn oversized_line_truncates_and_flags() {
        let raw = "<13>Mar  3 10:00:00 myhost myapp: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let cfg = RawCapture {
            enabled: true,
            max_bytes: 16,
            on_oversize: crate::config::OversizePolicy::Truncate,
        };
        let json: serde_json::Value =
            serde_json::from_slice(&syslog_to_json(raw, cfg).unwrap()).unwrap();

        assert_eq!(json["_raw"], &raw[..16]);
        assert_eq!(json["_raw_truncated"], true);
        // The parsed fields are unaffected by the cap on _raw.
        assert_eq!(json["hostname"], "myhost");
    }

    #[test]
    fn oversized_line_can_be_dropped_instead() {
        let raw = "<13>Mar  3 10:00:00 myhost myapp: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let cfg = RawCapture {
            enabled: true,
            max_bytes: 16,
            on_oversize: crate::config::OversizePolicy::Omit,
        };
        let json: serde_json::Value =
            serde_json::from_slice(&syslog_to_json(raw, cfg).unwrap()).unwrap();

        assert!(json.get("_raw").is_none());
        assert!(json.get("_raw_truncated").is_none());
        assert_eq!(json["hostname"], "myhost");
    }
}
