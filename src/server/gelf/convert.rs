// Project:   dfe-receiver
// File:      src/server/gelf/convert.rs
// Purpose:   GELF message validation and conversion to pipeline JSON
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! GELF message conversion.
//!
//! Validates GELF 1.1 messages and converts them to pipeline-ready JSON.
//! Required fields: `version`, `host`, `short_message`.
//! Custom fields (prefixed with `_`) are preserved as-is.

use bytes::Bytes;

use crate::error::{Error, Result};

/// Severity level names matching syslog levels 0-7.
const SEVERITY_NAMES: [&str; 8] = [
    "emergency",
    "alert",
    "critical",
    "error",
    "warning",
    "notice",
    "informational",
    "debug",
];

/// Convert a raw GELF JSON message to pipeline-ready JSON bytes.
///
/// Validates required fields and injects `_source: "gelf"` for routing.
/// The original GELF fields are preserved; `short_message` is also
/// copied to `message` for consistency with other handlers.
pub fn gelf_to_json(raw: &[u8]) -> Result<Bytes> {
    let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(raw)
        .map_err(|e| Error::Validation(format!("GELF JSON parse failed: {e}")))?;

    // Validate required fields
    if !obj.contains_key("version") {
        return Err(Error::Validation(
            "GELF message missing required field: version".into(),
        ));
    }
    if !obj.contains_key("host") {
        return Err(Error::Validation(
            "GELF message missing required field: host".into(),
        ));
    }
    if !obj.contains_key("short_message") {
        return Err(Error::Validation(
            "GELF message missing required field: short_message".into(),
        ));
    }

    // Copy short_message to message for consistent downstream processing
    if let Some(short_msg) = obj.get("short_message").cloned() {
        obj.entry("message".to_string()).or_insert(short_msg);
    }

    // Map numeric level to severity name
    if let Some(level) = obj.get("level").and_then(serde_json::Value::as_u64)
        && let Some(name) = SEVERITY_NAMES.get(level as usize)
    {
        obj.insert(
            "severity".to_string(),
            serde_json::Value::String((*name).to_string()),
        );
    }

    // Source tag for routing
    obj.insert(
        "_source".to_string(),
        serde_json::Value::String("gelf".to_string()),
    );

    serde_json::to_vec(&obj)
        .map(Bytes::from)
        .map_err(|e| Error::Validation(format!("GELF JSON serialisation failed: {e}")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_gelf_message() {
        let raw = br#"{"version":"1.1","host":"web01","short_message":"Test message","level":6,"_user_id":"123"}"#;
        let result = gelf_to_json(raw).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(json["version"], "1.1");
        assert_eq!(json["host"], "web01");
        assert_eq!(json["short_message"], "Test message");
        assert_eq!(json["message"], "Test message");
        assert_eq!(json["level"], 6);
        assert_eq!(json["severity"], "informational");
        assert_eq!(json["_source"], "gelf");
        assert_eq!(json["_user_id"], "123");
    }

    #[test]
    fn test_missing_version() {
        let raw = br#"{"host":"web01","short_message":"Test"}"#;
        assert!(gelf_to_json(raw).is_err());
    }

    #[test]
    fn test_missing_host() {
        let raw = br#"{"version":"1.1","short_message":"Test"}"#;
        assert!(gelf_to_json(raw).is_err());
    }

    #[test]
    fn test_missing_short_message() {
        let raw = br#"{"version":"1.1","host":"web01"}"#;
        assert!(gelf_to_json(raw).is_err());
    }

    #[test]
    fn test_invalid_json() {
        let raw = b"not json";
        assert!(gelf_to_json(raw).is_err());
    }

    #[test]
    fn test_source_always_set() {
        let raw = br#"{"version":"1.1","host":"h","short_message":"m"}"#;
        let result = gelf_to_json(raw).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["_source"], "gelf");
    }

    #[test]
    fn test_severity_mapping() {
        for level in 0u64..8 {
            let raw =
                format!(r#"{{"version":"1.1","host":"h","short_message":"m","level":{level}}}"#);
            let result = gelf_to_json(raw.as_bytes()).unwrap();
            let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
            assert_eq!(json["severity"], SEVERITY_NAMES[level as usize]);
        }
    }

    #[test]
    fn test_custom_fields_preserved() {
        let raw = br#"{"version":"1.1","host":"h","short_message":"m","_env":"prod","_request_id":"abc"}"#;
        let result = gelf_to_json(raw).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["_env"], "prod");
        assert_eq!(json["_request_id"], "abc");
    }

    #[test]
    fn test_full_message_preserved() {
        let raw = br#"{"version":"1.1","host":"h","short_message":"brief","full_message":"detailed error\nwith stacktrace"}"#;
        let result = gelf_to_json(raw).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["full_message"], "detailed error\nwith stacktrace");
    }

    #[test]
    fn test_timestamp_preserved() {
        let raw = br#"{"version":"1.1","host":"h","short_message":"m","timestamp":1678876543.123}"#;
        let result = gelf_to_json(raw).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!((json["timestamp"].as_f64().unwrap() - 1_678_876_543.123).abs() < 0.001);
    }
}
