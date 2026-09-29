// Project:   dfe-receiver
// File:      src/validation/mod.rs
// Purpose:   Request validation (JSON format, required fields)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Request validation module.
//!
//! Validates incoming payloads for JSON format and required field presence
//! using on-demand parsing for maximum performance.
//!
//! JSON is the only payload format. A body that is not JSON is refused for
//! good; `dlq_on_invalid` decides only what happens to JSON that fails a
//! required-field check.

pub mod depth;

use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use scalo::logger::log_debounced;
use scalo::logger::security;
use sonic_rs::LazyValue;

use crate::config::ValidationConfig;
use crate::metrics::{Metrics, ValidationFailureReason};

/// Shortest gap between two security lines for one validation failure reason.
const VALIDATION_LOG_INTERVAL_MS: u64 = 5_000;

/// When each failure reason last wrote its security line, indexed by
/// [`ValidationFailureReason::index`].
pub(crate) static VALIDATION_FAILURE_LOGGED: [AtomicU64; ValidationFailureReason::ALL.len()] =
    [const { AtomicU64::new(0) }; ValidationFailureReason::ALL.len()];

/// Record a record refused at `action` for `reason`.
///
/// Every refusal counts on `receiver_validation_failures_total`. The security
/// event is written at most once per `VALIDATION_LOG_INTERVAL_MS` per reason
/// and names the reason label only, so a sender can neither drive the log at
/// the rate it sends nor write its record into it.
pub fn record_validation_failure(
    metrics: Option<&Metrics>,
    action: &str,
    reason: ValidationFailureReason,
) {
    if let Some(metrics) = metrics {
        metrics.inc_validation_failure(reason);
    }
    if log_debounced(
        &VALIDATION_FAILURE_LOGGED[reason.index()],
        VALIDATION_LOG_INTERVAL_MS,
    ) {
        security::input_validation_failure(action, reason.label(), None);
    }
}

/// Validation result indicating the outcome.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationResult {
    /// Payload is valid.
    Valid,
    /// Payload is not JSON, so it is refused whatever `dlq_on_invalid` says.
    NotJson(String),
    /// Payload should go to DLQ.
    Dlq(String),
    /// Payload should be rejected.
    Reject(String),
}

/// Pre-split field path for efficient nested lookups.
struct FieldPath {
    raw: String,
    parts: Vec<String>,
}

/// Validator for incoming payloads.
pub struct Validator {
    config: ValidationConfig,
    /// Pre-split required field paths (avoids split('.') per message).
    required_field_paths: Vec<FieldPath>,
}

impl Validator {
    /// Create a new validator with the given configuration.
    pub fn new(config: ValidationConfig) -> Self {
        let required_field_paths = config
            .required_fields
            .iter()
            .map(|f| FieldPath {
                parts: f.split('.').map(String::from).collect(),
                raw: f.clone(),
            })
            .collect();
        Self {
            config,
            required_field_paths,
        }
    }

    /// Validate a payload.
    ///
    /// This is a HOT PATH function - optimised for minimal allocations.
    #[inline]
    pub fn validate(&self, payload: &Bytes) -> ValidationResult {
        // Check JSON format using sonic-rs LazyValue (no full parse)
        if let Err(reason) = Self::validate_json(payload) {
            return ValidationResult::NotJson(reason);
        }

        // Check required fields (using pre-split paths)
        for fp in &self.required_field_paths {
            if !Self::has_field_parts(payload, &fp.parts) {
                let reason = format!("missing required field: {}", fp.raw);
                return if self.config.dlq_on_invalid {
                    ValidationResult::Dlq(reason)
                } else {
                    ValidationResult::Reject(reason)
                };
            }
        }

        ValidationResult::Valid
    }

    /// Check if the payload is valid JSON.
    ///
    /// Uses sonic-rs LazyValue for fast format detection without full parsing.
    #[inline]
    fn validate_json(payload: &Bytes) -> std::result::Result<(), String> {
        // Empty payload is invalid
        if payload.is_empty() {
            return Err("empty payload".to_string());
        }

        // Try to parse as LazyValue - this validates JSON structure
        // without building a full DOM tree
        match sonic_rs::from_slice::<LazyValue>(payload) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("invalid JSON: {e}")),
        }
    }

    /// Check if a field exists using pre-split parts.
    #[inline]
    fn has_field_parts(payload: &Bytes, parts: &[String]) -> bool {
        if parts.len() > 1 {
            let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
            sonic_rs::get_from_slice(payload, refs.as_slice()).is_ok()
        } else {
            sonic_rs::get_from_slice(payload, [parts[0].as_str()].as_slice()).is_ok()
        }
    }
}

#[cfg(test)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::format_push_string)]
#[allow(clippy::single_char_add_str)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn default_config() -> ValidationConfig {
        ValidationConfig {
            required_fields: vec![],
            dlq_on_invalid: true,
        }
    }

    #[test]
    fn test_valid_json() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"key": "value"}"#);

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_invalid_json() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("not json");

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_empty_payload() {
        let validator = Validator::new(default_config());
        let payload = Bytes::new();

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_required_field_present() {
        let config = ValidationConfig {
            required_fields: vec!["org_id".to_string()],
            dlq_on_invalid: true,
        };
        let validator = Validator::new(config);
        let payload = Bytes::from(r#"{"org_id": "test", "data": "value"}"#);

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_required_field_missing() {
        let config = ValidationConfig {
            required_fields: vec!["org_id".to_string()],
            dlq_on_invalid: true,
        };
        let validator = Validator::new(config);
        let payload = Bytes::from(r#"{"data": "value"}"#);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::Dlq(_)
        ));
    }

    #[test]
    fn test_nested_required_field() {
        let config = ValidationConfig {
            required_fields: vec!["tags.event_category".to_string()],
            dlq_on_invalid: true,
        };
        let validator = Validator::new(config);

        let valid = Bytes::from(r#"{"tags": {"event_category": "auth"}}"#);
        assert_eq!(validator.validate(&valid), ValidationResult::Valid);

        let invalid = Bytes::from(r#"{"tags": {"other": "value"}}"#);
        assert!(matches!(
            validator.validate(&invalid),
            ValidationResult::Dlq(_)
        ));
    }

    #[test]
    fn test_reject_mode() {
        let config = ValidationConfig {
            required_fields: vec!["org_id".to_string()],
            dlq_on_invalid: false,
        };
        let validator = Validator::new(config);
        let payload = Bytes::from(r#"{"data": "value"}"#);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::Reject(_)
        ));
    }

    #[test]
    fn a_body_that_is_not_json_is_refused_whatever_dlq_on_invalid_says() {
        for dlq_on_invalid in [true, false] {
            let validator = Validator::new(ValidationConfig {
                dlq_on_invalid,
                ..default_config()
            });
            assert!(matches!(
                validator.validate(&Bytes::from("not json")),
                ValidationResult::NotJson(_)
            ));
        }
    }

    // ===== Fuzzing-style tests for malformed/bad inbound data =====

    #[test]
    fn test_binary_garbage() {
        let validator = Validator::new(default_config());
        // Random binary data
        let payload = Bytes::from_static(&[0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD, 0x80, 0x81]);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_null_bytes() {
        let validator = Validator::new(default_config());
        // JSON with embedded null bytes
        let payload = Bytes::from_static(b"{\"key\": \"val\x00ue\"}");

        // sonic-rs may or may not accept this - just ensure no panic
        let result = validator.validate(&payload);
        assert!(matches!(
            result,
            ValidationResult::Valid | ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_truncated_json_object() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"key": "value"#);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_truncated_json_array() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r"[1, 2, 3");

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_truncated_json_string() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"key": "unterminated"#);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_unmatched_braces() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"key": "value"}}"#);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_unmatched_brackets() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r"[1, 2, 3]]");

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_invalid_utf8() {
        let validator = Validator::new(default_config());
        // Invalid UTF-8 sequence
        let payload = Bytes::from_static(&[0x7B, 0x22, 0x6B, 0x22, 0x3A, 0xFF, 0xFE, 0x7D]);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_bom_prefix() {
        let validator = Validator::new(default_config());
        // UTF-8 BOM followed by JSON
        let mut payload = vec![0xEF, 0xBB, 0xBF];
        payload.extend_from_slice(b"{\"key\": \"value\"}");
        let payload = Bytes::from(payload);

        // BOM is not valid JSON - should be rejected
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_leading_whitespace() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("   \n\t{\"key\": \"value\"}");

        // Leading whitespace should be acceptable JSON
        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_trailing_whitespace() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("{\"key\": \"value\"}   \n\t");

        // Trailing whitespace should be acceptable JSON
        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_trailing_garbage() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"key": "value"}garbage"#);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_multiple_json_objects() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"a": 1}{"b": 2}"#);

        // Multiple objects is invalid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_xml_instead_of_json() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("<root><key>value</key></root>");

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_html_instead_of_json() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("<!DOCTYPE html><html><body>test</body></html>");

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_csv_instead_of_json() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("a,b,c\n1,2,3\n4,5,6");

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_single_quotes_instead_of_double() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("{'key': 'value'}");

        // Single quotes are not valid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_unquoted_keys() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("{key: \"value\"}");

        // Unquoted keys are not valid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_trailing_comma_in_object() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"a": 1, "b": 2,}"#);

        // Trailing commas are not valid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_trailing_comma_in_array() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("[1, 2, 3,]");

        // Trailing commas are not valid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_comments_in_json() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"key": "value" /* comment */}"#);

        // Comments are not valid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_line_comments_in_json() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("{\"key\": \"value\"} // comment");

        // Comments are not valid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_infinity_value() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"value": Infinity}"#);

        // Infinity is not valid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_nan_value() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"value": NaN}"#);

        // NaN is not valid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_undefined_value() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"value": undefined}"#);

        // undefined is not valid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_hex_numbers() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"value": 0xFF}"#);

        // Hex numbers are not valid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_octal_numbers() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"value": 0777}"#);

        // Octal numbers are not valid JSON (leading zero)
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_plus_sign_number() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"value": +42}"#);

        // Plus sign prefix is not valid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_deeply_nested() {
        // sonic-rs recurses once per nesting level, at about 56 KiB of stack a
        // level in a debug build, so this runs on 8 MiB; the pipeline refuses
        // anything deeper than depth::MAX_PARSE_DEPTH before the validator sees it.
        let handle = std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                let validator = Validator::new(default_config());
                let mut json = String::new();
                for _ in 0..30 {
                    json.push_str("{\"a\":");
                }
                json.push_str("1");
                for _ in 0..30 {
                    json.push('}');
                }
                let payload = Bytes::from(json);

                // Should handle nesting without panic
                let result = validator.validate(&payload);
                assert_eq!(result, ValidationResult::Valid);
            })
            .expect("spawn validation thread");
        handle.join().expect("validation thread panicked");
    }

    #[test]
    fn test_very_long_string() {
        let validator = Validator::new(default_config());
        // 1MB string value
        let long_value: String = "x".repeat(1024 * 1024);
        let json = format!(r#"{{"key": "{}"}}"#, long_value);
        let payload = Bytes::from(json);

        // Should handle large strings without panic
        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_very_long_key() {
        let validator = Validator::new(default_config());
        // Very long key name (64KB)
        let long_key: String = "k".repeat(64 * 1024);
        let json = format!(r#"{{"{}": "value"}}"#, long_key);
        let payload = Bytes::from(json);

        // Should handle large keys without panic
        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_many_keys() {
        let validator = Validator::new(default_config());
        // Object with many keys
        let mut json = String::from("{");
        for i in 0..1000 {
            if i > 0 {
                json.push_str(", ");
            }
            json.push_str(&format!(r#""key{}": {}"#, i, i));
        }
        json.push('}');
        let payload = Bytes::from(json);

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_duplicate_keys() {
        let validator = Validator::new(default_config());
        // Duplicate keys - technically valid JSON but unusual
        let payload = Bytes::from(r#"{"key": "first", "key": "second"}"#);

        // sonic-rs should accept this (JSON spec doesn't forbid duplicates)
        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_control_characters_in_string() {
        let validator = Validator::new(default_config());
        // Control characters must be escaped in JSON strings
        let payload = Bytes::from_static(b"{\"key\": \"tab\there\"}");

        // Unescaped tab is invalid JSON
        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_escaped_control_characters() {
        let validator = Validator::new(default_config());
        // Properly escaped control characters
        let payload = Bytes::from(r#"{"key": "tab\there\nnewline"}"#);

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_invalid_escape_sequence() {
        let validator = Validator::new(default_config());
        // Invalid escape sequence \x is not valid JSON
        let payload = Bytes::from(r#"{"key": "hex\x00value"}"#);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_invalid_unicode_escape() {
        let validator = Validator::new(default_config());
        // Incomplete unicode escape
        let payload = Bytes::from(r#"{"key": "\u00"}"#);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_just_whitespace() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("   \n\t  ");

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_just_null() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("null");

        // "null" is valid JSON
        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_just_true() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("true");

        // "true" is valid JSON
        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_just_false() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("false");

        // "false" is valid JSON
        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_just_number() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("42");

        // A number is valid JSON
        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_just_string() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("\"hello\"");

        // A string is valid JSON
        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_just_array() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("[1, 2, 3]");

        // An array is valid JSON
        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_empty_object() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("{}");

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_empty_array() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("[]");

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_empty_string() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from("\"\"");

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_scientific_notation() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"value": 1.23e10}"#);

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_negative_exponent() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"value": 1.23e-10}"#);

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_uppercase_exponent() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"value": 1.23E10}"#);

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_negative_number() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"value": -42}"#);

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_zero() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"value": 0}"#);

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_negative_zero() {
        let validator = Validator::new(default_config());
        let payload = Bytes::from(r#"{"value": -0}"#);

        assert_eq!(validator.validate(&payload), ValidationResult::Valid);
    }

    #[test]
    fn test_gzip_compressed() {
        let validator = Validator::new(default_config());
        // Gzip magic bytes
        let payload = Bytes::from_static(&[0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00]);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_protobuf_like() {
        let validator = Validator::new(default_config());
        // Bytes that might look like protobuf
        let payload = Bytes::from_static(&[0x08, 0x96, 0x01, 0x12, 0x07, 0x74, 0x65, 0x73, 0x74]);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_msgpack_is_not_json() {
        let validator = Validator::new(default_config());
        // MessagePack is refused like any other body that is not JSON.
        let payload = Bytes::from_static(&[0x82, 0xA3, 0x66, 0x6F, 0x6F, 0x01]);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_cbor_like() {
        let validator = Validator::new(default_config());
        // CBOR map header
        let payload = Bytes::from_static(&[0xBF, 0x63, 0x66, 0x6F, 0x6F, 0x01, 0xFF]);

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::NotJson(_)
        ));
    }
}
