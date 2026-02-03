// Project:   dfe-receiver
// File:      src/validation/mod.rs
// Purpose:   Request validation (JSON format, required fields)
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

//! Request validation module.
//!
//! Validates incoming payloads for JSON format and required field presence
//! using on-demand parsing for maximum performance.

use bytes::Bytes;
use sonic_rs::LazyValue;

use crate::config::ValidationConfig;

/// Validation result indicating the outcome.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationResult {
    /// Payload is valid.
    Valid,
    /// Payload should go to DLQ.
    Dlq(String),
    /// Payload should be rejected.
    Reject(String),
}

/// Validator for incoming payloads.
pub struct Validator {
    config: ValidationConfig,
}

impl Validator {
    /// Create a new validator with the given configuration.
    pub fn new(config: ValidationConfig) -> Self {
        Self { config }
    }

    /// Validate a payload.
    ///
    /// This is a HOT PATH function - optimised for minimal allocations.
    #[inline]
    pub fn validate(&self, payload: &Bytes) -> ValidationResult {
        // Check JSON format using sonic-rs LazyValue (no full parse)
        if self.config.require_json {
            if let Err(reason) = self.validate_json(payload) {
                return if self.config.dlq_on_invalid {
                    ValidationResult::Dlq(reason)
                } else {
                    ValidationResult::Reject(reason)
                };
            }
        }

        // Check required fields
        for field in &self.config.required_fields {
            if !self.has_field(payload, field) {
                let reason = format!("missing required field: {field}");
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
    fn validate_json(&self, payload: &Bytes) -> std::result::Result<(), String> {
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

    /// Check if a field exists in the payload.
    ///
    /// Uses on-demand extraction for zero-copy field access.
    #[inline]
    fn has_field(&self, payload: &Bytes, field: &str) -> bool {
        // Handle nested fields (dot notation)
        if field.contains('.') {
            let parts: Vec<&str> = field.split('.').collect();
            sonic_rs::get_from_slice(payload, parts.as_slice()).is_ok()
        } else {
            sonic_rs::get_from_slice(payload, [field].as_slice()).is_ok()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> ValidationConfig {
        ValidationConfig {
            require_json: true,
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
            ValidationResult::Dlq(_)
        ));
    }

    #[test]
    fn test_empty_payload() {
        let validator = Validator::new(default_config());
        let payload = Bytes::new();

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::Dlq(_)
        ));
    }

    #[test]
    fn test_required_field_present() {
        let config = ValidationConfig {
            require_json: true,
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
            require_json: true,
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
            require_json: true,
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
            require_json: true,
            required_fields: vec![],
            dlq_on_invalid: false,
        };
        let validator = Validator::new(config);
        let payload = Bytes::from("not json");

        assert!(matches!(
            validator.validate(&payload),
            ValidationResult::Reject(_)
        ));
    }
}
