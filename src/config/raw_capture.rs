// Project:   dfe-receiver
// File:      src/config/raw_capture.rs
// Purpose:   Opt-in retention of the original wire payload in `_raw`
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Raw-payload capture configuration.
//!
//! The dfe-schemas common header (`timeseries` profile) carries a `_raw` text
//! column, full-text indexed, holding the original event payload. dfe-loader
//! populates it by renaming `first(logoriginal/_raw/raw/raw_log/message)` and
//! is a silent no-op when `_raw` is already present -- so a receiver-populated
//! `_raw` wins with no loader change.
//!
//! That matters because the loader's fallback can only reach fields that
//! survived our conversion. For syslog it lands `message`, the PARSED body:
//! the PRI number, the header text and the exact wire spacing are gone by
//! then, and `syslog_loose` never errors, so a malformed line silently becomes
//! `message = <whole line>` and reads as a clean parse. Capturing at the
//! transport is the only place the true bytes still exist.
//!
//! Capture roughly doubles the payload for text protocols, on top of an ngram
//! text index downstream, which is why it is opt-in and configured per
//! transport rather than globally forced.
//!
//! # Cascade
//!
//! ```yaml
//! raw_capture:              # common default for every transport
//!   enabled: false
//!   max_bytes: 65536        # 0 = unlimited
//!   on_oversize: truncate   # truncate | omit
//! syslog:
//!   raw_capture:
//!     enabled: true         # per-transport override, inherits the rest
//! ```
//!
//! Every field is optional at both levels. An unset field inherits the common
//! block, and an unset common field falls back to the built-in default -- so
//! "off" and "inherit" stay distinguishable, which a bare `bool` cannot do.

use serde::{Deserialize, Serialize};

/// Built-in default cap on a captured payload (64 KiB).
pub const DEFAULT_MAX_BYTES: usize = 64 * 1024;

/// What to do with a payload larger than `max_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OversizePolicy {
    /// Keep the leading `max_bytes` and flag it with `_raw_truncated: true`.
    Truncate,
    /// Emit no `_raw` at all for this event.
    Omit,
}

impl Default for OversizePolicy {
    fn default() -> Self {
        Self::Truncate
    }
}

impl OversizePolicy {
    /// Parse a config string. `None` for an unrecognised value -- callers warn
    /// and keep the previous setting rather than silently picking a policy.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "truncate" => Some(Self::Truncate),
            "omit" => Some(Self::Omit),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Truncate => "truncate",
            Self::Omit => "omit",
        }
    }
}

/// Raw-capture settings as written in config, at either cascade level.
///
/// Every field is `Option` so an unset field means "inherit", not "off".
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct RawCaptureConfig {
    /// Capture the original payload into `_raw`.
    pub enabled: Option<bool>,

    /// Maximum captured bytes; 0 means unlimited.
    pub max_bytes: Option<usize>,

    /// Behaviour when the payload exceeds `max_bytes`.
    pub on_oversize: Option<OversizePolicy>,
}

impl RawCaptureConfig {
    /// A fully-specified block, for tests and for the common-level default.
    pub fn new(enabled: bool, max_bytes: usize, on_oversize: OversizePolicy) -> Self {
        Self {
            enabled: Some(enabled),
            max_bytes: Some(max_bytes),
            on_oversize: Some(on_oversize),
        }
    }

    /// Resolve this per-transport override against the common block.
    ///
    /// Field-wise: transport override, else common, else built-in default.
    pub fn resolve(&self, common: &RawCaptureConfig) -> RawCapture {
        RawCapture {
            enabled: self.enabled.or(common.enabled).unwrap_or(false),
            max_bytes: self
                .max_bytes
                .or(common.max_bytes)
                .unwrap_or(DEFAULT_MAX_BYTES),
            on_oversize: self.on_oversize.or(common.on_oversize).unwrap_or_default(),
        }
    }
}

/// Effective raw-capture settings handed to a transport handler.
///
/// Produced by [`RawCaptureConfig::resolve`]; every field is concrete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawCapture {
    pub enabled: bool,
    pub max_bytes: usize,
    pub on_oversize: OversizePolicy,
}

impl Default for RawCapture {
    fn default() -> Self {
        Self {
            enabled: false,
            max_bytes: DEFAULT_MAX_BYTES,
            on_oversize: OversizePolicy::Truncate,
        }
    }
}

impl RawCapture {
    /// Capture disabled -- the default for every transport.
    pub const OFF: Self = Self {
        enabled: false,
        max_bytes: DEFAULT_MAX_BYTES,
        on_oversize: OversizePolicy::Truncate,
    };

    /// Capture enabled with the built-in cap, for tests.
    pub fn on() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn unset_everywhere_resolves_to_off() {
        let resolved = RawCaptureConfig::default().resolve(&RawCaptureConfig::default());
        assert!(!resolved.enabled);
        assert_eq!(resolved.max_bytes, DEFAULT_MAX_BYTES);
        assert_eq!(resolved.on_oversize, OversizePolicy::Truncate);
    }

    #[test]
    fn common_applies_when_transport_is_silent() {
        let common = RawCaptureConfig::new(true, 128, OversizePolicy::Omit);
        let resolved = RawCaptureConfig::default().resolve(&common);
        assert!(resolved.enabled);
        assert_eq!(resolved.max_bytes, 128);
        assert_eq!(resolved.on_oversize, OversizePolicy::Omit);
    }

    #[test]
    fn transport_override_beats_common() {
        let common = RawCaptureConfig::new(true, 128, OversizePolicy::Omit);
        let transport = RawCaptureConfig {
            enabled: Some(false),
            ..RawCaptureConfig::default()
        };
        let resolved = transport.resolve(&common);
        // enabled overridden off, the other two still inherited
        assert!(!resolved.enabled);
        assert_eq!(resolved.max_bytes, 128);
        assert_eq!(resolved.on_oversize, OversizePolicy::Omit);
    }

    #[test]
    fn transport_can_opt_in_against_a_common_off() {
        let common = RawCaptureConfig::new(false, 1024, OversizePolicy::Truncate);
        let transport = RawCaptureConfig {
            enabled: Some(true),
            max_bytes: Some(512),
            on_oversize: None,
        };
        let resolved = transport.resolve(&common);
        assert!(resolved.enabled);
        assert_eq!(resolved.max_bytes, 512);
        assert_eq!(resolved.on_oversize, OversizePolicy::Truncate);
    }

    #[test]
    fn explicit_off_is_distinguishable_from_unset() {
        // The whole reason the fields are Option: a transport that says
        // `enabled: false` must beat a common `enabled: true`, while a
        // transport that says nothing must inherit it.
        let common = RawCaptureConfig::new(true, 4096, OversizePolicy::Truncate);
        let explicit_off = RawCaptureConfig {
            enabled: Some(false),
            ..RawCaptureConfig::default()
        };
        assert!(!explicit_off.resolve(&common).enabled);
        assert!(RawCaptureConfig::default().resolve(&common).enabled);
    }

    #[test]
    fn oversize_policy_parse_round_trips() {
        assert_eq!(
            OversizePolicy::parse("truncate"),
            Some(OversizePolicy::Truncate)
        );
        assert_eq!(OversizePolicy::parse("OMIT"), Some(OversizePolicy::Omit));
        assert_eq!(OversizePolicy::parse("drop"), None);
        assert_eq!(OversizePolicy::Truncate.label(), "truncate");
        assert_eq!(OversizePolicy::Omit.label(), "omit");
    }

    #[test]
    fn yaml_round_trip_keeps_unset_fields_unset() {
        let parsed: RawCaptureConfig = serde_yaml_ng::from_str("enabled: true").unwrap();
        assert_eq!(parsed.enabled, Some(true));
        assert!(parsed.max_bytes.is_none());
        assert!(parsed.on_oversize.is_none());
    }
}
