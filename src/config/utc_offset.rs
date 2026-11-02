// Project:   dfe-receiver
// File:      src/config/utc_offset.rs
// Purpose:   A fixed UTC offset written as text in config
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A fixed offset from UTC, read from config as `UTC`, `Z`, `+HH:MM` or `+HHMM`.
//!
//! A fixed offset has no daylight saving: a site whose zone shifts sets the
//! offset its senders stamp in, or has them stamp in UTC.

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use chrono::{FixedOffset, Offset, Utc};
use serde::{Deserialize, Serialize};

/// A fixed offset from UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct UtcOffset(FixedOffset);

impl UtcOffset {
    /// The offset as chrono holds it.
    #[must_use]
    pub fn fixed(self) -> FixedOffset {
        self.0
    }
}

impl Default for UtcOffset {
    fn default() -> Self {
        Self(Utc.fix())
    }
}

impl FromStr for UtcOffset {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("utc") || s.eq_ignore_ascii_case("z") {
            return Ok(Self::default());
        }
        // chrono stops at the offset and ignores what follows, so the length
        // admits `+HHMM` and `+HH:MM` only.
        match (s.len(), s.parse::<FixedOffset>()) {
            (5 | 6, Ok(offset)) => Ok(Self(offset)),
            _ => Err(format!("{s:?} is not UTC, Z or an offset like +10:00")),
        }
    }
}

impl TryFrom<String> for UtcOffset {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl From<UtcOffset> for String {
    fn from(offset: UtcOffset) -> Self {
        offset.to_string()
    }
}

impl fmt::Display for UtcOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.local_minus_utc() == 0 {
            f.write_str("UTC")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

impl schemars::JsonSchema for UtcOffset {
    fn schema_name() -> Cow<'static, str> {
        "UtcOffset".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": "^([Uu][Tt][Cc]|[Zz]|[+-][0-9]{2}:?[0-9]{2})$",
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn offsets_parse_and_print_back() {
        for (text, seconds, printed) in [
            ("UTC", 0, "UTC"),
            ("utc", 0, "UTC"),
            ("Z", 0, "UTC"),
            ("+00:00", 0, "UTC"),
            ("+10:00", 36_000, "+10:00"),
            ("+1000", 36_000, "+10:00"),
            ("-05:30", -19_800, "-05:30"),
            ("-0800", -28_800, "-08:00"),
        ] {
            let offset: UtcOffset = text.parse().unwrap();
            assert_eq!(offset.fixed().local_minus_utc(), seconds, "{text}");
            assert_eq!(offset.to_string(), printed, "{text}");
        }
    }

    #[test]
    fn names_and_malformed_offsets_are_refused() {
        for text in [
            "",
            "AEST",
            "Australia/Sydney",
            "10:00",
            "+10",
            "+10:60",
            "+10:00x",
            "+1:30",
            "+ab:cd",
        ] {
            assert!(text.parse::<UtcOffset>().is_err(), "{text:?} parsed");
        }
    }

    #[test]
    fn config_reads_and_writes_the_text() {
        let offset: UtcOffset = serde_json::from_str("\"+11:00\"").unwrap();
        assert_eq!(serde_json::to_string(&offset).unwrap(), "\"+11:00\"");
        assert!(serde_json::from_str::<UtcOffset>("\"Mars/Olympus\"").is_err());
    }
}
