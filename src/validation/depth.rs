// Project:   dfe-receiver
// File:      src/validation/depth.rs
// Purpose:   Nesting-depth pre-check ahead of every lazy sonic-rs parse
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Nesting-depth pre-check for untrusted JSON.
//!
//! sonic-rs validates a lazy value by recursing once per nesting level with no
//! depth limit, so a payload nested a few thousand levels deep exhausts a
//! 2 MiB Tokio worker stack and aborts the whole process. Every payload is
//! measured here, iteratively, before a lazy sonic-rs call reads it.

use scalo::logger::security;

use crate::error::{Error, Result};
use crate::metrics::{Metrics, ValidationFailureReason};

/// Deepest nesting any ingress accepts, the bound scalo's parse path uses.
pub const MAX_PARSE_DEPTH: usize = 64;

/// Deepest nesting a batch body may reach: its events plus the array around them.
pub const MAX_BATCH_DEPTH: usize = MAX_PARSE_DEPTH + 1;

// SHORTCUT: app-local copy of scalo's json_depth_within, until scalo exposes it
/// `true` if the JSON payload nests no deeper than `max`.
///
/// One forward pass counting `{` and `[` outside strings, honouring `\`
/// escapes. Not a validator: on malformed input the parser stops at the first
/// bad token, which is no deeper than this pass has already counted.
#[must_use]
pub fn json_depth_within(payload: &[u8], max: usize) -> bool {
    let mut depth: usize = 0;
    let mut in_string = false;
    let mut escaped = false;
    for &b in payload {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > max {
                    return false;
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    true
}

/// Refuse a payload that nests deeper than `max`, counting the refusal.
///
/// # Errors
///
/// Returns [`Error::Validation`], which every listener answers as a refusal
/// the sender must not retry.
pub fn admit(payload: &[u8], max: usize, metrics: Option<&Metrics>) -> Result<()> {
    if json_depth_within(payload, max) {
        return Ok(());
    }
    Err(refuse(metrics))
}

#[cold]
#[inline(never)]
fn refuse(metrics: Option<&Metrics>) -> Error {
    let reason = format!("payload nesting exceeds the maximum parse depth of {MAX_PARSE_DEPTH}");
    security::input_validation_failure("json_depth", &reason, None);
    if let Some(metrics) = metrics {
        metrics.inc_validation_failure(ValidationFailureReason::NestingTooDeep);
    }
    Error::Validation(reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested(open: &str, close: &str, depth: usize) -> Vec<u8> {
        let mut payload = open.repeat(depth).into_bytes();
        payload.extend_from_slice(b"1");
        payload.extend_from_slice(close.repeat(depth).as_bytes());
        payload
    }

    #[test]
    fn flat_and_shallow_pass() {
        assert!(json_depth_within(br"{}", MAX_PARSE_DEPTH));
        assert!(json_depth_within(
            br#"{"a":1,"b":[1,2,3]}"#,
            MAX_PARSE_DEPTH
        ));
        assert!(json_depth_within(
            br#"{"a":{"b":{"c":1}}}"#,
            MAX_PARSE_DEPTH
        ));
        assert!(json_depth_within(b"", MAX_PARSE_DEPTH));
    }

    #[test]
    fn exactly_at_the_bound_passes_and_one_over_fails() {
        assert!(json_depth_within(&nested("[", "]", 3), 3));
        assert!(!json_depth_within(&nested("[", "]", 4), 3));
        assert!(json_depth_within(
            &nested("{\"a\":", "}", MAX_PARSE_DEPTH),
            MAX_PARSE_DEPTH
        ));
        assert!(!json_depth_within(
            &nested("{\"a\":", "}", MAX_PARSE_DEPTH + 1),
            MAX_PARSE_DEPTH
        ));
    }

    #[test]
    fn sibling_containers_do_not_add_up() {
        // Depth is how far down the payload goes, not how many containers it has.
        let wide = format!("[{}]", vec!["[[1]]"; 1000].join(","));
        assert!(json_depth_within(wide.as_bytes(), 3));
    }

    #[test]
    fn brackets_inside_strings_do_not_count() {
        assert!(json_depth_within(br#"{"k":"{{{{{{{{[[[[["}"#, 2));
    }

    #[test]
    fn an_escaped_quote_keeps_the_string_open() {
        assert!(json_depth_within(br#"{"k":"a\"{{{{{"}"#, 2));
    }

    #[test]
    fn an_escaped_backslash_closes_the_string() {
        // `\\` is one literal backslash, so the quote after it ends the string
        // and the brackets that follow are structure.
        assert!(!json_depth_within(br#"["\\"[[[1]]]]"#, 3));
    }

    #[test]
    fn pathological_depth_is_refused() {
        for depth in [5_000, 20_000, 100_000] {
            assert!(!json_depth_within(
                &nested("[", "]", depth),
                MAX_PARSE_DEPTH
            ));
            assert!(!json_depth_within(
                &nested("{\"a\":", "}", depth),
                MAX_PARSE_DEPTH
            ));
        }
    }

    #[test]
    fn a_batch_may_nest_one_level_deeper_than_its_events() {
        let event = String::from_utf8(nested("[", "]", MAX_PARSE_DEPTH)).unwrap();
        let batch = format!("[{event},{event}]");
        assert!(json_depth_within(batch.as_bytes(), MAX_BATCH_DEPTH));
        assert!(!json_depth_within(batch.as_bytes(), MAX_PARSE_DEPTH));
    }

    #[test]
    fn a_refusal_is_a_validation_error_that_names_the_bound_and_is_counted() {
        let metrics = Metrics::default();
        let err = admit(&nested("[", "]", 65), MAX_PARSE_DEPTH, Some(&metrics))
            .expect_err("65 levels is over the bound");
        assert!(matches!(err, Error::Validation(_)), "{err}");
        assert!(!err.is_retryable(), "a sender must not retry it");
        assert!(
            err.to_string().contains("maximum parse depth of 64"),
            "{err}"
        );
        assert_eq!(metrics.get_validation_failures_total(), 1);

        admit(&nested("[", "]", 64), MAX_PARSE_DEPTH, Some(&metrics)).expect("64 levels is in");
        assert_eq!(metrics.get_validation_failures_total(), 1);
    }
}
