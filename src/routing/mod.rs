// Project:   dfe-receiver
// File:      src/routing/mod.rs
// Purpose:   Message routing to topics/destinations
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Message routing module.
//!
//! Routes messages to Kafka topics based on configurable field expressions
//! using zero-copy field extraction for maximum performance.

use std::borrow::Cow;
use std::sync::Arc;

use bytes::Bytes;
use rustc_hash::FxHashMap;
use sonic_rs::{JsonValueTrait, LazyValue, get_from_slice};

use crate::config::{DestinationsConfig, RoutingConfig, SourceRule};

/// Append `s` to `buf` as a quoted JSON string literal.
///
/// Source names are `[a-z0-9_]` in practice, so the common path writes the
/// bytes straight through; a quote, backslash or control byte would break the
/// record, so those go through serde rather than being written raw.
fn push_json_string(buf: &mut Vec<u8>, s: &str) {
    if s.bytes().any(|b| b == b'"' || b == b'\\' || b < 0x20) {
        let escaped = serde_json::Value::String(s.to_owned()).to_string();
        buf.extend_from_slice(escaped.as_bytes());
        return;
    }
    buf.push(b'"');
    buf.extend_from_slice(s.as_bytes());
    buf.push(b'"');
}

/// Write `_source` into a JSON object payload, byte-level, no full parse.
///
/// dfe-loader picks the destination table from `_source` in the record, so a
/// matched source rule that is never written down is lost on the loader route
/// and every source lands in the `main` table.
///
/// Returns the payload untouched when it is not a JSON object or already
/// carries a top-level `_source` -- the sender's value wins, and a second
/// top-level key of the same name makes the loader's ClickHouse JSON column
/// reject the whole record.
#[must_use]
pub fn stamp_source(payload: Bytes, source: &str) -> Bytes {
    let raw = payload.as_ref();
    let Some(insert_pos) = raw.iter().rposition(|&b| b == b'}') else {
        return payload;
    };
    if get_from_slice(raw, ["_source"].as_slice()).is_ok() {
        return payload;
    }

    let mut buf = Vec::with_capacity(raw.len() + source.len() + 16);
    buf.extend_from_slice(&raw[..insert_pos]);

    // No comma after `{`, and trailing whitespace before the brace is not content.
    if let Some(pos) = raw[..insert_pos]
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        && raw[pos] != b'{'
    {
        buf.push(b',');
    }

    buf.extend_from_slice(b"\"_source\":");
    push_json_string(&mut buf, source);
    buf.extend_from_slice(&raw[insert_pos..]);
    Bytes::from(buf)
}

/// The named destinations a record is bound for.
///
/// Shared, not copied: the list is resolved once per rule at config time, so
/// routing a record is a refcount bump rather than an allocation.
pub type Destinations = Arc<[Arc<str>]>;

/// Routing result: the named destinations, and the wire topic when one of them
/// is on the bus.
#[derive(Debug, Clone, PartialEq)]
pub enum RouteResult {
    /// Deliver to every named destination.
    Send {
        /// Destination names, resolved by the pipeline to sinks.
        destinations: Destinations,
        /// Topic for bus destinations. `None` when no destination needs one --
        /// a gRPC listener takes the record, not a topic name.
        topic: Option<String>,
    },
    /// Route to DLQ.
    Dlq(String),
}

impl RouteResult {
    /// The destination names, or empty for a DLQ route.
    #[must_use]
    pub fn destinations(&self) -> &[Arc<str>] {
        match self {
            Self::Send { destinations, .. } => destinations,
            Self::Dlq(_) => &[],
        }
    }

    /// The bus topic, when the route has one.
    #[must_use]
    pub fn topic(&self) -> Option<&str> {
        match self {
            Self::Send { topic, .. } => topic.as_deref(),
            Self::Dlq(topic) => Some(topic),
        }
    }
}

/// Router for determining message destinations and topics.
pub struct Router {
    /// Source rules (first match wins).
    source_rules: Vec<SourceRule>,
    /// Topic suffix (e.g., "_land").
    topic_suffix: String,
    /// `_source` for a record no rule matches.
    default_source: String,
    /// Pre-computed default topic (avoids format!() on every message).
    default_topic: String,
    /// Source-to-topic remapping (pre-computed with suffix).
    source_to_topic: FxHashMap<String, String>,
    /// DLQ topic.
    dlq_topic: String,
    /// DLQ enabled.
    dlq_enabled: bool,
    /// Destination for records no rule matches.
    default_destination: ResolvedDestinations,
    /// Destination routing rules with pre-split field paths.
    destination_rules: Vec<DestinationRule>,
    /// Whether enrichment (source rules) is enabled.
    enrichment_enabled: bool,
}

/// A destination list resolved at config time, with whether any member is on
/// the bus and therefore needs a topic computed per record.
#[derive(Clone)]
struct ResolvedDestinations {
    names: Destinations,
    needs_topic: bool,
}

impl ResolvedDestinations {
    fn new(reference: &crate::config::DestinationRef, destinations: &DestinationsConfig) -> Self {
        Self {
            names: reference
                .names()
                .iter()
                .map(|n| Arc::from(n.as_str()))
                .collect(),
            needs_topic: reference
                .names()
                .iter()
                .any(|name| destinations.is_bus(name)),
        }
    }
}

/// Internal destination rule representation with pre-split field paths.
struct DestinationRule {
    match_field: String,
    /// Pre-split field path for nested lookups (avoids split('.') per message).
    match_field_parts: Vec<String>,
    match_value: String,
    destination: ResolvedDestinations,
}

impl Router {
    /// Create a new router from configuration.
    pub fn new(
        routing: &RoutingConfig,
        destinations: &DestinationsConfig,
        enrichment_enabled: bool,
    ) -> Self {
        // Pre-compute source-to-topic with suffix applied
        let source_to_topic: FxHashMap<String, String> = routing
            .source_to_topic
            .iter()
            .map(|(k, v)| (k.clone(), format!("{v}{}", routing.topic_suffix)))
            .collect();

        // Pre-compute default topic (avoids format!() on every message)
        let default_topic = format!("{}{}", routing.default_source, routing.topic_suffix);

        // Convert destination rules with pre-split field paths
        let destination_rules = destinations
            .rules
            .iter()
            .map(|r| DestinationRule {
                match_field_parts: r.match_field.split('.').map(String::from).collect(),
                match_field: r.match_field.clone(),
                match_value: r.match_value.clone(),
                destination: ResolvedDestinations::new(&r.destination, destinations),
            })
            .collect();

        Self {
            source_rules: routing.effective_source_rules(),
            topic_suffix: routing.topic_suffix.clone(),
            default_source: routing.default_source.clone(),
            default_topic,
            source_to_topic,
            dlq_topic: routing.dlq.topic.clone(),
            dlq_enabled: routing.dlq.enabled,
            default_destination: ResolvedDestinations::new(&destinations.default, destinations),
            destination_rules,
            enrichment_enabled,
        }
    }

    /// Route a message to its destination.
    ///
    /// This is a HOT PATH function - uses zero-copy field extraction.
    #[inline]
    pub fn route(&self, payload: &Bytes) -> RouteResult {
        self.route_with_source(payload).0
    }

    /// Route a message, returning the source it belongs to with the destination.
    ///
    /// The source is evaluated on the loader route as well as the Kafka one.
    /// Only the Kafka route encodes it, in the topic name; on the loader route
    /// nothing downstream can recover it unless the caller writes it into the
    /// record, which is what [`stamp_source`] is for.
    ///
    /// A record no rule matches is the catch-all source, not an absent one:
    /// it reports `default_source` so the loader gets a `_source` in the data
    /// rather than a NULL column and a table picked by its own fallback.
    #[inline]
    pub fn route_with_source(&self, payload: &Bytes) -> (RouteResult, Option<String>) {
        let source = self.evaluate_source(payload);
        let destination = self.determine_destination(payload);

        // The topic is computed only when a destination is on the bus: a gRPC
        // listener takes the record, and the source travels in it.
        let topic = destination.needs_topic.then(|| match source {
            // Pre-computed source-to-topic entries already carry the suffix.
            Some(ref s) => self
                .source_to_topic
                .get(s)
                .cloned()
                .unwrap_or_else(|| format!("{s}{}", self.topic_suffix)),
            None => self.default_topic.clone(),
        });

        (
            RouteResult::Send {
                destinations: Arc::clone(&destination.names),
                topic,
            },
            source,
        )
    }

    /// Route a message to DLQ.
    #[inline]
    pub fn route_dlq(&self, _reason: &str) -> RouteResult {
        if self.dlq_enabled {
            RouteResult::Dlq(self.dlq_topic.clone())
        } else {
            RouteResult::Send {
                destinations: Arc::clone(&self.default_destination.names),
                topic: self
                    .default_destination
                    .needs_topic
                    .then(|| self.default_topic.clone()),
            }
        }
    }

    /// Determine which named destinations take the record (first match wins).
    #[inline]
    fn determine_destination(&self, payload: &Bytes) -> &ResolvedDestinations {
        for rule in &self.destination_rules {
            if let Some(value) =
                Self::extract_field_with_parts(payload, &rule.match_field, &rule.match_field_parts)
                && value.as_ref() == rule.match_value
            {
                return &rule.destination;
            }
        }
        &self.default_destination
    }

    /// Evaluate source rules against the payload (first match wins).
    ///
    /// Falls back to `default_source` when no rule matches. Returns `None` only
    /// when enrichment is disabled, which is the mode that adds no fields.
    #[inline]
    fn evaluate_source(&self, payload: &Bytes) -> Option<String> {
        if !self.enrichment_enabled {
            return None;
        }
        for rule in &self.source_rules {
            match rule.mode.as_str() {
                "key_present" if Self::extract_field_cow(payload, &rule.field).is_some() => {
                    return rule.source.clone();
                }
                "key_value_set" => {
                    if let (Some(val), Some(match_val)) = (
                        Self::extract_field_cow(payload, &rule.field),
                        &rule.match_value,
                    ) && val.as_ref() == match_val.as_str()
                    {
                        return rule.source.clone();
                    }
                }
                "key_value_use" => {
                    if let Some(val) = Self::extract_field_cow(payload, &rule.field) {
                        return Some(val.into_owned());
                    }
                }
                _ => {}
            }
        }
        Some(self.default_source.clone())
    }

    /// Extract a field value using pre-split parts (avoids split('.') per message).
    #[inline]
    fn extract_field_with_parts<'a>(
        payload: &'a Bytes,
        field: &str,
        parts: &[String],
    ) -> Option<Cow<'a, str>> {
        let lazy: LazyValue = if parts.len() > 1 {
            let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
            get_from_slice(payload, refs.as_slice()).ok()?
        } else {
            get_from_slice(payload, [field].as_slice()).ok()?
        };
        Self::lazy_to_cow(lazy)
    }

    /// Extract a field value using zero-copy when possible.
    ///
    /// Uses `Cow<str>` to avoid allocation for non-escaped strings.
    #[inline]
    fn extract_field_cow<'a>(payload: &'a Bytes, field: &str) -> Option<Cow<'a, str>> {
        // Handle nested fields (dot notation)
        let lazy: LazyValue = if field.contains('.') {
            let parts: Vec<&str> = field.split('.').collect();
            get_from_slice(payload, parts.as_slice()).ok()?
        } else {
            get_from_slice(payload, [field].as_slice()).ok()?
        };
        Self::lazy_to_cow(lazy)
    }

    /// Convert a LazyValue to Cow<str> with zero-copy when possible.
    #[inline]
    #[allow(clippy::needless_pass_by_value)]
    fn lazy_to_cow(lazy: LazyValue<'_>) -> Option<Cow<'_, str>> {
        if !lazy.is_str() {
            return None;
        }

        let raw_cow = lazy.as_raw_cow();

        match raw_cow {
            Cow::Borrowed(s) if s.len() >= 2 => {
                let inner = &s[1..s.len() - 1];
                if inner.contains('\\') {
                    lazy.as_str().map(|s| Cow::Owned(s.to_string()))
                } else {
                    Some(Cow::Borrowed(inner))
                }
            }
            Cow::Owned(s) if s.len() >= 2 => {
                let inner = &s[1..s.len() - 1];
                if inner.contains('\\') {
                    lazy.as_str().map(|s| Cow::Owned(s.to_string()))
                } else {
                    Some(Cow::Owned(inner.to_string()))
                }
            }
            _ => None,
        }
    }
}

impl Default for Router {
    /// Built from the default configs, so every topic name is authored once.
    fn default() -> Self {
        Self::new(
            &RoutingConfig::default(),
            &DestinationsConfig::default(),
            true,
        )
    }
}

#[cfg(test)]
#[allow(clippy::uninlined_format_args)]
mod tests {
    use super::*;
    use crate::config::{DestinationRule as ConfigRule, DlqConfig, SourceRule};

    /// The route went to the bus under `topic`.
    #[track_caller]
    fn assert_bus(route: &RouteResult, topic: &str) {
        assert_destinations(route, &["kafka"]);
        assert_eq!(route.topic(), Some(topic));
    }

    /// The route went to exactly these named destinations.
    #[track_caller]
    fn assert_destinations(route: &RouteResult, expected: &[&str]) {
        let actual: Vec<&str> = route.destinations().iter().map(AsRef::as_ref).collect();
        assert_eq!(actual, expected, "route: {route:?}");
    }

    fn default_routing_config() -> RoutingConfig {
        RoutingConfig::default()
    }

    fn legacy_routing_config() -> RoutingConfig {
        RoutingConfig {
            legacy_compat: true,
            ..default_routing_config()
        }
    }

    fn default_destinations_config() -> DestinationsConfig {
        DestinationsConfig::default()
    }

    /// Two gRPC destinations beside the built-in names, as a source with a
    /// transform and an archive compiles to.
    fn named_destinations_config(default: &str, rules: Vec<ConfigRule>) -> DestinationsConfig {
        let mut named = std::collections::HashMap::new();
        for name in ["transform_orders", "archiver"] {
            named.insert(
                name.to_string(),
                crate::config::DestinationSpec {
                    grpc: Some(crate::config::GrpcDestination {
                        endpoint: format!("http://dfe-{name}:6000"),
                        ..crate::config::GrpcDestination::default()
                    }),
                    kafka: None,
                },
            );
        }
        DestinationsConfig {
            default: default.into(),
            rules,
            named,
        }
    }

    // --- No rules (default source) ---

    #[test]
    fn test_no_rules_uses_default() {
        let router = Router::new(
            &default_routing_config(),
            &default_destinations_config(),
            true,
        );
        let payload = Bytes::from(r#"{"data": "test"}"#);

        assert_bus(&router.route(&payload), "main_land");
    }

    // --- key_value_use: if field exists, use its value as source ---

    #[test]
    fn test_source_rule_key_value_use() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "_source".to_string(),
                mode: "key_value_use".to_string(),
                match_value: None,
                source: None,
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload = Bytes::from(r#"{"_source": "auth", "data": "test"}"#);

        assert_bus(&router.route(&payload), "auth_land");
    }

    #[test]
    fn test_source_rule_key_value_use_missing_field() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "_source".to_string(),
                mode: "key_value_use".to_string(),
                match_value: None,
                source: None,
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload = Bytes::from(r#"{"data": "test"}"#);

        assert_bus(&router.route(&payload), "main_land");
    }

    // --- key_present: if field exists, use configured source ---

    #[test]
    fn test_source_rule_key_present() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "host".to_string(),
                mode: "key_present".to_string(),
                match_value: None,
                source: Some("firewall".to_string()),
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload = Bytes::from(r#"{"host": "fw-1", "data": "test"}"#);

        assert_bus(&router.route(&payload), "firewall_land");
    }

    #[test]
    fn test_source_rule_key_present_missing() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "host".to_string(),
                mode: "key_present".to_string(),
                match_value: None,
                source: Some("firewall".to_string()),
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload = Bytes::from(r#"{"data": "test"}"#);

        assert_bus(&router.route(&payload), "main_land");
    }

    // --- key_value_set: if field == match_value, use configured source ---

    #[test]
    fn test_source_rule_key_value_set() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "type".to_string(),
                mode: "key_value_set".to_string(),
                match_value: Some("syslog".to_string()),
                source: Some("logs_syslog".to_string()),
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload = Bytes::from(r#"{"type": "syslog", "data": "test"}"#);

        assert_bus(&router.route(&payload), "logs_syslog_land");
    }

    #[test]
    fn test_source_rule_key_value_set_no_match() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "type".to_string(),
                mode: "key_value_set".to_string(),
                match_value: Some("syslog".to_string()),
                source: Some("logs_syslog".to_string()),
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload = Bytes::from(r#"{"type": "netflow", "data": "test"}"#);

        assert_bus(&router.route(&payload), "main_land");
    }

    // --- The routed source is written into the record ---
    //
    // dfe-loader reads `_source` out of the data to pick the table, and on the
    // loader route no topic is computed, so a source left unwritten sends every
    // receiver-routed record to the `main` table.

    fn kvproof_routing() -> RoutingConfig {
        RoutingConfig {
            source_rules: vec![SourceRule {
                field: "app".to_string(),
                mode: "key_value_set".to_string(),
                match_value: Some("kvproof".to_string()),
                source: Some("kvproof".to_string()),
            }],
            ..default_routing_config()
        }
    }

    fn loader_destinations_config() -> DestinationsConfig {
        DestinationsConfig {
            default: "loader".into(),
            ..DestinationsConfig::default()
        }
    }

    #[test]
    fn test_loader_route_reports_the_matched_source() {
        let router = Router::new(&kvproof_routing(), &loader_destinations_config(), true);
        let payload = Bytes::from(r#"{"app":"kvproof","message":"hello"}"#);

        let (route, source) = router.route_with_source(&payload);
        assert_destinations(&route, &["loader"]);
        assert_eq!(source.as_deref(), Some("kvproof"));

        let stamped = stamp_source(payload, "kvproof");
        let parsed: serde_json::Value = serde_json::from_slice(&stamped).expect("valid JSON");
        assert_eq!(parsed["_source"], "kvproof");
        assert_eq!(parsed["message"], "hello");
    }

    #[test]
    fn test_loader_route_with_no_match_reports_the_catch_all_source() {
        let router = Router::new(&kvproof_routing(), &loader_destinations_config(), true);
        let payload = Bytes::from(r#"{"app":"something_else","message":"hello"}"#);

        let (route, source) = router.route_with_source(&payload);
        assert_destinations(&route, &["loader"]);
        assert_eq!(source.as_deref(), Some("main"));
    }

    #[test]
    fn test_an_unmatched_record_is_stamped_with_the_catch_all_source() {
        // The direct-to-loader route carries no topic, so without this stamp the
        // catch-all rows land with `_source` NULL.
        let router = Router::new(&kvproof_routing(), &loader_destinations_config(), true);
        let payload = Bytes::from(r#"{"app":"something_else","message":"hello"}"#);

        let (_, source) = router.route_with_source(&payload);
        let stamped = stamp_source(payload, &source.expect("the catch-all source"));
        let parsed: serde_json::Value = serde_json::from_slice(&stamped).expect("valid JSON");
        assert_eq!(parsed["_source"], "main");
        assert_eq!(parsed["message"], "hello");
    }

    #[test]
    fn test_a_sender_supplied_source_survives_the_catch_all_stamp() {
        // stamp_source leaves a top-level `_source` alone, so stamping every
        // unmatched record does not overwrite what the sender already set.
        let router = Router::new(&kvproof_routing(), &loader_destinations_config(), true);
        let payload = Bytes::from(r#"{"app":"something_else","_source":"crates_audit"}"#);

        let (_, source) = router.route_with_source(&payload);
        let stamped = stamp_source(payload, &source.expect("the catch-all source"));
        let parsed: serde_json::Value = serde_json::from_slice(&stamped).expect("valid JSON");
        assert_eq!(parsed["_source"], "crates_audit");
    }

    #[test]
    fn test_kafka_route_reports_the_matched_source_with_the_topic() {
        let router = Router::new(&kvproof_routing(), &default_destinations_config(), true);
        let payload = Bytes::from(r#"{"app":"kvproof","message":"hello"}"#);

        let (route, source) = router.route_with_source(&payload);
        assert_bus(&route, "kvproof_land");
        assert_eq!(source.as_deref(), Some("kvproof"));
    }

    #[test]
    fn test_route_with_source_is_silent_when_enrichment_is_disabled() {
        let router = Router::new(&kvproof_routing(), &loader_destinations_config(), false);
        let payload = Bytes::from(r#"{"app":"kvproof","message":"hello"}"#);

        assert_eq!(router.route_with_source(&payload).1, None);
    }

    #[test]
    fn test_stamp_source_leaves_an_existing_source_alone() {
        let payload = Bytes::from(r#"{"app":"kvproof","_source":"crates_audit"}"#);
        let stamped = stamp_source(payload.clone(), "kvproof");
        assert_eq!(stamped, payload);
    }

    #[test]
    fn test_stamp_source_into_an_empty_object() {
        let stamped = stamp_source(Bytes::from("{}"), "kvproof");
        assert_eq!(&stamped[..], br#"{"_source":"kvproof"}"#);
    }

    #[test]
    fn test_stamp_source_escapes_the_name() {
        let stamped = stamp_source(Bytes::from(r#"{"a":1}"#), r#"we"ird\name"#);
        let parsed: serde_json::Value = serde_json::from_slice(&stamped).expect("valid JSON");
        assert_eq!(parsed["_source"], r#"we"ird\name"#);
    }

    #[test]
    fn test_stamp_source_leaves_a_non_object_payload_alone() {
        let payload = Bytes::from("not json at all");
        let stamped = stamp_source(payload.clone(), "kvproof");
        assert_eq!(stamped, payload);
    }

    #[test]
    fn test_stamp_source_ignores_a_nested_source() {
        let stamped = stamp_source(Bytes::from(r#"{"inner":{"_source":"nested"}}"#), "kvproof");
        let parsed: serde_json::Value = serde_json::from_slice(&stamped).expect("valid JSON");
        assert_eq!(parsed["_source"], "kvproof");
        assert_eq!(parsed["inner"]["_source"], "nested");
    }

    // --- Fetcher-origin sources: key_value_set on the top-level `_source` ---
    //
    // `field` is the bare key: `extract_field_cow` splits on '.' for nesting, so
    // a `_json.` prefix would look for a nested object that is not there.

    #[test]
    fn test_fetcher_source_rule_routes_to_its_own_topic() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "_source".to_string(),
                mode: "key_value_set".to_string(),
                match_value: Some("crates_audit".to_string()),
                source: Some("crates_audit".to_string()),
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload = Bytes::from(
            r#"{"crate":"dfe-fetcher","_timestamp_fetcher":1757000000000,"_source":"crates_audit","_source_fetcher":"crates_io.crates"}"#,
        );

        assert_bus(&router.route(&payload), "crates_audit_land");
    }

    #[test]
    fn test_fetcher_source_rule_is_an_allow_list() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "_source".to_string(),
                mode: "key_value_set".to_string(),
                match_value: Some("crates_audit".to_string()),
                source: Some("crates_audit".to_string()),
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload = Bytes::from(r#"{"_source":"someone_elses_table","data":"test"}"#);

        assert_bus(&router.route(&payload), "main_land");
    }

    #[test]
    fn test_fetcher_source_rule_needs_enrichment_enabled() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "_source".to_string(),
                mode: "key_value_set".to_string(),
                match_value: Some("crates_audit".to_string()),
                source: Some("crates_audit".to_string()),
            }],
            ..default_routing_config()
        };
        // enrichment_enabled is server.auth.include_common_header; false switches
        // every source rule off.
        let router = Router::new(&routing, &default_destinations_config(), false);
        let payload = Bytes::from(r#"{"_source":"crates_audit","data":"test"}"#);

        assert_bus(&router.route(&payload), "main_land");
    }

    #[test]
    fn test_loader_destination_wins_over_the_source_rule() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "_source".to_string(),
                mode: "key_value_set".to_string(),
                match_value: Some("crates_audit".to_string()),
                source: Some("crates_audit".to_string()),
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &loader_destinations_config(), true);
        let payload = Bytes::from(r#"{"_source":"crates_audit","data":"test"}"#);

        // No topic is computed on the loader route, so the loader picks the table
        // from `_source` in the payload.
        assert_destinations(&router.route(&payload), &["loader"]);
    }

    // --- First match wins ---

    #[test]
    fn test_source_rules_first_match_wins() {
        let routing = RoutingConfig {
            source_rules: vec![
                SourceRule {
                    field: "priority_source".to_string(),
                    mode: "key_value_use".to_string(),
                    match_value: None,
                    source: None,
                },
                SourceRule {
                    field: "_source".to_string(),
                    mode: "key_value_use".to_string(),
                    match_value: None,
                    source: None,
                },
            ],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload =
            Bytes::from(r#"{"priority_source": "high", "_source": "auth", "data": "test"}"#);

        assert_bus(&router.route(&payload), "high_land");
    }

    // --- Enrichment disabled ---

    #[test]
    fn test_source_rules_disabled_when_no_common_header() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "_source".to_string(),
                mode: "key_value_use".to_string(),
                match_value: None,
                source: None,
            }],
            ..default_routing_config()
        };
        // enrichment_enabled = false
        let router = Router::new(&routing, &default_destinations_config(), false);
        let payload = Bytes::from(r#"{"_source": "auth", "data": "test"}"#);

        assert_bus(&router.route(&payload), "main_land");
    }

    // --- source_to_topic remapping ---

    #[test]
    fn test_source_to_topic_remapping() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "_source".to_string(),
                mode: "key_value_use".to_string(),
                match_value: None,
                source: None,
            }],
            source_to_topic: [("auth".to_string(), "logs_auth".to_string())]
                .into_iter()
                .collect(),
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload = Bytes::from(r#"{"_source": "auth"}"#);

        assert_bus(&router.route(&payload), "logs_auth_land");
    }

    // --- Legacy compat ---

    #[test]
    fn test_legacy_compat_enabled() {
        let router = Router::new(
            &legacy_routing_config(),
            &default_destinations_config(),
            true,
        );
        let payload = Bytes::from(r#"{"event_category": "auth", "data": "test"}"#);

        assert_bus(&router.route(&payload), "auth_land");
    }

    #[test]
    fn test_legacy_compat_nested_field() {
        let router = Router::new(
            &legacy_routing_config(),
            &default_destinations_config(),
            true,
        );
        let payload =
            Bytes::from(r#"{"tags": {"event": {"category": "network"}}, "data": "test"}"#);

        assert_bus(&router.route(&payload), "network_land");
    }

    #[test]
    fn test_legacy_compat_disabled() {
        let router = Router::new(
            &default_routing_config(),
            &default_destinations_config(),
            true,
        );
        let payload = Bytes::from(r#"{"event_category": "auth", "data": "test"}"#);

        // No rules, no legacy compat → default source
        assert_bus(&router.route(&payload), "main_land");
    }

    #[test]
    fn test_legacy_compat_field_priority() {
        let router = Router::new(
            &legacy_routing_config(),
            &default_destinations_config(),
            true,
        );
        // Both legacy fields present - tags.event.category has priority (first rule)
        let payload = Bytes::from(
            r#"{"tags": {"event": {"category": "network"}}, "event_category": "auth"}"#,
        );

        assert_bus(&router.route(&payload), "network_land");
    }

    // --- Destination rules ---

    #[test]
    fn test_route_to_loader() {
        let destinations = DestinationsConfig {
            rules: vec![ConfigRule {
                match_field: "destination".to_string(),
                match_value: "direct".to_string(),
                destination: "loader".into(),
            }],
            ..DestinationsConfig::default()
        };

        let router = Router::new(&default_routing_config(), &destinations, true);
        let payload = Bytes::from(r#"{"destination": "direct", "data": "test"}"#);

        assert_destinations(&router.route(&payload), &["loader"]);
    }

    // --- Named destinations and fan-out ---

    fn app_rule(value: &str, destination: crate::config::DestinationRef) -> ConfigRule {
        ConfigRule {
            match_field: "app".to_string(),
            match_value: value.to_string(),
            destination,
        }
    }

    #[test]
    fn test_rules_route_to_their_own_named_destination() {
        let destinations = named_destinations_config(
            "loader",
            vec![
                app_rule("orders", "transform_orders".into()),
                app_rule("audit", "archiver".into()),
            ],
        );
        let router = Router::new(&default_routing_config(), &destinations, true);

        assert_destinations(
            &router.route(&Bytes::from(r#"{"app":"orders"}"#)),
            &["transform_orders"],
        );
        assert_destinations(
            &router.route(&Bytes::from(r#"{"app":"audit"}"#)),
            &["archiver"],
        );
        // Unmatched falls to the default.
        assert_destinations(
            &router.route(&Bytes::from(r#"{"app":"something_else"}"#)),
            &["loader"],
        );
    }

    #[test]
    fn test_a_rule_destination_list_fans_out() {
        let destinations = named_destinations_config(
            "loader",
            vec![app_rule(
                "orders",
                crate::config::DestinationRef::Many(vec![
                    "loader".to_string(),
                    "archiver".to_string(),
                ]),
            )],
        );
        let router = Router::new(&default_routing_config(), &destinations, true);

        let route = router.route(&Bytes::from(r#"{"app":"orders"}"#));
        assert_destinations(&route, &["loader", "archiver"]);
        assert_eq!(
            route.topic(),
            None,
            "no destination is on the bus, so no topic is computed"
        );
    }

    #[test]
    fn test_a_fan_out_that_includes_the_bus_still_carries_the_topic() {
        let destinations = named_destinations_config(
            "loader",
            vec![app_rule(
                "orders",
                crate::config::DestinationRef::Many(vec![
                    "kafka".to_string(),
                    "archiver".to_string(),
                ]),
            )],
        );
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "app".to_string(),
                mode: "key_value_use".to_string(),
                match_value: None,
                source: None,
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &destinations, true);

        let route = router.route(&Bytes::from(r#"{"app":"orders"}"#));
        assert_destinations(&route, &["kafka", "archiver"]);
        assert_eq!(route.topic(), Some("orders_land"));
    }

    // --- DLQ ---

    #[test]
    fn test_route_dlq() {
        let router = Router::new(
            &default_routing_config(),
            &default_destinations_config(),
            true,
        );

        match router.route_dlq("test error") {
            RouteResult::Dlq(topic) => assert_eq!(topic, "dfe_receiver_dlq"),
            RouteResult::Send { .. } => panic!("expected DLQ route"),
        }
    }

    #[test]
    fn test_route_dlq_disabled_fallback() {
        let routing = RoutingConfig {
            dlq: DlqConfig {
                enabled: false,
                ..DlqConfig::default()
            },
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);

        assert_bus(&router.route_dlq("test error"), "main_land");
    }

    // --- Edge cases ---

    #[test]
    fn test_route_with_escaped_string() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "event_category".to_string(),
                mode: "key_value_use".to_string(),
                match_value: None,
                source: None,
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload = Bytes::from(r#"{"event_category": "auth\"test", "data": "test"}"#);

        assert_bus(&router.route(&payload), "auth\"test_land");
    }

    #[test]
    fn test_route_with_non_string_value() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "event_category".to_string(),
                mode: "key_value_use".to_string(),
                match_value: None,
                source: None,
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        // Integer, null, boolean, object, array - all should fall back to default
        for payload_str in [
            r#"{"event_category": 123}"#,
            r#"{"event_category": null}"#,
            r#"{"event_category": true}"#,
            r#"{"event_category": {"nested": "v"}}"#,
            r#"{"event_category": ["auth"]}"#,
        ] {
            let payload = Bytes::from(payload_str);
            assert_bus(&router.route(&payload), "main_land");
        }
    }

    #[test]
    fn test_route_with_unicode() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "event_category".to_string(),
                mode: "key_value_use".to_string(),
                match_value: None,
                source: None,
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload = Bytes::from(r#"{"event_category": "日本語"}"#);

        assert_bus(&router.route(&payload), "日本語_land");
    }

    #[test]
    fn test_route_with_unicode_escaped() {
        let routing = RoutingConfig {
            source_rules: vec![SourceRule {
                field: "event_category".to_string(),
                mode: "key_value_use".to_string(),
                match_value: None,
                source: None,
            }],
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        let payload = Bytes::from(r#"{"event_category": "\u65e5\u672c\u8a9e"}"#);

        assert_bus(&router.route(&payload), "日本語_land");
    }

    #[test]
    fn test_route_nested_field_partial_path() {
        let routing = RoutingConfig {
            legacy_compat: true,
            ..default_routing_config()
        };
        let router = Router::new(&routing, &default_destinations_config(), true);
        // tags exists but tags.event doesn't - should fall back to event_category
        let payload = Bytes::from(r#"{"tags": {"other": "value"}, "event_category": "auth"}"#);

        assert_bus(&router.route(&payload), "auth_land");
    }
}
