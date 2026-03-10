// Project:   dfe-receiver
// File:      src/routing/mod.rs
// Purpose:   Message routing to topics/destinations
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Message routing module.
//!
//! Routes messages to Kafka topics based on configurable field expressions
//! using zero-copy field extraction for maximum performance.

use std::borrow::Cow;

use bytes::Bytes;
use rustc_hash::FxHashMap;
use sonic_rs::{JsonValueTrait, LazyValue, get_from_slice};

use crate::config::{DestinationsConfig, RoutingConfig, SourceRule};

/// Routing result with destination and topic.
#[derive(Debug, Clone, PartialEq)]
pub enum RouteResult {
    /// Route to Kafka topic.
    Kafka(String),
    /// Route to dfe-loader.
    Loader,
    /// Route to DLQ.
    Dlq(String),
}

/// Router for determining message destinations and topics.
pub struct Router {
    /// Source rules (first match wins).
    source_rules: Vec<SourceRule>,
    /// Default source when no rule matches.
    default_source: String,
    /// Topic suffix (e.g., "_land").
    topic_suffix: String,
    /// Source-to-topic remapping.
    source_to_topic: FxHashMap<String, String>,
    /// DLQ topic.
    dlq_topic: String,
    /// DLQ enabled.
    dlq_enabled: bool,
    /// Default destination.
    default_destination: String,
    /// Destination routing rules.
    destination_rules: Vec<DestinationRule>,
    /// Whether enrichment (source rules) is enabled.
    enrichment_enabled: bool,
}

/// Internal destination rule representation.
struct DestinationRule {
    match_field: String,
    match_value: String,
    destination: String,
}

impl Router {
    /// Create a new router from configuration.
    pub fn new(
        routing: &RoutingConfig,
        destinations: &DestinationsConfig,
        enrichment_enabled: bool,
    ) -> Self {
        // Convert HashMap to FxHashMap for faster lookups
        let source_to_topic: FxHashMap<String, String> = routing
            .source_to_topic
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Convert destination rules
        let destination_rules = destinations
            .rules
            .iter()
            .map(|r| DestinationRule {
                match_field: r.match_field.clone(),
                match_value: r.match_value.clone(),
                destination: r.destination.clone(),
            })
            .collect();

        Self {
            source_rules: routing.effective_source_rules(),
            default_source: routing.default_source.clone(),
            topic_suffix: routing.topic_suffix.clone(),
            source_to_topic,
            dlq_topic: routing.dlq.topic.clone(),
            dlq_enabled: routing.dlq.enabled,
            default_destination: destinations.default.clone(),
            destination_rules,
            enrichment_enabled,
        }
    }

    /// Route a message to its destination.
    ///
    /// This is a HOT PATH function - uses zero-copy field extraction.
    #[inline]
    pub fn route(&self, payload: &Bytes) -> RouteResult {
        // Check destination rules first
        let destination = self.determine_destination(payload);

        if destination == "loader" {
            return RouteResult::Loader;
        }

        // Extract topic from payload
        let topic = self.extract_topic(payload);

        RouteResult::Kafka(topic)
    }

    /// Route a message to DLQ.
    #[inline]
    pub fn route_dlq(&self, _reason: &str) -> RouteResult {
        if self.dlq_enabled {
            RouteResult::Dlq(self.dlq_topic.clone())
        } else {
            // Fall back to default topic if DLQ disabled
            RouteResult::Kafka(format!("{}{}", self.default_source, self.topic_suffix))
        }
    }

    /// Determine the destination (kafka or loader) based on rules.
    #[inline]
    fn determine_destination(&self, payload: &Bytes) -> &str {
        for rule in &self.destination_rules {
            if let Some(value) = self.extract_field_cow(payload, &rule.match_field) && value.as_ref() == rule.match_value {
                return &rule.destination;
            }
        }
        &self.default_destination
    }

    /// Evaluate source rules against the payload (first match wins).
    ///
    /// Returns `None` when enrichment is disabled or no rule matches.
    #[inline]
    fn evaluate_source(&self, payload: &Bytes) -> Option<String> {
        if !self.enrichment_enabled {
            return None;
        }
        for rule in &self.source_rules {
            match rule.mode.as_str() {
                "key_present" => {
                    if self.extract_field_cow(payload, &rule.field).is_some() {
                        return rule.source.clone();
                    }
                }
                "key_value_set" => {
                    if let (Some(val), Some(match_val)) = (
                        self.extract_field_cow(payload, &rule.field),
                        &rule.match_value,
                    ) {
                        if val.as_ref() == match_val.as_str() {
                            return rule.source.clone();
                        }
                    }
                }
                "key_value_use" => {
                    if let Some(val) = self.extract_field_cow(payload, &rule.field) {
                        return Some(val.into_owned());
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Extract the topic from the payload using source rules.
    #[inline]
    fn extract_topic(&self, payload: &Bytes) -> String {
        let source = self
            .evaluate_source(payload)
            .unwrap_or_else(|| self.default_source.clone());

        let topic = self
            .source_to_topic
            .get(&source)
            .map(String::as_str)
            .unwrap_or(&source);

        format!("{topic}{}", self.topic_suffix)
    }

    /// Extract a field value using zero-copy when possible.
    ///
    /// Uses `Cow<str>` to avoid allocation for non-escaped strings.
    #[inline]
    fn extract_field_cow<'a>(&self, payload: &'a Bytes, field: &str) -> Option<Cow<'a, str>> {
        // Handle nested fields (dot notation)
        let lazy: LazyValue = if field.contains('.') {
            let parts: Vec<&str> = field.split('.').collect();
            get_from_slice(payload, parts.as_slice()).ok()?
        } else {
            get_from_slice(payload, [field].as_slice()).ok()?
        };

        // Check if it's a string
        if !lazy.is_str() {
            return None;
        }

        // Get the raw JSON text
        let raw_cow = lazy.as_raw_cow();

        match raw_cow {
            Cow::Borrowed(s) if s.len() >= 2 => {
                // Strip quotes from JSON string
                let inner = &s[1..s.len() - 1];
                if inner.contains('\\') {
                    // Has escapes - need to parse
                    lazy.as_str().map(|s| Cow::Owned(s.to_string()))
                } else {
                    // Zero-copy borrow
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
    fn default() -> Self {
        Self {
            source_rules: vec![],
            default_source: "default".to_string(),
            topic_suffix: "_land".to_string(),
            source_to_topic: FxHashMap::default(),
            dlq_topic: "dlq_land".to_string(),
            dlq_enabled: true,
            default_destination: "kafka".to_string(),
            destination_rules: vec![],
            enrichment_enabled: true,
        }
    }
}

#[cfg(test)]
#[allow(clippy::uninlined_format_args)]
mod tests {
    use super::*;
    use crate::config::{DestinationRule as ConfigRule, DlqConfig, SourceRule};
    use std::collections::HashMap;

    fn default_routing_config() -> RoutingConfig {
        RoutingConfig {
            source_rules: vec![],
            default_source: "default".to_string(),
            topic_suffix: "_land".to_string(),
            source_to_topic: HashMap::new(),
            legacy_compat: false,
            dlq: DlqConfig::default(),
        }
    }

    fn legacy_routing_config() -> RoutingConfig {
        RoutingConfig {
            legacy_compat: true,
            ..default_routing_config()
        }
    }

    fn default_destinations_config() -> DestinationsConfig {
        DestinationsConfig {
            default: "kafka".to_string(),
            rules: vec![],
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "default_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "auth_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "default_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "firewall_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "default_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "logs_syslog_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "default_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "high_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "default_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "logs_auth_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "auth_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "network_land"),
            _ => panic!("expected Kafka route"),
        }
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
        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "default_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "network_land"),
            _ => panic!("expected Kafka route"),
        }
    }

    // --- Destination rules ---

    #[test]
    fn test_route_to_loader() {
        let destinations = DestinationsConfig {
            default: "kafka".to_string(),
            rules: vec![ConfigRule {
                match_field: "destination".to_string(),
                match_value: "direct".to_string(),
                destination: "loader".to_string(),
            }],
        };

        let router = Router::new(&default_routing_config(), &destinations, true);
        let payload = Bytes::from(r#"{"destination": "direct", "data": "test"}"#);

        assert_eq!(router.route(&payload), RouteResult::Loader);
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
            RouteResult::Dlq(topic) => assert_eq!(topic, "dlq_land"),
            _ => panic!("expected DLQ route"),
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

        match router.route_dlq("test error") {
            RouteResult::Kafka(topic) => assert_eq!(topic, "default_land"),
            _ => panic!("expected Kafka fallback"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "auth\"test_land"),
            _ => panic!("expected Kafka route"),
        }
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
            match router.route(&payload) {
                RouteResult::Kafka(topic) => {
                    assert_eq!(topic, "default_land", "for payload: {payload_str}")
                }
                _ => panic!("expected Kafka route for payload: {payload_str}"),
            }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "日本語_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "日本語_land"),
            _ => panic!("expected Kafka route"),
        }
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

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "auth_land"),
            _ => panic!("expected Kafka route"),
        }
    }
}
