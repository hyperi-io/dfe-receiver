// Project:   dfe-receiver
// File:      src/routing/mod.rs
// Purpose:   Message routing to topics/destinations
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

//! Message routing module.
//!
//! Routes messages to Kafka topics based on configurable field expressions
//! using zero-copy field extraction for maximum performance.

use std::borrow::Cow;

use bytes::Bytes;
use rustc_hash::FxHashMap;
use sonic_rs::{get_from_slice, JsonValueTrait, LazyValue};

use crate::config::{DestinationsConfig, RoutingConfig};

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
    /// Fields to check for topic (priority order).
    topic_fields: Vec<String>,
    /// Default topic.
    default_topic: String,
    /// Topic suffix (e.g., "_land").
    topic_suffix: String,
    /// Category to topic mapping.
    category_to_topic: FxHashMap<String, String>,
    /// DLQ topic.
    dlq_topic: String,
    /// DLQ enabled.
    dlq_enabled: bool,
    /// Default destination.
    default_destination: String,
    /// Destination routing rules.
    destination_rules: Vec<DestinationRule>,
}

/// Internal destination rule representation.
struct DestinationRule {
    match_field: String,
    match_value: String,
    destination: String,
}

impl Router {
    /// Create a new router from configuration.
    pub fn new(routing: &RoutingConfig, destinations: &DestinationsConfig) -> Self {
        // Convert HashMap to FxHashMap for faster lookups
        let category_to_topic: FxHashMap<String, String> = routing
            .category_to_topic
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
            topic_fields: routing.topic_fields.clone(),
            default_topic: routing.default_topic.clone(),
            topic_suffix: routing.topic_suffix.clone(),
            category_to_topic,
            dlq_topic: routing.dlq.topic.clone(),
            dlq_enabled: routing.dlq.enabled,
            default_destination: destinations.default.clone(),
            destination_rules,
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
            RouteResult::Kafka(format!("{}{}", self.default_topic, self.topic_suffix))
        }
    }

    /// Determine the destination (kafka or loader) based on rules.
    #[inline]
    fn determine_destination(&self, payload: &Bytes) -> &str {
        for rule in &self.destination_rules {
            if let Some(value) = self.extract_field_cow(payload, &rule.match_field) {
                if value.as_ref() == rule.match_value {
                    return &rule.destination;
                }
            }
        }
        &self.default_destination
    }

    /// Extract the topic from the payload.
    #[inline]
    fn extract_topic(&self, payload: &Bytes) -> String {
        // Try each topic field in priority order
        let category = self
            .topic_fields
            .iter()
            .find_map(|field| self.extract_field_cow(payload, field));

        let category = match category {
            Some(cat) => cat,
            None => return format!("{}{}", self.default_topic, self.topic_suffix),
        };

        // Check category to topic mapping
        let topic = self
            .category_to_topic
            .get(category.as_ref())
            .map(String::as_str)
            .unwrap_or(category.as_ref());

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
            topic_fields: vec![
                "tags.event.category".to_string(),
                "event_category".to_string(),
            ],
            default_topic: "unmatched".to_string(),
            topic_suffix: "_land".to_string(),
            category_to_topic: FxHashMap::default(),
            dlq_topic: "dlq_land".to_string(),
            dlq_enabled: true,
            default_destination: "kafka".to_string(),
            destination_rules: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DestinationRule as ConfigRule, DlqConfig};
    use std::collections::HashMap;

    fn default_routing_config() -> RoutingConfig {
        RoutingConfig {
            topic_fields: vec![
                "tags.event.category".to_string(),
                "event_category".to_string(),
            ],
            default_topic: "unmatched".to_string(),
            topic_suffix: "_land".to_string(),
            category_to_topic: HashMap::new(),
            dlq: DlqConfig {
                enabled: true,
                topic: "dlq_land".to_string(),
            },
        }
    }

    fn default_destinations_config() -> DestinationsConfig {
        DestinationsConfig {
            default: "kafka".to_string(),
            rules: vec![],
        }
    }

    #[test]
    fn test_route_with_category() {
        let router = Router::new(&default_routing_config(), &default_destinations_config());
        let payload = Bytes::from(r#"{"event_category": "auth", "data": "test"}"#);

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "auth_land"),
            _ => panic!("expected Kafka route"),
        }
    }

    #[test]
    fn test_route_with_nested_category() {
        let router = Router::new(&default_routing_config(), &default_destinations_config());
        let payload = Bytes::from(r#"{"tags": {"event": {"category": "network"}}, "data": "test"}"#);

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "network_land"),
            _ => panic!("expected Kafka route"),
        }
    }

    #[test]
    fn test_route_default_topic() {
        let router = Router::new(&default_routing_config(), &default_destinations_config());
        let payload = Bytes::from(r#"{"data": "test"}"#);

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "unmatched_land"),
            _ => panic!("expected Kafka route"),
        }
    }

    #[test]
    fn test_route_with_mapping() {
        let mut routing = default_routing_config();
        routing.category_to_topic.insert("auth".to_string(), "logs_auth".to_string());

        let router = Router::new(&routing, &default_destinations_config());
        let payload = Bytes::from(r#"{"event_category": "auth"}"#);

        match router.route(&payload) {
            RouteResult::Kafka(topic) => assert_eq!(topic, "logs_auth_land"),
            _ => panic!("expected Kafka route"),
        }
    }

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

        let router = Router::new(&default_routing_config(), &destinations);
        let payload = Bytes::from(r#"{"destination": "direct", "data": "test"}"#);

        assert_eq!(router.route(&payload), RouteResult::Loader);
    }

    #[test]
    fn test_route_dlq() {
        let router = Router::new(&default_routing_config(), &default_destinations_config());

        match router.route_dlq("test error") {
            RouteResult::Dlq(topic) => assert_eq!(topic, "dlq_land"),
            _ => panic!("expected DLQ route"),
        }
    }
}
