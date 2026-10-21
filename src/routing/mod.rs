// Project:   dfe-receiver
// File:      src/routing/mod.rs
// Purpose:   The rules, the tenancy clamp and the destinations for each record
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Message routing.
//!
//! The `rules` section is a scalo rule set that sets each record's `_source`,
//! then its `_org_id`, first match per field in declared order. The clamp then
//! holds `_org_id` to the orgs the sender's principal is bound to, so a record
//! lands only in an org its credential or listener may write. The `_source` a
//! row set, else `routing.default_source`, picks the record's destinations and
//! topic, and every header field is written back into the record in one pass.
//!
//! For `_org_id`, in order: the sender's own value under
//! `routing.source_org_id: allow`, the configured rows, then the binding's
//! default. The `_source` written: the configured rows, the listener's own
//! stamp on a record the listener built, then `routing.default_source`. A
//! listener's stamp alone never moves a record off the default topic, and any
//! other `_source` a record carries is the sender's, replaced and parked.

mod header;

use std::borrow::Cow;
use std::sync::Arc;

use bytes::Bytes;
use rustc_hash::FxHashMap;
use scalo::rules::{
    Batch, Clamp, CompileOptions, FieldPath, FieldSource, JsonPayload, Lookup, Outcome, RuleConfig,
    RuleSet, SetAction, TargetId,
};
use sonic_rs::get_from_slice;

pub use self::header::{Header, Write};
use crate::config::{Config, DestinationRef, DestinationsConfig, SourceOrgId};
use crate::error::{Error, Result};
use crate::tenancy::{Binding, Bindings, Ingest};

/// The field a record's source travels in, which dfe-loader picks the table from.
pub const SOURCE_FIELD: &str = "_source";

/// The field a record's tenant travels in.
pub const ORG_FIELD: &str = "_org_id";

/// When DFE first received the record, in epoch milliseconds.
pub const RECEIVED_FIELD: &str = "_timestamp_received";

/// The fields the rules set, in evaluation order, so an org row can test the
/// source the same pass chose.
pub const RULE_TARGETS: [&str; 2] = [SOURCE_FIELD, ORG_FIELD];

/// The row that keeps a sender's own `_org_id` under `routing.source_org_id: allow`.
pub const SENDER_ORG_RULE: &str = "receiver.sender-org-id";

/// How a record spells each header key, from one pass over its bytes with no parse.
///
/// Spelled with no escape, a key is its quoted name in the bytes, so a record
/// with no `\` that never quotes a name holds no such key at any depth and its
/// lookup is skipped: a lookup that misses builds an error, the dearest step
/// on the hot path. Any `\` sends every key to its lookup.
///
/// A lookup reads only the first of two keys of one name, so a value it finds
/// is the record's only copy just when the key is spelled [`once`](Self::once).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Spelled {
    escapes: bool,
    source: u32,
    org: u32,
    received: u32,
}

impl Spelled {
    fn of(raw: &[u8]) -> Self {
        let mut spelled = Self {
            escapes: raw.contains(&b'\\'),
            source: 0,
            org: 0,
            received: 0,
        };
        for (at, pair) in raw.windows(2).enumerate() {
            if pair == b"\"_" {
                let key = &raw[at + 1..];
                spelled.source += u32::from(quoted(key, SOURCE_FIELD));
                spelled.org += u32::from(quoted(key, ORG_FIELD));
                spelled.received += u32::from(quoted(key, RECEIVED_FIELD));
            }
        }
        spelled
    }

    /// Whether a key spelled `count` times may be in the record at all.
    fn may_hold(self, count: u32) -> bool {
        self.escapes || count > 0
    }

    /// Whether a key spelled `count` times is spelled once, and with no escape.
    fn once(self, count: u32) -> bool {
        !self.escapes && count == 1
    }
}

/// Whether `rest` opens with `name` and its closing quote.
fn quoted(rest: &[u8], name: &str) -> bool {
    rest.strip_prefix(name.as_bytes())
        .is_some_and(|after| after.first() == Some(&b'"'))
}

/// Remove a top-level `_org_id` the sender wrote, so only DFE's org rules pick the tenant.
///
/// A record without one, the common case, is returned untouched after one
/// pass over its bytes.
///
/// # Errors
///
/// [`Error::Validation`] when the record carries one and does not parse
/// whole, so it would reach the rules with a tenant the sender picked.
pub fn strip_org_id(payload: Bytes) -> Result<Bytes> {
    let spelled = Spelled::of(&payload);
    if !spelled.may_hold(spelled.org)
        || get_from_slice(payload.as_ref(), [ORG_FIELD].as_slice()).is_err()
    {
        return Ok(payload);
    }
    let mut value = serde_json::from_slice::<serde_json::Value>(&payload).map_err(|e| {
        Error::Validation(format!(
            "record carries an _org_id that cannot be removed: {e}"
        ))
    })?;
    let Some(object) = value.as_object_mut() else {
        return Err(Error::Validation(crate::validation::NOT_AN_OBJECT.into()));
    };
    object.remove(ORG_FIELD);
    metrics::counter!("receiver_source_org_id_stripped_total").increment(1);
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|e| Error::Validation(format!("record cannot be rewritten: {e}")))
}

/// The rule set `config` routes by.
///
/// It sets `_source` then `_org_id` whatever targets the config lists, and
/// under `routing.source_org_id: allow` a first `_org_id` row keeps the
/// sender's own value, ahead of every configured row.
///
/// # Errors
///
/// [`Error::Config`] naming every finding when the rows do not compile, or
/// when the config lists other targets.
pub fn compile_rules(config: &Config) -> Result<RuleSet> {
    let mut rules = config.rules.clone();
    if rules.targets.is_empty() {
        rules.targets = RULE_TARGETS.map(String::from).to_vec();
    } else if rules.targets != RULE_TARGETS {
        return Err(Error::Config(format!(
            "rules.targets is {:?}; the receiver sets {RULE_TARGETS:?}, in that order",
            rules.targets
        )));
    }
    let options = CompileOptions::from_cascade();
    let invalid = |e: scalo::rules::RulesError| Error::Config(format!("rules: {e}"));
    if config.routing.source_org_id == SourceOrgId::Allow {
        // Checked before the receiver's row goes in, so each finding names the operator's own row.
        let found = RuleSet::check(&rules, &options);
        if found.iter().any(scalo::rules::RuleDiagnostic::is_error) {
            return Err(invalid(scalo::rules::RulesError::Invalid(found)));
        }
        rules.rules.insert(
            0,
            RuleConfig::new(SENDER_ORG_RULE, ORG_FIELD, SetAction::from(ORG_FIELD)),
        );
    }
    RuleSet::compile_with(&rules, &options).map_err(invalid)
}

/// The named destinations a record is bound for.
///
/// Shared, not copied: the list is resolved once per source at config time, so
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

/// One record routed.
#[derive(Debug)]
pub struct Routed<'r> {
    /// Where it goes.
    pub route: RouteResult,
    /// The record with its header fields written.
    pub payload: Bytes,
    /// The org it lands in.
    pub org: &'r str,
}

/// A destination list resolved at config time, with whether any member is on
/// the bus and therefore needs a topic computed per record.
#[derive(Clone)]
struct ResolvedDestinations {
    names: Destinations,
    needs_topic: bool,
}

impl ResolvedDestinations {
    fn new(reference: &DestinationRef, destinations: &DestinationsConfig) -> Self {
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

/// The header fields the router reads from a record before the rules run.
struct Found<'j> {
    source: Lookup<'j>,
    /// Whether `source` is the record's only `_source`.
    source_once: bool,
    org: Lookup<'j>,
    /// Whether `org` is the record's only `_org_id`.
    org_once: bool,
    /// Whether the record already carries `_timestamp_received`.
    has_received: bool,
}

/// A record as the rules read it: the header fields the router already read,
/// and every other field from the bytes.
struct Record<'j> {
    json: &'j JsonPayload<'j>,
    found: &'j Found<'j>,
}

impl FieldSource for Record<'_> {
    fn lookup(&self, path: &FieldPath) -> Lookup<'_> {
        match path.as_str() {
            SOURCE_FIELD => self.found.source.clone(),
            ORG_FIELD => self.found.org.clone(),
            _ => self.json.lookup(path),
        }
    }

    fn cel_value(&self, name: &str) -> Option<cel::Value> {
        self.json.cel_value(name)
    }
}

/// The text a lookup found, when it found non-empty text.
fn text<'a>(found: &'a Lookup<'_>) -> Option<&'a str> {
    match found {
        Lookup::Text(text) if !text.is_empty() => Some(text.as_ref()),
        _ => None,
    }
}

/// Router: the compiled rules, the bindings they are clamped to, and the
/// destinations each source resolves to.
pub struct Router {
    rules: RuleSet,
    source: TargetId,
    org: TargetId,
    bindings: Bindings,
    source_path: FieldPath,
    org_path: FieldPath,
    received_path: FieldPath,
    /// Topic suffix (e.g., "_land").
    topic_suffix: Box<str>,
    /// `_source` for a record no rule matches.
    default_source: Box<str>,
    /// Pre-computed default topic (avoids format!() on every message).
    default_topic: String,
    /// Source-to-topic remapping (pre-computed with suffix).
    source_to_topic: FxHashMap<Box<str>, String>,
    /// DLQ topic.
    dlq_topic: String,
    /// DLQ enabled.
    dlq_enabled: bool,
    /// Destination for a record whose source names none of its own.
    default_destination: ResolvedDestinations,
    /// Destinations by the record's final `_source`.
    source_destinations: FxHashMap<Box<str>, ResolvedDestinations>,
    /// Whether `_source` and `_timestamp_received` are written.
    enrichment_enabled: bool,
    /// Whether a sender's `_org_id` is removed before routing.
    strips_org_id: bool,
}

impl Router {
    /// The router `config` describes, sending to `destinations`.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the rules do not compile.
    pub fn new(config: &Config, destinations: &DestinationsConfig) -> Result<Self> {
        let rules = compile_rules(config)?;
        for warning in rules.warnings() {
            tracing::warn!(rule_set = rules.name(), %warning, "rule set compiled with a warning");
        }
        let (Some(source), Some(org)) = (rules.target(SOURCE_FIELD), rules.target(ORG_FIELD))
        else {
            return Err(Error::Config(
                "rules: the set assigns neither _source nor _org_id".into(),
            ));
        };
        let path = |field: &str| {
            FieldPath::parse(field)
                .ok_or_else(|| Error::Config(format!("{field} is not a field path")))
        };
        let routing = &config.routing;
        Ok(Self {
            source,
            org,
            bindings: Bindings::new(&config.ingest_bindings, &routing.default_org_id),
            source_path: path(SOURCE_FIELD)?,
            org_path: path(ORG_FIELD)?,
            received_path: path(RECEIVED_FIELD)?,
            topic_suffix: routing.topic_suffix.as_str().into(),
            default_source: routing.default_source.as_str().into(),
            default_topic: format!("{}{}", routing.default_source, routing.topic_suffix),
            source_to_topic: routing
                .source_to_topic
                .iter()
                .map(|(k, v)| {
                    (
                        Box::from(k.as_str()),
                        format!("{v}{}", routing.topic_suffix),
                    )
                })
                .collect(),
            dlq_topic: routing.dlq.topic.clone(),
            dlq_enabled: routing.dlq.enabled,
            default_destination: ResolvedDestinations::new(&destinations.default, destinations),
            source_destinations: destinations
                .sources
                .iter()
                .map(|(name, reference)| {
                    (
                        Box::from(name.as_str()),
                        ResolvedDestinations::new(reference, destinations),
                    )
                })
                .collect(),
            enrichment_enabled: config.server.auth.include_common_header,
            strips_org_id: routing.source_org_id == SourceOrgId::Disallow,
            rules,
        })
    }

    /// Whether a sender's `_org_id` is removed before routing.
    #[must_use]
    pub fn strips_org_id(&self) -> bool {
        self.strips_org_id
    }

    /// The compiled rule set.
    #[must_use]
    pub fn rules(&self) -> &RuleSet {
        &self.rules
    }

    /// A tally of the rows that fire, added to the rule counters when it drops.
    /// Route a request's records through one, from this router.
    pub fn tally(&self) -> Batch<'_> {
        self.rules.batch()
    }

    /// Route one record from `ingest`: run the rules, hold its `_org_id` to
    /// the sender's binding, write its header fields, and pick its destinations.
    ///
    /// This is a HOT PATH function. `received_ms` is written as
    /// `_timestamp_received` when the record carries none. `tally` comes from
    /// [`tally`](Self::tally) on this router.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when a header field must be replaced and the
    /// record does not parse as a JSON object.
    pub fn route<'r>(
        &'r self,
        payload: Bytes,
        ingest: &Ingest,
        received_ms: u128,
        tally: &mut Batch<'r>,
    ) -> Result<Routed<'r>> {
        let bytes = payload.clone();
        let json = JsonPayload::trusted(&bytes);
        let found = self.found(&json, &bytes, self.enrichment_enabled);
        let binding = self.bindings.resolve(ingest.principal());
        let record = Record {
            json: &json,
            found: &found,
        };
        let clamp = Clamp::new(self.org, binding.orgs(), binding.default_org());
        let outcome = tally.evaluate_clamped(&record, &clamp);
        let org = self.final_org(&outcome, &found, binding);
        let assigned = outcome.get(self.source).map(|row| row.clone().into_value());
        // A row picks the destinations and topic, else the default does, whatever `_source` is written.
        let route = self.destination(
            self.enrichment_enabled
                .then(|| assigned.as_deref().unwrap_or(&self.default_source)),
        );
        let source = self
            .enrichment_enabled
            .then(|| self.final_source(assigned, &found, ingest));
        let header = Header {
            received_ms: (self.enrichment_enabled && !found.has_received).then_some(received_ms),
            source: source.as_deref().map_or(Write::Keep, |source| {
                Write::of(&found.source, found.source_once, source, true)
            }),
            org: Write::of(&found.org, found.org_once, org, false),
        };
        Ok(Routed {
            route,
            payload: header.write(payload)?,
            org,
        })
    }

    /// Hold one record's `_org_id` to `ingest`'s binding and write it, leaving
    /// every other field as it is: the org pass for a record bound for a topic
    /// its listener chose.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when the record holds another `_org_id` and does
    /// not parse as a JSON object.
    pub fn assign_org<'r>(
        &'r self,
        payload: Bytes,
        ingest: &Ingest,
        tally: &mut Batch<'r>,
    ) -> Result<(Bytes, &'r str)> {
        let bytes = payload.clone();
        let json = JsonPayload::trusted(&bytes);
        let found = self.found(&json, &bytes, false);
        let binding = self.bindings.resolve(ingest.principal());
        let record = Record {
            json: &json,
            found: &found,
        };
        let clamp = Clamp::new(self.org, binding.orgs(), binding.default_org());
        let outcome = tally.evaluate_clamped(&record, &clamp);
        let org = self.final_org(&outcome, &found, binding);
        let header = Header {
            received_ms: None,
            source: Write::Keep,
            org: Write::of(&found.org, found.org_once, org, false),
        };
        Ok((header.write(payload)?, org))
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

    /// The header fields of `json`, whose bytes are `raw`, routing reads.
    /// `_timestamp_received` only when `check_received`.
    fn found<'j>(&self, json: &'j JsonPayload<'_>, raw: &[u8], check_received: bool) -> Found<'j> {
        let spelled = Spelled::of(raw);
        Found {
            source: if spelled.may_hold(spelled.source) {
                json.lookup(&self.source_path)
            } else {
                Lookup::Missing
            },
            source_once: spelled.once(spelled.source),
            // The strip has removed the sender's, so there is nothing to read.
            org: if self.strips_org_id || !spelled.may_hold(spelled.org) {
                Lookup::Missing
            } else {
                json.lookup(&self.org_path)
            },
            org_once: spelled.once(spelled.org),
            has_received: check_received
                && spelled.may_hold(spelled.received)
                && !matches!(json.lookup(&self.received_path), Lookup::Missing),
        }
    }

    /// The org a record lands in: the clamped assignment, else the record's
    /// own value the clamp kept.
    fn final_org<'r>(
        &'r self,
        outcome: &Outcome<'_>,
        found: &Found<'_>,
        binding: &'r Binding,
    ) -> &'r str {
        outcome
            .value(self.org)
            .or_else(|| text(&found.org))
            .and_then(|org| self.bindings.org(org))
            .unwrap_or(binding.default_org())
    }

    /// The `_source` a record is written with: the row's, else the listener's
    /// own stamp when the record carries it, else the default. A value the
    /// listener did not stamp is the sender's, and is parked like one.
    fn final_source<'a>(
        &'a self,
        assigned: Option<Cow<'a, str>>,
        found: &Found<'_>,
        ingest: &Ingest,
    ) -> Cow<'a, str> {
        if let Some(assigned) = assigned {
            return assigned;
        }
        match ingest.listener_stamp() {
            Some(stamp) if text(&found.source) == Some(stamp) => Cow::Borrowed(stamp),
            _ => Cow::Borrowed(&self.default_source),
        }
    }

    /// Where a record routed as `source` goes, and its topic when a destination
    /// is on the bus. A record with no source (enrichment off) takes the defaults.
    fn destination(&self, source: Option<&str>) -> RouteResult {
        let destination = source
            .and_then(|s| self.source_destinations.get(s))
            .unwrap_or(&self.default_destination);
        let topic = destination.needs_topic.then(|| match source {
            Some(s) => self.source_to_topic.get(s).cloned().unwrap_or_else(|| {
                if s == &*self.default_source {
                    self.default_topic.clone()
                } else {
                    format!("{s}{}", self.topic_suffix)
                }
            }),
            None => self.default_topic.clone(),
        });
        RouteResult::Send {
            destinations: Arc::clone(&destination.names),
            topic,
        }
    }
}

#[cfg(test)]
#[allow(clippy::uninlined_format_args)]
mod tests {
    use std::collections::BTreeMap;

    use scalo::rules::MatchConfig;

    use super::*;
    use crate::config::{DlqConfig, IngestBindingsConfig, OrgBinding};
    use crate::tenancy::Principal;

    /// A fixed arrival time.
    const NOW: u128 = 1_760_000_000_000;

    fn listener(name: &'static str) -> Ingest {
        Ingest::sent(Principal::Listener(name))
    }

    fn credential(id: &str) -> Ingest {
        Ingest::sent(Principal::Credential(Arc::from(id)))
    }

    fn equals(id: &str, field: &str, value: &str, target: &str, set: &str) -> RuleConfig {
        RuleConfig::new(id, target, SetAction::value(set))
            .matching(MatchConfig::equals(field, value))
    }

    fn config_with(rows: Vec<RuleConfig>) -> Config {
        let mut config = Config::default();
        config.rules.rules = rows;
        config
    }

    fn router(config: &Config) -> Router {
        Router::new(config, &config.destinations).expect("router builds")
    }

    /// Route `record` and return its route, the record as written, and its org.
    fn route(
        router: &Router,
        ingest: &Ingest,
        record: &str,
    ) -> (RouteResult, serde_json::Value, String) {
        let mut tally = router.tally();
        let done = router
            .route(Bytes::from(record.to_string()), ingest, NOW, &mut tally)
            .expect("routes");
        let value = serde_json::from_slice(&done.payload).expect("valid JSON");
        (done.route, value, done.org.to_string())
    }

    fn bound(orgs: &[&str]) -> OrgBinding {
        OrgBinding {
            orgs: orgs.iter().map(ToString::to_string).collect(),
            default_org: None,
        }
    }

    fn with_bindings(mut config: Config) -> Config {
        config.ingest_bindings = IngestBindingsConfig {
            credentials: BTreeMap::from([
                ("acme-agents".to_string(), bound(&["acme", "acme-dev"])),
                ("beta-agents".to_string(), bound(&["beta"])),
            ]),
            listeners: BTreeMap::from([("syslog".to_string(), bound(&["netops"]))]),
        };
        config
    }

    // --- the default path ---

    #[test]
    fn an_unmatched_record_takes_the_catch_all_source_and_the_default_org() {
        let r = router(&Config::default());
        let (route, value, org) = route(&r, &listener("http"), r#"{"data":"test"}"#);
        assert_eq!(route.topic(), Some("main_land"));
        assert_eq!(value["_source"], "main");
        assert_eq!(value["_org_id"], "default");
        assert_eq!(value["_timestamp_received"], 1_760_000_000_000_u64);
        assert_eq!(org, "default");
    }

    #[test]
    fn a_matching_row_sets_the_source_and_its_topic() {
        let r = router(&config_with(vec![equals(
            "auth",
            "type",
            "auth",
            SOURCE_FIELD,
            "auth",
        )]));
        let (route, value, _) = route(&r, &listener("http"), r#"{"type":"auth"}"#);
        assert_eq!(route.topic(), Some("auth_land"));
        assert_eq!(value["_source"], "auth");
    }

    #[test]
    fn the_first_matching_row_in_declared_order_wins() {
        let rows = vec![
            equals("first", "type", "auth", SOURCE_FIELD, "first"),
            equals("second", "type", "auth", SOURCE_FIELD, "second"),
        ];
        let r = router(&config_with(rows));
        let (_, value, _) = route(&r, &listener("http"), r#"{"type":"auth"}"#);
        assert_eq!(value["_source"], "first");
    }

    // --- a sender's _source loses to the rules ---

    #[test]
    fn a_sender_source_the_rules_replace_is_parked() {
        let r = router(&config_with(vec![equals(
            "web",
            "host",
            "web-1",
            SOURCE_FIELD,
            "web",
        )]));
        let (_, value, _) = route(
            &r,
            &listener("http"),
            r#"{"host":"web-1","_source":"spoof"}"#,
        );
        assert_eq!(value["_source"], "web");
        assert_eq!(value["_source_original"], "spoof");
    }

    #[test]
    fn a_sender_source_no_rule_matches_takes_the_catch_all() {
        let r = router(&Config::default());
        let (route, value, _) = route(
            &r,
            &listener("http"),
            r#"{"_source":"someone_elses_table"}"#,
        );
        assert_eq!(value["_source"], "main");
        assert_eq!(value["_source_original"], "someone_elses_table");
        assert_eq!(route.topic(), Some("main_land"));
    }

    #[test]
    fn a_sender_source_the_rules_confirm_is_written_once_and_not_parked() {
        let r = router(&config_with(vec![equals(
            "crates",
            SOURCE_FIELD,
            "crates_audit",
            SOURCE_FIELD,
            "crates_audit",
        )]));
        let record = r#"{"_source":"crates_audit","_timestamp_received":1757000000000}"#;
        let mut tally = r.tally();
        let routed = r
            .route(Bytes::from(record), &listener("grpc"), NOW, &mut tally)
            .unwrap();
        let text = std::str::from_utf8(&routed.payload).unwrap();
        assert_eq!(text.matches("\"_source\"").count(), 1, "{text}");
        assert!(!text.contains("_source_original"), "{text}");
        assert_eq!(text.matches("\"_timestamp_received\"").count(), 1, "{text}");
        assert!(
            text.contains("1757000000000"),
            "the first hop's time stands: {text}"
        );
        assert_eq!(routed.route.topic(), Some("crates_audit_land"));
    }

    // --- a listener's own _source ---

    /// Records the syslog listener built, stamping `_source: syslog` itself.
    fn syslog() -> Ingest {
        Ingest::built(Principal::Listener("syslog"), "syslog")
    }

    #[test]
    fn a_listener_source_stands_on_the_default_topic_when_no_row_sets_one() {
        let r = router(&Config::default());
        let (route, value, org) = route(&r, &syslog(), r#"{"message":"m","_source":"syslog"}"#);
        assert_eq!(value["_source"], "syslog");
        assert!(value.get("_source_original").is_none());
        assert_eq!(route.topic(), Some("main_land"));
        assert_eq!(org, "default");
    }

    /// A built record's `_source` that is not the listener's stamp came from the
    /// sender, as a remote-write `_source` label does, and is parked like one.
    #[test]
    fn a_built_record_whose_source_is_not_the_stamp_is_treated_as_the_senders() {
        let prometheus = Ingest::built(Principal::Listener("prometheus_rw"), "prometheus");
        let r = router(&Config::default());
        let (to, value, _) = route(&r, &prometheus, r#"{"_source":"custom","value":1}"#);
        assert_eq!(value["_source"], "main");
        assert_eq!(value["_source_original"], "custom");
        assert_eq!(to.topic(), Some("main_land"));

        let (_, value, _) = route(&r, &prometheus, r#"{"_source":"prometheus","value":1}"#);
        assert_eq!(
            value["_source"], "prometheus",
            "the listener's own stamp stands"
        );
        assert!(value.get("_source_original").is_none());

        let r = router(&config_with(vec![equals(
            "job",
            "job",
            "node",
            SOURCE_FIELD,
            "node",
        )]));
        let (to, value, _) = route(&r, &prometheus, r#"{"_source":"custom","job":"node"}"#);
        assert_eq!(value["_source"], "node", "a matching row wins");
        assert_eq!(value["_source_original"], "custom");
        assert_eq!(to.topic(), Some("node_land"));
    }

    #[test]
    fn a_row_copying_the_listener_source_routes_by_it() {
        let copy = RuleConfig::new("own", SOURCE_FIELD, SetAction::from(SOURCE_FIELD));
        let r = router(&config_with(vec![copy]));
        let syslog = syslog();
        let (route, value, _) = route(&r, &syslog, r#"{"message":"m","_source":"syslog"}"#);
        assert_eq!(value["_source"], "syslog");
        assert_eq!(route.topic(), Some("syslog_land"));
    }

    #[test]
    fn a_row_that_matches_a_listener_record_overrides_its_source() {
        let r = router(&config_with(vec![equals(
            "sshd",
            "appname",
            "sshd",
            SOURCE_FIELD,
            "auth",
        )]));
        let (_, value, _) = route(&r, &syslog(), r#"{"appname":"sshd","_source":"syslog"}"#);
        assert_eq!(value["_source"], "auth");
        assert_eq!(value["_source_original"], "syslog");
    }

    // --- enrichment off ---

    #[test]
    fn with_enrichment_off_only_the_org_is_written() {
        let mut config = config_with(vec![equals("auth", "type", "auth", SOURCE_FIELD, "auth")]);
        config.server.auth.include_common_header = false;
        let r = router(&config);
        let (route, value, _) = route(&r, &listener("http"), r#"{"type":"auth"}"#);
        assert_eq!(
            value,
            serde_json::json!({"type": "auth", "_org_id": "default"})
        );
        assert_eq!(route.topic(), Some("main_land"));
    }

    // --- tenancy ---

    #[test]
    fn a_bound_credential_keeps_an_org_a_row_sets_inside_its_binding() {
        let r = router(&with_bindings(config_with(vec![equals(
            "dev", "env", "dev", ORG_FIELD, "acme-dev",
        )])));
        let (_, value, org) = route(&r, &credential("acme-agents"), r#"{"env":"dev"}"#);
        assert_eq!(value["_org_id"], "acme-dev");
        assert_eq!(org, "acme-dev");
    }

    #[test]
    fn a_bound_credential_cannot_land_a_record_outside_its_orgs() {
        let r = router(&with_bindings(config_with(vec![equals(
            "steal", "env", "x", ORG_FIELD, "beta",
        )])));
        let (_, value, org) = route(&r, &credential("acme-agents"), r#"{"env":"x"}"#);
        assert_eq!(value["_org_id"], "acme", "clamped to the binding's default");
        assert_eq!(org, "acme");
        let clamped: u64 = r
            .rules()
            .usage()
            .iter()
            .filter(|row| row.rule == "steal")
            .map(|row| row.clamped)
            .sum();
        assert_eq!(clamped, 1, "the clamp counts the row it overrode");
    }

    #[test]
    fn a_bound_credential_with_no_matching_row_takes_its_default() {
        let r = router(&with_bindings(Config::default()));
        let (_, value, _) = route(&r, &credential("beta-agents"), r#"{"a":1}"#);
        assert_eq!(value["_org_id"], "beta");
    }

    #[test]
    fn an_unbound_principal_lands_in_the_default_org_only() {
        let r = router(&with_bindings(config_with(vec![equals(
            "acme", "env", "x", ORG_FIELD, "acme",
        )])));
        for ingest in [
            credential("nobody"),
            Ingest::sent(Principal::UnnamedCredential),
            listener("http"),
        ] {
            let (_, value, _) = route(&r, &ingest, r#"{"env":"x"}"#);
            assert_eq!(value["_org_id"], "default", "{ingest:?}");
        }
    }

    #[test]
    fn a_listener_binding_holds_records_without_a_credential() {
        let r = router(&with_bindings(Config::default()));
        let (_, value, _) = route(&r, &syslog(), r#"{"_source":"syslog"}"#);
        assert_eq!(value["_org_id"], "netops");
    }

    #[test]
    fn an_org_row_can_test_the_source_the_same_pass_chose() {
        let rows = vec![
            equals("web", "host", "web-1", SOURCE_FIELD, "web"),
            equals("web-org", SOURCE_FIELD, "web", ORG_FIELD, "acme-dev"),
        ];
        let r = router(&with_bindings(config_with(rows)));
        let (_, value, _) = route(&r, &credential("acme-agents"), r#"{"host":"web-1"}"#);
        assert_eq!(value["_source"], "web");
        assert_eq!(value["_org_id"], "acme-dev");
    }

    #[test]
    fn under_allow_the_senders_org_goes_first_and_is_clamped() {
        let mut config = with_bindings(config_with(vec![equals(
            "row", "env", "x", ORG_FIELD, "acme-dev",
        )]));
        config.routing.source_org_id = SourceOrgId::Allow;
        let r = router(&config);
        let acme = credential("acme-agents");
        let (_, value, _) = route(&r, &acme, r#"{"env":"x","_org_id":"acme"}"#);
        assert_eq!(value["_org_id"], "acme", "the sender's value beats the row");
        let (_, value, _) = route(&r, &acme, r#"{"env":"x","_org_id":"beta"}"#);
        assert_eq!(value["_org_id"], "acme", "outside the binding, the default");
        let text = r#"{"env":"y","_org_id":null}"#;
        let (_, value, _) = route(&r, &acme, text);
        assert_eq!(value["_org_id"], "acme", "a null org takes the default");
    }

    /// A key spelled with escapes is the same key once decoded, so it is clamped and written once.
    #[test]
    fn under_allow_an_escaped_org_key_is_clamped_and_written_once() {
        let mut config = with_bindings(Config::default());
        config.routing.source_org_id = SourceOrgId::Allow;
        let r = router(&config);
        let acme = credential("acme-agents");
        let mut tally = r.tally();
        let done = r
            .route(
                Bytes::from_static(b"{\"a\":1,\"\\u005forg_id\":\"beta\"}"),
                &acme,
                NOW,
                &mut tally,
            )
            .expect("routes");
        let text = std::str::from_utf8(&done.payload).expect("utf8");
        assert_eq!(done.org, "acme", "{text}");
        assert!(!text.contains("beta"), "the sender's org survived: {text}");
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["_org_id"], "acme", "{text}");
    }

    #[test]
    fn under_allow_an_unbound_sender_cannot_pick_an_org() {
        let mut config = with_bindings(Config::default());
        config.routing.source_org_id = SourceOrgId::Allow;
        let r = router(&config);
        let (_, value, _) = route(&r, &listener("http"), r#"{"_org_id":"acme"}"#);
        assert_eq!(value["_org_id"], "default");
    }

    #[test]
    fn under_allow_an_org_rule_error_names_the_operators_own_row() {
        let mut config = config_with(vec![RuleConfig::new(
            "bad",
            ORG_FIELD,
            SetAction::default(),
        )]);
        config.routing.source_org_id = SourceOrgId::Allow;
        let err = compile_rules(&config).expect_err("refused").to_string();
        assert!(err.contains("rules[0]"), "{err}");
    }

    // --- compile ---

    #[test]
    fn other_targets_are_refused() {
        let mut config = Config::default();
        config.rules.targets = vec![ORG_FIELD.to_string(), SOURCE_FIELD.to_string()];
        let err = compile_rules(&config).expect_err("refused").to_string();
        assert!(err.contains("rules.targets"), "{err}");
    }

    #[test]
    fn the_listed_targets_in_order_compile() {
        let mut config = Config::default();
        config.rules.targets = RULE_TARGETS.map(String::from).to_vec();
        assert!(compile_rules(&config).is_ok());
    }

    #[test]
    fn a_row_that_does_not_compile_is_refused_with_its_location() {
        let config = config_with(vec![
            RuleConfig::new("bad", SOURCE_FIELD, SetAction::value("x")).when("size("),
        ]);
        let err = compile_rules(&config).expect_err("refused").to_string();
        assert!(err.contains("rules[0].when"), "{err}");
    }

    // --- destinations ---

    fn named(default: &str) -> Config {
        let mut config = Config::default();
        config.destinations.default = default.into();
        for name in ["transform_orders", "archiver"] {
            config.destinations.named.insert(
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
        config
    }

    fn names(route: &RouteResult) -> Vec<&str> {
        route.destinations().iter().map(AsRef::as_ref).collect()
    }

    #[test]
    fn a_source_names_its_own_destinations() {
        let mut config = named("loader");
        config.rules.rules = vec![equals("orders", "app", "orders", SOURCE_FIELD, "orders")];
        config.destinations.sources = BTreeMap::from([(
            "orders".to_string(),
            DestinationRef::Many(vec!["transform_orders".into(), "archiver".into()]),
        )]);
        let r = router(&config);
        let (route_orders, _, _) = route(&r, &listener("http"), r#"{"app":"orders"}"#);
        assert_eq!(names(&route_orders), ["transform_orders", "archiver"]);
        assert_eq!(route_orders.topic(), None, "no destination is on the bus");
        let (other, _, _) = route(&r, &listener("http"), r#"{"app":"other"}"#);
        assert_eq!(names(&other), ["loader"]);
    }

    #[test]
    fn a_fan_out_that_includes_the_bus_carries_the_topic() {
        let mut config = named("loader");
        config.rules.rules = vec![equals("orders", "app", "orders", SOURCE_FIELD, "orders")];
        config.destinations.sources = BTreeMap::from([(
            "orders".to_string(),
            DestinationRef::Many(vec!["kafka".into(), "archiver".into()]),
        )]);
        let r = router(&config);
        let (orders, _, _) = route(&r, &listener("http"), r#"{"app":"orders"}"#);
        assert_eq!(names(&orders), ["kafka", "archiver"]);
        assert_eq!(orders.topic(), Some("orders_land"));
    }

    #[test]
    fn a_source_to_topic_entry_remaps_the_topic() {
        let mut config = config_with(vec![equals("auth", "type", "auth", SOURCE_FIELD, "auth")]);
        config.routing.source_to_topic = [("auth".to_string(), "logs_auth".to_string())].into();
        let r = router(&config);
        let (routed, _, _) = route(&r, &listener("http"), r#"{"type":"auth"}"#);
        assert_eq!(routed.topic(), Some("logs_auth_land"));
    }

    #[test]
    fn test_route_dlq() {
        let r = router(&Config::default());
        match r.route_dlq("test error") {
            RouteResult::Dlq(topic) => assert_eq!(topic, "dfe_receiver_dlq"),
            RouteResult::Send { .. } => panic!("expected DLQ route"),
        }
    }

    #[test]
    fn test_route_dlq_disabled_fallback() {
        let mut config = Config::default();
        config.routing.dlq = DlqConfig {
            enabled: false,
            ..DlqConfig::default()
        };
        let r = router(&config);
        assert_eq!(r.route_dlq("test error").topic(), Some("main_land"));
    }

    // --- the org pass alone ---

    #[test]
    fn the_org_pass_writes_the_org_and_leaves_the_rest() {
        let r = router(&with_bindings(Config::default()));
        let mut tally = r.tally();
        let (payload, org) = r
            .assign_org(
                Bytes::from(r#"{"_source":"runzero","a":1}"#),
                &credential("beta-agents"),
                &mut tally,
            )
            .unwrap();
        assert_eq!(org, "beta");
        assert_eq!(
            &payload[..],
            br#"{"_source":"runzero","a":1,"_org_id":"beta"}"#
        );
    }

    // --- strip ---

    #[test]
    fn test_strip_org_id_removes_only_the_top_level_key() {
        let stripped = strip_org_id(Bytes::from(
            r#"{"_org_id":"victim","inner":{"_org_id":"nested"},"n":1}"#,
        ))
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&stripped).expect("valid JSON");
        assert!(parsed.get("_org_id").is_none());
        assert_eq!(parsed["inner"]["_org_id"], "nested");
        assert_eq!(parsed["n"], 1);
    }

    #[test]
    fn test_strip_org_id_removes_a_key_spelled_with_escapes() {
        for record in [
            "{\"\\u005forg_id\":\"victim\",\"n\":1}",
            "{\"_org\\u005Fid\":\"victim\",\"n\":1}",
            "{\"\\u005f\\u006f\\u0072\\u0067\\u005f\\u0069\\u0064\":\"victim\",\"n\":1}",
        ] {
            let stripped = strip_org_id(Bytes::from(record)).unwrap();
            let parsed: serde_json::Value = serde_json::from_slice(&stripped).expect("valid JSON");
            assert!(
                parsed.get("_org_id").is_none(),
                "{record} kept it: {parsed}"
            );
            assert_eq!(parsed["n"], 1);
        }
    }

    /// A key is skipped only when no spelling of it can be in the bytes, and
    /// counts as spelled once only when it is quoted once with no escape anywhere.
    #[test]
    fn a_key_counts_as_spelled_wherever_it_could_be_one() {
        let none = Spelled::of(br#"{"org_id":"a","source":"b","_sourcey":1}"#);
        assert!(!none.may_hold(none.source) && !none.may_hold(none.org));
        assert!(!none.may_hold(none.received));

        let escaped = Spelled::of(b"{\"a\":\"\\u0041\",\"_org_id\":\"o\"}");
        assert!(escaped.may_hold(escaped.source), "an escape anywhere");
        assert!(
            !escaped.once(escaped.org),
            "an escape could spell a second key"
        );

        let nested = Spelled::of(br#"{"a":{"_org_id":"x"},"b":"x _source y"}"#);
        assert!(
            nested.may_hold(nested.org),
            "a nested key is left to the lookup"
        );
        assert!(
            !nested.may_hold(nested.source),
            "a name inside a longer string is no key"
        );

        let header = Spelled::of(br#"{"_source":"s","_timestamp_received":1,"_org_id":"o"}"#);
        assert!(header.once(header.source) && header.once(header.org));
        assert!(header.once(header.received));

        let twice = Spelled::of(br#"{"_org_id":"acme","_org_id":"beta"}"#);
        assert_eq!(twice.org, 2);
        assert!(!twice.once(twice.org));
    }

    /// Under `allow`, a record that spells `_org_id` twice, or with an escape,
    /// leaves with one `_org_id`, inside the binding, whichever copy came first.
    #[test]
    fn under_allow_a_duplicate_org_key_leaves_as_one_key_inside_the_binding() {
        let mut config = with_bindings(Config::default());
        config.routing.source_org_id = SourceOrgId::Allow;
        let r = router(&config);
        let acme = credential("acme-agents");
        for record in [
            r#"{"_org_id":"acme","_org_id":"beta","a":1}"#,
            r#"{"_org_id":"beta","_org_id":"acme","a":1}"#,
            "{\"_org_id\":\"acme\",\"\\u005forg_id\":\"beta\",\"a\":1}",
            "{\"\\u005forg_id\":\"beta\",\"_org_id\":\"acme\",\"a\":1}",
            "{\"_org_id\":\"ac\\u006de\",\"_org_id\":\"beta\"}",
            r#"{"_org_id":"acme","a":{"_org_id":"beta"}}"#,
        ] {
            let mut tally = r.tally();
            let routed = r
                .route(Bytes::from(record), &acme, NOW, &mut tally)
                .expect("routes");
            let orgs = top_level(&routed.payload, ORG_FIELD);
            assert_eq!(orgs.len(), 1, "{record} left as {orgs:?}");
            assert!(
                ["acme", "acme-dev"].contains(&orgs[0].as_str()),
                "{record} -> {orgs:?}"
            );
            let (out, org) = r
                .assign_org(Bytes::from(record), &acme, &mut tally)
                .expect("assigns");
            let orgs = top_level(&out, ORG_FIELD);
            assert_eq!(orgs, [org], "{record} on the org pass");
        }
    }

    /// Every top-level value of `key` in `record`, decoded, in the order written.
    fn top_level(record: &[u8], key: &str) -> Vec<String> {
        sonic_rs::to_object_iter(record)
            .map(|pair| pair.expect("a top-level field"))
            .filter(|(name, _)| name.as_ref() == key)
            .map(|(_, value)| sonic_rs::from_str::<String>(value.as_raw_str()).expect("a string"))
            .collect()
    }

    #[test]
    fn test_strip_org_id_leaves_a_record_without_one_byte_identical() {
        let payload = Bytes::from(r#"{"org_id":"legacy",  "n":1}"#);
        assert_eq!(strip_org_id(payload.clone()).unwrap(), payload);
    }

    /// A record whose `_org_id` the strip cannot remove is refused, never
    /// passed on with the sender's tenant.
    #[test]
    fn a_record_whose_org_cannot_be_stripped_is_refused() {
        let lone_surrogate = Bytes::from(r#"{"_org_id":"victim","s":"\ud800"}"#);
        assert!(strip_org_id(lone_surrogate).is_err());
    }

    #[test]
    fn test_router_strips_org_id_unless_allowed() {
        assert!(router(&Config::default()).strips_org_id());
        let mut allow = Config::default();
        allow.routing.source_org_id = SourceOrgId::Allow;
        assert!(!router(&allow).strips_org_id());
    }
}
