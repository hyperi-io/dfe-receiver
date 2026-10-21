// Project:   dfe-receiver
// File:      src/tenancy.rs
// Purpose:   Ingest principals and the orgs each may land records in
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Who sent a record, and which orgs its records may land in.
//!
//! Every batch reaches the pipeline with a [`Principal`]: the credential that
//! authenticated it, or the listener it arrived on when it carried none.
//! `ingest_bindings` binds credential ids and listeners to orgs, and the
//! routing clamp holds each record's `_org_id` to its principal's
//! [`Binding`]. A principal with no binding may assign the deployment's
//! default org alone.

use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::config::{IngestBindingsConfig, OrgBinding};

/// The listeners a binding can name, by the transport label each records under.
pub const LISTENERS: [&str; 11] = [
    "http",
    "grpc",
    "otlp",
    "splunk_hec",
    "prometheus_rw",
    "syslog",
    "fluent",
    "lumberjack",
    "gelf",
    "netflow",
    "sflow",
];

/// Longest credential id.
const MAX_CREDENTIAL_ID_LEN: usize = 128;

/// Who sent a batch of records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    /// A credential with this id authenticated the request.
    Credential(Arc<str>),
    /// A credential with no id authenticated the request: a bearer token
    /// listed without one, or an accepted header value.
    UnnamedCredential,
    /// The request carried no credential the receiver can name, so the
    /// listener it arrived on stands for its sender.
    Listener(&'static str),
}

/// Where a batch came from, as routing reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ingest {
    principal: Principal,
    listener_source: bool,
}

impl Ingest {
    /// Records their sender wrote, from `principal`.
    #[must_use]
    pub fn sent(principal: Principal) -> Self {
        Self {
            principal,
            listener_source: false,
        }
    }

    /// Records the listener built from its wire protocol, `_source` included,
    /// so that `_source` stands when no rule sets one.
    #[must_use]
    pub fn built(principal: Principal) -> Self {
        Self {
            principal,
            listener_source: true,
        }
    }

    /// Who sent the batch.
    #[must_use]
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Whether a record's own `_source` is the listener's, kept when no rule sets one.
    #[must_use]
    pub fn keeps_listener_source(&self) -> bool {
        self.listener_source
    }
}

/// The orgs one principal's records may land in, and the org they take otherwise.
#[derive(Debug, Clone)]
pub struct Binding {
    orgs: FxHashSet<String>,
    default_org: String,
}

impl Binding {
    fn new(orgs: impl IntoIterator<Item = String>, default_org: String) -> Self {
        let mut orgs: FxHashSet<String> = orgs.into_iter().collect();
        orgs.insert(default_org.clone());
        Self { orgs, default_org }
    }

    /// The orgs a record may land in, the default included.
    #[must_use]
    pub fn orgs(&self) -> &FxHashSet<String> {
        &self.orgs
    }

    /// The org a record takes when no rule picks one it may land in.
    #[must_use]
    pub fn default_org(&self) -> &str {
        &self.default_org
    }
}

/// Every principal's binding, resolved once per config.
#[derive(Debug, Clone)]
pub struct Bindings {
    credentials: FxHashMap<Box<str>, Binding>,
    listeners: FxHashMap<Box<str>, Binding>,
    unbound: Binding,
    /// Every org any binding names, so a record's org is held without a copy.
    names: FxHashSet<Box<str>>,
}

impl Bindings {
    /// The bindings `config` declares. A principal it does not bind may assign
    /// `default_org_id` alone. [`validate`] has refused a config that does not bind.
    #[must_use]
    pub fn new(config: &IngestBindingsConfig, default_org_id: &str) -> Self {
        let build = |binding: &OrgBinding| {
            let default_org = binding
                .default_org
                .clone()
                .or_else(|| binding.orgs.first().cloned())
                .unwrap_or_else(|| default_org_id.to_string());
            Binding::new(binding.orgs.iter().cloned(), default_org)
        };
        let credentials: FxHashMap<Box<str>, Binding> = config
            .credentials
            .iter()
            .map(|(id, binding)| (Box::from(id.as_str()), build(binding)))
            .collect();
        let listeners: FxHashMap<Box<str>, Binding> = config
            .listeners
            .iter()
            .map(|(name, binding)| (Box::from(name.as_str()), build(binding)))
            .collect();
        let unbound = Binding::new([], default_org_id.to_string());
        let names = credentials
            .values()
            .chain(listeners.values())
            .chain(std::iter::once(&unbound))
            .flat_map(|binding| binding.orgs.iter())
            .map(|org| Box::from(org.as_str()))
            .collect();
        Self {
            credentials,
            listeners,
            unbound,
            names,
        }
    }

    /// The binding `principal`'s records are held to.
    #[must_use]
    pub fn resolve(&self, principal: &Principal) -> &Binding {
        let bound = match principal {
            Principal::Credential(id) => self.credentials.get(id.as_ref()),
            Principal::Listener(name) => self.listeners.get(*name),
            Principal::UnnamedCredential => None,
        };
        bound.unwrap_or(&self.unbound)
    }

    /// `name` as the bindings hold it, `None` when no binding names it.
    #[must_use]
    pub fn org(&self, name: &str) -> Option<&str> {
        self.names.get(name).map(AsRef::as_ref)
    }
}

/// Whether `id` can name a credential: 1 to 128 letters, digits, `.`, `_` or
/// `-`, starting with a letter or digit. A `:` would split a token entry.
#[must_use]
pub fn is_credential_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_CREDENTIAL_ID_LEN
        && id.starts_with(|c: char| c.is_ascii_alphanumeric())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Refuse bindings that would not hold records where they read as holding them.
///
/// # Errors
///
/// A message naming the first binding with no org, an empty org, a default
/// outside its orgs, a malformed credential id, or a listener the receiver
/// does not run.
pub fn validate(config: &IngestBindingsConfig) -> Result<(), String> {
    for (id, binding) in &config.credentials {
        if !is_credential_id(id) {
            return Err(format!(
                "ingest_bindings.credentials.{id}: a credential id is 1 to \
                 {MAX_CREDENTIAL_ID_LEN} letters, digits, '.', '_' or '-', starting with a \
                 letter or digit"
            ));
        }
        validate_binding(&format!("ingest_bindings.credentials.{id}"), binding)?;
    }
    for (name, binding) in &config.listeners {
        if !LISTENERS.contains(&name.as_str()) {
            return Err(format!(
                "ingest_bindings.listeners.{name} names no listener; bind one of {}",
                LISTENERS.join(", ")
            ));
        }
        validate_binding(&format!("ingest_bindings.listeners.{name}"), binding)?;
    }
    Ok(())
}

fn validate_binding(scope: &str, binding: &OrgBinding) -> Result<(), String> {
    if binding.orgs.is_empty() {
        return Err(format!(
            "{scope}.orgs is empty; a binding names the orgs its records may land in"
        ));
    }
    if binding.orgs.iter().any(String::is_empty) {
        return Err(format!("{scope}.orgs holds an empty org id"));
    }
    if let Some(default_org) = &binding.default_org
        && !binding.orgs.contains(default_org)
    {
        return Err(format!(
            "{scope}.default_org '{default_org}' is not one of {scope}.orgs"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn binding(orgs: &[&str], default_org: Option<&str>) -> OrgBinding {
        OrgBinding {
            orgs: orgs.iter().map(ToString::to_string).collect(),
            default_org: default_org.map(ToString::to_string),
        }
    }

    fn config() -> IngestBindingsConfig {
        IngestBindingsConfig {
            credentials: BTreeMap::from([
                (
                    "acme-agents".to_string(),
                    binding(&["acme", "acme-dev"], None),
                ),
                ("beta".to_string(), binding(&["beta"], Some("beta"))),
            ]),
            listeners: BTreeMap::from([("syslog".to_string(), binding(&["netops"], None))]),
        }
    }

    #[test]
    fn a_bound_credential_holds_its_orgs_and_takes_the_first_as_default() {
        let bindings = Bindings::new(&config(), "default");
        let bound = bindings.resolve(&Principal::Credential(Arc::from("acme-agents")));
        assert_eq!(bound.default_org(), "acme");
        assert!(bound.orgs().contains("acme-dev"));
        assert!(!bound.orgs().contains("default"));
    }

    #[test]
    fn an_unbound_principal_may_assign_the_default_org_alone() {
        let bindings = Bindings::new(&config(), "default");
        for principal in [
            Principal::Credential(Arc::from("nobody")),
            Principal::UnnamedCredential,
            Principal::Listener("http"),
        ] {
            let unbound = bindings.resolve(&principal);
            assert_eq!(unbound.default_org(), "default", "{principal:?}");
            assert_eq!(unbound.orgs().len(), 1, "{principal:?}");
        }
    }

    #[test]
    fn a_listener_binding_covers_its_listener_only() {
        let bindings = Bindings::new(&config(), "default");
        assert_eq!(
            bindings
                .resolve(&Principal::Listener("syslog"))
                .default_org(),
            "netops"
        );
        assert_eq!(
            bindings
                .resolve(&Principal::Listener("fluent"))
                .default_org(),
            "default"
        );
    }

    #[test]
    fn every_bound_org_is_held_once() {
        let bindings = Bindings::new(&config(), "default");
        for org in ["acme", "acme-dev", "beta", "netops", "default"] {
            assert_eq!(bindings.org(org), Some(org));
        }
        assert_eq!(bindings.org("victim"), None);
    }

    #[test]
    fn a_binding_that_does_not_bind_is_refused() {
        let refused = [
            ("credentials", "acme", binding(&[], None)),
            ("credentials", "acme", binding(&["acme", ""], None)),
            ("credentials", "acme", binding(&["acme"], Some("beta"))),
            ("credentials", "acme:prod", binding(&["acme"], None)),
            ("credentials", "-acme", binding(&["acme"], None)),
            ("listeners", "syslogd", binding(&["acme"], None)),
        ];
        for (kind, key, bad) in refused {
            let mut config = IngestBindingsConfig::default();
            let map = if kind == "credentials" {
                &mut config.credentials
            } else {
                &mut config.listeners
            };
            map.insert(key.to_string(), bad);
            assert!(validate(&config).is_err(), "{kind}.{key} was accepted");
        }
        assert!(validate(&self::config()).is_ok());
    }

    #[test]
    fn every_listener_label_is_bindable() {
        for listener in LISTENERS {
            let config = IngestBindingsConfig {
                listeners: BTreeMap::from([(listener.to_string(), binding(&["a"], None))]),
                ..IngestBindingsConfig::default()
            };
            assert!(validate(&config).is_ok(), "{listener}");
        }
    }
}
