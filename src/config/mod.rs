// Project:   dfe-receiver
// File:      src/config/mod.rs
// Purpose:   Configuration loading and validation
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration management using scalo's 7-layer cascade.
//!
//! Priority (highest to lowest):
//! 1. CLI arguments
//! 2. Environment variables (DFE_RECEIVER_*)
//! 3. .env file
//! 4. settings.{env}.yaml
//! 5. settings.yaml
//! 6. defaults.yaml
//! 7. Hard-coded defaults

pub mod raw_capture;
mod shared;

pub use raw_capture::{OversizePolicy, RawCapture, RawCaptureConfig};
pub use shared::SharedConfig;

use std::collections::HashMap;

use scalo::SensitiveString;
use scalo::config::flat_env::{self, ApplyFlatEnv, Normalize};
use scalo::config::{self, ConfigOptions};
use scalo::transport::AcknowledgementsConfig;
use serde::{Deserialize, Serialize};

use scalo::scaling::{ScalingComponent, ScalingPressure, ScalingPressureConfig};

use crate::error::{Error, Result};

/// Environment variable prefix for configuration.
pub const ENV_PREFIX: &str = "DFE_RECEIVER";

/// Main configuration struct.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct Config {
    /// HTTP server configuration.
    pub server: ServerConfig,

    /// gRPC server configuration.
    pub grpc: GrpcConfig,

    /// OTLP receiver configuration.
    #[cfg(feature = "otlp")]
    pub otlp: OtlpConfig,

    /// Lumberjack v2 (Beats) receiver configuration.
    pub lumberjack: LumberjackConfig,

    /// Splunk HEC receiver configuration.
    pub splunk_hec: SplunkHecConfig,

    /// Syslog receiver configuration.
    pub syslog: SyslogConfig,

    /// Prometheus Remote Write receiver configuration.
    pub prometheus_rw: PrometheusRwConfig,

    /// Fluent Forward protocol receiver configuration.
    pub fluent: FluentConfig,

    /// GELF receiver configuration.
    pub gelf: GelfConfig,

    /// Flow (NetFlow v5/v9 + IPFIX + sFlow v5) receiver configuration.
    pub flow: crate::server::flow::config::FlowConfig,

    /// Generic authenticated webhook intake (`POST /webhook/{caller}`).
    pub webhook: WebhookConfig,

    /// Common raw-payload capture default, inherited by every transport that
    /// supports capture. Per-transport `raw_capture:` blocks override it
    /// field by field. See [`raw_capture`].
    pub raw_capture: RawCaptureConfig,

    /// Validation rules.
    pub validation: ValidationConfig,

    /// Routing configuration.
    pub routing: RoutingConfig,

    /// Destination configuration.
    pub destinations: DestinationsConfig,

    /// Kafka producer configuration.
    pub kafka: KafkaConfig,

    /// dfe-loader connection configuration.
    pub loader: LoaderConfig,

    /// Buffer and memory configuration.
    pub buffer: BufferConfig,

    /// Metrics configuration.
    pub metrics: MetricsConfig,

    /// Scaling pressure configuration for KEDA autoscaling.
    pub scaling: ScalingConfig,

    /// Periodic config reload interval in seconds (0 = disabled, SIGHUP only).
    #[serde(default)]
    pub config_reload_secs: u64,

    /// Path to the config file (set by loader, not deserialized).
    #[serde(skip)]
    pub config_path: Option<String>,

    /// Debug file sink -- writes all processed messages to a file.
    pub file_sink: FileSinkConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            grpc: GrpcConfig::default(),
            #[cfg(feature = "otlp")]
            otlp: OtlpConfig::default(),
            lumberjack: LumberjackConfig::default(),
            splunk_hec: SplunkHecConfig::default(),
            syslog: SyslogConfig::default(),
            prometheus_rw: PrometheusRwConfig::default(),
            fluent: FluentConfig::default(),
            gelf: GelfConfig::default(),
            flow: crate::server::flow::config::FlowConfig::default(),
            webhook: WebhookConfig::default(),
            raw_capture: RawCaptureConfig::default(),
            validation: ValidationConfig::default(),
            routing: RoutingConfig::default(),
            destinations: DestinationsConfig::default(),
            kafka: KafkaConfig::default(),
            loader: LoaderConfig::default(),
            buffer: BufferConfig::default(),
            metrics: MetricsConfig::default(),
            scaling: ScalingConfig::default(),
            config_reload_secs: 0,
            config_path: None,
            file_sink: FileSinkConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration with cascade: CLI -> ENV -> .env -> file -> defaults
    ///
    /// Priority (highest to lowest):
    /// 1. CLI arguments (handled by caller, merged after)
    /// 2. Environment variables (DFE_RECEIVER_ prefix)
    /// 3. .env file (loaded by dotenvy via scalo)
    /// 4. Config file (YAML)
    /// 5. Hard-coded defaults
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        // If an explicit config file is provided, load it directly
        if let Some(path) = config_path {
            init_cascade(path)?;
            return Self::load_from_file(path);
        }

        // Otherwise, use scalo's 7-layer cascade
        config::setup(ConfigOptions {
            env_prefix: ENV_PREFIX.to_string(),
            config_paths: Vec::new(),
            load_dotenv: true,
            ..Default::default()
        })
        .map_err(|e| Error::Config(format!("failed to setup config: {e}")))?;

        // Get the global config and unmarshal to our struct
        let cfg = config::get();

        // Unmarshal config -- warn and fall back to defaults on failure
        let mut config: Config = match cfg.unmarshal() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "config unmarshal failed, using defaults -- check YAML syntax");
                Config::default()
            }
        };

        // Store config path for reload support
        config.config_path = config_path.map(String::from);

        // Apply flat env var overrides (DFE_RECEIVER_*)
        config.apply_flat_env(ENV_PREFIX);
        config.normalize();

        Ok(config)
    }

    /// Load configuration from a YAML file directly (for backwards compatibility).
    pub fn load_from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("failed to read config file: {e}")))?;

        let mut config: Config = serde_yaml_ng::from_str(&content)?;
        config.config_path = Some(path.to_string());
        config.apply_flat_env(ENV_PREFIX);
        config.normalize();
        Ok(config)
    }

    /// Resolve a transport's raw-capture override against the common block.
    ///
    /// Called once per handler at construction, so the effective settings are
    /// computed in one place rather than re-derived on the hot path.
    pub fn raw_capture_for(&self, transport: &RawCaptureConfig) -> RawCapture {
        transport.resolve(&self.raw_capture)
    }

    /// The destination set with the built-in `loader` compiled into a declared
    /// destination, so every consumer sees one kind of destination.
    ///
    /// `loader.transport: kafka` is a bus destination with no fixed topic --
    /// the record lands on the topic its source resolves to, exactly as the
    /// built-in `kafka` destination does. `grpc` is a gRPC destination at the
    /// loader's endpoint. `memory` stays unresolved: it has no transport, so
    /// the pipeline discards what reaches it.
    #[must_use]
    pub fn resolved_destinations(&self) -> DestinationsConfig {
        let mut resolved = self.destinations.clone();
        let referenced = resolved
            .referenced_names()
            .any(|name| name == LOADER_DESTINATION);
        if !referenced || resolved.named.contains_key(LOADER_DESTINATION) {
            return resolved;
        }

        let spec = match self.loader.transport.as_str() {
            "kafka" => DestinationSpec {
                grpc: None,
                kafka: Some(KafkaDestination::default()),
            },
            "grpc" => DestinationSpec {
                grpc: Some(GrpcDestination {
                    endpoint: self.loader.effective_grpc_endpoint(),
                    ..GrpcDestination::default()
                }),
                kafka: None,
            },
            _ => return resolved,
        };
        resolved.named.insert(LOADER_DESTINATION.to_string(), spec);
        resolved
    }

    /// Whether some enabled listener holds its answer until every destination
    /// confirmed delivery.
    #[must_use]
    pub fn holds_answers(&self) -> bool {
        self.ack_capable_listeners().any(|acks| acks.enabled)
    }

    /// Whether some enabled listener answers once a record is queued, so each
    /// destination needs its buffer, and its spool when spillover is on.
    ///
    /// Syslog, GELF and flow carry no acknowledgement, so they always count.
    #[must_use]
    pub fn answers_at_enqueue(&self) -> bool {
        self.syslog.enabled
            || self.gelf.enabled
            || self.flow.enabled
            || self.flow.split.is_some()
            || self.ack_capable_listeners().any(|acks| !acks.enabled)
    }

    /// The `acknowledgements` section of every enabled listener that has one.
    fn ack_capable_listeners(&self) -> impl Iterator<Item = AcknowledgementsConfig> + '_ {
        #[cfg(feature = "otlp")]
        let otlp = self.otlp.enabled.then_some(self.otlp.acknowledgements);
        #[cfg(not(feature = "otlp"))]
        let otlp = None;
        [
            Some(self.server.acknowledgements),
            self.grpc.enabled.then_some(self.grpc.acknowledgements),
            otlp,
            self.lumberjack
                .enabled
                .then_some(self.lumberjack.acknowledgements),
            self.splunk_hec
                .enabled
                .then_some(self.splunk_hec.acknowledgements),
            self.prometheus_rw
                .enabled
                .then_some(self.prometheus_rw.acknowledgements),
            self.fluent.enabled.then_some(self.fluent.acknowledgements),
            self.webhook
                .enabled
                .then_some(self.webhook.acknowledgements),
        ]
        .into_iter()
        .flatten()
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        // Validate server config
        if self.server.bind_address.is_empty() {
            return Err(Error::Config("server.bind_address is required".into()));
        }

        if !LOADER_TRANSPORTS.contains(&self.loader.transport.as_str()) {
            return Err(Error::Config(format!(
                "loader.transport '{}' is not one of {}",
                self.loader.transport,
                DELIVERING_LOADER_TRANSPORTS.join(", ")
            )));
        }

        // The discard transport reads as a working config and delivers nothing,
        // so startup refuses it. A test that wants a brokerless pipeline builds
        // the Config directly and never comes through here.
        if self.loader.transport == DISCARD_TRANSPORT {
            return Err(Error::Config(format!(
                "loader.transport '{DISCARD_TRANSPORT}' accepts records and drops them; \
                 use one of {}",
                DELIVERING_LOADER_TRANSPORTS.join(", ")
            )));
        }

        // Every destination a rule names must resolve, or matched records have
        // nowhere to go and the failure only shows up under traffic.
        for (name, spec) in &self.destinations.named {
            match (&spec.grpc, &spec.kafka) {
                (Some(grpc), None) if grpc.endpoint.is_empty() => {
                    return Err(Error::Config(format!(
                        "destinations.{name}.grpc.endpoint is required"
                    )));
                }
                (Some(_), None) | (None, Some(_)) => {}
                _ => {
                    return Err(Error::Config(format!(
                        "destinations.{name} needs exactly one of grpc or kafka"
                    )));
                }
            }
        }
        for name in self.destinations.referenced_names() {
            if !self.destinations.named.contains_key(name)
                && name != BUS_DESTINATION
                && name != LOADER_DESTINATION
            {
                return Err(Error::Config(format!(
                    "destination '{name}' is not declared under destinations"
                )));
            }
        }

        // `loader` with `loader.transport: kafka` is a bus destination, so a
        // brokerless config that routes there is refused here rather than per
        // record under traffic.
        if self.resolved_destinations().uses_bus() && self.kafka.brokers.is_empty() {
            return Err(Error::Config(
                "kafka.brokers is required when a destination is on the bus \
                 (destinations, or loader.transport: kafka)"
                    .into(),
            ));
        }

        // Every Kafka client, the DLQ's included, is built only when brokers are set.
        if !self.kafka.brokers.is_empty() {
            self.kafka.to_scalo_kafka_config_for_producer()?;
        }

        self.validate_auth()?;
        self.validate_ip_filters()?;
        crate::server::client_ip::TrustedProxies::parse(&self.server.trusted_proxies)
            .map_err(|e| Error::Config(format!("server.trusted_proxies: {e}")))?;
        // A zero hold answers every held OTLP HTTP export as not confirmed.
        #[cfg(feature = "otlp")]
        if self.otlp.enabled
            && self.otlp.acknowledgements.enabled
            && self.otlp.http_max_hold_ms == 0
        {
            return Err(Error::Config(
                "otlp.http_max_hold_ms must be greater than zero while acknowledgements are on"
                    .into(),
            ));
        }
        self.webhook.validate()?;
        // The flow handler refuses this block at build, so it has to fail startup here.
        self.flow
            .validate()
            .map_err(|e| Error::Config(format!("flow: {e}")))?;
        // A caller's topic is a Kafka topic; without brokers every accepted
        // record would fail at delivery.
        if self.webhook.enabled && self.kafka.brokers.is_empty() {
            return Err(Error::Config(
                "webhook.enabled is true but kafka.brokers is empty -- every webhook \
                 caller delivers to a Kafka topic"
                    .into(),
            ));
        }

        // A rate of zero has no interval between replenishments, so the limiter
        // has nothing to build a quota from and the derivation would divide by
        // it. An operator who wants no requests through disables the listener.
        if self.server.rate_limit.enabled && self.server.rate_limit.requests_per_second == 0 {
            return Err(Error::Config(
                "server.rate_limit.requests_per_second is 0 -- the limiter replenishes one \
                 request every 1/requests_per_second of a second, which zero does not \
                 describe. Set a rate, or server.rate_limit.enabled: false"
                    .into(),
            ));
        }

        // Validate buffer config
        if self.buffer.pressure_threshold < 0.0 || self.buffer.pressure_threshold > 1.0 {
            return Err(Error::Config(
                "buffer.pressure_threshold must be between 0.0 and 1.0".into(),
            ));
        }

        // Validate spillover config
        if self.buffer.spillover.enabled {
            let pct = self.buffer.spillover.max_usage_percent;
            if pct <= 0.0 || pct > 1.0 {
                return Err(Error::Config(format!(
                    "buffer.spillover.max_usage_percent must be in (0.0, 1.0], got {pct}"
                )));
            }
        }

        Ok(())
    }

    /// Refuse an auth mode the listener that carries it does not enforce.
    ///
    /// The listeners below own an `auth:` block of the same shape, but they do
    /// not all read the same amount of it, and a mode a listener ignores is not
    /// a weaker door -- it is an open one that reads as shut. Each rule below
    /// names the code that does or does not run.
    fn validate_auth(&self) -> Result<()> {
        // A mode the parser does not recognise is refused on every listener,
        // running or not: it is a typo whichever block it sits in.
        known_mode("server", &self.server.auth)?;
        known_mode("grpc", &self.grpc.auth)?;
        #[cfg(feature = "otlp")]
        known_mode("otlp", &self.otlp.auth)?;
        known_mode("lumberjack", &self.lumberjack.auth)?;
        known_mode("splunk_hec", &self.splunk_hec.auth)?;
        known_mode("syslog", &self.syslog.auth)?;
        known_mode("prometheus_rw", &self.prometheus_rw.auth)?;

        // Always-on HTTP listener.
        cert_half_is_enforced("server", &self.server.auth, &self.server.tls)?;
        header_credentials_configured("server", &self.server.auth)?;

        // gRPC and OTLP authenticate through a tonic interceptor that only
        // calls validate_bearer_auth, so `header` and `both` never reach
        // validate_header_auth on those ports.
        if self.grpc.enabled {
            bearer_only("grpc", &self.grpc.auth)?;
            cert_half_is_enforced("grpc", &self.grpc.auth, &self.grpc.tls)?;
        }
        #[cfg(feature = "otlp")]
        if self.otlp.enabled {
            bearer_only("otlp", &self.otlp.auth)?;
            cert_half_is_enforced("otlp", &self.otlp.auth, &self.otlp.tls)?;
        }

        // Both run the shared token_auth_middleware, so every mode but the
        // certificate half is enforced.
        if self.splunk_hec.enabled {
            cert_half_is_enforced("splunk_hec", &self.splunk_hec.auth, &self.splunk_hec.tls)?;
            header_credentials_configured("splunk_hec", &self.splunk_hec.auth)?;
        }
        if self.prometheus_rw.enabled {
            cert_half_is_enforced(
                "prometheus_rw",
                &self.prometheus_rw.auth,
                &self.prometheus_rw.tls,
            )?;
            header_credentials_configured("prometheus_rw", &self.prometheus_rw.auth)?;
        }

        // Nothing in either module reads its auth block at all.
        if self.lumberjack.enabled {
            no_application_auth("lumberjack", &self.lumberjack.auth)?;
            unauthenticated_is_accepted(
                "lumberjack",
                "a Lumberjack frame",
                &self.lumberjack.tls,
                self.lumberjack.accept_unauthenticated,
            )?;
        }
        if self.syslog.enabled {
            no_application_auth("syslog", &self.syslog.auth)?;
            syslog_is_accepted(self.syslog.accept_unauthenticated)?;
        }
        if self.fluent.enabled {
            unauthenticated_is_accepted(
                "fluent",
                "a Forward frame",
                &self.fluent.tls,
                self.fluent.accept_unauthenticated,
            )?;
        }
        if self.gelf.enabled {
            unauthenticated_is_accepted(
                "gelf",
                "a GELF message",
                &self.gelf.tls,
                self.gelf.accept_unauthenticated,
            )?;
        }

        Ok(())
    }

    /// Refuse an IP filter that would not filter what it reads as filtering.
    ///
    /// Checked wherever the filter takes effect: `server.ip_filter` on every
    /// accept loop, a flow listener's own filter while that listener is on.
    fn validate_ip_filters(&self) -> Result<()> {
        ip_filter_parses("server.ip_filter", &self.server.ip_filter)?;
        if self.flow.enabled
            && let Some(filter) = &self.flow.ip_filter
        {
            ip_filter_parses("flow.ip_filter", filter)?;
        }
        if let Some(split) = &self.flow.split {
            for (side, listener) in [("netflow", &split.netflow), ("sflow", &split.sflow)] {
                if listener.enabled
                    && let Some(filter) = &listener.ip_filter
                {
                    ip_filter_parses(&format!("flow.split.{side}.ip_filter"), filter)?;
                }
            }
        }
        Ok(())
    }
}

/// Refuse an IP filter [`IpFilter::parse`](crate::server::ip_filter::IpFilter::parse)
/// rejects: an unknown mode, an entry that is not a CIDR, an empty allowlist.
fn ip_filter_parses(scope: &str, filter: &IpFilterConfig) -> Result<()> {
    crate::server::ip_filter::IpFilter::parse(filter)
        .map(drop)
        .map_err(|e| Error::Config(format!("{scope}: {e}")))
}

/// The five modes `AuthMode::parse` recognises.
const AUTH_MODES: [&str; 5] = ["none", "header", "bearer", "mtls", "both"];

/// Refuse a mode string outside [`AUTH_MODES`], an empty one included.
///
/// The request path refuses every request under a mode it cannot parse, so
/// a typo ("bearrer", "Bearer Token") would otherwise start a listener that
/// answers nobody.
fn known_mode(scope: &str, auth: &AuthConfig) -> Result<()> {
    if crate::server::auth::AuthMode::parse(&auth.mode).is_some() {
        return Ok(());
    }
    Err(Error::Config(format!(
        "{scope}.auth.mode is '{}', which is not one of {}",
        auth.mode,
        AUTH_MODES.join(", ")
    )))
}

/// Refuse `header` / `both` with no credential to compare a request against.
///
/// No header is accepted implicitly, so without `accepted_headers`, the
/// legacy `header_name` or a bearer token, every request is answered 500.
fn header_credentials_configured(scope: &str, auth: &AuthConfig) -> Result<()> {
    let mode = auth.mode.to_ascii_lowercase();
    if mode != "header" && mode != "both" {
        return Ok(());
    }
    let has_bearer = !auth.bearer.tokens.is_empty() || auth.bearer.secret_source.is_some();
    if has_bearer || !auth.effective_headers().is_empty() {
        return Ok(());
    }
    Err(Error::Config(format!(
        "{scope}.auth.mode is '{mode}' but {scope}.auth.accepted_headers is empty and no \
         bearer token is configured -- every request would be refused. List the \
         headers to accept in {scope}.auth.accepted_headers"
    )))
}

/// Whether the TLS handshake on a listener refuses a client with no certificate.
fn handshake_requires_a_client_certificate(tls: &TlsConfig) -> bool {
    tls.enabled
        && tls.client_auth.eq_ignore_ascii_case("required")
        && (tls.ca_file.is_some() || tls.ca_secret.is_some())
}

/// Refuse a listener whose wire protocol carries no credential unless its
/// clients are authenticated at the handshake or `accept_unauthenticated` is set.
fn unauthenticated_is_accepted(
    scope: &str,
    unit: &str,
    tls: &TlsConfig,
    accept_unauthenticated: bool,
) -> Result<()> {
    if accept_unauthenticated || handshake_requires_a_client_certificate(tls) {
        return Ok(());
    }
    Err(Error::Config(format!(
        "{scope} accepts any client that can reach its port -- {unit} carries no \
         credential. Authenticate clients at the handshake with {scope}.tls.enabled: \
         true, {scope}.tls.client_auth: required and {scope}.tls.ca_file (or \
         {scope}.tls.ca_secret) naming the CA their certificates are issued from, or \
         set {scope}.accept_unauthenticated: true and restrict the senders with \
         server.ip_filter.cidrs"
    )))
}

/// Refuse syslog unless `syslog.accept_unauthenticated` is set.
///
/// The UDP and plain TCP listeners bind whenever syslog is on, and neither has
/// a handshake, so client certificates on the TLS listener cannot close them.
fn syslog_is_accepted(accept_unauthenticated: bool) -> Result<()> {
    if accept_unauthenticated {
        return Ok(());
    }
    Err(Error::Config(
        "syslog accepts any sender that can reach its ports -- the UDP and plain TCP \
         listeners always bind and have no handshake to authenticate at, so \
         syslog.tls.client_auth: required closes only the TLS listener. Set \
         syslog.accept_unauthenticated: true and restrict the senders with \
         server.ip_filter.cidrs"
            .into(),
    ))
}

/// Require the TLS half of `mtls` / `both` to actually be armed.
///
/// Neither mode checks a certificate in the request path: `mtls` skips it
/// entirely (auth.rs `requires_token_auth` excludes it) and `both` runs only
/// its token half there. The certificate half is the TLS handshake, so without
/// `tls.enabled` and a REQUIRED client certificate it authenticates nobody
/// while reading as though it does. `optional` is not enough -- it validates a
/// certificate when one is offered and admits clients that offer none.
fn cert_half_is_enforced(scope: &str, auth: &AuthConfig, tls: &TlsConfig) -> Result<()> {
    let mode = auth.mode.to_ascii_lowercase();
    if mode != "mtls" && mode != "both" {
        return Ok(());
    }
    if !tls.enabled {
        return Err(Error::Config(format!(
            "{scope}.auth.mode is '{mode}' but {scope}.tls.enabled is false -- the \
             certificate half of that mode is enforced at the TLS handshake, so it \
             accepts every client unauthenticated"
        )));
    }
    if !tls.client_auth.eq_ignore_ascii_case("required") {
        return Err(Error::Config(format!(
            "{scope}.auth.mode is '{mode}' but {scope}.tls.client_auth is '{}' -- \
             it must be 'required', or clients presenting no certificate are \
             admitted",
            tls.client_auth
        )));
    }
    Ok(())
}

/// Refuse `header` and `both` on a listener whose interceptor is bearer-only.
///
/// `make_auth_interceptor` in server/grpc and server/otlp builds a HeaderMap
/// holding one key, `authorization`, and calls `validate_bearer_auth`. It never
/// calls `validate_header_auth`, so an `accepted_headers` list configured
/// against those ports is never consulted.
fn bearer_only(scope: &str, auth: &AuthConfig) -> Result<()> {
    let mode = auth.mode.to_ascii_lowercase();
    if mode != "header" && mode != "both" {
        return Ok(());
    }
    Err(Error::Config(format!(
        "{scope}.auth.mode is '{mode}' but the {scope} interceptor validates bearer \
         tokens only -- accepted_headers is never consulted on this listener. Use \
         'bearer', or 'mtls' with {scope}.tls.client_auth: required"
    )))
}

/// Refuse any real mode on a listener that has no application auth at all.
///
/// The lumberjack and syslog modules contain no reference to their `auth`
/// block: neither wire protocol carries a credential to check. A mode written
/// there changed nothing and reported nothing.
fn no_application_auth(scope: &str, auth: &AuthConfig) -> Result<()> {
    let mode = auth.mode.to_ascii_lowercase();
    if mode == "none" {
        return Ok(());
    }
    Err(Error::Config(format!(
        "{scope}.auth.mode is '{mode}' but the {scope} listener has no application \
         auth -- the wire protocol carries no credential and nothing reads this \
         field. Authenticate clients at the handshake instead, with \
         {scope}.tls.enabled: true and {scope}.tls.client_auth: required"
    )))
}

/// Set up scalo's cascade with the `--config` file as its settings layer.
///
/// The receiver reads its own sections from the file directly, but the sections
/// scalo's runtime owns (`version_check`, `metrics`, `scaling` and the rest)
/// resolve from the cascade alone. A reload re-enters here, and the cascade is
/// set once per process.
fn init_cascade(path: &str) -> Result<()> {
    let opts = ConfigOptions {
        env_prefix: ENV_PREFIX.to_string(),
        config_paths: vec![std::path::PathBuf::from(path)],
        // A working-tree `.env` must not reach a deployment's `--config` load.
        load_dotenv: false,
        ..Default::default()
    };
    match config::setup(opts) {
        Ok(()) | Err(config::ConfigError::AlreadyInitialised) => Ok(()),
        Err(e) => Err(Error::Config(format!("failed to setup config: {e}"))),
    }
}

/// Reload configuration from the same source.
///
/// Re-runs the full cascade (file + env overrides + validate) using
/// the original config path. Used for hot-reload via SIGHUP or periodic timer.
pub fn reload_config(current: &Config) -> Result<Config> {
    Config::load(current.config_path.as_deref())
}

impl ApplyFlatEnv for Config {
    fn apply_flat_env(&mut self, prefix: &str) {
        // Server
        if let Some(v) = flat_env::flat_env_string(prefix, "BIND_ADDRESS") {
            self.server.bind_address = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<usize>(prefix, "MAX_BODY_SIZE") {
            self.server.max_body_size = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<u64>(prefix, "REQUEST_TIMEOUT_MS") {
            self.server.request_timeout_ms = v;
        }
        if let Some(v) = flat_env::flat_env_bool(prefix, "COMMON_HEADER") {
            self.server.auth.include_common_header = v;
        }
        // Bearer tokens arrive as a mounted Secret in the pod environment, so
        // this is the route the chart takes -- the `auth` secret group in
        // deployment.rs declares exactly this name. Split on comma and newline,
        // matching how BearerTokenProvider::load_tokens parses a secret
        // payload, and read through the sensitive helper so no token reaches a
        // log line.
        if let Some(v) = flat_env::flat_env_string_sensitive(prefix, "BEARER_TOKENS") {
            self.server.auth.bearer.tokens = v
                .lines()
                .flat_map(|line| line.split(','))
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(SensitiveString::new)
                .collect();
        }

        // Kafka
        if let Some(v) = flat_env::flat_env_list(prefix, "KAFKA_BROKERS") {
            self.kafka.brokers = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_CLIENT_ID") {
            self.kafka.client_id = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_SASL_MECHANISM") {
            let sasl = self.kafka.sasl.get_or_insert_with(SaslConfig::default);
            sasl.mechanism = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_SECURITY_PROTOCOL") {
            self.kafka.tls.enabled = v.to_uppercase().contains("SSL");
        }
        if let Some(v) = flat_env::flat_env_string_sensitive(prefix, "KAFKA_SASL_USER") {
            let sasl = self.kafka.sasl.get_or_insert_with(SaslConfig::default);
            sasl.username = v;
        }
        if let Some(v) = flat_env::flat_env_string_sensitive(prefix, "KAFKA_SASL_PASSWORD") {
            let sasl = self.kafka.sasl.get_or_insert_with(SaslConfig::default);
            sasl.password = v.into();
        }

        // Routing
        if let Some(v) = flat_env::flat_env_string(prefix, "DEFAULT_SOURCE") {
            self.routing.default_source = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "TOPIC_SUFFIX") {
            self.routing.topic_suffix = v;
        }

        // DLQ (fleet-uniform names: DLQ_ENABLED / DLQ_TOPIC / DLQ_MODE)
        if let Some(v) = flat_env::flat_env_bool(prefix, "DLQ_ENABLED") {
            self.routing.dlq.enabled = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "DLQ_TOPIC") {
            self.routing.dlq.topic = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "DLQ_MODE") {
            self.routing.dlq.mode = v;
        }

        // Buffer
        if let Some(v) = flat_env::flat_env_parsed::<usize>(prefix, "MEMORY_LIMIT") {
            self.buffer.memory_limit = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<f64>(prefix, "PRESSURE_THRESHOLD") {
            self.buffer.pressure_threshold = v;
        }

        // Spillover
        if let Some(v) = flat_env::flat_env_bool(prefix, "BUFFER_SPILLOVER_ENABLED") {
            self.buffer.spillover.enabled = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "BUFFER_SPILLOVER_PATH") {
            self.buffer.spillover.path = std::path::PathBuf::from(v);
        }

        // Metrics
        if let Some(v) = flat_env::flat_env_string(prefix, "METRICS_ADDRESS") {
            self.metrics.address = v;
        }

        // Scaling
        if let Some(v) = flat_env::flat_env_bool(prefix, "SCALING_ENABLED") {
            self.scaling.enabled = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<f64>(prefix, "SCALING_MEMORY_GATE_THRESHOLD") {
            self.scaling.memory_gate_threshold = v;
        }

        // Config reload
        if let Some(v) = flat_env::flat_env_parsed::<u64>(prefix, "CONFIG_RELOAD_SECS") {
            self.config_reload_secs = v;
        }

        // Flow (NetFlow + sFlow)
        if let Some(v) = flat_env::flat_env_bool(prefix, "FLOW_ENABLED") {
            self.flow.enabled = v;
        }
        if let Some(v) = flat_env::flat_env_bool(prefix, "FLOW_EXPERIMENTAL") {
            self.flow.experimental = v;
        }
        if let Some(v) = flat_env::flat_env_list(prefix, "FLOW_PORTS") {
            self.flow.ports = v.iter().filter_map(|s| s.parse::<u16>().ok()).collect();
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "FLOW_OUTPUT_MODE") {
            // A typo'd mode must not silently pick an output shape. The YAML
            // path errors on an unknown variant; this one warns and keeps the
            // configured value, so both routes are loud.
            match crate::server::flow::config::OutputMode::parse(&v) {
                Some(mode) => self.flow.output.mode = mode,
                None => tracing::warn!(
                    mode = %v,
                    current = self.flow.output.mode.label(),
                    "unknown flow.output.mode, keeping current value \
                     (canonical_with_raw was removed -- use flow.raw_capture.enabled)"
                ),
            }
        }
        if let Some(v) = flat_env::flat_env_parsed::<usize>(prefix, "FLOW_RECV_BUFFER_BYTES") {
            self.flow.recv_buffer_bytes = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<usize>(prefix, "FLOW_CHANNEL_CAPACITY") {
            self.flow.channel_capacity = v;
        }
        if let Some(v) = flat_env::flat_env_bool(prefix, "FLOW_NETFLOW_ENABLED") {
            self.flow.netflow.enabled = v;
        }
        if let Some(v) = flat_env::flat_env_bool(prefix, "FLOW_SFLOW_ENABLED") {
            self.flow.sflow.enabled = v;
        }
        if let Some(v) = flat_env::flat_env_bool(prefix, "FLOW_RATE_LIMIT_ENABLED") {
            self.flow.rate_limit.enabled = v;
        }
        if let Some(v) =
            flat_env::flat_env_parsed::<u32>(prefix, "FLOW_RATE_LIMIT_PACKETS_PER_SECOND")
        {
            self.flow.rate_limit.packets_per_second = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<u32>(prefix, "FLOW_RATE_LIMIT_BURST") {
            self.flow.rate_limit.burst = v;
        }

        // Webhook intake. The caller table is structured and comes from the
        // config file; only the two switches an operator flips per deployment
        // are reachable from the environment.
        if let Some(v) = flat_env::flat_env_bool(prefix, "WEBHOOK_ENABLED") {
            self.webhook.enabled = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "WEBHOOK_BIND_ADDRESS") {
            self.webhook.bind_address = Some(v);
        }

        // Raw capture -- common block, then one override per capturing
        // transport. Both levels are reachable from the environment so a
        // container can turn capture on for a single transport without
        // shipping a config file.
        apply_raw_capture_env(&mut self.raw_capture, prefix, "RAW_CAPTURE");
        #[cfg(feature = "otlp")]
        apply_raw_capture_env(&mut self.otlp.raw_capture, prefix, "OTLP_RAW_CAPTURE");
        apply_raw_capture_env(
            &mut self.splunk_hec.raw_capture,
            prefix,
            "SPLUNK_HEC_RAW_CAPTURE",
        );
        apply_raw_capture_env(&mut self.syslog.raw_capture, prefix, "SYSLOG_RAW_CAPTURE");
        apply_raw_capture_env(
            &mut self.prometheus_rw.raw_capture,
            prefix,
            "PROMETHEUS_RW_RAW_CAPTURE",
        );
        apply_raw_capture_env(&mut self.fluent.raw_capture, prefix, "FLUENT_RAW_CAPTURE");
        apply_raw_capture_env(&mut self.gelf.raw_capture, prefix, "GELF_RAW_CAPTURE");
        apply_raw_capture_env(&mut self.flow.raw_capture, prefix, "FLOW_RAW_CAPTURE");
    }
}

/// Apply the three raw-capture keys for one cascade level.
///
/// `key` is the flat-env stem, e.g. `SYSLOG_RAW_CAPTURE`, giving
/// `DFE_RECEIVER_SYSLOG_RAW_CAPTURE_ENABLED` and friends. An absent variable
/// leaves the field unset, so it keeps inheriting rather than pinning a value.
fn apply_raw_capture_env(cfg: &mut RawCaptureConfig, prefix: &str, key: &str) {
    if let Some(v) = flat_env::flat_env_bool(prefix, &format!("{key}_ENABLED")) {
        cfg.enabled = Some(v);
    }
    if let Some(v) = flat_env::flat_env_parsed::<usize>(prefix, &format!("{key}_MAX_BYTES")) {
        cfg.max_bytes = Some(v);
    }
    if let Some(v) = flat_env::flat_env_string(prefix, &format!("{key}_ON_OVERSIZE")) {
        match OversizePolicy::parse(&v) {
            Some(policy) => cfg.on_oversize = Some(policy),
            // Silently defaulting to truncate would keep oversized payloads
            // an operator asked to drop, so say so and leave the setting be.
            None => tracing::warn!(
                key = %format!("{prefix}_{key}_ON_OVERSIZE"),
                value = %v,
                "unknown raw_capture.on_oversize, keeping current value (expected truncate|omit)"
            ),
        }
    }
}

impl Normalize for Config {
    fn normalize(&mut self) {
        // SASL credentials present -> enable SASL (regardless of how they arrived)
        if let Some(ref mut sasl) = self.kafka.sasl
            && (!sasl.username.is_empty() || !sasl.mechanism.is_empty())
        {
            sasl.enabled = true;
        }
    }
}

/// HTTP server configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct ServerConfig {
    /// Bind address (e.g., "0.0.0.0:443").
    pub bind_address: String,

    /// Maximum request body size in bytes.
    pub max_body_size: usize,

    /// Request timeout in milliseconds.
    pub request_timeout_ms: u64,

    /// Maximum concurrent in-flight requests (0 = unlimited).
    /// Protects against connection exhaustion and memory pressure.
    pub max_concurrent_requests: usize,

    /// Per-IP rate limiting for every HTTP listener.
    pub rate_limit: RateLimitConfig,

    /// Proxies, as CIDRs, whose `X-Forwarded-For` or `X-Real-IP` names the
    /// client on every HTTP listener. Empty (the default) believes neither
    /// header from anyone: a request is the TCP peer's.
    ///
    /// The rate limit keys on that client and auth-failure events report it.
    /// List only proxies that append `X-Forwarded-For` or overwrite
    /// `X-Real-IP`; a listed proxy that passes a client's own header through
    /// lets that client pick its rate-limit bucket.
    pub trusted_proxies: Vec<String>,

    /// IP filter (allowlist/denylist) for every listener the receiver owns an
    /// accept loop for.
    pub ip_filter: IpFilterConfig,

    /// TLS configuration.
    pub tls: TlsConfig,

    /// Authentication configuration.
    pub auth: AuthConfig,

    /// Answer `/ingest` only once every destination confirmed the records
    /// (default on). Off answers once they are queued.
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".to_string(),
            max_body_size: 10 * 1024 * 1024, // 10MB
            request_timeout_ms: 30_000,
            max_concurrent_requests: 10_000, // safe default for high-throughput ingest
            rate_limit: RateLimitConfig::default(),
            trusted_proxies: Vec::new(),
            ip_filter: IpFilterConfig::default(),
            tls: TlsConfig::default(),
            auth: AuthConfig::default(),
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

/// Per-IP rate limiting configuration using GCRA (token bucket variant).
///
/// Applies to `/ingest`, the webhook intake, Splunk HEC, Prometheus remote
/// write and OTLP HTTP. Each listener keeps its own budget, so the figures
/// below are per source IP per listener, not a receiver-wide total.
///
/// The source IP is the TCP peer, or the client a `server.trusted_proxies`
/// peer names. On Kubernetes a Service with `externalTrafficPolicy: Cluster`
/// (the default) rewrites every external source to a node address, so each
/// node's clients share one budget; set `externalTrafficPolicy: Local`, or
/// front the receiver with a proxy listed in `server.trusted_proxies`.
///
/// It cannot apply to the raw TCP and UDP listeners (syslog, Lumberjack, Fluent
/// Forward, GELF) or to the gRPC ports: there is no HTTP request there to count
/// and tonic owns its own accept loop. Flow has its own per-source packet limit
/// under `flow.rate_limit`.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct RateLimitConfig {
    /// Enable per-IP rate limiting.
    pub enabled: bool,

    /// Maximum sustained requests per second per source IP.
    ///
    /// A rate, not an interval: the limiter replenishes one request of the
    /// quota every `1/requests_per_second` of a second. At 100 that is one
    /// every 10ms. Must be at least 1 while `enabled` is true.
    pub requests_per_second: u64,

    /// Burst capacity above the sustained rate.
    pub burst: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            requests_per_second: 100,
            burst: 500,
        }
    }
}

/// IP filter (allowlist / denylist) configuration.
///
/// Enforced in the accept loop, before the TLS handshake and before any
/// protocol work: `/ingest`, the webhook intake, Splunk HEC, Prometheus remote
/// write, OTLP HTTP, syslog (UDP per datagram, TCP and TLS per connection),
/// Lumberjack, Fluent Forward and GELF.
///
/// It does not reach the gRPC ports (`grpc`, and OTLP on 4317): tonic owns
/// those accept loops, so authenticate them with `auth.mode: bearer`, or
/// `mtls` with `tls.client_auth: required`. Flow has its own optional
/// `flow.ip_filter` per listener.
///
/// UPGRADE: this list used to be `/ingest` and the webhook intake alone. One
/// filter now governs every listener, so an allowlist written for the `/ingest`
/// senders also decides which sources syslog, Lumberjack, Fluent Forward, GELF,
/// Splunk HEC, Prometheus remote write and OTLP HTTP are accepted from. Widen
/// `cidrs` to cover every sender, or those events are dropped in the accept
/// loop.
///
/// Each refusal counts on `receiver_ip_filter_rejected_total`, labelled with
/// the listener's transport.
///
/// Startup is refused for an unknown `mode`, an entry in `cidrs` that is not a
/// CIDR, or an `allowlist` with no `cidrs`.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct IpFilterConfig {
    /// Filter mode: "disabled", "allowlist", "denylist".
    pub mode: String,

    /// CIDR ranges (e.g., `["10.0.0.0/8", "192.168.0.0/16"]`).
    pub cidrs: Vec<String>,
}

impl Default for IpFilterConfig {
    fn default() -> Self {
        Self {
            mode: "disabled".to_string(),
            cidrs: Vec::new(),
        }
    }
}

/// TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct TlsConfig {
    /// Enable TLS.
    pub enabled: bool,

    /// Path to certificate file (local file path).
    pub cert_file: Option<String>,

    /// Path to private key file (local file path).
    pub key_file: Option<String>,

    /// Path to CA certificate for client verification (mTLS).
    pub ca_file: Option<String>,

    /// Client certificate requirement (none, optional, required).
    pub client_auth: String,

    /// Secret source for certificate (overrides cert_file).
    /// Format: "provider:path:key" (e.g., "vault:secret/tls:cert")
    pub cert_secret: Option<String>,

    /// Secret source for private key (overrides key_file).
    /// Format: "provider:path:key" (e.g., "vault:secret/tls:key")
    pub key_secret: Option<String>,

    /// Secret source for CA certificate (overrides ca_file).
    /// Format: "provider:path:key" (e.g., "vault:secret/tls:ca")
    pub ca_secret: Option<String>,

    /// Refresh interval for secrets in seconds.
    /// Default: 3600 (1 hour)
    pub refresh_interval_secs: u64,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_file: None,
            key_file: None,
            ca_file: None,
            client_auth: "none".to_string(),
            cert_secret: None,
            key_secret: None,
            ca_secret: None,
            refresh_interval_secs: 3600,
        }
    }
}

/// Authentication configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct AuthConfig {
    /// Auth mode (none, header, bearer, mtls, both).
    pub mode: String,

    /// List of accepted headers (any one must match).
    /// Each entry defines a header name and its allowed values.
    pub accepted_headers: Vec<AcceptedHeader>,

    /// Bearer token configuration.
    pub bearer: BearerConfig,

    /// Enrichment switch, read from `server.auth` alone: stamps
    /// `_timestamp_receiver` and evaluates `routing.source_rules` on routed
    /// records. It adds no accepted header. Default: true
    #[serde(default = "default_true")]
    pub include_common_header: bool,

    /// Legacy: Single header name for header-based auth.
    /// Deprecated: Use `accepted_headers` instead.
    #[serde(default)]
    pub header_name: String,

    /// Legacy: Allowed header values for single header.
    /// Deprecated: Use `accepted_headers` instead.
    #[serde(default)]
    pub header_values: Vec<SensitiveString>,
}

fn default_true() -> bool {
    true
}

/// Bearer token authentication configuration.
#[derive(Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct BearerConfig {
    /// Static tokens (for dev/simple deployments).
    /// In production, use `secret_source` instead.
    #[serde(default)]
    pub tokens: Vec<SensitiveString>,

    /// Secret source for dynamic token loading.
    /// Format: `provider:path[:key]` (e.g. "vault:secret/auth:bearer_tokens").
    pub secret_source: Option<String>,

    /// Token refresh interval in seconds (for secret-sourced tokens).
    /// Default: 300 (5 minutes)
    pub refresh_interval_secs: u64,
}

impl Default for BearerConfig {
    fn default() -> Self {
        Self {
            tokens: Vec::new(),
            secret_source: None,
            refresh_interval_secs: 300,
        }
    }
}

/// `config-check` prints this Debug dump, and scalo's masker does not match a
/// `tokens:` field name, so the values are redacted here.
impl std::fmt::Debug for BearerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BearerConfig")
            .field("tokens", &format_args!("<{} redacted>", self.tokens.len()))
            .field("secret_source", &self.secret_source)
            .field("refresh_interval_secs", &self.refresh_interval_secs)
            .finish()
    }
}

/// Defines an accepted authentication header.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AcceptedHeader {
    /// Header name (e.g., "x-hyperi-agent", "Authorization").
    pub name: String,

    /// Allowed values for this header.
    /// If empty, any non-empty value is accepted.
    #[serde(default)]
    pub values: Vec<SensitiveString>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: "none".to_string(),
            accepted_headers: vec![],
            bearer: BearerConfig::default(),
            include_common_header: true,
            // Legacy fields for backwards compatibility
            header_name: String::new(),
            header_values: Vec::new(),
        }
    }
}

impl AuthConfig {
    /// The headers header auth accepts: `accepted_headers` plus the legacy
    /// `header_name`. Nothing is added implicitly.
    pub fn effective_headers(&self) -> Vec<AcceptedHeader> {
        let mut headers = self.accepted_headers.clone();

        // Add legacy header if configured and not empty
        if !self.header_name.is_empty() {
            let already_exists = headers.iter().any(|h| h.name == self.header_name);
            if !already_exists {
                headers.push(AcceptedHeader {
                    name: self.header_name.clone(),
                    values: self.header_values.clone(),
                });
            }
        }

        headers
    }
}

/// gRPC server configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct GrpcConfig {
    /// Enable gRPC server.
    pub enabled: bool,

    /// Bind address for gRPC.
    pub bind_address: String,

    /// TLS configuration for gRPC server.
    pub tls: TlsConfig,

    /// Authentication configuration for gRPC server.
    pub auth: AuthConfig,

    /// Largest decoded push the server takes, in bytes.
    pub max_message_size: usize,

    /// Answer a push only once every destination confirmed its events
    /// (default on). Off answers once they are queued.
    pub acknowledgements: AcknowledgementsConfig,
}

/// The largest decoded gRPC message the receiver's gRPC listeners take.
pub const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:6000".to_string(),
            tls: TlsConfig::default(),
            auth: AuthConfig {
                mode: "none".to_string(),
                ..AuthConfig::default()
            },
            max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

/// OTLP (OpenTelemetry Protocol) receiver configuration.
#[cfg(feature = "otlp")]
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct OtlpConfig {
    /// Enable OTLP receiver.
    pub enabled: bool,

    /// Bind address for OTLP gRPC (standard port 4317).
    pub grpc_bind_address: String,

    /// Bind address for OTLP HTTP (standard port 4318).
    pub http_bind_address: String,

    /// Conversion mode: "hyperdx" (default) or "generic".
    ///
    /// - `hyperdx`: JSON matching the OTel ClickHouse exporter schema
    ///   for direct HyperDX compatibility.
    /// - `generic`: Normalised JSON envelope with routing fields.
    pub mode: String,

    /// TLS configuration for OTLP endpoints.
    pub tls: TlsConfig,

    /// Authentication configuration for OTLP endpoints.
    pub auth: AuthConfig,

    /// Raw-payload capture override (inherits the common `raw_capture` block).
    ///
    /// OTLP arrives as protobuf, so `_raw` carries the `generic`-mode
    /// rendering of the record -- the least-shaped decode we produce. In
    /// `generic` mode it is therefore a copy of the event itself.
    pub raw_capture: RawCaptureConfig,

    /// Largest decoded export the gRPC endpoint takes, in bytes.
    pub max_message_size: usize,

    /// Answer an export, on either endpoint, only once every destination
    /// confirmed its records (default on). Off answers once they are queued.
    pub acknowledgements: AcknowledgementsConfig,

    /// The longest an HTTP export is held for its destinations to confirm, in
    /// milliseconds. OTel exporters give up after 10 s by default, so a longer
    /// hold keeps the bytes of a request nobody is waiting on. Raise it with
    /// the exporters' `timeout`. A gRPC export is bounded by its own
    /// `grpc-timeout` instead.
    pub http_max_hold_ms: u64,
}

/// The OTLP HTTP hold: a second inside the OTel exporters' 10 s default timeout.
#[cfg(feature = "otlp")]
pub const DEFAULT_OTLP_HTTP_MAX_HOLD_MS: u64 = 9_000;

#[cfg(feature = "otlp")]
impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            grpc_bind_address: "0.0.0.0:4317".to_string(),
            http_bind_address: "0.0.0.0:4318".to_string(),
            mode: "hyperdx".to_string(),
            raw_capture: RawCaptureConfig::default(),
            tls: TlsConfig::default(),
            auth: AuthConfig {
                mode: "none".to_string(),
                ..AuthConfig::default()
            },
            max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
            acknowledgements: AcknowledgementsConfig::default(),
            http_max_hold_ms: DEFAULT_OTLP_HTTP_MAX_HOLD_MS,
        }
    }
}

/// Lumberjack v2 (Beats) protocol configuration.
///
/// Accepts data from Elastic Beats agents (Filebeat, Winlogbeat, etc.)
/// over the Lumberjack v2 wire protocol on TCP/TLS.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct LumberjackConfig {
    /// Enable Lumberjack/Beats receiver.
    pub enabled: bool,

    /// Bind address for Lumberjack TCP listener (standard port 5044).
    pub bind_address: String,

    /// TLS configuration (Beats clients typically require TLS).
    pub tls: TlsConfig,

    /// Authentication configuration.
    pub auth: AuthConfig,

    /// Start without client certificates, accepting every client that can
    /// reach the port. Without it, an enabled listener refuses to start unless
    /// `tls.client_auth` is `required`.
    pub accept_unauthenticated: bool,

    /// Acknowledge a window only once every destination confirmed its events
    /// (default on). Off acknowledges once they are queued.
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for LumberjackConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:5044".to_string(),
            tls: TlsConfig::default(),
            auth: AuthConfig {
                mode: "none".to_string(),
                ..AuthConfig::default()
            },
            accept_unauthenticated: false,
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

/// Splunk HEC (HTTP Event Collector) receiver configuration.
///
/// Accepts data from Splunk forwarders and HTTP clients over the HEC protocol.
/// Supports both `Authorization: Splunk <token>` and `Authorization: Bearer <token>`.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SplunkHecConfig {
    /// Enable Splunk HEC receiver.
    pub enabled: bool,

    /// Bind address for Splunk HEC HTTP listener (standard port 8088).
    pub bind_address: String,

    /// Maximum request body size in bytes.
    pub max_body_size: usize,

    /// Request timeout in milliseconds.
    pub request_timeout_ms: u64,

    /// TLS configuration.
    pub tls: TlsConfig,

    /// Authentication configuration.
    pub auth: AuthConfig,

    /// Raw-payload capture override (inherits the common `raw_capture` block).
    ///
    /// On `/services/collector/event`, `_raw` carries the submitted `event`
    /// value before HEC metadata is merged in. On `/services/collector/raw`
    /// it carries the original line bytes.
    pub raw_capture: RawCaptureConfig,

    /// Answer a request only once every destination confirmed its events
    /// (default on). Off answers once they are queued.
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for SplunkHecConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:8088".to_string(),
            max_body_size: 10 * 1024 * 1024,
            request_timeout_ms: 30_000,
            raw_capture: RawCaptureConfig::default(),
            tls: TlsConfig::default(),
            auth: AuthConfig {
                mode: "none".to_string(),
                ..AuthConfig::default()
            },
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

/// Syslog receiver configuration (RFC 5424 + RFC 3164).
///
/// Accepts syslog messages over UDP, TCP, and TLS/TCP.
/// Auto-detects message format (RFC 5424 vs RFC 3164) per message.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SyslogConfig {
    /// Enable syslog receiver.
    pub enabled: bool,

    /// Bind address for UDP listener (standard port 514).
    pub udp_bind_address: String,

    /// Bind address for TCP listener (standard port 514).
    pub tcp_bind_address: String,

    /// Bind address for TLS/TCP listener (standard port 6514, RFC 5425).
    pub tls_bind_address: String,

    /// Maximum syslog message size in bytes.
    pub max_message_size: usize,

    /// TLS configuration (for secure syslog on port 6514).
    pub tls: TlsConfig,

    /// Authentication configuration.
    pub auth: AuthConfig,

    /// Start the UDP and plain TCP listeners, which accept every sender that
    /// can reach them. Required whenever syslog is enabled: neither has a
    /// handshake to authenticate at.
    pub accept_unauthenticated: bool,

    /// Raw-payload capture override (inherits the common `raw_capture` block).
    ///
    /// The strongest case for capture: `_raw` holds the wire line including
    /// the PRI and header, none of which survives into the parsed envelope.
    pub raw_capture: RawCaptureConfig,
}

impl Default for SyslogConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            udp_bind_address: "0.0.0.0:514".to_string(),
            tcp_bind_address: "0.0.0.0:514".to_string(),
            tls_bind_address: "0.0.0.0:6514".to_string(),
            max_message_size: 64 * 1024,
            raw_capture: RawCaptureConfig::default(),
            tls: TlsConfig::default(),
            auth: AuthConfig {
                mode: "none".to_string(),
                ..AuthConfig::default()
            },
            accept_unauthenticated: false,
        }
    }
}

/// Prometheus Remote Write receiver configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct PrometheusRwConfig {
    /// Enable Prometheus Remote Write receiver.
    pub enabled: bool,

    /// Bind address for HTTP listener.
    pub bind_address: String,

    /// Output mode: "native" (default), "otel", or "hyperdx".
    /// - `native`: Flat JSON with labels as top-level fields.
    /// - `otel`: Generic OTel JSON envelope (snake_case, RFC 3339).
    /// - `hyperdx`: HyperDX ClickHouse-compatible JSON (PascalCase, DateTime64).
    pub mode: String,

    /// Maximum request body size in bytes.
    pub max_body_size: usize,

    /// Request timeout in milliseconds.
    pub request_timeout_ms: u64,

    /// TLS configuration.
    pub tls: TlsConfig,

    /// Authentication configuration.
    pub auth: AuthConfig,

    /// Raw-payload capture override (inherits the common `raw_capture` block).
    ///
    /// Remote Write arrives as snappy-framed protobuf, so `_raw` carries the
    /// `native`-mode rendering of the sample -- the least-shaped decode we
    /// produce. In `native` mode it is therefore a copy of the event itself.
    pub raw_capture: RawCaptureConfig,

    /// Answer a write only once every destination confirmed its samples
    /// (default on). Off answers once they are queued.
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for PrometheusRwConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:9091".to_string(),
            mode: "native".to_string(),
            raw_capture: RawCaptureConfig::default(),
            max_body_size: 10 * 1024 * 1024,
            request_timeout_ms: 30_000,
            tls: TlsConfig::default(),
            auth: AuthConfig {
                mode: "none".to_string(),
                ..AuthConfig::default()
            },
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

/// Fluent Forward protocol configuration.
///
/// Accepts data from Fluentd and Fluent Bit agents over the Forward
/// protocol (msgpack over TCP) on the standard port 24224.
///
/// There is no `auth` block: the Forward frames this handler reads carry no
/// credential. Close the port at the handshake with `tls.client_auth:
/// required`, or set `accept_unauthenticated` and restrict the senders with a
/// `server.ip_filter` allowlist.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct FluentConfig {
    /// Enable Fluent Forward receiver.
    pub enabled: bool,

    /// Bind address for TCP listener (standard port 24224).
    pub bind_address: String,

    /// Maximum message size in bytes.
    pub max_message_size: usize,

    /// TLS configuration.
    pub tls: TlsConfig,

    /// Start without client certificates, accepting every client that can
    /// reach the port. Without it, an enabled listener refuses to start unless
    /// `tls.client_auth` is `required`.
    pub accept_unauthenticated: bool,

    /// Raw-payload capture override (inherits the common `raw_capture` block).
    ///
    /// Forward arrives as msgpack, so `_raw` carries the record's verbatim
    /// msgpack-to-JSON decode, before the tag, timestamp and `_source` this
    /// handler adds.
    pub raw_capture: RawCaptureConfig,

    /// Acknowledge a chunk, or read past a message without one, only once
    /// every destination confirmed its records (default on). Off does so once
    /// they are queued.
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for FluentConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:24224".to_string(),
            max_message_size: 32 * 1024 * 1024,
            tls: TlsConfig::default(),
            accept_unauthenticated: false,
            raw_capture: RawCaptureConfig::default(),
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

/// GELF (Graylog Extended Log Format) receiver configuration.
///
/// Accepts GELF messages over TCP (null-byte delimited JSON)
/// on the standard port 12201.
///
/// There is no `auth` block: GELF has no in-protocol authentication. Close the
/// port at the handshake with `tls.client_auth: required`, or set
/// `accept_unauthenticated` and restrict the senders with a `server.ip_filter`
/// allowlist.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct GelfConfig {
    /// Enable GELF receiver.
    pub enabled: bool,

    /// Bind address for TCP listener (standard port 12201).
    pub bind_address: String,

    /// Maximum message size in bytes.
    pub max_message_size: usize,

    /// TLS configuration.
    pub tls: TlsConfig,

    /// Start without client certificates, accepting every client that can
    /// reach the port. Without it, an enabled listener refuses to start unless
    /// `tls.client_auth` is `required`.
    pub accept_unauthenticated: bool,

    /// Raw-payload capture override (inherits the common `raw_capture` block).
    ///
    /// `_raw` holds the GELF message exactly as it arrived, before the
    /// `message`, `severity` and `_source` fields this handler derives.
    pub raw_capture: RawCaptureConfig,
}

impl Default for GelfConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:12201".to_string(),
            max_message_size: 1024 * 1024,
            tls: TlsConfig::default(),
            accept_unauthenticated: false,
            raw_capture: RawCaptureConfig::default(),
        }
    }
}

/// Generic authenticated webhook intake.
///
/// One `POST /webhook/{caller}` route per declared caller. Each caller is a
/// product that pushes events (an alert rule, a SaaS notification hook) and
/// carries its own secret, its own topic and its own body shape, so callers
/// never share a credential and a wrong secret is only ever tried against the
/// caller the path names.
///
/// With `bind_address` unset the routes are served on the main ingest listener
/// under `server.tls`, `server.ip_filter` and `server.rate_limit`. Set it and
/// the intake gets its own listener under `webhook.tls`, so a deployment can
/// expose only this port to a product's egress and keep `/ingest` internal;
/// `server.ip_filter`, `server.rate_limit` and `server.max_concurrent_requests`
/// still apply to it.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct WebhookConfig {
    /// Enable the webhook intake.
    pub enabled: bool,

    /// Own listener address. Unset shares the main ingest listener.
    pub bind_address: Option<String>,

    /// Maximum request body size in bytes. Alerts are small; agents that post
    /// bulk data use `/ingest`, so this is deliberately far below
    /// `server.max_body_size`.
    pub max_body_size: usize,

    /// Request timeout in milliseconds, on either listener. It bounds the
    /// held answer, so the default keeps the hold past the Kafka message
    /// timeout.
    pub request_timeout_ms: u64,

    /// TLS for the own listener. Refused when `bind_address` is unset, since
    /// the shared listener is under `server.tls`.
    pub tls: TlsConfig,

    /// The callers, one entry per product.
    pub callers: Vec<WebhookCallerConfig>,

    /// Answer a request only once Kafka confirmed its records (default on).
    /// Off answers once they are queued.
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: None,
            max_body_size: 1024 * 1024,
            request_timeout_ms: 30_000,
            tls: TlsConfig::default(),
            callers: Vec::new(),
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

/// One webhook caller: a product identity, its secret, its topic.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct WebhookCallerConfig {
    /// The path segment (`POST /webhook/{name}`) and the `_source` stamped on
    /// every record. Lowercase letters, digits, `_` and `-` only.
    pub name: String,

    /// Kafka topic the caller's records land on, verbatim -- no
    /// `routing.topic_suffix` is applied.
    pub topic: String,

    /// How the caller authenticates.
    pub auth: WebhookAuthConfig,

    /// Whether a body is one record or a JSON array of records.
    pub body: WebhookBody,

    /// Optional CEL expression over the record; a false result drops it. An
    /// empty string keeps every record. Compiled once at load.
    pub filter: String,
}

impl Default for WebhookCallerConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            topic: String::new(),
            auth: WebhookAuthConfig::default(),
            body: WebhookBody::Single,
            filter: String::new(),
        }
    }
}

/// Per-caller authentication.
///
/// `hmac` is the strong mode: the product signs `"{timestamp}.{body}"` with
/// HMAC-SHA256 and the timestamp must be within `tolerance_secs` of the
/// receiver's clock, so a captured request cannot be replayed later. `header`
/// is for products that can only attach static headers to a webhook (runZero
/// alert rules are one): the named header carries a shared secret compared as
/// a SHA-256 hash, exactly as bearer tokens are. It has no integrity or replay
/// protection, which is why it is opt-in per caller rather than the default.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct WebhookAuthConfig {
    /// `hmac` or `header`.
    pub mode: WebhookAuthMode,

    /// Where the secret lives: `provider:path[:key]` as for bearer tokens
    /// (`file:`, `vault:` / `bao:` / `openbao:`, `env:`). Required. There is no
    /// static secret field on purpose: a secret in YAML surfaces in the
    /// config-schema dump and in every `config-check`.
    pub secret_source: String,

    /// Refresh interval for the secret in seconds.
    pub refresh_interval_secs: u64,

    /// `hmac`: the header carrying the signature (`sha256=<hex>` or bare
    /// hex). `header`: the header carrying the shared secret.
    pub header: String,

    /// `hmac` only: the header carrying the unix-seconds timestamp that was
    /// signed with the body.
    pub timestamp_header: String,

    /// `hmac` only: how far the signed timestamp may be from the receiver's
    /// clock, in seconds, in either direction.
    pub tolerance_secs: u64,
}

impl Default for WebhookAuthConfig {
    fn default() -> Self {
        Self {
            mode: WebhookAuthMode::Hmac,
            secret_source: String::new(),
            refresh_interval_secs: 300,
            header: "x-signature".to_string(),
            timestamp_header: "x-timestamp".to_string(),
            tolerance_secs: 300,
        }
    }
}

/// The two ways a webhook caller can prove itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum WebhookAuthMode {
    /// HMAC-SHA256 over `"{timestamp}.{body}"`, with a replay window.
    Hmac,
    /// A static header carrying a shared secret.
    Header,
}

/// What one POST body holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum WebhookBody {
    /// One JSON object, one record.
    Single,
    /// A JSON array; every element is a record.
    Array,
}

impl WebhookConfig {
    /// Refuse a webhook section that would start and then not do what it says.
    ///
    /// Only runs its checks when the intake is enabled -- a disabled block is
    /// inert, which is the one case where inert is honest.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.callers.is_empty() {
            return Err(Error::Config(
                "webhook.enabled is true but webhook.callers is empty -- the intake \
                 would answer 404 to everything"
                    .into(),
            ));
        }
        if self.bind_address.is_none() && self.tls.enabled {
            return Err(Error::Config(
                "webhook.tls.enabled is true but webhook.bind_address is unset -- the \
                 shared ingest listener is under server.tls, so this block would \
                 configure nothing. Set webhook.bind_address for an own TLS listener"
                    .into(),
            ));
        }
        if let Some(addr) = &self.bind_address
            && addr.parse::<std::net::SocketAddr>().is_err()
        {
            return Err(Error::Config(format!(
                "webhook.bind_address '{addr}' is not a socket address"
            )));
        }
        if self.max_body_size == 0 {
            return Err(Error::Config(
                "webhook.max_body_size must be greater than zero".into(),
            ));
        }

        let mut seen = std::collections::HashSet::new();
        for caller in &self.callers {
            let scope = format!("webhook.callers[{}]", caller.name);
            if caller.name.is_empty() {
                return Err(Error::Config("webhook.callers[].name is required".into()));
            }
            if !caller
                .name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
            {
                return Err(Error::Config(format!(
                    "{scope}.name must be lowercase letters, digits, '_' or '-' -- it is a \
                     path segment and the record's _source"
                )));
            }
            if !seen.insert(caller.name.as_str()) {
                return Err(Error::Config(format!(
                    "{scope} is declared twice -- one route cannot serve two callers"
                )));
            }
            if caller.topic.is_empty() {
                return Err(Error::Config(format!("{scope}.topic is required")));
            }
            if caller.auth.secret_source.is_empty() {
                return Err(Error::Config(format!(
                    "{scope}.auth.secret_source is required -- every caller carries its \
                     own secret, and there is no static secret field"
                )));
            }
            if caller.auth.header.is_empty() {
                return Err(Error::Config(format!("{scope}.auth.header is required")));
            }
            if http::HeaderName::from_bytes(caller.auth.header.as_bytes()).is_err() {
                return Err(Error::Config(format!(
                    "{scope}.auth.header '{}' is not a valid header name",
                    caller.auth.header
                )));
            }
            if caller.auth.mode == WebhookAuthMode::Hmac {
                if caller.auth.timestamp_header.is_empty() {
                    return Err(Error::Config(format!(
                        "{scope}.auth.timestamp_header is required in hmac mode -- the \
                         timestamp is what stops a captured request being replayed"
                    )));
                }
                if http::HeaderName::from_bytes(caller.auth.timestamp_header.as_bytes()).is_err() {
                    return Err(Error::Config(format!(
                        "{scope}.auth.timestamp_header '{}' is not a valid header name",
                        caller.auth.timestamp_header
                    )));
                }
                if caller.auth.tolerance_secs == 0 {
                    return Err(Error::Config(format!(
                        "{scope}.auth.tolerance_secs must be greater than zero"
                    )));
                }
            }
            if !caller.filter.trim().is_empty() {
                let errors = scalo::expression::validate(&caller.filter);
                if !errors.is_empty() {
                    return Err(Error::Config(format!(
                        "{scope}.filter is not a valid CEL expression: {}",
                        errors.join("; ")
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Validation configuration.
///
/// JSON is the only payload format: a body that is not JSON is refused with a
/// 400 and counted, never sent to the DLQ.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct ValidationConfig {
    /// Required fields (reject if missing).
    pub required_fields: Vec<String>,

    /// Send a record missing a required field to the DLQ instead of refusing it.
    pub dlq_on_invalid: bool,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            required_fields: vec![],
            dlq_on_invalid: true,
        }
    }
}

/// Rule for determining `_source` value from JSON payload.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SourceRule {
    /// JSON field path (dot notation for nested, e.g., "tags.event.category").
    pub field: String,

    /// Match mode: "key_present", "key_value_set", "key_value_use".
    ///
    /// - `key_present`: if field exists -> `_source = source`
    /// - `key_value_set`: if field value == `match_value` -> `_source = source`
    /// - `key_value_use`: if field exists -> `_source = <field value>`
    pub mode: String,

    /// Value to match against (for `key_value_set` mode only).
    #[serde(default)]
    pub match_value: Option<String>,

    /// Source value to set (for `key_present` and `key_value_set` modes).
    /// Ignored for `key_value_use` (uses the field value directly).
    #[serde(default)]
    pub source: Option<String>,
}

/// Routing configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct RoutingConfig {
    /// Rules for determining `_source` value (first match wins).
    /// Only evaluated when `include_common_header` is true.
    #[serde(default)]
    pub source_rules: Vec<SourceRule>,

    /// Default source when no rule matches.
    pub default_source: String,

    /// Suffix appended to source to form topic name.
    pub topic_suffix: String,

    /// Source-to-topic remapping (optional).
    #[serde(default)]
    pub source_to_topic: HashMap<String, String>,

    /// Enable pre-2.2 compatibility.
    /// Appends `key_value_use` rules for `tags.event.category` and `event_category`.
    #[serde(default)]
    pub legacy_compat: bool,

    /// DLQ configuration.
    pub dlq: DlqConfig,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            source_rules: vec![],
            default_source: "main".to_string(),
            topic_suffix: "_land".to_string(),
            source_to_topic: HashMap::new(),
            legacy_compat: false,
            dlq: DlqConfig::default(),
        }
    }
}

impl RoutingConfig {
    /// Get effective source rules, appending legacy compat rules if enabled.
    pub fn effective_source_rules(&self) -> Vec<SourceRule> {
        let mut rules = self.source_rules.clone();
        if self.legacy_compat {
            for field in &["tags.event.category", "event_category"] {
                if !rules.iter().any(|r| r.field == *field) {
                    rules.push(SourceRule {
                        field: field.to_string(),
                        mode: "key_value_use".to_string(),
                        match_value: None,
                        source: None,
                    });
                }
            }
        }
        rules
    }
}

/// DLQ (Dead Letter Queue) configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct DlqConfig {
    /// Enable DLQ.
    pub enabled: bool,

    /// Backend mode: cascade (default), fan_out, file_only, kafka_only.
    pub mode: String,

    /// DLQ topic name: when non-empty, every Kafka DLQ write routes here
    /// (routing=common). Empty selects per-destination `{dest}{topic_suffix}`
    /// routing -- reachable via config file only (flat-env drops empty values).
    pub topic: String,

    /// Topic suffix for per-table routing.
    pub topic_suffix: String,

    /// File backend settings.
    pub file_enabled: bool,
    pub file_path: String,

    /// Kafka backend settings.
    pub kafka_enabled: bool,

    /// How long a DLQ flush or shutdown waits for Kafka to ack queued entries,
    /// in milliseconds: scalo's `KafkaDlqConfig::send_timeout_ms`. Unset keeps
    /// scalo's default.
    pub kafka_send_timeout_ms: Option<u64>,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: "cascade".to_string(),
            topic: "dfe_receiver_dlq".to_string(),
            topic_suffix: ".dlq".to_string(),
            file_enabled: true,
            file_path: "/var/spool/dfe/dlq".to_string(),
            kafka_enabled: true,
            kafka_send_timeout_ms: None,
        }
    }
}

impl DlqConfig {
    /// Convert to scalo DlqConfig for the unified DLQ module.
    pub fn to_scalo_config(&self) -> scalo::dlq::DlqConfig {
        use scalo::dlq::{DlqMode, FileDlqConfig, KafkaDlqConfig};

        let mode = match self.mode.as_str() {
            "fan_out" => DlqMode::FanOut,
            "file_only" => DlqMode::FileOnly,
            "kafka_only" => DlqMode::KafkaOnly,
            "cascade" | "" => DlqMode::Cascade,
            other => {
                // A typo'd mode must not silently pick a backend -- cascade
                // includes the file backend, which is an EROFS no-op deployed.
                tracing::warn!(mode = %other, "unknown dlq.mode, using cascade");
                DlqMode::Cascade
            }
        };

        let kafka_defaults = KafkaDlqConfig::default();
        scalo::dlq::DlqConfig {
            enabled: self.enabled,
            mode,
            file: FileDlqConfig {
                enabled: self.file_enabled,
                path: self.file_path.clone().into(),
                ..FileDlqConfig::default()
            },
            kafka: KafkaDlqConfig {
                enabled: self.kafka_enabled,
                topic_suffix: self.topic_suffix.clone(),
                common_topic: self.topic.clone(),
                routing: if self.topic.is_empty() {
                    scalo::dlq::DlqRouting::PerTable
                } else {
                    scalo::dlq::DlqRouting::Common
                },
                send_timeout_ms: self
                    .kafka_send_timeout_ms
                    .unwrap_or(kafka_defaults.send_timeout_ms),
                ..kafka_defaults
            },
            ..scalo::dlq::DlqConfig::default()
        }
    }
}

impl KafkaConfig {
    /// Convert to scalo transport KafkaConfig with a given client ID suffix.
    fn to_scalo_config_with_suffix(&self, suffix: &str) -> scalo::transport::KafkaConfig {
        // Receiver's Kafka transports are produce-only (syslog -> Kafka). scalo
        // 2.9 dropped the explicit role field for a profile-based config: an
        // empty group (and no topics) means scalo builds no idle consumer (#44).
        let mut config = scalo::transport::KafkaConfig {
            brokers: self.brokers.clone(),
            client_id: format!("{}{}", self.client_id, suffix),
            group: String::new(),
            ..Default::default()
        };

        // SASL
        if let Some(ref sasl) = self.sasl
            && sasl.enabled
        {
            let protocol = if self.tls.enabled {
                "sasl_ssl"
            } else {
                "sasl_plaintext"
            };
            config.security_protocol = protocol.to_string();
            config.sasl_mechanism = Some(sasl.mechanism.to_uppercase());
            config.sasl_username = Some(sasl.username.clone());
            config.sasl_password = Some(sasl.password.clone());
        }

        // TLS
        if self.tls.enabled && self.sasl.as_ref().map_or(true, |s| !s.enabled) {
            config.security_protocol = "ssl".to_string();
        }
        config.ssl_ca_location = self.tls.ca_file.clone();
        config.ssl_certificate_location = self.tls.cert_file.clone();
        config.ssl_key_location = self.tls.key_file.clone();

        config.librdkafka_overrides = self.librdkafka_overrides.clone();

        config
    }

    /// The scalo client config for the main producer sink, checked as
    /// [`checked_client_config`] checks it.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the config names an unknown provider or a
    /// transport scalo refuses.
    pub fn to_scalo_kafka_config_for_producer(&self) -> Result<scalo::transport::KafkaConfig> {
        checked_client_config(self.to_scalo_config_with_suffix(""))
    }

    /// The scalo client config for the DLQ producer, checked as
    /// [`checked_client_config`] checks it.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the config names an unknown provider or a
    /// transport scalo refuses.
    pub fn to_scalo_kafka_config(&self) -> Result<scalo::transport::KafkaConfig> {
        checked_client_config(self.to_scalo_config_with_suffix("-dlq"))
    }
}

/// Apply `config`'s provider preset and refuse what scalo's `KafkaTransport`
/// refuses, before any client is built from it: a client built first could
/// send a PLAIN password in the clear before a later check refused it.
///
/// # Errors
///
/// [`Error::Config`] for an unknown provider, SASL PLAIN on anything but
/// `sasl_ssl` in any environment, and in production (`scalo::env::is_production`)
/// `ssl_skip_verify` or an unencrypted transport.
pub fn checked_client_config(
    mut config: scalo::transport::KafkaConfig,
) -> Result<scalo::transport::KafkaConfig> {
    config
        .apply_provider()
        .map_err(|e| Error::Config(format!("kafka: {e}")))?;
    config.validate(scalo::env::is_production()).map_err(|e| {
        Error::Config(format!(
            "{e} (the receiver sets security_protocol from kafka.tls.enabled and \
                 kafka.sasl.enabled)"
        ))
    })?;
    Ok(config)
}

/// The named destination set: where a matched record goes.
///
/// `kafka` and `loader` are always available without being declared -- the bus
/// (the record's own topic) and the app named by the `loader` block. Any other
/// name is declared here as a sibling key, so a match rule can send a record to
/// a transform's Push listener, or fan it out to several destinations at once:
///
/// ```yaml
/// destinations:
///   default: loader
///   rules:
///     - match_field: app
///       match_value: orders
///       destination: [transform-orders, archiver]
///   transform-orders:
///     grpc:
///       endpoint: "http://dfe-transform-orders:6000"
///   archiver:
///     grpc:
///       endpoint: "http://dfe-archiver:6000"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct DestinationsConfig {
    /// Destination for records no rule matches.
    pub default: DestinationRef,

    /// Routing rules for destination selection (first match wins).
    pub rules: Vec<DestinationRule>,

    /// Declared destinations, keyed by name.
    #[serde(flatten)]
    pub named: std::collections::HashMap<String, DestinationSpec>,
}

/// The bus destination, available without being declared.
pub const BUS_DESTINATION: &str = "kafka";

/// The loader destination, available without being declared: it takes its
/// transport and address from the `loader` config block.
pub const LOADER_DESTINATION: &str = "loader";

/// The transports `loader.transport` accepts.
pub const LOADER_TRANSPORTS: [&str; 3] = ["kafka", "grpc", DISCARD_TRANSPORT];

/// The transports that deliver a record somewhere.
pub const DELIVERING_LOADER_TRANSPORTS: [&str; 2] = ["kafka", "grpc"];

/// The transport that accepts a record and drops it. It exists so an ingest
/// test needs no broker; `Config::validate` refuses it, so it never starts.
pub const DISCARD_TRANSPORT: &str = "memory";

impl Default for DestinationsConfig {
    fn default() -> Self {
        Self {
            default: DestinationRef::from(BUS_DESTINATION),
            rules: vec![],
            named: std::collections::HashMap::new(),
        }
    }
}

impl DestinationsConfig {
    /// Every destination name the config refers to, default and rules.
    pub fn referenced_names(&self) -> impl Iterator<Item = &str> {
        self.default
            .names()
            .iter()
            .chain(self.rules.iter().flat_map(|r| r.destination.names()))
            .map(String::as_str)
    }

    /// Whether `name` resolves to the bus: the built-in `kafka`, or a declared
    /// destination with a `kafka` block.
    pub fn is_bus(&self, name: &str) -> bool {
        match self.named.get(name) {
            Some(spec) => spec.kafka.is_some(),
            None => name == BUS_DESTINATION,
        }
    }

    /// Whether any referenced destination reaches the bus, so the config needs
    /// brokers.
    pub fn uses_bus(&self) -> bool {
        self.referenced_names().any(|name| self.is_bus(name))
    }
}

/// One destination name, or a list of them to fan a matched record out to.
///
/// A fan-out is delivered when every destination has accepted it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum DestinationRef {
    /// A single destination name.
    One(String),
    /// Several destination names -- the record goes to all of them.
    Many(Vec<String>),
}

impl DestinationRef {
    /// The names, one or many.
    pub fn names(&self) -> &[String] {
        match self {
            Self::One(name) => std::slice::from_ref(name),
            Self::Many(names) => names,
        }
    }
}

impl Default for DestinationRef {
    fn default() -> Self {
        Self::One(BUS_DESTINATION.to_string())
    }
}

impl From<&str> for DestinationRef {
    fn from(name: &str) -> Self {
        Self::One(name.to_string())
    }
}

impl From<String> for DestinationRef {
    fn from(name: String) -> Self {
        Self::One(name)
    }
}

impl std::fmt::Display for DestinationRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.names().join(","))
    }
}

/// A declared destination: exactly one transport block.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct DestinationSpec {
    /// Deliver over gRPC to a scalo Push listener (a transform, the loader, the
    /// archiver).
    pub grpc: Option<GrpcDestination>,

    /// Deliver over the bus.
    pub kafka: Option<KafkaDestination>,
}

/// A gRPC destination -- the address of a scalo Push listener.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GrpcDestination {
    /// Endpoint URI, e.g. `http://dfe-transform-orders:6000`.
    pub endpoint: String,

    /// Whether the listener answers a push only once its records are durable.
    /// Set false for one that answers on receipt, as the archiver's direct
    /// listener does: every listener whose records can reach it then reports
    /// `best_effort` in `pipeline_delivery_guarantee`. Delivery is unchanged.
    #[serde(default = "default_true")]
    pub confirms_delivery: bool,
}

impl Default for GrpcDestination {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            confirms_delivery: true,
        }
    }
}

/// A bus destination.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct KafkaDestination {
    /// Fixed topic for this destination. Unset means the topic the record's
    /// source resolves to, which is what the built-in `kafka` destination does.
    pub topic: Option<String>,
}

/// Destination routing rule.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DestinationRule {
    /// Field to match.
    pub match_field: String,

    /// Value to match.
    pub match_value: String,

    /// Destination for matched messages: one name, or a list to fan out.
    pub destination: DestinationRef,
}

/// Kafka producer configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct KafkaConfig {
    /// Broker addresses.
    pub brokers: Vec<String>,

    /// Client ID.
    pub client_id: String,

    /// SASL configuration.
    pub sasl: Option<SaslConfig>,

    /// TLS configuration.
    pub tls: KafkaTlsConfig,

    /// Raw librdkafka configuration overrides (highest priority), the one
    /// place producer batching, linger, compression and acks are tuned.
    pub librdkafka_overrides: std::collections::HashMap<String, String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        let mut overrides = std::collections::HashMap::new();
        // "0" rather than absent: scalo's producer profiles turn stats on at 1 s, and no context here reads them.
        overrides.insert("statistics.interval.ms".to_string(), "0".to_string());

        Self {
            // Empty on purpose: a bus destination with no brokers refuses to
            // boot in `validate` rather than dial a guessed localhost.
            brokers: vec![],
            client_id: "dfe-receiver".to_string(),
            sasl: None,
            tls: KafkaTlsConfig::default(),
            librdkafka_overrides: overrides,
        }
    }
}

/// SASL authentication configuration.
#[derive(Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SaslConfig {
    /// Enable SASL.
    pub enabled: bool,

    /// SASL mechanism (plain, scram_sha_256, scram_sha_512).
    pub mechanism: String,

    /// Username.
    pub username: String,

    /// Password, redacted wherever it is printed or serialised.
    pub password: SensitiveString,
}

impl std::fmt::Debug for SaslConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslConfig")
            .field("enabled", &self.enabled)
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"***REDACTED***")
            .finish()
    }
}

/// Kafka TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct KafkaTlsConfig {
    /// Enable TLS for Kafka.
    pub enabled: bool,

    /// CA certificate file.
    pub ca_file: Option<String>,

    /// Client certificate file.
    pub cert_file: Option<String>,

    /// Client key file.
    pub key_file: Option<String>,
}

impl Default for KafkaTlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ca_file: None,
            cert_file: None,
            key_file: None,
        }
    }
}

/// dfe-loader connection configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct LoaderConfig {
    /// Loader address, the manifest's `endpoints.push` port (apps.yaml).
    /// Override with grpc_endpoint to dial a different URI.
    pub address: String,

    /// Transport type (kafka, grpc; `memory` discards and is refused at startup).
    pub transport: String,

    /// Per-RPC deadline for the gRPC loader client, in milliseconds (0 = none).
    /// The default sits inside the hold a listener gives its sender.
    pub timeout_ms: u64,

    /// gRPC endpoint URI for loader (only used when transport = "grpc").
    /// Defaults to http://{address} if not set.
    pub grpc_endpoint: Option<String>,
}

impl Default for LoaderConfig {
    fn default() -> Self {
        Self {
            address: "dfe-loader:6000".to_string(),
            transport: "kafka".to_string(),
            timeout_ms: crate::pipeline::acks::NEXT_HOP_DEADLINE_MS,
            grpc_endpoint: None,
        }
    }
}

impl LoaderConfig {
    /// Returns the effective gRPC endpoint URI.
    ///
    /// Uses `grpc_endpoint` if set, otherwise derives `http://{address}`.
    pub fn effective_grpc_endpoint(&self) -> String {
        if let Some(ref ep) = self.grpc_endpoint {
            ep.clone()
        } else {
            format!("http://{}", self.address)
        }
    }
}

/// Debug file sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct FileSinkConfig {
    /// Enable writing all processed messages to a file.
    pub enabled: bool,

    /// Path to the output file (NDJSON format, appended).
    pub path: String,
}

impl Default for FileSinkConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: "/tmp/dfe-receiver-debug.ndjson".to_string(),
        }
    }
}

/// The in-memory queue a destination holds records in while its sink is
/// unavailable, and its optional disk spillover.
///
/// These settings bound that queue and nothing else. The 503 a listener
/// answers under process memory pressure comes from the memory guard, set by
/// `DFE_RECEIVER_MEMORY_LIMIT_BYTES` and `DFE_RECEIVER_MEMORY_PRESSURE_THRESHOLD`,
/// and from `self_regulation` when it is on.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct BufferConfig {
    /// Bytes the in-memory queue is sized from. 0 bounds the queue at 1000
    /// records instead.
    pub memory_limit: usize,

    /// Share of `memory_limit` the in-memory queue may hold (0.0-1.0). A record
    /// past it is refused and its request answered as retryable (HTTP 503,
    /// gRPC UNAVAILABLE). Unused while `memory_limit` is 0.
    pub pressure_threshold: f64,

    /// Optional disk spillover configuration.
    /// When enabled, messages are spilled to disk via scalo's TieredSink
    /// when the primary sink is unavailable (instead of in-memory only).
    #[serde(default)]
    pub spillover: SpilloverConfig,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            memory_limit: 0, // Auto-detect
            pressure_threshold: 0.8,
            spillover: SpilloverConfig::default(),
        }
    }
}

/// Records the in-memory queue holds when `memory_limit` is auto-detected.
pub const DEFAULT_QUEUE_RECORDS: usize = 1000;

/// What the in-memory queue refuses at.
///
/// One bound, picked by the config: an auto-detected `memory_limit` gives the
/// queue no byte figure to work from, so it counts records instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueBound {
    /// Hold at most this many records.
    Records(usize),
    /// Hold at most this many queued payload bytes.
    Bytes(usize),
}

impl BufferConfig {
    /// The bound the in-memory queue refuses at, so an unreachable destination
    /// back-pressures the ingest instead of holding without limit.
    ///
    /// `pressure_threshold` is the share of `memory_limit` the queue may take,
    /// leaving the rest of the limit for records in flight.
    #[must_use]
    pub fn queue_bound(&self) -> QueueBound {
        if self.memory_limit == 0 {
            return QueueBound::Records(DEFAULT_QUEUE_RECORDS);
        }
        QueueBound::Bytes((self.memory_limit as f64 * self.pressure_threshold) as usize)
    }
}

/// Disk spillover configuration (opt-in, default disabled).
///
/// When enabled, failed sends are spilled to disk via scalo's TieredSink
/// instead of being held in an in-memory queue. This provides crash-resilient
/// buffering at the cost of disk I/O on the failure path.
///
/// Only records a listener answers at enqueue are spooled. With every enabled
/// listener holding its answer (`acknowledgements.enabled`, the default) no
/// spool is opened: the sender keeps its copy until a destination confirms it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
#[serde(default)]
pub struct SpilloverConfig {
    /// Enable disk spillover (default: false).
    pub enabled: bool,

    /// Directory for spool files. The bus spools under `kafka/` and each gRPC
    /// destination under `grpc/<name>/`, since two sinks cannot share a spool.
    pub path: std::path::PathBuf,

    /// Maximum filesystem usage percentage (0.0-1.0) before pausing spool writes.
    pub max_usage_percent: f64,

    /// How often to check disk usage, in seconds.
    pub poll_interval_secs: u64,
}

fn default_spillover_path() -> std::path::PathBuf {
    std::path::PathBuf::from("/var/spool/dfe-receiver")
}

fn default_max_usage_percent() -> f64 {
    0.8
}

fn default_poll_interval_secs() -> u64 {
    5
}

impl Default for SpilloverConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: default_spillover_path(),
            max_usage_percent: default_max_usage_percent(),
            poll_interval_secs: default_poll_interval_secs(),
        }
    }
}

/// Metrics configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct MetricsConfig {
    /// Enable metrics.
    pub enabled: bool,

    /// Metrics server address.
    pub address: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            address: "0.0.0.0:9090".to_string(),
        }
    }
}

/// Scaling pressure configuration for KEDA autoscaling.
///
/// Configures the weighted composite metric that KEDA uses to scale the receiver.
/// Each component has a weight (relative importance) and saturation point (value
/// at which it contributes its full weight to the composite).
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct ScalingConfig {
    /// Enable scaling pressure calculation.
    pub enabled: bool,

    /// Memory usage ratio that triggers the memory gate (0.0-1.0).
    pub memory_gate_threshold: f64,

    /// Weight for request rate component (default 0.30).
    pub weight_request_rate: f64,

    /// Weight for queue depth component (default 0.25).
    pub weight_queue_depth: f64,

    /// Weight for memory component (default 0.25).
    pub weight_memory: f64,

    /// Weight for active connections component (default 0.10).
    pub weight_connections: f64,

    /// Weight for spilled messages component (default 0.10).
    pub weight_spill: f64,

    /// Saturation point for request rate (req/s).
    pub saturation_request_rate: f64,

    /// Saturation point for queue depth (messages).
    pub saturation_queue_depth: f64,

    /// Saturation point for active connections.
    pub saturation_connections: f64,

    /// Saturation point for spilled messages.
    pub saturation_spill: f64,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            memory_gate_threshold: 0.8,
            weight_request_rate: 0.30,
            weight_queue_depth: 0.25,
            weight_memory: 0.25,
            weight_connections: 0.10,
            weight_spill: 0.10,
            saturation_request_rate: 100_000.0,
            saturation_queue_depth: 10_000.0,
            saturation_connections: 1_000.0,
            saturation_spill: 1_000.0,
        }
    }
}

impl ScalingConfig {
    /// The scalo `ScalingPressureConfig` (gate thresholds) from this config.
    #[must_use]
    pub fn pressure_config(&self) -> ScalingPressureConfig {
        ScalingPressureConfig {
            enabled: self.enabled,
            memory_gate_threshold: self.memory_gate_threshold,
        }
    }

    /// The weighted components KEDA scores against.
    ///
    /// Shared between [`build_pressure`](Self::build_pressure) and the
    /// `ServiceApp::scaling_components` hook so the runtime's
    /// `ScalingPressure` (the engine `/scaling/pressure` serves to KEDA) and
    /// any standalone engine register the SAME set. `queue_depth` is the sum of
    /// all sink producer queues (the outbound term) and `connections` is the
    /// per-pod inbound concurrency proxy -- between them they subsume the
    /// signals the old scalo runtime signal cell fed (in-flight, produce-queue
    /// depth), so the pipeline no longer pushes a separate per-pod feed.
    #[must_use]
    pub fn components(&self) -> Vec<ScalingComponent> {
        vec![
            ScalingComponent::new(
                "request_rate",
                self.weight_request_rate,
                self.saturation_request_rate,
            ),
            ScalingComponent::new(
                "queue_depth",
                self.weight_queue_depth,
                self.saturation_queue_depth,
            ),
            ScalingComponent::new("memory", self.weight_memory, 1.0),
            ScalingComponent::new(
                "connections",
                self.weight_connections,
                self.saturation_connections,
            ),
            ScalingComponent::new("spill", self.weight_spill, self.saturation_spill),
        ]
    }

    /// Build a standalone `ScalingPressure` engine from this config.
    ///
    /// Used by tests / standalone contexts. Production shares the runtime's
    /// engine (registered via `ServiceApp::scaling_components`).
    #[must_use]
    pub fn build_pressure(&self) -> ScalingPressure {
        ScalingPressure::new(self.pressure_config(), self.components())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temp_env;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.bind_address, "0.0.0.0:8080");
    }

    /// The producer is tuned through scalo's builder alone: a removed
    /// `kafka.producer` block in an old config parses and changes nothing.
    #[test]
    fn a_leftover_producer_block_changes_nothing() {
        let with_block: KafkaConfig =
            serde_yaml_ng::from_str("producer:\n  compression: lz4\n  linger_ms: 20\n").unwrap();
        let client = with_block.to_scalo_kafka_config_for_producer().unwrap();
        assert_eq!(
            client.librdkafka_overrides,
            KafkaConfig::default().librdkafka_overrides
        );
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        // Default destination is kafka, so we need brokers
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        assert!(config.validate().is_ok());
    }

    /// An OTLP HTTP export is held a second inside the exporters' default
    /// timeout, and a zero hold is refused while acknowledgements are on.
    #[cfg(feature = "otlp")]
    #[test]
    fn the_otlp_http_hold_defaults_inside_the_exporter_timeout() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.otlp.enabled = true;
        assert_eq!(config.otlp.http_max_hold_ms, 9_000);
        assert!(config.validate().is_ok());

        config.otlp.http_max_hold_ms = 0;
        let err = config.validate().expect_err("a zero hold is refused");
        assert!(err.to_string().contains("otlp.http_max_hold_ms"), "{err}");

        config.otlp.acknowledgements = AcknowledgementsConfig::new(false);
        assert!(config.validate().is_ok(), "no hold to bound with acks off");
    }

    /// A config that passes every rule outside the flow block.
    fn flow_base() -> Config {
        let mut config = Config::default();
        config.destinations.default = "loader".into();
        config.loader.transport = "grpc".to_string();
        config
    }

    #[test]
    fn an_invalid_flow_block_fails_startup() {
        // Unified and split at once: the flow handler refuses this at build, and
        // the pod used to start and report ready without it.
        let mut config = flow_base();
        config.flow = serde_yaml_ng::from_str(
            "enabled: true\nsplit:\n  netflow:\n    ports: [2055]\n    topic: n\n  \
             sflow:\n    ports: [6343]\n    topic: s\n",
        )
        .expect("flow yaml");

        let err = config.validate().expect_err("must not start");
        assert!(err.to_string().contains("flow"), "{err}");
    }

    #[test]
    fn overlapping_split_flow_ports_fail_startup() {
        let mut config = flow_base();
        config.flow = serde_yaml_ng::from_str(
            "split:\n  netflow:\n    ports: [2055, 6343]\n    topic: n\n  \
             sflow:\n    ports: [6343]\n    topic: s\n",
        )
        .expect("flow yaml");

        let err = config.validate().expect_err("must not start");
        assert!(err.to_string().contains("overlap"), "{err}");
    }

    #[test]
    fn flow_enabled_with_no_ports_fails_startup() {
        // DFE_RECEIVER_FLOW_PORTS drops every entry that is not a port, so a typo lands here too.
        let mut config = flow_base();
        config.flow.enabled = true;
        config.flow.ports = Vec::new();

        let err = config.validate().expect_err("must not start");
        assert!(err.to_string().contains("flow.ports"), "{err}");
    }

    #[test]
    fn a_valid_flow_block_starts() {
        let mut config = flow_base();
        config.flow.enabled = true;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn mtls_mode_without_tls_is_refused() {
        // The trap: this reads as mutual TLS and enforces nothing, because the
        // request path short-circuits for mtls and the handshake is not doing
        // the work either.
        let mut config = Config::default();
        // Direct gRPC, so validate() does not stop at the brokers rule first
        // and each test below fails only on the auth rule it is about.
        config.destinations.default = "loader".into();
        config.loader.transport = "grpc".to_string();
        config.server.auth.mode = "mtls".to_string();

        let err = config.validate().expect_err("must not start");
        assert!(
            err.to_string().contains("tls.enabled is false"),
            "the error must name the missing half, got: {err}"
        );
    }

    #[test]
    fn mtls_mode_with_optional_client_auth_is_refused() {
        // `optional` validates a certificate when one is offered and admits
        // clients that offer none, which is not authentication.
        let mut config = Config::default();
        // Direct gRPC, so validate() does not stop at the brokers rule first
        // and each test below fails only on the auth rule it is about.
        config.destinations.default = "loader".into();
        config.loader.transport = "grpc".to_string();
        config.server.auth.mode = "mtls".to_string();
        config.server.tls.enabled = true;
        config.server.tls.client_auth = "optional".to_string();

        let err = config.validate().expect_err("must not start");
        assert!(
            err.to_string().contains("must be 'required'"),
            "the error must say what to set, got: {err}"
        );
    }

    #[test]
    fn mtls_mode_with_required_client_auth_is_accepted() {
        let mut config = Config::default();
        // Direct gRPC, so validate() does not stop at the brokers rule first
        // and each test below fails only on the auth rule it is about.
        config.destinations.default = "loader".into();
        config.loader.transport = "grpc".to_string();
        config.server.auth.mode = "mtls".to_string();
        config.server.tls.enabled = true;
        config.server.tls.client_auth = "required".to_string();

        assert!(config.validate().is_ok());
    }

    // -- auth modes a listener does not enforce --
    //
    // One shape, checked per listener: a mode that parses cleanly, changes the
    // config that reaches the handler, and changes nothing about who gets in.

    /// A config that validates, so each test below fails only on its own rule.
    fn auth_base() -> Config {
        let mut config = Config::default();
        // Direct gRPC, so validate() does not stop at the brokers rule first
        // and each test below fails only on the auth rule it is about.
        config.destinations.default = "loader".into();
        config.loader.transport = "grpc".to_string();
        config
    }

    #[test]
    fn both_mode_without_tls_is_refused() {
        // `both` means token auth AND mTLS (see AuthMode's docs). Only the
        // token half runs in the request path, so with TLS off the deployment
        // gets one of the two things it asked for and no word about the other.
        let mut config = auth_base();
        config.server.auth.mode = "both".to_string();
        config.server.auth.accepted_headers = vec![AcceptedHeader {
            name: "x-api-key".to_string(),
            values: vec!["k".into()],
        }];

        let err = config.validate().expect_err("must not start");
        assert!(
            err.to_string().contains("server.tls.enabled is false"),
            "the error must name the missing half, got: {err}"
        );
    }

    #[test]
    fn both_mode_with_required_client_auth_is_accepted() {
        let mut config = auth_base();
        config.server.auth.mode = "both".to_string();
        config.server.auth.accepted_headers = vec![AcceptedHeader {
            name: "x-api-key".to_string(),
            values: vec!["k".into()],
        }];
        config.server.tls.enabled = true;
        config.server.tls.client_auth = "required".to_string();

        assert!(config.validate().is_ok());
    }

    #[test]
    fn mtls_on_an_enabled_hec_listener_is_refused() {
        // splunk_hec runs the same token_auth_middleware, which short-circuits
        // for mtls -- so this admitted every HEC post on 8088.
        let mut config = auth_base();
        config.splunk_hec.enabled = true;
        config.splunk_hec.auth.mode = "mtls".to_string();

        let err = config.validate().expect_err("must not start");
        assert!(
            err.to_string().contains("splunk_hec.tls.enabled is false"),
            "the error must name the listener, got: {err}"
        );
    }

    #[test]
    fn mtls_on_an_enabled_prometheus_rw_listener_is_refused() {
        let mut config = auth_base();
        config.prometheus_rw.enabled = true;
        config.prometheus_rw.auth.mode = "mtls".to_string();

        let err = config.validate().expect_err("must not start");
        assert!(
            err.to_string()
                .contains("prometheus_rw.tls.enabled is false"),
            "the error must name the listener, got: {err}"
        );
    }

    #[test]
    fn a_disabled_listener_does_not_block_startup() {
        // The block is inert because the listener is not running, which is the
        // one case where inert is honest. Refusing here would be noise.
        let mut config = auth_base();
        config.splunk_hec.enabled = false;
        config.splunk_hec.auth.mode = "mtls".to_string();
        config.syslog.enabled = false;
        config.syslog.auth.mode = "bearer".to_string();

        assert!(config.validate().is_ok());
    }

    #[test]
    fn header_mode_on_grpc_is_refused() {
        // make_auth_interceptor builds a HeaderMap carrying only
        // `authorization` and calls validate_bearer_auth. An accepted_headers
        // list configured here is never read.
        let mut config = auth_base();
        config.grpc.enabled = true;
        config.grpc.auth.mode = "header".to_string();
        config.grpc.auth.accepted_headers = vec![AcceptedHeader {
            name: "x-api-key".to_string(),
            values: vec!["k".into()],
        }];

        let err = config.validate().expect_err("must not start");
        assert!(
            err.to_string().contains("bearer tokens only"),
            "the error must say what the interceptor does, got: {err}"
        );
    }

    #[test]
    fn bearer_mode_on_grpc_is_accepted() {
        let mut config = auth_base();
        config.grpc.enabled = true;
        config.grpc.auth.mode = "bearer".to_string();
        config.grpc.auth.bearer.tokens = vec!["t".into()];

        assert!(config.validate().is_ok());
    }

    #[test]
    fn an_auth_mode_on_syslog_is_refused() {
        // Nothing in server/syslog references its auth block: there is no
        // credential on a syslog line to check. Writing a mode here bought a
        // wide-open 514 that read as authenticated.
        let mut config = auth_base();
        config.syslog.enabled = true;
        config.syslog.auth.mode = "bearer".to_string();
        config.syslog.auth.bearer.tokens = vec!["t".into()];

        let err = config.validate().expect_err("must not start");
        assert!(
            err.to_string().contains("no application auth"),
            "the error must say the listener has none, got: {err}"
        );
        assert!(
            err.to_string().contains("syslog.tls.client_auth: required"),
            "the error must name the way that does work, got: {err}"
        );
    }

    #[test]
    fn an_auth_mode_on_lumberjack_is_refused() {
        let mut config = auth_base();
        config.lumberjack.enabled = true;
        config.lumberjack.auth.mode = "bearer".to_string();

        let err = config.validate().expect_err("must not start");
        assert!(
            err.to_string().contains("no application auth"),
            "the error must say the listener has none, got: {err}"
        );
    }

    #[test]
    fn syslog_with_no_auth_mode_starts_once_unauthenticated_senders_are_accepted() {
        let mut config = auth_base();
        config.syslog.enabled = true;
        config.syslog.accept_unauthenticated = true;

        assert!(config.validate().is_ok());
    }

    // -- listeners whose wire protocol carries no credential --
    //
    // Lumberjack, Fluent Forward and GELF start only with client certificates
    // required at the handshake or accept_unauthenticated set. Syslog's UDP and
    // plain TCP listeners have no handshake, so it needs the opt-out always.

    /// A TLS block that refuses clients presenting no certificate.
    fn client_certificates_required() -> TlsConfig {
        TlsConfig {
            enabled: true,
            cert_file: Some("/etc/ssl/receiver.crt".to_string()),
            key_file: Some("/etc/ssl/receiver.key".to_string()),
            ca_file: Some("/etc/ssl/ca.crt".to_string()),
            client_auth: "required".to_string(),
            ..TlsConfig::default()
        }
    }

    /// `auth_base` with one credential-less listener enabled, as `tls` and the opt-out say.
    fn with_listener(scope: &str, tls: TlsConfig, accept_unauthenticated: bool) -> Config {
        let mut config = auth_base();
        match scope {
            "lumberjack" => {
                config.lumberjack.enabled = true;
                config.lumberjack.tls = tls;
                config.lumberjack.accept_unauthenticated = accept_unauthenticated;
            }
            "fluent" => {
                config.fluent.enabled = true;
                config.fluent.tls = tls;
                config.fluent.accept_unauthenticated = accept_unauthenticated;
            }
            "gelf" => {
                config.gelf.enabled = true;
                config.gelf.tls = tls;
                config.gelf.accept_unauthenticated = accept_unauthenticated;
            }
            "syslog" => {
                config.syslog.enabled = true;
                config.syslog.tls = tls;
                config.syslog.accept_unauthenticated = accept_unauthenticated;
            }
            other => panic!("no such listener: {other}"),
        }
        config
    }

    #[test]
    fn a_credential_less_listener_with_no_client_certificates_is_refused() {
        for scope in ["lumberjack", "fluent", "gelf", "syslog"] {
            let err = with_listener(scope, TlsConfig::default(), false)
                .validate()
                .expect_err(scope)
                .to_string();
            assert!(
                err.contains(&format!("{scope}.accept_unauthenticated: true")),
                "the error must name the opt-out key, got: {err}"
            );
            assert!(
                err.contains("server.ip_filter.cidrs"),
                "the error must name the allowlist, got: {err}"
            );
        }
    }

    #[test]
    fn the_refusal_names_every_key_the_handshake_needs() {
        let err = with_listener("gelf", TlsConfig::default(), false)
            .validate()
            .expect_err("must not start")
            .to_string();
        for key in [
            "gelf.tls.enabled: true",
            "gelf.tls.client_auth: required",
            "gelf.tls.ca_file",
        ] {
            assert!(err.contains(key), "{key} is missing from: {err}");
        }
    }

    #[test]
    fn client_certificates_required_at_the_handshake_start_the_listener() {
        for scope in ["lumberjack", "fluent", "gelf"] {
            let config = with_listener(scope, client_certificates_required(), false);
            assert!(config.validate().is_ok(), "{scope}");
        }
    }

    /// `optional` admits a client with no certificate, and `required` with no
    /// CA has nothing to verify a certificate against.
    #[test]
    fn a_handshake_that_admits_certificate_less_clients_is_not_enough() {
        let optional = TlsConfig {
            client_auth: "optional".to_string(),
            ..client_certificates_required()
        };
        let no_ca = TlsConfig {
            ca_file: None,
            ..client_certificates_required()
        };
        let off = TlsConfig {
            enabled: false,
            ..client_certificates_required()
        };
        for tls in [optional, no_ca, off] {
            assert!(
                with_listener("fluent", tls.clone(), false)
                    .validate()
                    .is_err(),
                "{tls:?}"
            );
        }
    }

    #[test]
    fn a_ca_secret_names_the_ca_as_well_as_a_ca_file() {
        let tls = TlsConfig {
            ca_file: None,
            ca_secret: Some("vault:secret/tls:ca".to_string()),
            ..client_certificates_required()
        };
        assert!(with_listener("lumberjack", tls, false).validate().is_ok());
    }

    /// Client certificates on syslog's TLS listener leave UDP and plain TCP open.
    #[test]
    fn syslog_needs_the_opt_out_even_with_client_certificates() {
        let err = with_listener("syslog", client_certificates_required(), false)
            .validate()
            .expect_err("UDP and TCP are still open")
            .to_string();
        assert!(err.contains("syslog.accept_unauthenticated"), "{err}");
    }

    #[test]
    fn accepting_unauthenticated_clients_starts_the_listener() {
        for scope in ["lumberjack", "fluent", "gelf", "syslog"] {
            let config = with_listener(scope, TlsConfig::default(), true);
            assert!(config.validate().is_ok(), "{scope}");
        }
    }

    #[test]
    fn a_disabled_credential_less_listener_needs_no_opt_out() {
        let mut config = auth_base();
        config.lumberjack.enabled = false;
        config.fluent.enabled = false;
        config.gelf.enabled = false;
        config.syslog.enabled = false;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn the_opt_out_parses_from_yaml() {
        let config: Config = serde_yaml_ng::from_str(
            "fluent:\n  enabled: true\n  accept_unauthenticated: true\n\
             syslog:\n  enabled: true\n  accept_unauthenticated: true\n",
        )
        .unwrap();
        assert!(config.fluent.accept_unauthenticated);
        assert!(config.syslog.accept_unauthenticated);
        assert!(!config.gelf.accept_unauthenticated, "off unless written");
    }

    #[test]
    fn a_rate_limit_of_zero_requests_per_second_is_refused() {
        // The limiter is configured by the interval between replenishments,
        // which is 1/rate: zero has no such interval and the derivation would
        // divide by it.
        let mut config = auth_base();
        config.server.rate_limit.enabled = true;
        config.server.rate_limit.requests_per_second = 0;

        let err = config.validate().expect_err("must not start");
        assert!(
            err.to_string()
                .contains("server.rate_limit.requests_per_second"),
            "the error must name the field, got: {err}"
        );
    }

    #[test]
    fn a_rate_limit_of_zero_is_inert_while_the_limiter_is_off() {
        let mut config = auth_base();
        config.server.rate_limit.enabled = false;
        config.server.rate_limit.requests_per_second = 0;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn a_misspelled_auth_mode_is_refused() {
        let mut config = auth_base();
        config.server.auth.mode = "bearrer".to_string();

        let err = config.validate().expect_err("must not start").to_string();
        assert!(
            err.contains("'bearrer'"),
            "the error must name the value, got: {err}"
        );
        assert!(
            err.contains("none, header, bearer, mtls, both"),
            "the error must name the valid set, got: {err}"
        );
    }

    #[test]
    fn an_empty_auth_mode_is_refused() {
        let mut config = auth_base();
        config.server.auth.mode = String::new();
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("server.auth.mode is ''"), "{err}");
    }

    /// A typo is refused in the block of a listener that is not running too:
    /// enabling the listener later must not be what surfaces it.
    #[test]
    fn a_misspelled_auth_mode_on_a_disabled_listener_is_refused() {
        for scope in [
            "grpc",
            "lumberjack",
            "splunk_hec",
            "syslog",
            "prometheus_rw",
        ] {
            let mut config = auth_base();
            let auth = match scope {
                "grpc" => &mut config.grpc.auth,
                "lumberjack" => &mut config.lumberjack.auth,
                "splunk_hec" => &mut config.splunk_hec.auth,
                "syslog" => &mut config.syslog.auth,
                _ => &mut config.prometheus_rw.auth,
            };
            auth.mode = "Bearer Token".to_string();
            let err = config.validate().expect_err(scope).to_string();
            assert!(err.contains(&format!("{scope}.auth.mode")), "{err}");
        }
    }

    #[cfg(feature = "otlp")]
    #[test]
    fn a_misspelled_auth_mode_on_a_disabled_otlp_listener_is_refused() {
        let mut config = auth_base();
        config.otlp.auth.mode = "Bearer Token".to_string();
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("otlp.auth.mode"), "{err}");
    }

    /// With no implicit header, `header` mode needs one written down.
    #[test]
    fn header_mode_with_nothing_to_accept_is_refused() {
        let mut config = auth_base();
        config.server.auth.mode = "header".to_string();
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("server.auth.accepted_headers"), "{err}");

        config.server.auth.bearer.tokens = vec!["t".into()];
        assert!(config.validate().is_ok(), "a bearer token is a credential");

        config.server.auth.bearer.tokens.clear();
        config.server.auth.header_name = "x-legacy".to_string();
        config.server.auth.header_values = vec!["v".into()];
        assert!(
            config.validate().is_ok(),
            "the legacy header is a credential"
        );
    }

    #[test]
    fn header_mode_with_nothing_to_accept_is_refused_on_hec_and_remote_write() {
        let mut config = auth_base();
        config.splunk_hec.enabled = true;
        config.splunk_hec.auth.mode = "header".to_string();
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("splunk_hec.auth.accepted_headers"), "{err}");

        let mut config = auth_base();
        config.prometheus_rw.enabled = true;
        config.prometheus_rw.auth.mode = "header".to_string();
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("prometheus_rw.auth.accepted_headers"), "{err}");
    }

    #[test]
    fn every_documented_auth_mode_is_accepted_on_the_server_listener() {
        // The five AuthMode::parse recognises, so known_mode can never
        // drift away from the parser it guards.
        for mode in ["none", "header", "bearer"] {
            let mut config = auth_base();
            config.server.auth.mode = mode.to_string();
            config.server.auth.accepted_headers = vec![AcceptedHeader {
                name: "x-api-key".to_string(),
                values: vec!["k".into()],
            }];
            assert!(config.validate().is_ok(), "{mode} must be accepted");
        }
        for mode in ["mtls", "both"] {
            let mut config = auth_base();
            config.server.auth.mode = mode.to_string();
            config.server.auth.accepted_headers = vec![AcceptedHeader {
                name: "x-api-key".to_string(),
                values: vec!["k".into()],
            }];
            config.server.tls.enabled = true;
            config.server.tls.client_auth = "required".to_string();
            assert!(config.validate().is_ok(), "{mode} must be accepted");
        }
    }

    // -- the IP filter fails closed --

    fn ip_filter(mode: &str, cidrs: &[&str]) -> IpFilterConfig {
        IpFilterConfig {
            mode: mode.to_string(),
            cidrs: cidrs.iter().map(ToString::to_string).collect(),
        }
    }

    /// Each of these reads as a filter and would admit what it names as barred.
    #[test]
    fn an_ip_filter_that_would_not_filter_refuses_to_start() {
        for (filter, says) in [
            (ip_filter("allowlist", &[]), "cidrs is empty"),
            (ip_filter("allow", &["10.0.0.0/8"]), "'allow'"),
            (
                ip_filter("allowlist", &["10.0.0.0/8", "10.0.0/8"]),
                "'10.0.0/8'",
            ),
            (ip_filter("denylist", &["192.0.2.1"]), "'192.0.2.1'"),
        ] {
            let mut config = auth_base();
            config.server.ip_filter = filter.clone();
            let err = config.validate().expect_err("must not start").to_string();
            assert!(err.contains("server.ip_filter"), "{filter:?}: {err}");
            assert!(err.contains(says), "{filter:?}: {err}");
        }
    }

    #[test]
    fn a_valid_ip_filter_starts() {
        for filter in [
            ip_filter("disabled", &[]),
            ip_filter("allowlist", &["10.0.0.0/8", "fd00::/8"]),
            ip_filter("denylist", &[]),
        ] {
            let mut config = auth_base();
            config.server.ip_filter = filter.clone();
            assert!(config.validate().is_ok(), "{filter:?}");
        }
    }

    #[test]
    fn an_enabled_flow_listeners_ip_filter_is_checked_too() {
        let mut config = flow_base();
        config.flow.enabled = true;
        config.flow.ip_filter = Some(ip_filter("allowlist", &[]));
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("flow.ip_filter"), "{err}");

        config.flow.enabled = false;
        assert!(config.validate().is_ok(), "an inert block is not checked");
    }

    #[test]
    fn a_split_flow_listeners_ip_filter_is_checked_too() {
        let mut config = flow_base();
        config.flow = serde_yaml_ng::from_str(
            "split:\n  netflow:\n    ports: [2055]\n    topic: n\n  \
             sflow:\n    ports: [6343]\n    topic: s\n    \
             ip_filter:\n      mode: allowlist\n      cidrs: [\"not-a-cidr\"]\n",
        )
        .expect("flow yaml");
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("flow.split.sflow.ip_filter"), "{err}");
    }

    // -- trusted proxies --

    #[test]
    fn a_trusted_proxy_that_is_not_a_cidr_refuses_to_start() {
        let mut config = auth_base();
        config.server.trusted_proxies = vec!["10.0.0.0/8".to_string(), "proxy".to_string()];
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("server.trusted_proxies"), "{err}");
        assert!(err.contains("'proxy'"), "{err}");
    }

    #[test]
    fn trusted_proxies_default_to_none_and_parse_from_yaml() {
        assert!(Config::default().server.trusted_proxies.is_empty());
        let config: Config =
            serde_yaml_ng::from_str("server:\n  trusted_proxies: [\"10.0.0.0/8\"]\n").unwrap();
        assert_eq!(config.server.trusted_proxies, ["10.0.0.0/8"]);
    }

    // -- the webhook caller table --
    //
    // Every rule here refuses a config that would start and then do something
    // other than what it reads as: a caller with no secret, two callers on one
    // path, a filter that never compiles, a TLS block nothing listens under.

    fn webhook_caller(name: &str) -> WebhookCallerConfig {
        WebhookCallerConfig {
            name: name.to_string(),
            topic: format!("{name}_land"),
            auth: WebhookAuthConfig {
                secret_source: "file:/run/secrets/webhook".to_string(),
                ..WebhookAuthConfig::default()
            },
            ..WebhookCallerConfig::default()
        }
    }

    fn webhook_base() -> Config {
        let mut config = auth_base();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.webhook.enabled = true;
        config.webhook.callers = vec![webhook_caller("runzero")];
        config
    }

    #[test]
    fn a_webhook_caller_with_a_secret_and_a_topic_is_accepted() {
        assert!(webhook_base().validate().is_ok());
    }

    #[test]
    fn an_enabled_webhook_without_brokers_is_refused() {
        let mut config = webhook_base();
        config.kafka.brokers.clear();
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("kafka.brokers is empty"), "got: {err}");
    }

    #[test]
    fn a_disabled_webhook_block_is_not_checked() {
        let mut config = auth_base();
        config.webhook.enabled = false;
        config.webhook.callers = vec![WebhookCallerConfig::default()];
        assert!(config.validate().is_ok());
    }

    #[test]
    fn an_enabled_webhook_with_no_callers_is_refused() {
        let mut config = webhook_base();
        config.webhook.callers.clear();
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("webhook.callers is empty"), "got: {err}");
    }

    #[test]
    fn duplicate_webhook_caller_names_are_refused() {
        let mut config = webhook_base();
        config.webhook.callers.push(webhook_caller("runzero"));
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("declared twice"), "got: {err}");
    }

    #[test]
    fn a_webhook_caller_name_that_is_not_a_path_segment_is_refused() {
        for bad in ["Run Zero", "runzero/alerts", "RUNZERO", ""] {
            let mut config = webhook_base();
            config.webhook.callers[0].name = bad.to_string();
            assert!(
                config.validate().is_err(),
                "caller name {bad:?} must be refused"
            );
        }
    }

    #[test]
    fn a_webhook_caller_without_a_topic_is_refused() {
        let mut config = webhook_base();
        config.webhook.callers[0].topic = String::new();
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("topic is required"), "got: {err}");
    }

    #[test]
    fn a_webhook_caller_without_a_secret_source_is_refused() {
        let mut config = webhook_base();
        config.webhook.callers[0].auth.secret_source = String::new();
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("secret_source is required"), "got: {err}");
    }

    #[test]
    fn an_hmac_caller_without_a_timestamp_header_is_refused() {
        let mut config = webhook_base();
        config.webhook.callers[0].auth.timestamp_header = String::new();
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("timestamp_header is required"), "got: {err}");
    }

    #[test]
    fn a_header_caller_needs_no_timestamp_header() {
        let mut config = webhook_base();
        config.webhook.callers[0].auth.mode = WebhookAuthMode::Header;
        config.webhook.callers[0].auth.timestamp_header = String::new();
        config.webhook.callers[0].auth.tolerance_secs = 0;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn a_webhook_header_that_is_not_a_header_name_is_refused() {
        let mut config = webhook_base();
        config.webhook.callers[0].auth.header = "not a header".to_string();
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("not a valid header name"), "got: {err}");
    }

    #[test]
    fn a_webhook_filter_that_does_not_compile_is_refused() {
        let mut config = webhook_base();
        config.webhook.callers[0].filter = "severity ==".to_string();
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("not a valid CEL expression"), "got: {err}");
    }

    #[test]
    fn a_webhook_filter_that_compiles_is_accepted() {
        let mut config = webhook_base();
        config.webhook.callers[0].filter = r#"severity == "high""#.to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn webhook_tls_on_the_shared_listener_is_refused() {
        // The shared listener is under server.tls; a webhook.tls block there
        // configures nothing and reads as though the intake were encrypted.
        let mut config = webhook_base();
        config.webhook.tls.enabled = true;
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("webhook.bind_address is unset"), "got: {err}");
    }

    #[test]
    fn webhook_tls_on_an_own_listener_is_accepted() {
        let mut config = webhook_base();
        config.webhook.bind_address = Some("0.0.0.0:8090".to_string());
        config.webhook.tls.enabled = true;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn a_webhook_bind_address_that_is_not_a_socket_address_is_refused() {
        let mut config = webhook_base();
        config.webhook.bind_address = Some("8090".to_string());
        let err = config.validate().expect_err("must not start").to_string();
        assert!(err.contains("not a socket address"), "got: {err}");
    }

    #[test]
    fn the_webhook_section_parses_from_yaml() {
        let yaml = r#"
webhook:
  enabled: true
  max_body_size: 65536
  callers:
    - name: runzero
      topic: runzero_alerts_land
      auth:
        mode: header
        secret_source: "file:/run/secrets/runzero-webhook"
        header: x-webhook-secret
      body: single
    - name: pager
      topic: pager_land
      auth:
        mode: hmac
        secret_source: "vault:kv/data/dfe/webhooks:pager"
        header: x-signature
        timestamp_header: x-timestamp
        tolerance_secs: 120
      body: array
      filter: 'severity == "high"'
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(config.webhook.enabled);
        assert!(config.webhook.bind_address.is_none());
        assert_eq!(config.webhook.max_body_size, 65536);
        assert_eq!(config.webhook.callers.len(), 2);
        assert_eq!(config.webhook.callers[0].auth.mode, WebhookAuthMode::Header);
        assert_eq!(config.webhook.callers[0].body, WebhookBody::Single);
        assert_eq!(config.webhook.callers[1].auth.mode, WebhookAuthMode::Hmac);
        assert_eq!(config.webhook.callers[1].auth.tolerance_secs, 120);
        assert_eq!(config.webhook.callers[1].body, WebhookBody::Array);
        assert_eq!(config.webhook.callers[1].filter, r#"severity == "high""#);
    }

    #[test]
    fn test_env_override_webhook() {
        with_env(
            &[
                ("DFE_RECEIVER_WEBHOOK_ENABLED", "true"),
                ("DFE_RECEIVER_WEBHOOK_BIND_ADDRESS", "0.0.0.0:8090"),
            ],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                assert!(config.webhook.enabled);
                assert_eq!(config.webhook.bind_address.as_deref(), Some("0.0.0.0:8090"));
            },
        );
    }

    #[test]
    fn test_config_validation_no_brokers_required_for_loader() {
        let mut config = Config::default();
        // Direct gRPC, so validate() does not stop at the brokers rule first
        // and each test below fails only on the auth rule it is about.
        config.destinations.default = "loader".into();
        config.loader.transport = "grpc".to_string();
        config.loader.transport = "grpc".to_string();
        assert!(config.validate().is_ok());
    }

    /// The loader over the bus needs brokers, and says which two keys put it
    /// there rather than dropping records under traffic.
    #[test]
    fn the_loader_on_the_bus_needs_brokers() {
        let mut config = Config::default();
        // Direct gRPC, so validate() does not stop at the brokers rule first
        // and each test below fails only on the auth rule it is about.
        config.destinations.default = "loader".into();
        config.loader.transport = "grpc".to_string();
        config.loader.transport = "kafka".to_string();

        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("kafka.brokers"), "got: {err}");
        assert!(err.contains("loader.transport"), "got: {err}");
    }

    #[test]
    fn an_unknown_loader_transport_is_refused() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.loader.transport = "Kafka".to_string();

        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("loader.transport 'Kafka'"), "got: {err}");
        assert!(err.contains("kafka, grpc"), "got: {err}");
        assert!(!err.contains(DISCARD_TRANSPORT), "got: {err}");
    }

    /// The discard transport reads as a working config and delivers nothing, so
    /// it never starts -- whatever else the config says.
    #[test]
    fn the_discard_transport_is_refused_at_startup() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.loader.transport = DISCARD_TRANSPORT.to_string();

        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("accepts records and drops them"), "got: {err}");
        assert!(err.contains("kafka, grpc"), "got: {err}");
    }

    /// The loader's default address is the port the manifest gives its Push
    /// listener (dfe-infra apps.yaml `dfe-loader.endpoints.push`).
    #[test]
    fn the_loader_default_address_is_the_manifest_push_port() {
        let config = Config::default();
        assert_eq!(config.loader.address, "dfe-loader:6000");
        assert_eq!(
            config.loader.effective_grpc_endpoint(),
            "http://dfe-loader:6000"
        );
    }

    #[test]
    fn test_invalid_pressure_threshold() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.buffer.pressure_threshold = 1.5;

        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("buffer.pressure_threshold"), "got: {err}");
    }

    /// An auto-detected memory limit leaves the queue counting records.
    #[test]
    fn the_default_buffer_bound_counts_records() {
        assert_eq!(
            BufferConfig::default().queue_bound(),
            QueueBound::Records(DEFAULT_QUEUE_RECORDS)
        );
    }

    /// A configured memory limit bounds the queue in bytes, scaled by the
    /// pressure threshold.
    #[test]
    fn a_configured_memory_limit_bounds_the_queue_in_bytes() {
        let config = BufferConfig {
            memory_limit: 8 * 1024 * 1024,
            pressure_threshold: 0.75,
            ..Default::default()
        };
        assert_eq!(config.queue_bound(), QueueBound::Bytes(6 * 1024 * 1024));
    }

    // -- env override tests --
    // Each test sets env vars, runs apply_flat_env on a default config,
    // then cleans up. Tests are serial-safe because they use unique var names.

    fn with_env<F: FnOnce()>(vars: &[(&str, &str)], f: F) {
        // temp_env handles unsafe set_var/remove_var internally with a mutex guard.
        let owned: Vec<(&str, Option<&str>)> = vars.iter().map(|(k, v)| (*k, Some(*v))).collect();
        temp_env::with_vars(owned, f);
    }

    #[test]
    fn test_env_override_bind_address() {
        with_env(&[("DFE_RECEIVER_BIND_ADDRESS", "127.0.0.1:9999")], || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            assert_eq!(config.server.bind_address, "127.0.0.1:9999");
        });
    }

    #[test]
    fn test_env_override_max_body_size() {
        with_env(&[("DFE_RECEIVER_MAX_BODY_SIZE", "5242880")], || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            assert_eq!(config.server.max_body_size, 5_242_880);
        });
    }

    #[test]
    fn test_env_override_request_timeout_ms() {
        with_env(&[("DFE_RECEIVER_REQUEST_TIMEOUT_MS", "60000")], || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            assert_eq!(config.server.request_timeout_ms, 60_000);
        });
    }

    #[test]
    fn test_env_override_common_header() {
        // Test true
        with_env(&[("DFE_RECEIVER_COMMON_HEADER", "true")], || {
            let mut config = Config::default();
            config.server.auth.include_common_header = false;
            config.apply_flat_env(ENV_PREFIX);
            assert!(config.server.auth.include_common_header);
        });

        // Test false
        with_env(&[("DFE_RECEIVER_COMMON_HEADER", "false")], || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            assert!(!config.server.auth.include_common_header);
        });
    }

    #[test]
    fn bearer_tokens_arrive_from_the_environment() {
        // The only route the chart has: a mounted Secret in the pod
        // environment. Without this reader the auth Secret was mounted and read
        // by nothing, and `auth.mode: bearer` came up with an empty token set.
        with_env(&[("DFE_RECEIVER_BEARER_TOKENS", "alpha,beta")], || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            assert_eq!(
                exposed(&config.server.auth.bearer.tokens),
                ["alpha", "beta"]
            );
        });
    }

    /// The values behind a list of secrets.
    fn exposed(secrets: &[SensitiveString]) -> Vec<&str> {
        secrets.iter().map(SensitiveString::expose).collect()
    }

    #[test]
    fn bearer_tokens_from_the_environment_split_on_newlines_too() {
        // A K8s Secret holding one token per line is as likely as a CSV, and
        // BearerTokenProvider::load_tokens accepts both.
        with_env(
            &[("DFE_RECEIVER_BEARER_TOKENS", "alpha\n beta \n\ngamma")],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                assert_eq!(
                    exposed(&config.server.auth.bearer.tokens),
                    ["alpha", "beta", "gamma"],
                    "blank entries must not become empty tokens"
                );
            },
        );
    }

    #[test]
    fn bearer_tokens_from_config_survive_an_unset_environment() {
        with_env(&[], || {
            let mut config = Config::default();
            config.server.auth.bearer.tokens = vec!["from-yaml".into()];
            config.apply_flat_env(ENV_PREFIX);
            assert_eq!(exposed(&config.server.auth.bearer.tokens), ["from-yaml"]);
        });
    }

    #[test]
    fn test_env_override_dlq() {
        with_env(
            &[
                ("DFE_RECEIVER_DLQ_ENABLED", "true"),
                ("DFE_RECEIVER_DLQ_TOPIC", "dfe_receiver_dlq"),
                ("DFE_RECEIVER_DLQ_MODE", "kafka_only"),
            ],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                assert!(config.routing.dlq.enabled);
                assert_eq!(config.routing.dlq.topic, "dfe_receiver_dlq");
                assert_eq!(config.routing.dlq.mode, "kafka_only");
            },
        );
    }

    #[test]
    fn dlq_to_scalo_topic_selects_common_routing() {
        let cfg = DlqConfig {
            topic: "dfe_receiver_dlq".to_string(),
            ..DlqConfig::default()
        };
        let rc = cfg.to_scalo_config();
        assert_eq!(rc.kafka.common_topic, "dfe_receiver_dlq");
        assert_eq!(rc.kafka.routing, scalo::dlq::DlqRouting::Common);
    }

    #[test]
    fn dlq_to_scalo_empty_topic_keeps_per_table_routing() {
        let cfg = DlqConfig {
            topic: String::new(),
            ..DlqConfig::default()
        };
        let rc = cfg.to_scalo_config();
        assert_eq!(rc.kafka.routing, scalo::dlq::DlqRouting::PerTable);
    }

    #[test]
    fn dlq_to_scalo_unknown_mode_falls_back_to_cascade() {
        let cfg = DlqConfig {
            mode: "kafka-only".to_string(),
            ..DlqConfig::default()
        };
        let rc = cfg.to_scalo_config();
        assert_eq!(rc.mode, scalo::dlq::DlqMode::Cascade);
        assert!(rc.enabled);
    }

    #[test]
    fn dlq_kafka_send_timeout_reaches_the_scalo_kafka_backend() {
        let config: Config =
            serde_yaml_ng::from_str("routing:\n  dlq:\n    kafka_send_timeout_ms: 250\n").unwrap();
        let rc = config.routing.dlq.to_scalo_config();
        assert_eq!(rc.kafka.send_timeout_ms, 250);
    }

    #[test]
    fn an_unset_dlq_kafka_send_timeout_keeps_the_scalo_default() {
        let rc = DlqConfig::default().to_scalo_config();
        assert_eq!(
            rc.kafka.send_timeout_ms,
            scalo::dlq::KafkaDlqConfig::default().send_timeout_ms
        );
    }

    #[test]
    fn test_env_override_kafka_brokers() {
        with_env(
            &[("DFE_RECEIVER_KAFKA_BROKERS", "broker1:9092, broker2:9092")],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                assert_eq!(
                    config.kafka.brokers,
                    vec!["broker1:9092".to_string(), "broker2:9092".to_string()]
                );
            },
        );
    }

    #[test]
    fn test_env_override_kafka_client_id() {
        with_env(&[("DFE_RECEIVER_KAFKA_CLIENT_ID", "my-receiver")], || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            assert_eq!(config.kafka.client_id, "my-receiver");
        });
    }

    #[test]
    fn test_env_override_kafka_sasl() {
        with_env(
            &[
                ("DFE_RECEIVER_KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
                ("DFE_RECEIVER_KAFKA_SASL_USER", "admin"),
                ("DFE_RECEIVER_KAFKA_SASL_PASSWORD", "secret"),
            ],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                config.normalize();
                let sasl = config.kafka.sasl.unwrap();
                assert!(sasl.enabled);
                assert_eq!(sasl.mechanism, "SCRAM-SHA-512");
                assert_eq!(sasl.username, "admin");
                assert_eq!(sasl.password.expose(), "secret");
            },
        );
    }

    #[test]
    fn test_env_override_bearer_tokens() {
        with_env(
            &[("DFE_RECEIVER_BEARER_TOKENS", "tok-a, tok-b ,,tok-c")],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                assert_eq!(
                    exposed(&config.server.auth.bearer.tokens),
                    ["tok-a", "tok-b", "tok-c"]
                );
            },
        );
    }

    #[test]
    fn test_env_override_kafka_security_protocol() {
        // SSL variant
        with_env(
            &[("DFE_RECEIVER_KAFKA_SECURITY_PROTOCOL", "SASL_SSL")],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                assert!(config.kafka.tls.enabled);
            },
        );
        // PLAINTEXT variant
        with_env(
            &[("DFE_RECEIVER_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                assert!(!config.kafka.tls.enabled);
            },
        );
    }

    #[test]
    fn test_env_override_default_source() {
        with_env(&[("DFE_RECEIVER_DEFAULT_SOURCE", "firewall")], || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            assert_eq!(config.routing.default_source, "firewall");
        });
    }

    #[test]
    fn test_env_override_topic_suffix() {
        with_env(&[("DFE_RECEIVER_TOPIC_SUFFIX", "_raw")], || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            assert_eq!(config.routing.topic_suffix, "_raw");
        });
    }

    #[test]
    fn test_env_override_memory_limit() {
        with_env(&[("DFE_RECEIVER_MEMORY_LIMIT", "1073741824")], || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            assert_eq!(config.buffer.memory_limit, 1_073_741_824);
        });
    }

    #[test]
    fn test_env_override_pressure_threshold() {
        with_env(&[("DFE_RECEIVER_PRESSURE_THRESHOLD", "0.9")], || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            assert!((config.buffer.pressure_threshold - 0.9).abs() < f64::EPSILON);
        });
    }

    #[test]
    fn test_env_override_metrics_address() {
        with_env(&[("DFE_RECEIVER_METRICS_ADDRESS", "0.0.0.0:8888")], || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            assert_eq!(config.metrics.address, "0.0.0.0:8888");
        });
    }

    #[test]
    fn test_env_override_config_reload_secs() {
        with_env(&[("DFE_RECEIVER_CONFIG_RELOAD_SECS", "60")], || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            assert_eq!(config.config_reload_secs, 60);
        });
    }

    #[test]
    fn test_env_override_invalid_number_ignored() {
        with_env(&[("DFE_RECEIVER_MAX_BODY_SIZE", "not_a_number")], || {
            let mut config = Config::default();
            let original_size = config.server.max_body_size;
            config.apply_flat_env(ENV_PREFIX);
            assert_eq!(config.server.max_body_size, original_size);
        });
    }

    #[test]
    fn test_kafka_default_turns_stats_off() {
        // The sink's and the DLQ producer's contexts have no stats handler, so
        // librdkafka building the JSON is work nothing reads.
        let config = KafkaConfig::default();
        assert_eq!(
            config.librdkafka_overrides.get("statistics.interval.ms"),
            Some(&"0".to_string()),
        );
    }

    #[test]
    fn test_kafka_overrides_passed_to_scalo() {
        let mut config = KafkaConfig {
            brokers: vec!["localhost:9092".to_string()],
            ..KafkaConfig::default()
        };
        config
            .librdkafka_overrides
            .insert("message.max.bytes".to_string(), "2097152".to_string());

        let scalo = config.to_scalo_kafka_config_for_producer().unwrap();
        assert_eq!(
            scalo.librdkafka_overrides.get("statistics.interval.ms"),
            Some(&"0".to_string()),
        );
        assert_eq!(
            scalo.librdkafka_overrides.get("message.max.bytes"),
            Some(&"2097152".to_string()),
        );
    }

    #[test]
    fn test_kafka_yaml_overrides_stats() {
        let yaml = r#"
kafka:
  brokers:
    - "localhost:9092"
  librdkafka_overrides:
    statistics.interval.ms: "5000"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        let scalo = config.kafka.to_scalo_kafka_config_for_producer().unwrap();
        assert_eq!(
            scalo.librdkafka_overrides.get("statistics.interval.ms"),
            Some(&"5000".to_string()),
            "user config must override the default"
        );
    }

    /// Run `f` with scalo's app environment resolving from `app_env` alone.
    fn in_app_env<R>(app_env: Option<&str>, f: impl FnOnce() -> R) -> R {
        temp_env::with_vars(
            [("APP_ENV", app_env), ("ENVIRONMENT", None), ("ENV", None)],
            f,
        )
    }

    /// A bus config authenticating with SASL `mechanism`, over TLS or not.
    fn sasl_bus(mechanism: &str, tls: bool) -> Config {
        let mut config = Config::default();
        config.kafka.brokers = vec!["kafka:9092".to_string()];
        config.kafka.sasl = Some(SaslConfig {
            enabled: true,
            mechanism: mechanism.to_string(),
            username: "dfe".to_string(),
            password: SensitiveString::new("secret"),
        });
        config.kafka.tls.enabled = tls;
        config
    }

    /// A PLAIN password never crosses a plaintext transport, whatever the
    /// environment: startup refuses it and names the setting that fixes it.
    #[test]
    fn plain_over_a_plaintext_transport_is_refused_in_any_environment() {
        for app_env in [None, Some("development"), Some("production")] {
            in_app_env(app_env, || {
                let message = sasl_bus("plain", false)
                    .validate()
                    .expect_err("PLAIN over sasl_plaintext")
                    .to_string();
                assert!(
                    message.contains("SASL PLAIN requires security_protocol=sasl_ssl"),
                    "{app_env:?}: {message}"
                );
                assert!(message.contains("kafka.tls.enabled"), "{message}");
                assert!(
                    sasl_bus("plain", true).validate().is_ok(),
                    "{app_env:?}: PLAIN over TLS"
                );
            });
        }
    }

    /// SCRAM over a plaintext transport starts outside production. In
    /// production the unencrypted transport is refused, and the receiver has no
    /// `allow_insecure_transport` to opt back in.
    #[test]
    fn an_unencrypted_transport_is_refused_only_in_production() {
        in_app_env(None, || {
            assert!(sasl_bus("SCRAM-SHA-512", false).validate().is_ok());
        });
        in_app_env(Some("production"), || {
            let message = sasl_bus("SCRAM-SHA-512", false)
                .validate()
                .expect_err("sasl_plaintext in production")
                .to_string();
            assert!(message.contains("not permitted in production"), "{message}");
            assert!(sasl_bus("SCRAM-SHA-512", true).validate().is_ok());
        });
    }

    /// No brokers builds no Kafka client, so there is nothing to refuse.
    #[test]
    fn a_brokerless_config_skips_the_kafka_check() {
        let mut config = sasl_bus("plain", false);
        config.kafka.brokers.clear();
        config.destinations.default = "loader".into();
        config.loader.transport = "grpc".to_string();
        in_app_env(None, || assert!(config.validate().is_ok()));
    }

    /// The chart's shipped config, a plaintext broker with no SASL, still
    /// starts and builds a plaintext client.
    #[test]
    fn the_shipped_chart_default_still_starts() {
        let values: serde_json::Value =
            serde_yaml_ng::from_str(include_str!("../../chart/values.yaml")).unwrap();
        let config: Config = serde_json::from_value(values["config"].clone()).unwrap();
        assert_eq!(
            config.kafka.brokers,
            ["kafka:9092"],
            "the chart default moved"
        );
        assert!(config.kafka.sasl.is_none(), "the chart default moved");

        in_app_env(None, || {
            config.validate().expect("the shipped default starts");
            let client = config.kafka.to_scalo_kafka_config_for_producer().unwrap();
            assert_eq!(client.security_protocol, "plaintext");
            assert_eq!(client.sasl_mechanism, None);
        });
    }

    /// A provider preset replaces the transport the receiver derived.
    #[test]
    fn a_provider_preset_is_applied() {
        let mut client = KafkaConfig {
            brokers: vec!["kafka:9092".to_string()],
            ..KafkaConfig::default()
        }
        .to_scalo_config_with_suffix("");
        assert_eq!(client.security_protocol, "plaintext");
        client.provider = Some("strimzi".to_string());

        let checked = in_app_env(None, || checked_client_config(client)).unwrap();
        assert_eq!(checked.security_protocol, "sasl_ssl");
        assert_eq!(checked.sasl_mechanism.as_deref(), Some("SCRAM-SHA-512"));
    }

    #[test]
    fn an_unknown_provider_is_refused() {
        let mut client = KafkaConfig::default().to_scalo_config_with_suffix("");
        client.provider = Some("kinesis".to_string());

        let message = in_app_env(None, || checked_client_config(client))
            .expect_err("an unknown provider")
            .to_string();
        assert!(
            message.contains(r#"unknown kafka provider "kinesis""#),
            "{message}"
        );
    }

    /// A config written for a release that had `validation.require_json` still
    /// loads, and a body that is not JSON is refused whatever it said.
    #[test]
    fn a_config_setting_the_removed_require_json_still_loads() {
        let yaml = r#"
validation:
  require_json: false
  required_fields: ["org_id"]
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(
            config.validation.required_fields,
            vec!["org_id".to_string()]
        );
        let validator = crate::validation::Validator::new(config.validation);
        assert!(matches!(
            validator.validate(&bytes::Bytes::from("not json")),
            crate::validation::ValidationResult::NotJson(_)
        ));
    }

    #[test]
    fn test_kafka_yaml_other_override_loses_stats_default() {
        // When user provides librdkafka_overrides in YAML, serde replaces
        // the entire map -- the default stats.interval.ms=0 is NOT merged.
        // This is acceptable: users who set overrides are advanced and can
        // add statistics.interval.ms themselves if needed.
        let yaml = r#"
kafka:
  brokers:
    - "localhost:9092"
  librdkafka_overrides:
    message.max.bytes: "2097152"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(
            config
                .kafka
                .librdkafka_overrides
                .get("statistics.interval.ms"),
            None,
            "serde replaces default map -- only user-specified keys present"
        );
    }

    #[test]
    fn test_env_override_no_vars_set() {
        // Must go through temp_env even though it sets nothing: temp_env's
        // guard is what serialises against the other env tests, and reading
        // the process env unguarded means a concurrent with_env sets the very
        // variables this asserts are absent.
        temp_env::with_vars(
            [
                ("DFE_RECEIVER_BIND_ADDRESS", None::<&str>),
                ("DFE_RECEIVER_KAFKA_BROKERS", None),
                ("DFE_RECEIVER_DEFAULT_SOURCE", None),
                ("DFE_RECEIVER_CONFIG_RELOAD_SECS", None),
            ],
            || {
                let mut config = Config::default();
                let original = config.clone();
                config.apply_flat_env(ENV_PREFIX);
                config.normalize();
                assert_eq!(config.server.bind_address, original.server.bind_address);
                assert_eq!(config.kafka.brokers, original.kafka.brokers);
                assert_eq!(
                    config.routing.default_source,
                    original.routing.default_source
                );
                assert_eq!(config.config_reload_secs, original.config_reload_secs);
            },
        );
    }

    // ---------------------------------------------------------------------
    // Raw capture: flat env at both cascade levels
    // ---------------------------------------------------------------------

    #[test]
    fn test_env_override_raw_capture_common() {
        with_env(
            &[
                ("DFE_RECEIVER_RAW_CAPTURE_ENABLED", "true"),
                ("DFE_RECEIVER_RAW_CAPTURE_MAX_BYTES", "2048"),
                ("DFE_RECEIVER_RAW_CAPTURE_ON_OVERSIZE", "omit"),
            ],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);

                let resolved = config.raw_capture_for(&config.syslog.raw_capture);
                assert!(resolved.enabled);
                assert_eq!(resolved.max_bytes, 2048);
                assert_eq!(resolved.on_oversize, OversizePolicy::Omit);
            },
        );
    }

    #[test]
    fn test_env_override_raw_capture_per_transport() {
        with_env(
            &[("DFE_RECEIVER_SYSLOG_RAW_CAPTURE_ENABLED", "true")],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);

                // Only the named transport is switched on; the rest still inherit
                // the common block, which is still off.
                assert!(config.raw_capture_for(&config.syslog.raw_capture).enabled);
                assert!(!config.raw_capture_for(&config.gelf.raw_capture).enabled);
            },
        );
    }

    #[test]
    fn test_env_override_raw_capture_transport_beats_common() {
        with_env(
            &[
                ("DFE_RECEIVER_RAW_CAPTURE_ENABLED", "true"),
                ("DFE_RECEIVER_GELF_RAW_CAPTURE_ENABLED", "false"),
            ],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);

                assert!(config.raw_capture_for(&config.syslog.raw_capture).enabled);
                assert!(!config.raw_capture_for(&config.gelf.raw_capture).enabled);
            },
        );
    }

    #[test]
    fn test_env_override_raw_capture_unknown_policy_keeps_current() {
        // A typo must not silently switch to truncate and retain payloads the
        // operator asked to drop.
        with_env(
            &[
                ("DFE_RECEIVER_RAW_CAPTURE_ON_OVERSIZE", "omit"),
                ("DFE_RECEIVER_SYSLOG_RAW_CAPTURE_ON_OVERSIZE", "drop"),
            ],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);

                assert!(config.syslog.raw_capture.on_oversize.is_none());
                assert_eq!(
                    config
                        .raw_capture_for(&config.syslog.raw_capture)
                        .on_oversize,
                    OversizePolicy::Omit
                );
            },
        );
    }

    // ---------------------------------------------------------------------
    // Security: SASL password redaction in Debug output
    // ---------------------------------------------------------------------

    #[test]
    fn bearer_debug_redacts_tokens() {
        let bearer = BearerConfig {
            tokens: vec!["super-secret-token".into(), "another".into()],
            secret_source: Some("file:/etc/secrets/tokens".to_string()),
            refresh_interval_secs: 300,
        };
        let debug_output = format!("{bearer:?}");

        assert!(
            !debug_output.contains("super-secret-token") && !debug_output.contains("another"),
            "token values must not appear in debug: {debug_output}"
        );
        assert!(
            debug_output.contains('2'),
            "token count should be visible: {debug_output}"
        );
        assert!(
            debug_output.contains("file:/etc/secrets/tokens"),
            "secret_source should stay visible: {debug_output}"
        );
    }

    #[test]
    fn bearer_tokens_env_accepts_newline_separated_secrets() {
        with_env(
            &[("DFE_RECEIVER_BEARER_TOKENS", "tok-a\ntok-b\n\ntok-c")],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                assert_eq!(
                    exposed(&config.server.auth.bearer.tokens),
                    ["tok-a", "tok-b", "tok-c"]
                );
            },
        );
    }

    #[test]
    fn test_sasl_debug_redacts_password() {
        let sasl = SaslConfig {
            enabled: true,
            mechanism: "SCRAM-SHA-512".to_string(),
            username: "kafka-admin".to_string(),
            password: "super-secret-production-password".into(),
        };
        let debug_output = format!("{sasl:?}");

        // Username and mechanism remain visible for operational debugging
        assert!(
            debug_output.contains("kafka-admin"),
            "username should be visible in debug: {debug_output}"
        );
        assert!(
            debug_output.contains("SCRAM-SHA-512"),
            "mechanism should be visible in debug: {debug_output}"
        );

        // Password must never appear, even partially
        assert!(
            !debug_output.contains("super-secret-production-password"),
            "password leaked in debug output: {debug_output}"
        );
        assert!(
            !debug_output.contains("super-secret"),
            "password prefix leaked in debug output: {debug_output}"
        );
        assert!(
            debug_output.contains("REDACTED"),
            "debug should explicitly mark redacted field: {debug_output}"
        );
    }

    #[test]
    fn test_sasl_debug_redacts_empty_password() {
        // Edge case: empty password is still redacted (never expose field content)
        let sasl = SaslConfig {
            enabled: false,
            mechanism: String::new(),
            username: String::new(),
            password: SensitiveString::default(),
        };
        let debug_output = format!("{sasl:?}");
        assert!(debug_output.contains("REDACTED"));
        // Empty password must not render as `password: ""` anywhere
        assert!(!debug_output.contains("password: \"\""));
    }

    #[test]
    fn test_sasl_debug_nested_in_kafka_config() {
        // Verify redaction survives nested Debug formatting
        let kafka = KafkaConfig {
            sasl: Some(SaslConfig {
                enabled: true,
                mechanism: "PLAIN".to_string(),
                username: "u".to_string(),
                password: "leakable-password-xyz".into(),
            }),
            ..KafkaConfig::default()
        };
        let debug_output = format!("{kafka:?}");
        assert!(!debug_output.contains("leakable-password-xyz"));
        assert!(debug_output.contains("REDACTED"));
    }

    // ---------------------------------------------------------------------
    // The receiver's `scaling:` YAML reaches the scalo ScalingPressure model.
    //
    // scalo 2.9 dropped the old transport/params horizontal-engine config
    // (`ScalingEngineConfig`) for a weighted-component `ScalingPressure`. The
    // receiver feeds those components onto the runtime's shared engine (served
    // at `/scaling/pressure` to KEDA) via `ServiceApp::scaling_components`.
    // This loads a real `--config` file -- the deployed k8s path -- and asserts
    // the section yields the expected weighted components. No mocks.
    // ---------------------------------------------------------------------

    #[test]
    fn test_scaling_section_yields_pressure_components() {
        let dir = std::env::temp_dir().join(format!(
            "dfe-receiver-scaling-cascade-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("config.yaml");
        std::fs::write(
            &file,
            r"
scaling:
  enabled: true
  weight_request_rate: 0.30
  saturation_request_rate: 100000.0
  memory_gate_threshold: 0.8
",
        )
        .unwrap();

        let config = Config::load_from_file(file.to_str().unwrap())
            .expect("load receiver config from --config file");

        assert!(config.scaling.enabled);
        assert!((config.scaling.memory_gate_threshold - 0.8).abs() < f64::EPSILON);

        // The components the runtime's ScalingPressure registers for KEDA.
        let components = config.scaling.components();
        let names: Vec<&str> = components.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"request_rate"));
        assert!(names.contains(&"queue_depth"));
        assert!(names.contains(&"connections"));
        assert!(names.contains(&"memory"));
        assert!(names.contains(&"spill"));
        // The receiver is a push originator: no inbound Kafka lag component.
        assert!(!names.contains(&"lag"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_receiver_scaling_config_tolerates_engine_keys() {
        // The receiver's OWN ScalingConfig must still deserialise when the YAML
        // also carries the engine's transport/params keys (no deny_unknown).
        let yaml = r"
scaling:
  enabled: true
  memory_gate_threshold: 0.8
  transport:
    inbound: http
    outbound: kafka
  params:
    cpu_target: 0.70
";
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(config.scaling.enabled);
        assert!((config.scaling.memory_gate_threshold - 0.8).abs() < f64::EPSILON);
        // Legacy weighted defaults preserved (the engine keys were ignored).
        assert!((config.scaling.weight_request_rate - 0.30).abs() < f64::EPSILON);
    }

    /// Every key the example config sets under `scaling:` is one the receiver
    /// reads, so the example offers no knob that tunes nothing.
    #[test]
    fn every_scaling_key_in_the_example_is_read() {
        let example: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(include_str!("../../config.example.yaml")).unwrap();
        let read = serde_yaml_ng::to_value(ScalingConfig::default()).unwrap();
        let read = read.as_mapping().unwrap();
        let example = example["scaling"]
            .as_mapping()
            .expect("the example has a scaling section");

        let unread: Vec<&serde_yaml_ng::Value> = example
            .keys()
            .filter(|key| !read.contains_key(*key))
            .collect();
        assert!(
            unread.is_empty(),
            "example scaling keys nothing reads: {unread:?}"
        );
    }

    /// The example config parses and validates as the receiver loads it.
    #[test]
    fn the_example_config_loads() {
        let config: Config =
            serde_yaml_ng::from_str(include_str!("../../config.example.yaml")).unwrap();
        config.validate().unwrap();
    }
}
