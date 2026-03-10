// Project:   dfe-receiver
// File:      src/config/mod.rs
// Purpose:   Configuration loading and validation
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration management using hyperi-rustlib's 7-layer cascade.
//!
//! Priority (highest to lowest):
//! 1. CLI arguments
//! 2. Environment variables (DFE_RECEIVER_*)
//! 3. .env file
//! 4. settings.{env}.yaml
//! 5. settings.yaml
//! 6. defaults.yaml
//! 7. Hard-coded defaults

mod shared;

pub use shared::SharedConfig;

use std::collections::HashMap;

use hyperi_rustlib::config::{self, ConfigOptions};
use serde::{Deserialize, Serialize};
use tracing::debug;

use hyperi_rustlib::scaling::{ScalingComponent, ScalingPressure, ScalingPressureConfig};

use crate::error::{Error, Result};

/// Environment variable prefix for configuration.
pub const ENV_PREFIX: &str = "DFE_RECEIVER";

/// Common header name injected when `include_common_header` is enabled.
pub const COMMON_HEADER_NAME: &str = "x-hyperi-agent";

/// Common header value for the injected header.
pub const COMMON_HEADER_VALUE: &str = "1.0";

/// Main configuration struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Debug file sink — writes all processed messages to a file.
    pub file_sink: FileSinkConfig,

    /// External protocol plugins.
    #[cfg(feature = "plugins")]
    pub plugins: PluginsConfig,
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
            #[cfg(feature = "plugins")]
            plugins: PluginsConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration with cascade: CLI → ENV → .env → file → defaults
    ///
    /// Priority (highest to lowest):
    /// 1. CLI arguments (handled by caller, merged after)
    /// 2. Environment variables (DFE_RECEIVER_ prefix)
    /// 3. .env file (loaded by dotenvy via hyperi-rustlib)
    /// 4. Config file (YAML)
    /// 5. Hard-coded defaults
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        // If an explicit config file is provided, load it directly
        if let Some(path) = config_path {
            return Self::load_from_file(path);
        }

        // Otherwise, use hyperi-rustlib's 7-layer cascade
        config::setup(ConfigOptions {
            env_prefix: ENV_PREFIX.to_string(),
            config_paths: Vec::new(),
            load_dotenv: true,
            ..Default::default()
        })
        .map_err(|e| Error::Config(format!("failed to setup config: {e}")))?;

        // Get the global config and unmarshal to our struct
        let cfg = config::get();

        // Try to unmarshal the full config, falling back to defaults
        let mut config: Config = cfg.unmarshal().unwrap_or_default();

        // Store config path for reload support
        config.config_path = config_path.map(String::from);

        // Apply flat env var overrides (DFE_RECEIVER_*)
        apply_env_overrides(&mut config);

        Ok(config)
    }

    /// Load configuration from a YAML file directly (for backwards compatibility).
    pub fn load_from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("failed to read config file: {e}")))?;

        let mut config: Config = serde_yaml_ng::from_str(&content)?;
        config.config_path = Some(path.to_string());
        apply_env_overrides(&mut config);
        Ok(config)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        // Validate server config
        if self.server.bind_address.is_empty() {
            return Err(Error::Config("server.bind_address is required".into()));
        }

        // Validate Kafka config if Kafka destination enabled
        if self.destinations.default == "kafka" && self.kafka.brokers.is_empty() {
            return Err(Error::Config(
                "kafka.brokers is required when using Kafka destination".into(),
            ));
        }

        // Validate buffer config
        if self.buffer.pressure_threshold < 0.0 || self.buffer.pressure_threshold > 1.0 {
            return Err(Error::Config(
                "buffer.pressure_threshold must be between 0.0 and 1.0".into(),
            ));
        }

        Ok(())
    }
}

/// Reload configuration from the same source.
///
/// Re-runs the full cascade (file + env overrides + validate) using
/// the original config path. Used for hot-reload via SIGHUP or periodic timer.
pub fn reload_config(current: &Config) -> Result<Config> {
    Config::load(current.config_path.as_deref())
}

/// Read an env var with the DFE_RECEIVER_ prefix.
fn env_var(name: &str) -> std::result::Result<String, std::env::VarError> {
    std::env::var(format!("DFE_RECEIVER_{name}"))
}

/// Apply flat environment variable overrides (DFE_RECEIVER_* prefix).
///
/// Provides operator-friendly flat env vars alongside rustlib's nested `__` cascade.
/// Flat overrides take final priority (applied after rustlib config merge).
fn apply_env_overrides(config: &mut Config) {
    // Server
    if let Ok(v) = env_var("BIND_ADDRESS") {
        config.server.bind_address = v;
        debug!("Override: server.bind_address from env");
    }
    if let Ok(v) = env_var("MAX_BODY_SIZE")
        && let Ok(n) = v.parse()
    {
        config.server.max_body_size = n;
        debug!("Override: server.max_body_size from env");
    }
    if let Ok(v) = env_var("REQUEST_TIMEOUT_MS")
        && let Ok(n) = v.parse()
    {
        config.server.request_timeout_ms = n;
        debug!("Override: server.request_timeout_ms from env");
    }

    // Common header / enrichment toggle
    if let Ok(v) = env_var("COMMON_HEADER") {
        config.server.auth.include_common_header =
            matches!(v.to_lowercase().as_str(), "true" | "1" | "yes");
        debug!("Override: include_common_header from env");
    }

    // Kafka
    if let Ok(v) = env_var("KAFKA_BROKERS") {
        config.kafka.brokers = v.split(',').map(|s| s.trim().to_string()).collect();
        debug!("Override: kafka.brokers from env");
    }
    if let Ok(v) = env_var("KAFKA_CLIENT_ID") {
        config.kafka.client_id = v;
        debug!("Override: kafka.client_id from env");
    }
    if let Ok(v) = env_var("KAFKA_SASL_MECHANISM") {
        let sasl = config.kafka.sasl.get_or_insert_with(|| SaslConfig {
            enabled: true,
            mechanism: String::new(),
            username: String::new(),
            password: String::new(),
        });
        sasl.mechanism = v;
        sasl.enabled = true;
        debug!("Override: kafka.sasl.mechanism from env");
    }
    if let Ok(v) = env_var("KAFKA_SECURITY_PROTOCOL") {
        config.kafka.tls.enabled = v.to_uppercase().contains("SSL");
        debug!("Override: kafka.tls from env (protocol={v})");
    }
    if let Ok(v) = env_var("KAFKA_SASL_USER") {
        let sasl = config.kafka.sasl.get_or_insert_with(|| SaslConfig {
            enabled: true,
            mechanism: String::new(),
            username: String::new(),
            password: String::new(),
        });
        sasl.username = v;
        debug!("Override: kafka.sasl.username from env");
    }
    if let Ok(v) = env_var("KAFKA_SASL_PASSWORD") {
        let sasl = config.kafka.sasl.get_or_insert_with(|| SaslConfig {
            enabled: true,
            mechanism: String::new(),
            username: String::new(),
            password: String::new(),
        });
        sasl.password = v;
        debug!("Override: kafka.sasl.password from env (redacted)");
    }

    // Routing
    if let Ok(v) = env_var("DEFAULT_SOURCE") {
        config.routing.default_source = v;
        debug!("Override: routing.default_source from env");
    }
    if let Ok(v) = env_var("TOPIC_SUFFIX") {
        config.routing.topic_suffix = v;
        debug!("Override: routing.topic_suffix from env");
    }

    // Buffer / memory
    if let Ok(v) = env_var("MEMORY_LIMIT")
        && let Ok(n) = v.parse()
    {
        config.buffer.memory_limit = n;
        debug!("Override: buffer.memory_limit from env");
    }
    if let Ok(v) = env_var("PRESSURE_THRESHOLD")
        && let Ok(n) = v.parse()
    {
        config.buffer.pressure_threshold = n;
        debug!("Override: buffer.pressure_threshold from env");
    }

    // Metrics
    if let Ok(v) = env_var("METRICS_ADDRESS") {
        config.metrics.address = v;
        debug!("Override: metrics.address from env");
    }

    // Scaling pressure
    if let Ok(v) = env_var("SCALING_ENABLED") {
        config.scaling.enabled = matches!(v.to_lowercase().as_str(), "true" | "1" | "yes");
        debug!("Override: scaling.enabled from env");
    }
    if let Ok(v) = env_var("SCALING_MEMORY_GATE_THRESHOLD")
        && let Ok(n) = v.parse()
    {
        config.scaling.memory_gate_threshold = n;
        debug!("Override: scaling.memory_gate_threshold from env");
    }

    // Config reload
    if let Ok(v) = env_var("CONFIG_RELOAD_SECS")
        && let Ok(n) = v.parse()
    {
        config.config_reload_secs = n;
        debug!("Override: config_reload_secs from env");
    }
}

/// HTTP server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Bind address (e.g., "0.0.0.0:443").
    pub bind_address: String,

    /// Maximum request body size in bytes.
    pub max_body_size: usize,

    /// Request timeout in milliseconds.
    pub request_timeout_ms: u64,

    /// TLS configuration.
    pub tls: TlsConfig,

    /// Authentication configuration.
    pub auth: AuthConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".to_string(),
            max_body_size: 10 * 1024 * 1024, // 10MB
            request_timeout_ms: 30_000,
            tls: TlsConfig::default(),
            auth: AuthConfig::default(),
        }
    }
}

/// TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Auth mode (none, header, bearer, mtls, both).
    pub mode: String,

    /// List of accepted headers (any one must match).
    /// Each entry defines a header name and its allowed values.
    pub accepted_headers: Vec<AcceptedHeader>,

    /// Bearer token configuration.
    pub bearer: BearerConfig,

    /// Include the common `x-hyperi-agent` header in accepted headers.
    /// Also gates source rule evaluation and timestamp enrichment.
    /// Default: true
    #[serde(default = "default_true")]
    pub include_common_header: bool,

    /// Legacy: Single header name for header-based auth.
    /// Deprecated: Use `accepted_headers` instead.
    #[serde(default)]
    pub header_name: String,

    /// Legacy: Allowed header values for single header.
    /// Deprecated: Use `accepted_headers` instead.
    #[serde(default)]
    pub header_values: Vec<String>,
}

fn default_true() -> bool {
    true
}

/// Bearer token authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BearerConfig {
    /// Static tokens (for dev/simple deployments).
    /// In production, use `secret_source` instead.
    #[serde(default)]
    pub tokens: Vec<String>,

    /// Secret source for dynamic token loading.
    /// Format: "provider:path:key" (e.g., "vault:secret/auth:bearer_tokens")
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

/// Defines an accepted authentication header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptedHeader {
    /// Header name (e.g., "x-hyperi-agent", "Authorization").
    pub name: String,

    /// Allowed values for this header.
    /// If empty, any non-empty value is accepted.
    #[serde(default)]
    pub values: Vec<String>,
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
    /// Get effective accepted headers (merges common + legacy headers).
    pub fn effective_headers(&self) -> Vec<AcceptedHeader> {
        let mut headers = self.accepted_headers.clone();

        // Inject common header if enabled and not already present
        if self.include_common_header {
            let already_exists = headers.iter().any(|h| h.name == COMMON_HEADER_NAME);
            if !already_exists {
                headers.push(AcceptedHeader {
                    name: COMMON_HEADER_NAME.to_string(),
                    values: vec![COMMON_HEADER_VALUE.to_string()],
                });
            }
        }

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

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
        }
    }
}

/// OTLP (OpenTelemetry Protocol) receiver configuration.
#[cfg(feature = "otlp")]
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

#[cfg(feature = "otlp")]
impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            grpc_bind_address: "0.0.0.0:4317".to_string(),
            http_bind_address: "0.0.0.0:4318".to_string(),
            mode: "hyperdx".to_string(),
            tls: TlsConfig::default(),
            auth: AuthConfig {
                mode: "none".to_string(),
                ..AuthConfig::default()
            },
        }
    }
}

/// Lumberjack v2 (Beats) protocol configuration.
///
/// Accepts data from Elastic Beats agents (Filebeat, Winlogbeat, etc.)
/// over the Lumberjack v2 wire protocol on TCP/TLS.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        }
    }
}

/// Splunk HEC (HTTP Event Collector) receiver configuration.
///
/// Accepts data from Splunk forwarders and HTTP clients over the HEC protocol.
/// Supports both `Authorization: Splunk <token>` and `Authorization: Bearer <token>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

impl Default for SplunkHecConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:8088".to_string(),
            max_body_size: 10 * 1024 * 1024,
            request_timeout_ms: 30_000,
            tls: TlsConfig::default(),
            auth: AuthConfig {
                mode: "none".to_string(),
                ..AuthConfig::default()
            },
        }
    }
}

/// Syslog receiver configuration (RFC 5424 + RFC 3164).
///
/// Accepts syslog messages over UDP, TCP, and TLS/TCP.
/// Auto-detects message format (RFC 5424 vs RFC 3164) per message.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

impl Default for SyslogConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            udp_bind_address: "0.0.0.0:514".to_string(),
            tcp_bind_address: "0.0.0.0:514".to_string(),
            tls_bind_address: "0.0.0.0:6514".to_string(),
            max_message_size: 64 * 1024,
            tls: TlsConfig::default(),
            auth: AuthConfig {
                mode: "none".to_string(),
                ..AuthConfig::default()
            },
        }
    }
}

/// Prometheus Remote Write receiver configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

impl Default for PrometheusRwConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:9090".to_string(),
            mode: "native".to_string(),
            max_body_size: 10 * 1024 * 1024,
            request_timeout_ms: 30_000,
            tls: TlsConfig::default(),
            auth: AuthConfig {
                mode: "none".to_string(),
                ..AuthConfig::default()
            },
        }
    }
}

/// Fluent Forward protocol configuration.
///
/// Accepts data from Fluentd and Fluent Bit agents over the Forward
/// protocol (msgpack over TCP) on the standard port 24224.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

impl Default for FluentConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:24224".to_string(),
            max_message_size: 32 * 1024 * 1024,
            tls: TlsConfig::default(),
        }
    }
}

/// GELF (Graylog Extended Log Format) receiver configuration.
///
/// Accepts GELF messages over TCP (null-byte delimited JSON)
/// on the standard port 12201.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

impl Default for GelfConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:12201".to_string(),
            max_message_size: 1024 * 1024,
            tls: TlsConfig::default(),
        }
    }
}

/// Validation configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ValidationConfig {
    /// Require JSON format.
    pub require_json: bool,

    /// Required fields (reject if missing).
    pub required_fields: Vec<String>,

    /// Send invalid messages to DLQ instead of rejecting.
    pub dlq_on_invalid: bool,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            require_json: true,
            required_fields: vec![],
            dlq_on_invalid: true,
        }
    }
}

/// Rule for determining `_source` value from JSON payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRule {
    /// JSON field path (dot notation for nested, e.g., "tags.event.category").
    pub field: String,

    /// Match mode: "key_present", "key_value_set", "key_value_use".
    ///
    /// - `key_present`: if field exists → `_source = source`
    /// - `key_value_set`: if field value == `match_value` → `_source = source`
    /// - `key_value_use`: if field exists → `_source = <field value>`
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
            default_source: "default".to_string(),
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DlqConfig {
    /// Enable DLQ.
    pub enabled: bool,

    /// Backend mode: cascade (default), fan_out, file_only, kafka_only.
    pub mode: String,

    /// DLQ topic name (used as common_topic for Kafka backend).
    pub topic: String,

    /// Topic suffix for per-table routing.
    pub topic_suffix: String,

    /// File backend settings.
    pub file_enabled: bool,
    pub file_path: String,

    /// Kafka backend settings.
    pub kafka_enabled: bool,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: "cascade".to_string(),
            topic: "dlq_land".to_string(),
            topic_suffix: ".dlq".to_string(),
            file_enabled: true,
            file_path: "/var/spool/dfe/dlq".to_string(),
            kafka_enabled: true,
        }
    }
}

impl DlqConfig {
    /// Convert to rustlib DlqConfig for the unified DLQ module.
    pub fn to_rustlib_config(&self) -> hyperi_rustlib::dlq::DlqConfig {
        use hyperi_rustlib::dlq::{DlqMode, FileDlqConfig, KafkaDlqConfig};

        let mode = match self.mode.as_str() {
            "fan_out" => DlqMode::FanOut,
            "file_only" => DlqMode::FileOnly,
            "kafka_only" => DlqMode::KafkaOnly,
            _ => DlqMode::Cascade,
        };

        hyperi_rustlib::dlq::DlqConfig {
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
                ..KafkaDlqConfig::default()
            },
        }
    }
}

impl KafkaConfig {
    /// Convert to rustlib transport KafkaConfig with a given client ID suffix.
    fn to_rustlib_config_with_suffix(
        &self,
        suffix: &str,
    ) -> hyperi_rustlib::transport::KafkaConfig {
        let mut config = hyperi_rustlib::transport::KafkaConfig {
            brokers: self.brokers.clone(),
            client_id: format!("{}{}", self.client_id, suffix),
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

        config
    }

    /// Convert to rustlib transport KafkaConfig for the main producer sink.
    pub fn to_rustlib_kafka_config_for_producer(&self) -> hyperi_rustlib::transport::KafkaConfig {
        self.to_rustlib_config_with_suffix("")
    }

    /// Convert to rustlib transport KafkaConfig for DLQ producer.
    pub fn to_rustlib_kafka_config(&self) -> hyperi_rustlib::transport::KafkaConfig {
        self.to_rustlib_config_with_suffix("-dlq")
    }
}

/// Destinations configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DestinationsConfig {
    /// Default destination (kafka or loader).
    pub default: String,

    /// Routing rules for destination selection.
    pub rules: Vec<DestinationRule>,
}

impl Default for DestinationsConfig {
    fn default() -> Self {
        Self {
            default: "kafka".to_string(),
            rules: vec![],
        }
    }
}

/// Destination routing rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationRule {
    /// Field to match.
    pub match_field: String,

    /// Value to match.
    pub match_value: String,

    /// Destination for matched messages.
    pub destination: String,
}

/// Kafka producer configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Producer-specific settings.
    pub producer: ProducerConfig,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: vec![],
            client_id: "dfe-receiver".to_string(),
            sasl: None,
            tls: KafkaTlsConfig::default(),
            producer: ProducerConfig::default(),
        }
    }
}

/// SASL authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaslConfig {
    /// Enable SASL.
    pub enabled: bool,

    /// SASL mechanism (plain, scram_sha_256, scram_sha_512).
    pub mechanism: String,

    /// Username.
    pub username: String,

    /// Password.
    pub password: String,
}

/// Kafka TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Kafka producer settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProducerConfig {
    /// Maximum batch size in bytes.
    pub batch_size: usize,

    /// Maximum messages per batch.
    pub batch_messages: usize,

    /// Linger time in milliseconds.
    pub linger_ms: u32,

    /// Compression type (none, gzip, snappy, lz4, zstd).
    pub compression: String,

    /// Acknowledgment level (0, 1, all).
    pub acks: String,

    /// Number of retries.
    pub retries: u32,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            batch_size: 8 * 1024 * 1024, // 8MiB
            batch_messages: 10_000,
            linger_ms: 20,
            compression: "lz4".to_string(),
            acks: "all".to_string(),
            retries: 5,
        }
    }
}

/// dfe-loader connection configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoaderConfig {
    /// Loader address (used for Kafka mode reference; override with grpc_endpoint for gRPC mode).
    pub address: String,

    /// Transport type (kafka, memory, grpc).
    pub transport: String,

    /// Connection timeout in milliseconds.
    pub timeout_ms: u64,

    /// gRPC endpoint URI for loader (only used when transport = "grpc").
    /// Defaults to http://{address} if not set.
    pub grpc_endpoint: Option<String>,
}

impl Default for LoaderConfig {
    fn default() -> Self {
        Self {
            address: "dfe-loader:9000".to_string(),
            transport: "kafka".to_string(),
            timeout_ms: 5000,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Buffer and memory configuration.
///
/// ## Design Decision: No Disk Spillover
///
/// Memory-only buffering is used because:
/// 1. K8s memory limits trigger OOMKill, which triggers KEDA scale-up
/// 2. Vector clients have their own disk buffers for retries
/// 3. Circuit breaker + 503 responses propagate backpressure upstream
/// 4. Disk I/O would bottleneck the hot path at PB/s scale
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BufferConfig {
    /// Maximum memory for buffers in bytes (0 = auto-detect 67% of available).
    pub memory_limit: usize,

    /// Memory pressure threshold (0.0-1.0).
    /// When usage exceeds this, backpressure is applied (503 responses).
    pub pressure_threshold: f64,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            memory_limit: 0, // Auto-detect
            pressure_threshold: 0.8,
        }
    }
}

/// Metrics configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Build a `ScalingPressure` engine from this config.
    #[must_use]
    pub fn build_pressure(&self) -> ScalingPressure {
        let base = ScalingPressureConfig {
            enabled: self.enabled,
            memory_gate_threshold: self.memory_gate_threshold,
        };
        let components = vec![
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
        ];
        ScalingPressure::new(base, components)
    }
}

/// External protocol plugin configuration.
///
/// Plugins are loaded as shared libraries (.so files) at startup.
/// Each plugin implements the dfe-protocol-sdk's `ProtocolPlugin` trait
/// and is loaded via the C ABI interface.
///
/// ## Configuration
///
/// ```yaml
/// plugins:
///   directory: "/opt/dfe/plugins"   # optional: auto-discover .so files
///   syslog:
///     path: "/opt/dfe/plugins/libdfe_receiver_plugin_syslog.so"
///     bind_address: "0.0.0.0:514"
///     topic: "syslog_land"
/// ```
///
/// The map key (e.g. `syslog`) is the plugin's logical name, used in
/// logs, metrics, and health checks. The `path` field is consumed by
/// the loader; all other fields are passed as JSON to the plugin's
/// `create()` function.
///
/// Environment variable overrides work naturally:
/// `RECEIVER_PLUGINS_SYSLOG_BIND_ADDRESS=0.0.0.0:1514`
#[cfg(feature = "plugins")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PluginsConfig {
    /// Directory to scan for plugin .so files (optional).
    /// All `.so` files in this directory will be loaded with default config.
    pub directory: Option<String>,

    /// Named plugin entries. Each key is the plugin's logical name.
    /// The entry must contain a `path` field; all other fields are
    /// passed through as config JSON to the plugin.
    #[serde(flatten)]
    pub plugins: HashMap<String, PluginEntry>,
}

/// A single plugin entry specifying the .so path and its configuration.
///
/// The `path` field is consumed by the loader. All other fields are
/// collected via `#[serde(flatten)]` and passed as JSON to the plugin.
#[cfg(feature = "plugins")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginEntry {
    /// Path to the .so file.
    pub path: String,

    /// All remaining fields are plugin-specific configuration,
    /// passed as JSON to the plugin's `create()` function.
    #[serde(flatten)]
    pub config: serde_json::Map<String, serde_json::Value>,
}

#[cfg(feature = "plugins")]
impl Default for PluginsConfig {
    fn default() -> Self {
        Self {
            directory: None,
            plugins: HashMap::new(),
        }
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
        assert_eq!(config.kafka.producer.batch_messages, 10_000);
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        // Default destination is kafka, so we need brokers
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_no_brokers_required_for_loader() {
        let mut config = Config::default();
        config.destinations.default = "loader".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_pressure_threshold() {
        let mut config = Config::default();
        config.buffer.pressure_threshold = 1.5;
        assert!(config.validate().is_err());
    }

    // -- env override tests --
    // Each test sets env vars, runs apply_env_overrides on a default config,
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
            apply_env_overrides(&mut config);
            assert_eq!(config.server.bind_address, "127.0.0.1:9999");
        });
    }

    #[test]
    fn test_env_override_max_body_size() {
        with_env(&[("DFE_RECEIVER_MAX_BODY_SIZE", "5242880")], || {
            let mut config = Config::default();
            apply_env_overrides(&mut config);
            assert_eq!(config.server.max_body_size, 5_242_880);
        });
    }

    #[test]
    fn test_env_override_request_timeout_ms() {
        with_env(&[("DFE_RECEIVER_REQUEST_TIMEOUT_MS", "60000")], || {
            let mut config = Config::default();
            apply_env_overrides(&mut config);
            assert_eq!(config.server.request_timeout_ms, 60_000);
        });
    }

    #[test]
    fn test_env_override_common_header() {
        // Test true
        with_env(&[("DFE_RECEIVER_COMMON_HEADER", "true")], || {
            let mut config = Config::default();
            config.server.auth.include_common_header = false;
            apply_env_overrides(&mut config);
            assert!(config.server.auth.include_common_header);
        });

        // Test false
        with_env(&[("DFE_RECEIVER_COMMON_HEADER", "false")], || {
            let mut config = Config::default();
            apply_env_overrides(&mut config);
            assert!(!config.server.auth.include_common_header);
        });
    }

    #[test]
    fn test_env_override_kafka_brokers() {
        with_env(
            &[("DFE_RECEIVER_KAFKA_BROKERS", "broker1:9092, broker2:9092")],
            || {
                let mut config = Config::default();
                apply_env_overrides(&mut config);
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
            apply_env_overrides(&mut config);
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
                apply_env_overrides(&mut config);
                let sasl = config.kafka.sasl.unwrap();
                assert!(sasl.enabled);
                assert_eq!(sasl.mechanism, "SCRAM-SHA-512");
                assert_eq!(sasl.username, "admin");
                assert_eq!(sasl.password, "secret");
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
                apply_env_overrides(&mut config);
                assert!(config.kafka.tls.enabled);
            },
        );
        // PLAINTEXT variant
        with_env(
            &[("DFE_RECEIVER_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")],
            || {
                let mut config = Config::default();
                apply_env_overrides(&mut config);
                assert!(!config.kafka.tls.enabled);
            },
        );
    }

    #[test]
    fn test_env_override_default_source() {
        with_env(&[("DFE_RECEIVER_DEFAULT_SOURCE", "firewall")], || {
            let mut config = Config::default();
            apply_env_overrides(&mut config);
            assert_eq!(config.routing.default_source, "firewall");
        });
    }

    #[test]
    fn test_env_override_topic_suffix() {
        with_env(&[("DFE_RECEIVER_TOPIC_SUFFIX", "_raw")], || {
            let mut config = Config::default();
            apply_env_overrides(&mut config);
            assert_eq!(config.routing.topic_suffix, "_raw");
        });
    }

    #[test]
    fn test_env_override_memory_limit() {
        with_env(&[("DFE_RECEIVER_MEMORY_LIMIT", "1073741824")], || {
            let mut config = Config::default();
            apply_env_overrides(&mut config);
            assert_eq!(config.buffer.memory_limit, 1_073_741_824);
        });
    }

    #[test]
    fn test_env_override_pressure_threshold() {
        with_env(&[("DFE_RECEIVER_PRESSURE_THRESHOLD", "0.9")], || {
            let mut config = Config::default();
            apply_env_overrides(&mut config);
            assert!((config.buffer.pressure_threshold - 0.9).abs() < f64::EPSILON);
        });
    }

    #[test]
    fn test_env_override_metrics_address() {
        with_env(&[("DFE_RECEIVER_METRICS_ADDRESS", "0.0.0.0:8888")], || {
            let mut config = Config::default();
            apply_env_overrides(&mut config);
            assert_eq!(config.metrics.address, "0.0.0.0:8888");
        });
    }

    #[test]
    fn test_env_override_config_reload_secs() {
        with_env(&[("DFE_RECEIVER_CONFIG_RELOAD_SECS", "60")], || {
            let mut config = Config::default();
            apply_env_overrides(&mut config);
            assert_eq!(config.config_reload_secs, 60);
        });
    }

    #[test]
    fn test_env_override_invalid_number_ignored() {
        with_env(&[("DFE_RECEIVER_MAX_BODY_SIZE", "not_a_number")], || {
            let mut config = Config::default();
            let original_size = config.server.max_body_size;
            apply_env_overrides(&mut config);
            assert_eq!(config.server.max_body_size, original_size);
        });
    }

    #[test]
    fn test_env_override_no_vars_set() {
        let mut config = Config::default();
        let original = config.clone();
        apply_env_overrides(&mut config);
        assert_eq!(config.server.bind_address, original.server.bind_address);
        assert_eq!(config.kafka.brokers, original.kafka.brokers);
        assert_eq!(
            config.routing.default_source,
            original.routing.default_source
        );
        assert_eq!(config.config_reload_secs, original.config_reload_secs);
    }
}
