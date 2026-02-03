// Project:   dfe-receiver
// File:      src/config/mod.rs
// Purpose:   Configuration loading and validation
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

//! Configuration management using hs-rustlib's 7-layer cascade.
//!
//! Priority (highest to lowest):
//! 1. CLI arguments
//! 2. Environment variables (RECEIVER_*)
//! 3. .env file
//! 4. settings.{env}.yaml
//! 5. settings.yaml
//! 6. defaults.yaml
//! 7. Hard-coded defaults

use std::collections::HashMap;
use std::path::PathBuf;

use hs_rustlib::config::{self, ConfigOptions};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Environment variable prefix for configuration.
pub const ENV_PREFIX: &str = "RECEIVER";

/// Main configuration struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// HTTP server configuration.
    pub server: ServerConfig,

    /// gRPC server configuration.
    pub grpc: GrpcConfig,

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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            grpc: GrpcConfig::default(),
            validation: ValidationConfig::default(),
            routing: RoutingConfig::default(),
            destinations: DestinationsConfig::default(),
            kafka: KafkaConfig::default(),
            loader: LoaderConfig::default(),
            buffer: BufferConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration using hs-rustlib's 7-layer cascade.
    ///
    /// Layers (highest to lowest priority):
    /// 1. CLI arguments (merged separately)
    /// 2. Environment variables (RECEIVER_*)
    /// 3. .env file
    /// 4. settings.{env}.yaml
    /// 5. settings.yaml
    /// 6. defaults.yaml
    /// 7. Hard-coded defaults
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        let mut config_paths = Vec::new();

        // Add explicit config path if provided
        if let Some(path) = config_path {
            config_paths.push(
                PathBuf::from(path)
                    .parent()
                    .unwrap_or(&PathBuf::from("."))
                    .to_path_buf(),
            );
        }

        // Setup hs-rustlib config with our prefix
        config::setup(ConfigOptions {
            env_prefix: ENV_PREFIX.to_string(),
            config_paths,
            load_dotenv: true,
            ..Default::default()
        })
        .map_err(|e| Error::Config(format!("failed to setup config: {e}")))?;

        // Get the global config and unmarshal to our struct
        let cfg = config::get();

        // Try to unmarshal the full config, falling back to defaults
        let config: Config = cfg.unmarshal().unwrap_or_default();

        Ok(config)
    }

    /// Load configuration from a YAML file directly (for backwards compatibility).
    pub fn load_from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("failed to read config file: {e}")))?;

        let config: Config = serde_yaml::from_str(&content)?;
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

    /// Legacy: Single header name for header-based auth.
    /// Deprecated: Use `accepted_headers` instead.
    #[serde(default)]
    pub header_name: String,

    /// Legacy: Allowed header values for single header.
    /// Deprecated: Use `accepted_headers` instead.
    #[serde(default)]
    pub header_values: Vec<String>,
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
    /// Header name (e.g., "x-hypersec-agent", "Authorization").
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
            accepted_headers: vec![AcceptedHeader {
                name: "x-hypersec-agent".to_string(),
                values: vec!["1.0".to_string()],
            }],
            bearer: BearerConfig::default(),
            // Legacy fields for backwards compatibility
            header_name: String::new(),
            header_values: Vec::new(),
        }
    }
}

impl AuthConfig {
    /// Get effective accepted headers (merges legacy config if present).
    pub fn effective_headers(&self) -> Vec<AcceptedHeader> {
        let mut headers = self.accepted_headers.clone();

        // Add legacy header if configured and not empty
        if !self.header_name.is_empty() {
            // Check if already in accepted_headers
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
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:6000".to_string(),
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

/// Routing configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutingConfig {
    /// Fields to check for topic name (priority order).
    pub topic_fields: Vec<String>,

    /// Default topic if no field matches.
    pub default_topic: String,

    /// Suffix to append to topic names.
    pub topic_suffix: String,

    /// Category to topic mapping.
    pub category_to_topic: HashMap<String, String>,

    /// DLQ configuration.
    pub dlq: DlqConfig,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            topic_fields: vec![
                "tags.event.category".to_string(),
                "event_category".to_string(),
            ],
            default_topic: "unmatched".to_string(),
            topic_suffix: "_land".to_string(),
            category_to_topic: HashMap::new(),
            dlq: DlqConfig::default(),
        }
    }
}

/// DLQ (Dead Letter Queue) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DlqConfig {
    /// Enable DLQ.
    pub enabled: bool,

    /// DLQ topic name.
    pub topic: String,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            topic: "dlq_land".to_string(),
        }
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
    /// Loader address.
    pub address: String,

    /// Transport type (kafka, zenoh, memory).
    pub transport: String,

    /// Connection timeout in milliseconds.
    pub timeout_ms: u64,
}

impl Default for LoaderConfig {
    fn default() -> Self {
        Self {
            address: "dfe-loader:9000".to_string(),
            transport: "kafka".to_string(),
            timeout_ms: 5000,
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
