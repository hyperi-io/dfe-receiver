// Project:   dfe-receiver
// File:      tests/common/mod.rs
// Purpose:   Shared test infrastructure — dual-mode (remote/docker) config helpers
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Test infrastructure helpers for dual-mode integration tests.
//!
//! Supports two backends controlled by `TEST_MODE` in `.env`:
//! - `remote` (default) — DevEx cluster via env vars (KAFKA_BROKERS, etc.)
//! - `docker` — dfe-docker infra profile (localhost:19092, no auth)

use std::env;

/// Test backend mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestMode {
    /// Use remote endpoints from env vars. Skip if unreachable.
    Remote,
    /// Use dfe-docker infra profile (localhost, no auth, no TLS).
    Docker,
}

impl TestMode {
    pub fn detect() -> Self {
        load_dotenv();
        match env::var("TEST_MODE").unwrap_or_default().as_str() {
            "docker" => Self::Docker,
            _ => Self::Remote,
        }
    }
}

/// Load .env file (idempotent, errors ignored).
pub fn load_dotenv() {
    let _ = dotenvy::dotenv();
}

// ---------------------------------------------------------------------------
// Kafka
// ---------------------------------------------------------------------------

/// Kafka connection config for the active test mode.
pub struct KafkaTestConfig {
    pub brokers: String,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_user: Option<String>,
    pub sasl_password: Option<String>,
}

impl KafkaTestConfig {
    /// Apply SASL settings to an rdkafka ClientConfig (if configured).
    pub fn apply_sasl(&self, config: &mut rdkafka::ClientConfig) {
        config.set("security.protocol", &self.security_protocol);
        if let Some(ref mechanism) = self.sasl_mechanism {
            config.set("sasl.mechanism", mechanism);
        }
        if let Some(ref user) = self.sasl_user {
            config.set("sasl.username", user);
        }
        if let Some(ref password) = self.sasl_password {
            config.set("sasl.password", password);
        }
    }

    /// Check if broker is reachable via TCP.
    pub fn is_reachable(&self) -> bool {
        use std::net::ToSocketAddrs;
        let first = self.brokers.split(',').next().unwrap_or(&self.brokers);
        first
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.next())
            .map(|a| {
                std::net::TcpStream::connect_timeout(&a, std::time::Duration::from_secs(3)).is_ok()
            })
            .unwrap_or(false)
    }
}

/// Returns Kafka connection config for the active test mode.
///
/// Docker mode: `localhost:19092`, PLAINTEXT, no SASL.
/// Remote mode: from env vars (KAFKA_BROKERS, KAFKA_SASL_*, etc.)
pub fn kafka_test_config() -> KafkaTestConfig {
    load_dotenv();
    match TestMode::detect() {
        TestMode::Docker => KafkaTestConfig {
            brokers: "localhost:19092".into(),
            security_protocol: "PLAINTEXT".into(),
            sasl_mechanism: None,
            sasl_user: None,
            sasl_password: None,
        },
        TestMode::Remote => KafkaTestConfig {
            brokers: env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into()),
            security_protocol: env::var("KAFKA_SECURITY_PROTOCOL")
                .unwrap_or_else(|_| "SASL_PLAINTEXT".into()),
            sasl_mechanism: env::var("KAFKA_SASL_MECHANISM").ok(),
            sasl_user: env::var("KAFKA_SASL_USER").ok(),
            sasl_password: env::var("KAFKA_SASL_PASSWORD").ok(),
        },
    }
}

/// Get test topic prefix from environment or use default.
pub fn test_topic_prefix() -> String {
    load_dotenv();
    env::var("TEST_TOPIC_PREFIX").unwrap_or_else(|_| "dfe-receiver-test".into())
}

/// Create a unique test topic name.
pub fn test_topic(suffix: &str) -> String {
    format!(
        "{}-{}-{}",
        test_topic_prefix(),
        suffix,
        uuid::Uuid::new_v4()
    )
}

impl KafkaTestConfig {
    /// Build a `dfe_receiver::config::KafkaConfig` from the test config.
    pub fn to_receiver_kafka_config(&self) -> dfe_receiver::config::KafkaConfig {
        let mut config = dfe_receiver::config::KafkaConfig::default();
        config.brokers = self
            .brokers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        config.client_id = "dfe-receiver-test".to_string();

        if let (Some(mechanism), Some(user), Some(password)) =
            (&self.sasl_mechanism, &self.sasl_user, &self.sasl_password)
        {
            config.sasl = Some(dfe_receiver::config::SaslConfig {
                enabled: true,
                mechanism: mechanism.clone(),
                username: user.clone(),
                password: password.clone(),
            });
        }

        config
    }

    /// Build a `dfe_receiver::config::Config` with Kafka configured.
    pub fn to_receiver_config(&self) -> dfe_receiver::config::Config {
        let mut config = dfe_receiver::config::Config::default();
        config.kafka = self.to_receiver_kafka_config();
        config
    }
}

/// Skip test if Kafka is not available in the current test mode.
#[macro_export]
macro_rules! skip_if_no_kafka {
    () => {
        let kf = $crate::common::kafka_test_config();
        if !kf.is_reachable() {
            eprintln!(
                "Skipping: Kafka not reachable at {} (TEST_MODE={:?})",
                kf.brokers,
                $crate::common::TestMode::detect()
            );
            return;
        }
    };
}
