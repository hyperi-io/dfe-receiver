// Project:   dfe-receiver
// File:      tests/common/mod.rs
// Purpose:   Shared test infrastructure — dual-mode (remote/docker) config helpers
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Test infrastructure helpers for dual-mode integration tests.
//!
//! Supports two backends controlled by `TEST_MODE` in `.env`:
//! - `remote` (default) — DevEx cluster via env vars (KAFKA_BROKERS, etc.)
//! - `docker` — dfe-docker infra profile (localhost:19092, no auth)

use std::env;

/// Test backend mode — live services preferred, testcontainers fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestMode {
    /// Use live endpoints from env vars / `.env` (preferred — more realistic
    /// and faster than spinning up containers). Falls back to `Testcontainers`
    /// if the live endpoint is unreachable or auth fails.
    Live,
    /// Use a local docker-compose stack from `docker-compose.test.yaml`.
    Docker,
    /// Use ephemeral testcontainers-rs containers (CI / no live infra).
    Testcontainers,
}

impl TestMode {
    /// Detect preferred test mode from env. Defaults to `Live`.
    pub fn detect() -> Self {
        load_dotenv();
        match env::var("TEST_MODE").unwrap_or_default().as_str() {
            "docker" => Self::Docker,
            "testcontainers" => Self::Testcontainers,
            _ => Self::Live,
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
/// Live mode: from env vars (KAFKA_BROKERS, KAFKA_SASL_*, etc.)
/// Docker mode: `localhost:19092`, PLAINTEXT, no SASL.
/// Testcontainers mode: callers should use [`start_kafka_container`] directly.
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
        TestMode::Live | TestMode::Testcontainers => KafkaTestConfig {
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
        let sasl = if let (Some(mechanism), Some(user), Some(password)) =
            (&self.sasl_mechanism, &self.sasl_user, &self.sasl_password)
        {
            Some(dfe_receiver::config::SaslConfig {
                enabled: true,
                mechanism: mechanism.clone(),
                username: user.clone(),
                password: password.clone(),
            })
        } else {
            None
        };

        // TLS is derived from security_protocol: SASL_SSL or SSL imply TLS on.
        let tls_enabled = matches!(
            self.security_protocol.to_uppercase().as_str(),
            "SASL_SSL" | "SSL"
        );

        dfe_receiver::config::KafkaConfig {
            brokers: self
                .brokers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            client_id: "dfe-receiver-test".to_string(),
            sasl,
            tls: dfe_receiver::config::KafkaTlsConfig {
                enabled: tls_enabled,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Build a `dfe_receiver::config::Config` with Kafka configured.
    pub fn to_receiver_config(&self) -> dfe_receiver::config::Config {
        dfe_receiver::config::Config {
            kafka: self.to_receiver_kafka_config(),
            ..Default::default()
        }
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

// ---------------------------------------------------------------------------
// Kafka consumer helper
// ---------------------------------------------------------------------------

/// Build a consumer subscribed to `topic` with a unique group. Caller can
/// then poll `recv()` for messages. The consumer must be created *before*
/// producing to avoid race conditions with librdkafka's metadata refresh.
pub fn kafka_consumer(
    cfg: &KafkaTestConfig,
    topic: &str,
) -> Option<rdkafka::consumer::StreamConsumer> {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", format!("test-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("allow.auto.create.topics", "true");

    cfg.apply_sasl(&mut client_config);

    let consumer: StreamConsumer = client_config.create().ok()?;
    consumer.subscribe(&[topic]).ok()?;
    Some(consumer)
}

/// Poll a consumer for a single message payload until timeout.
pub async fn kafka_consume_next(
    consumer: &rdkafka::consumer::StreamConsumer,
    timeout: std::time::Duration,
) -> Option<Vec<u8>> {
    use rdkafka::Message;

    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline - tokio::time::Instant::now();
        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(msg)) => return msg.payload().map(<[u8]>::to_vec),
            Ok(Err(_)) => {}
            Err(_) => return None,
        }
    }
    None
}

/// Create a consumer, subscribe, and poll until message found or timeout.
///
/// Note: caller should prefer [`kafka_consumer`] + [`kafka_consume_next`] when
/// the test controls the producer, so the consumer can subscribe *before*
/// the producer sends. This helper is for tests that don't control ordering.
pub async fn kafka_consume_one(
    cfg: &KafkaTestConfig,
    topic: &str,
    timeout: std::time::Duration,
) -> Option<Vec<u8>> {
    let consumer = kafka_consumer(cfg, topic)?;
    // Brief warm-up so consumer group is joined before producer sends
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    kafka_consume_next(&consumer, timeout).await
}

// ---------------------------------------------------------------------------
// MinIO / S3 (for disk spillover / archival tests)
// ---------------------------------------------------------------------------

/// S3-compatible storage connection config.
pub struct S3TestConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

impl S3TestConfig {
    /// Detect S3/MinIO availability from env vars or docker MinIO on localhost:9000.
    pub fn detect() -> Option<Self> {
        load_dotenv();
        // Env var set for remote S3
        if let Ok(endpoint) = env::var("S3_ENDPOINT") {
            return Some(Self {
                endpoint,
                access_key: env::var("S3_ACCESS_KEY").unwrap_or_default(),
                secret_key: env::var("S3_SECRET_KEY").unwrap_or_default(),
                region: env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
            });
        }
        // Fallback: try docker MinIO on localhost:9000
        if tcp_reachable("localhost:9000", std::time::Duration::from_secs(2)) {
            return Some(Self {
                endpoint: "http://localhost:9000".into(),
                access_key: "minioadmin".into(),
                secret_key: "minioadmin".into(),
                region: "us-east-1".into(),
            });
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Vault / OpenBao (for bearer-token / secret tests)
// ---------------------------------------------------------------------------

/// Vault connection config for dev-mode tests.
pub struct VaultTestConfig {
    pub address: String,
    pub token: String,
}

impl VaultTestConfig {
    /// Detect Vault/OpenBao availability from env vars or localhost dev mode.
    pub fn detect() -> Option<Self> {
        load_dotenv();
        if let Ok(address) = env::var("VAULT_ADDR") {
            return Some(Self {
                address,
                token: env::var("VAULT_TOKEN").unwrap_or_default(),
            });
        }
        // Fallback: localhost:8200 (Vault dev default)
        if tcp_reachable("localhost:8200", std::time::Duration::from_secs(2)) {
            return Some(Self {
                address: "http://localhost:8200".into(),
                token: env::var("VAULT_DEV_ROOT_TOKEN_ID").unwrap_or_else(|_| "root".into()),
            });
        }
        None
    }
}

// ---------------------------------------------------------------------------
// TCP reachability helper
// ---------------------------------------------------------------------------

fn tcp_reachable(addr: &str, timeout: std::time::Duration) -> bool {
    use std::net::ToSocketAddrs;
    addr.to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.next())
        .map(|a| std::net::TcpStream::connect_timeout(&a, timeout).is_ok())
        .unwrap_or(false)
}

/// Skip test if S3/MinIO is not available.
#[macro_export]
macro_rules! skip_if_no_s3 {
    () => {
        match $crate::common::S3TestConfig::detect() {
            Some(cfg) => cfg,
            None => {
                eprintln!("Skipping: no S3/MinIO available (set S3_ENDPOINT or run MinIO on localhost:9000)");
                return;
            }
        }
    };
}

/// Skip test if Vault is not available.
#[macro_export]
macro_rules! skip_if_no_vault {
    () => {
        match $crate::common::VaultTestConfig::detect() {
            Some(cfg) => cfg,
            None => {
                eprintln!("Skipping: no Vault available (set VAULT_ADDR or run Vault dev on localhost:8200)");
                return;
            }
        }
    };
}

// ---------------------------------------------------------------------------
// testcontainers-rs helpers (auto-lifecycle: containers stop when dropped)
// ---------------------------------------------------------------------------
//
// Tests that need a guaranteed-clean backend (rather than relying on live
// services with potentially stale credentials) use testcontainers-rs. The
// returned ContainerAsync is dropped at the end of each test, which
// triggers Docker to stop and remove the container automatically.
//
// Tests skip gracefully when Docker is unavailable LOCALLY, and fail hard in
// CI -- see `skip_if_no_docker!`.

/// Check if Docker daemon is reachable.
pub fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Skip this test locally when Docker is down; PANIC when `$CI` is set.
///
/// A skip is the right call on a developer machine, but in CI it makes every
/// container-backed test pass VACUOUSLY -- the suite reports green while
/// exercising none of the integration surface. That is not a hypothetical: a
/// bad third-party URL shipped to CI precisely because the test that would
/// have caught it skipped itself when the local daemon was down, and CI never
/// re-checked. A gate that disappears along with its environment is not a gate.
#[macro_export]
macro_rules! skip_if_no_docker {
    () => {
        if !$crate::common::docker_available() {
            assert!(
                std::env::var_os("CI").is_none(),
                "Docker daemon unreachable in CI -- container tests must RUN here, \
                 not skip. Skipping would report green while testing nothing."
            );
            eprintln!("Skipping: Docker daemon not available");
            return;
        }
    };
}

// =============================================================================
// Test image pins
// =============================================================================
//
// Pinned HERE rather than left to testcontainers-modules' defaults, which lag
// badly: Kafka 3.8.0, MinIO from February 2025. A tag baked into a dependency's
// source is invisible to dependency review -- Renovate reads Cargo.toml,
// correctly reports the crate current, and never sees the image. Hoisting the
// tags out is what puts them back under review, hence the annotations.

/// renovate: datasource=docker depName=apache/kafka-native
const KAFKA_TAG: &str = "4.3.1";

/// renovate: datasource=docker depName=minio/minio
const MINIO_TAG: &str = "RELEASE.2025-09-07T16-13-09Z";

/// OpenBao, not hashicorp/vault. The estate runs OpenBao and so does the
/// sibling fetcher's harness; testing the secrets path against the product we
/// forked away from is a fidelity gap, not a convenience. The KV v2 API this
/// exercises is identical across both.
///
/// renovate: datasource=docker depName=openbao/openbao
const OPENBAO_TAG: &str = "2.6.1";

/// Start a Kafka container and return (container_handle, bootstrap_address).
///
/// The returned handle holds the container alive; drop it to stop the container.
///
/// # Errors
///
/// Returns an error if Docker is unavailable or the container fails to start.
pub async fn start_kafka_container() -> Result<
    (
        testcontainers::ContainerAsync<testcontainers_modules::kafka::apache::Kafka>,
        String,
    ),
    String,
> {
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::apache;

    let node = apache::Kafka::default()
        .with_tag(KAFKA_TAG)
        .start()
        .await
        .map_err(|e| format!("failed to start Kafka container: {e}"))?;

    let port = node
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .map_err(|e| format!("failed to get Kafka port: {e}"))?;

    let bootstrap = format!("127.0.0.1:{port}");
    Ok((node, bootstrap))
}

/// Start an OpenBao container in dev mode.
///
/// Returns (container_handle, url, root_token). Dev mode only -- an in-memory
/// server with a fixed root token, never a production shape.
///
/// The env vars are `BAO_`-prefixed and the readiness line reads "OpenBao
/// server started!". The `VAULT_`-prefixed spellings are silently ignored: set
/// `VAULT_DEV_ROOT_TOKEN_ID` and the server issues a random token instead, so
/// every subsequent request 403s with nothing pointing at the cause.
pub async fn start_vault_container()
-> Result<(testcontainers::ContainerAsync<testcontainers::GenericImage>, String, String), String> {
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    let node = GenericImage::new("openbao/openbao", OPENBAO_TAG)
        .with_exposed_port(8200u16.tcp())
        .with_wait_for(WaitFor::message_on_stdout("OpenBao server started"))
        .with_env_var("BAO_DEV_ROOT_TOKEN_ID", "root")
        .with_env_var("BAO_DEV_LISTEN_ADDRESS", "0.0.0.0:8200")
        .with_cmd(["server", "-dev"])
        .start()
        .await
        .map_err(|e| format!("failed to start OpenBao container: {e}"))?;

    let port = node
        .get_host_port_ipv4(8200)
        .await
        .map_err(|e| format!("failed to get OpenBao port: {e}"))?;

    let url = format!("http://127.0.0.1:{port}");
    Ok((node, url, "root".to_string()))
}

/// Start a MinIO container and return (handle, endpoint, access_key, secret_key).
pub async fn start_minio_container() -> Result<
    (
        testcontainers::ContainerAsync<testcontainers_modules::minio::MinIO>,
        String,
        String,
        String,
    ),
    String,
> {
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::minio::MinIO;

    let node = MinIO::default()
        .with_tag(MINIO_TAG)
        .start()
        .await
        .map_err(|e| format!("failed to start MinIO container: {e}"))?;

    let port = node
        .get_host_port_ipv4(9000)
        .await
        .map_err(|e| format!("failed to get MinIO port: {e}"))?;

    let endpoint = format!("http://127.0.0.1:{port}");
    // MinIO's default credentials are minioadmin/minioadmin
    Ok((
        node,
        endpoint,
        "minioadmin".to_string(),
        "minioadmin".to_string(),
    ))
}

/// Build a test Kafka config from a bootstrap address (no SASL/TLS).
#[must_use]
pub fn kafka_plain_config(bootstrap: &str) -> KafkaTestConfig {
    KafkaTestConfig {
        brokers: bootstrap.to_string(),
        security_protocol: "PLAINTEXT".to_string(),
        sasl_mechanism: None,
        sasl_user: None,
        sasl_password: None,
    }
}

/// Kafka-container lifecycle handle: holds either a testcontainers container
/// (dropped → stopped) or nothing when using live/docker infrastructure.
///
/// The `Container` variant is boxed to equalise variant sizes (avoids
/// `clippy::large_enum_variant`).
#[allow(dead_code)]
pub enum KafkaHandle {
    /// Testcontainers-managed Kafka; container stops on drop.
    Container(Box<testcontainers::ContainerAsync<testcontainers_modules::kafka::apache::Kafka>>),
    /// External Kafka (live or docker-compose); nothing to manage.
    External,
}

/// Obtain a Kafka backend for tests, preferring live/docker over testcontainers.
///
/// Resolution order (fastest, most realistic first):
///
/// 1. `TEST_MODE=live` (default) and `KAFKA_BROKERS` is reachable AND auth
///    works: use the live cluster. Returns (`KafkaHandle::External`, config).
/// 2. `TEST_MODE=docker`: use `docker-compose.test.yaml` on localhost:19092.
/// 3. `TEST_MODE=testcontainers` OR fallback on live unreachable: spin up a
///    fresh Kafka container. Caller drops the handle to stop it.
///
/// Tests that *require* a specific mode can use [`start_kafka_container`]
/// or [`kafka_test_config`] directly.
pub async fn kafka_backend() -> Option<(KafkaHandle, KafkaTestConfig)> {
    let mode = TestMode::detect();

    // Try live first (unless forced to testcontainers)
    if matches!(mode, TestMode::Live | TestMode::Docker) {
        let cfg = kafka_test_config();
        if cfg.is_reachable() && kafka_auth_works(&cfg).await {
            return Some((KafkaHandle::External, cfg));
        }
        if matches!(mode, TestMode::Docker) {
            // Docker mode is explicit — don't silently fall back
            return None;
        }
    }

    // Fall back to testcontainers
    match start_kafka_container().await {
        Ok((container, bootstrap)) => Some((
            KafkaHandle::Container(Box::new(container)),
            kafka_plain_config(&bootstrap),
        )),
        Err(_) => None,
    }
}

/// Quick auth sanity check: issue a `fetch_metadata` call. Returns true if
/// SASL/TLS handshake succeeds within 5 seconds.
async fn kafka_auth_works(cfg: &KafkaTestConfig) -> bool {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", format!("auth-probe-{}", uuid::Uuid::new_v4()))
        .set("session.timeout.ms", "6000")
        .set("socket.timeout.ms", "5000");
    cfg.apply_sasl(&mut client_config);

    let Ok(consumer) = client_config.create::<StreamConsumer>() else {
        return false;
    };

    tokio::task::spawn_blocking(move || {
        consumer
            .fetch_metadata(None, std::time::Duration::from_secs(5))
            .is_ok()
    })
    .await
    .unwrap_or(false)
}
