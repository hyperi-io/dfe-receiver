// Project:   dfe-receiver
// File:      tests/common/mod.rs
// Purpose:   Shared test infrastructure — dual-mode (remote/docker) config helpers
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Test infrastructure helpers for dual-mode integration tests.
//!
//! Backends controlled by `TEST_MODE` in `.env`:
//! - `live` (default) -- the cluster `KAFKA_BROKERS` names, else a container
//! - `docker` -- dfe-docker infra profile (localhost:19092, no auth)
//! - `testcontainers` -- the run's shared broker, one container for every test

use std::env;

pub mod peers;

/// Test backend mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestMode {
    /// Use the live endpoints env vars or `.env` name, which must work. With
    /// none named, a container as for `Testcontainers`.
    Live,
    /// Use a local docker-compose stack from `docker-compose.test.yaml`.
    Docker,
    /// Use the run's shared containers (CI / no live infra).
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

/// Load this repo's own `.env` (idempotent, errors ignored):
/// `dotenvy::dotenv()` would load the first `.env` in any parent.
pub fn load_dotenv() {
    let _ = dotenvy::from_path(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".env"));
}

/// A sender's records over an HTTP listener with no credential.
pub fn http_ingest() -> dfe_receiver::tenancy::Ingest {
    dfe_receiver::tenancy::Ingest::sent(dfe_receiver::tenancy::Principal::Listener("http"))
}

// ---------------------------------------------------------------------------
// Listener addresses
// ---------------------------------------------------------------------------
//
// A listener binds port 0 and reports the port it got, because a port picked
// and handed over can be taken by any process before the bind lands.

/// How long a receiver listener gets to bind before the test gives up on it.
const BIND_DEADLINE: std::time::Duration = std::time::Duration::from_secs(15);

/// Wait for a receiver listener configured on port 0 to bind, and return the
/// address it took.
///
/// `task` runs the handler's `start()`. A handler that cannot bind returns its
/// error, and watching the task reports that error instead of a timeout.
///
/// # Panics
///
/// When the task ends before the bind lands, or the bind takes longer than
/// [`BIND_DEADLINE`].
pub async fn bound_addr<T: std::fmt::Debug>(
    listener: &str,
    bound: &dfe_receiver::server::traits::BoundAddr,
    task: &mut tokio::task::JoinHandle<T>,
) -> std::net::SocketAddr {
    tokio::select! {
        biased;
        addr = bound.wait() => addr,
        outcome = task => panic!("the {listener} listener exited before it bound: {outcome:?}"),
        () = tokio::time::sleep(BIND_DEADLINE) => {
            panic!("the {listener} listener did not bind within {BIND_DEADLINE:?}")
        }
    }
}

/// POST to `path` on `addr` with a body that stops 1000 bytes short of its
/// Content-Length, and return the status the server answers within `wait`.
///
/// `None` when the server has not answered by then, which is how a listener
/// with no request timeout behaves.
pub async fn post_stalled_body(
    addr: std::net::SocketAddr,
    path: &str,
    wait: std::time::Duration,
) -> Option<u16> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr).await.ok()?;
    let head = format!(
        "POST {path} HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/x-protobuf\r\n\
         content-length: 1024\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await.ok()?;
    stream.write_all(&[0u8; 24]).await.ok()?;
    let mut answer = vec![0u8; 256];
    let read = tokio::time::timeout(wait, stream.read(&mut answer))
        .await
        .ok()?
        .ok()?;
    let status_line = std::str::from_utf8(&answer[..read]).ok()?;
    status_line.split_whitespace().nth(1)?.parse().ok()
}

/// Poll `condition` until it holds, for at most `wait`.
pub async fn eventually(wait: std::time::Duration, condition: impl Fn() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + wait;
    while tokio::time::Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    condition()
}

/// Start a scalo gRPC Push server on a free loopback port, standing in for a
/// destination such as dfe-loader, and return its endpoint URL with it.
///
/// # Panics
///
/// When the server cannot bind.
pub async fn grpc_destination() -> (String, scalo::transport::grpc::GrpcTransport) {
    use scalo::transport::grpc::{GrpcConfig, GrpcTransport};

    // `new` returns once the listener is bound, so the endpoint is ready here.
    let transport = match GrpcTransport::new(&GrpcConfig::server("127.0.0.1:0")).await {
        Ok(transport) => transport,
        Err(e) => panic!("the gRPC destination could not bind a loopback port: {e}"),
    };
    let Some(addr) = transport.local_addr() else {
        panic!("a server-mode gRPC transport reports the address it bound");
    };
    (format!("http://{addr}"), transport)
}

/// A loopback TCP port held bound but never listening.
///
/// A connection to it is refused, and no other socket can bind it while this
/// holds it, so the address stays closed for as long as the test needs.
pub struct ClosedPort {
    _socket: socket2::Socket,
    addr: std::net::SocketAddr,
}

impl ClosedPort {
    /// Hold a free loopback port.
    ///
    /// # Errors
    ///
    /// When the socket cannot be created or bound.
    pub fn loopback() -> std::io::Result<Self> {
        use socket2::{Domain, Protocol, Socket, Type};

        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
        // Left without SO_REUSEADDR, so no other socket can share the port, even one that sets it.
        socket.bind(&std::net::SocketAddr::from(([127, 0, 0, 1], 0)).into())?;
        let addr = socket
            .local_addr()?
            .as_socket()
            .ok_or_else(|| std::io::Error::other("a bound IPv4 socket has no inet address"))?;
        Ok(Self {
            _socket: socket,
            addr,
        })
    }

    /// The address held closed.
    pub fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    /// Give the port up for a server that must come up on this exact address.
    pub fn release(self) -> std::net::SocketAddr {
        self.addr
    }
}

/// A loopback UDP port held bound, so no other socket can bind it.
///
/// The UDP counterpart of [`ClosedPort`]: a TCP socket does not reserve the UDP
/// port of the same number.
pub struct HeldUdpPort {
    _socket: std::net::UdpSocket,
    addr: std::net::SocketAddr,
}

impl HeldUdpPort {
    /// Hold a free loopback UDP port.
    ///
    /// # Errors
    ///
    /// When the socket cannot be bound.
    pub fn loopback() -> std::io::Result<Self> {
        // Bound without SO_REUSEPORT, so even a listener that sets it cannot share the port.
        let socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let addr = socket.local_addr()?;
        Ok(Self {
            _socket: socket,
            addr,
        })
    }

    /// The address held.
    pub fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }
}

/// Write a throwaway self-signed ECDSA P-384 certificate and key into `dir`,
/// valid for `localhost` and `127.0.0.1`.
///
/// # Panics
///
/// When openssl cannot run or cannot generate the pair: a TLS test that skips
/// itself reports green while testing nothing.
pub fn self_signed_cert(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let cert = dir.join("server.crt");
    let key = dir.join("server.key");
    let output = std::process::Command::new("openssl")
        .args([
            "req",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-384",
            "-nodes",
            "-x509",
            "-keyout",
        ])
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .args([
            "-days",
            "1",
            "-subj",
            "/CN=localhost/O=Test/C=AU",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ])
        .output()
        .unwrap_or_else(|e| panic!("openssl could not run: {e}"));
    assert!(
        output.status.success(),
        "openssl could not generate a test certificate: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    (cert, key)
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
/// Testcontainers mode: callers use [`kafka_backend`], which starts the shared broker.
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

/// A `routing.topic_suffix` no other test or earlier run shares, so a test
/// reading a topic its source names reads only its own records on a shared
/// broker.
pub fn test_topic_suffix() -> String {
    format!("_land_{}", uuid::Uuid::new_v4().simple())
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
                password: password.as_str().into(),
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
// Docker
// ---------------------------------------------------------------------------
//
// Tests skip when Docker is unavailable LOCALLY, and fail hard in CI -- see
// `skip_if_no_docker!`.

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
// Backing services
// =============================================================================
//
// One container per service per test run, shared by every nextest process,
// through scalo's `test_support::shared_service`: the first process to ask
// starts it, the rest join it by name, and a watcher removes it once the run
// ends, whether it passed, failed or was interrupted. Tests on a shared service
// keep to their own topics and secret paths. A test that stops its broker holds
// one of its own instead ([`start_kafka_container`]), removed when it drops.

use scalo::deployment::test_support::shared_service::{
    self, OwnedContainer, Ready, ServiceSpec, SharedContainer, Suite,
};
use scalo::transport::kafka::test_support::{KAFKA_PORT, kafka_spec, shared_kafka};

/// This repo's suite: its containers' names, labels and run log.
pub const SUITE: Suite = Suite {
    repo: "dfe-receiver",
    manifest_dir: env!("CARGO_MANIFEST_DIR"),
    tmp_dir: option_env!("CARGO_TARGET_TMPDIR"),
};

/// OpenBao, not hashicorp/vault. The estate runs OpenBao and so does the
/// sibling fetcher's harness; testing the secrets path against the product we
/// forked away from is a fidelity gap, not a convenience. The KV v2 API this
/// exercises is identical across both.
///
/// renovate: datasource=docker depName=openbao/openbao
pub const OPENBAO_IMAGE: &str =
    "openbao/openbao:2.6.1@sha256:5b2486ab0fb90bbc788cc345b0a08616dfb375873ee8be5df3a2fd4d378a67e0";

/// The root token the OpenBao dev server issues.
pub const OPENBAO_ROOT_TOKEN: &str = "root";

/// An OpenBao dev server: in memory, with [`OPENBAO_ROOT_TOKEN`], never a
/// production shape.
///
/// The env vars are `BAO_`-prefixed and the readiness line reads "OpenBao
/// server started!". The `VAULT_`-prefixed spellings are silently ignored: set
/// `VAULT_DEV_ROOT_TOKEN_ID` and the server issues a random token instead, so
/// every subsequent request 403s with nothing pointing at the cause.
fn openbao_spec() -> ServiceSpec {
    ServiceSpec {
        ports: vec![8200],
        env: vec![
            (
                "BAO_DEV_ROOT_TOKEN_ID".to_string(),
                OPENBAO_ROOT_TOKEN.to_string(),
            ),
            (
                "BAO_DEV_LISTEN_ADDRESS".to_string(),
                "0.0.0.0:8200".to_string(),
            ),
        ],
        args: vec!["server".to_string(), "-dev".to_string()],
        ready: Ready::Log("OpenBao server started"),
        ..ServiceSpec::new("openbao", OPENBAO_IMAGE, std::time::Duration::from_secs(60))
    }
}

/// The run's shared OpenBao dev server and its URL.
///
/// # Panics
///
/// When no Docker daemon answers, or the server cannot be started or joined.
pub async fn shared_openbao() -> (SharedContainer, String) {
    static SHARED: std::sync::OnceLock<SharedContainer> = std::sync::OnceLock::new();
    let container = tokio::task::spawn_blocking(|| {
        SHARED
            .get_or_init(|| shared_service::acquire(&SUITE, &openbao_spec()))
            .clone()
    })
    .await
    .unwrap_or_else(|e| panic!("acquire the shared OpenBao server: {e}"));
    let url = format!("http://{}", container.addr(8200));
    (container, url)
}

/// A Kafka broker one test holds alone, for a test that pauses it. Removed
/// when it drops.
pub struct OwnedBroker(OwnedContainer);

impl OwnedBroker {
    /// Freeze the broker, so a produce request gets no answer.
    pub async fn pause(&self) -> Result<(), String> {
        self.docker("pause").await
    }

    /// Thaw a paused broker.
    pub async fn unpause(&self) -> Result<(), String> {
        self.docker("unpause").await
    }

    async fn docker(&self, verb: &'static str) -> Result<(), String> {
        let name = self.0.container.name.clone();
        let out = tokio::task::spawn_blocking(move || shared_service::docker(&[verb, &name]))
            .await
            .map_err(|e| format!("docker {verb}: {e}"))?;
        if out.status.success() {
            Ok(())
        } else {
            Err(String::from_utf8_lossy(&out.stderr).into_owned())
        }
    }
}

/// Start a Kafka broker `test` holds alone and return it with its bootstrap
/// address. Pass `test_name!()`.
///
/// # Errors
///
/// Returns an error if Docker is unavailable or the broker fails to start.
pub async fn start_kafka_container(test: &str) -> Result<(OwnedBroker, String), String> {
    let name = shared_service::container_name(&SUITE, Some(test), "kafka");
    let owned = tokio::task::spawn_blocking(move || {
        shared_service::start_alone(&SUITE, &kafka_spec(None), &name)
    })
    .await
    .map_err(|e| format!("failed to start Kafka container: {e}"))?;
    let bootstrap = owned.container.addr(KAFKA_PORT);
    Ok((OwnedBroker(owned), bootstrap))
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

/// Where a test's Kafka broker lives.
#[allow(dead_code)]
pub enum KafkaHandle {
    /// The run's shared broker, removed when the run ends.
    Shared(SharedContainer),
    /// External Kafka (live or docker-compose); nothing to manage.
    External,
}

/// Obtain a Kafka backend for tests.
///
/// 1. `TEST_MODE=docker`, or `TEST_MODE=live` with `KAFKA_BROKERS` set: that
///    cluster, which must be reachable with the configured credentials.
/// 2. Otherwise the run's shared broker.
///
/// `None` only where Docker is down outside CI, by the rule of
/// `skip_if_no_docker!`. A configured cluster that refuses, or a broker that
/// will not start, panics: a test that swapped a SASL cluster for a plaintext
/// broker, or skipped, would pass without testing what it names.
pub async fn kafka_backend() -> Option<(KafkaHandle, KafkaTestConfig)> {
    let mode = TestMode::detect();
    let configured = match mode {
        TestMode::Docker => true,
        TestMode::Live => env::var_os("KAFKA_BROKERS").is_some(),
        TestMode::Testcontainers => false,
    };
    if configured {
        let cfg = kafka_test_config();
        assert!(
            cfg.is_reachable() && kafka_auth_works(&cfg).await,
            "TEST_MODE={mode:?} names the Kafka cluster at {}, which is unreachable or \
             refuses the configured credentials",
            cfg.brokers
        );
        return Some((KafkaHandle::External, cfg));
    }

    if !docker_available() {
        assert!(
            std::env::var_os("CI").is_none(),
            "Docker daemon unreachable in CI -- Kafka tests must RUN here, not skip."
        );
        eprintln!("Skipping: Docker daemon not available for a Kafka container");
        return None;
    }
    let broker = shared_kafka(SUITE).await;
    let bootstrap = broker.addr(KAFKA_PORT);
    Some((KafkaHandle::Shared(broker), kafka_plain_config(&bootstrap)))
}

/// The name of the test this expands inside, for naming its containers.
///
/// Rust has no way to read the current test's name, and a hand-written literal
/// per call site would drift the moment a test is renamed. `type_name` of a
/// function declared right here reports the path it is nested in, which is the
/// calling test -- hence a macro: expanded in a helper it would report the helper.
///
/// An `async fn` body becomes a generated future, so the path picks up
/// `::{{closure}}`; the trailing generated segments are trimmed off.
#[macro_export]
macro_rules! test_name {
    () => {{
        fn probe() {}
        fn path_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let mut name = path_of(probe);
        name = name.strip_suffix("::probe").unwrap_or(name);
        name = name.strip_suffix("::{{closure}}").unwrap_or(name);
        name.rsplit("::").next().unwrap_or(name)
    }};
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
