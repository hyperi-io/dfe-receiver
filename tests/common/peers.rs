// Project:   dfe-receiver
// File:      tests/common/peers.rs
// Purpose:   Real external peers in Docker, the built receiver binary, and a stand-in destination
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Harness for tests whose counterpart is a real tool, not our own code.
//!
//! A peer (Fluent Bit, Filebeat, Logstash, Vector, the OpenTelemetry Collector,
//! `openssl s_client`) runs in one Docker container for the whole test, driven
//! by the docker CLI. The receiver under test is the built `dfe-receiver`
//! binary started with a config file, the path an operator takes, so the
//! binary's own start-up code runs, including the rustls provider install.
//! Records are read from a gRPC destination the test stands up in its place.
//!
//! nextest gives each test its own process, so a container is shared by the
//! cases inside one test, not across tests. Each peer therefore has one test
//! with a table of cases, which keeps it to one container per tool.
//!
//! Peers run on the Docker host's network (`--network host`), so they reach the
//! receiver on `127.0.0.1`, the same assumption the Kafka and OpenBao helpers
//! make about the daemon.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dfe_receiver::config::{Config, TlsConfig};
use scalo::transport::TransportReceiver;
use tokio::process::Command;

use super::{claim_container_name, container_name, grpc_destination, test_labels};

/// Throwaway certificates are made by this image, so the host needs no openssl.
///
/// renovate: datasource=docker depName=alpine/openssl
pub const OPENSSL_IMAGE: &str =
    "alpine/openssl:3.5.9@sha256:1c361a204e9791bec6202bccbd1065eafaa65cb5a1a9a06ae000238569fba6a2";

/// renovate: datasource=docker depName=fluent/fluent-bit
pub const FLUENT_BIT_IMAGE: &str = "fluent/fluent-bit:5.1.5@sha256:2740f387f6e68fab66d28e4fa6a853387b9cab6ffb4cb78958f69bba4eb0e7b4";

/// renovate: datasource=docker depName=otel/opentelemetry-collector-contrib
pub const OTEL_COLLECTOR_IMAGE: &str = "otel/opentelemetry-collector-contrib:0.164.0@sha256:340c9ca6a36617084c6661bdc4f0fe92b7cda5a805d61673ec8db375fddc522b";

/// Elastic's Filebeat, from Elastic's registry.
///
/// renovate: datasource=docker depName=docker.elastic.co/beats/filebeat
pub const FILEBEAT_IMAGE: &str = "docker.elastic.co/beats/filebeat:9.5.5@sha256:4b2042fcbeb781800bc7271d400f764bb7d670310dfd4fe03ade53d6c7ab78aa";

/// Elastic's Logstash, from Elastic's registry.
///
/// renovate: datasource=docker depName=docker.elastic.co/logstash/logstash
pub const LOGSTASH_IMAGE: &str = "docker.elastic.co/logstash/logstash:9.5.5@sha256:d170c92166492ff82433395734121dcd2c73140038a03e252299384a01c9aae5";

/// renovate: datasource=docker depName=timberio/vector
pub const VECTOR_IMAGE: &str = "timberio/vector:0.60.0-distroless-libc@sha256:a9ed9a4710bdc2efa66e94c1e8edc7b439709e6e50079f1e1b2b1bf479d387da";

/// Directory inside a peer container the test's files are copied to.
pub const PEER_DIR: &str = "/dfe-test";

/// How long a container image may take to pull.
const PEER_START_DEADLINE: Duration = Duration::from_secs(300);

/// How long one command in a peer may run before the test fails it.
const EXEC_DEADLINE: Duration = Duration::from_secs(90);

/// How long the receiver binary gets to open its listeners.
const RECEIVER_START_DEADLINE: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------

/// First port of the range the receiver binary's ports come from, below the kernel's ephemeral range.
const PORT_BASE: u32 = 20_000;

/// Ports each test process draws from, and how many such windows fit between [`PORT_BASE`] and the ephemeral range.
const PORT_WINDOW: u32 = 50;
const PORT_WINDOWS: u32 = 255;

/// Ports already handed out by this process.
static PORTS_TAKEN: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// A loopback port nothing is bound to at the moment of the call.
///
/// The receiver binary binds its own listeners, so the port has to be named in
/// its config before it starts. nextest runs tests as concurrent processes, and
/// two of them asking the kernel for a free port can be given the same one. Each
/// process therefore draws from a window picked by its pid, and falls back to the
/// kernel's choice only once the window is used up.
///
/// # Panics
///
/// When no loopback port can be bound.
#[must_use]
pub fn free_port() -> u16 {
    use std::sync::atomic::Ordering;

    let window = PORT_BASE + (std::process::id() % PORT_WINDOWS) * PORT_WINDOW;
    loop {
        let taken = PORTS_TAKEN.fetch_add(1, Ordering::Relaxed);
        if taken >= PORT_WINDOW {
            break;
        }
        let port = window + taken;
        if std::net::TcpListener::bind(("127.0.0.1", u16::try_from(port).unwrap_or(0))).is_ok() {
            return u16::try_from(port).unwrap_or(0);
        }
    }
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .unwrap_or_else(|e| panic!("no loopback port could be bound: {e}"))
}

/// A loopback address on a fresh free port.
#[must_use]
pub fn free_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], free_port()))
}

// ---------------------------------------------------------------------------
// Certificates
// ---------------------------------------------------------------------------

/// Certificates for a test, made by `openssl` in a container.
///
/// `ca.crt` signs `server` (valid for `localhost` and `127.0.0.1`) and `client`.
/// `rogue-ca.crt` is an unrelated CA that signs `rogue-client`, for the cases
/// where a peer must refuse or be refused. The client keys are RSA because
/// Logstash's HTTP client reads no other kind, and the rest are P-256. Every
/// file is world-readable, since a peer container may run as any user and
/// these keys are throwaway.
pub struct Pki {
    dir: tempfile::TempDir,
}

const PKI_SCRIPT: &str = r#"set -e
mkdir /pki && cd /pki
key() { openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1"; }
rsakey() { openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$1"; }
ca() {
  key "$1.key"
  openssl req -x509 -new -key "$1.key" -subj "/CN=$1" -days 2 \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -out "$1.crt"
}
leaf() {
  "${4:-key}" "$1.key"
  openssl req -new -key "$1.key" -subj "/CN=$1" -out "$1.csr"
  printf '%s\n' "basicConstraints=CA:FALSE" "$3" > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA "$2.crt" -CAkey "$2.key" -CAcreateserial -days 2 -extfile "$1.ext" -out "$1.crt"
}
ca ca
ca rogue-ca
leaf server ca "subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth"
leaf client ca "extendedKeyUsage=clientAuth" rsakey
leaf rogue-client rogue-ca "extendedKeyUsage=clientAuth" rsakey
rm -f *.csr *.ext *.srl
chmod 644 *
tar -cf - .
"#;

impl Pki {
    /// Generate the certificates.
    ///
    /// # Panics
    ///
    /// When Docker or the openssl image fails: a TLS test that cannot make its
    /// certificates has nothing to test.
    pub async fn generate() -> Self {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("temp dir: {e}"));
        let out = Command::new("docker")
            .args(["run", "--rm", "--entrypoint", "sh", OPENSSL_IMAGE, "-c"])
            .arg(PKI_SCRIPT)
            .stdin(Stdio::null())
            .output()
            .await
            .unwrap_or_else(|e| panic!("docker could not run: {e}"));
        assert!(
            out.status.success(),
            "the openssl container could not make certificates: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let tar = dir.path().join("pki.tar");
        std::fs::write(&tar, &out.stdout).unwrap_or_else(|e| panic!("write pki.tar: {e}"));
        let status = std::process::Command::new("tar")
            .arg("-xf")
            .arg(&tar)
            .arg("-C")
            .arg(dir.path())
            .status()
            .unwrap_or_else(|e| panic!("tar could not run: {e}"));
        assert!(status.success(), "tar could not unpack the certificates");
        std::fs::remove_file(&tar).unwrap_or_else(|e| panic!("remove pki.tar: {e}"));
        Self { dir }
    }

    /// The directory holding the files, to copy into a peer.
    #[must_use]
    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    /// Path of a generated file on the host, such as `ca.crt` or `server.key`.
    #[must_use]
    pub fn file(&self, name: &str) -> String {
        self.dir.path().join(name).to_string_lossy().into_owned()
    }

    /// Path of a generated file inside a peer given [`Pki::dir`] as its files.
    #[must_use]
    pub fn in_peer(name: &str) -> String {
        format!("{PEER_DIR}/{name}")
    }

    /// Receiver-side TLS for a listener: the server certificate, and unless
    /// `client_auth` is `none`, the CA its clients' certificates must chain to.
    #[must_use]
    pub fn server_tls(&self, client_auth: &str) -> TlsConfig {
        TlsConfig {
            enabled: true,
            cert_file: Some(self.file("server.crt")),
            key_file: Some(self.file("server.key")),
            ca_file: (client_auth != "none").then(|| self.file("ca.crt")),
            client_auth: client_auth.to_string(),
            ..TlsConfig::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Peer containers
// ---------------------------------------------------------------------------

/// What to start as a peer.
pub struct PeerSpec<'a> {
    /// The owning test, from `test_name!()`.
    pub test: &'a str,
    /// The tool, as it appears in the container name and its label.
    pub service: &'a str,
    /// Image reference pinned by digest.
    pub image: &'a str,
    /// Replaces the image's entrypoint when set.
    pub entrypoint: Option<&'a str>,
    /// Arguments after the image.
    pub args: Vec<String>,
    /// Host directory copied to [`PEER_DIR`] before the container starts.
    pub files: Option<&'a Path>,
    /// User to run as, such as `0:0`, when the image's default cannot read the files.
    pub user: Option<&'a str>,
    /// Environment variables for the container.
    pub env: Vec<(&'a str, &'a str)>,
    /// Single files copied in before the container starts, as host path and container path.
    pub extra_files: Vec<(PathBuf, String)>,
}

/// A running peer container, removed when dropped.
pub struct Peer {
    name: String,
}

/// The outcome of a command run in a peer.
#[derive(Debug)]
pub struct Exec {
    /// Exit status.
    pub code: Option<i32>,
    /// Standard output and error, in that order.
    pub output: String,
}

async fn docker(args: &[&str]) -> Result<std::process::Output, String> {
    Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| format!("docker could not run: {e}"))
}

impl Peer {
    /// Create, fill and start the container.
    ///
    /// # Errors
    ///
    /// The docker CLI's complaint when the image cannot be pulled, the files
    /// cannot be copied in, or the container will not start.
    pub async fn start(spec: PeerSpec<'_>) -> Result<Self, String> {
        let name = claim_container_name(&container_name(Some(spec.test), spec.service));
        let mut create: Vec<String> = ["create", "--name", &name, "--network", "host"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        for (key, value) in test_labels(spec.service) {
            create.extend(["--label".to_string(), format!("{key}={value}")]);
        }
        if let Some(user) = spec.user {
            create.extend(["--user".to_string(), user.to_string()]);
        }
        for (key, value) in &spec.env {
            create.extend(["--env".to_string(), format!("{key}={value}")]);
        }
        if let Some(entrypoint) = spec.entrypoint {
            create.extend(["--entrypoint".to_string(), entrypoint.to_string()]);
        }
        create.push(spec.image.to_string());
        create.extend(spec.args);

        let refs: Vec<&str> = create.iter().map(String::as_str).collect();
        let created = tokio::time::timeout(PEER_START_DEADLINE, docker(&refs))
            .await
            .map_err(|_| format!("{} did not pull within {PEER_START_DEADLINE:?}", spec.image))??;
        if !created.status.success() {
            return Err(format!(
                "docker create {} failed: {}",
                spec.image,
                String::from_utf8_lossy(&created.stderr)
            ));
        }
        // From here the Drop impl removes the container on any early return.
        let peer = Self { name };

        if let Some(dir) = spec.files {
            let src = format!("{}/.", dir.display());
            let dest = format!("{}:{PEER_DIR}", peer.name);
            let copied = docker(&["cp", &src, &dest]).await?;
            if !copied.status.success() {
                return Err(format!(
                    "docker cp into {} failed: {}",
                    peer.name,
                    String::from_utf8_lossy(&copied.stderr)
                ));
            }
        }
        for (host, inside) in &spec.extra_files {
            let dest = format!("{}:{inside}", peer.name);
            let copied = docker(&["cp", &host.to_string_lossy(), &dest]).await?;
            if !copied.status.success() {
                return Err(format!(
                    "docker cp {} into {} failed: {}",
                    host.display(),
                    peer.name,
                    String::from_utf8_lossy(&copied.stderr)
                ));
            }
        }
        let started = docker(&["start", &peer.name]).await?;
        if !started.status.success() {
            return Err(format!(
                "docker start {} failed: {}",
                peer.name,
                String::from_utf8_lossy(&started.stderr)
            ));
        }
        Ok(peer)
    }

    /// The container's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Run a command in the container.
    ///
    /// # Panics
    ///
    /// When the docker CLI cannot run.
    pub async fn exec(&self, args: &[&str]) -> Exec {
        let mut full = vec!["exec", self.name.as_str()];
        full.extend_from_slice(args);
        let out = tokio::time::timeout(EXEC_DEADLINE, docker(&full))
            .await
            .unwrap_or_else(|_| panic!("docker exec {full:?} ran past {EXEC_DEADLINE:?}"))
            .unwrap_or_else(|e| panic!("docker exec: {e}"));
        let mut output = String::from_utf8_lossy(&out.stdout).into_owned();
        output.push_str(&String::from_utf8_lossy(&out.stderr));
        Exec {
            code: out.status.code(),
            output,
        }
    }

    /// Start a command in the container and leave it running.
    ///
    /// # Panics
    ///
    /// When docker cannot start the command.
    pub async fn exec_detached(&self, args: &[&str]) {
        let mut full = vec!["exec", "--detach", self.name.as_str()];
        full.extend_from_slice(args);
        let out = docker(&full)
            .await
            .unwrap_or_else(|e| panic!("docker exec --detach: {e}"));
        assert!(
            out.status.success(),
            "docker exec --detach {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Everything the container has written to stdout and stderr.
    pub async fn logs(&self) -> String {
        match docker(&["logs", &self.name]).await {
            Ok(out) => {
                let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
                text.push_str(&String::from_utf8_lossy(&out.stderr));
                text
            }
            Err(e) => e,
        }
    }

    /// Whether the container is still running.
    pub async fn running(&self) -> bool {
        docker(&["inspect", "--format", "{{.State.Running}}", &self.name])
            .await
            .is_ok_and(|out| String::from_utf8_lossy(&out.stdout).trim() == "true")
    }

    /// Wait until the container's logs contain `needle`.
    ///
    /// # Errors
    ///
    /// The log so far, when the container exits first or the wait runs out.
    pub async fn wait_for_log(&self, needle: &str, within: Duration) -> Result<(), String> {
        let deadline = Instant::now() + within;
        loop {
            let logs = self.logs().await;
            if logs.contains(needle) {
                return Ok(());
            }
            let running = self.running().await;
            if !running || Instant::now() > deadline {
                return Err(format!(
                    "{} never logged {needle:?} (running: {running}). Log:\n{logs}",
                    self.name
                ));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", "--volumes", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

// ---------------------------------------------------------------------------
// Stand-in destination
// ---------------------------------------------------------------------------

/// Whether `record` holds `marker` as a whole token, so `case-a` does not match inside `case-a-wrong-ca`.
fn has_marker(record: &str, marker: &str) -> bool {
    record.match_indices(marker).any(|(start, _)| {
        record[start + marker.len()..]
            .chars()
            .next()
            .is_none_or(|next| !(next.is_alphanumeric() || next == '-' || next == '_'))
    })
}

/// A gRPC Push listener standing in for dfe-loader, collecting every record
/// the receiver delivers to it as text.
pub struct Sink {
    endpoint: String,
    seen: Arc<parking_lot::Mutex<Vec<String>>>,
    collector: tokio::task::JoinHandle<()>,
}

impl Sink {
    /// Start the listener on a free loopback port.
    pub async fn start() -> Self {
        let (endpoint, transport) = grpc_destination().await;
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let collected = Arc::clone(&seen);
        let collector = tokio::spawn(async move {
            loop {
                if let Ok(batch) = transport.recv(100).await {
                    let mut seen = collected.lock();
                    for record in batch.records {
                        seen.push(String::from_utf8_lossy(&record.payload).into_owned());
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        });
        Self {
            endpoint,
            seen,
            collector,
        }
    }

    /// The `http://host:port` the receiver delivers to.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// How many records so far contain `marker`.
    #[must_use]
    pub fn count(&self, marker: &str) -> usize {
        self.seen
            .lock()
            .iter()
            .filter(|r| has_marker(r, marker))
            .count()
    }

    /// Wait until one record contains every one of `markers`.
    pub async fn wait_for_all(&self, markers: &[&str], within: Duration) -> bool {
        let deadline = Instant::now() + within;
        loop {
            if self
                .seen
                .lock()
                .iter()
                .any(|r| markers.iter().all(|m| has_marker(r, m)))
            {
                return true;
            }
            if Instant::now() > deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// A record containing `marker`, if one has arrived.
    #[must_use]
    pub fn find(&self, marker: &str) -> Option<String> {
        self.seen
            .lock()
            .iter()
            .find(|r| has_marker(r, marker))
            .cloned()
    }

    /// Wait until a record containing `marker` has arrived.
    pub async fn wait_for(&self, marker: &str, within: Duration) -> bool {
        self.wait_for_count(marker, 1, within).await
    }

    /// Wait until `want` records containing `marker` have arrived.
    pub async fn wait_for_count(&self, marker: &str, want: usize, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        loop {
            if self.count(marker) >= want {
                return true;
            }
            if Instant::now() > deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The first records received, for a failure message.
    #[must_use]
    pub fn dump(&self) -> String {
        let seen = self.seen.lock();
        let shown: Vec<&str> = seen.iter().take(20).map(String::as_str).collect();
        format!(
            "destination holds {} records, first {}:\n{}",
            seen.len(),
            shown.len(),
            shown.join("\n")
        )
    }
}

impl Drop for Sink {
    fn drop(&mut self) {
        self.collector.abort();
    }
}

// ---------------------------------------------------------------------------
// Receiver binary
// ---------------------------------------------------------------------------

/// The built receiver, started with a config file as an operator would, and
/// killed when dropped.
pub struct Receiver {
    child: tokio::process::Child,
    log: PathBuf,
    _dir: tempfile::TempDir,
}

impl Receiver {
    /// Write `config` to a file, start the binary on it, and wait until every
    /// address in `listeners` accepts a connection.
    ///
    /// The sink becomes the only destination.
    ///
    /// # Errors
    ///
    /// The binary's log when it exits before its listeners open, or when they
    /// do not open in time.
    pub async fn start(
        mut config: Config,
        sink: &Sink,
        listeners: &[SocketAddr],
    ) -> Result<Self, String> {
        config.destinations.default = "loader".into();
        config.loader.transport = "grpc".to_string();
        config.loader.grpc_endpoint = Some(sink.endpoint().to_string());
        config.routing.dlq.enabled = false;

        let dir = tempfile::tempdir().map_err(|e| format!("temp dir: {e}"))?;
        let config_path = dir.path().join("receiver.yaml");
        let text = serde_json::to_string_pretty(&config).map_err(|e| format!("config: {e}"))?;
        std::fs::write(&config_path, text).map_err(|e| format!("write config: {e}"))?;
        let log = dir.path().join("receiver.log");
        let log_file = std::fs::File::create(&log).map_err(|e| format!("create log: {e}"))?;
        let log_err = log_file
            .try_clone()
            .map_err(|e| format!("clone log: {e}"))?;

        // A clean environment keeps this machine's DFE_RECEIVER_* and .env out of the run.
        let child = Command::new(env!("CARGO_BIN_EXE_dfe-receiver"))
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", dir.path())
            .env("DFE_RECEIVER_VERSION_CHECK__ENABLED", "false")
            // The default span export target is localhost:4317, which another service on the host may own.
            .env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
            .arg("--config")
            .arg(&config_path)
            .args([
                "--metrics-addr",
                &free_addr().to_string(),
                "--log-format",
                "text",
            ])
            .current_dir(dir.path())
            .stdin(Stdio::null())
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_err))
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("the receiver binary could not start: {e}"))?;
        let mut receiver = Self {
            child,
            log,
            _dir: dir,
        };
        receiver.wait_for_listeners(listeners).await?;
        Ok(receiver)
    }

    async fn wait_for_listeners(&mut self, listeners: &[SocketAddr]) -> Result<(), String> {
        let deadline = Instant::now() + RECEIVER_START_DEADLINE;
        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(format!(
                    "the receiver exited with {status} before its listeners opened. Log:\n{}",
                    self.log_text()
                ));
            }
            // A listener that cannot bind leaves the process up, so the log is the only sign.
            if self.log_text().contains("Protocol handler failed") {
                return Err(format!(
                    "a receiver listener failed to start. Log:\n{}",
                    self.log_text()
                ));
            }
            let mut open = true;
            for addr in listeners {
                if tokio::net::TcpStream::connect(addr).await.is_err() {
                    open = false;
                    break;
                }
            }
            if open {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "the receiver did not open {listeners:?} within {RECEIVER_START_DEADLINE:?}. Log:\n{}",
                    self.log_text()
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Everything the binary has logged so far.
    #[must_use]
    pub fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Whether the process is still running.
    pub fn alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}

// ---------------------------------------------------------------------------
// Table results
// ---------------------------------------------------------------------------

/// Collects the outcome of each case in a table, so one run reports every
/// failing case and not only the first.
#[derive(Default)]
pub struct Cases {
    passed: Vec<String>,
    failed: Vec<String>,
}

impl Cases {
    /// Record one case.
    pub fn check(&mut self, name: &str, ok: bool, detail: impl FnOnce() -> String) {
        if ok {
            self.passed.push(name.to_string());
        } else {
            self.failed.push(format!("{name}: {}", detail()));
        }
    }

    /// Print what passed and fail on anything that did not.
    ///
    /// # Panics
    ///
    /// When any case failed, with every failure listed.
    pub fn finish(self, context: impl FnOnce() -> String) {
        for name in &self.passed {
            eprintln!("case ok: {name}");
        }
        assert!(
            self.failed.is_empty(),
            "{} of {} cases failed:\n{}\n\n{}",
            self.failed.len(),
            self.failed.len() + self.passed.len(),
            self.failed.join("\n"),
            context()
        );
    }
}
