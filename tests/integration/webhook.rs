// Project:   dfe-receiver
// File:      tests/integration/webhook.rs
// Purpose:   Integration tests for the generic webhook intake
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for `POST /webhook/{caller}`.
//!
//! Every test starts a real handler and drives it with reqwest. The pipeline
//! runs on the loader's memory transport, so no broker is needed; the Kafka
//! round trip lives in `protocol_kafka_roundtrip.rs`.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::fmt::Write as _;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dfe_receiver::config::{
    Config, SharedConfig, WebhookAuthConfig, WebhookAuthMode, WebhookBody, WebhookCallerConfig,
};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::http;
use dfe_receiver::server::traits::ProtocolHandler;
use dfe_receiver::server::webhook::WebhookHandler;
use ring::hmac;
use scalo::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;

const HMAC_SECRET: &str = "a-signing-secret-for-tests";
const HEADER_SECRET: &str = "a-static-shared-secret";

/// A port the OS says is free right now.
fn random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// Poll the loopback port until it accepts, or panic on the budget.
async fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("port {port} never accepted connections within 30s");
}

/// Write a secret to a temp file and hand back the file (held open by the
/// caller) and its `file:` source reference.
fn secret_file(secret: &str) -> (NamedTempFile, String) {
    let mut file = NamedTempFile::new().expect("create temp file");
    writeln!(file, "{secret}").expect("write secret");
    file.flush().expect("flush");
    let source = format!("file:{}", file.path().display());
    (file, source)
}

fn hmac_caller(name: &str, source: &str) -> WebhookCallerConfig {
    WebhookCallerConfig {
        name: name.to_string(),
        topic: format!("{name}_land"),
        auth: WebhookAuthConfig {
            mode: WebhookAuthMode::Hmac,
            secret_source: source.to_string(),
            refresh_interval_secs: 0,
            ..WebhookAuthConfig::default()
        },
        body: WebhookBody::Single,
        filter: String::new(),
    }
}

fn header_caller(name: &str, source: &str) -> WebhookCallerConfig {
    WebhookCallerConfig {
        name: name.to_string(),
        topic: format!("{name}_land"),
        auth: WebhookAuthConfig {
            mode: WebhookAuthMode::Header,
            secret_source: source.to_string(),
            refresh_interval_secs: 0,
            header: "x-webhook-secret".to_string(),
            ..WebhookAuthConfig::default()
        },
        body: WebhookBody::Single,
        filter: String::new(),
    }
}

/// A config with the webhook intake on its own listener, no broker.
fn own_listener_config(port: u16, callers: Vec<WebhookCallerConfig>) -> Config {
    let mut config = Config::default();
    config.server.bind_address = format!("127.0.0.1:{}", random_port());
    config.server.auth.mode = "none".to_string();
    config.webhook.enabled = true;
    config.webhook.bind_address = Some(format!("127.0.0.1:{port}"));
    config.webhook.callers = callers;
    // The loader on its memory transport: accepted, sent nowhere, no broker.
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config
}

/// A config with the webhook routes on the shared ingest listener, which
/// itself requires a bearer token the webhook callers do not carry.
fn shared_listener_config(port: u16, callers: Vec<WebhookCallerConfig>) -> Config {
    let mut config = Config::default();
    config.server.bind_address = format!("127.0.0.1:{port}");
    config.server.auth.mode = "bearer".to_string();
    config.server.auth.bearer.tokens = vec!["ingest-token".to_string()];
    config.webhook.enabled = true;
    config.webhook.bind_address = None;
    config.webhook.callers = callers;
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config
}

struct Started {
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
    pipeline: Arc<PipelineState>,
    url: String,
}

async fn pipeline_for(config: &Config, guard: Option<Arc<MemoryGuard>>) -> Arc<PipelineState> {
    Arc::new(
        PipelineState::with_governor(
            SharedConfig::new(config.clone()),
            CancellationToken::new(),
            None,
            guard,
        )
        .await
        .expect("pipeline init"),
    )
}

/// Start the webhook handler on its own listener.
async fn start_own_listener(config: Config, guard: Option<Arc<MemoryGuard>>) -> Started {
    let port: u16 = config
        .webhook
        .bind_address
        .as_deref()
        .unwrap()
        .rsplit(':')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = pipeline_for(&config, guard).await;

    let handler = WebhookHandler::new(config, pipeline.clone(), metrics.clone());
    assert_eq!(handler.name(), "webhook");
    let handler_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if let Err(e) = handler.start(handler_shutdown).await {
            eprintln!("webhook handler exited with an error: {e}");
        }
    });
    wait_for_port(port).await;

    Started {
        shutdown,
        metrics,
        pipeline,
        url: format!("http://127.0.0.1:{port}"),
    }
}

/// Start the main HTTP server with the webhook routes merged in.
async fn start_shared_listener(config: Config) -> Started {
    let port: u16 = config
        .server
        .bind_address
        .rsplit(':')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = pipeline_for(&config, None).await;

    let bind = config.server.bind_address.clone();
    let server_pipeline = pipeline.clone();
    let server_metrics = metrics.clone();
    let server_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if let Err(e) =
            http::run_server(&bind, server_pipeline, server_metrics, server_shutdown).await
        {
            eprintln!("http server exited with an error: {e}");
        }
    });
    wait_for_port(port).await;

    Started {
        shutdown,
        metrics,
        pipeline,
        url: format!("http://127.0.0.1:{port}"),
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Sign the way a product would: HMAC-SHA256 over `"{ts}.{body}"`, hex.
fn sign(secret: &str, timestamp: u64, body: &[u8]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    let mut message = timestamp.to_string().into_bytes();
    message.push(b'.');
    message.extend_from_slice(body);
    hmac::sign(&key, &message)
        .as_ref()
        .iter()
        .fold(String::new(), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

async fn post_signed(
    url: &str,
    caller: &str,
    secret: &str,
    timestamp: u64,
    body: &str,
) -> reqwest::Response {
    client()
        .post(format!("{url}/webhook/{caller}"))
        .header("content-type", "application/json")
        .header("x-timestamp", timestamp.to_string())
        .header(
            "x-signature",
            format!("sha256={}", sign(secret, timestamp, body.as_bytes())),
        )
        .body(body.to_string())
        .send()
        .await
        .expect("POST failed")
}

// =============================================================================
// HMAC mode
// =============================================================================

#[tokio::test]
async fn a_signed_post_is_accepted_with_202() {
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let started = start_own_listener(
        own_listener_config(port, vec![hmac_caller("pager", &source)]),
        None,
    )
    .await;

    let resp = post_signed(
        &started.url,
        "pager",
        HMAC_SECRET,
        now_secs(),
        r#"{"event":"alert","severity":"high"}"#,
    )
    .await;
    assert_eq!(resp.status(), 202, "body: {}", resp.text().await.unwrap());
    assert_eq!(started.metrics.get_requests_total(), 1);
    assert_eq!(started.metrics.get_requests_success(), 1);
    assert_eq!(started.metrics.get_auth_failures_total(), 0);

    started.shutdown.cancel();
}

#[tokio::test]
async fn a_bad_signature_is_401_invalid_signature() {
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let started = start_own_listener(
        own_listener_config(port, vec![hmac_caller("pager", &source)]),
        None,
    )
    .await;

    let resp = post_signed(
        &started.url,
        "pager",
        "not-the-secret",
        now_secs(),
        r#"{"event":"alert"}"#,
    )
    .await;
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "invalid_signature");
    assert_eq!(started.metrics.get_auth_failures_total(), 1);
    assert_eq!(started.metrics.get_requests_error(), 1);

    started.shutdown.cancel();
}

#[tokio::test]
async fn a_missing_signature_is_401() {
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let started = start_own_listener(
        own_listener_config(port, vec![hmac_caller("pager", &source)]),
        None,
    )
    .await;

    let resp = client()
        .post(format!("{}/webhook/pager", started.url))
        .header("x-timestamp", now_secs().to_string())
        .body(r#"{"event":"alert"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "missing_signature");

    started.shutdown.cancel();
}

#[tokio::test]
async fn a_replayed_request_outside_the_window_is_401_stale_signature() {
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let mut config = own_listener_config(port, vec![hmac_caller("pager", &source)]);
    config.webhook.callers[0].auth.tolerance_secs = 60;
    let started = start_own_listener(config, None).await;

    // A correctly signed request whose timestamp is ten minutes old.
    let old = now_secs() - 600;
    let resp = post_signed(
        &started.url,
        "pager",
        HMAC_SECRET,
        old,
        r#"{"event":"alert"}"#,
    )
    .await;
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "stale_signature");
    assert_eq!(started.metrics.get_auth_failures_total(), 1);

    started.shutdown.cancel();
}

#[tokio::test]
async fn a_tampered_body_is_401() {
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let started = start_own_listener(
        own_listener_config(port, vec![hmac_caller("pager", &source)]),
        None,
    )
    .await;

    let ts = now_secs();
    let signature = sign(HMAC_SECRET, ts, br#"{"amount":1}"#);
    let resp = client()
        .post(format!("{}/webhook/pager", started.url))
        .header("x-timestamp", ts.to_string())
        .header("x-signature", signature)
        .body(r#"{"amount":1000}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    started.shutdown.cancel();
}

// =============================================================================
// Header mode
// =============================================================================

#[tokio::test]
async fn a_static_header_secret_is_accepted_and_a_wrong_one_refused() {
    let (_file, source) = secret_file(HEADER_SECRET);
    let port = random_port();
    let started = start_own_listener(
        own_listener_config(port, vec![header_caller("runzero", &source)]),
        None,
    )
    .await;

    let resp = client()
        .post(format!("{}/webhook/runzero", started.url))
        .header("x-webhook-secret", HEADER_SECRET)
        .body(r#"{"text":"alert"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    let resp = client()
        .post(format!("{}/webhook/runzero", started.url))
        .header("x-webhook-secret", "guess")
        .body(r#"{"text":"alert"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "invalid_header_value");

    let resp = client()
        .post(format!("{}/webhook/runzero", started.url))
        .body(r#"{"text":"alert"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "missing_auth_header");

    assert_eq!(started.metrics.get_auth_failures_total(), 2);
    started.shutdown.cancel();
}

#[tokio::test]
async fn a_caller_secret_is_not_valid_for_another_caller() {
    // Two callers, two secrets: the path pins which secret is tried.
    let (_a, source_a) = secret_file("secret-for-a");
    let (_b, source_b) = secret_file("secret-for-b");
    let port = random_port();
    let started = start_own_listener(
        own_listener_config(
            port,
            vec![header_caller("a", &source_a), header_caller("b", &source_b)],
        ),
        None,
    )
    .await;

    let resp = client()
        .post(format!("{}/webhook/b", started.url))
        .header("x-webhook-secret", "secret-for-a")
        .body(r#"{"x":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    let resp = client()
        .post(format!("{}/webhook/b", started.url))
        .header("x-webhook-secret", "secret-for-b")
        .body(r#"{"x":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    started.shutdown.cancel();
}

// =============================================================================
// Routing, size, readiness
// =============================================================================

#[tokio::test]
async fn an_unknown_caller_is_404() {
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let started = start_own_listener(
        own_listener_config(port, vec![hmac_caller("pager", &source)]),
        None,
    )
    .await;

    let resp = post_signed(
        &started.url,
        "nobody",
        HMAC_SECRET,
        now_secs(),
        r#"{"event":"alert"}"#,
    )
    .await;
    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "unknown_caller");

    started.shutdown.cancel();
}

#[tokio::test]
async fn an_oversize_body_is_413_and_counted() {
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let mut config = own_listener_config(port, vec![hmac_caller("pager", &source)]);
    config.webhook.max_body_size = 256;
    let started = start_own_listener(config, None).await;

    let big = format!(r#"{{"pad":"{}"}}"#, "x".repeat(1024));
    let resp = post_signed(&started.url, "pager", HMAC_SECRET, now_secs(), &big).await;
    assert_eq!(resp.status(), 413);
    assert_eq!(
        started.metrics.get_body_size_rejected_total(),
        1,
        "the 413 must leave a body_size_rejected count behind"
    );
    assert_eq!(started.metrics.get_requests_error(), 1);

    // Within the limit still works.
    let resp = post_signed(
        &started.url,
        "pager",
        HMAC_SECRET,
        now_secs(),
        r#"{"ok":true}"#,
    )
    .await;
    assert_eq!(resp.status(), 202);

    started.shutdown.cancel();
}

#[tokio::test]
async fn a_request_under_pressure_is_503_with_retry_after() {
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let mut config = own_listener_config(port, vec![hmac_caller("pager", &source)]);
    config.buffer.memory_limit = 100;
    config.buffer.pressure_threshold = 0.8;
    let guard = Arc::new(MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes: 100,
            pressure_threshold: 0.8,
            ..Default::default()
        },
        UsageSource::Reservations,
    ));
    let started = start_own_listener(config, Some(guard)).await;

    started.pipeline.memory_guard().add_bytes(90);
    assert!(!started.pipeline.is_ready());

    let resp = post_signed(
        &started.url,
        "pager",
        HMAC_SECRET,
        now_secs(),
        r#"{"event":"alert"}"#,
    )
    .await;
    assert_eq!(resp.status(), 503);
    assert_eq!(resp.headers().get("retry-after").unwrap(), "5");

    started.pipeline.memory_guard().release(90);
    let resp = post_signed(
        &started.url,
        "pager",
        HMAC_SECRET,
        now_secs(),
        r#"{"event":"alert"}"#,
    )
    .await;
    assert_eq!(resp.status(), 202);

    started.shutdown.cancel();
}

#[tokio::test]
async fn readiness_is_not_probed_without_authentication() {
    // An unauthenticated client gets 401 whatever the pipeline's state.
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let mut config = own_listener_config(port, vec![hmac_caller("pager", &source)]);
    config.buffer.memory_limit = 100;
    let guard = Arc::new(MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes: 100,
            pressure_threshold: 0.8,
            ..Default::default()
        },
        UsageSource::Reservations,
    ));
    let started = start_own_listener(config, Some(guard)).await;
    started.pipeline.memory_guard().add_bytes(90);

    let resp = post_signed(
        &started.url,
        "pager",
        "wrong",
        now_secs(),
        r#"{"event":"alert"}"#,
    )
    .await;
    assert_eq!(resp.status(), 401);

    started.pipeline.memory_guard().release(90);
    started.shutdown.cancel();
}

// =============================================================================
// Body shapes and the filter
// =============================================================================

#[tokio::test]
async fn an_array_body_is_fanned_out_when_declared() {
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let mut caller = hmac_caller("bulk", &source);
    caller.body = WebhookBody::Array;
    let started = start_own_listener(own_listener_config(port, vec![caller]), None).await;

    let resp = post_signed(
        &started.url,
        "bulk",
        HMAC_SECRET,
        now_secs(),
        r#"[{"n":1},{"n":2},{"n":3}]"#,
    )
    .await;
    assert_eq!(resp.status(), 202);

    // A non-array body on an array caller is the product's shape being wrong.
    let resp = post_signed(&started.url, "bulk", HMAC_SECRET, now_secs(), r#"{"n":1}"#).await;
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "body_not_an_array");

    started.shutdown.cancel();
}

#[tokio::test]
async fn an_array_with_a_bad_element_is_400_as_a_whole() {
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let mut caller = hmac_caller("bulk", &source);
    caller.body = WebhookBody::Array;
    let started = start_own_listener(own_listener_config(port, vec![caller]), None).await;

    let resp = post_signed(
        &started.url,
        "bulk",
        HMAC_SECRET,
        now_secs(),
        r#"[{"a":1}, 5]"#,
    )
    .await;
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "record_not_an_object");
    assert_eq!(started.metrics.get_requests_error(), 1);
    assert_eq!(started.metrics.get_requests_success(), 0);

    started.shutdown.cancel();
}

#[tokio::test]
async fn a_record_that_is_not_an_object_is_400() {
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let started = start_own_listener(
        own_listener_config(port, vec![hmac_caller("pager", &source)]),
        None,
    )
    .await;

    let resp = post_signed(
        &started.url,
        "pager",
        HMAC_SECRET,
        now_secs(),
        r#"["not","an","object"]"#,
    )
    .await;
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "record_not_an_object");

    started.shutdown.cancel();
}

#[tokio::test]
async fn a_filtered_out_record_is_dropped_and_still_202() {
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let mut caller = hmac_caller("pager", &source);
    caller.filter = r#"severity == "high""#.to_string();
    let started = start_own_listener(own_listener_config(port, vec![caller]), None).await;

    let resp = post_signed(
        &started.url,
        "pager",
        HMAC_SECRET,
        now_secs(),
        r#"{"severity":"low"}"#,
    )
    .await;
    assert_eq!(resp.status(), 202);
    let resp = post_signed(
        &started.url,
        "pager",
        HMAC_SECRET,
        now_secs(),
        r#"{"severity":"high"}"#,
    )
    .await;
    assert_eq!(resp.status(), 202);
    assert_eq!(started.metrics.get_requests_success(), 2);

    started.shutdown.cancel();
}

// =============================================================================
// Shared listener
// =============================================================================

#[tokio::test]
async fn on_the_shared_listener_the_caller_secret_is_the_only_credential_needed() {
    // The ingest listener requires a bearer token; the webhook route on the
    // same port must not, and must still refuse a bad caller secret.
    let (_file, source) = secret_file(HEADER_SECRET);
    let port = random_port();
    let started = start_shared_listener(shared_listener_config(
        port,
        vec![header_caller("runzero", &source)],
    ))
    .await;

    let resp = client()
        .post(format!("{}/webhook/runzero", started.url))
        .header("x-webhook-secret", HEADER_SECRET)
        .body(r#"{"text":"alert"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202, "body: {}", resp.text().await.unwrap());

    let resp = client()
        .post(format!("{}/webhook/runzero", started.url))
        .header("x-webhook-secret", "guess")
        .body(r#"{"text":"alert"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // The ingest route keeps its own auth: the caller secret is no bearer token.
    let resp = client()
        .post(format!("{}/ingest", started.url))
        .header("x-webhook-secret", HEADER_SECRET)
        .body(r#"{"text":"alert"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    started.shutdown.cancel();
}

#[tokio::test]
async fn on_the_shared_listener_the_webhook_body_limit_is_its_own() {
    let (_file, source) = secret_file(HEADER_SECRET);
    let port = random_port();
    let mut config = shared_listener_config(port, vec![header_caller("runzero", &source)]);
    config.webhook.max_body_size = 256;
    config.server.max_body_size = 1024 * 1024;
    let started = start_shared_listener(config).await;

    let big = format!(r#"{{"pad":"{}"}}"#, "x".repeat(1024));
    let resp = client()
        .post(format!("{}/webhook/runzero", started.url))
        .header("x-webhook-secret", HEADER_SECRET)
        .body(big.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
    assert_eq!(started.metrics.get_body_size_rejected_total(), 1);

    // The same body is fine on the ingest route under its larger limit.
    let resp = client()
        .post(format!("{}/ingest", started.url))
        .header("authorization", "Bearer ingest-token")
        .body(big)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    started.shutdown.cancel();
}

// =============================================================================
// Own-listener admission limits (server.ip_filter, server.rate_limit)
// =============================================================================

#[tokio::test]
async fn on_its_own_listener_a_denied_source_address_is_dropped_before_auth() {
    // The IP filter runs at accept, so a denied peer gets no HTTP response at
    // all and the handler never sees the request.
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let mut config = own_listener_config(port, vec![hmac_caller("pager", &source)]);
    config.server.ip_filter.mode = "denylist".to_string();
    config.server.ip_filter.cidrs = vec!["127.0.0.1/32".to_string()];
    let started = start_own_listener(config, None).await;

    let body = r#"{"event":"alert"}"#;
    let ts = now_secs();
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
        .post(format!("{}/webhook/pager", started.url))
        .header("x-timestamp", ts.to_string())
        .header(
            "x-signature",
            format!("sha256={}", sign(HMAC_SECRET, ts, body.as_bytes())),
        )
        .body(body)
        .send()
        .await;
    assert!(
        response.is_err(),
        "a denied source address must get no response, got: {:?}",
        response.ok().map(|r| r.status())
    );
    assert_eq!(started.metrics.get_requests_total(), 0);
    assert_eq!(started.metrics.get_auth_failures_total(), 0);

    started.shutdown.cancel();
}

#[tokio::test]
async fn on_its_own_listener_an_exhausted_rate_limit_is_429_with_retry_after() {
    // No proxy header, as a product posting straight to the port sends none:
    // the limiter keys on the peer address the accept loop hands it.
    let (_file, source) = secret_file(HMAC_SECRET);
    let port = random_port();
    let mut config = own_listener_config(port, vec![hmac_caller("pager", &source)]);
    config.server.rate_limit.enabled = true;
    config.server.rate_limit.requests_per_second = 1;
    config.server.rate_limit.burst = 1;
    let started = start_own_listener(config, None).await;

    let body = r#"{"event":"alert"}"#;
    let ts = now_secs();
    let signature = format!("sha256={}", sign(HMAC_SECRET, ts, body.as_bytes()));
    let mut requests = Vec::new();
    for _ in 0..5 {
        let url = format!("{}/webhook/pager", started.url);
        let signature = signature.clone();
        requests.push(tokio::spawn(async move {
            client()
                .post(url)
                .header("x-timestamp", ts.to_string())
                .header("x-signature", signature)
                .body(body)
                .send()
                .await
                .expect("POST failed")
        }));
    }

    let mut accepted = 0usize;
    let mut limited = 0usize;
    for request in requests {
        let response = request.await.expect("request task panicked");
        match response.status().as_u16() {
            202 => accepted += 1,
            429 => {
                limited += 1;
                assert!(
                    response.headers().get("retry-after").is_some(),
                    "a 429 must carry retry-after: {:?}",
                    response.headers()
                );
                assert!(
                    response.headers().get("x-ratelimit-after").is_some(),
                    "a 429 must carry x-ratelimit-after: {:?}",
                    response.headers()
                );
            }
            other => panic!("unexpected status {other}"),
        }
    }
    assert!(accepted >= 1, "the burst must admit the first request");
    assert!(
        limited >= 1,
        "five requests in one second must exhaust a limit of one"
    );
    assert_eq!(
        started.metrics.get_requests_total(),
        accepted as u64,
        "a rate-limited request must not reach the handler"
    );

    started.shutdown.cancel();
}

// =============================================================================
// Startup refusal
// =============================================================================

#[tokio::test]
async fn a_caller_whose_secret_cannot_be_read_refuses_to_start() {
    let port = random_port();
    let config = own_listener_config(
        port,
        vec![hmac_caller("pager", "file:/nonexistent/webhook-secret")],
    );
    let pipeline = pipeline_for(&config, None).await;
    let handler = WebhookHandler::new(config, pipeline, Arc::new(Metrics::default()));
    let err = handler
        .start(CancellationToken::new())
        .await
        .expect_err("a caller with no secret must not start serving");
    assert!(
        err.to_string().contains("nonexistent"),
        "the error must name the source: {err}"
    );
}
