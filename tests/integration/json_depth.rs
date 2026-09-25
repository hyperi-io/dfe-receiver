// Project:   dfe-receiver
// File:      tests/integration/json_depth.rs
// Purpose:   Deeply nested JSON is refused at the listeners without exhausting a worker stack
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deeply nested JSON against the real HTTP and webhook listeners.
//!
//! Tokio workers run on 2 MiB stacks, and sonic-rs recursing through 20,000
//! levels overflows one. Each test runs a current-thread runtime on a thread of
//! that size (bar a debug build of the at-the-bound test, see [`BOUND_STACK`]),
//! so the listener, the handler and every parse run on the stack production
//! gives them: a missing depth check aborts the test binary instead of failing
//! an assertion.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::future::Future;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{
    Config, SourceRule, WebhookAuthConfig, WebhookAuthMode, WebhookBody, WebhookCallerConfig,
};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::Orchestrator;
use dfe_receiver::server::http::HttpHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use dfe_receiver::server::webhook::WebhookHandler;
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;

/// The stack a Tokio worker thread gets by default.
const WORKER_STACK: usize = 2 * 1024 * 1024;

/// A stack that holds 64 levels of sonic-rs recursion, which costs about 56 KiB
/// a level in a debug build against under 200 bytes in release.
const BOUND_STACK: usize = if cfg!(debug_assertions) {
    16 * 1024 * 1024
} else {
    WORKER_STACK
};

/// The reason every depth refusal carries back to the sender.
const TOO_DEEP: &str = "payload nesting exceeds the maximum parse depth of 64";

/// Run `test` on a thread with a Tokio worker's stack, and fail unless it returns.
fn on_worker_stack<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    on_stack(WORKER_STACK, test);
}

/// Run `test` on a thread with `stack` bytes of stack, and fail unless it returns.
fn on_stack<F, Fut>(stack: usize, test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    let thread = std::thread::Builder::new()
        .name("json-depth".to_string())
        .stack_size(stack)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(test());
        })
        .expect("spawn the worker-sized thread");
    thread
        .join()
        .expect("the ingress thread must return rather than panic");
}

fn nested_array(depth: usize) -> String {
    format!("{}1{}", "[".repeat(depth), "]".repeat(depth))
}

fn nested_object(depth: usize) -> String {
    format!("{}1{}", "{\"a\":".repeat(depth), "}".repeat(depth))
}

/// The ingest listener on the loader's memory transport, answering a malformed
/// record with 400 rather than routing it to a DLQ this config has none of.
fn ingest_config() -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config.validation.dlq_on_invalid = false;
    config
}

struct Started {
    url: String,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
}

/// Start the ingest listener over a pipeline that counts on `metrics`.
async fn start_ingest(config: Config) -> Started {
    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let orchestrator = Orchestrator::new(config.clone(), metrics.clone(), shutdown.clone())
        .await
        .expect("pipeline init");

    let handler = HttpHandler::new(
        config.server.bind_address.clone(),
        orchestrator.state(),
        metrics.clone(),
    );
    let bound = handler.bound_addr();
    let server_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(server_shutdown).await });
    let addr = crate::common::bound_addr("HTTP", &bound, &mut task).await;

    Started {
        url: format!("http://{addr}/ingest"),
        metrics,
        shutdown,
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

/// POST `body` and hand back the status and the response text.
async fn post(url: &str, body: String) -> (u16, String) {
    let response = client()
        .post(url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("POST failed");
    let status = response.status().as_u16();
    (status, response.text().await.unwrap())
}

#[test]
fn a_deeply_nested_body_is_refused_and_the_listener_keeps_serving() {
    on_worker_stack(|| async {
        let started = start_ingest(ingest_config()).await;

        for depth in [20_000, 100_000] {
            for body in [nested_array(depth), nested_object(depth)] {
                let (status, text) = post(&started.url, body).await;
                assert_eq!(status, 400, "depth {depth}: {text}");
                assert!(text.contains(TOO_DEEP), "depth {depth}: {text}");
            }
        }
        assert_eq!(started.metrics.get_validation_failures_total(), 4);

        let (status, text) = post(&started.url, r#"{"ok":true}"#.to_string()).await;
        assert_eq!(
            status, 202,
            "the same process must still take a record: {text}"
        );

        started.shutdown.cancel();
    });
}

#[test]
fn a_deep_sibling_ahead_of_the_routing_field_is_refused_with_validation_off() {
    on_worker_stack(|| async {
        let mut config = ingest_config();
        // With validation off, routing is the first parse, and it walks past the sibling.
        config.validation.require_json = false;
        config.routing.source_rules = vec![SourceRule {
            field: "event_type".to_string(),
            mode: "key_value_use".to_string(),
            match_value: None,
            source: None,
        }];
        let started = start_ingest(config).await;

        let shallow = format!(r#"{{"sibling":{},"event_type":"auth"}}"#, nested_array(3));
        let (status, text) = post(&started.url, shallow).await;
        assert_eq!(status, 202, "a shallow sibling routes: {text}");

        for depth in [20_000, 100_000] {
            let deep = format!(
                r#"{{"sibling":{},"event_type":"auth"}}"#,
                nested_array(depth)
            );
            let (status, text) = post(&started.url, deep).await;
            assert_eq!(status, 400, "depth {depth}: {text}");
            assert!(text.contains(TOO_DEEP), "depth {depth}: {text}");
        }

        started.shutdown.cancel();
    });
}

#[test]
fn the_bound_is_64_levels_per_event() {
    on_stack(BOUND_STACK, || async {
        let started = start_ingest(ingest_config()).await;

        let (status, text) = post(&started.url, nested_object(64)).await;
        assert_eq!(status, 202, "64 levels is inside the bound: {text}");
        let (status, text) = post(&started.url, nested_object(65)).await;
        assert_eq!(status, 400, "65 levels is over it: {text}");
        assert!(text.contains(TOO_DEEP), "{text}");

        // The array around a batch is framing, not nesting in its events.
        let batch = format!("[{},{}]", nested_array(64), nested_array(64));
        let (status, text) = post(&started.url, batch).await;
        assert_eq!(status, 202, "a batch of 64-level events: {text}");
        let batch = format!("[{},{}]", nested_array(64), nested_array(65));
        let (status, text) = post(&started.url, batch).await;
        assert_eq!(status, 400, "a batch holding a 65-level event: {text}");
        assert!(text.contains(TOO_DEEP), "{text}");

        // NDJSON splits first, so only the deep line is refused.
        let ndjson = format!("{{\"a\":1}}\n{}", nested_array(65));
        let (status, text) = post(&started.url, ndjson).await;
        assert_eq!(status, 400, "{text}");
        assert!(text.contains(TOO_DEEP), "{text}");

        assert_eq!(started.metrics.get_validation_failures_total(), 3);
        started.shutdown.cancel();
    });
}

#[test]
fn malformed_bodies_that_hide_their_depth_are_refused_without_a_crash() {
    // Each hides 20,000 brackets from the depth count behind a token the
    // parser rejects before it reaches them.
    let hidden = "[".repeat(20_000);
    let bodies = [
        format!("\"{hidden}"),
        format!("[\\\"{hidden}"),
        format!("[1\"{hidden}"),
        format!("{{\"a\":1}}\n\"{hidden}"),
    ];
    on_worker_stack(move || async move {
        let started = start_ingest(ingest_config()).await;
        for body in bodies {
            let (status, text) = post(&started.url, body).await;
            assert_eq!(status, 400, "{text}");
        }
        let (status, text) = post(&started.url, r#"{"ok":true}"#.to_string()).await;
        assert_eq!(status, 202, "{text}");
        started.shutdown.cancel();
    });
}

const WEBHOOK_SECRET: &str = "a-static-shared-secret";

fn secret_file() -> (NamedTempFile, String) {
    let mut file = NamedTempFile::new().expect("create temp file");
    writeln!(file, "{WEBHOOK_SECRET}").expect("write secret");
    file.flush().expect("flush");
    let source = format!("file:{}", file.path().display());
    (file, source)
}

fn webhook_caller(name: &str, source: &str, body: WebhookBody) -> WebhookCallerConfig {
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
        body,
        filter: String::new(),
    }
}

#[test]
fn a_deeply_nested_webhook_record_is_refused_before_it_is_stamped() {
    let (file, source) = secret_file();
    on_worker_stack(move || async move {
        let _file = file;
        let mut config = ingest_config();
        config.webhook.enabled = true;
        config.webhook.bind_address = Some("127.0.0.1:0".to_string());
        config.webhook.callers = vec![
            webhook_caller("single", &source, WebhookBody::Single),
            webhook_caller("bulk", &source, WebhookBody::Array),
        ];

        let metrics = Arc::new(Metrics::default());
        let shutdown = CancellationToken::new();
        let orchestrator = Orchestrator::new(config.clone(), metrics.clone(), shutdown.clone())
            .await
            .expect("pipeline init");
        let handler = WebhookHandler::new(config, orchestrator.state(), metrics.clone());
        let bound = handler.bound_addr();
        let handler_shutdown = shutdown.clone();
        let mut task = tokio::spawn(async move { handler.start(handler_shutdown).await });
        let addr = crate::common::bound_addr("webhook", &bound, &mut task).await;

        let send = |caller: &'static str, body: String| {
            let url = format!("http://{addr}/webhook/{caller}");
            async move {
                let response = client()
                    .post(url)
                    .header("content-type", "application/json")
                    .header("x-webhook-secret", WEBHOOK_SECRET)
                    .body(body)
                    .send()
                    .await
                    .expect("POST failed");
                let status = response.status().as_u16();
                (status, response.text().await.unwrap())
            }
        };

        for depth in [20_000, 100_000] {
            let (status, text) = send("single", nested_object(depth)).await;
            assert_eq!(status, 400, "single, depth {depth}: {text}");
            assert!(text.contains(TOO_DEEP), "{text}");

            let (status, text) = send("bulk", format!("[{}]", nested_object(depth))).await;
            assert_eq!(status, 400, "bulk, depth {depth}: {text}");
            assert!(text.contains(TOO_DEEP), "{text}");
        }
        assert_eq!(metrics.get_validation_failures_total(), 4);

        let (status, text) = send("single", r#"{"ok":true}"#.to_string()).await;
        assert_eq!(status, 202, "{text}");

        shutdown.cancel();
    });
}
