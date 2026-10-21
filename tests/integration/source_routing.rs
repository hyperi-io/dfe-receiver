// Project:   dfe-receiver
// File:      tests/integration/source_routing.rs
// Purpose:   gRPC ingest -> source routing -> loader forward, in-process
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Ingest through the router to the loader, with no external infrastructure.
//!
//! Two origins share one rule set. A fetcher-based source is matched on the
//! top-level `_source` dfe-fetcher stamps on every record; a receiver-based
//! source is matched on a field of its own and needs `_source` written into the
//! record on the way past, because dfe-loader reads it to pick the table.
//! Every record also leaves with the `_org_id` its sender's binding allows.
//!
//! The gRPC tests drive the receiver's own listener with scalo's
//! `VectorCompatClient`, the client dfe-fetcher's extractors use. The HTTP
//! tests POST to `/ingest`. Both assert what reaches the loader.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
// The config structs put the inline PipelineState future just over clippy's
// 16 KiB threshold, as in tests/integration/vector.rs.
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, OrgBinding, SharedConfig};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::grpc::GrpcVectorHandler;
use dfe_receiver::server::http::HttpHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use scalo::rules::{MatchConfig, RuleConfig, SetAction};
use scalo::transport::grpc::GrpcTransport;
use scalo::transport::{TransportReceiver, VectorCompatClient};
use tokio_util::sync::CancellationToken;

/// A row setting `target` to `set` on a record whose `field` equals `value`.
fn row(id: &str, field: &str, value: &str, target: &str, set: &str) -> RuleConfig {
    RuleConfig::new(id, target, SetAction::value(set)).matching(MatchConfig::equals(field, value))
}

/// The row the engine compiles for a fetcher-origin source on its own topic.
fn fetcher_source_rule(source: &str) -> RuleConfig {
    row(source, "_source", source, "_source", source)
}

/// One enriched record as dfe-fetcher emits it.
fn fetcher_record(source: &str) -> serde_json::Value {
    serde_json::json!({
        "crate": "dfe-fetcher",
        "downloads": 42,
        "_timestamp_fetcher": 1_757_000_000_000_u64,
        "_timestamp_received": 1_757_000_000_000_u64,
        "_source": source,
        "_source_fetcher": "crates_io.crates",
    })
}

/// Start the receiver's gRPC listener over a pipeline built from `config`.
async fn start_receiver_grpc(config: Config) -> (SocketAddr, CancellationToken) {
    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("pipeline"),
    );

    // The default gRPC auth mode is none, so the handler registers no interceptor.
    let handler = GrpcVectorHandler::new(config, pipeline, metrics);
    let bound = handler.bound_addr();
    let server_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(server_shutdown).await });
    let addr = crate::common::bound_addr("gRPC", &bound, &mut task).await;
    (addr, shutdown)
}

/// Start the receiver's HTTP listener over a pipeline built from `config`.
async fn start_receiver_http(config: Config) -> (SocketAddr, CancellationToken) {
    let metrics = Arc::new(Metrics::default());
    let shutdown = CancellationToken::new();
    let pipeline = Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("pipeline"),
    );

    let handler = HttpHandler::new(config.server.bind_address.clone(), pipeline, metrics);
    let bound = handler.bound_addr();
    let server_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(server_shutdown).await });
    let addr = crate::common::bound_addr("HTTP", &bound, &mut task).await;
    (addr, shutdown)
}

/// A receiver-based source: routed on a field of its own, not on `_source`.
fn kvproof_config(loader_endpoint: String) -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    config.destinations.default = "loader".into();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint);
    config.routing.dlq.enabled = false;
    config.rules.rules = vec![row("kvproof", "app", "kvproof", "_source", "kvproof")];
    config
}

/// Collect up to `want` records from the mock loader, or give up after 10s.
async fn drain_loader(server: &GrpcTransport, want: usize) -> Vec<bytes::Bytes> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while out.len() < want && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = server.recv(want).await {
            out.extend(batch.records.into_iter().map(|r| r.payload));
        }
    }
    out
}

/// A record pushed over gRPC reaches the loader with its `_source` intact.
///
/// This is the no-Kafka forward: `destinations.default = loader` with
/// `loader.transport = grpc`. The loader maps `_source` to the table, so the
/// receiver must hand the field through untouched.
#[tokio::test]
async fn test_grpc_push_forwards_source_to_the_loader() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;

    let mut config = Config::default();
    config.grpc.enabled = true;
    config.grpc.bind_address = "127.0.0.1:0".to_string();
    config.destinations.default = "loader".into();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint);
    config.rules.rules = vec![fetcher_source_rule("crates_audit")];
    config.routing.dlq.enabled = false;

    let (addr, shutdown) = start_receiver_grpc(config).await;

    let client =
        VectorCompatClient::connect_lazy(&format!("http://{addr}")).expect("vector client");
    client
        .send_events(&[fetcher_record("crates_audit")])
        .await
        .expect("push_events");

    let received = drain_loader(&loader, 1).await;
    shutdown.cancel();

    assert_eq!(
        received.len(),
        1,
        "loader should receive exactly one record"
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&received[0]).expect("loader payload is JSON");
    assert_eq!(parsed["_source"], "crates_audit");
    assert!(parsed.get("_source_original").is_none(), "{parsed}");
    assert_eq!(parsed["_source_fetcher"], "crates_io.crates");
    assert_eq!(parsed["crate"], "dfe-fetcher");
    // The receiver writes its own receive time, once, and keeps the sender's beside it.
    assert!(
        parsed["_timestamp_received"].as_u64() > Some(1_757_000_000_000),
        "{parsed}"
    );
    assert_eq!(
        parsed["_timestamp_received_original"],
        1_757_000_000_000_u64
    );
    assert_eq!(parsed["_org_id"], "default");
    let body = std::str::from_utf8(&received[0]).expect("utf8");
    assert_eq!(body.matches("\"_timestamp_received\"").count(), 1, "{body}");
}

/// The gRPC ingest accepts a gzip-compressed PushEvents.
///
/// `VectorCompatClient` compresses unconditionally, so a server without the
/// encoding enabled rejects every scalo-originated push.
#[tokio::test]
async fn test_grpc_push_accepts_gzip() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;

    let mut config = Config::default();
    config.grpc.enabled = true;
    config.grpc.bind_address = "127.0.0.1:0".to_string();
    config.destinations.default = "loader".into();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint);
    config.routing.dlq.enabled = false;

    let (addr, shutdown) = start_receiver_grpc(config).await;

    let client =
        VectorCompatClient::connect_lazy(&format!("http://{addr}")).expect("vector client");
    let result = client.send_events(&[fetcher_record("main")]).await;
    let received = drain_loader(&loader, 1).await;
    shutdown.cancel();

    result.expect("gzip-compressed push_events must be accepted");
    assert_eq!(received.len(), 1);
}

/// A 10 MiB push is taken: the gRPC listener decodes up to
/// `grpc.max_message_size` (16 MiB), past tonic's own 4 MiB default.
#[tokio::test]
async fn test_grpc_push_takes_ten_mebibytes() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;

    let mut config = Config::default();
    config.grpc.enabled = true;
    config.grpc.bind_address = "127.0.0.1:0".to_string();
    config.destinations.default = "loader".into();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint);
    config.routing.dlq.enabled = false;

    let (addr, shutdown) = start_receiver_grpc(config).await;

    let mut record = fetcher_record("main");
    record["blob"] = serde_json::Value::String("x".repeat(10 * 1024 * 1024));
    let client =
        VectorCompatClient::connect_lazy(&format!("http://{addr}")).expect("vector client");
    let result = client.send_events(&[record]).await;
    let received = drain_loader(&loader, 1).await;
    shutdown.cancel();

    result.expect("a 10 MiB push_events must be taken");
    assert_eq!(received.len(), 1);
}

/// A receiver-based source reaches the loader with `_source` written in.
///
/// The loader route computes no topic, so the matched rule is only recoverable
/// from the record. Without the stamp the row lands in the default table with
/// `_source` NULL, which is what a live slim deploy showed.
#[tokio::test]
async fn test_http_ingest_stamps_the_matched_source_for_the_loader() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let (addr, shutdown) = start_receiver_http(kvproof_config(loader_endpoint)).await;

    let response = reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .header("content-type", "application/json")
        .body(r#"{"app":"kvproof","message":"hello"}"#)
        .send()
        .await
        .expect("POST /ingest");
    assert!(
        response.status().is_success(),
        "ingest rejected: {}",
        response.status()
    );

    let received = drain_loader(&loader, 1).await;
    shutdown.cancel();

    assert_eq!(
        received.len(),
        1,
        "loader should receive exactly one record"
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&received[0]).expect("loader payload is JSON");
    assert_eq!(parsed["_source"], "kvproof");
    assert_eq!(parsed["app"], "kvproof");
    assert_eq!(parsed["message"], "hello");
    assert!(parsed["_timestamp_received"].is_number());
    assert!(parsed.get("_timestamp_receiver").is_none());
    assert_eq!(parsed["_org_id"], "default");
}

/// A payload no rule matches reaches the loader stamped with the catch-all.
///
/// The loader route carries no topic, so an unstamped record lands with
/// `_source` NULL and a table picked by the loader's own fallback.
#[tokio::test]
async fn test_http_ingest_stamps_the_catch_all_source_when_no_rule_matches() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let (addr, shutdown) = start_receiver_http(kvproof_config(loader_endpoint)).await;

    let response = reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .header("content-type", "application/json")
        .body(r#"{"app":"something_else","message":"hello"}"#)
        .send()
        .await
        .expect("POST /ingest");
    assert!(response.status().is_success());

    let received = drain_loader(&loader, 1).await;
    shutdown.cancel();

    assert_eq!(received.len(), 1);
    let parsed: serde_json::Value =
        serde_json::from_slice(&received[0]).expect("loader payload is JSON");
    assert_eq!(parsed["_source"], "main");
    assert_eq!(parsed["app"], "something_else");
    assert_eq!(parsed["message"], "hello");
}

/// A sender's own `_source` loses to a matching rule and is kept as `_source_original`.
///
/// A second top-level key of the same name makes the loader's ClickHouse JSON
/// column reject the whole record, so the rule's value replaces it in place.
#[tokio::test]
async fn test_http_ingest_parks_a_sender_source_a_rule_replaces() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let (addr, shutdown) = start_receiver_http(kvproof_config(loader_endpoint)).await;

    let response = reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .header("content-type", "application/json")
        .body(r#"{"app":"kvproof","_source":"crates_audit"}"#)
        .send()
        .await
        .expect("POST /ingest");
    assert!(response.status().is_success());

    let received = drain_loader(&loader, 1).await;
    shutdown.cancel();

    assert_eq!(received.len(), 1);
    let body = std::str::from_utf8(&received[0]).expect("utf8");
    assert_eq!(
        body.matches("\"_source\"").count(),
        1,
        "exactly one _source must reach the loader: {body}"
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&received[0]).expect("loader payload is JSON");
    assert_eq!(parsed["_source"], "kvproof");
    assert_eq!(parsed["_source_original"], "crates_audit");
}

/// POST one body to `/ingest` and return the record the loader received.
async fn ingest_one(
    config: Config,
    loader: &GrpcTransport,
    body: &'static str,
) -> serde_json::Value {
    let (addr, shutdown) = start_receiver_http(config).await;
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("POST /ingest");
    assert!(response.status().is_success());

    let received = drain_loader(loader, 1).await;
    shutdown.cancel();
    assert_eq!(received.len(), 1);
    serde_json::from_slice(&received[0]).expect("loader payload is JSON")
}

/// A sender's `_org_id` is removed by default, so it cannot pick another org's tenant.
#[tokio::test]
async fn test_http_ingest_drops_a_sender_org_id_by_default() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let body = r#"{"app":"kvproof","_org_id":"victim","org":"kept"}"#;
    let parsed = ingest_one(kvproof_config(loader_endpoint), &loader, body).await;

    assert_eq!(
        parsed["_org_id"], "default",
        "the sender's _org_id reached the loader: {parsed}"
    );
    assert_eq!(parsed["org"], "kept");
    assert_eq!(parsed["_source"], "kvproof");
}

/// `kvproof_config` with sender `_org_id` allowed and the HTTP listener bound to `orgs`.
fn allowing(loader_endpoint: String, orgs: &[&str]) -> Config {
    let mut config = kvproof_config(loader_endpoint);
    config.routing.source_org_id = dfe_receiver::config::SourceOrgId::Allow;
    if !orgs.is_empty() {
        config.ingest_bindings.listeners.insert(
            "http".to_string(),
            OrgBinding {
                orgs: orgs.iter().map(ToString::to_string).collect(),
                default_org: None,
            },
        );
    }
    config
}

/// With `source_org_id: allow`, a sender's `_org_id` inside its listener's binding reaches the loader.
#[tokio::test]
async fn test_http_ingest_keeps_a_sender_org_id_when_allowed_and_bound() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let config = allowing(loader_endpoint, &["acme"]);
    let parsed = ingest_one(config, &loader, r#"{"app":"kvproof","_org_id":"acme"}"#).await;

    assert_eq!(parsed["_org_id"], "acme");
}

/// Allowed or not, an unbound listener's records land in the default org only.
#[tokio::test]
async fn test_http_ingest_clamps_an_allowed_sender_org_id_on_an_unbound_listener() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let config = allowing(loader_endpoint, &[]);
    let parsed = ingest_one(config, &loader, r#"{"app":"kvproof","_org_id":"acme"}"#).await;

    assert_eq!(parsed["_org_id"], "default", "{parsed}");
}

/// A key spelled with JSON escapes is the same key once decoded, so it is stripped too.
#[tokio::test]
async fn test_http_ingest_drops_a_sender_org_id_spelled_with_escapes() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let body = "{\"app\":\"kvproof\",\"\\u005forg_id\":\"victim\",\"org\":\"kept\"}";
    let parsed = ingest_one(kvproof_config(loader_endpoint), &loader, body).await;

    assert_eq!(
        parsed["_org_id"], "default",
        "the sender's _org_id reached the loader: {parsed}"
    );
    assert_eq!(parsed["org"], "kept");
}

/// The bearer entries every tenancy test below authenticates with.
const TOKENS: [&str; 2] = ["acme-ingest:token-acme-1", "token-unnamed-1"];

/// `kvproof_config` behind bearer auth, the `acme-ingest` credential bound to
/// `acme`, and rows that try to land records in `globex` and `acme`.
fn bearer_tenancy(loader_endpoint: String) -> Config {
    let mut config = kvproof_config(loader_endpoint);
    config.server.auth.mode = "bearer".to_string();
    config.server.auth.bearer.tokens = TOKENS.map(scalo::SensitiveString::new).to_vec();
    config.server.auth.bearer.refresh_interval_secs = 0;
    config.ingest_bindings.credentials.insert(
        "acme-ingest".to_string(),
        OrgBinding {
            orgs: vec!["acme".to_string()],
            default_org: None,
        },
    );
    config.rules.rules.extend([
        row("to-globex", "app", "steal", "_org_id", "globex"),
        row("to-acme", "app", "own", "_org_id", "acme"),
    ]);
    config
}

/// POST each body with `token` and return the records the loader received, in order.
async fn ingest_as(
    config: Config,
    loader: &GrpcTransport,
    token: &str,
    bodies: &[&'static str],
) -> Vec<serde_json::Value> {
    let (addr, shutdown) = start_receiver_http(config).await;
    let client = reqwest::Client::new();
    for body in bodies {
        let response = client
            .post(format!("http://{addr}/ingest"))
            .header("content-type", "application/json")
            .bearer_auth(token)
            .body(*body)
            .send()
            .await
            .expect("POST /ingest");
        assert!(response.status().is_success(), "{}", response.status());
    }
    let received = drain_loader(loader, bodies.len()).await;
    shutdown.cancel();
    assert_eq!(received.len(), bodies.len());
    received
        .iter()
        .map(|record| serde_json::from_slice(record).expect("loader payload is JSON"))
        .collect()
}

/// A bound token cannot land a record outside its orgs, whatever a row or the sender says.
#[tokio::test]
async fn test_a_bound_token_cannot_land_a_record_outside_its_orgs() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let records = ingest_as(
        bearer_tenancy(loader_endpoint),
        &loader,
        "token-acme-1",
        &[
            r#"{"app":"steal","n":1}"#,
            r#"{"app":"own","n":2}"#,
            r#"{"app":"other","_org_id":"globex","n":3}"#,
        ],
    )
    .await;

    let orgs: Vec<&str> = records
        .iter()
        .map(|record| record["_org_id"].as_str().expect("an org"))
        .collect();
    assert_eq!(orgs, ["acme", "acme", "acme"], "{records:?}");
}

/// A token whose entry names no credential is unbound, so its records land in the default org only.
#[tokio::test]
async fn test_an_unnamed_token_lands_records_in_the_default_org_only() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let records = ingest_as(
        bearer_tenancy(loader_endpoint),
        &loader,
        "token-unnamed-1",
        &[r#"{"app":"own","n":1}"#, r#"{"app":"steal","n":2}"#],
    )
    .await;

    for record in &records {
        assert_eq!(record["_org_id"], "default", "{record}");
    }
}

/// POST `body`, which carries one record that is not an object and then
/// `{"app":"kvproof","n":1}`: the request is answered 400 and only the object
/// reaches the loader.
async fn assert_only_the_object_reaches_the_loader(body: &'static str) {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let (addr, shutdown) = start_receiver_http(kvproof_config(loader_endpoint)).await;

    let response = reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("POST /ingest");
    let status = response.status();
    let answer = response.text().await.expect("answer body");

    let received = drain_loader(&loader, 1).await;
    shutdown.cancel();

    assert_eq!(
        status, 400,
        "the record that is not an object was taken: {answer}"
    );
    assert!(answer.contains("record_not_an_object"), "{answer}");
    assert_eq!(received.len(), 1, "the object must still be delivered");
    let parsed: serde_json::Value =
        serde_json::from_slice(&received[0]).expect("loader payload is JSON");
    assert!(
        parsed.is_object(),
        "loader got a record that is not an object: {parsed}"
    );
    assert_eq!(parsed["n"], 1, "loader got the wrong record: {parsed}");
    assert_eq!(parsed["_org_id"], "default", "{parsed}");
    assert!(
        !std::str::from_utf8(&received[0])
            .expect("utf8")
            .contains("victim"),
        "a sender _org_id reached the loader: {parsed}"
    );
}

/// An array element of an array body is refused.
#[tokio::test]
async fn test_http_ingest_refuses_an_array_inside_an_array_body() {
    assert_only_the_object_reaches_the_loader(
        r#"[[{"app":"kvproof","_org_id":"victim"}],{"app":"kvproof","n":1}]"#,
    )
    .await;
}

/// An NDJSON line that is an array is refused.
#[tokio::test]
async fn test_http_ingest_refuses_an_ndjson_line_that_is_an_array() {
    assert_only_the_object_reaches_the_loader(
        "[{\"app\":\"kvproof\",\"_org_id\":\"victim\"}]\n{\"app\":\"kvproof\",\"n\":1}",
    )
    .await;
}

/// An NDJSON line that is a scalar is refused.
#[tokio::test]
async fn test_http_ingest_refuses_an_ndjson_line_that_is_a_scalar() {
    assert_only_the_object_reaches_the_loader("5\n{\"app\":\"kvproof\",\"n\":1}").await;
}

/// With enrichment off, no source is written and the record takes only its org.
#[tokio::test]
async fn test_http_ingest_adds_no_source_when_enrichment_is_disabled() {
    let (loader_endpoint, loader) = crate::common::grpc_destination().await;
    let mut config = kvproof_config(loader_endpoint);
    config.server.auth.include_common_header = false;
    let (addr, shutdown) = start_receiver_http(config).await;

    let response = reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .header("content-type", "application/json")
        .body(r#"{"app":"kvproof","message":"hello"}"#)
        .send()
        .await
        .expect("POST /ingest");
    assert!(response.status().is_success());

    let received = drain_loader(&loader, 1).await;
    shutdown.cancel();

    assert_eq!(received.len(), 1);
    assert_eq!(
        &received[0][..],
        br#"{"app":"kvproof","message":"hello","_org_id":"default"}"#
    );
}
