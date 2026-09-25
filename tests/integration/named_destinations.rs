// Project:   dfe-receiver
// File:      tests/integration/named_destinations.rs
// Purpose:   Match rules to named gRPC destinations, and fan-out, in-process
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A matched record reaches the destination its rule names, and only that one.
//!
//! Three scalo Push listeners stand in for a transform, the archiver and the
//! loader. The assertion is the artefact -- which listener received the record
//! -- not that a router returned a name.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
// The config structs put the inline PipelineState future just over clippy's
// 16 KiB threshold, as in tests/integration/source_routing.rs.
#![allow(clippy::large_futures)]

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use dfe_receiver::config::{
    Config, DestinationRef, DestinationRule, DestinationSpec, GrpcDestination, SharedConfig,
};
use dfe_receiver::pipeline::PipelineState;
use scalo::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};
use scalo::transport::grpc::GrpcTransport;
use scalo::transport::{AcknowledgementsConfig, TransportReceiver};
use tokio_util::sync::CancellationToken;

/// Start a scalo Push listener: what a transform, the archiver and the loader
/// all expose on the direct transport.
async fn start_listener() -> (String, GrpcTransport) {
    crate::common::grpc_destination().await
}

/// Collect up to `want` records, or give up after `secs`.
async fn drain(server: &GrpcTransport, want: usize, secs: u64) -> Vec<Bytes> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while out.len() < want && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = server.recv(want).await {
            out.extend(batch.records.into_iter().map(|r| r.payload));
        }
    }
    out
}

fn grpc_destination(endpoint: &str) -> DestinationSpec {
    DestinationSpec {
        grpc: Some(GrpcDestination {
            endpoint: endpoint.to_string(),
        }),
        kafka: None,
    }
}

fn app_rule(value: &str, destination: DestinationRef) -> DestinationRule {
    DestinationRule {
        match_field: "app".to_string(),
        match_value: value.to_string(),
        destination,
    }
}

/// The receiver's destination set as the engine compiles it for a deployment
/// with no broker: a transform, an archiver, and the loader as the default.
fn config_with(named: HashMap<String, DestinationSpec>, rules: Vec<DestinationRule>) -> Config {
    let mut config = Config::default();
    config.server.auth.mode = "none".to_string();
    config.routing.dlq.enabled = false;
    config.destinations.default = "loader".into();
    config.destinations.named = named;
    config.destinations.rules = rules;
    config
}

async fn pipeline_for(config: Config) -> PipelineState {
    PipelineState::new(SharedConfig::new(config), CancellationToken::new())
        .await
        .expect("pipeline")
}

/// A pipeline whose guard reads its own reservations, so a synthetic byte
/// budget means something in a process whose real usage dwarfs it.
async fn pipeline_on_reservations(config: Config) -> PipelineState {
    let guard = MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes: config.buffer.memory_limit as u64,
            pressure_threshold: config.buffer.pressure_threshold,
            ..Default::default()
        },
        UsageSource::Reservations,
    );
    PipelineState::with_governor(
        SharedConfig::new(config),
        CancellationToken::new(),
        None,
        Some(std::sync::Arc::new(guard)),
    )
    .await
    .expect("pipeline")
}

/// Rule A lands on endpoint A, rule B on endpoint B, and an unmatched record on
/// the default -- each on its own listener, none on the others.
#[tokio::test]
async fn each_rule_reaches_only_its_own_destination() {
    let (transform_endpoint, transform) = start_listener().await;
    let (archiver_endpoint, archiver) = start_listener().await;
    let (loader_endpoint, loader) = start_listener().await;

    let mut named = HashMap::new();
    named.insert(
        "transform_orders".to_string(),
        grpc_destination(&transform_endpoint),
    );
    named.insert("archiver".to_string(), grpc_destination(&archiver_endpoint));
    named.insert("loader".to_string(), grpc_destination(&loader_endpoint));

    let config = config_with(
        named,
        vec![
            app_rule("orders", "transform_orders".into()),
            app_rule("audit", "archiver".into()),
        ],
    );
    let pipeline = pipeline_for(config).await;

    pipeline
        .process(Bytes::from(r#"{"app":"orders","id":1}"#))
        .await
        .expect("orders record accepted");
    pipeline
        .process(Bytes::from(r#"{"app":"audit","id":2}"#))
        .await
        .expect("audit record accepted");
    pipeline
        .process(Bytes::from(r#"{"app":"anything_else","id":3}"#))
        .await
        .expect("unmatched record accepted");

    for (name, server, expect_id) in [
        ("transform", &transform, 1),
        ("archiver", &archiver, 2),
        ("loader", &loader, 3),
    ] {
        let received = drain(server, 1, 10).await;
        assert_eq!(
            received.len(),
            1,
            "{name} received {} records",
            received.len()
        );
        let record: serde_json::Value =
            serde_json::from_slice(&received[0]).expect("valid JSON at {name}");
        assert_eq!(record["id"], expect_id, "wrong record at {name}");
    }
}

/// A rule whose destination is a LIST delivers the record to every one of them.
#[tokio::test]
async fn a_destination_list_fans_the_record_out() {
    let (loader_endpoint, loader) = start_listener().await;
    let (archiver_endpoint, archiver) = start_listener().await;

    let mut named = HashMap::new();
    named.insert("loader".to_string(), grpc_destination(&loader_endpoint));
    named.insert("archiver".to_string(), grpc_destination(&archiver_endpoint));

    let config = config_with(
        named,
        vec![app_rule(
            "orders",
            DestinationRef::Many(vec!["loader".to_string(), "archiver".to_string()]),
        )],
    );
    let pipeline = pipeline_for(config).await;

    pipeline
        .process(Bytes::from(r#"{"app":"orders","id":7}"#))
        .await
        .expect("record accepted");

    for (name, server) in [("loader", &loader), ("archiver", &archiver)] {
        let received = drain(server, 1, 10).await;
        assert_eq!(received.len(), 1, "{name} did not receive the record");
        let record: serde_json::Value = serde_json::from_slice(&received[0]).expect("valid JSON");
        assert_eq!(record["id"], 7, "wrong record at {name}");
    }
}

/// A destination that cannot be reached HOLDS records in the receiver's buffer
/// and then back-pressures the ingest, rather than dropping them to a DLQ that
/// a deployment with no broker does not have.
#[tokio::test]
async fn an_unreachable_destination_holds_then_back_pressures_the_ingest() {
    // Held for the whole test, so nothing can start listening on it.
    let unreachable = crate::common::ClosedPort::loopback().expect("hold a closed port");
    let mut named = HashMap::new();
    named.insert(
        "loader".to_string(),
        grpc_destination(&format!("http://{}", unreachable.addr())),
    );

    let mut config = config_with(named, vec![]);
    // The buffer exists only for a listener that answers at enqueue.
    config.server.acknowledgements = AcknowledgementsConfig::new(false);
    // A 2 KiB buffer budget, so the queue fills in tens of records.
    config.buffer.memory_limit = 2048;
    let pipeline = pipeline_on_reservations(config).await;

    // The buffer holds the first records, so the ingest keeps accepting.
    for id in 0..10 {
        pipeline
            .process(Bytes::from(format!(r#"{{"app":"orders","id":{id}}}"#)))
            .await
            .expect("a held record is accepted, not rejected");
    }

    // Once the buffer is full the ingest is told to hold off -- the record is
    // refused, never routed elsewhere.
    let mut refused = false;
    for id in 10..2_000 {
        if pipeline
            .process(Bytes::from(format!(r#"{{"app":"orders","id":{id}}}"#)))
            .await
            .is_err()
        {
            refused = true;
            break;
        }
    }
    assert!(
        refused,
        "a full buffer must back-pressure the ingest instead of accepting forever"
    );
}
