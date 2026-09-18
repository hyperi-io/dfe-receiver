// Project:   dfe-receiver
// File:      tests/integration/grpc_outage.rs
// Purpose:   A record answered 202 survives a gRPC destination outage
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The receiver's no-loss contract across a gRPC destination outage.
//!
//! Every record the HTTP ingest answers 202 for must reach the destination
//! once it returns, including records accepted while it was down and records
//! accepted after it came back. The destination is scalo's own `GrpcTransport`
//! in server mode, standing in for dfe-loader in-process.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, DestinationRef};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::Orchestrator;
use dfe_receiver::server::http;
use scalo::transport::TransportReceiver;
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use tokio_util::sync::CancellationToken;

/// Records sent in each phase of the outage.
const PER_PHASE: u64 = 20;

/// How long a record may take to arrive after the destination returns: the
/// buffer's circuit stays open 30s after it trips, then drains.
const RECOVERY_BUDGET: Duration = Duration::from_secs(90);

fn random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

async fn port_accepts(port: u16, attempts: u32) -> bool {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..attempts {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Start the full receiver -- orchestrator drain tasks and HTTP ingest -- with
/// every record routed to the gRPC destination at `loader_endpoint`.
async fn start_receiver(loader_endpoint: &str) -> (String, CancellationToken) {
    for _ in 0..20 {
        let port = random_port();

        let mut config = Config::default();
        config.server.bind_address = format!("127.0.0.1:{port}");
        config.destinations.default = DestinationRef::One("loader".to_string());
        config.loader.transport = "grpc".to_string();
        config.loader.grpc_endpoint = Some(loader_endpoint.to_string());

        let shutdown = CancellationToken::new();
        let metrics = Arc::new(Metrics::default());
        let orchestrator = Orchestrator::new(config.clone(), metrics.clone(), shutdown.clone())
            .await
            .expect("orchestrator init");
        let pipeline = orchestrator.state();
        orchestrator.start().expect("orchestrator start");
        let drain_shutdown = shutdown.clone();
        tokio::spawn(async move {
            drain_shutdown.cancelled().await;
            let _ = orchestrator.shutdown().await;
        });

        let bind_addr = config.server.bind_address.clone();
        let server_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = http::run_server(&bind_addr, pipeline, metrics, server_shutdown).await;
        });

        if port_accepts(port, 40).await {
            return (format!("http://127.0.0.1:{port}"), shutdown);
        }
        shutdown.cancel();
    }
    panic!("receiver never accepted connections on any of 20 ports");
}

/// POST one record per id and return the ids answered 202.
async fn post_ids(url: &str, ids: std::ops::Range<u64>) -> BTreeSet<u64> {
    let client = reqwest::Client::new();
    let mut accepted = BTreeSet::new();
    for id in ids {
        let status = client
            .post(format!("{url}/ingest"))
            .header("content-type", "application/json")
            .body(format!(r#"{{"event_category":"outage","id":{id}}}"#))
            .send()
            .await
            .expect("request failed")
            .status();
        if status == reqwest::StatusCode::ACCEPTED {
            accepted.insert(id);
        }
    }
    accepted
}

/// The `id` field of a record the destination received.
fn record_id(payload: &[u8]) -> Option<u64> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    value.get("id")?.as_u64()
}

#[tokio::test]
async fn every_record_answered_202_arrives_after_a_destination_outage() {
    // The destination's address is fixed up front, with nothing listening on it.
    let port = random_port();
    let endpoint = format!("http://127.0.0.1:{port}");
    let (url, shutdown) = start_receiver(&endpoint).await;

    let during = post_ids(&url, 0..PER_PHASE).await;
    assert!(
        !during.is_empty(),
        "no record was accepted while the destination was down, so nothing was held"
    );

    let loader = GrpcTransport::new(&GrpcConfig::server(&format!("127.0.0.1:{port}")))
        .await
        .expect("bring the destination up on its original address");

    let after = post_ids(&url, PER_PHASE..2 * PER_PHASE).await;
    let accepted: BTreeSet<u64> = during.union(&after).copied().collect();

    let deadline = tokio::time::Instant::now() + RECOVERY_BUDGET;
    let mut received = BTreeSet::new();
    while !accepted.is_subset(&received) && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = loader.recv(100).await {
            received.extend(batch.records.iter().filter_map(|r| record_id(&r.payload)));
        }
    }
    shutdown.cancel();

    let lost: Vec<u64> = accepted.difference(&received).copied().collect();
    assert!(
        lost.is_empty(),
        "{} of {} records answered 202 never arrived: {lost:?} \
         (accepted during the outage: {}, after: {})",
        lost.len(),
        accepted.len(),
        during.len(),
        after.len()
    );
}
