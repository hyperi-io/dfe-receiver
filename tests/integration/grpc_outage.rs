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
//! accepted after it came back. Answered at enqueue, the buffer carries the
//! outage; held, no record is answered 202 until the destination took it. The
//! destination is scalo's own `GrpcTransport` in server mode, standing in for
//! dfe-loader in-process.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, DestinationRef};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::Orchestrator;
use dfe_receiver::server::http::HttpHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::{AcknowledgementsConfig, TransportReceiver};
use tokio_util::sync::CancellationToken;

/// Records sent in each phase of the outage.
const PER_PHASE: u64 = 20;

/// How long a record may take to arrive after the destination returns: the
/// buffer's circuit stays open 30s after it trips, then drains.
const RECOVERY_BUDGET: Duration = Duration::from_secs(90);

/// A receiver config routing every record to the gRPC destination at
/// `loader_endpoint`, with the shipped acknowledgement default.
fn receiver_config(loader_endpoint: &str) -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.destinations.default = DestinationRef::One("loader".to_string());
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(loader_endpoint.to_string());
    config
}

/// Start the full receiver -- orchestrator drain tasks and HTTP ingest -- with
/// every record routed to the gRPC destination at `loader_endpoint`.
async fn start_receiver(loader_endpoint: &str, held: bool) -> (String, CancellationToken) {
    let mut config = receiver_config(loader_endpoint);
    config.server.acknowledgements = AcknowledgementsConfig::new(held);
    let (url, shutdown, _) = serve(config).await;
    (url, shutdown)
}

/// Start the full receiver on `config`: its URL, its shutdown, and its metrics.
async fn serve(config: Config) -> (String, CancellationToken, Arc<Metrics>) {
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

    let handler = HttpHandler::new(
        config.server.bind_address.clone(),
        pipeline,
        Arc::clone(&metrics),
    );
    let bound = handler.bound_addr();
    let server_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(server_shutdown).await });
    let addr = crate::common::bound_addr("HTTP", &bound, &mut task).await;
    (format!("http://{addr}"), shutdown, metrics)
}

/// POST one record per id and return each id's answer.
async fn post_statuses(
    url: &str,
    ids: impl IntoIterator<Item = u64>,
) -> Vec<(u64, reqwest::StatusCode)> {
    let client = reqwest::Client::new();
    let mut answers = Vec::new();
    for id in ids {
        let status = client
            .post(format!("{url}/ingest"))
            .header("content-type", "application/json")
            .body(format!(r#"{{"event_category":"outage","id":{id}}}"#))
            .send()
            .await
            .expect("request failed")
            .status();
        answers.push((id, status));
    }
    answers
}

/// POST one record per id and return the ids answered 202.
async fn post_ids(url: &str, ids: impl IntoIterator<Item = u64>) -> BTreeSet<u64> {
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
    // The destination's address is fixed up front and held closed through the outage.
    let closed = crate::common::ClosedPort::loopback().expect("hold the destination's port");
    let endpoint = format!("http://{}", closed.addr());
    let (url, shutdown) = start_receiver(&endpoint, false).await;

    let during = post_ids(&url, 0..PER_PHASE).await;
    assert!(
        !during.is_empty(),
        "no record was accepted while the destination was down, so nothing was held"
    );

    let destination = closed.release();
    let loader = GrpcTransport::new(&GrpcConfig::server(&destination.to_string()))
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

#[tokio::test]
async fn a_held_answer_is_202_only_once_the_destination_took_the_record() {
    let closed = crate::common::ClosedPort::loopback().expect("hold the destination's port");
    let endpoint = format!("http://{}", closed.addr());
    let (url, shutdown) = start_receiver(&endpoint, true).await;

    let during = post_ids(&url, 0..PER_PHASE).await;
    assert!(
        during.is_empty(),
        "answered 202 while the destination was down: {during:?}"
    );

    let destination = closed.release();
    let loader = GrpcTransport::new(&GrpcConfig::server(&destination.to_string()))
        .await
        .expect("bring the destination up on its original address");

    // The sender retries each record until it is answered 202.
    let deadline = tokio::time::Instant::now() + RECOVERY_BUDGET;
    let mut accepted = BTreeSet::new();
    while accepted.len() < PER_PHASE as usize && tokio::time::Instant::now() < deadline {
        let missing: Vec<u64> = (0..PER_PHASE).filter(|id| !accepted.contains(id)).collect();
        accepted.extend(post_ids(&url, missing).await);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        accepted.len(),
        PER_PHASE as usize,
        "the destination returned but only {accepted:?} were answered 202"
    );

    // A held 202 means the destination already had the record.
    let mut received = BTreeSet::new();
    while let Ok(batch) = loader.recv(100).await {
        if batch.records.is_empty() {
            break;
        }
        received.extend(batch.records.iter().filter_map(|r| record_id(&r.payload)));
    }
    shutdown.cancel();

    let missing: Vec<u64> = accepted.difference(&received).copied().collect();
    assert!(
        missing.is_empty(),
        "answered 202 before the destination had {missing:?}"
    );
}

/// How long the destination must stay quiet before the drain counts it done.
const QUIET: Duration = Duration::from_secs(3);

#[tokio::test]
async fn a_refused_destination_is_answered_503_and_each_record_arrives_once() {
    // The shipped acknowledgement default: nothing here turns holding on or off.
    let closed = crate::common::ClosedPort::loopback().expect("hold the destination's port");
    let endpoint = format!("http://{}", closed.addr());
    let (url, shutdown, metrics) = serve(receiver_config(&endpoint)).await;

    let refused = post_statuses(&url, 0..PER_PHASE).await;
    let not_503: Vec<_> = refused
        .iter()
        .filter(|(_, status)| *status != reqwest::StatusCode::SERVICE_UNAVAILABLE)
        .collect();
    assert!(
        not_503.is_empty(),
        "a refused destination must answer 503, got {not_503:?}"
    );
    assert_eq!(
        metrics.get_requests_success(),
        0,
        "a record the destination refused was counted delivered"
    );

    let destination = closed.release();
    let loader = GrpcTransport::new(&GrpcConfig::server(&destination.to_string()))
        .await
        .expect("bring the destination up on its original address");

    // The sender retries each record until it is answered 202.
    let deadline = tokio::time::Instant::now() + RECOVERY_BUDGET;
    let mut accepted = BTreeSet::new();
    while accepted.len() < PER_PHASE as usize && tokio::time::Instant::now() < deadline {
        let missing: Vec<u64> = (0..PER_PHASE).filter(|id| !accepted.contains(id)).collect();
        accepted.extend(post_ids(&url, missing).await);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        accepted.len(),
        PER_PHASE as usize,
        "the destination returned but only {accepted:?} were answered 202"
    );

    // Every copy the destination got, until it has been quiet for QUIET.
    let mut copies: std::collections::BTreeMap<u64, u32> = std::collections::BTreeMap::new();
    let mut quiet_since = tokio::time::Instant::now();
    while quiet_since.elapsed() < QUIET {
        if let Ok(batch) = loader.recv(100).await
            && !batch.records.is_empty()
        {
            for id in batch.records.iter().filter_map(|r| record_id(&r.payload)) {
                *copies.entry(id).or_default() += 1;
            }
            quiet_since = tokio::time::Instant::now();
        }
    }
    shutdown.cancel();

    let not_once: Vec<_> = (0..PER_PHASE)
        .map(|id| (id, copies.get(&id).copied().unwrap_or(0)))
        .filter(|(_, n)| *n != 1)
        .collect();
    assert!(
        not_once.is_empty(),
        "each record must arrive exactly once, (id, copies): {not_once:?}"
    );
    assert_eq!(metrics.get_requests_success(), PER_PHASE);
}
