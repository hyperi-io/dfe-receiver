// Project:   dfe-receiver
// File:      tests/integration/record_counts.rs
// Purpose:   The records_* counters count records, not requests, on the real listeners
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A request carrying N records moves the `records_*` counters by N.
//!
//! Each test drives a real listener over the pipeline the orchestrator builds,
//! and reads the counters from a recorder on the test's own thread: a
//! `#[tokio::test]` runtime is single-threaded, so the listener tasks record
//! into it too. The loader runs on its memory transport, so nothing external is
//! needed.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
// Test helpers build a PipelineState inline; the config structs put the future
// just over clippy's 16 KiB threshold. Mirrors the lib crate's allow (main.rs).
#![allow(clippy::large_futures)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dfe_receiver::config::Config;
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::{Orchestrator, PipelineState};
use dfe_receiver::server::grpc::GrpcVectorHandler;
use dfe_receiver::server::http::HttpHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use parking_lot::Mutex;
use scalo::metrics::{MetricsConfig, MetricsManager};
use scalo::transport::VectorCompatClient;
use tokio_util::sync::CancellationToken;

/// Sums each counter across its label sets, as a `sum()` over the name reads it.
#[derive(Default)]
struct CounterTotals {
    counters: Mutex<HashMap<String, Arc<AtomicU64>>>,
}

impl CounterTotals {
    fn total(&self, name: &str) -> u64 {
        self.counters
            .lock()
            .get(name)
            .map_or(0, |cell| cell.load(Ordering::Relaxed))
    }
}

impl metrics::Recorder for CounterTotals {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        let cell = Arc::clone(
            self.counters
                .lock()
                .entry(key.name().to_string())
                .or_default(),
        );
        metrics::Counter::from_arc(cell)
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// A config whose records the loader's memory transport takes and discards.
fn test_config() -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    config.grpc.enabled = true;
    config.grpc.bind_address = "127.0.0.1:0".to_string();
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config
}

/// The receiver's metrics on a manager, and the pipeline the orchestrator
/// builds over them, as the service wires both.
///
/// Call with the test's recorder already in place: the app metric group binds
/// its handles when it is built.
async fn counted_pipeline(config: &Config) -> (Arc<PipelineState>, Arc<Metrics>) {
    let manager = MetricsManager::with_config(MetricsConfig::offline(""));
    let metrics = Arc::new(Metrics::register_on(
        Arc::new(config.scaling.build_pressure()),
        &manager,
    ));
    let orchestrator = Orchestrator::new(
        config.clone(),
        Arc::clone(&metrics),
        CancellationToken::new(),
    )
    .await
    .expect("pipeline");
    (orchestrator.state(), metrics)
}

/// Start `handler` and wait for its listener to bind.
async fn serve<H: ProtocolHandler + 'static>(
    handler: H,
    bound: &dfe_receiver::server::traits::BoundAddr,
    name: &str,
) -> (SocketAddr, CancellationToken) {
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(server_shutdown).await });
    let addr = crate::common::bound_addr(name, bound, &mut task).await;
    (addr, shutdown)
}

/// An NDJSON POST of five records counts five records, and one request.
#[tokio::test]
async fn an_http_batch_counts_every_record_it_carries() {
    let totals = CounterTotals::default();
    let _recorder = metrics::set_default_local_recorder(&totals);
    let config = test_config();
    let (pipeline, metrics) = counted_pipeline(&config).await;

    let handler = HttpHandler::new(config.server.bind_address.clone(), pipeline, metrics);
    let bound = handler.bound_addr();
    let (addr, shutdown) = serve(handler, &bound, "HTTP").await;

    let body = (1..=5)
        .map(|i| format!(r#"{{"event_category":"e{i}"}}"#))
        .collect::<Vec<_>>()
        .join("\n");
    let status = reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("request failed")
        .status();
    shutdown.cancel();

    assert!(status.is_success(), "ingest refused: {status}");
    assert_eq!(totals.total("receiver_requests_total"), 1, "one request");
    assert_eq!(
        totals.total("records_received_total"),
        5,
        "five records received"
    );
    assert_eq!(
        totals.total("records_processed_total"),
        5,
        "five records taken"
    );
    assert_eq!(
        totals.total("records_delivered_total"),
        5,
        "five records taken"
    );
    assert_eq!(totals.total("records_error_total"), 0, "none refused");
}

/// A gRPC push of four events counts four records, and one request.
#[tokio::test]
async fn a_grpc_push_counts_every_event_it_carries() {
    let totals = CounterTotals::default();
    let _recorder = metrics::set_default_local_recorder(&totals);
    let config = test_config();
    let (pipeline, metrics) = counted_pipeline(&config).await;

    let handler = GrpcVectorHandler::new(config, pipeline, metrics);
    let bound = handler.bound_addr();
    let (addr, shutdown) = serve(handler, &bound, "gRPC").await;

    let events: Vec<serde_json::Value> = (1..=4)
        .map(|i| serde_json::json!({ "event_category": format!("e{i}") }))
        .collect();
    let pushed = VectorCompatClient::connect_lazy(&format!("http://{addr}"))
        .expect("vector client")
        .send_events(&events)
        .await;
    shutdown.cancel();

    pushed.expect("push_events");
    assert_eq!(totals.total("receiver_requests_total"), 1, "one request");
    assert_eq!(
        totals.total("records_received_total"),
        4,
        "four records received"
    );
    assert_eq!(
        totals.total("records_delivered_total"),
        4,
        "four records taken"
    );
}

/// A record refused for good counts as received and as an error, and the
/// records around it as taken.
#[tokio::test]
async fn a_refused_record_counts_as_received_and_as_an_error() {
    let totals = CounterTotals::default();
    let _recorder = metrics::set_default_local_recorder(&totals);
    let mut config = test_config();
    config.validation.dlq_on_invalid = false;
    let (pipeline, metrics) = counted_pipeline(&config).await;

    let handler = HttpHandler::new(config.server.bind_address.clone(), pipeline, metrics);
    let bound = handler.bound_addr();
    let (addr, shutdown) = serve(handler, &bound, "HTTP").await;

    let status = reqwest::Client::new()
        .post(format!("http://{addr}/ingest"))
        .header("content-type", "application/json")
        .body("{\"ok\":1}\nnot json\n{\"ok\":2}")
        .send()
        .await
        .expect("request failed")
        .status();
    shutdown.cancel();

    assert!(
        status.is_client_error(),
        "a refused record is the sender's to hear: {status}"
    );
    assert_eq!(totals.total("records_received_total"), 3);
    assert_eq!(totals.total("records_delivered_total"), 2);
    assert_eq!(totals.total("records_error_total"), 1);
}
