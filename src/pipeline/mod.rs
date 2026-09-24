// Project:   dfe-receiver
// File:      src/pipeline/mod.rs
// Purpose:   Main processing pipeline orchestration
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Pipeline orchestration module.
//!
//! Coordinates the flow of messages through validation, routing,
//! and delivery to sinks with backpressure support.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// State-change flag for memory pressure log deduplication.
static PRESSURE_LOGGED: AtomicBool = AtomicBool::new(false);

use scalo::UnifiedPressure;
use scalo::dlq::{Dlq, DlqEntry};
use scalo::logger::security;

use rustc_hash::FxHashMap;

use crate::buffer::{
    InMemoryBuffer, MemoryGuard, MemoryGuardConfig, MemoryPressure, Rejects, SinkBackend,
};
use crate::config::{Config, SharedConfig};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::routing::{self, RouteResult, Router};
use crate::server::traits::BoundAddr;
use crate::sink::Sink;
use crate::sink::file::FileSink;
use crate::sink::grpc::GrpcSink;
use crate::sink::kafka::KafkaSink;
use crate::validation::{ValidationResult, Validator};

// Trace-level logging imports (only used for per-message tracing)
use tracing::trace;

/// Tracked bytes released on drop, so a cancelled request cannot leak them.
///
/// A client disconnect or request timeout drops the in-flight future between
/// `add_bytes` and `release`, and the counter never decays: the guard then
/// reports pressure forever, `/readyz` 503s while `/livez` keeps passing, and
/// nothing restarts the pod.
#[must_use]
struct MemoryLease<'a> {
    guard: &'a MemoryGuard,
    bytes: u64,
}

impl<'a> MemoryLease<'a> {
    // Borrowed, not `Arc<MemoryGuard>`: the borrow proves the lease cannot
    // outlive the guard, and spends no refcount pair per event on the hot path.
    fn acquire(guard: &'a MemoryGuard, bytes: u64) -> Self {
        guard.add_bytes(bytes);
        Self { guard, bytes }
    }
}

impl Drop for MemoryLease<'_> {
    fn drop(&mut self) {
        self.guard.release(self.bytes);
    }
}

/// What a named destination resolves to.
///
/// The name is the routing decision; this is the delivery. Every variant keeps
/// the receiver's own buffer/spillover semantics in front of it, so a
/// destination that stops accepting back-pressures the HTTP ingest rather than
/// spilling records to a DLQ a brokerless deployment does not have.
enum DestinationSink {
    /// The bus, using the topic the record's source resolves to, or a topic
    /// fixed by the destination.
    Bus { topic: Option<Arc<str>> },
    /// A scalo Push listener -- a transform, the loader, the archiver.
    Grpc(Arc<SinkBackend<GrpcSink>>),
    /// `loader.transport: memory` -- no transport, so the record is accepted
    /// and goes nowhere.
    Discard,
}

/// Shared pipeline state accessible from handlers.
pub struct PipelineState {
    shared_config: SharedConfig,
    validator: RwLock<Validator>,
    router: RwLock<Router>,
    kafka_sink: Option<Arc<SinkBackend<KafkaSink>>>,
    /// The named destination set: name -> sink. Built once at startup, since
    /// the endpoints are connections and a chart rolls the pod on any config
    /// change.
    destinations: FxHashMap<Arc<str>, DestinationSink>,
    file_sink: Option<Arc<FileSink>>,
    memory_guard: Arc<MemoryGuard>,
    /// Self-regulation pressure latch from the runtime governor.
    ///
    /// `Some` when self-regulation is enabled (the default): the originator
    /// ingest brake (HTTP 503 / gRPC `UNAVAILABLE`) consults this unified,
    /// hysteretic pressure signal -- the HARD memory source is the never-OOM
    /// authority. `None` when `self_regulation.enabled = false`, in which case
    /// the brake falls back to the bespoke memory-guard `under_pressure()`
    /// check (byte-identical to pre-governor behaviour).
    pressure: Option<Arc<UnifiedPressure>>,
    dlq: Option<Arc<Dlq>>,
    /// The listeners readiness waits on, `None` until the server declares them.
    listeners: RwLock<Option<Vec<BoundAddr>>>,
}

impl PipelineState {
    /// Create new pipeline state with self-regulation disabled.
    ///
    /// The ingest brake falls back to the bespoke YAML-configured memory-guard
    /// threshold check (byte-identical to pre-governor behaviour). Used by
    /// tests and the `self_regulation.enabled = false` path. Production wires
    /// the governor via [`with_governor`](Self::with_governor).
    pub async fn new(shared_config: SharedConfig, shutdown: CancellationToken) -> Result<Self> {
        Self::with_governor(shared_config, shutdown, None, None).await
    }

    /// Create new pipeline state, optionally wired to the runtime governor.
    ///
    /// `governor` is the runtime self-regulation governor (the originator brake
    /// source of truth). When `Some`, the pipeline tracks ingest bytes on the
    /// governor's OWN memory guard so the governor's HARD memory source -- and
    /// thus the `UnifiedPressure` latch the ingest brake consults -- actually
    /// reacts to in-flight load. When `None` (self-regulation disabled, or
    /// tests) the pipeline builds the bespoke YAML-configured guard and the
    /// brake falls back to its threshold check (byte-identical to pre-governor
    /// behaviour).
    pub async fn with_governor(
        shared_config: SharedConfig,
        shutdown: CancellationToken,
        governor: Option<&scalo::SelfRegulationGovernor>,
        runtime_memory_guard: Option<Arc<MemoryGuard>>,
    ) -> Result<Self> {
        Self::build(
            shared_config,
            shutdown,
            governor,
            runtime_memory_guard,
            None,
        )
        .await
    }

    /// [`with_governor`](Self::with_governor), counting dead-lettered records
    /// on `metrics` when it is `Some`.
    async fn build(
        shared_config: SharedConfig,
        shutdown: CancellationToken,
        governor: Option<&scalo::SelfRegulationGovernor>,
        runtime_memory_guard: Option<Arc<MemoryGuard>>,
        metrics: Option<Arc<Metrics>>,
    ) -> Result<Self> {
        let config = shared_config.get();
        let validator = Validator::new(config.validation.clone());
        // The built-in `loader` is compiled into a declared destination here, so
        // the router and the sink set below both see one kind of destination.
        let destinations_config = config.resolved_destinations();
        let router = Router::new(
            &config.routing,
            &destinations_config,
            config.server.auth.include_common_header,
        );
        // Memory guard + pressure source of truth.
        //
        // With self-regulation ON, reuse the runtime's guard (the one the
        // governor's HARD memory source watches) so ingest byte reservations
        // feed the UnifiedPressure latch. Otherwise build the bespoke
        // YAML-configured guard (env > YAML > cgroup auto-detect).
        let (memory_guard, pressure) = match (governor, runtime_memory_guard) {
            (Some(gov), Some(guard)) => (guard, Some(gov.pressure())),
            // An injected guard is the caller's whether a governor came with it
            // or not: building a second one leaves two guards accounting for the
            // same process.
            (None, Some(guard)) => (guard, None),
            _ => {
                let mut mg_config = MemoryGuardConfig::from_env("DFE_RECEIVER");
                if config.buffer.memory_limit > 0 && mg_config.limit_bytes == 0 {
                    mg_config.limit_bytes = config.buffer.memory_limit as u64;
                }
                if (config.buffer.pressure_threshold - 0.8).abs() > f64::EPSILON {
                    mg_config.pressure_threshold = config.buffer.pressure_threshold;
                }
                (Arc::new(MemoryGuard::new(mg_config)), None)
            }
        };

        // DLQ (unified scalo module - cascade: Kafka primary, file fallback)
        let dlq = if config.routing.dlq.enabled {
            let dlq_config = config.routing.dlq.to_scalo_config();
            let kafka_config = config.kafka.to_scalo_kafka_config();
            match Dlq::spawn(
                &dlq_config,
                "receiver",
                Some(&kafka_config),
                shutdown.clone(),
            ) {
                Ok(d) => {
                    info!(mode = ?dlq_config.mode, "DLQ enabled");
                    Some(Arc::new(d))
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create DLQ, disabled");
                    None
                }
            }
        } else {
            debug!("DLQ disabled by config");
            None
        };
        // Records a destination refuses for good go to the same DLQ.
        let rejects = Rejects::new(dlq.clone(), metrics);

        // Initialise Kafka sink with buffer wrapper if brokers configured
        let kafka_sink = if !config.kafka.brokers.is_empty() {
            let primary = KafkaSink::new(&config.kafka)?;
            Some(Arc::new(
                build_sink_backend(primary, &config.buffer, rejects.clone()).await?,
            ))
        } else {
            None
        };

        // Build the named destination set: one sink per destination the config
        // actually refers to, so an unused declaration opens no connection.
        let mut destinations: FxHashMap<Arc<str>, DestinationSink> = FxHashMap::default();
        for name in destinations_config.referenced_names() {
            if destinations.contains_key(name) {
                continue;
            }
            let sink = match destinations_config.named.get(name) {
                Some(spec) => match (&spec.grpc, &spec.kafka) {
                    (Some(grpc), _) => {
                        // loader.timeout_ms bounds the loader's own Push RPC; a
                        // declared destination has no timeout key of its own.
                        let deadline = (name == crate::config::LOADER_DESTINATION)
                            .then_some(config.loader.timeout_ms);
                        let primary = GrpcSink::new(&grpc.endpoint, deadline).await?;
                        DestinationSink::Grpc(Arc::new(
                            build_sink_backend(primary, &config.buffer, rejects.clone()).await?,
                        ))
                    }
                    (None, Some(bus)) => DestinationSink::Bus {
                        topic: bus.topic.as_deref().map(Arc::from),
                    },
                    (None, None) => {
                        return Err(Error::Config(format!(
                            "destination '{name}' needs exactly one of grpc or kafka"
                        )));
                    }
                },
                // `loader` survives resolution only on the memory transport.
                None if name == crate::config::LOADER_DESTINATION => DestinationSink::Discard,
                None => DestinationSink::Bus { topic: None },
            };
            destinations.insert(Arc::from(name), sink);
        }

        // Initialise debug file sink if enabled
        let file_sink = if config.file_sink.enabled {
            match FileSink::new(&config.file_sink.path) {
                Ok(sink) => Some(Arc::new(sink)),
                Err(e) => {
                    warn!(error = %e, path = %config.file_sink.path, "Failed to open file sink, disabled");
                    None
                }
            }
        } else {
            None
        };

        Ok(Self {
            shared_config,
            validator: RwLock::new(validator),
            router: RwLock::new(router),
            kafka_sink,
            destinations,
            file_sink,
            memory_guard,
            pressure,
            dlq,
            listeners: RwLock::new(None),
        })
    }

    /// Get the current configuration.
    pub fn config(&self) -> Config {
        self.shared_config.get()
    }

    /// Get the shared config handle.
    pub fn shared_config(&self) -> SharedConfig {
        self.shared_config.clone()
    }

    /// Admission check for a request: shed with 503 when this is false.
    ///
    /// Consults sink health as well as pressure, because accepting a record
    /// the pipeline cannot deliver loses it. Listener state stays with
    /// [`probe_ready`](Self::probe_ready): one listener that failed to bind
    /// does not shed the traffic another one is serving.
    pub fn is_ready(&self) -> bool {
        if self.under_pressure() {
            return false;
        }

        // Check sink health
        if let Some(ref kafka) = self.kafka_sink
            && !kafka.is_healthy()
        {
            return false;
        }

        // Every named destination must be able to take a record: a record the
        // rules send to one of them cannot be served by the others.
        self.destinations.values().all(|sink| match sink {
            DestinationSink::Bus { .. } | DestinationSink::Discard => true,
            DestinationSink::Grpc(s) => s.is_healthy(),
        })
    }

    /// What `/readyz` answers: every listener the server enabled is bound and
    /// serving, and there is no pressure. NOT sink health.
    ///
    /// False until [`watch_listeners`](Self::watch_listeners) declares the
    /// listeners, so a probe that lands before the server starts cannot pass.
    ///
    /// A sink outage is shared by every replica, so failing the probe on it
    /// empties the Service of endpoints fleet-wide and turns a degradation
    /// into an outage. Shedding stays with [`is_ready`](Self::is_ready), which
    /// still answers 503 + retry-after per request while the pod remains
    /// routable.
    pub fn probe_ready(&self) -> bool {
        let serving = self
            .listeners
            .read()
            .as_deref()
            .is_some_and(|listeners| listeners.iter().all(BoundAddr::is_serving));

        // Not ready under high pressure (originator brake source of truth).
        serving && !self.under_pressure()
    }

    /// Declare the listeners [`probe_ready`](Self::probe_ready) waits on:
    /// every one must be bound and serving for the probe to pass. Replaces
    /// any earlier declaration.
    pub fn watch_listeners(&self, listeners: Vec<BoundAddr>) {
        *self.listeners.write() = Some(listeners);
    }

    /// Get memory pressure level.
    pub fn memory_pressure(&self) -> MemoryPressure {
        self.memory_guard.pressure()
    }

    /// Originator pressure signal -- the source of truth for the ingest brake.
    ///
    /// When self-regulation is enabled, this is the runtime governor's unified,
    /// hysteretic pressure latch (HARD memory source = never-OOM authority).
    /// When disabled, it falls back to the bespoke memory-guard threshold
    /// check, byte-identical to pre-governor behaviour.
    ///
    /// This is an INBOUND brake only. Under pressure the HTTP/gRPC ingest
    /// handlers shed (503 / `UNAVAILABLE`) BEFORE accepting -- relying on
    /// upstream retry. The OUTBOUND drain (Kafka / loader / tiered sink) is
    /// NEVER gated here; gating the drain would deadlock the pipeline.
    #[inline]
    fn under_pressure(&self) -> bool {
        match self.pressure {
            Some(ref p) => p.should_hold(),
            None => self.memory_guard.under_pressure(),
        }
    }

    /// Check if backpressure should be applied.
    pub fn should_apply_backpressure(&self) -> bool {
        self.under_pressure()
    }

    /// Process a message through the pipeline.
    ///
    /// This is a HOT PATH function.
    #[inline]
    pub async fn process(&self, payload: Bytes) -> Result<()> {
        // Check for backpressure
        if self.should_apply_backpressure() {
            if scalo::logger::log_state_change(&PRESSURE_LOGGED, true) {
                warn!("Memory pressure HIGH -- backpressure active");
            }
            return Err(Error::Buffer("server under memory pressure".into()));
        }
        // Log recovery when pressure drops
        if scalo::logger::log_state_change(&PRESSURE_LOGGED, false) {
            info!("Memory pressure recovered");
        }

        // Tracked for the life of this future.
        let _lease = MemoryLease::acquire(&self.memory_guard, payload.len() as u64);

        self.process_inner(payload).await
    }

    /// Process a batch of messages through the pipeline.
    ///
    /// Amortises overhead: single backpressure check, single memory tracking
    /// update, and per-message process_inner() calls. Each message is processed
    /// independently -- a failure in one does not stop the rest.
    ///
    /// Returns the count of successfully processed messages and the first error
    /// (if any). Callers can use the success count for metrics.
    pub async fn process_batch(&self, payloads: &[Bytes]) -> (usize, Option<Error>) {
        if payloads.is_empty() {
            return (0, None);
        }

        // Single backpressure check for the entire batch
        if self.should_apply_backpressure() {
            if scalo::logger::log_state_change(&PRESSURE_LOGGED, true) {
                warn!("Memory pressure HIGH -- backpressure active (batch)");
            }
            return (
                0,
                Some(Error::Buffer("server under memory pressure".into())),
            );
        }
        if scalo::logger::log_state_change(&PRESSURE_LOGGED, false) {
            info!("Memory pressure recovered");
        }

        // One lease for the whole batch, held across every message.
        let total_bytes: u64 = payloads.iter().map(|p| p.len() as u64).sum();
        let _lease = MemoryLease::acquire(&self.memory_guard, total_bytes);

        let mut success_count = 0usize;
        let mut first_error: Option<Error> = None;

        for payload in payloads {
            match self.process_inner(payload.clone()).await {
                Ok(()) => success_count += 1,
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        (success_count, first_error)
    }

    /// Check if enrichment (timestamp injection, source rules) is enabled.
    #[inline]
    fn enrichment_enabled(&self) -> bool {
        self.shared_config
            .with(|c| c.server.auth.include_common_header)
    }

    /// Inject `_timestamp_receiver` (now, epoch ms) into a validated JSON
    /// object payload.
    #[inline]
    #[must_use]
    pub fn enrich_payload(payload: Bytes) -> Bytes {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        Self::enrich_payload_at(payload, now_ms)
    }

    /// Inject `_timestamp_receiver` with the given epoch-millisecond value.
    ///
    /// Performs byte-level append before the closing `}` to avoid a full
    /// JSON parse/rewrite on the hot path. The key is appended whether or not
    /// the payload already carries one; a caller that must not produce a
    /// duplicate key checks first.
    #[inline]
    #[must_use]
    pub fn enrich_payload_at(payload: Bytes, now_ms: u128) -> Bytes {
        let raw = payload.as_ref();
        let Some(insert_pos) = raw.iter().rposition(|&b| b == b'}') else {
            return payload;
        };

        let mut buf = Vec::with_capacity(raw.len() + 40);
        buf.extend_from_slice(&raw[..insert_pos]);

        // Add comma if there's content before the closing brace (not empty object)
        if let Some(pos) = raw[..insert_pos]
            .iter()
            .rposition(|b| !b.is_ascii_whitespace())
            && raw[pos] != b'{'
        {
            buf.push(b',');
        }
        buf.extend_from_slice(format!("\"_timestamp_receiver\":{now_ms}").as_bytes());
        buf.extend_from_slice(&raw[insert_pos..]);
        Bytes::from(buf)
    }

    /// Inner processing logic (after backpressure check).
    #[inline]
    async fn process_inner(&self, payload: Bytes) -> Result<()> {
        trace!(bytes = payload.len(), "Processing message");

        // Validate (read guard dropped before any .await)
        let validation = self.validator.read().validate(&payload);
        match validation {
            ValidationResult::Valid => {
                trace!(bytes = payload.len(), "Message validation passed");
            }
            ValidationResult::Dlq(reason) => {
                debug!(reason = %reason, bytes = payload.len(), "Message validation failed, routing to DLQ");
                security::input_validation_failure("json_validate", &reason, None);
                return self.send_to_dlq(&payload, &reason).await;
            }
            ValidationResult::Reject(reason) => {
                debug!(reason = %reason, bytes = payload.len(), "Message rejected by validator");
                security::input_validation_failure("json_validate", &reason, None);
                return Err(Error::Validation(reason));
            }
        }

        // Enrich (only when common header / enrichment enabled)
        let payload = if self.enrichment_enabled() {
            let enriched = Self::enrich_payload(payload);
            trace!(
                bytes = enriched.len(),
                "Enriched payload with receiver timestamp"
            );
            enriched
        } else {
            payload
        };

        // Route (read guard dropped before any .await)
        let (route, routed_source) = self.router.read().route_with_source(&payload);

        // The topic carries the source on the Kafka route and nothing carries it
        // on the loader route, so the source is written into the record --
        // dfe-loader reads `_source` out of the data to pick the table. An
        // unmatched record carries the catch-all source, not nothing.
        let payload = match routed_source {
            Some(source) => routing::stamp_source(payload, &source),
            None => payload,
        };

        let dispatch_start = std::time::Instant::now();
        match route {
            RouteResult::Send {
                ref destinations,
                ref topic,
            } => {
                self.send_to_destinations(destinations, topic.as_deref(), &payload)
                    .await?;
                trace!(
                    destinations = destinations.len(),
                    topic = topic.as_deref().unwrap_or(""),
                    bytes = payload.len(),
                    duration_us = dispatch_start.elapsed().as_micros(),
                    "Dispatched to destinations"
                );
            }
            RouteResult::Dlq(ref topic) => {
                self.send_to_kafka(topic, payload.clone()).await?;
                trace!(
                    topic = %topic,
                    bytes = payload.len(),
                    duration_us = dispatch_start.elapsed().as_micros(),
                    "Dispatched DLQ message to Kafka topic"
                );
            }
        }

        // Debug tap: fire-and-forget write to file sink (errors are logged, not propagated)
        if let Some(ref fsink) = self.file_sink
            && let Err(e) = fsink.send("", payload).await
        {
            warn!(error = %e, "File sink write failed");
        }

        Ok(())
    }

    /// Process a message, sending directly to a specific Kafka topic.
    ///
    /// Skips routing but still applies validation and backpressure.
    /// Used by protocol handlers that handle their own protocol-to-topic mapping.
    ///
    /// On the test-only memory transport (`loader.transport: memory`, refused
    /// at startup) there is no broker and a valid record is accepted and
    /// dropped, as the routed path does.
    #[inline]
    pub async fn process_to_topic(&self, payload: Bytes, topic: &str) -> Result<()> {
        // Check for backpressure
        if self.should_apply_backpressure() {
            if scalo::logger::log_state_change(&PRESSURE_LOGGED, true) {
                warn!("Memory pressure HIGH -- backpressure active");
            }
            return Err(Error::Buffer("server under memory pressure".into()));
        }
        // Log recovery when pressure drops
        if scalo::logger::log_state_change(&PRESSURE_LOGGED, false) {
            info!("Memory pressure recovered");
        }

        // Tracked for the life of this future.
        let _lease = MemoryLease::acquire(&self.memory_guard, payload.len() as u64);

        // Validate (acquire and release lock before any await)
        let validation = self.validator.read().validate(&payload);
        match validation {
            ValidationResult::Valid if self.kafka_sink.is_none() && self.discards() => Ok(()),
            ValidationResult::Valid => self.send_to_kafka(topic, payload).await,
            ValidationResult::Dlq(reason) => {
                debug!(reason = %reason, "Message validation failed, routing to DLQ");
                security::input_validation_failure("json_validate", &reason, None);
                self.send_to_dlq(&payload, &reason).await
            }
            ValidationResult::Reject(reason) => {
                security::input_validation_failure("json_validate", &reason, None);
                Err(Error::Validation(reason))
            }
        }
    }

    /// Send message to Kafka.
    #[inline]
    async fn send_to_kafka(&self, topic: &str, payload: Bytes) -> Result<()> {
        let Some(ref sink) = self.kafka_sink else {
            return Err(Error::Config("Kafka sink not configured".into()));
        };

        sink.send(topic, payload).await
    }

    /// Whether the destination set holds the memory transport's discard sink.
    fn discards(&self) -> bool {
        self.destinations
            .values()
            .any(|sink| matches!(sink, DestinationSink::Discard))
    }

    /// Deliver one record to every named destination the route chose.
    ///
    /// The first failure propagates, and the ingest handler turns it into
    /// backpressure on the sender, which re-sends the record. A destination
    /// that already accepted sees it twice -- at-least-once, duplicates never
    /// loss.
    ///
    /// On the bus, accepted means librdkafka queued the record, not that a
    /// broker holds it: the verdict arrives later on a delivery report, and a
    /// record refused then is lost with the sender already answered. That
    /// failure shows up as `receiver_kafka_delivery_failures_total` and an
    /// unhealthy sink, never as an error on the request.
    #[inline]
    async fn send_to_destinations(
        &self,
        destinations: &[Arc<str>],
        topic: Option<&str>,
        payload: &Bytes,
    ) -> Result<()> {
        for name in destinations {
            let Some(sink) = self.destinations.get(name.as_ref()) else {
                return Err(Error::Config(format!(
                    "destination '{name}' is not configured"
                )));
            };
            // Bytes clone is a refcount bump, not a payload copy.
            match sink {
                DestinationSink::Bus { topic: fixed } => {
                    let topic = fixed.as_deref().or(topic).ok_or_else(|| {
                        Error::Config(format!("destination '{name}' resolved no topic"))
                    })?;
                    self.send_to_kafka(topic, payload.clone()).await?;
                }
                // A gRPC listener takes the record itself, so it is sent with
                // no wire key -- the source travels inside the record.
                DestinationSink::Grpc(sink) => sink.send("", payload.clone()).await?,
                DestinationSink::Discard => {}
            }
        }
        Ok(())
    }

    /// Send message to DLQ via unified scalo module (cascade: Kafka -> file).
    #[inline]
    async fn send_to_dlq(&self, payload: &Bytes, reason: &str) -> Result<()> {
        if let Some(ref dlq) = self.dlq {
            let entry = DlqEntry::new("receiver", reason, payload.to_vec());
            dlq.send(entry)
                .await
                .map_err(|e| Error::Config(format!("DLQ send failed: {e}")))?;
            Ok(())
        } else {
            // With no DLQ configured the record follows the routing table, so a
            // brokerless deployment does not need a DLQ topic to reject a bad
            // record.
            let dlq_route = { self.router.read().route_dlq(reason) };
            match dlq_route {
                RouteResult::Dlq(topic) => self.send_to_kafka(&topic, payload.clone()).await,
                RouteResult::Send {
                    ref destinations,
                    ref topic,
                } => {
                    self.send_to_destinations(destinations, topic.as_deref(), payload)
                        .await
                }
            }
        }
    }

    /// Get memory guard for external access.
    pub fn memory_guard(&self) -> &Arc<MemoryGuard> {
        &self.memory_guard
    }

    /// Snapshot pipeline state into metrics gauges.
    ///
    /// Called every second by the orchestrator. Samples buffer manager and
    /// tiered sink stats without touching the hot path.
    pub async fn update_metrics(&self, metrics: &Metrics) {
        // Memory usage
        metrics.set_memory_usage(
            self.memory_guard.current_bytes(),
            self.memory_guard.limit_bytes(),
        );

        // Kafka sink stats
        let mut total_queue = 0u64;
        if let Some(ref kafka) = self.kafka_sink {
            let stats = kafka.stats().await;
            total_queue += stats.queue_size as u64;
            metrics.set_circuit_state(stats.circuit_state, stats.consecutive_failures);
        }

        // Named destination stats
        for sink in self.destinations.values() {
            let queue = match sink {
                DestinationSink::Bus { .. } | DestinationSink::Discard => 0,
                DestinationSink::Grpc(s) => s.stats().await.queue_size,
            };
            total_queue += queue as u64;
        }

        metrics.set_batch_queue_size(total_queue);

        // EPS gauge -- events per second from the rate window
        metrics::gauge!("receiver_events_per_second").set(metrics.request_rate());

        // Sync all metrics into the scaling pressure engine. This is the SHARED
        // scalo `ScalingPressure` the runtime serves at `/scaling/pressure` to
        // KEDA: `update_scaling` feeds the per-pod, locally-knowable signals --
        // connections (the inbound concurrency proxy), queue_depth (the summed
        // sink producer queues = the outbound term, set above via
        // `set_batch_queue_size`), request_rate, memory, spill, and the
        // outbound circuit gate. The receiver is a push originator (inbound
        // HTTP/gRPC/syslog) so there is no inbound Kafka lag term. KEDA can also
        // scale on the gratis bare ingress metrics directly.
        metrics.update_scaling();
    }

    /// Reload configuration, rebuilding router and validator.
    ///
    /// Called on SIGHUP or periodic reload. Sinks are not rebuilt
    /// (Kafka/loader connections are long-lived and should not be disrupted).
    pub fn reload_config(&self, new_config: Config) -> Result<()> {
        self.rebuild_components(&new_config);

        // Update shared config (bumps version, notifies subscribers)
        self.shared_config.update(new_config);
        security::config_changed(
            "config_reload",
            "system",
            "pipeline config reloaded (router + validator)",
        );

        let version = self.shared_config.version();
        info!(version, "Configuration reloaded successfully");
        Ok(())
    }

    /// Rebuild mutable pipeline components from new configuration.
    ///
    /// Called by the config change subscriber when `SharedConfig` is updated
    /// externally (e.g., by `ConfigReloader`). Does NOT update `SharedConfig`
    /// itself -- that's already been done by the caller.
    ///
    /// Routing rules rebuild in place, so a rule pointed at a different
    /// destination takes effect live. The destination SINKS do not: they are
    /// live connections, and every app chart rolls the pod on a config change,
    /// so a new or re-addressed destination arrives with the new pod.
    pub fn rebuild_components(&self, new_config: &Config) {
        let new_router = Router::new(
            &new_config.routing,
            &new_config.resolved_destinations(),
            new_config.server.auth.include_common_header,
        );
        let new_validator = Validator::new(new_config.validation.clone());

        *self.router.write() = new_router;
        *self.validator.write() = new_validator;
    }
}

/// Carry the receiver's spillover settings onto scalo's `TieredSink`.
fn spillover_config(
    spillover: &crate::config::SpilloverConfig,
) -> scalo::tiered_sink::TieredSinkConfig {
    let mut config = scalo::tiered_sink::TieredSinkConfig::new(&spillover.path);
    config.disk_aware = Some(scalo::tiered_sink::DiskAwareConfig {
        max_usage_percent: spillover.max_usage_percent,
        poll_interval_secs: spillover.poll_interval_secs,
    });
    config
}

/// Build the appropriate `SinkBackend` based on spillover configuration.
///
/// When `spillover.enabled` is true, wraps the primary sink in scalo's `TieredSink`
/// with disk spillover. Otherwise, uses the default in-memory buffer. Either way
/// a record the primary refuses for good goes to `rejects`, never the buffer.
async fn build_sink_backend<S: crate::sink::Sink + 'static>(
    primary: S,
    buffer_config: &crate::config::BufferConfig,
    rejects: Rejects,
) -> Result<SinkBackend<S>> {
    if buffer_config.spillover.enabled {
        let adapter = crate::buffer::adapter::ScaloSinkAdapter::new(Arc::new(primary), rejects);
        let spillover = &buffer_config.spillover;

        let tiered = scalo::tiered_sink::TieredSink::new(adapter, spillover_config(spillover))
            .await
            .map_err(|e| Error::Config(format!("failed to create tiered sink: {e}")))?;

        info!(
            path = %spillover.path.display(),
            max_usage_percent = spillover.max_usage_percent,
            "Disk spillover enabled"
        );

        Ok(SinkBackend::Tiered(tiered))
    } else {
        Ok(SinkBackend::InMemory(
            InMemoryBuffer::new(primary, buffer_config).with_rejects(rejects),
        ))
    }
}

/// Main pipeline orchestrator.
pub struct Orchestrator {
    state: Arc<PipelineState>,
    shared_config: SharedConfig,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
}

impl Orchestrator {
    /// Create a new orchestrator with self-regulation disabled (tests).
    pub async fn new(
        config: Config,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        Self::with_governor(config, metrics, shutdown, None, None).await
    }

    /// Create a new orchestrator, optionally wired to the runtime governor.
    ///
    /// `governor` + `runtime_memory_guard` come from the `ServiceRuntime`: when
    /// self-regulation is enabled, the ingest brake (HTTP 503 / gRPC
    /// `UNAVAILABLE`) is driven by the governor's `UnifiedPressure` latch and
    /// byte tracking lands on the guard that latch watches. The horizontal
    /// scaling pressure KEDA reads is driven separately, via the shared
    /// `ScalingPressure` engine the metrics loop feeds in `update_metrics`.
    pub async fn with_governor(
        config: Config,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
        governor: Option<&scalo::SelfRegulationGovernor>,
        runtime_memory_guard: Option<Arc<MemoryGuard>>,
    ) -> Result<Self> {
        let shared_config = SharedConfig::new(config);
        let state = PipelineState::build(
            shared_config.clone(),
            shutdown.clone(),
            governor,
            runtime_memory_guard,
            Some(Arc::clone(&metrics)),
        )
        .await?;

        Ok(Self {
            state: Arc::new(state),
            shared_config,
            metrics,
            shutdown,
        })
    }

    /// Get shared pipeline state.
    pub fn state(&self) -> Arc<PipelineState> {
        Arc::clone(&self.state)
    }

    /// Get shared config handle.
    pub fn shared_config(&self) -> SharedConfig {
        self.shared_config.clone()
    }

    /// Start the background tasks.
    ///
    /// Call before the listeners start accepting. The default `InMemoryBuffer`
    /// only moves queued records when its drain task drains it, and the KEDA
    /// scaling gauge only updates from the metrics feed, so both have to run
    /// for the whole serving window.
    pub fn start(&self) -> Result<()> {
        info!("Pipeline orchestrator running");

        // Start drain tasks for tiered sinks
        if let Some(ref kafka) = self.state.kafka_sink {
            kafka.clone().start_drain_task(self.shutdown.clone());
        }
        for sink in self.state.destinations.values() {
            match sink {
                DestinationSink::Bus { .. } | DestinationSink::Discard => {}
                DestinationSink::Grpc(s) => s.clone().start_drain_task(self.shutdown.clone()),
            }
        }

        // Periodic metrics update (1s interval)
        let metrics_state = Arc::clone(&self.state);
        let metrics_ref = Arc::clone(&self.metrics);
        let metrics_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        metrics_state.update_metrics(&metrics_ref).await;
                    }
                    _ = metrics_shutdown.cancelled() => break,
                }
            }
        });

        Ok(())
    }

    /// Flush every sink once the listeners have stopped.
    pub async fn shutdown(&self) -> Result<()> {
        info!("Pipeline orchestrator shutting down");

        // Flush all sinks
        if let Some(ref kafka) = self.state.kafka_sink
            && let Err(e) = kafka.flush().await
        {
            error!(error = %e, "Failed to flush Kafka sink");
        }

        for (name, sink) in &self.state.destinations {
            let flushed = match sink {
                DestinationSink::Bus { .. } | DestinationSink::Discard => Ok(()),
                DestinationSink::Grpc(s) => s.flush().await,
            };
            if let Err(e) = flushed {
                error!(error = %e, destination = %name, "Failed to flush destination sink");
            }
        }

        if let Some(ref fsink) = self.state.file_sink
            && let Err(e) = fsink.flush().await
        {
            error!(error = %e, "Failed to flush file sink");
        }

        info!("Pipeline orchestrator stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        let mut config = Config::default();
        // Use loader destination to avoid needing Kafka brokers
        config.destinations.default = "loader".into();
        config.loader.transport = "memory".to_string();
        config
    }

    async fn test_state() -> PipelineState {
        let config = test_config();
        PipelineState::new(SharedConfig::new(config), CancellationToken::new())
            .await
            .unwrap()
    }

    async fn test_state_with(config: Config) -> PipelineState {
        PipelineState::new(SharedConfig::new(config), CancellationToken::new())
            .await
            .unwrap()
    }

    /// A pipeline whose guard reads its own reservations, so a synthetic byte
    /// budget means something in a process whose real usage dwarfs it.
    async fn test_state_on_reservations(config: Config) -> PipelineState {
        let guard = MemoryGuard::with_usage_source(
            MemoryGuardConfig {
                limit_bytes: config.buffer.memory_limit as u64,
                pressure_threshold: config.buffer.pressure_threshold,
                ..Default::default()
            },
            scalo::memory::UsageSource::Reservations,
        );
        PipelineState::with_governor(
            SharedConfig::new(config),
            CancellationToken::new(),
            None,
            Some(Arc::new(guard)),
        )
        .await
        .unwrap()
    }

    /// The spillover thresholds an operator sets reach scalo's TieredSink.
    #[test]
    fn spillover_settings_reach_the_tiered_sink() {
        let spillover = crate::config::SpilloverConfig {
            enabled: true,
            path: std::path::PathBuf::from("/var/spool/somewhere-else"),
            max_usage_percent: 0.55,
            poll_interval_secs: 17,
        };

        let built = spillover_config(&spillover);

        assert_eq!(built.spool_path, spillover.path);
        let disk = built.disk_aware.unwrap();
        assert!((disk.max_usage_percent - 0.55).abs() < f64::EPSILON);
        assert_eq!(disk.poll_interval_secs, 17);
    }

    /// `loader.transport: kafka` is the bus with no fixed topic, so a record
    /// lands on the topic its source resolves to.
    #[test]
    fn the_loader_on_the_bus_takes_the_record_topic() {
        let mut config = Config::default();
        config.destinations.default = "loader".into();
        config.loader.transport = "kafka".to_string();

        let resolved = config.resolved_destinations();

        assert!(
            resolved.uses_bus(),
            "loader over kafka is a bus destination"
        );
        let spec = resolved.named.get("loader").unwrap();
        assert!(spec.grpc.is_none());
        assert_eq!(spec.kafka.as_ref().unwrap().topic, None);
    }

    /// `loader.transport: grpc` resolves to the loader's own endpoint.
    #[test]
    fn the_loader_over_grpc_resolves_to_its_endpoint() {
        let mut config = Config::default();
        config.destinations.default = "loader".into();
        config.loader.transport = "grpc".to_string();

        let resolved = config.resolved_destinations();

        assert!(!resolved.uses_bus());
        let endpoint = &resolved
            .named
            .get("loader")
            .unwrap()
            .grpc
            .as_ref()
            .unwrap()
            .endpoint;
        assert_eq!(endpoint, &config.loader.effective_grpc_endpoint());
    }

    /// A declared `loader` destination wins over the built-in one.
    #[test]
    fn a_declared_loader_destination_is_left_alone() {
        let mut config = Config::default();
        config.destinations.default = "loader".into();
        config.loader.transport = "kafka".to_string();
        config.destinations.named.insert(
            "loader".to_string(),
            crate::config::DestinationSpec {
                grpc: Some(crate::config::GrpcDestination {
                    endpoint: "http://elsewhere:6000".to_string(),
                }),
                kafka: None,
            },
        );

        let resolved = config.resolved_destinations();

        let endpoint = &resolved
            .named
            .get("loader")
            .unwrap()
            .grpc
            .as_ref()
            .unwrap()
            .endpoint;
        assert_eq!(endpoint, "http://elsewhere:6000");
    }

    #[tokio::test]
    async fn test_pipeline_validation_reject() {
        let state = test_state().await;

        // Invalid JSON with dlq_on_invalid=true attempts DLQ routing, but no Kafka/DLQ
        // sink is configured in test state -- expect config error from the fallback path
        let result = state.process(Bytes::from("not json")).await;
        assert!(
            result.is_err(),
            "expected error without DLQ sink: {result:?}"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Kafka sink not configured"),
            "expected Kafka sink error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_pipeline_valid_json() {
        let state = test_state().await;

        let valid_json = Bytes::from(r#"{"test": "data"}"#);
        let result = state.process(valid_json).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_ready_check() {
        let state = test_state().await;
        assert!(state.is_ready());
    }

    #[tokio::test]
    async fn pressure_fails_both_the_probe_and_admission() {
        // Sink health is deliberately absent from probe_ready, which this does
        // NOT cover: the test state's memory loader is always healthy and the
        // sinks are concrete types with no seam to fail one. The divergence is
        // held by construction and by the callers in main.rs and server/.
        let mut config = test_config();
        config.buffer.memory_limit = 1000;
        config.buffer.pressure_threshold = 0.8;
        let state = test_state_on_reservations(config).await;
        state.watch_listeners(Vec::new());

        assert!(state.probe_ready(), "a fresh pipeline must pass the probe");
        assert!(state.is_ready(), "and must admit requests");

        state.memory_guard().add_bytes(900);

        assert!(
            !state.probe_ready(),
            "pressure must fail the probe -- a scale-out relieves it"
        );
        assert!(!state.is_ready(), "and must also stop admitting");
    }

    #[tokio::test]
    async fn the_probe_waits_for_every_declared_listener() {
        let state = test_state().await;
        assert!(
            !state.probe_ready(),
            "no probe may pass before the server declares its listeners"
        );

        let http = BoundAddr::default();
        let syslog = BoundAddr::default();
        state.watch_listeners(vec![http.clone(), syslog.clone()]);
        assert!(!state.probe_ready(), "neither listener has bound");

        let addr = Ok(std::net::SocketAddr::from(([127, 0, 0, 1], 9)));
        let _http_serving = http.publish(&addr);
        assert!(!state.probe_ready(), "syslog has not bound");
        assert!(state.is_ready(), "admission does not wait on listeners");

        let syslog_serving = syslog.publish(&addr);
        assert!(state.probe_ready(), "every declared listener is serving");

        drop(syslog_serving);
        assert!(
            !state.probe_ready(),
            "a listener that stopped must fail the probe"
        );
    }

    #[tokio::test]
    async fn test_pipeline_memory_pressure() {
        let mut config = test_config();
        config.buffer.memory_limit = 1000;
        config.buffer.pressure_threshold = 0.8;

        let state = test_state_on_reservations(config).await;

        // Should start without pressure
        assert!(!state.should_apply_backpressure());
        assert_eq!(state.memory_pressure(), MemoryPressure::Low);
    }

    #[tokio::test]
    async fn memory_lease_releases_on_every_drop_path() {
        // The lease is dropped however the future ends, so its Drop contract is
        // what covers the cancellation path.
        let state = test_state().await;
        let guard = state.memory_guard().clone();
        // The lease balance is `reserved_bytes`; `current_bytes` is what the
        // kernel charges the process and moves on its own.
        let before = guard.reserved_bytes();

        {
            let _lease = MemoryLease::acquire(&guard, 4096);
            assert_eq!(
                guard.reserved_bytes(),
                before + 4096,
                "acquire must track the bytes"
            );
        }

        assert_eq!(
            guard.reserved_bytes(),
            before,
            "drop must return the tracked bytes to baseline"
        );
    }

    /// A lease held across an await must release when the future is dropped.
    ///
    /// The scope-exit test above never suspends, so it cannot fail the way a
    /// client disconnect does. `pending` guarantees the timeout drops the
    /// future while the lease is still held.
    #[tokio::test]
    async fn memory_lease_releases_when_its_future_is_cancelled() {
        let state = test_state().await;
        let guard = state.memory_guard().clone();
        let before = guard.reserved_bytes();
        let held = Arc::clone(&guard);

        let outcome = tokio::time::timeout(Duration::from_millis(50), async move {
            let _lease = MemoryLease::acquire(&held, 4096);
            assert_eq!(
                held.reserved_bytes(),
                before + 4096,
                "the lease must be tracked before the future suspends"
            );
            std::future::pending::<()>().await;
        })
        .await;

        assert!(
            outcome.is_err(),
            "the future must be dropped while suspended"
        );
        assert_eq!(
            guard.reserved_bytes(),
            before,
            "cancelling a suspended future must release its tracked bytes"
        );
    }

    #[tokio::test]
    async fn process_returns_tracked_bytes_to_baseline() {
        let state = test_state().await;
        let before = state.memory_guard().reserved_bytes();

        // Positive control: under backpressure `process` returns before it
        // acquires anything, and the balance assertion below would pass vacuously.
        assert!(
            !state.should_apply_backpressure(),
            "guard must be idle for this to exercise the accounting"
        );

        let _ = state.process(Bytes::from(vec![b'x'; 4096])).await;

        assert_eq!(state.memory_guard().reserved_bytes(), before);
    }

    /// Cancelling `process` itself must return its bytes, not just the lease type.
    ///
    /// The two lease tests above build a `MemoryLease` by hand, so every one of
    /// them stays green if the lease is deleted from `process`; the balance test
    /// above stays green too, because a charge that never happens also balances.
    /// This is the only test that reddens on a revert of the fix.
    ///
    /// A silent listener -- accepts the TCP connection, then never speaks HTTP/2
    /// -- suspends the gRPC send, which is where a real request is dropped when
    /// the client disconnects or the 30s request timeout fires. `Box::pin` owns
    /// the future so `drop` actually drops it; `pin!` would only drop a borrow
    /// and prove nothing.
    #[tokio::test]
    async fn cancelling_process_releases_its_tracked_bytes() {
        use std::future::Future;
        use std::task::{Context, Waker};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind silent listener");
        let addr = listener.local_addr().expect("listener addr");
        let accepted = tokio::spawn(async move {
            // Hold every stream open: dropping one would fail the send fast
            // instead of leaving it suspended.
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });

        let mut config = test_config();
        config.loader.transport = "grpc".to_string();
        config.loader.grpc_endpoint = Some(format!("http://{addr}"));
        let state = test_state_with(config).await;

        let payload = Bytes::from(r#"{"cancelled":true}"#);
        let bytes = payload.len() as u64;
        let before = state.memory_guard().reserved_bytes();

        let mut inflight = Box::pin(state.process(payload));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            inflight.as_mut().poll(&mut cx).is_pending(),
            "process must still be in the sink for this to test cancellation"
        );
        assert_eq!(
            state.memory_guard().reserved_bytes(),
            before + bytes,
            "process must charge the guard before it suspends"
        );

        drop(inflight);

        assert_eq!(
            state.memory_guard().reserved_bytes(),
            before,
            "dropping the suspended request must return its tracked bytes"
        );

        accepted.abort();
    }

    #[tokio::test]
    async fn test_enrich_payload_injects_timestamp() {
        let payload = Bytes::from(r#"{"key": "value"}"#);
        let enriched = PipelineState::enrich_payload(payload);
        let enriched_str = std::str::from_utf8(&enriched).unwrap();

        assert!(enriched_str.contains("\"_timestamp_receiver\":"));
        // Verify it's still valid JSON
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert!(parsed.get("_timestamp_receiver").is_some());
        assert_eq!(parsed.get("key").unwrap(), "value");
    }

    #[tokio::test]
    async fn test_enrich_payload_empty_object() {
        let payload = Bytes::from(r"{}");
        let enriched = PipelineState::enrich_payload(payload);

        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert!(parsed.get("_timestamp_receiver").is_some());
    }

    #[tokio::test]
    async fn test_enrichment_disabled() {
        let mut config = test_config();
        config.server.auth.include_common_header = false;
        let state = test_state_with(config).await;

        assert!(!state.enrichment_enabled());
    }

    #[tokio::test]
    async fn test_enrichment_enabled_by_default() {
        let state = test_state().await;
        assert!(state.enrichment_enabled());
    }

    #[tokio::test]
    async fn test_reload_config_updates_version() {
        let state = test_state().await;
        assert_eq!(state.shared_config().version(), 0);

        let mut new_config = test_config();
        new_config.routing.default_source = "reloaded".to_string();
        state.reload_config(new_config).unwrap();

        assert_eq!(state.shared_config().version(), 1);
        assert_eq!(state.config().routing.default_source, "reloaded");
    }

    #[tokio::test]
    async fn test_reload_config_toggles_enrichment() {
        let state = test_state().await;
        assert!(state.enrichment_enabled());

        // Disable enrichment via reload
        let mut new_config = test_config();
        new_config.server.auth.include_common_header = false;
        state.reload_config(new_config).unwrap();

        assert!(!state.enrichment_enabled());

        // Re-enable via another reload
        let mut re_enable = test_config();
        re_enable.server.auth.include_common_header = true;
        state.reload_config(re_enable).unwrap();

        assert!(state.enrichment_enabled());
    }

    #[tokio::test]
    async fn test_reload_config_while_processing() {
        let state = Arc::new(test_state().await);

        // Process a message before reload
        let result = state.process(Bytes::from(r#"{"a": 1}"#)).await;
        assert!(result.is_ok());

        // Reload config with different default source
        let mut new_config = test_config();
        new_config.routing.default_source = "updated".to_string();
        state.reload_config(new_config).unwrap();

        // Process a message after reload -- should still work
        let result = state.process(Bytes::from(r#"{"b": 2}"#)).await;
        assert!(result.is_ok());

        // Verify config actually changed
        assert_eq!(state.config().routing.default_source, "updated");
    }

    #[tokio::test]
    async fn test_reload_config_subscriber_notified() {
        let state = test_state().await;
        let mut rx = state.shared_config().subscribe();

        let new_config = test_config();
        state.reload_config(new_config).unwrap();

        rx.changed().await.expect("should receive notification");
        assert_eq!(*rx.borrow(), 1);
    }

    // ---------------------------------------------------------------------
    // Additional edge-case tests (non-trivial paths)
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_pipeline_batch_processes_all() {
        // Batch processing: all-valid payloads should all succeed
        let state = test_state().await;
        let payloads = vec![
            Bytes::from(r#"{"id":1}"#),
            Bytes::from(r#"{"id":2}"#),
            Bytes::from(r#"{"id":3}"#),
        ];
        let (success_count, first_err) = state.process_batch(&payloads).await;
        assert_eq!(success_count, 3, "all 3 items should succeed");
        assert!(first_err.is_none(), "no errors expected");
    }

    #[tokio::test]
    async fn test_pipeline_batch_with_invalid_items() {
        // Batch with mixed valid/invalid -- valid ones should still dispatch.
        // process_batch returns (success_count, first_error). Invalid JSON
        // with no DLQ sink triggers an error.
        let state = test_state().await;
        let payloads = vec![
            Bytes::from(r#"{"ok":1}"#),
            Bytes::from("not json"),
            Bytes::from(r#"{"ok":2}"#),
        ];
        let (success_count, first_err) = state.process_batch(&payloads).await;
        // Valid items succeed; invalid item errors out
        assert_eq!(success_count, 2, "2 valid items should succeed");
        assert!(first_err.is_some(), "invalid item must produce error");
    }

    #[tokio::test]
    async fn test_pipeline_large_payload() {
        // Large valid JSON should process without issue
        let state = test_state().await;
        let mut large = String::from(r#"{"data":""#);
        large.push_str(&"x".repeat(100_000));
        large.push_str(r#""}"#);
        let result = state.process(Bytes::from(large)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_empty_object() {
        let state = test_state().await;
        let result = state.process(Bytes::from(r"{}")).await;
        assert!(result.is_ok(), "empty JSON object is valid");
    }

    #[tokio::test]
    async fn test_pipeline_nested_json() {
        let state = test_state().await;
        let nested =
            Bytes::from(r#"{"a":{"b":{"c":{"d":{"e":{"f":"deep"}}}}},"arr":[1,2,3,[4,[5]]]}"#);
        let result = state.process(nested).await;
        assert!(result.is_ok(), "deeply nested JSON should succeed");
    }

    #[tokio::test]
    async fn test_pipeline_unicode_payload() {
        let state = test_state().await;
        let unicode = Bytes::from(r#"{"msg":"日本語🎉émojí","emoji":"🔥"}"#);
        let result = state.process(unicode).await;
        assert!(result.is_ok(), "unicode should process correctly");
    }

    #[tokio::test]
    async fn test_enrich_payload_preserves_existing_timestamp_field() {
        // If the payload already has a _timestamp_receiver field, enrichment
        // injects a new one (implementation currently prepends) -- verify
        // the result is still valid JSON
        let payload = Bytes::from(r#"{"_timestamp_receiver":"old","key":"val"}"#);
        let enriched = PipelineState::enrich_payload(payload);
        // Must still parse as JSON
        let parsed = serde_json::from_slice::<serde_json::Value>(&enriched);
        assert!(parsed.is_ok(), "enriched JSON must remain parseable");
    }

    #[tokio::test]
    async fn test_enrich_payload_array_root_not_supported() {
        // Top-level arrays are not valid for enrichment (expects object).
        // Verify it doesn't panic -- either passes through or returns as-is.
        let payload = Bytes::from("[1,2,3]");
        let enriched = PipelineState::enrich_payload(payload.clone());
        // Should not panic; result is implementation-defined for non-object roots
        assert!(!enriched.is_empty());
    }

    #[tokio::test]
    async fn test_pipeline_memory_pressure_tracking() {
        // Memory pressure starts low and can be queried
        let state = test_state().await;
        let initial = state.memory_pressure();
        assert!(matches!(
            initial,
            MemoryPressure::Low | MemoryPressure::Medium
        ));

        // should_apply_backpressure returns false in low pressure
        assert!(!state.should_apply_backpressure());
    }

    #[tokio::test]
    async fn test_pipeline_reload_preserves_ready_state() {
        // Config reload must not leave the pipeline in an unusable state
        let state = test_state().await;
        assert!(state.is_ready());

        let new_config = test_config();
        state.reload_config(new_config).unwrap();

        // Pipeline should remain ready after reload
        assert!(state.is_ready());

        // And continue to process payloads
        let result = state.process(Bytes::from(r#"{"ok":true}"#)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_multiple_reloads() {
        // Repeated reloads should not leak resources or stuck state
        let state = test_state().await;
        for i in 0..10 {
            let mut cfg = test_config();
            cfg.routing.default_source = format!("src-{i}");
            state.reload_config(cfg).unwrap();
        }
        // After many reloads, pipeline must still work
        let result = state.process(Bytes::from(r#"{"final":true}"#)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_malformed_utf8_rejected() {
        // Invalid UTF-8 bytes must not cause a panic
        let state = test_state().await;
        let bad = Bytes::from(vec![0xff, 0xfe, 0xfd, 0xfc, 0x00, 0x01]);
        let result = state.process(bad).await;
        // Must return an error, not panic
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pipeline_truncated_json() {
        // Truncated JSON (common in network corruption) should be rejected cleanly
        let state = test_state().await;
        let truncated = Bytes::from(r#"{"key":"val"#);
        let result = state.process(truncated).await;
        assert!(result.is_err());
    }
}
