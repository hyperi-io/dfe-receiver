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
//!
//! A listener that holds its answer ([`acks`]) sends straight to each
//! destination's sink and answers from what the destinations confirmed. One
//! that answers at enqueue sends through the buffer in front of the sink.

pub mod acks;

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};
use scalo::UnifiedPressure;
use scalo::dlq::{Dlq, DlqEntry};
use scalo::logger::security;
use scalo::transport::AcknowledgementsConfig;
use scalo::transport::DeliveryStatus;
use scalo::transport::ack::Ticket;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

pub use acks::Acks;

use crate::buffer::{
    InMemoryBuffer, InMemoryBufferStats, MemoryGuard, MemoryGuardConfig, MemoryPressure, Rejects,
    SinkBackend,
};
use crate::config::{BufferConfig, Config, SharedConfig};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::routing::{self, RouteResult, Router};
use crate::server::traits::BoundAddr;
use crate::sink::Sink;
use crate::sink::file::FileSink;
use crate::sink::grpc::GrpcSink;
use crate::sink::kafka::KafkaSink;
use crate::validation::{ValidationResult, Validator};

/// State-change flag for memory pressure log deduplication.
static PRESSURE_LOGGED: AtomicBool = AtomicBool::new(false);

/// The bus's spool directory under `buffer.spillover.path`.
const BUS_SPOOL: &str = "kafka";

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

/// What the pipeline made of a batch of records, in the terms a listener
/// needs to answer its sender honestly.
///
/// A batch stops at the first failure the sender should retry: the sender is
/// told to resend the whole request, so taking the records behind it would
/// only add duplicates. A record refused for good does not stop it.
#[derive(Debug, Default)]
#[must_use]
pub struct BatchOutcome {
    /// Records the pipeline took.
    pub accepted: usize,
    /// Records refused for good: the record itself is at fault.
    pub rejected: usize,
    /// The first refusal, for the answer's detail.
    pub first_rejection: Option<Error>,
    /// The failure that stopped the batch. The record it names and every one
    /// after it were not taken.
    pub unavailable: Option<Error>,
}

impl BatchOutcome {
    /// Count one record's result. `Break` once a retryable failure stops the
    /// batch.
    pub fn record(&mut self, result: Result<()>) -> ControlFlow<()> {
        match result {
            Ok(()) => self.accepted += 1,
            Err(e) if e.is_retryable() => {
                self.unavailable = Some(e);
                return ControlFlow::Break(());
            }
            Err(e) => {
                self.rejected += 1;
                self.first_rejection.get_or_insert(e);
            }
        }
        ControlFlow::Continue(())
    }

    /// Records the pipeline settled, taken or refused for good. When the batch
    /// stopped early, the records from this index on were not taken.
    #[must_use]
    pub fn settled(&self) -> usize {
        self.accepted + self.rejected
    }

    /// The one error a single-error answer should report: the retryable one
    /// first, since answering a partial failure as final loses the records
    /// behind it.
    #[must_use]
    pub fn into_error(self) -> Option<Error> {
        self.unavailable.or(self.first_rejection)
    }

    /// Mark the whole batch for a resend: no record in it counts as settled,
    /// so a caller re-offering what was not settled re-offers all of it.
    fn retry_all(&mut self, error: Error) {
        self.accepted = 0;
        self.rejected = 0;
        self.first_rejection = None;
        self.unavailable = Some(error);
    }
}

/// One destination's sink, and the buffer in front of it where some listener
/// answers at enqueue.
struct Delivery<S: Sink + 'static> {
    /// A held answer sends here directly: nothing buffers or spools a record
    /// whose sender still keeps its copy.
    primary: Arc<S>,
    /// An at-enqueue answer sends here. `None` when every enabled listener
    /// holds its answer, so no buffer or spool is built at all.
    queued: Option<Arc<SinkBackend<S>>>,
}

impl<S: Sink + 'static> Delivery<S> {
    /// The sink, with a buffer in front of it when `queued` and its spool,
    /// with spillover on, at `spool` under the spillover path.
    async fn build(
        primary: S,
        spool: &Path,
        buffer: &BufferConfig,
        rejects: Rejects,
        queued: bool,
    ) -> Result<Self> {
        let primary = Arc::new(primary);
        let queued = if queued {
            Some(Arc::new(
                build_sink_backend(Arc::clone(&primary), spool, buffer, rejects).await?,
            ))
        } else {
            None
        };
        Ok(Self { primary, queued })
    }

    /// The sink an at-enqueue answer sends through.
    fn at_enqueue(&self) -> &dyn Sink {
        match &self.queued {
            Some(queued) => queued.as_ref(),
            None => self.primary.as_ref(),
        }
    }

    fn is_healthy(&self) -> bool {
        self.at_enqueue().is_healthy()
    }

    async fn stats(&self) -> Option<InMemoryBufferStats> {
        match &self.queued {
            Some(queued) => Some(queued.stats().await),
            None => None,
        }
    }

    /// Start the buffer's drain, where there is a buffer.
    fn start(&self, shutdown: CancellationToken) {
        if let Some(queued) = &self.queued {
            Arc::clone(queued).start_drain_task(shutdown);
        }
    }

    async fn flush(&self) -> Result<()> {
        self.at_enqueue().flush().await
    }
}

/// What a named destination resolves to.
///
/// The name is the routing decision; this is the delivery. A destination that
/// stops accepting back-pressures the ingest rather than spilling records to a
/// DLQ a brokerless deployment does not have.
enum DestinationSink {
    /// The bus, using the topic the record's source resolves to, or a topic
    /// fixed by the destination.
    Bus { topic: Option<Arc<str>> },
    /// A scalo Push listener -- a transform, the loader, the archiver.
    Grpc(Delivery<GrpcSink>),
    /// `loader.transport: memory` -- no transport, so the record is accepted
    /// and goes nowhere.
    Discard,
}

/// Where a batch's records go.
#[derive(Clone, Copy)]
enum Target<'a> {
    /// Enriched and routed to the destinations the routing rules pick.
    Routed,
    /// Sent to this Kafka topic, which the listener chose.
    Topic(&'a str),
}

/// One request's sends: the ticket its answer is held on, and the records
/// bound for each gRPC destination, sent together once the request is routed.
struct Dispatch<'t> {
    ticket: Option<&'t Ticket>,
    grpc: Vec<(Arc<str>, Vec<Bytes>)>,
}

impl<'t> Dispatch<'t> {
    fn new(ticket: Option<&'t Ticket>) -> Self {
        Self {
            ticket,
            grpc: Vec::new(),
        }
    }

    fn push_grpc(&mut self, name: &Arc<str>, payload: Bytes) {
        match self.grpc.iter_mut().find(|(n, _)| n == name) {
            Some((_, payloads)) => payloads.push(payload),
            None => self.grpc.push((Arc::clone(name), vec![payload])),
        }
    }
}

/// Shared pipeline state accessible from handlers.
pub struct PipelineState {
    shared_config: SharedConfig,
    validator: RwLock<Validator>,
    router: RwLock<Router>,
    kafka: Option<Delivery<KafkaSink>>,
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
    /// Where a record a destination refuses for good goes.
    rejects: Rejects,
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

        // A buffer, and its spool, only serve a listener that answers at
        // enqueue; a held answer sends past them.
        let at_enqueue = config.answers_at_enqueue();
        let held_message_timeout = config.holds_answers().then_some(acks::HELD_MESSAGE_TIMEOUT);
        info!(
            holds_answers = config.holds_answers(),
            answers_at_enqueue = at_enqueue,
            "Listener acknowledgements resolved"
        );

        let mut spools = FxHashSet::default();
        let kafka = if config.kafka.brokers.is_empty() {
            None
        } else {
            let primary = KafkaSink::new(&config.kafka, held_message_timeout)?;
            let spool = claim_spool(&mut spools, PathBuf::from(BUS_SPOOL))?;
            Some(
                Delivery::build(primary, &spool, &config.buffer, rejects.clone(), at_enqueue)
                    .await?,
            )
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
                        // loader.timeout_ms bounds the loader's own RPC; a
                        // declared destination has no timeout key of its own.
                        let deadline = if name == crate::config::LOADER_DESTINATION {
                            config.loader.timeout_ms
                        } else {
                            acks::NEXT_HOP_DEADLINE_MS
                        };
                        let primary = GrpcSink::new(&grpc.endpoint, Some(deadline)).await?;
                        let spool = claim_spool(&mut spools, grpc_spool(name))?;
                        DestinationSink::Grpc(
                            Delivery::build(
                                primary,
                                &spool,
                                &config.buffer,
                                rejects.clone(),
                                at_enqueue,
                            )
                            .await?,
                        )
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
            kafka,
            destinations,
            file_sink,
            memory_guard,
            pressure,
            dlq,
            rejects,
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

    /// The acknowledgement setting of listener `transport`, holding at most a
    /// quarter of the memory limit unanswered.
    #[must_use]
    pub fn acks(
        &self,
        transport: &'static str,
        config: AcknowledgementsConfig,
        request_timeout: Option<Duration>,
    ) -> Acks {
        Acks::new(
            transport,
            config,
            request_timeout,
            acks::max_held_bytes(self.memory_guard.limit_bytes()),
        )
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
        if let Some(ref kafka) = self.kafka
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

    /// The inbound brake: refuse a request while the pipeline is under
    /// pressure, logging the edge in each direction once.
    fn brake(&self) -> Result<()> {
        if self.should_apply_backpressure() {
            if scalo::logger::log_state_change(&PRESSURE_LOGGED, true) {
                warn!("Memory pressure HIGH -- backpressure active");
            }
            return Err(Error::Buffer("server under memory pressure".into()));
        }
        if scalo::logger::log_state_change(&PRESSURE_LOGGED, false) {
            info!("Memory pressure recovered");
        }
        Ok(())
    }

    /// Process a message through the pipeline, answered at enqueue.
    ///
    /// This is a HOT PATH function.
    #[inline]
    pub async fn process(&self, payload: Bytes) -> Result<()> {
        self.brake()?;

        // Tracked for the life of this future.
        let _lease = MemoryLease::acquire(&self.memory_guard, payload.len() as u64);

        let mut dispatch = Dispatch::new(None);
        self.process_inner(payload, Target::Routed, &mut dispatch)
            .await?;
        self.send_grpc(dispatch).await
    }

    /// Process a batch of messages, answered at enqueue.
    ///
    /// Amortises overhead: single backpressure check, single memory tracking
    /// update, and per-message process_inner() calls. A record refused for
    /// good does not stop the rest; a retryable failure does, see
    /// [`BatchOutcome`].
    pub async fn process_batch(&self, payloads: &[Bytes]) -> BatchOutcome {
        self.run(payloads, Target::Routed, None).await
    }

    /// Process a batch for a listener with the setting `acks`, whose sender
    /// allows `sender_deadline`.
    ///
    /// Holding, the outcome counts every record taken only once every
    /// destination confirmed it; anything short of that is a resend of the
    /// whole batch.
    pub async fn process_batch_acked(
        &self,
        payloads: &[Bytes],
        acks: &Acks,
        sender_deadline: Option<Duration>,
    ) -> BatchOutcome {
        self.run(payloads, Target::Routed, Some((acks, sender_deadline)))
            .await
    }

    /// [`process_batch_acked`](Self::process_batch_acked) to a Kafka topic
    /// the listener chose, with no routing or enrichment. On the test-only
    /// memory transport (`loader.transport: memory`, refused at startup) there
    /// is no broker and a valid record is accepted and dropped.
    pub async fn process_batch_to_topic(
        &self,
        payloads: &[Bytes],
        topic: &str,
        acks: &Acks,
    ) -> BatchOutcome {
        self.run(payloads, Target::Topic(topic), Some((acks, None)))
            .await
    }

    /// Run a batch to `target`, held on a ticket when `hold` admits one.
    async fn run(
        &self,
        payloads: &[Bytes],
        target: Target<'_>,
        hold: Option<(&Acks, Option<Duration>)>,
    ) -> BatchOutcome {
        let mut outcome = BatchOutcome::default();
        if payloads.is_empty() {
            return outcome;
        }
        if let Err(e) = self.brake() {
            outcome.unavailable = Some(e);
            return outcome;
        }

        // One lease for the whole batch, held until its answer.
        let total_bytes: u64 = payloads.iter().map(|p| p.len() as u64).sum();
        let _lease = MemoryLease::acquire(&self.memory_guard, total_bytes);

        let ticket = match hold.and_then(|(acks, deadline)| acks.admit(total_bytes, deadline)) {
            None => None,
            Some(Ok(ticket)) => Some(ticket),
            Some(Err(refused)) => {
                outcome.unavailable = Some(Error::Buffer(format!(
                    "held answers refused a request: {refused}"
                )));
                return outcome;
            }
        };

        let Some(ticket) = ticket else {
            self.dispatch(payloads, target, None, &mut outcome).await;
            return outcome;
        };
        // A slow destination must not hold the answer past the sender's deadline.
        let hold_ends = tokio::time::Instant::from_std(ticket.deadline());
        let dispatched = self.dispatch(payloads, target, Some(&ticket), &mut outcome);
        if tokio::time::timeout_at(hold_ends, dispatched)
            .await
            .is_err()
        {
            // A send cut off here may still land: the resend is a duplicate, never a loss.
            ticket.piece().report(DeliveryStatus::Errored);
            let answered = ticket.outcome().await;
            debug!(
                outcome = answered.as_str(),
                records = payloads.len(),
                "Destinations did not answer within the hold; answering retryable"
            );
            outcome.retry_all(Error::Transport(format!(
                "delivery not confirmed within the hold ({})",
                answered.as_str()
            )));
            return outcome;
        }
        // A held answer is all or nothing: the sender resends the whole batch.
        if let Some(e) = outcome.unavailable.take() {
            outcome.retry_all(e);
            return outcome;
        }
        let answered = ticket.outcome().await;
        if !answered.is_success() {
            debug!(
                outcome = answered.as_str(),
                records = payloads.len(),
                "Destinations did not confirm the batch; answering retryable"
            );
            outcome.retry_all(Error::Transport(format!(
                "delivery not confirmed ({})",
                answered.as_str()
            )));
        }
        outcome
    }

    /// Route and send each record of a batch, then send the gRPC destinations
    /// their share, recording in `outcome` what was taken.
    async fn dispatch(
        &self,
        payloads: &[Bytes],
        target: Target<'_>,
        ticket: Option<&Ticket>,
        outcome: &mut BatchOutcome,
    ) {
        let mut dispatch = Dispatch::new(ticket);
        for payload in payloads {
            let result = self
                .process_inner(payload.clone(), target, &mut dispatch)
                .await;
            if outcome.record(result).is_break() {
                break;
            }
        }
        if let Err(e) = self.send_grpc(dispatch).await {
            outcome.retry_all(e);
        }
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
    async fn process_inner(
        &self,
        payload: Bytes,
        target: Target<'_>,
        dispatch: &mut Dispatch<'_>,
    ) -> Result<()> {
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
                return self.send_to_dlq(&payload, &reason, dispatch).await;
            }
            ValidationResult::Reject(reason) => {
                debug!(reason = %reason, bytes = payload.len(), "Message rejected by validator");
                security::input_validation_failure("json_validate", &reason, None);
                return Err(Error::Validation(reason));
            }
        }

        match target {
            Target::Routed => self.process_routed(payload, dispatch).await,
            // The memory transport has no broker for a topic the listener picked.
            Target::Topic(_) if self.kafka.is_none() && self.discards() => Ok(()),
            Target::Topic(topic) => self.send_to_kafka(topic, &payload, dispatch).await,
        }
    }

    /// Enrich, route and send a validated record.
    async fn process_routed(&self, payload: Bytes, dispatch: &mut Dispatch<'_>) -> Result<()> {
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
                self.send_to_destinations(destinations, topic.as_deref(), &payload, dispatch)
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
                self.send_to_kafka(topic, &payload, dispatch).await?;
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

    /// Send one record to a Kafka topic: through the buffer at enqueue, or
    /// straight to the producer with a piece its delivery report settles.
    #[inline]
    async fn send_to_kafka(
        &self,
        topic: &str,
        payload: &Bytes,
        dispatch: &Dispatch<'_>,
    ) -> Result<()> {
        let Some(ref kafka) = self.kafka else {
            return Err(Error::Config("Kafka sink not configured".into()));
        };
        let Some(ticket) = dispatch.ticket else {
            return kafka.at_enqueue().send(topic, payload.clone()).await;
        };
        match kafka.primary.send_held(topic, payload, ticket.piece()) {
            Ok(()) => Ok(()),
            Err((Error::Rejected(reason), Some(piece))) => {
                self.rejects
                    .dispose_held(topic, payload, &reason, piece)
                    .await
            }
            Err((e, _unsettled)) => Err(e),
        }
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
    /// A gRPC destination's records are collected in `dispatch` and sent
    /// together by [`send_grpc`](Self::send_grpc). At enqueue, the bus counts
    /// a record accepted once librdkafka queued it; a held answer waits for
    /// its delivery report.
    #[inline]
    async fn send_to_destinations(
        &self,
        destinations: &[Arc<str>],
        topic: Option<&str>,
        payload: &Bytes,
        dispatch: &mut Dispatch<'_>,
    ) -> Result<()> {
        for name in destinations {
            let Some(sink) = self.destinations.get(name.as_ref()) else {
                return Err(Error::Config(format!(
                    "destination '{name}' is not configured"
                )));
            };
            match sink {
                DestinationSink::Bus { topic: fixed } => {
                    let topic = fixed.as_deref().or(topic).ok_or_else(|| {
                        Error::Config(format!("destination '{name}' resolved no topic"))
                    })?;
                    self.send_to_kafka(topic, payload, dispatch).await?;
                }
                // A gRPC listener takes the record itself, so it is sent with
                // no wire key -- the source travels inside the record.
                DestinationSink::Grpc(delivery) => match delivery.primary.refuses(payload) {
                    // Never batched: the batch would leave it out and report it sent.
                    Some(reason) => self.settle_refused("", payload, &reason, dispatch).await?,
                    // Bytes clone is a refcount bump, not a payload copy.
                    None => dispatch.push_grpc(name, payload.clone()),
                },
                DestinationSink::Discard => {}
            }
        }
        Ok(())
    }

    /// Send each gRPC destination's records from `dispatch` in one batch.
    ///
    /// # Errors
    ///
    /// The first destination's failure; a held answer's piece for it is then
    /// left unreported, which settles it `Errored`.
    async fn send_grpc(&self, dispatch: Dispatch<'_>) -> Result<()> {
        let Dispatch { ticket, grpc } = dispatch;
        for (name, payloads) in grpc {
            let Some(DestinationSink::Grpc(delivery)) = self.destinations.get(name.as_ref()) else {
                return Err(Error::Config(format!(
                    "destination '{name}' is not a gRPC destination"
                )));
            };
            match ticket {
                None => delivery.at_enqueue().send_batch("", &payloads).await?,
                Some(ticket) => {
                    let piece = ticket.piece();
                    delivery.primary.send_batch("", &payloads).await?;
                    piece.report(DeliveryStatus::Delivered);
                }
            }
        }
        Ok(())
    }

    /// Settle a record a destination refuses for good: dead-letter it, or
    /// drop it where no DLQ is configured.
    async fn settle_refused(
        &self,
        topic: &str,
        payload: &Bytes,
        reason: &str,
        dispatch: &Dispatch<'_>,
    ) -> Result<()> {
        match dispatch.ticket {
            Some(ticket) => {
                self.rejects
                    .dispose_held(topic, payload, reason, ticket.piece())
                    .await
            }
            None => self.rejects.dispose(topic, payload, reason).await.answer(),
        }
    }

    /// Send a record validation refused to the DLQ via the unified scalo
    /// module (cascade: Kafka -> file).
    ///
    /// Held, the record counts as settled only once the DLQ confirmed the
    /// write.
    #[inline]
    async fn send_to_dlq(
        &self,
        payload: &Bytes,
        reason: &str,
        dispatch: &mut Dispatch<'_>,
    ) -> Result<()> {
        if let Some(ref dlq) = self.dlq {
            let entry = DlqEntry::new("receiver", reason, payload.to_vec());
            let Some(ticket) = dispatch.ticket else {
                return dlq
                    .send(entry)
                    .await
                    .map_err(|e| Error::Config(format!("DLQ send failed: {e}")));
            };
            let piece = ticket.piece();
            crate::buffer::rejects::dead_letter_confirmed(dlq, entry)
                .await
                .map_err(|e| Error::Transport(format!("DLQ did not confirm the write: {e}")))?;
            piece.report(DeliveryStatus::Rejected);
            Ok(())
        } else {
            // With no DLQ configured the record follows the routing table, so a
            // brokerless deployment does not need a DLQ topic to reject a bad
            // record.
            let dlq_route = { self.router.read().route_dlq(reason) };
            match dlq_route {
                RouteResult::Dlq(topic) => self.send_to_kafka(&topic, payload, dispatch).await,
                RouteResult::Send {
                    ref destinations,
                    ref topic,
                } => {
                    self.send_to_destinations(destinations, topic.as_deref(), payload, dispatch)
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
        if let Some(ref kafka) = self.kafka
            && let Some(stats) = kafka.stats().await
        {
            total_queue += stats.queue_size as u64;
            metrics.set_circuit_state(stats.circuit_state, stats.consecutive_failures);
        }

        // Named destination stats
        for sink in self.destinations.values() {
            let queue = match sink {
                DestinationSink::Bus { .. } | DestinationSink::Discard => 0,
                DestinationSink::Grpc(s) => s.stats().await.map_or(0, |stats| stats.queue_size),
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

/// The spool directory of gRPC destination `name`, relative to the spillover
/// path: a path segment of its own, with anything outside `[A-Za-z0-9_-]`
/// replaced.
fn grpc_spool(name: &str) -> PathBuf {
    let segment: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    Path::new("grpc").join(segment)
}

/// Take `spool` for one sink, refusing a second sink the same directory.
fn claim_spool(claimed: &mut FxHashSet<PathBuf>, spool: PathBuf) -> Result<PathBuf> {
    if !claimed.insert(spool.clone()) {
        return Err(Error::Config(format!(
            "two destinations would share the spool directory {} -- rename one",
            spool.display()
        )));
    }
    Ok(spool)
}

/// Carry the receiver's spillover settings onto scalo's `TieredSink`, spooling
/// under `spool` within the spillover path.
fn spillover_config(
    spillover: &crate::config::SpilloverConfig,
    spool: &Path,
) -> scalo::tiered_sink::TieredSinkConfig {
    let mut config = scalo::tiered_sink::TieredSinkConfig::new(spillover.path.join(spool));
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
    primary: Arc<S>,
    spool: &Path,
    buffer_config: &BufferConfig,
    rejects: Rejects,
) -> Result<SinkBackend<S>> {
    if buffer_config.spillover.enabled {
        let adapter = crate::buffer::adapter::ScaloSinkAdapter::new(primary, rejects);
        let config = spillover_config(&buffer_config.spillover, spool);
        tokio::fs::create_dir_all(&config.spool_path)
            .await
            .map_err(|e| {
                Error::Config(format!(
                    "failed to create spool directory {}: {e}",
                    config.spool_path.display()
                ))
            })?;
        let path = config.spool_path.clone();

        let tiered = scalo::tiered_sink::TieredSink::new(adapter, config)
            .await
            .map_err(|e| Error::Config(format!("failed to create tiered sink: {e}")))?;

        info!(
            path = %path.display(),
            max_usage_percent = buffer_config.spillover.max_usage_percent,
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
        if let Some(ref kafka) = self.state.kafka {
            kafka.start(self.shutdown.clone());
        }
        for sink in self.state.destinations.values() {
            match sink {
                DestinationSink::Bus { .. } | DestinationSink::Discard => {}
                DestinationSink::Grpc(s) => s.start(self.shutdown.clone()),
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
        if let Some(ref kafka) = self.state.kafka
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

    /// The acknowledgement setting a listener with acks on gets.
    fn holding(state: &PipelineState) -> Acks {
        state.acks("test", AcknowledgementsConfig::default(), None)
    }

    /// Declared gRPC destinations `names`, every record fanned out to all of
    /// them, each at an address nothing listens on.
    fn grpc_destinations(config: &mut Config, names: &[&str]) {
        config.destinations.default =
            crate::config::DestinationRef::Many(names.iter().map(ToString::to_string).collect());
        for (port, name) in (1_u16..).zip(names) {
            config.destinations.named.insert(
                (*name).to_string(),
                crate::config::DestinationSpec {
                    grpc: Some(crate::config::GrpcDestination {
                        endpoint: format!("http://127.0.0.1:{port}"),
                    }),
                    kafka: None,
                },
            );
        }
    }

    /// Spillover on, under `dir`.
    fn spill_to(config: &mut Config, dir: &std::path::Path) {
        config.buffer.spillover = crate::config::SpilloverConfig {
            enabled: true,
            path: dir.to_path_buf(),
            ..crate::config::SpilloverConfig::default()
        };
    }

    /// The spillover thresholds an operator sets reach scalo's TieredSink,
    /// under the bus's own directory.
    #[test]
    fn spillover_settings_reach_the_tiered_sink() {
        let spillover = crate::config::SpilloverConfig {
            enabled: true,
            path: std::path::PathBuf::from("/var/spool/somewhere-else"),
            max_usage_percent: 0.55,
            poll_interval_secs: 17,
        };

        let built = spillover_config(&spillover, Path::new(BUS_SPOOL));

        assert_eq!(built.spool_path, spillover.path.join("kafka"));
        let disk = built.disk_aware.unwrap();
        assert!((disk.max_usage_percent - 0.55).abs() < f64::EPSILON);
        assert_eq!(disk.poll_interval_secs, 17);
    }

    /// A destination name becomes one path segment under `grpc/`, so no name
    /// reaches outside the spillover path.
    #[test]
    fn a_destination_spools_under_its_own_segment() {
        assert_eq!(grpc_spool("orders"), Path::new("grpc/orders"));
        assert_eq!(grpc_spool("../etc"), Path::new("grpc/___etc"));
        assert_eq!(grpc_spool("a/b"), Path::new("grpc/a_b"));
    }

    /// Two names that map to one directory are refused rather than left to
    /// share a spool.
    #[test]
    fn two_sinks_never_share_a_spool() {
        let mut claimed = FxHashSet::default();
        assert!(claim_spool(&mut claimed, grpc_spool("a.b")).is_ok());
        assert!(claim_spool(&mut claimed, grpc_spool("a_b")).is_err());
    }

    /// Two gRPC destinations with spillover each open a spool of their own, so
    /// both start: sharing one path fails the second at startup.
    #[tokio::test]
    async fn two_grpc_destinations_with_spillover_start() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        grpc_destinations(&mut config, &["alpha", "beta"]);
        spill_to(&mut config, dir.path());
        // An at-enqueue listener is what needs the spools.
        config.server.acknowledgements = AcknowledgementsConfig::new(false);

        let built = PipelineState::new(SharedConfig::new(config), CancellationToken::new()).await;

        assert!(built.is_ok(), "{:?}", built.err());
        assert!(dir.path().join("grpc/alpha").is_dir());
        assert!(dir.path().join("grpc/beta").is_dir());
    }

    /// With every listener holding its answer, no buffer or spool is built:
    /// a spooled record could be spooled again on every resend.
    #[tokio::test]
    async fn held_answers_open_no_spool() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        grpc_destinations(&mut config, &["alpha"]);
        spill_to(&mut config, dir.path());

        let state = test_state_with(config).await;

        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "a spool was opened with every answer held"
        );
        let Some(DestinationSink::Grpc(delivery)) = state.destinations.get("alpha") else {
            panic!("alpha is a gRPC destination");
        };
        assert!(delivery.queued.is_none(), "a buffer was built");
    }

    /// A destination that refuses the record answers the held request as a
    /// resend of all of it, with nothing counted as taken.
    #[tokio::test]
    async fn a_refusing_destination_answers_a_held_request_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        grpc_destinations(&mut config, &["alpha"]);
        spill_to(&mut config, dir.path());
        let state = test_state_with(config).await;

        let outcome = state
            .process_batch_acked(
                &[Bytes::from(r#"{"a":1}"#), Bytes::from(r#"{"a":2}"#)],
                &holding(&state),
                None,
            )
            .await;

        assert!(
            outcome
                .unavailable
                .as_ref()
                .is_some_and(Error::is_retryable),
            "{:?}",
            outcome.unavailable
        );
        assert_eq!(outcome.settled(), 0, "a held batch is all or nothing");
    }

    /// A next hop that takes the connection and never answers is cut off at
    /// the hold, not at its 20 s send deadline, so the sender hears retryable
    /// while it still waits.
    #[tokio::test]
    async fn a_stalled_next_hop_is_answered_at_the_hold() {
        let stalled = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = stalled.local_addr().unwrap();
        // Accepts every connection and holds it, reading and writing nothing.
        let holder = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = stalled.accept().await {
                held.push(socket);
            }
        });
        let mut config = test_config();
        grpc_destinations(&mut config, &["alpha"]);
        if let Some(grpc) = config
            .destinations
            .named
            .get_mut("alpha")
            .and_then(|spec| spec.grpc.as_mut())
        {
            grpc.endpoint = format!("http://{addr}");
        }
        let state = test_state_with(config).await;
        // A 2 s request timeout holds for 1 s.
        let acks = state.acks(
            "test",
            AcknowledgementsConfig::default(),
            Some(Duration::from_secs(2)),
        );

        let started = std::time::Instant::now();
        let outcome = state
            .process_batch_acked(&[Bytes::from(r#"{"a":1}"#)], &acks, None)
            .await;
        let answered_in = started.elapsed();
        holder.abort();

        assert!(
            answered_in < Duration::from_secs(5),
            "answered after {answered_in:?}, not at the 1 s hold"
        );
        assert!(
            outcome
                .unavailable
                .as_ref()
                .is_some_and(Error::is_retryable),
            "{:?}",
            outcome.unavailable
        );
        assert_eq!(outcome.settled(), 0, "a held batch is all or nothing");
    }

    /// A Kafka config pointed at an unroutable broker whose records time out
    /// locally after a second.
    fn unroutable_bus(config: &mut Config) {
        config.destinations.default = crate::config::BUS_DESTINATION.into();
        config.kafka.brokers = vec!["192.0.2.1:9092".to_string()];
        config
            .kafka
            .librdkafka_overrides
            .insert("message.timeout.ms".to_string(), "1000".to_string());
    }

    /// Held, a record no broker confirms answers retryable; at enqueue the
    /// same record is taken once librdkafka queues it.
    #[tokio::test]
    async fn a_held_answer_waits_for_the_kafka_delivery_report() {
        let mut config = test_config();
        unroutable_bus(&mut config);
        let state = test_state_with(config).await;
        let record = [Bytes::from(r#"{"a":1}"#)];

        let at_enqueue = state.process_batch(&record).await;
        assert_eq!(at_enqueue.accepted, 1);
        assert!(at_enqueue.unavailable.is_none());

        let held = tokio::time::timeout(
            Duration::from_secs(20),
            state.process_batch_acked(&record, &holding(&state), None),
        )
        .await
        .expect("the delivery report settles the request inside its hold");
        assert!(
            held.unavailable.as_ref().is_some_and(Error::is_retryable),
            "a record no broker confirmed was answered as taken: {held:?}"
        );
        assert_eq!(held.settled(), 0);
    }

    /// A validation DLQ writing to files under `dir`.
    fn file_dlq(config: &mut Config, dir: &std::path::Path) {
        config.routing.dlq = crate::config::DlqConfig {
            enabled: true,
            mode: "file_only".to_string(),
            file_path: dir.display().to_string(),
            kafka_enabled: false,
            ..crate::config::DlqConfig::default()
        };
    }

    /// Held, a dead-lettered record counts once the DLQ confirmed the write.
    #[tokio::test]
    async fn a_dead_letter_the_dlq_confirms_is_taken() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        file_dlq(&mut config, dir.path());
        let state = test_state_with(config).await;

        let outcome = state
            .process_batch_acked(&[Bytes::from("not json")], &holding(&state), None)
            .await;

        assert!(outcome.unavailable.is_none(), "{:?}", outcome.unavailable);
        assert_eq!(outcome.accepted, 1);
    }

    /// Held, a dead letter the DLQ refuses answers retryable: the record is in
    /// neither the destination nor the DLQ.
    #[tokio::test]
    async fn a_dead_letter_the_dlq_refuses_answers_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        file_dlq(&mut config, dir.path());
        let state = test_state_with(config).await;
        // Every file write fails once the service directory is a regular file.
        std::fs::remove_dir_all(dir.path().join("receiver")).unwrap();
        std::fs::write(dir.path().join("receiver"), b"not a directory").unwrap();

        let outcome = state
            .process_batch_acked(&[Bytes::from("not json")], &holding(&state), None)
            .await;

        assert!(
            outcome
                .unavailable
                .as_ref()
                .is_some_and(Error::is_retryable),
            "{:?}",
            outcome.unavailable
        );
        assert_eq!(outcome.settled(), 0);
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
        let outcome = state.process_batch(&payloads).await;
        assert_eq!(outcome.accepted, 3, "all 3 items should succeed");
        assert!(outcome.into_error().is_none(), "no errors expected");
    }

    /// A record refused for good does not stop the records behind it, and the
    /// batch reports the refusal.
    #[tokio::test]
    async fn a_rejected_record_does_not_stop_the_batch() {
        let mut config = test_config();
        config.validation.dlq_on_invalid = false;
        let state = test_state_with(config).await;
        let payloads = vec![
            Bytes::from(r#"{"ok":1}"#),
            Bytes::from("not json"),
            Bytes::from(r#"{"ok":2}"#),
        ];

        let outcome = state.process_batch(&payloads).await;

        assert_eq!(outcome.accepted, 2);
        assert_eq!(outcome.rejected, 1);
        assert!(outcome.unavailable.is_none(), "{:?}", outcome.unavailable);
        assert!(
            matches!(outcome.first_rejection, Some(Error::Validation(_))),
            "{:?}",
            outcome.first_rejection
        );
    }

    /// Held, a record refused for good is still the sender's to hear about,
    /// and the records around it are taken once confirmed.
    #[tokio::test]
    async fn a_held_batch_reports_a_rejected_record() {
        let mut config = test_config();
        config.validation.dlq_on_invalid = false;
        let state = test_state_with(config).await;
        let payloads = vec![
            Bytes::from(r#"{"ok":1}"#),
            Bytes::from("not json"),
            Bytes::from(r#"{"ok":2}"#),
        ];

        let outcome = state
            .process_batch_acked(&payloads, &holding(&state), None)
            .await;

        assert_eq!(outcome.accepted, 2);
        assert_eq!(outcome.rejected, 1);
        assert!(outcome.unavailable.is_none(), "{:?}", outcome.unavailable);
    }

    /// A retryable failure stops the batch where it happened: the sender is
    /// told to resend, so taking the records behind it only adds duplicates.
    ///
    /// The invalid record is routed to a DLQ topic with no broker configured,
    /// which the receiver cannot serve now but could once the route works.
    #[tokio::test]
    async fn a_retryable_failure_stops_the_batch() {
        let state = test_state().await;
        let payloads = vec![
            Bytes::from(r#"{"ok":1}"#),
            Bytes::from("not json"),
            Bytes::from(r#"{"ok":2}"#),
        ];

        let outcome = state.process_batch(&payloads).await;

        assert_eq!(outcome.accepted, 1, "the record before the failure");
        assert_eq!(outcome.settled(), 1, "nothing after the failure is taken");
        assert!(
            outcome
                .unavailable
                .as_ref()
                .is_some_and(Error::is_retryable),
            "{:?}",
            outcome.unavailable
        );
    }

    /// Pressure refuses the whole batch before any record is taken.
    #[tokio::test]
    async fn pressure_refuses_the_whole_batch_as_retryable() {
        let mut config = test_config();
        config.buffer.memory_limit = 1000;
        config.buffer.pressure_threshold = 0.8;
        let state = test_state_on_reservations(config).await;
        state.memory_guard().add_bytes(900);

        let outcome = state.process_batch(&[Bytes::from(r#"{"a":1}"#)]).await;

        assert_eq!(outcome.settled(), 0);
        assert!(matches!(outcome.unavailable, Some(Error::Buffer(_))));
    }

    /// A single-error answer reports the retryable failure over an earlier
    /// refusal, so the sender resends rather than dropping the batch.
    #[test]
    fn the_retryable_error_wins_the_single_error_answer() {
        let mut outcome = BatchOutcome::default();
        assert!(
            outcome
                .record(Err(Error::Validation("bad".into())))
                .is_continue()
        );
        assert!(
            outcome
                .record(Err(Error::Transport("down".into())))
                .is_break()
        );
        assert!(matches!(outcome.into_error(), Some(Error::Transport(_))));
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
