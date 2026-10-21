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
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use rustc_hash::{FxHashMap, FxHashSet};
use scalo::UnifiedPressure;
use scalo::dlq::{Dlq, DlqEntry, DlqError};
use scalo::logger::security;
use scalo::transport::AcknowledgementsConfig;
use scalo::transport::DeliveryStatus;
use scalo::transport::ack::{SinkConfirmation, Ticket};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

pub use acks::Acks;

use crate::buffer::rejects::DeadLetter;
use crate::buffer::{
    InMemoryBuffer, InMemoryBufferStats, MemoryGuard, MemoryGuardConfig, MemoryPressure, Rejects,
    SinkBackend,
};
use crate::config::{BufferConfig, Config, DestinationsConfig, SharedConfig};
use crate::error::{Error, Result};
use crate::metrics::{Metrics, ValidationFailureReason};
use crate::routing::{self, RouteResult, Routed, Router};
use crate::server::traits::BoundAddr;
use crate::sink::Sink;
use crate::sink::file::FileSink;
use crate::sink::grpc::GrpcSink;
use crate::sink::kafka::KafkaSink;
use crate::tenancy::Ingest;
use crate::validation::depth::{self, MAX_PARSE_DEPTH};
use crate::validation::{self, ValidationResult, Validator};

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
    /// The sink behind `queued` when it is not `primary`.
    enqueue_sink: Option<Arc<S>>,
    /// The buffer's drain task.
    drain: Mutex<Drain>,
}

/// Where a buffer's drain task is in its life.
enum Drain {
    /// Not started: the orchestrator starts the drains before the listeners accept.
    Idle,
    /// Running until `stop` is cancelled, when it flushes the buffer and ends.
    Running {
        stop: CancellationToken,
        task: JoinHandle<()>,
    },
    /// Stopped for good, so a late start cannot bring it back.
    Stopped,
}

impl<S: Sink + 'static> Delivery<S> {
    /// The sink, with a buffer in front of it when `queued` and its spool,
    /// with spillover on, at `spool` under the spillover path. The buffer
    /// sends through `enqueue_sink` where given, else through `primary`.
    async fn build(
        primary: S,
        enqueue_sink: Option<S>,
        spool: &Path,
        buffer: &BufferConfig,
        rejects: Rejects,
        queued: bool,
    ) -> Result<Self> {
        let primary = Arc::new(primary);
        let enqueue_sink = enqueue_sink.filter(|_| queued).map(Arc::new);
        let queued = if queued {
            let behind = enqueue_sink
                .as_ref()
                .map_or_else(|| Arc::clone(&primary), Arc::clone);
            Some(Arc::new(
                build_sink_backend(behind, spool, buffer, rejects).await?,
            ))
        } else {
            None
        };
        Ok(Self {
            primary,
            queued,
            enqueue_sink,
            drain: Mutex::new(Drain::Idle),
        })
    }

    /// The sink an at-enqueue answer sends through.
    fn at_enqueue(&self) -> &dyn Sink {
        match &self.queued {
            Some(queued) => queued.as_ref(),
            None => self.primary.as_ref(),
        }
    }

    fn is_healthy(&self) -> bool {
        self.at_enqueue().is_healthy() && (self.enqueue_sink.is_none() || self.primary.is_healthy())
    }

    async fn stats(&self) -> Option<InMemoryBufferStats> {
        match &self.queued {
            Some(queued) => Some(queued.stats().await),
            None => None,
        }
    }

    /// Start the buffer's drain, where there is a buffer, to run until
    /// `shutdown` or [`stop_drain`](Self::stop_drain). A second start does
    /// nothing.
    fn start(&self, shutdown: &CancellationToken) {
        let Some(queued) = &self.queued else {
            return;
        };
        let mut drain = self.drain.lock();
        if !matches!(*drain, Drain::Idle) {
            return;
        }
        let stop = shutdown.child_token();
        if let Some(task) = Arc::clone(queued).start_drain_task(stop.clone()) {
            *drain = Drain::Running { stop, task };
        }
    }

    /// Stop the drain task for good and wait for its last flush.
    async fn stop_drain(&self) {
        let drain = std::mem::replace(&mut *self.drain.lock(), Drain::Stopped);
        if let Drain::Running { stop, task } = drain {
            stop.cancel();
            if let Err(e) = task.await {
                error!(error = %e, "A buffer's drain task ended abnormally");
            }
        }
    }

    /// Drain the buffer into its sink and flush it, and flush `primary` too
    /// when it is a sink of its own.
    async fn flush(&self) -> Result<()> {
        if self.enqueue_sink.is_none() {
            return self.at_enqueue().flush().await;
        }
        let (queued, held) = tokio::join!(self.at_enqueue().flush(), self.primary.flush());
        queued.and(held)
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

/// What a named destination's sink is built from. A reload rebuilds a
/// destination only when its wiring changes.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Wiring {
    /// The bus, on the topic the record's source resolves to or a fixed one.
    Bus { topic: Option<Arc<str>> },
    /// A scalo Push listener at `endpoint`, each send bounded by `deadline_ms`.
    Grpc {
        endpoint: String,
        deadline_ms: u64,
        confirms_delivery: bool,
    },
    /// The memory transport's loader.
    Discard,
}

impl Wiring {
    /// What destination `name` of `destinations` is built from.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when its declaration names neither transport.
    fn of(name: &str, destinations: &DestinationsConfig, loader_timeout_ms: u64) -> Result<Self> {
        match destinations.named.get(name) {
            Some(spec) => match (&spec.grpc, &spec.kafka) {
                (Some(grpc), _) => Ok(Self::Grpc {
                    endpoint: grpc.endpoint.clone(),
                    // loader.timeout_ms bounds the loader's own RPC; a declared
                    // destination has no timeout key of its own.
                    deadline_ms: if name == crate::config::LOADER_DESTINATION {
                        loader_timeout_ms
                    } else {
                        acks::NEXT_HOP_DEADLINE_MS
                    },
                    confirms_delivery: grpc.confirms_delivery,
                }),
                (None, Some(bus)) => Ok(Self::Bus {
                    topic: bus.topic.as_deref().map(Arc::from),
                }),
                (None, None) => Err(Error::Config(format!(
                    "destination '{name}' needs exactly one of grpc or kafka"
                ))),
            },
            // `loader` survives resolution only on the memory transport.
            None if name == crate::config::LOADER_DESTINATION => Ok(Self::Discard),
            None => Ok(Self::Bus { topic: None }),
        }
    }

    /// What a listener whose records can reach this destination confirms.
    fn confirms(&self) -> SinkConfirmation {
        match self {
            Self::Grpc {
                confirms_delivery: false,
                ..
            }
            | Self::Discard => SinkConfirmation::None,
            Self::Grpc { .. } | Self::Bus { .. } => SinkConfirmation::Remote,
        }
    }
}

/// A named destination's sink and the wiring it was built from.
struct Destination {
    wiring: Wiring,
    sink: DestinationSink,
}

impl Destination {
    /// Build destination `name` from `wiring`, claiming its spool in `spools`.
    ///
    /// # Errors
    ///
    /// The gRPC sink, a spool another destination claimed, or the buffer
    /// failing to build.
    async fn build(
        name: &str,
        wiring: Wiring,
        settings: &DeliverySettings,
        rejects: &Rejects,
        spools: &mut FxHashSet<PathBuf>,
    ) -> Result<Self> {
        let sink = match &wiring {
            Wiring::Bus { topic } => DestinationSink::Bus {
                topic: topic.clone(),
            },
            Wiring::Discard => DestinationSink::Discard,
            Wiring::Grpc {
                endpoint,
                deadline_ms,
                ..
            } => {
                let primary = GrpcSink::new(endpoint, Some(*deadline_ms)).await?;
                let spool = claim_spool(spools, grpc_spool(name))?;
                DestinationSink::Grpc(
                    Delivery::build(
                        primary,
                        None,
                        &spool,
                        &settings.buffer,
                        rejects.clone(),
                        settings.at_enqueue,
                    )
                    .await?,
                )
            }
        };
        Ok(Self { wiring, sink })
    }

    /// The delivery of a gRPC destination.
    fn grpc(&self) -> Option<&Delivery<GrpcSink>> {
        match &self.sink {
            DestinationSink::Grpc(delivery) => Some(delivery),
            DestinationSink::Bus { .. } | DestinationSink::Discard => None,
        }
    }
}

/// The named destination set: name -> destination.
type DestinationSet = FxHashMap<Arc<str>, Arc<Destination>>;

/// What a request routes by: the rules and the destinations they name, swapped
/// together so a request never routes to a name its set lacks.
struct Routes {
    router: Arc<Router>,
    destinations: DestinationSet,
}

/// A destination whose buffer spools to disk, rewired by a reload: its sender
/// moves from `from` to `wiring`, on the spool `from` holds.
struct SpoolMove {
    name: Arc<str>,
    from: Arc<Destination>,
    wiring: Wiring,
}

/// What every destination's delivery is built with, fixed at startup: the
/// listeners, and so where they answer, do not change on a reload.
struct DeliverySettings {
    buffer: BufferConfig,
    at_enqueue: bool,
}

impl DeliverySettings {
    /// Whether a gRPC destination's buffer spools to disk.
    fn spools(&self) -> bool {
        self.at_enqueue && self.buffer.spillover.enabled
    }
}

/// Whether a batch reaches the pipeline for the first time or again.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Offer {
    /// First offered, so its records count as received.
    First,
    /// Offered again by a listener holding it for a retry: already counted.
    Again,
}

/// Where a batch's records go.
#[derive(Clone, Copy)]
enum Target<'a> {
    /// Enriched and routed to the destinations the routing rules pick.
    Routed,
    /// Sent to this Kafka topic, which the listener chose.
    Topic(&'a str),
}

/// One request's sends: the ticket its answer is held on, the routes it was
/// taken under, the rows its records fired, and the records bound for each
/// gRPC destination, sent together once the request is routed.
struct Dispatch<'a> {
    ticket: Option<&'a Ticket>,
    /// Fixed for the request, so a reload cannot take a destination from under
    /// a record already routed to it.
    routes: &'a Routes,
    /// Counts the rows that fire, added to the rule counters once per request.
    tally: scalo::rules::Batch<'a>,
    grpc: Vec<(Arc<str>, Vec<Bytes>)>,
    usage: Usage,
}

impl<'a> Dispatch<'a> {
    fn new(ticket: Option<&'a Ticket>, routes: &'a Routes) -> Self {
        Self {
            ticket,
            routes,
            tally: routes.router.tally(),
            grpc: Vec::new(),
            usage: Usage::default(),
        }
    }

    /// Whether the routes hold the memory transport's discard sink.
    fn discards(&self) -> bool {
        self.routes
            .destinations
            .values()
            .any(|destination| matches!(destination.sink, DestinationSink::Discard))
    }

    fn push_grpc(&mut self, name: &Arc<str>, payload: Bytes) {
        match self.grpc.iter_mut().find(|(n, _)| n == name) {
            Some((_, payloads)) => payloads.push(payload),
            None => self.grpc.push((Arc::clone(name), vec![payload])),
        }
    }
}

/// The decoded bytes each org's taken records carried in one batch, counted
/// on `usage_ingested_bytes_total` only once the batch settles as taken.
#[derive(Debug, Default)]
struct Usage(Vec<(String, u64)>);

impl Usage {
    /// Count a taken record of `bytes` for `org`. A batch carries few orgs.
    fn add(&mut self, org: &str, bytes: u64) {
        match self.0.iter_mut().find(|(seen, _)| seen == org) {
            Some((_, total)) => *total += bytes,
            None => self.0.push((org.to_string(), bytes)),
        }
    }

    /// Add the batch's bytes to each org's counter.
    fn commit(self) {
        for (org, bytes) in self.0 {
            metrics::counter!("usage_ingested_bytes_total", "org_id" => org).increment(bytes);
        }
    }
}

/// Shared pipeline state accessible from handlers.
pub struct PipelineState {
    shared_config: SharedConfig,
    validator: RwLock<Validator>,
    /// The routing rules and the destination set, swapped whole by a reload.
    routes: ArcSwap<Routes>,
    kafka: Option<Delivery<KafkaSink>>,
    /// What a destination a reload adds is built with.
    settings: DeliverySettings,
    /// The token the drains run under, once the orchestrator started them, so
    /// a destination a reload adds starts its own.
    drains: OnceLock<CancellationToken>,
    /// One reload at a time: each builds on the destination set it replaces.
    reloading: tokio::sync::Mutex<()>,
    /// Destinations a reload removed, still handing back what they hold.
    retiring: Mutex<JoinSet<()>>,
    /// Ends a retirement still holding records nothing would take.
    shutdown: CancellationToken,
    file_sink: Option<Arc<FileSink>>,
    memory_guard: Arc<MemoryGuard>,
    /// Self-regulation pressure latch from the runtime governor.
    ///
    /// `Some` when self-regulation is enabled (the default): the originator
    /// ingest brake (HTTP 503 / gRPC `UNAVAILABLE`) consults this unified,
    /// hysteretic pressure signal -- the HARD memory source is the never-OOM
    /// authority. `None` when `self_regulation.enabled = false`, in which case
    /// the brake reads `memory_guard`'s own `under_pressure()` threshold.
    pressure: Option<Arc<UnifiedPressure>>,
    dlq: Option<Arc<Dlq>>,
    /// Where a record a destination refuses for good goes.
    rejects: Rejects,
    /// What the weakest destination a record can reach confirms.
    sink_confirmation: SinkConfirmation,
    /// The listeners readiness waits on, `None` until the server declares them.
    listeners: RwLock<Option<Vec<BoundAddr>>>,
    /// Counts every record received and settled, and those refused before
    /// validation; `None` in tests built without it.
    metrics: Option<Arc<Metrics>>,
}

impl PipelineState {
    /// Create pipeline state with no governor and no injected memory guard, so
    /// it builds its own guard from `buffer.memory_limit` and
    /// `buffer.pressure_threshold`.
    ///
    /// Only tests take this path: production always injects the runtime's
    /// guard through [`with_governor`](Self::with_governor), with
    /// self-regulation on or off.
    pub async fn new(shared_config: SharedConfig, shutdown: CancellationToken) -> Result<Self> {
        Self::with_governor(shared_config, shutdown, None, None).await
    }

    /// Create new pipeline state, optionally wired to the runtime governor.
    ///
    /// `governor` is the runtime self-regulation governor (the originator brake
    /// source of truth). When `Some`, the pipeline tracks ingest bytes on the
    /// governor's OWN memory guard so the governor's HARD memory source -- and
    /// thus the `UnifiedPressure` latch the ingest brake consults -- actually
    /// reacts to in-flight load. When `None` (self-regulation disabled) the
    /// brake reads `runtime_memory_guard`'s own threshold.
    ///
    /// Production always passes `runtime_memory_guard`. Only with it `None`,
    /// which tests alone do, does the pipeline build a guard of its own from
    /// `buffer.memory_limit` and `buffer.pressure_threshold`.
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
        let router = Router::new(&config, &destinations_config)?;
        // Memory guard + pressure source of truth.
        //
        // Production injects the runtime's guard (the one the governor's HARD
        // memory source watches) so ingest byte reservations feed the
        // UnifiedPressure latch. The buffer-configured guard is built only
        // when no guard is injected, which tests alone do.
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

        let dlq = spawn_dlq(&config, &shutdown)?;
        // Records a destination refuses for good go to the same DLQ.
        let rejects = Rejects::new(dlq.clone(), metrics.clone());

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
            // A record answered at enqueue has no resend behind it once librdkafka
            // gives up, so it keeps librdkafka's own timeout on its own producer.
            let enqueue_sink = (at_enqueue && held_message_timeout.is_some())
                .then(|| KafkaSink::new(&config.kafka, None))
                .transpose()?;
            let spool = claim_spool(&mut spools, PathBuf::from(BUS_SPOOL))?;
            Some(
                Delivery::build(
                    primary,
                    enqueue_sink,
                    &spool,
                    &config.buffer,
                    rejects.clone(),
                    at_enqueue,
                )
                .await?,
            )
        };

        // Build the named destination set: one sink per destination the config
        // actually refers to, so an unused declaration opens no connection.
        // A record can reach any of them, so a listener confirms no more than
        // the weakest does.
        let settings = DeliverySettings {
            buffer: config.buffer.clone(),
            at_enqueue,
        };
        let mut destinations = DestinationSet::default();
        let mut sink_confirmation = SinkConfirmation::Remote;
        for name in destinations_config.referenced_names() {
            if destinations.contains_key(name) {
                continue;
            }
            let wiring = Wiring::of(name, &destinations_config, config.loader.timeout_ms)?;
            sink_confirmation = acks::weakest(sink_confirmation, wiring.confirms());
            let destination =
                Destination::build(name, wiring, &settings, &rejects, &mut spools).await?;
            destinations.insert(Arc::from(name), Arc::new(destination));
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
            routes: ArcSwap::from_pointee(Routes {
                router: Arc::new(router),
                destinations,
            }),
            kafka,
            settings,
            drains: OnceLock::new(),
            reloading: tokio::sync::Mutex::new(()),
            retiring: Mutex::new(JoinSet::new()),
            shutdown,
            file_sink,
            memory_guard,
            pressure,
            dlq,
            rejects,
            sink_confirmation,
            listeners: RwLock::new(None),
            metrics,
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

    /// The routes as they stand.
    fn routes(&self) -> Arc<Routes> {
        self.routes.load_full()
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
            self.sink_confirmation,
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
        self.routes
            .load()
            .destinations
            .values()
            .all(|destination| destination.grpc().is_none_or(Delivery::is_healthy))
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
    /// When disabled, it is the memory guard's own threshold check.
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

    /// Process a message from `ingest` through the pipeline, answered at enqueue.
    ///
    /// This is a HOT PATH function.
    #[inline]
    pub async fn process(&self, payload: Bytes, ingest: &Ingest) -> Result<()> {
        self.count_received(1);
        let mut outcome = BatchOutcome::default();
        let _ = outcome.record(self.process_one(payload, ingest).await);
        self.count_settled(&outcome);
        outcome.into_error().map_or(Ok(()), Err)
    }

    /// [`process`](Self::process), uncounted.
    #[inline]
    async fn process_one(&self, payload: Bytes, ingest: &Ingest) -> Result<()> {
        self.brake()?;

        // Tracked for the life of this future.
        let _lease = MemoryLease::acquire(&self.memory_guard, payload.len() as u64);

        let routes = self.routes();
        let mut dispatch = Dispatch::new(None, &routes);
        self.process_inner(payload, Target::Routed, ingest, &mut dispatch)
            .await?;
        self.send_grpc(&mut dispatch).await?;
        std::mem::take(&mut dispatch.usage).commit();
        Ok(())
    }

    /// Process a batch of messages from `ingest`, answered at enqueue.
    ///
    /// Amortises overhead: single backpressure check, single memory tracking
    /// update, and per-message process_inner() calls. A record refused for
    /// good does not stop the rest; a retryable failure does, see
    /// [`BatchOutcome`].
    pub async fn process_batch(&self, payloads: &[Bytes], ingest: &Ingest) -> BatchOutcome {
        self.run(payloads, Target::Routed, ingest, None, Offer::First)
            .await
    }

    /// Process a batch from `ingest` for a listener with the setting `acks`,
    /// whose sender allows `sender_deadline`.
    ///
    /// Holding, the outcome counts every record taken only once every
    /// destination confirmed it; anything short of that is a resend of the
    /// whole batch.
    pub async fn process_batch_acked(
        &self,
        payloads: &[Bytes],
        acks: &Acks,
        sender_deadline: Option<Duration>,
        ingest: &Ingest,
    ) -> BatchOutcome {
        self.run(
            payloads,
            Target::Routed,
            ingest,
            Some((acks, sender_deadline)),
            Offer::First,
        )
        .await
    }

    /// [`process_batch_acked`](Self::process_batch_acked) with no sender
    /// deadline, for a listener that may offer the same records again.
    pub(crate) async fn offer_batch_acked(
        &self,
        payloads: &[Bytes],
        acks: &Acks,
        offer: Offer,
        ingest: &Ingest,
    ) -> BatchOutcome {
        self.run(payloads, Target::Routed, ingest, Some((acks, None)), offer)
            .await
    }

    /// [`process_batch_acked`](Self::process_batch_acked) to a Kafka topic
    /// the listener chose, with no routing or enrichment: each record takes
    /// only its `_org_id`, held to `ingest`'s binding. On the test-only memory
    /// transport (`loader.transport: memory`, refused at startup) there is no
    /// broker and a valid record is accepted and dropped.
    pub async fn process_batch_to_topic(
        &self,
        payloads: &[Bytes],
        topic: &str,
        acks: &Acks,
        ingest: &Ingest,
    ) -> BatchOutcome {
        self.run(
            payloads,
            Target::Topic(topic),
            ingest,
            Some((acks, None)),
            Offer::First,
        )
        .await
    }

    /// Count a batch's records as received on its first offer, and what the
    /// pipeline made of them on every offer.
    async fn run(
        &self,
        payloads: &[Bytes],
        target: Target<'_>,
        ingest: &Ingest,
        hold: Option<(&Acks, Option<Duration>)>,
        offer: Offer,
    ) -> BatchOutcome {
        if matches!(offer, Offer::First) {
            self.count_received(payloads.len());
        }
        let outcome = self.run_batch(payloads, target, ingest, hold).await;
        self.count_settled(&outcome);
        outcome
    }

    /// Count records a listener offered for the first time.
    fn count_received(&self, records: usize) {
        if let Some(ref metrics) = self.metrics {
            metrics.add_records_received(records as u64);
        }
    }

    /// Count the records `outcome` settled, taken or refused for good.
    fn count_settled(&self, outcome: &BatchOutcome) {
        if let Some(ref metrics) = self.metrics {
            metrics.add_records_settled(outcome.accepted as u64, outcome.rejected as u64);
        }
    }

    /// Run a batch to `target`, held on a ticket when `hold` admits one.
    ///
    /// The batch's usage is counted once it settles with records taken: a
    /// batch answered as a resend of all of it counts nothing.
    async fn run_batch(
        &self,
        payloads: &[Bytes],
        target: Target<'_>,
        ingest: &Ingest,
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
            let usage = self
                .dispatch(payloads, target, ingest, None, &mut outcome)
                .await;
            if outcome.accepted > 0 {
                usage.commit();
            }
            return outcome;
        };
        // A slow destination must not hold the answer past the sender's deadline.
        let hold_ends = tokio::time::Instant::from_std(ticket.deadline());
        let dispatched = self.dispatch(payloads, target, ingest, Some(&ticket), &mut outcome);
        let Ok(usage) = tokio::time::timeout_at(hold_ends, dispatched).await else {
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
        };
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
            return outcome;
        }
        usage.commit();
        outcome
    }

    /// Route and send each record of a batch, then send the gRPC destinations
    /// their share, recording in `outcome` what was taken. Returns the usage
    /// of the records taken, for the caller to count once the batch settles.
    async fn dispatch(
        &self,
        payloads: &[Bytes],
        target: Target<'_>,
        ingest: &Ingest,
        ticket: Option<&Ticket>,
        outcome: &mut BatchOutcome,
    ) -> Usage {
        let routes = self.routes();
        let mut dispatch = Dispatch::new(ticket, &routes);
        for payload in payloads {
            let result = self
                .process_inner(payload.clone(), target, ingest, &mut dispatch)
                .await;
            if outcome.record(result).is_break() {
                break;
            }
        }
        if let Err(e) = self.send_grpc(&mut dispatch).await {
            outcome.retry_all(e);
        }
        std::mem::take(&mut dispatch.usage)
    }

    /// Inner processing logic (after backpressure check).
    #[inline]
    async fn process_inner(
        &self,
        payload: Bytes,
        target: Target<'_>,
        ingest: &Ingest,
        dispatch: &mut Dispatch<'_>,
    ) -> Result<()> {
        trace!(bytes = payload.len(), "Processing message");

        // Validation, routing and stamping parse lazily, so depth is settled first.
        depth::admit(&payload, MAX_PARSE_DEPTH, self.metrics.as_deref())?;

        // Validate (read guard dropped before any .await)
        let validation = self.validator.read().validate(&payload);

        // Before the DLQ and both targets, so no copy the receiver forwards keeps it.
        let forwarded = matches!(
            validation,
            ValidationResult::Valid | ValidationResult::Dlq(_)
        );
        let payload = if dispatch.routes.router.strips_org_id() && forwarded {
            routing::strip_org_id(payload).inspect_err(|_| {
                self.count_validation_failure(ValidationFailureReason::InvalidJson);
            })?
        } else {
            payload
        };

        match validation {
            ValidationResult::Valid => {
                trace!(bytes = payload.len(), "Message validation passed");
            }
            ValidationResult::NotJson(reason) => {
                debug!(reason = %reason, bytes = payload.len(), "Message refused, not JSON");
                self.count_validation_failure(ValidationFailureReason::InvalidJson);
                return Err(Error::Validation(reason));
            }
            ValidationResult::NotObject(reason) => {
                debug!(bytes = payload.len(), "Message refused, not a JSON object");
                self.count_validation_failure(ValidationFailureReason::NotAnObject);
                return Err(Error::Validation(reason));
            }
            ValidationResult::Dlq(reason) => {
                debug!(reason = %reason, bytes = payload.len(), "Message validation failed, routing to DLQ");
                self.count_validation_failure(ValidationFailureReason::MissingField);
                return self.send_to_dlq(&payload, &reason, ingest, dispatch).await;
            }
            ValidationResult::Reject(reason) => {
                debug!(reason = %reason, bytes = payload.len(), "Message rejected by validator");
                self.count_validation_failure(ValidationFailureReason::MissingField);
                return Err(Error::Validation(reason));
            }
        }

        match target {
            Target::Routed => self.process_routed(payload, ingest, dispatch).await,
            // The memory transport has no broker for a topic the listener picked.
            Target::Topic(_) if self.kafka.is_none() && dispatch.discards() => Ok(()),
            Target::Topic(topic) => {
                let decoded = payload.len() as u64;
                let routes = dispatch.routes;
                let (payload, org) =
                    routes
                        .router
                        .assign_org(payload, ingest, &mut dispatch.tally)?;
                self.send_to_kafka(topic, &payload, dispatch).await?;
                dispatch.usage.add(org, decoded);
                Ok(())
            }
        }
    }

    /// Route, stamp and send a validated record.
    ///
    /// The topic carries the source on the Kafka route and nothing carries it
    /// on the loader route, so the router writes `_source` into the record --
    /// dfe-loader reads it out of the data to pick the table -- and `_org_id`
    /// beside it.
    async fn process_routed(
        &self,
        payload: Bytes,
        ingest: &Ingest,
        dispatch: &mut Dispatch<'_>,
    ) -> Result<()> {
        let decoded = payload.len() as u64;
        let routes = dispatch.routes;
        let Routed {
            route,
            payload,
            org,
        } = routes
            .router
            .route(payload, ingest, now_millis(), &mut dispatch.tally)?;

        let dispatch_start = std::time::Instant::now();
        match route {
            RouteResult::Send {
                ref destinations,
                ref topic,
            } => {
                self.send_to_destinations(destinations, topic.as_deref(), &payload, dispatch)
                    .await?;
                // A dead-lettered record is not ingested, so only this arm counts usage.
                dispatch.usage.add(org, decoded);
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

    /// Record a record the validator turned away.
    fn count_validation_failure(&self, reason: ValidationFailureReason) {
        validation::record_validation_failure(self.metrics.as_deref(), "json_validate", reason);
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
            let Some(destination) = dispatch.routes.destinations.get(name.as_ref()) else {
                return Err(Error::Config(format!(
                    "destination '{name}' is not configured"
                )));
            };
            match &destination.sink {
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
    async fn send_grpc(&self, dispatch: &mut Dispatch<'_>) -> Result<()> {
        let ticket = dispatch.ticket;
        let routes = dispatch.routes;
        for (name, payloads) in std::mem::take(&mut dispatch.grpc) {
            let Some(delivery) = routes
                .destinations
                .get(name.as_ref())
                .and_then(|destination| destination.grpc())
            else {
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
        ingest: &Ingest,
        dispatch: &mut Dispatch<'_>,
    ) -> Result<()> {
        if let Some(ref dlq) = self.dlq {
            // A dead letter skips the clamp, so it keeps no `_org_id` the sender
            // wrote. One that does not parse names no org, and is kept as sent.
            let payload =
                routing::strip_org_id(payload.clone()).unwrap_or_else(|_| payload.clone());
            let entry = DlqEntry::new("receiver", reason, payload.to_vec());
            let Some(ticket) = dispatch.ticket else {
                return dlq
                    .send(entry)
                    .await
                    .map_err(|e| Error::Config(format!("DLQ send failed: {e}")));
            };
            let piece = ticket.piece();
            let written = crate::buffer::rejects::dead_letter_confirmed(dlq, entry)
                .await
                .map_err(|e| Error::Transport(format!("DLQ did not confirm the write: {e}")))?;
            match written {
                DeadLetter::Written => {
                    piece.report(DeliveryStatus::Rejected);
                    Ok(())
                }
                // No DLQ backend can hold it, so the sender hears the record is at fault.
                DeadLetter::Unwritable => {
                    piece.report(DeliveryStatus::Dropped);
                    Err(Error::Validation(reason.to_string()))
                }
            }
        } else {
            // With no DLQ configured the record follows the routing table, so a
            // brokerless deployment does not need a DLQ topic to reject a bad
            // record. It lands like any other record, so its org is held too.
            let routes = dispatch.routes;
            let (payload, _) =
                routes
                    .router
                    .assign_org(payload.clone(), ingest, &mut dispatch.tally)?;
            match routes.router.route_dlq(reason) {
                RouteResult::Dlq(topic) => self.send_to_kafka(&topic, &payload, dispatch).await,
                RouteResult::Send {
                    ref destinations,
                    ref topic,
                } => {
                    self.send_to_destinations(destinations, topic.as_deref(), &payload, dispatch)
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
        let (mut spilled, mut drained) = (0u64, 0u64);
        if let Some(ref kafka) = self.kafka
            && let Some(stats) = kafka.stats().await
        {
            total_queue += stats.queue_size as u64;
            spilled += stats.queued_total;
            drained += stats.drained_total;
            metrics.set_circuit_state(stats.circuit_state, stats.consecutive_failures);
        }

        // Named destination stats
        let routes = self.routes();
        for destination in routes.destinations.values() {
            let stats = match destination.grpc() {
                Some(delivery) => delivery.stats().await,
                None => None,
            };
            if let Some(stats) = stats {
                total_queue += stats.queue_size as u64;
                spilled += stats.queued_total;
                drained += stats.drained_total;
            }
        }

        metrics.set_batch_queue_size(total_queue);
        metrics.set_buffered_totals(spilled, drained);

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

    /// Reload configuration, rebuilding the router, the validator and the
    /// destination senders as [`rebuild_components`](Self::rebuild_components)
    /// does, then publishing the new config.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the router does not build. The routes and the
    /// published config both stay on the last good version. A destination
    /// whose sender fails to build is reported and skipped, not returned.
    pub async fn reload_config(&self, new_config: Config) -> Result<()> {
        self.rebuild_components(&new_config).await?;

        // Published only once the routes it describes are live.
        self.shared_config.update(new_config);
        security::config_changed(
            "config_reload",
            "system",
            "pipeline config reloaded (router, validator, destinations)",
        );

        let version = self.shared_config.version();
        info!(version, "Configuration reloaded successfully");
        Ok(())
    }

    /// Rebuild mutable pipeline components from new configuration.
    ///
    /// Does NOT update `SharedConfig`: the caller publishes `new_config` once
    /// this returns `Ok`, so a failed rebuild leaves the routes and the
    /// published config on the same version.
    ///
    /// The routing rules and the destination senders change together, so a
    /// rule takes effect with the sender it names. A destination the config
    /// adds gets its sender here, and one whose wiring is unchanged keeps its
    /// sender and what it has in flight. One the config removes, or whose
    /// address changes, is retired: what its buffer holds goes to it or else
    /// the DLQ, never dropped while the process runs. A sender that fails to
    /// build is reported, and its records are answered unavailable until a
    /// later reload builds it.
    ///
    /// A gRPC destination whose address changes while its buffer spools to
    /// disk moves to the new address on the same spool once no request uses
    /// its old sender, and the reload waits for that; until then its records
    /// are answered unavailable. The Kafka producer and the DLQ are built at
    /// startup only.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the router does not build. Nothing is swapped.
    pub async fn rebuild_components(&self, new_config: &Config) -> Result<()> {
        let _one_at_a_time = self.reloading.lock().await;
        let resolved = new_config.resolved_destinations();
        let router = match Router::new(new_config, &resolved) {
            Ok(router) => Arc::new(router),
            Err(e) => {
                error!(error = %e, "A config reload's router did not build; the current routes stay");
                return Err(e);
            }
        };
        let (destinations, moves) = self.rebuild_destinations(new_config, &resolved).await;
        let next = Arc::new(Routes {
            router,
            destinations,
        });

        let replaced = self.routes.swap(Arc::clone(&next));
        *self.validator.write() = Validator::new(new_config.validation.clone());

        let retired: Vec<(Arc<str>, Arc<Destination>)> = replaced
            .destinations
            .iter()
            .filter(|(name, old)| {
                !next
                    .destinations
                    .get(*name)
                    .is_some_and(|kept| Arc::ptr_eq(kept, old))
                    && !moves.iter().any(|moving| moving.name == **name)
            })
            .map(|(name, old)| (Arc::clone(name), Arc::clone(old)))
            .collect();
        // A retirement or a move waits until nothing else holds the destination, this included.
        drop(replaced);
        for (name, old) in retired {
            self.retire(name, old);
        }
        for moving in moves {
            self.move_spool(moving).await;
        }
        Ok(())
    }

    /// The destination set `config` asks for: the current sender where a
    /// destination's wiring is unchanged, a new one where it is new or changed.
    ///
    /// A rewired destination whose buffer spools to disk is left out of the
    /// set and returned as a [`SpoolMove`]: its successor can open the spool
    /// only once the current sender has let it go.
    async fn rebuild_destinations(
        &self,
        config: &Config,
        resolved: &DestinationsConfig,
    ) -> (DestinationSet, Vec<SpoolMove>) {
        let mut wanted: Vec<(Arc<str>, Result<Wiring>)> = Vec::new();
        for name in resolved.referenced_names() {
            if wanted.iter().all(|(seen, _)| seen.as_ref() != name) {
                let wiring = Wiring::of(name, resolved, config.loader.timeout_ms);
                wanted.push((Arc::from(name), wiring));
            }
        }
        let current = self.routes();
        let mut next = DestinationSet::default();
        let mut moves = Vec::new();
        let mut spools = FxHashSet::default();
        let mut to_build = Vec::new();
        for (name, wiring) in wanted {
            let wiring = match wiring {
                Ok(wiring) => wiring,
                Err(e) => {
                    report_build_failure(&name, &e);
                    continue;
                }
            };
            match current.destinations.get(&name) {
                Some(kept) if kept.wiring == wiring => {
                    if kept.grpc().is_some() {
                        spools.insert(grpc_spool(&name));
                    }
                    next.insert(name, Arc::clone(kept));
                }
                Some(kept)
                    if self.settings.spools()
                        && kept.grpc().is_some()
                        && matches!(wiring, Wiring::Grpc { .. }) =>
                {
                    // Claimed here so no destination this reload adds can take the spool.
                    spools.insert(grpc_spool(&name));
                    moves.push(SpoolMove {
                        name,
                        from: Arc::clone(kept),
                        wiring,
                    });
                }
                Some(_) | None => to_build.push((name, wiring)),
            }
        }
        drop(current);

        for (name, wiring) in to_build {
            if let Some(destination) = self.build_destination(&name, wiring, &mut spools).await {
                next.insert(name, destination);
            }
        }
        (next, moves)
    }

    /// Build destination `name` from `wiring` for a reload, its drain started
    /// once the drains run, or report why it could not be built.
    async fn build_destination(
        &self,
        name: &str,
        wiring: Wiring,
        spools: &mut FxHashSet<PathBuf>,
    ) -> Option<Arc<Destination>> {
        match Destination::build(name, wiring, &self.settings, &self.rejects, spools).await {
            Ok(destination) => {
                if let (Some(delivery), Some(drains)) = (destination.grpc(), self.drains.get()) {
                    delivery.start(drains);
                }
                info!(
                    destination = name,
                    "Destination sender built by a config reload"
                );
                Some(Arc::new(destination))
            }
            Err(e) => {
                report_build_failure(name, &e);
                None
            }
        }
    }

    /// Move a destination whose buffer spools to disk to its new wiring, on
    /// the spool it holds.
    ///
    /// A spool directory takes one sender at a time, so the successor is built
    /// only once no request uses the current sender and its spool is shut
    /// down; until then a record routed to the destination is answered
    /// unavailable. The successor replays what the current sender left
    /// spooled. A sender still in use after [`RETIRE_QUIESCE`] goes back into
    /// the routes unchanged, for a later reload to move.
    async fn move_spool(&self, moving: SpoolMove) {
        let SpoolMove { name, from, wiring } = moving;
        if !wait_unused(&from, &self.shutdown, Some(RETIRE_QUIESCE)).await {
            if !self.shutdown.is_cancelled() {
                warn!(
                    destination = %name,
                    "A destination a config reload readdressed is still in use; \
                     it keeps its current sender until a later reload"
                );
                self.install(name, from);
            }
            return;
        }
        close_spool(from).await;
        // Claimed against the rest when the reload planned the move.
        let mut spools = FxHashSet::default();
        if let Some(destination) = self.build_destination(&name, wiring, &mut spools).await {
            self.install(name, destination);
        }
    }

    /// Add `destination` to the routes as `name`. Only a reload calls this,
    /// under the reload lock, so no other swap can come between the load and
    /// the store.
    fn install(&self, name: Arc<str>, destination: Arc<Destination>) {
        let current = self.routes.load_full();
        let mut destinations = current.destinations.clone();
        destinations.insert(name, destination);
        self.routes.store(Arc::new(Routes {
            router: Arc::clone(&current.router),
            destinations,
        }));
    }

    /// Hand back, in the background, what a destination a reload replaced
    /// still holds.
    fn retire(&self, name: Arc<str>, destination: Arc<Destination>) {
        // Nothing buffered: a send in flight finishes on its own.
        if destination
            .grpc()
            .is_none_or(|delivery| delivery.queued.is_none())
        {
            return;
        }
        info!(destination = %name, "Retiring a destination sender a config reload replaced");
        let dlq = self.dlq.clone();
        let shutdown = self.shutdown.clone();
        let mut retiring = self.retiring.lock();
        while retiring.try_join_next().is_some() {}
        retiring.spawn(retire_destination(name, destination, dlq, shutdown));
    }

    /// Wait until every destination a reload retired has handed back what it
    /// held, or given up at shutdown.
    async fn retirements_settled(&self) {
        let mut retiring = std::mem::take(&mut *self.retiring.lock());
        while let Some(done) = retiring.join_next().await {
            if let Err(e) = done {
                error!(error = %e, "A destination retirement ended abnormally");
            }
        }
    }

    /// Start every buffer's drain task, and those of buffers a reload builds
    /// later, to run until `shutdown`.
    fn start_drains(&self, shutdown: &CancellationToken) {
        if let Some(ref kafka) = self.kafka {
            kafka.start(shutdown);
        }
        // Set before the walk below, so a reload either sees it or is seen.
        let drains = self.drains.get_or_init(|| shutdown.clone());
        for destination in self.routes().destinations.values() {
            if let Some(delivery) = destination.grpc() {
                delivery.start(drains);
            }
        }
    }
}

/// Now, in epoch milliseconds: a record's `_timestamp_received`.
fn now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Report a destination whose sender a reload could not build.
fn report_build_failure(name: &str, error: &Error) {
    metrics::counter!("receiver_destination_build_failures_total").increment(1);
    error!(
        destination = name,
        error = %error,
        "Could not build the sender of a destination a config reload asked for; \
         its records are answered unavailable until a reload builds it"
    );
}

/// How often a retirement checks for requests still using the destination.
const RETIRE_POLL: Duration = Duration::from_millis(10);

/// Past every send deadline and hold, so a retirement still waiting after
/// this is reported.
const RETIRE_QUIESCE: Duration = Duration::from_secs(30);

/// How long a removed destination's spool gets to drain into it before the
/// spool is closed with what it still holds.
const RETIRE_SPOOL_DRAIN: Duration = Duration::from_secs(30);

/// How often a retirement offers again what neither its destination nor the
/// DLQ took.
const RETIRE_RETRY: Duration = Duration::from_secs(5);

/// The DLQ reason for a record a retired destination could not send.
const RETIRED_REASON: &str = "destination retired by a config reload";

/// Wait until no request uses `destination`: true once nothing else holds it,
/// false at `shutdown` or once `limit`, where given, has passed.
async fn wait_unused(
    destination: &Arc<Destination>,
    shutdown: &CancellationToken,
    limit: Option<Duration>,
) -> bool {
    let mut waited = Duration::ZERO;
    while Arc::strong_count(destination) > 1 {
        if shutdown.is_cancelled() || limit.is_some_and(|limit| waited >= limit) {
            return false;
        }
        tokio::select! {
            () = shutdown.cancelled() => {}
            () = tokio::time::sleep(RETIRE_POLL) => {}
        }
        waited += RETIRE_POLL;
    }
    true
}

/// Shut down the disk spool of `destination`, the last handle on it, so its
/// directory is free for the next sink opened on it, which replays what the
/// spool still holds.
async fn close_spool(destination: Arc<Destination>) {
    let Some(Destination {
        sink: DestinationSink::Grpc(delivery),
        ..
    }) = Arc::into_inner(destination)
    else {
        return;
    };
    if let Some(SinkBackend::Tiered(spool)) = delivery.queued.and_then(Arc::into_inner) {
        spool.shutdown().await;
    }
}

/// Retire destination `name` once no request uses it: stop its buffer's
/// drain, then hand what an in-memory buffer still holds to the destination,
/// else the DLQ, or give a disk spool time to drain and shut it down.
///
/// A record its buffer held was answered at enqueue, so its sender has no
/// copy: what neither the destination nor the DLQ takes is offered again until
/// `shutdown`. What a spool still holds stays on disk.
async fn retire_destination(
    name: Arc<str>,
    destination: Arc<Destination>,
    dlq: Option<Arc<Dlq>>,
    shutdown: CancellationToken,
) {
    if !wait_unused(&destination, &shutdown, Some(RETIRE_QUIESCE)).await && !shutdown.is_cancelled()
    {
        warn!(destination = %name, "A retired destination is still in use; waiting for it");
        wait_unused(&destination, &shutdown, None).await;
    }
    let Some(delivery) = destination.grpc() else {
        return;
    };
    delivery.stop_drain().await;
    match delivery.queued.as_deref() {
        Some(SinkBackend::InMemory(buffer)) => {
            let held = buffer.take_queued().into_iter().map(|(_, p)| p).collect();
            hand_back(&name, &delivery.primary, held, dlq.as_deref(), &shutdown).await;
        }
        Some(SinkBackend::Tiered(spool)) => {
            let deadline = tokio::time::Instant::now() + RETIRE_SPOOL_DRAIN;
            while !spool.spool_is_empty().await
                && tokio::time::Instant::now() < deadline
                && !shutdown.is_cancelled()
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let left = spool.spool_len().await;
            if left > 0 {
                count_retired("spooled", left);
                warn!(
                    destination = %name,
                    records = left,
                    "A retired destination's spool did not drain; its records stay on disk \
                     and replay when a destination of this name is configured again"
                );
            }
        }
        None => {}
    }
    close_spool(destination).await;
}

/// Send `held` straight to the retired destination, past its buffer's
/// circuit, and dead-letter what it does not take, until shutdown.
async fn hand_back(
    name: &str,
    primary: &GrpcSink,
    mut held: Vec<Bytes>,
    dlq: Option<&Dlq>,
    shutdown: &CancellationToken,
) {
    loop {
        if held.is_empty() {
            return;
        }
        match primary.send_batch("", &held).await {
            Ok(()) => {
                count_retired("sent", held.len());
                return;
            }
            Err(e) => {
                debug!(destination = name, error = %e, "A retired destination did not take its records")
            }
        }
        if let Some(dlq) = dlq {
            held = dead_letter_retired(name, held, dlq).await;
            if held.is_empty() {
                return;
            }
        }
        warn!(
            destination = name,
            records = held.len(),
            "Neither a retired destination nor the DLQ took its records; offering them again"
        );
        tokio::select! {
            () = shutdown.cancelled() => {
                count_retired("dropped", held.len());
                error!(
                    destination = name,
                    records = held.len(),
                    "Shutting down with records a retired destination could not hand back; dropped them"
                );
                return;
            }
            () = tokio::time::sleep(RETIRE_RETRY) => {}
        }
    }
}

/// Dead-letter `held`, returning what the DLQ did not take.
async fn dead_letter_retired(name: &str, held: Vec<Bytes>, dlq: &Dlq) -> Vec<Bytes> {
    let mut rest = held.into_iter();
    while let Some(payload) = rest.next() {
        let entry =
            DlqEntry::new("receiver", RETIRED_REASON, payload.to_vec()).with_destination(name);
        match crate::buffer::rejects::dead_letter_confirmed(dlq, entry).await {
            Ok(DeadLetter::Written) => count_retired("dead_lettered", 1),
            // Counted in pipeline_dead_letters_dropped_total, and logged, as it was refused.
            Ok(DeadLetter::Unwritable) => count_retired("dropped", 1),
            Err(e) => {
                debug!(destination = name, error = %e, "The DLQ did not take a retired destination's record");
                return std::iter::once(payload).chain(rest).collect();
            }
        }
    }
    Vec::new()
}

/// Count `records` a retired destination held under `outcome`.
fn count_retired(outcome: &'static str, records: usize) {
    metrics::counter!("receiver_retired_records_total", "outcome" => outcome)
        .increment(records as u64);
}

/// The scalo DLQ config, and the Kafka client config its Kafka backend is
/// built on.
///
/// A receiver with no brokers delivers over gRPC alone, so its DLQ gets no
/// Kafka backend and no Kafka client config: dead letters go to the file
/// backend. A Kafka backend there queues every entry for a broker nobody
/// configured, and scalo's cascade never hands a queued entry on to the file
/// backend.
///
/// # Errors
///
/// [`Error::Config`] when the Kafka config names an unknown provider or a
/// transport scalo refuses.
fn dlq_backends(
    config: &Config,
) -> Result<(scalo::dlq::DlqConfig, Option<scalo::transport::KafkaConfig>)> {
    let mut dlq = config.routing.dlq.to_scalo_config();
    if config.kafka.brokers.is_empty() {
        dlq.kafka.enabled = false;
        return Ok((dlq, None));
    }
    Ok((dlq, Some(config.kafka.to_scalo_kafka_config()?)))
}

/// The DLQ backend whose start failed, for `receiver_dlq_start_failures_total`.
fn dlq_start_failure_backend(err: &DlqError) -> &'static str {
    match err {
        DlqError::Kafka(_) => "kafka",
        DlqError::File(_) | DlqError::Io(_) => "file",
        _ => "other",
    }
}

/// Start the DLQ the config asks for, or `None` when it is disabled or fails
/// to start.
///
/// A DLQ that fails to start leaves the receiver running without one, since
/// a transport failure never stops the app.
///
/// # Errors
///
/// [`Error::Config`] when the Kafka config is refused, before any client is
/// built: the main producer shares it and would be refused next.
fn spawn_dlq(config: &Config, shutdown: &CancellationToken) -> Result<Option<Arc<Dlq>>> {
    if !config.routing.dlq.enabled {
        debug!("DLQ disabled by config");
        return Ok(None);
    }
    let (dlq_config, kafka) = dlq_backends(config)?;
    match Dlq::spawn(&dlq_config, "receiver", kafka.as_ref(), shutdown.clone()) {
        Ok(d) => {
            info!(
                mode = ?dlq_config.mode,
                kafka_backend = dlq_config.kafka.enabled,
                file_backend = dlq_config.file.enabled,
                "DLQ enabled"
            );
            Ok(Some(Arc::new(d)))
        }
        Err(e) => {
            let backend = dlq_start_failure_backend(&e);
            metrics::counter!("receiver_dlq_start_failures_total", "backend" => backend)
                .increment(1);
            error!(
                error = %e,
                backend,
                "DLQ failed to start; running without one, so a record a destination \
                 refuses for good is dropped"
            );
            Ok(None)
        }
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

        self.state.start_drains(&self.shutdown);

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

    /// Reload the config from `config_path` on a file change, a SIGHUP or every
    /// `reload_secs` (0 for never), and rebuild the routes and destinations
    /// from each reload that validates. The rules and bindings change with no
    /// restart.
    ///
    /// The reloader publishes to a staging copy. The config every reader sees
    /// moves only once the routes it describes are live, so a rebuild that
    /// fails leaves both on the last good version.
    pub fn watch_config(&self, config_path: Option<&str>, reload_secs: u64) {
        use scalo::config::reloader::{ConfigReloader, ReloaderConfig};

        type BoxError = Box<dyn std::error::Error + Send + Sync>;
        let path = config_path.map(String::from);
        let reloader_config = ReloaderConfig {
            config_path: path.as_ref().map(PathBuf::from),
            poll_interval: Duration::from_secs(reload_secs.max(5)),
            periodic_interval: if reload_secs > 0 {
                Duration::from_secs(reload_secs)
            } else {
                Duration::ZERO
            },
            debounce: Duration::from_millis(500),
            enable_sighup: true,
        };
        let placeholder = Config {
            config_path: path,
            ..Config::default()
        };
        let staged = SharedConfig::new(self.shared_config.get());
        let reloader = ConfigReloader::new(
            reloader_config,
            staged.clone(),
            move || crate::config::reload_config(&placeholder).map_err(|e| Box::new(e) as BoxError),
            |config: &Config| config.validate().map_err(|e| Box::new(e) as BoxError),
        );
        let _reloading = reloader.start();

        let state = Arc::clone(&self.state);
        let mut changes = staged.subscribe();
        tokio::spawn(async move {
            while changes.changed().await.is_ok() {
                let config = staged.get();
                if let Err(e) = state.reload_config(config).await {
                    warn!(error = %e, "Config reload refused; the last good config stays");
                }
            }
        });
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

        let routes = self.state.routes();
        for (name, destination) in &routes.destinations {
            if let Some(delivery) = destination.grpc()
                && let Err(e) = delivery.flush().await
            {
                error!(error = %e, destination = %name, "Failed to flush destination sink");
            }
        }
        drop(routes);
        self.state.retirements_settled().await;

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
    use crate::metrics::testing::CounterTotals;
    use crate::tenancy::Principal;

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

    /// A sender's records over an HTTP listener with no credential.
    fn http() -> Ingest {
        Ingest::sent(Principal::Listener("http"))
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
                        ..crate::config::GrpcDestination::default()
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
        let routes = state.routes();
        let Some(delivery) = routes.destinations.get("alpha").and_then(|d| d.grpc()) else {
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
                &http(),
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
            .process_batch_acked(&[Bytes::from(r#"{"a":1}"#)], &acks, None, &http())
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

    /// A record answered at enqueue keeps librdkafka's own message timeout:
    /// beside a held listener it gets a producer of its own, since a held
    /// timeout would give it up after 20 s with nothing to resend it.
    #[tokio::test]
    async fn records_answered_at_enqueue_never_run_on_the_held_timeout() {
        let held = Some(acks::HELD_MESSAGE_TIMEOUT);
        let mut config = test_config();
        unroutable_bus(&mut config);

        // The default: every listener holds, so one producer on the held timeout.
        let state = test_state_with(config.clone()).await;
        let kafka = state.kafka.as_ref().unwrap();
        assert_eq!(kafka.primary.held_message_timeout(), held);
        assert!(kafka.queued.is_none() && kafka.enqueue_sink.is_none());

        // Syslog answers at enqueue beside the held HTTP listener.
        config.syslog.enabled = true;
        let state = test_state_with(config.clone()).await;
        let kafka = state.kafka.as_ref().unwrap();
        assert_eq!(kafka.primary.held_message_timeout(), held);
        assert_eq!(
            kafka
                .enqueue_sink
                .as_ref()
                .map(|sink| sink.held_message_timeout()),
            Some(None),
            "the buffer sends through a producer on librdkafka's own timeout"
        );

        // Nothing holds, so one producer on librdkafka's own timeout.
        config.server.acknowledgements = AcknowledgementsConfig::new(false);
        let state = test_state_with(config).await;
        let kafka = state.kafka.as_ref().unwrap();
        assert_eq!(kafka.primary.held_message_timeout(), None);
        assert!(kafka.enqueue_sink.is_none());
    }

    /// Held, a record no broker confirms answers retryable; at enqueue the
    /// same record is taken once librdkafka queues it.
    #[tokio::test]
    async fn a_held_answer_waits_for_the_kafka_delivery_report() {
        let mut config = test_config();
        unroutable_bus(&mut config);
        let state = test_state_with(config).await;
        let record = [Bytes::from(r#"{"a":1}"#)];

        let at_enqueue = state.process_batch(&record, &http()).await;
        assert_eq!(at_enqueue.accepted, 1);
        assert!(at_enqueue.unavailable.is_none());

        let held = tokio::time::timeout(
            Duration::from_secs(20),
            state.process_batch_acked(&record, &holding(&state), None, &http()),
        )
        .await
        .expect("the delivery report settles the request inside its hold");
        assert!(
            held.unavailable.as_ref().is_some_and(Error::is_retryable),
            "a record no broker confirmed was answered as taken: {held:?}"
        );
        assert_eq!(held.settled(), 0);
    }

    /// A validation DLQ writing to files under `dir`, for records missing `org_id`.
    fn file_dlq(config: &mut Config, dir: &std::path::Path) {
        config.routing.dlq = crate::config::DlqConfig {
            enabled: true,
            mode: "file_only".to_string(),
            file_path: dir.display().to_string(),
            kafka_enabled: false,
            ..crate::config::DlqConfig::default()
        };
        // JSON missing a required field is the one record validation dead-letters.
        config.validation.required_fields = vec!["org_id".to_string()];
    }

    /// A record `file_dlq` dead-letters.
    fn missing_org_id() -> Bytes {
        Bytes::from(r#"{"a":1}"#)
    }

    /// Held, a dead-lettered record counts once the DLQ confirmed the write.
    #[tokio::test]
    async fn a_dead_letter_the_dlq_confirms_is_taken() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        file_dlq(&mut config, dir.path());
        let state = test_state_with(config).await;

        let outcome = state
            .process_batch_acked(&[missing_org_id()], &holding(&state), None, &http())
            .await;

        assert!(outcome.unavailable.is_none(), "{:?}", outcome.unavailable);
        assert_eq!(outcome.accepted, 1);
        state.dlq.as_ref().unwrap().flush().await.unwrap();
        let written =
            std::fs::read_to_string(dir.path().join("receiver").join("dlq.ndjson")).unwrap();
        assert!(
            written.contains("missing required field: org_id"),
            "the DLQ does not hold the record: {written:?}"
        );
    }

    /// A dead letter drops a sender's `_org_id`, the same as a delivered record.
    #[tokio::test]
    async fn a_dead_letter_drops_a_sender_org_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        file_dlq(&mut config, dir.path());
        let state = test_state_with(config).await;
        let spoofed = Bytes::from(r#"{"a":1,"_org_id":"another-org"}"#);

        let outcome = state
            .process_batch_acked(&[spoofed], &holding(&state), None, &http())
            .await;

        assert_eq!(outcome.accepted, 1);
        state.dlq.as_ref().unwrap().flush().await.unwrap();
        let written =
            std::fs::read_to_string(dir.path().join("receiver").join("dlq.ndjson")).unwrap();
        let entry: DlqEntry = serde_json::from_str(written.lines().next().unwrap()).unwrap();
        let record: serde_json::Value = serde_json::from_slice(&entry.payload).unwrap();
        assert_eq!(record, serde_json::json!({"a": 1}));
    }

    /// Under `allow`, a dead letter skips the clamp, so it leaves with no
    /// `_org_id` the sender wrote, however the key is spelled.
    #[tokio::test]
    async fn under_allow_a_dead_letter_drops_a_sender_org_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        file_dlq(&mut config, dir.path());
        config.routing.source_org_id = crate::config::SourceOrgId::Allow;
        let state = test_state_with(config).await;
        let spoofed = [
            Bytes::from(r#"{"a":1,"_org_id":"another-org"}"#),
            Bytes::from("{\"a\":1,\"\\u005forg_id\":\"another-org\"}"),
        ];

        let outcome = state
            .process_batch_acked(&spoofed, &holding(&state), None, &http())
            .await;

        assert_eq!(outcome.accepted, 2);
        state.dlq.as_ref().unwrap().flush().await.unwrap();
        let written =
            std::fs::read_to_string(dir.path().join("receiver").join("dlq.ndjson")).unwrap();
        let records: Vec<serde_json::Value> = written
            .lines()
            .map(|line| {
                let entry: DlqEntry = serde_json::from_str(line).unwrap();
                serde_json::from_slice(&entry.payload).unwrap()
            })
            .collect();
        assert_eq!(records, vec![serde_json::json!({"a": 1}); 2], "{written}");
    }

    /// A dead letter whose `_org_id` cannot be removed still reaches the DLQ,
    /// as sent: it does not parse, so no org it names can be read from it.
    #[tokio::test]
    async fn under_allow_a_dead_letter_that_cannot_be_stripped_is_kept_as_sent() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        file_dlq(&mut config, dir.path());
        config.routing.source_org_id = crate::config::SourceOrgId::Allow;
        let state = test_state_with(config).await;
        let unparsable = Bytes::from(r#"{"_org_id":"another-org","s":"\ud800"}"#);

        let outcome = state
            .process_batch_acked(
                std::slice::from_ref(&unparsable),
                &holding(&state),
                None,
                &http(),
            )
            .await;

        assert_eq!(outcome.accepted, 1, "{:?}", outcome.unavailable);
        state.dlq.as_ref().unwrap().flush().await.unwrap();
        let written =
            std::fs::read_to_string(dir.path().join("receiver").join("dlq.ndjson")).unwrap();
        let entry: DlqEntry = serde_json::from_str(written.lines().next().unwrap()).unwrap();
        assert_eq!(entry.payload, unparsable.to_vec());
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
            .process_batch_acked(&[missing_org_id()], &holding(&state), None, &http())
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

    /// With no brokers the receiver delivers over gRPC alone. A Kafka DLQ
    /// backend there queues every entry for a broker nobody configured, and
    /// scalo's cascade never hands a queued entry on to the file backend.
    #[tokio::test]
    async fn a_brokerless_dead_letter_lands_in_the_file_dlq_and_no_kafka_client_is_built() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        grpc_destinations(&mut config, &["loader_leg"]);
        config.routing.dlq.file_path = dir.path().display().to_string();
        config.validation.required_fields = vec!["org_id".to_string()];
        assert_eq!(config.kafka.brokers, [] as [std::string::String; 0]);
        assert_eq!(config.routing.dlq.mode, "cascade", "the shipped default");
        assert!(config.routing.dlq.kafka_enabled, "the shipped default");

        let (dlq_config, kafka) = dlq_backends(&config).unwrap();
        assert!(
            kafka.is_none(),
            "a Kafka client config reached Dlq::spawn with no brokers"
        );
        assert!(
            !dlq_config.kafka.enabled,
            "the brokerless DLQ asked for a Kafka backend"
        );

        let state = test_state_with(config).await;
        let outcome = state
            .process_batch_acked(&[missing_org_id()], &holding(&state), None, &http())
            .await;

        assert!(outcome.unavailable.is_none(), "{:?}", outcome.unavailable);
        assert_eq!(outcome.accepted, 1);
        let written =
            std::fs::read_to_string(dir.path().join("receiver").join("dlq.ndjson")).unwrap();
        assert!(
            written.contains("missing required field: org_id"),
            "the dead letter is not in the file DLQ: {written:?}"
        );
    }

    #[test]
    fn a_receiver_with_brokers_keeps_its_kafka_dlq_backend() {
        let mut config = test_config();
        unroutable_bus(&mut config);

        let (dlq_config, kafka) = dlq_backends(&config).unwrap();
        assert!(dlq_config.kafka.enabled);
        assert!(
            kafka.is_some(),
            "the Kafka DLQ backend lost its client config"
        );
    }

    /// A bus config sending a PLAIN password over a plaintext transport.
    fn plain_over_plaintext() -> Config {
        let mut config = test_config();
        unroutable_bus(&mut config);
        config.kafka.sasl = Some(crate::config::SaslConfig {
            enabled: true,
            mechanism: "PLAIN".to_string(),
            username: "dfe".to_string(),
            password: scalo::SensitiveString::new("secret"),
        });
        config
    }

    /// A Kafka config scalo refuses stops the build before any Kafka client
    /// exists: the DLQ, built first, never gets a client config.
    #[tokio::test]
    async fn a_refused_kafka_config_stops_the_build_before_the_dlq() {
        let config = plain_over_plaintext();
        assert!(
            config.routing.dlq.enabled && config.routing.dlq.kafka_enabled,
            "the shipped default"
        );

        assert!(
            matches!(dlq_backends(&config), Err(Error::Config(_))),
            "the DLQ got a client config for a refused transport"
        );
        let built = PipelineState::new(SharedConfig::new(config), CancellationToken::new()).await;
        let Err(Error::Config(message)) = built else {
            panic!("expected a config refusal, got {:?}", built.err());
        };
        assert!(message.contains("SASL PLAIN"), "{message}");
    }

    /// A config whose DLQ cannot start: its file backend's directory would sit
    /// under a regular file.
    fn unstartable_dlq(dir: &Path) -> Config {
        let blocker = dir.join("not-a-directory");
        std::fs::write(&blocker, b"").unwrap();
        let mut config = test_config();
        config.routing.dlq = crate::config::DlqConfig {
            mode: "file_only".to_string(),
            file_path: blocker.display().to_string(),
            kafka_enabled: false,
            ..crate::config::DlqConfig::default()
        };
        config
    }

    /// A DLQ that fails to start is counted by the backend that failed and
    /// logged at error with the reason.
    #[test]
    fn a_dlq_that_fails_to_start_is_counted_and_logged_at_error() {
        let dir = tempfile::tempdir().unwrap();
        let config = unstartable_dlq(dir.path());
        let recorder = crate::buffer::rejects::CountedKeys::default();
        let lines = crate::metrics::testing::LogLines::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(lines.clone())
            .finish();

        let dlq = tracing::subscriber::with_default(subscriber, || {
            metrics::with_local_recorder(&recorder, || {
                spawn_dlq(&config, &CancellationToken::new())
            })
        });

        assert!(
            dlq.expect("a DLQ that fails to start is not fatal")
                .is_none()
        );
        assert_eq!(
            recorder.get("receiver_dlq_start_failures_total{backend=file}"),
            1
        );
        let log = lines.text();
        assert!(log.contains("ERROR"), "{log}");
        assert!(!log.contains("WARN"), "{log}");
        assert!(log.contains("DLQ failed to start"), "{log}");
        assert!(
            log.contains("failed to create DLQ writer"),
            "the reason is not in the log: {log}"
        );
    }

    /// The receiver builds and takes records with a DLQ that failed to start.
    #[tokio::test]
    async fn a_dlq_that_fails_to_start_leaves_the_receiver_running() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state_with(unstartable_dlq(dir.path())).await;

        assert!(state.dlq.is_none());
        state
            .process(Bytes::from(r#"{"id":1}"#), &http())
            .await
            .unwrap();
    }

    /// An archived source's records also reach the archiver, whose direct
    /// listener answers on receipt, so every listener confirms only as far as
    /// that leg does. The other legs confirming does not lift it.
    #[tokio::test]
    async fn an_archiver_leg_that_answers_on_receipt_sets_the_guarantee() {
        let mut config = test_config();
        grpc_destinations(&mut config, &["loader_leg", "archiver"]);
        let state = test_state_with(config.clone()).await;
        assert_eq!(
            state.sink_confirmation,
            SinkConfirmation::Remote,
            "every leg confirms"
        );

        if let Some(grpc) = config
            .destinations
            .named
            .get_mut("archiver")
            .and_then(|spec| spec.grpc.as_mut())
        {
            grpc.confirms_delivery = false;
        }
        let state = test_state_with(config).await;
        assert_eq!(state.sink_confirmation, SinkConfirmation::None);
    }

    /// Held, a record too large for the topic and for a Kafka-only DLQ is
    /// answered refused for good: retrying would be refused the same, for ever.
    #[tokio::test]
    async fn a_record_too_large_for_the_bus_and_its_dlq_is_answered_not_retried() {
        let mut config = test_config();
        unroutable_bus(&mut config);
        config.routing.dlq = crate::config::DlqConfig {
            enabled: true,
            mode: "kafka_only".to_string(),
            file_enabled: false,
            ..crate::config::DlqConfig::default()
        };
        let state = test_state_with(config).await;
        let record = Bytes::from(format!(
            r#"{{"pad":"{}"}}"#,
            "x".repeat(scalo::transport::kafka::MESSAGE_MAX_BYTES as usize)
        ));

        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            state.process_batch_acked(&[record], &holding(&state), None, &http()),
        )
        .await
        .expect("answered without waiting on the DLQ");

        assert!(outcome.unavailable.is_none(), "{:?}", outcome.unavailable);
        assert_eq!(outcome.rejected, 1, "refused for good, not a retry");
        assert!(
            outcome
                .into_error()
                .is_some_and(|e| matches!(e, Error::Rejected(_)))
        );
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
                    ..crate::config::GrpcDestination::default()
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
        let mut config = test_config();
        config.validation.required_fields = vec!["org_id".to_string()];
        let state = test_state_with(config).await;

        // A record missing a required field with dlq_on_invalid=true attempts DLQ
        // routing, but no Kafka/DLQ sink is configured in test state -- expect
        // config error from the fallback path
        let result = state.process(Bytes::from(r#"{"a":1}"#), &http()).await;
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

    /// A body that is not JSON is refused for good and counted, never sent to
    /// the DLQ, whatever `dlq_on_invalid` says.
    #[tokio::test]
    async fn a_body_that_is_not_json_is_refused_and_counted() {
        let config = test_config();
        assert!(config.validation.dlq_on_invalid, "the default dead-letters");
        let metrics = Arc::new(Metrics::default());
        let state = PipelineState::build(
            SharedConfig::new(config),
            CancellationToken::new(),
            None,
            None,
            Some(Arc::clone(&metrics)),
        )
        .await
        .unwrap();

        // MessagePack for {"foo": 1}: DFE takes JSON only.
        let msgpack = Bytes::from_static(&[0x81, 0xA3, b'f', b'o', b'o', 0x01]);
        for body in [Bytes::from("not json"), msgpack] {
            let err = state
                .process(body.clone(), &http())
                .await
                .expect_err("not JSON");
            assert!(matches!(err, Error::Validation(_)), "{err}");
            assert!(!err.is_retryable(), "a sender must not retry it");
            let outcome = state
                .process_batch_to_topic(&[body], "t_land", &holding(&state), &http())
                .await;
            assert_eq!(outcome.rejected, 1, "{outcome:?}");
            assert!(outcome.unavailable.is_none(), "{:?}", outcome.unavailable);
            assert!(
                matches!(outcome.first_rejection, Some(Error::Validation(_))),
                "{:?}",
                outcome.first_rejection
            );
        }
        assert_eq!(metrics.get_validation_failures_total(), 4);
    }

    /// A record that is JSON but not an object is refused for good and counted
    /// on both targets.
    #[tokio::test]
    async fn a_record_that_is_not_an_object_is_refused_and_counted() {
        let metrics = Arc::new(Metrics::default());
        let state = PipelineState::build(
            SharedConfig::new(test_config()),
            CancellationToken::new(),
            None,
            None,
            Some(Arc::clone(&metrics)),
        )
        .await
        .unwrap();

        let records = [
            Bytes::from_static(br#"[{"_org_id":"victim"}]"#),
            Bytes::from_static(b"[]"),
            Bytes::from_static(b"5"),
            Bytes::from_static(br#""text""#),
            Bytes::from_static(b"null"),
        ];
        for record in &records {
            let err = state
                .process(record.clone(), &http())
                .await
                .expect_err("not an object");
            assert!(matches!(err, Error::Validation(_)), "{err}");
            assert!(!err.is_retryable(), "a sender must not retry it");
            let outcome = state
                .process_batch_to_topic(
                    std::slice::from_ref(record),
                    "t_land",
                    &holding(&state),
                    &http(),
                )
                .await;
            assert_eq!(outcome.rejected, 1, "{outcome:?}");
            assert_eq!(outcome.accepted, 0, "{outcome:?}");
        }
        assert_eq!(
            metrics.get_validation_failures_total(),
            2 * records.len() as u64
        );
    }

    /// A flood of refused records writes one security line per reason, counts
    /// every record, and puts none of the records' bytes in the log.
    #[tokio::test(flavor = "current_thread")]
    async fn a_flood_of_refused_records_logs_one_line_per_reason_and_no_record_bytes() {
        const RECORDS: u64 = 5;
        let metrics = Arc::new(Metrics::default());
        let state = PipelineState::build(
            SharedConfig::new(test_config()),
            CancellationToken::new(),
            None,
            None,
            Some(Arc::clone(&metrics)),
        )
        .await
        .unwrap();
        for logged in &crate::validation::VALIDATION_FAILURE_LOGGED {
            logged.store(0, std::sync::atomic::Ordering::Relaxed);
        }
        let lines = crate::metrics::testing::LogLines::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(lines.clone())
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _default = tracing::subscriber::set_default(subscriber);

        // The parser quotes the bytes around the fault, so this refusal's reason carries XYZZY.
        let not_json = Bytes::from_static(br#"{"k":XYZZY}"#);
        let too_deep =
            Bytes::from("[".repeat(MAX_PARSE_DEPTH + 1) + &"]".repeat(MAX_PARSE_DEPTH + 1));
        for _ in 0..RECORDS {
            let err = state
                .process(not_json.clone(), &http())
                .await
                .expect_err("not JSON");
            assert!(err.to_string().contains("XYZZY"), "{err}");
            let err = state
                .process(too_deep.clone(), &http())
                .await
                .expect_err("too deep");
            assert!(matches!(err, Error::Validation(_)), "{err}");
        }

        let log = lines.text();
        assert_eq!(
            log.lines().count(),
            2,
            "one line per reason for the whole flood: {log}"
        );
        assert!(log.contains("invalid_json"), "{log}");
        assert!(log.contains("nesting_too_deep"), "{log}");
        assert!(
            !log.contains("XYZZY"),
            "a record's bytes reached the log: {log}"
        );
        assert_eq!(metrics.get_validation_failures_total(), 2 * RECORDS);
    }

    #[tokio::test]
    async fn test_pipeline_valid_json() {
        let state = test_state().await;

        let valid_json = Bytes::from(r#"{"test": "data"}"#);
        let result = state.process(valid_json, &http()).await;
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

        let _ = state.process(Bytes::from(vec![b'x'; 4096]), &http()).await;

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

        let ingest = http();
        let mut inflight = Box::pin(state.process(payload, &ingest));
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

    /// `record` as the routes `state` holds now write it.
    fn routed(state: &PipelineState, record: &'static str) -> serde_json::Value {
        let routes = state.routes();
        let mut tally = routes.router.tally();
        let written = routes
            .router
            .route(Bytes::from(record), &http(), 1_760_000_000_000, &mut tally)
            .unwrap()
            .payload;
        serde_json::from_slice(&written).unwrap()
    }

    #[tokio::test]
    async fn a_record_takes_the_receivers_own_receive_time() {
        let state = test_state().await;

        let stamped = routed(&state, r#"{"key": "value"}"#);
        assert_eq!(stamped[routing::RECEIVED_FIELD], 1_760_000_000_000_u64);
        assert_eq!(stamped["key"], "value");
        assert!(stamped.get("_timestamp_receiver").is_none());

        let relayed = routed(&state, r#"{"_timestamp_received": 5}"#);
        assert_eq!(relayed[routing::RECEIVED_FIELD], 1_760_000_000_000_u64);
        assert_eq!(
            relayed["_timestamp_received_original"], 5,
            "a sender's receive time is kept beside the receiver's own"
        );
    }

    #[tokio::test]
    async fn with_the_common_header_off_no_receive_time_is_written() {
        let mut config = test_config();
        config.server.auth.include_common_header = false;
        let state = test_state_with(config).await;

        let record = routed(&state, "{}");
        assert!(record.get(routing::RECEIVED_FIELD).is_none(), "{record}");
        assert_eq!(record[routing::ORG_FIELD], "default");
    }

    #[tokio::test]
    async fn test_reload_config_updates_version() {
        let state = test_state().await;
        assert_eq!(state.shared_config().version(), 0);

        let mut new_config = test_config();
        new_config.routing.default_source = "reloaded".to_string();
        state.reload_config(new_config).await.unwrap();

        assert_eq!(state.shared_config().version(), 1);
        assert_eq!(state.config().routing.default_source, "reloaded");
    }

    #[tokio::test]
    async fn test_reload_config_toggles_enrichment() {
        let state = test_state().await;
        assert!(routed(&state, "{}").get(routing::RECEIVED_FIELD).is_some());

        // Disable enrichment via reload
        let mut new_config = test_config();
        new_config.server.auth.include_common_header = false;
        state.reload_config(new_config).await.unwrap();

        assert!(routed(&state, "{}").get(routing::RECEIVED_FIELD).is_none());

        // Re-enable via another reload
        let mut re_enable = test_config();
        re_enable.server.auth.include_common_header = true;
        state.reload_config(re_enable).await.unwrap();

        assert!(routed(&state, "{}").get(routing::RECEIVED_FIELD).is_some());
    }

    /// A reload that edits or reorders the rules routes the next record by
    /// the new rows, with no restart.
    #[tokio::test]
    async fn a_reload_routes_by_the_edited_and_reordered_rules() {
        use scalo::rules::{MatchConfig, RuleConfig, SetAction};

        let row = |id: &str, field: &str, value: &str, target: &str, set: &str| {
            RuleConfig::new(id, target, SetAction::value(set))
                .matching(MatchConfig::equals(field, value))
        };
        let mut config = test_config();
        config.rules.rules = vec![
            row("by-app", "app", "pay", "_source", "payments"),
            row("by-host", "host", "h1", "_source", "hosts"),
        ];
        let state = test_state_with(config.clone()).await;
        let record = r#"{"app":"pay","host":"h1"}"#;
        assert_eq!(routed(&state, record)["_source"], "payments");

        config.rules.rules.reverse();
        state.reload_config(config.clone()).await.unwrap();
        assert_eq!(
            routed(&state, record)["_source"],
            "hosts",
            "the reorder did not land"
        );

        config.rules.rules[0] = row("by-host", "host", "h1", "_source", "edited");
        state.reload_config(config).await.unwrap();
        assert_eq!(
            routed(&state, record)["_source"],
            "edited",
            "the edit did not land"
        );
    }

    /// Rules that do not compile never replace the running rules.
    #[tokio::test]
    async fn a_reload_with_rules_that_do_not_compile_keeps_the_running_rules() {
        use scalo::rules::{RuleConfig, SetAction};

        let mut config = test_config();
        config.rules.rules = vec![RuleConfig::new("all", "_source", SetAction::value("kept"))];
        let state = test_state_with(config.clone()).await;

        config.rules.rules = vec![RuleConfig::new("bad id!", "_source", SetAction::value("x"))];
        assert!(
            routing::compile_rules(&config).is_err(),
            "the reloader's validate step refuses it"
        );
        let version = state.shared_config().version();
        assert!(state.reload_config(config).await.is_err());
        assert_eq!(routed(&state, "{}")["_source"], "kept");
        // The published config stays with the routes, never ahead of them.
        assert_eq!(state.shared_config().version(), version);
        assert_eq!(state.config().rules.rules[0].id, "all");
    }

    #[tokio::test]
    async fn test_reload_config_while_processing() {
        let state = Arc::new(test_state().await);

        // Process a message before reload
        let result = state.process(Bytes::from(r#"{"a": 1}"#), &http()).await;
        assert!(result.is_ok());

        // Reload config with different default source
        let mut new_config = test_config();
        new_config.routing.default_source = "updated".to_string();
        state.reload_config(new_config).await.unwrap();

        // Process a message after reload -- should still work
        let result = state.process(Bytes::from(r#"{"b": 2}"#), &http()).await;
        assert!(result.is_ok());

        // Verify config actually changed
        assert_eq!(state.config().routing.default_source, "updated");
    }

    #[tokio::test]
    async fn test_reload_config_subscriber_notified() {
        let state = test_state().await;
        let mut rx = state.shared_config().subscribe();

        let new_config = test_config();
        state.reload_config(new_config).await.unwrap();

        rx.changed().await.expect("should receive notification");
        assert_eq!(*rx.borrow(), 1);
    }

    /// A reload that leaves a destination's wiring alone keeps its sender, so
    /// nothing reconnects and nothing in flight is cut off.
    #[tokio::test]
    async fn a_reload_keeps_the_sender_of_an_unchanged_destination() {
        let mut config = test_config();
        grpc_destinations(&mut config, &["alpha"]);
        let state = test_state_with(config.clone()).await;
        let before = Arc::clone(state.routes().destinations.get("alpha").unwrap());

        config.routing.default_source = "reloaded".to_string();
        state.reload_config(config).await.unwrap();

        let routes = state.routes();
        assert!(
            Arc::ptr_eq(&before, routes.destinations.get("alpha").unwrap()),
            "an unchanged destination was rebuilt"
        );
    }

    /// A destination a reload readdresses while its buffer spools to disk
    /// gets a sender at the new address, spooling to the same directory, with
    /// no restart and no stalled reload.
    #[tokio::test]
    async fn a_readdressed_destination_that_spools_moves_to_its_new_address() {
        let totals = CounterTotals::default();
        let _recorder = metrics::set_default_local_recorder(&totals);
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        grpc_destinations(&mut config, &["alpha"]);
        spill_to(&mut config, dir.path());
        // An at-enqueue listener is what builds the spool.
        config.server.acknowledgements = AcknowledgementsConfig::new(false);
        let state = test_state_with(config.clone()).await;

        let moved = "http://127.0.0.1:9".to_string();
        if let Some(grpc) = config
            .destinations
            .named
            .get_mut("alpha")
            .and_then(|spec| spec.grpc.as_mut())
        {
            grpc.endpoint.clone_from(&moved);
        }
        tokio::time::timeout(Duration::from_secs(10), state.reload_config(config))
            .await
            .expect("the reload finished")
            .unwrap();

        assert_eq!(totals.total("receiver_destination_build_failures_total"), 0);
        let routes = state.routes();
        let alpha = routes
            .destinations
            .get("alpha")
            .expect("alpha has a sender");
        assert!(
            matches!(&alpha.wiring, Wiring::Grpc { endpoint, .. } if *endpoint == moved),
            "alpha is still wired to {:?}",
            alpha.wiring
        );
        assert!(
            alpha
                .grpc()
                .and_then(|delivery| delivery.queued.as_deref())
                .is_some_and(|queued| matches!(queued, SinkBackend::Tiered(_))),
            "alpha's new sender does not spool to disk"
        );
    }

    /// A destination a reload adds whose sender cannot be built is counted,
    /// its records are answered retryable, and every other destination keeps
    /// taking records.
    #[tokio::test]
    async fn a_sender_a_reload_cannot_build_leaves_the_rest_running() {
        let totals = CounterTotals::default();
        let _recorder = metrics::set_default_local_recorder(&totals);
        let mut config = test_config();
        let state = test_state_with(config.clone()).await;

        config.destinations.named.insert(
            "broken".to_string(),
            crate::config::DestinationSpec {
                grpc: Some(crate::config::GrpcDestination {
                    // A space is not valid in a URI, so the gRPC client cannot be built.
                    endpoint: "http://not a uri:6000".to_string(),
                    ..crate::config::GrpcDestination::default()
                }),
                kafka: None,
            },
        );
        config.rules.rules = vec![
            scalo::rules::RuleConfig::new(
                "app-broken",
                "_source",
                scalo::rules::SetAction::value("broken"),
            )
            .matching(scalo::rules::MatchConfig::equals("app", "broken")),
        ];
        config
            .destinations
            .sources
            .insert("broken".to_string(), "broken".into());
        state.reload_config(config).await.unwrap();

        assert_eq!(totals.total("receiver_destination_build_failures_total"), 1);
        let refused = state
            .process(Bytes::from(r#"{"app":"broken"}"#), &http())
            .await;
        assert!(
            refused.as_ref().is_err_and(Error::is_retryable),
            "a record for the unbuilt destination was answered {refused:?}"
        );
        state
            .process(Bytes::from(r#"{"app":"other"}"#), &http())
            .await
            .expect("the default destination still takes records");
    }

    /// With no DLQ to take them, a retired destination's records are held and
    /// offered again until shutdown, and only then dropped and counted.
    #[tokio::test]
    async fn a_retired_destination_holds_its_records_until_shutdown() {
        let totals = CounterTotals::default();
        let _recorder = metrics::set_default_local_recorder(&totals);
        // Port 1 on loopback refuses every connection.
        let primary = GrpcSink::new("http://127.0.0.1:1", Some(1_000))
            .await
            .unwrap();
        let held = || vec![Bytes::from(r#"{"a":1}"#), Bytes::from(r#"{"a":2}"#)];

        let running = CancellationToken::new();
        let still_holding = tokio::time::timeout(
            Duration::from_millis(500),
            hand_back("gone", &primary, held(), None, &running),
        )
        .await;
        assert!(
            still_holding.is_err(),
            "the records were let go while running"
        );
        assert_eq!(totals.total("receiver_retired_records_total"), 0);

        let stopping = CancellationToken::new();
        stopping.cancel();
        hand_back("gone", &primary, held(), None, &stopping).await;
        assert_eq!(totals.total("receiver_retired_records_total"), 2);
    }

    // ---------------------------------------------------------------------
    // Additional edge-case tests (non-trivial paths)
    // ---------------------------------------------------------------------

    /// A pipeline counting on the receiver's metrics, as the orchestrator
    /// builds it, on `guard` when one is given.
    ///
    /// Call with the test's recorder already in place: the app metric group
    /// binds its handles when it is built.
    async fn counting_state(
        config: Config,
        guard: Option<Arc<MemoryGuard>>,
    ) -> (PipelineState, Arc<Metrics>) {
        let manager =
            scalo::metrics::MetricsManager::with_config(scalo::metrics::MetricsConfig::offline(""));
        let metrics = Arc::new(Metrics::register_on(
            Arc::new(config.scaling.build_pressure()),
            &manager,
        ));
        let state = Box::pin(PipelineState::build(
            SharedConfig::new(config),
            CancellationToken::new(),
            None,
            guard,
            Some(Arc::clone(&metrics)),
        ))
        .await
        .unwrap();
        (state, metrics)
    }

    /// Every record of a batch counts once as received, and each one taken as
    /// processed and delivered.
    #[tokio::test]
    async fn a_batch_counts_every_record_it_carries() {
        let totals = CounterTotals::default();
        let _recorder = metrics::set_default_local_recorder(&totals);
        let (state, _) = counting_state(test_config(), None).await;
        let payloads: Vec<Bytes> = (1..=5)
            .map(|i| Bytes::from(format!(r#"{{"id":{i}}}"#)))
            .collect();

        let outcome = state.process_batch(&payloads, &http()).await;

        assert_eq!(outcome.accepted, 5);
        assert_eq!(totals.total("records_received_total"), 5);
        assert_eq!(totals.total("records_processed_total"), 5);
        assert_eq!(totals.total("records_delivered_total"), 5);
        assert_eq!(totals.total("records_error_total"), 0);
    }

    /// A record refused for good is received and counted as an error; the
    /// records around it are taken.
    #[tokio::test]
    async fn a_refused_record_counts_as_received_and_as_an_error() {
        let totals = CounterTotals::default();
        let _recorder = metrics::set_default_local_recorder(&totals);
        let mut config = test_config();
        config.validation.dlq_on_invalid = false;
        let (state, _) = counting_state(config, None).await;
        let payloads = vec![
            Bytes::from(r#"{"ok":1}"#),
            Bytes::from("not json"),
            Bytes::from(r#"{"ok":2}"#),
        ];

        let outcome = state
            .process_batch_acked(&payloads, &holding(&state), None, &http())
            .await;

        assert_eq!(outcome.rejected, 1);
        assert_eq!(totals.total("records_received_total"), 3);
        assert_eq!(totals.total("records_delivered_total"), 2);
        assert_eq!(totals.total("records_error_total"), 1);
    }

    /// A single record through `process` counts once, taken or refused.
    #[tokio::test]
    async fn a_single_record_counts_once() {
        let totals = CounterTotals::default();
        let _recorder = metrics::set_default_local_recorder(&totals);
        let (state, _) = counting_state(test_config(), None).await;

        state
            .process(Bytes::from(r#"{"id":1}"#), &http())
            .await
            .unwrap();
        let _ = state
            .process(Bytes::from("not json"), &http())
            .await
            .unwrap_err();

        assert_eq!(totals.total("records_received_total"), 2);
        assert_eq!(totals.total("records_delivered_total"), 1);
        assert_eq!(totals.total("records_error_total"), 1);
    }

    /// Records the pipeline turns away under pressure were still received, and
    /// none of them was taken.
    #[tokio::test]
    async fn records_refused_under_pressure_count_as_received_not_taken() {
        let totals = CounterTotals::default();
        let _recorder = metrics::set_default_local_recorder(&totals);
        let guard = MemoryGuard::with_usage_source(
            MemoryGuardConfig {
                limit_bytes: 1000,
                pressure_threshold: 0.8,
                ..Default::default()
            },
            scalo::memory::UsageSource::Reservations,
        );
        let (state, _) = counting_state(test_config(), Some(Arc::new(guard))).await;
        state.memory_guard().add_bytes(900);

        let outcome = state
            .process_batch(
                &[Bytes::from(r#"{"a":1}"#), Bytes::from(r#"{"a":2}"#)],
                &http(),
            )
            .await;

        assert!(outcome.unavailable.is_some(), "{outcome:?}");
        assert_eq!(totals.total("records_received_total"), 2);
        assert_eq!(totals.total("records_delivered_total"), 0);
        assert_eq!(totals.total("records_error_total"), 0);
    }

    /// Records a destination's buffer holds back reach
    /// `receiver_messages_spilled_total` on the next metrics update.
    #[tokio::test]
    async fn records_a_buffer_holds_back_reach_the_spilled_counter() {
        let totals = CounterTotals::default();
        let _recorder = metrics::set_default_local_recorder(&totals);
        let mut config = test_config();
        grpc_destinations(&mut config, &["alpha"]);
        // An at-enqueue listener is what builds the buffer.
        config.server.acknowledgements = AcknowledgementsConfig::new(false);
        let (state, metrics) = counting_state(config, None).await;

        let outcome = state
            .process_batch(
                &[Bytes::from(r#"{"a":1}"#), Bytes::from(r#"{"a":2}"#)],
                &http(),
            )
            .await;
        state.update_metrics(&metrics).await;

        assert_eq!(outcome.accepted, 2, "taken into the buffer: {outcome:?}");
        assert_eq!(totals.total("receiver_messages_spilled_total"), 2);
        assert_eq!(totals.total("receiver_messages_drained_total"), 0);
    }

    #[tokio::test]
    async fn test_pipeline_batch_processes_all() {
        // Batch processing: all-valid payloads should all succeed
        let state = test_state().await;
        let payloads = vec![
            Bytes::from(r#"{"id":1}"#),
            Bytes::from(r#"{"id":2}"#),
            Bytes::from(r#"{"id":3}"#),
        ];
        let outcome = state.process_batch(&payloads, &http()).await;
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

        let outcome = state.process_batch(&payloads, &http()).await;

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
            .process_batch_acked(&payloads, &holding(&state), None, &http())
            .await;

        assert_eq!(outcome.accepted, 2);
        assert_eq!(outcome.rejected, 1);
        assert!(outcome.unavailable.is_none(), "{:?}", outcome.unavailable);
    }

    /// A retryable failure stops the batch where it happened: the sender is
    /// told to resend, so taking the records behind it only adds duplicates.
    ///
    /// The record missing its required field is routed to a DLQ topic with no
    /// broker configured, which the receiver cannot serve now but could once
    /// the route works.
    #[tokio::test]
    async fn a_retryable_failure_stops_the_batch() {
        let mut config = test_config();
        config.validation.required_fields = vec!["ok".to_string()];
        let state = test_state_with(config).await;
        let payloads = vec![
            Bytes::from(r#"{"ok":1}"#),
            Bytes::from(r#"{"not_ok":1}"#),
            Bytes::from(r#"{"ok":2}"#),
        ];

        let outcome = state.process_batch(&payloads, &http()).await;

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

    /// Run `test` on a thread with a Tokio worker's 2 MiB stack.
    fn on_worker_stack<F, Fut>(test: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()>,
    {
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(test());
            })
            .unwrap()
            .join()
            .expect("the pipeline thread must return");
    }

    fn nested(depth: usize) -> Bytes {
        Bytes::from(format!("{}1{}", "[".repeat(depth), "]".repeat(depth)))
    }

    /// Every listener enters through `process` or one of the batch entries, the
    /// routed ones and `process_batch_to_topic`, and each refuses a deep record
    /// before it parses.
    #[test]
    fn a_deeply_nested_record_is_refused_at_every_pipeline_entry() {
        on_worker_stack(|| async {
            let state = test_state().await;

            for depth in [20_000, 100_000] {
                let err = state
                    .process(nested(depth), &http())
                    .await
                    .expect_err("too deep");
                assert!(
                    matches!(err, Error::Validation(ref r) if r.contains("maximum parse depth")),
                    "{err}"
                );
                let outcome = state
                    .process_batch_to_topic(
                        &[nested(depth)],
                        "deep_land",
                        &holding(&state),
                        &http(),
                    )
                    .await;
                assert_eq!(outcome.rejected, 1, "{outcome:?}");
                assert!(outcome.unavailable.is_none(), "{:?}", outcome.unavailable);
                assert!(
                    matches!(outcome.first_rejection, Some(Error::Validation(ref r)) if r.contains("maximum parse depth")),
                    "{:?}",
                    outcome.first_rejection
                );
            }
        });
    }

    /// A deep record is refused for good, so the records behind it are still taken.
    #[test]
    fn a_deeply_nested_record_does_not_stop_the_batch() {
        on_worker_stack(|| async {
            let state = test_state().await;
            let payloads = vec![
                Bytes::from(r#"{"ok":1}"#),
                nested(20_000),
                Bytes::from(r#"{"ok":2}"#),
            ];

            let outcome = state.process_batch(&payloads, &http()).await;

            assert_eq!(outcome.accepted, 2);
            assert_eq!(outcome.rejected, 1);
            assert!(outcome.unavailable.is_none(), "{:?}", outcome.unavailable);
            assert!(
                matches!(outcome.first_rejection, Some(Error::Validation(_))),
                "{:?}",
                outcome.first_rejection
            );
        });
    }

    /// Pressure refuses the whole batch before any record is taken.
    #[tokio::test]
    async fn pressure_refuses_the_whole_batch_as_retryable() {
        let mut config = test_config();
        config.buffer.memory_limit = 1000;
        config.buffer.pressure_threshold = 0.8;
        let state = test_state_on_reservations(config).await;
        state.memory_guard().add_bytes(900);

        let outcome = state
            .process_batch(&[Bytes::from(r#"{"a":1}"#)], &http())
            .await;

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
        let result = state.process(Bytes::from(large), &http()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_empty_object() {
        let state = test_state().await;
        let result = state.process(Bytes::from(r"{}"), &http()).await;
        assert!(result.is_ok(), "empty JSON object is valid");
    }

    #[tokio::test]
    async fn test_pipeline_nested_json() {
        let state = test_state().await;
        let nested =
            Bytes::from(r#"{"a":{"b":{"c":{"d":{"e":{"f":"deep"}}}}},"arr":[1,2,3,[4,[5]]]}"#);
        let result = state.process(nested, &http()).await;
        assert!(result.is_ok(), "deeply nested JSON should succeed");
    }

    #[tokio::test]
    async fn test_pipeline_unicode_payload() {
        let state = test_state().await;
        let unicode = Bytes::from(r#"{"msg":"日本語🎉émojí","emoji":"🔥"}"#);
        let result = state.process(unicode, &http()).await;
        assert!(result.is_ok(), "unicode should process correctly");
    }

    /// `test_config` with credential `acme-ingest` bound to `acme` and `beta`,
    /// and a row landing `{"app":"b"}` in `beta`.
    fn two_org_config() -> Config {
        let mut config = test_config();
        config.ingest_bindings.credentials.insert(
            "acme-ingest".to_string(),
            crate::config::OrgBinding {
                orgs: vec!["acme".to_string(), "beta".to_string()],
                default_org: None,
            },
        );
        config.rules.rules = vec![
            scalo::rules::RuleConfig::new(
                "to-beta",
                "_org_id",
                scalo::rules::SetAction::value("beta"),
            )
            .matching(scalo::rules::MatchConfig::equals("app", "b")),
        ];
        config
    }

    fn acme_ingest() -> Ingest {
        Ingest::sent(Principal::Credential("acme-ingest".into()))
    }

    /// Each org's accepted records add their decoded bytes to its own usage series.
    #[tokio::test]
    async fn usage_counts_the_bytes_each_org_took() {
        let recorder = crate::buffer::rejects::CountedKeys::default();
        let _local = metrics::set_default_local_recorder(&recorder);
        let state = test_state_with(two_org_config()).await;
        let to_beta = Bytes::from(r#"{"app":"b","n":1}"#);
        let to_acme = Bytes::from(r#"{"app":"x"}"#);

        let outcome = state
            .process_batch(
                &[to_beta.clone(), to_acme.clone(), to_acme.clone()],
                &acme_ingest(),
            )
            .await;

        assert_eq!(outcome.accepted, 3, "{outcome:?}");
        assert_eq!(
            recorder.get("usage_ingested_bytes_total{org_id=beta}"),
            to_beta.len() as u64
        );
        assert_eq!(
            recorder.get("usage_ingested_bytes_total{org_id=acme}"),
            2 * to_acme.len() as u64
        );
        assert_eq!(
            recorder.get("usage_ingested_bytes_total{org_id=default}"),
            0
        );
    }

    /// A dead-lettered record is not ingested, so it adds no usage.
    #[tokio::test]
    async fn a_dead_letter_counts_no_usage() {
        let recorder = crate::buffer::rejects::CountedKeys::default();
        let _local = metrics::set_default_local_recorder(&recorder);
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        file_dlq(&mut config, dir.path());
        let state = test_state_with(config).await;

        let outcome = state
            .process_batch_acked(&[missing_org_id()], &holding(&state), None, &http())
            .await;

        assert_eq!(outcome.accepted, 1, "the DLQ took it: {outcome:?}");
        assert_eq!(
            recorder.get("usage_ingested_bytes_total{org_id=default}"),
            0
        );
    }

    /// A held batch answered as a resend of all of it adds no usage: the resend counts it.
    #[tokio::test]
    async fn a_held_batch_answered_retryable_counts_no_usage() {
        let recorder = crate::buffer::rejects::CountedKeys::default();
        let _local = metrics::set_default_local_recorder(&recorder);
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        grpc_destinations(&mut config, &["alpha"]);
        spill_to(&mut config, dir.path());
        let state = test_state_with(config).await;

        let outcome = state
            .process_batch_acked(
                &[Bytes::from(r#"{"a":1}"#)],
                &holding(&state),
                None,
                &http(),
            )
            .await;

        assert!(outcome.unavailable.is_some(), "{outcome:?}");
        assert_eq!(
            recorder.get("usage_ingested_bytes_total{org_id=default}"),
            0
        );
    }

    /// A record stamped under the old name keeps it beside the new one, so
    /// no key is written twice.
    #[tokio::test]
    async fn a_record_with_the_old_receive_stamp_takes_the_new_one_beside_it() {
        let state = test_state().await;
        let stamped = routed(&state, r#"{"_timestamp_receiver":"old","key":"val"}"#);
        assert_eq!(stamped["_timestamp_receiver"], "old");
        assert_eq!(stamped[routing::RECEIVED_FIELD], 1_760_000_000_000_u64);
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
        state.reload_config(new_config).await.unwrap();

        // Pipeline should remain ready after reload
        assert!(state.is_ready());

        // And continue to process payloads
        let result = state.process(Bytes::from(r#"{"ok":true}"#), &http()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_multiple_reloads() {
        // Repeated reloads should not leak resources or stuck state
        let state = test_state().await;
        for i in 0..10 {
            let mut cfg = test_config();
            cfg.routing.default_source = format!("src-{i}");
            state.reload_config(cfg).await.unwrap();
        }
        // After many reloads, pipeline must still work
        let result = state
            .process(Bytes::from(r#"{"final":true}"#), &http())
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_malformed_utf8_rejected() {
        // Invalid UTF-8 bytes must not cause a panic
        let state = test_state().await;
        let bad = Bytes::from(vec![0xff, 0xfe, 0xfd, 0xfc, 0x00, 0x01]);
        let result = state.process(bad, &http()).await;
        // Must return an error, not panic
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pipeline_truncated_json() {
        // Truncated JSON (common in network corruption) should be rejected cleanly
        let state = test_state().await;
        let truncated = Bytes::from(r#"{"key":"val"#);
        let result = state.process(truncated, &http()).await;
        assert!(result.is_err());
    }
}
