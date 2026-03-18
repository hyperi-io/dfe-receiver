# TODO - dfe-receiver

This is the **single source of truth** for all tasks and progress.

---

## Work Breakdown Structure (WBS)

### Multi-Protocol Ingestion (Approach C: Dual Mode)

**Goal:** Support enterprise agent protocols natively so dfe-receiver can replace intermediary collectors (OTel Collector, Logstash, etc.)

All protocols follow: receive -> convert to JSON -> validate -> route -> Kafka/dfe-loader

#### Phase 0: Protocol Framework [DONE]

- [x] Define `ProtocolHandler` trait in `src/server/traits.rs`
- [x] Refactor HTTP handler to implement `ProtocolHandler`
- [x] Refactor gRPC/Vector handler to implement `ProtocolHandler`
- [x] Update `Server::run` to iterate enabled handlers (spawn all in parallel)

#### Phase 1: OTLP (Tier 1) [DONE]

- [x] Vendor OTLP protos from opentelemetry-proto v1.5.0
- [x] Configure tonic-build for OTLP service definitions (logs, metrics, traces)
- [x] Create `src/server/otlp/mod.rs` -- gRPC server (port 4317), HTTP server (port 4318)
- [x] Implement ExportLogsService, ExportMetricsService, ExportTraceService RPCs
- [x] Create `src/server/otlp/convert.rs` -- dual mode converter (HyperDX + Generic)
- [x] HyperDX mode: logs/traces/metrics JSON matching ClickHouse OTel schema
- [x] Generic mode: normalised JSON envelope with routing fields
- [x] OTLP HTTP endpoints: `/v1/logs`, `/v1/metrics`, `/v1/traces`
- [x] Add `OtlpConfig` to config (grpc_bind_address, http_bind_address, mode, tls, auth)
- [x] Wire OTLP handler into server orchestration
- [x] Suppress proto doctests via `doctest = false` in Cargo.toml
- [x] Clippy fixes for OTLP code
- [x] OTLP feature flag in Cargo.toml (`otlp` feature, default-enabled)
- [x] Integration tests: send OTLP data via gRPC client, verify pipeline (9 tests)
- [x] Integration tests: send OTLP data via HTTP, verify pipeline

#### Phase 2: Prometheus Remote Write (Tier 1) [DONE]

- [x] HTTP endpoint: `POST /api/v1/write`
- [x] Snappy decompression + protobuf decode (v1)
- [x] TimeSeries -> JSON conversion (native, otel, hyperdx modes)
- [x] `PrometheusRwConfig` in config, wired into server orchestration
- [x] 21 unit tests + 10 integration tests

#### Phase 3: Lumberjack/Beats (Tier 2) [DONE]

- [x] TCP/TLS listener, Lumberjack v2 frame parser
- [x] Windowed ACK protocol (window + ACK frames)
- [x] Zlib decompression of compressed frames
- [x] Beats fields -> JSON conversion
- [x] `LumberjackConfig` in config
- [x] Integration tests with real Filebeat binary (auto-downloaded)

#### Phase 4: Syslog (Tier 2) [DONE]

- [x] UDP listener (port 514)
- [x] TCP listener (port 514)
- [x] TLS/TCP listener (port 6514)
- [x] RFC 5424 parser (structured data) + RFC 3164 parser (BSD format)
- [x] Auto-detect format per message (syslog_loose Variant::Either)
- [x] Octet-counting + non-transparent framing (TCP, RFC 6587)
- [x] `SyslogConfig` in config
- [x] 18 unit tests (7 convert + 10 framing + 1 config)
- [x] 8 integration tests using `logger` binary (bsdutils)

#### Phase 5: Splunk HEC (Tier 2) [DONE]

- [x] HTTP endpoints: `/services/collector/event`, `/services/collector/raw`, `/services/collector/health`
- [x] Splunk token auth (`Authorization: Splunk <token>` + `Bearer <token>`)
- [x] NDJSON event parsing via StreamDeserializer
- [x] HEC JSON -> normalised JSON (metadata injection, field precedence)
- [x] HEC response format with standard error codes (0, 5, 6, 8, 9, 12, 17, 18)
- [x] `SplunkHecConfig` in config (port 8088)
- [x] 15 unit tests + 11 integration tests (reqwest-based)

#### Phase 6: Fluent Forward (Tier 3) [DONE]

- [x] TCP listener, Forward protocol parser (msgpack via rmpv)
- [x] Message/Forward/PackedForward modes
- [x] EventTime extension type 0 support
- [x] Chunk ACK response
- [x] `FluentConfig` in config (port 24224)
- [x] 10 unit tests + 3 integration tests (fluent-bit binary, auto-downloaded)

#### Phase 7: GELF (Tier 3) [DONE]

- [x] TCP listener (null-byte delimited via tokio codec)
- [x] GELF 1.1 JSON validation (version, host, short_message)
- [x] Severity mapping, short_message→message copy, `_source: "gelf"` injection
- [x] `GelfConfig` in config (port 12201)
- [x] 14 unit tests + 3 integration tests (fluent-bit GELF output, auto-downloaded)

---

## Active Tasks

- [x] Remove dead `#[cfg(feature = "plugins")]` code — already removed in prior session (commit `4a453fb`)

### ~~Consume hyperi-rustlib v1.16.0 (Dynamic Linking)~~ [DONE]

- [x] Bump hyperi-rustlib from `1.13.2` to `1.16.0`
- [x] Add `NativeDepsContract` + `ImageProfile` to deployment contract
- [x] Regenerate Dockerfile from contract (Confluent APT repo, runtime packages)
- [x] `cargo update` + full test suite passing

---

## Completed (Recent Sessions)

- [x] Fix GH #3: rdkafka stats spam — added `librdkafka_overrides` to `KafkaConfig`, defaults `statistics.interval.ms` to `0`
  - GA release v1.13.11 with fix, binaries on GH Releases + R2
  - 4 new tests for default, passthrough, YAML override, serde replacement
- [x] Documentation audit — fixed 12 issues across README, CLAUDE.md, config.example.yaml, DESIGN.md, and 3 docs/ files
  - Removed stale CI-REQUIREMENTS doc, Vector Agent Module section, wrong metric names, wrong ports
- [x] Full CI pipeline working end-to-end: Quality → Test → Build (amd64+arm64) → Release → Publish (GH Release + R2)
  - GA release v1.13.10 with both binary architectures + checksums
  - R2 binaries live at `downloads.hyperi.io/dfe-receiver/v1.13.10/` and `/latest/`
  - Fixed flaky gRPC TLS test (TCP readiness loop + 30s Vector timeout)
  - Fixed Prometheus RW port conflict (9090→9091)
  - Added 5 missing protocol ports to deployment contract, Dockerfile, docker-compose
  - Fixed stale README routing docs
  - Enabled R2 binary publishing in `.hyperi-ci.yaml`
  - Reconciled main/release branch divergence via PR
- [x] Remove plugin system (dfe-plugin-loader, dfe-protocol-sdk deps), document sidecar transport pattern (`docs/SIDECAR-TRANSPORTS.md`)
- [x] Upgrade all deps to latest (sonic-rs 0.5, axum 0.8, tonic 0.14, prost 0.14, sysinfo 0.37), adopt edition 2024, migrate KafkaSink/LoaderSink to rustlib KafkaProducer (removed TopicBatch, rdkafka from prod deps)

- [x] v1.13.0 published — gRPC loader transport + file debug sink, full CI green (Quality ✓, Test ✓, Publish ✓)
- [x] gRPC loader transport (`loader.transport = "grpc"`) — `GrpcSink` wrapping rustlib `GrpcTransport`; receiver→loader without Kafka for dfe-docker
- [x] File debug sink (`file_sink.enabled`) — NDJSON tap writing all processed messages to disk
- [x] CI publish fixes: GHCR permissions (`GITHUB_TOKEN`), Helm stdout pollution (`>&2` on info/success)
- [x] Cargo publish excludes applied across all Rust dfe-* projects (dfe-archiver, dfe-fetcher, dfe-receiver-plugin-syslog, dfe-transform-vector, hyperi-rustlib)
- [x] Get clean CI build through to JFrog publish
- [x] Phase 2: Prometheus Remote Write v1 (snappy+protobuf, native/otel/hyperdx modes, 10 integration tests)
- [x] rustlib updated to 1.13.0 — CLI module wired (`DfeApp` trait, `CommonArgs`, `StandardCommand`)
- [x] Deployment artefacts generated — Dockerfile (Ubuntu 24.04, multi-arch), chart/, docker-compose.yaml
- [x] Container + Helm publishing enabled in `.hyperi-ci.yaml`
- [x] Phase 4: Syslog handler (UDP/TCP/TLS, RFC 5424+3164, octet-counting, logger integration tests)
- [x] Phase 3: Lumberjack/Beats handler (TCP/TLS, frame parser, zlib, ACK, Filebeat integration tests)
- [x] Phase 5: Splunk HEC handler (event/raw/health endpoints, NDJSON, auth, integration tests)
- [x] Changed default_source from "dfe" to "default" (happy path topic: default_land)
- [x] OTLP feature flag (compile-gated, default-enabled) + 9 OTLP integration tests
- [x] Phase 6: Fluent Forward handler (msgpack TCP, 3 modes, ACK, fluent-bit integration tests)
- [x] Phase 7: GELF handler (null-delimited JSON TCP, validation, fluent-bit integration tests)
- [x] CI fix: aarch64 cross-compile (`-fuse-ld=bfd` to avoid mold linker for cross targets)
- [x] CI fix: publish-binary.sh SCRIPT_DIR clobber (source order fix)
- [x] Published v1.10.3 to JFrog (first successful Publish pipeline in 5+ releases)
- [x] Phase 0: ProtocolHandler trait + server refactor
- [x] Phase 1: OTLP proto compilation (vendored opentelemetry-proto v1.5.0)
- [x] Phase 1: OTLP module structure and proto includes
- [x] Phase 1: OTLP->JSON converter (HyperDX + Generic dual mode)
- [x] Phase 1: OTLP gRPC receiver (port 4317)
- [x] Phase 1: OTLP HTTP receiver (port 4318)
- [x] Phase 1: OtlpConfig + server integration
- [x] Fixed proto doctest failures (doctest = false)
- [x] Clippy fixes (Option<&T>, write! macro, match arms, items ordering)
- [x] SharedConfig, env overrides, config reload, serde_yaml_ng migration
- [x] Source-rule routing, timestamp enrichment
- [x] Vector agent module scope (DESIGN.md)
- [x] Merged origin/main (OTLP, plugins, CI) with source-routing-enrichment branch
- [x] Resolved all merge conflicts (config/mod.rs, Cargo.toml, TODO.md, config.example.yaml, DESIGN.md, Cargo.lock)

---

## Backlog

### High Priority

- [x] GHCR container image publishing
  - [x] `Dockerfile` — Ubuntu 24.04, multi-arch via `ARG TARGETARCH` + BuildKit bind mount
  - [x] `publish.container` section in `.hyperi-ci.yaml` (ghcr, linux/amd64+arm64)
  - [x] `publish.helm` section in `.hyperi-ci.yaml` (oci://ghcr.io/hyperi-io/charts)
  - [ ] Test: trigger release, verify `ghcr.io/hyperi-io/dfe-receiver`
- [x] KEDA Prometheus trigger — added optional `keda.prometheus.*` section to Helm ScaledObject
- [x] Sidecar transport documentation — expanded with StatsD, Windows Event Log, Kafka examples + troubleshooting/sizing/health sections
- [x] Disk spillover — opt-in via `buffer.spillover.enabled`, rustlib TieredSink with disk-aware capacity, SinkBackend enum
- [x] Config hot-reload for auth — `spawn_auth_reload_watcher()` subscribes to SharedConfig, swaps bearer tokens atomically
- [x] Performance benchmarks — expanded criterion suite (router, metrics render) + profiling profile + baseline capture
- [x] Code review remediations — bounded queue backpressure, rate window sampling, config unmarshal warning, project files

### Observability Standardisation (WBS)

**Goal:** Common metrics + log spam protection across all dfe-* Rust services,
implemented in hyperi-rustlib and consumed by each project.

**Standards:** `hyperi-ai/standards/universal/METRICS.md`, `hyperi-ai/standards/universal/LOG-FLOODING.md`
**DFE design:** `docs/METRICS.md`, `docs/LOG-SPAMMING.md`

#### Phase 1: rustlib — Log Spam Protection [DONE]

- [x] Add `tracing-throttle` dep to rustlib `logger` feature
- [x] Wire throttle layer into `logger::setup()` — opt-in via `LOG_THROTTLE_ENABLED` env var
- [x] Add helper functions: `log_state_change()`, `log_sampled()`, `log_debounced()`
- [x] Tests for all three helpers + throttle layer integration
- [ ] Publish rustlib patch (push to main, let CI run)

#### Phase 2: rustlib — Standard DFE Metrics Framework [DONE]

- [x] Add `DfeMetrics` struct with standard `dfe_transport_*`, `dfe_pipeline_*`, `dfe_records_*`, `dfe_scaling_*` metrics
- [x] Pre-register all metrics at construction (counters init to 0)
- [x] Transport label support (`transport="kafka|grpc|file"`)
- [x] Tiered architecture: `MetricsManager` (generic base) → `DfeMetrics` (DFE extension)
- [x] Tests
- [ ] Publish rustlib patch (push to main, let CI run)

#### Phase 3: dfe-receiver — Consume New rustlib

- [ ] Bump rustlib dep, remove `[patch.crates-io]`
- [ ] Replace hand-rolled `Metrics` struct with rustlib `DfeMetrics`
- [ ] Emit `dfe_*` metrics (dual-emit old names alongside new)
- [ ] Wire log spam helpers into identified hot spots:
  - Memory pressure warn → state-transition
  - Kafka send error → sampled (1/1000)
  - Loader send error → sampled (1/1000)
  - Syslog UDP recv error → debounced (5s)
  - Lumberjack frame parse error → sampled (1/100)
- [ ] Update KEDA ScaledObject PromQL query
- [ ] Run benchmarks to verify no regression
- [ ] Push to main, verify CI

#### Phase 4: Other dfe-* Rust Projects (apply same pattern)

- [ ] dfe-loader: replace `prometheus` crate with rustlib `DfeMetrics`, fix coercion warn spam
- [ ] dfe-fetcher: replace hand-rolled metrics with rustlib, fix container stderr spam
- [ ] dfe-archiver: already uses MetricsManager — align metric names to `dfe_*`

#### Phase 5: hyperi-pylib — Mirror Rust Patterns for Python

- [ ] Add `RateLimitFilter` to `hyperi_pylib.logging.setup()` — opt-in global safety net
- [ ] Add helper classes: `StateLogger`, `SampledLogger` to `hyperi_pylib.logging`
- [ ] Add `DfeMetrics` wrapper to `hyperi_pylib.metrics` with standard `dfe_*` metric registration
- [ ] Apply to dfe-engine (FastAPI) — standard metrics + log spam protection
- [ ] Load Python standards (`hyperi-ai/standards/languages/PYTHON.md`) before implementation

#### Phase 6: Remediate All dfe-* Projects (Log Spam + Metrics)

Two-part remediation per project: (A) fix identified log spam sites, (B) replace
existing metrics with rustlib `DfeMetrics` standard `dfe_*` names.

**dfe-receiver:**

Log spam fixes:
- [ ] `src/pipeline/mod.rs:205,315` — memory pressure warn → state-transition
- [ ] `src/sink/kafka/mod.rs:61` — Kafka send error → sampled (1/1000) + metric
- [ ] `src/sink/loader/mod.rs:95` — loader send error → sampled (1/1000) + metric
- [ ] `src/server/syslog/mod.rs:73` — UDP recv error → debounced (5s)
- [ ] `src/server/lumberjack/mod.rs:94,135` — frame parse error → sampled (1/100)

Metrics migration:
- [ ] Replace hand-rolled `Metrics` struct + `render()` with rustlib `DfeMetrics`
- [ ] Emit standard `dfe_transport_*{transport="kafka|grpc|loader|file"}` metrics
- [ ] Emit standard `dfe_records_*`, `dfe_pipeline_*`, `dfe_scaling_*`, `dfe_spool_*`
- [ ] Add histograms: `dfe_transport_send_duration_seconds`
- [ ] Dual-emit old `receiver_*` names during transition, remove in next release
- [ ] Update KEDA ScaledObject PromQL to `dfe_scaling_pressure`

**dfe-loader:**

Log spam fixes:
- [ ] `src/transform/coerce.rs:103` — type coercion warn per-row → sampled (1/1000) + log batch total
- [ ] `src/clickhouse/inserter.rs:338,358` — retry warn → state-transition (first failure + recovery)
- [ ] `src/pipeline/orchestrator.rs:783,786` — DLQ channel full → debounced (5s)
- [ ] `src/kafka/consumer.rs:219` — consumer error → debounced (5s)

Metrics migration:
- [ ] Replace `prometheus` crate `Registry` with rustlib `DfeMetrics`
- [ ] Replace bespoke `hyper` metrics server with rustlib `MetricsManager::start_server()`
- [ ] Emit standard `dfe_*` names instead of `loader_*`
- [ ] Register `dfe_scaling_pressure` properly (currently appended as raw text)

**dfe-fetcher:**

Log spam fixes:
- [ ] `src/extractor/container/mod.rs:152` — container stderr warn per-line → sampled (1/100) + count
- [ ] `src/scheduler/mod.rs:118` — source not ready → debounced (10s)
- [ ] `src/output.rs:143` — transport send error → sampled (1/1000) + metric
- [ ] `src/pipeline/mod.rs:253` — DLQ send failure → debounced (5s)

Metrics migration:
- [ ] Replace hand-rolled `Metrics` struct + `render()` with rustlib `DfeMetrics`
- [ ] Resolve `dfe_pipeline_ready` collision with dfe-transform-vector (use `job` label)
- [ ] Add `dfe_scaling_pressure` (currently missing — no KEDA integration)

**dfe-archiver:**

Log spam fixes:
- [ ] `crates/archiver/src/archiver.rs:168` — routing failure → sampled (1/1000) + metric
- [ ] `crates/archiver/src/archiver.rs:191` — buffer push under pressure → state-transition
- [ ] `crates/core/src/buffer/tiered.rs:225,242` — spool full → state-transition
- [ ] `crates/archiver/src/archiver.rs:134` — Kafka recv error → debounced (5s)

Metrics migration:
- [ ] Already uses `MetricsManager` — align metric names from `dfe_archiver_*` to `dfe_*`
- [ ] Replace manual scaling pressure with rustlib `ScalingPressure`

**dfe-engine (Python):**

Log spam fixes:
- [ ] Audit all `logger.warning`/`logger.error` sites for per-request spam potential
- [ ] Apply `RateLimitFilter` globally via pylib
- [ ] Fix identified sites with `StateLogger`/`SampledLogger`

Metrics migration:
- [ ] Apply pylib `DfeMetrics` wrapper with standard `dfe_*` names
- [ ] Emit `dfe_transport_*`, `dfe_records_*`, `dfe_pipeline_ready`

#### Phase 7: Clean Up Standards Docs

After all remediations are complete, remove project-specific audit findings
from the universal standards — they belong in TODO.md, not in standards.

- [ ] `hyperi-ai/standards/universal/LOG-FLOODING.md` — remove "Current State", "Worst Offenders" table, and per-project audit from DFE section
- [ ] `hyperi-ai/standards/universal/METRICS.md` — remove "Migration" section (will be done) and any stale per-project references
- [ ] `dfe-receiver/docs/LOG-SPAMMING.md` — remove or archive (audit data moves to git history)
- [ ] `dfe-receiver/docs/METRICS.md` — remove migration section, keep as operational reference

### Bespoke Code Dedup (receiver vs rustlib)

- [ ] Audit dfe-receiver for functionality duplicated with rustlib
- [ ] Replace bespoke code with rustlib equivalents (extend rustlib if needed)
- [ ] Key suspects: hand-rolled `Metrics` struct, `RateWindow`, `BufferManager` memory detection, metrics HTTP server

### Security Logging Standard (rustlib)

- [ ] Design `SecurityEventLogger` for rustlib — structured security events separate from operational logs
- [ ] Standard event types: `auth.success`, `auth.failure`, `access.denied`, `config.changed`, `rate_limit.triggered`, `tls.failure`
- [ ] Mandatory fields: `event_type`, `actor`, `action`, `outcome`, `source_ip`, `timestamp`
- [ ] Separate security log sink (file or structured JSON to dedicated index)
- [ ] Wire into dfe-receiver auth middleware, TLS handler, rate limiting
- [ ] Add to `hyperi-ai/standards/universal/` as `SECURITY-LOGGING.md`

### Smart Log Combining (rustlib enhancement)

- [ ] Enhance `tracing-throttle` integration — configure `exclude_fields` for high-cardinality fields
  - Already supported: `exclude_fields(&["request_id", "span_id", "topic"])` strips variable parts before signature hash
  - The message template (format string) becomes the grouping key — identical template = same group regardless of field values
- [ ] Add collapsed summary on throttle: "N occurrences suppressed in last Xs" (tracing-throttle may support this natively)
- [ ] Consider post-collection pattern detection for dashboards (Grafana Loki pattern detection, not code-level)

### Hot Path Optimisation

- [ ] Pre-compute topic strings in Router (eliminate `format!()` per-message)
- [ ] Pre-split field paths at Router/Validator construction (eliminate `.split('.').collect()` per-message)

### Medium Priority

- [ ] Fix Helm `chart/templates/secret.yaml` — `bearer-tokens` hyphen in Go template field name
- [ ] Remove `[patch.crates-io]` from Cargo.toml after rustlib publishes (blocked on Phases 1/2)
- [ ] Documentation for deployment

### Low Priority

- [ ] Phase 2 removes need for this: Migrate hand-rolled Prometheus text render to `metrics-exporter-prometheus` crate

---

## Blocked

(none)

---

## Previously Completed

- [x] Initial project setup, GitHub repo, CI pipeline
- [x] Bearer token authentication (secret manager integration, refresh, tests)
- [x] hypersec -> hyperi rebrand (rustlib, agent header, CI configs, license headers)
- [x] gRPC Vector sink protocol (proto rewrite, protobuf-to-JSON, unary handler, TLS, auth)
- [x] TLS/mTLS certificate hot-reload from secret manager
- [x] Integration tests (bearer auth file-based, Vector HTTP/HTTPS/gRPC/TLS/bearer)
- [x] CI fixes (clippy approx_constant, cargo fmt, BuildJet -> standard runner)
- [x] Published v1.6.3 to JFrog Artifactory

---

## Notes for AI Assistants

This file is the **single source of truth** for tasks and progress.

**Rules:**

- All tasks go here, nowhere else
- Planning mode outputs go here (WBS section)
- Mark tasks `[IN PROGRESS]` when starting
- Mark tasks `[x]` when complete, move to Completed section
- Never add tasks to STATE.md or CLAUDE.md

**Status tags:**

- `[PENDING]` - Not started
- `[IN PROGRESS]` - Currently working on
- `[BLOCKED]` - Waiting on something
- `[x]` - Completed (checkbox checked)
