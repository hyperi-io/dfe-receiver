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

- [ ] Commit and push all pending changes (OTLP feature flag, Fluent Forward, GELF, integration tests) `[PENDING]`

---

## Completed (Recent Sessions)

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
- [ ] KEDA scaling metrics endpoint — expose backpressure metrics for KEDA ScaledObject
  - CPU utilisation (process-level)
  - Consumer group lag (Kafka topic lag via rdkafka stats)
  - In-memory buffer saturation (TieredSink queue depth / capacity)
  - Circuit breaker state (open/closed/half-open)
  - Endpoint: `/metrics/keda` or Prometheus `/metrics` with KEDA-compatible labels

### Medium Priority

- [ ] **Vector.dev agent module** — managed subprocess, auto-download, n-1 updates, cgroup memory isolation (see DESIGN.md)
- [ ] Disk spillover implementation (currently in-memory only)
- [ ] Config hot-reload for auth settings
- [ ] Performance benchmarks

### Low Priority

- [ ] Documentation for deployment

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
