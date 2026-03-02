# TODO - dfe-receiver

This is the **single source of truth** for all tasks and progress.

---

## Active Tasks

Tasks currently being worked on. Only one task should be `[IN PROGRESS]` at a time.

- [ ] Update Cargo.lock for hyperi-rustlib >=1.5 `[BLOCKED]`
  - Current state: Cargo.toml uses `version = ">=1.5"` with `config-reload` feature; merge to main complete and pushed
  - Next: Run `cargo update -p hyperi-rustlib` once rustlib >=1.5 is published to JFrog
  - Blockers: hyperi-rustlib >=1.5 not yet published (only 1.4.3 available); CI build will fail until resolved

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
- [ ] OTLP feature flag in Cargo.toml (currently always compiled) `[PENDING]`
- [ ] Integration tests: send OTLP data via gRPC client, verify pipeline `[PENDING]`
- [ ] Integration tests: send OTLP data via HTTP, verify pipeline `[PENDING]`

#### Phase 2: Prometheus Remote Write (Tier 1) [NOT STARTED]

- [ ] HTTP endpoint: `POST /api/v1/write`
- [ ] Snappy decompression + protobuf decode (v1 + v2)
- [ ] TimeSeries -> JSON conversion
- [ ] `PrometheusConfig` + `prometheus-rw` feature flag

#### Phase 3: Lumberjack/Beats (Tier 2) [NOT STARTED]

- [ ] TCP/TLS listener, Lumberjack v2 frame parser
- [ ] Windowed ACK protocol
- [ ] Beats fields -> JSON conversion
- [ ] `LumberjackConfig` + `lumberjack` feature flag

#### Phase 4: Syslog (Tier 2) [NOT STARTED]

- [ ] UDP listener (port 514)
- [ ] TCP listener (port 514)
- [ ] TLS/TCP listener (port 6514)
- [ ] RFC 5424 parser (structured data) + RFC 3164 parser (BSD format)
- [ ] Auto-detect format per message
- [ ] Octet-counting + non-transparent framing (TCP)
- [ ] `SyslogConfig` + `syslog` feature flag

#### Phase 5: Splunk HEC (Tier 2) [NOT STARTED]

- [ ] HTTP endpoints: `/services/collector/event` + `/services/collector/raw`
- [ ] Splunk token auth
- [ ] HEC JSON -> normalised JSON
- [ ] `SplunkHecConfig` + `splunk-hec` feature flag

#### Phase 6: Fluent Forward (Tier 3) [NOT STARTED]

- [ ] TCP listener, Forward protocol parser (msgpack)
- [ ] Message/Forward/PackedForward modes
- [ ] `FluentConfig` + `fluent-forward` feature flag

#### Phase 7: GELF (Tier 3) [NOT STARTED]

- [ ] TCP listener (null-delimited)
- [ ] GELF JSON parsing
- [ ] `GelfConfig` + `gelf` feature flag

---

## Completed (This Session)

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

- [ ] GHCR container image publishing (see `docs/CONTAINER-PUBLISHING.md`)
  1. [ ] Create `Dockerfile` in repo root (wraps pre-built binary, Option B)
  2. [ ] Add `publish.container` section to `.hyperi-ci.yaml`
  3. [ ] Update ci submodule to v1.59.0+
  4. [ ] Update publish workflow for container inputs
  5. [ ] Test: trigger release, verify `ghcr.io/hyperi-io/dfe-receiver`
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

- [ ] Binary build local test -- blocked on CI submodule libsasl2-dev multiarch fix

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
