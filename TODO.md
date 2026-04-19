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

### Performance Review

Audit applicable optimisations from [dfe-loader/docs/PERFORMANCE.md](/projects/dfe-loader/docs/PERFORMANCE.md).

- [ ] Allocator: enable `jemalloc` or `mimalloc` feature, benchmark vs system glibc on representative workload
- [ ] Build profile: confirm `lto = "thin"`, `codegen-units = 1`, `panic = "abort"`, `strip = true` in release
- [ ] Profile under load (perf, flamegraph, jeprof) — record baseline for regression detection
- [ ] PGO + BOLT: evaluate ROI for production binary (10-20% + 5-15% gain)
- [ ] Batch tuning: validate buffer/flush thresholds align with rustlib Kafka transport (10K recv / 20K prefetch)

### Security Hardening + Test Coverage + Dep Update [DONE]

**rustlib bump and compile fixes**

- [x] Bump `hyperi-rustlib` from `>=2.4.3` to `>=2.5.4` (resolves to 2.5.4)
- [x] Handle new `SendResult::FilteredDlq` variant in `src/sink/grpc/mod.rs`

**Security fixes (from code + security review)**

- [x] **Critical**: bearer token validation — store SHA-256 hashes instead
      of plaintext tokens (eliminates timing-attack side channel; uses
      `ring::digest`)
- [x] **Critical**: Fluent Forward unbounded buffer guard (OOM prevention;
      respects `max_message_size`)
- [x] **Important**: Prometheus RW snappy decompression bomb guard
      (64 MiB cap via `snap::decompress_len` pre-check)
- [x] **Important**: SASL password redaction in `Debug` output
      (custom `Debug` impl emits `***REDACTED***`)

**Dependency updates**

- [x] `compact_str` 0.8 → 0.9, `criterion` 0.5 → 0.8 (breaking — required
      `std::hint::black_box` migration), `ipnet-trie` 0.2 → 0.3,
      `reqwest` 0.12 → 0.13 (dev), `rdkafka` 0.39 (dev), `cargo update`
      for aws-lc, clap, hyper
- [x] Renovate PRs handled: #27 merged, #20 merged, #21 closed
      (superseded), #28 auto-closed
- [x] Dependabot alerts handled: #1/#2/#3 (aws-lc-sys) fixed by update,
      #5 (rand) dismissed — we're on 0.9.4, past 0.9.3 patch

**Test coverage (408 → 489 tests)**

- [x] Unit: timing-attack resistance, hash consistency, unicode tokens,
      SASL Debug redaction, Snappy bomb simulation, OTLP convert (13 new),
      Pipeline edge cases (12 new), Error response mapping (10 new)
- [x] Integration via testcontainers-rs (auto-lifecycle, no manual cleanup):
    - Kafka sink round-trip (6 tests: send, batch, binary, large,
      multi-topic, recovery)
    - gRPC loader sink with in-process server (6 tests: delivery, order,
      large payload, unreachable, recovery, concurrency)
    - Vault bearer tokens (9 tests: file provider variants + live Vault
      container)
    - MinIO S3 container smoke (2 tests)
    - Protocol → Kafka roundtrip: Prom RW, Splunk HEC, HTTP (3 tests)
- [x] Test helper `tests/common/mod.rs` extended with live-or-testcontainers
      pattern (env detection → Docker fallback, skip gracefully if no Docker)

### Clean Up Stale Branches [DONE]

- [x] Delete local branches: `release`, `chore/merge-to-release-v1.14.2`,
      `fix/merge-to-release`, `fix/merge-to-release-v1.15`
- [x] Prune stale remote refs (`git remote prune origin`)

---

## Follow-up / Tracked (Not this session)

### Security hardening (tracked for follow-up)

- [ ] X-Forwarded-For trusted-proxy config (currently trusts unconditionally).
      Requires new config field `server.trusted_proxies: Vec<IpNet>` and
      update to `extract_client_ip` + rate limiter key extractor.
- [ ] Apply `IpFilter` to Splunk HEC + Prometheus RW handlers (currently
      they call `IpFilter::disabled()`). Pass server-level filter through.
- [ ] Slowloris protection for Splunk HEC + Prom RW plain-HTTP paths
      (currently fall back to bare `axum::serve`; should use
      `hardened_http_builder`).
- [ ] Connection semaphore for Syslog/Fluent/GELF TCP listeners (unbounded
      concurrent connections today).
- [ ] TLS 1.3-only / cipher-suite config for FIPS/CNSA 2.0 compliance.

### Coverage gaps (can be closed with more integration work)

- [ ] End-to-end TLS/mTLS cert rotation integration tests
- [ ] Full HTTP server integration tests (currently handler-level only)
- [ ] Syslog UDP/TCP/TLS integration tests (fluent-bit/logger binary based,
      exists but coverage could expand)

### Environment / Credentials notes for operator (DEREK)

**Verified stale credentials (2026-04-16, tested via `kcat` / `bao`):**

| Secret | Location | Status | Fix |
|---|---|---|---|
| `KAFKA_SASL_PASSWORD` | `.env` (admin user) | ❌ Rejected by broker | Rotate SCRAM-SHA-512 at broker, update `.env` |
| `VAULT_TOKEN` / `OPENBAO_TOKEN` | shell env | ❌ 403 on `lookup-self` | Run `bao login -method=oidc` to get fresh OIDC token |

**Kafka brokers reachable** at `kafka.devex.hyperi.io:32089` (TCP OK), only auth
layer failing. **OpenBao server reachable** at `bao.devex.hyperi.io:8200` but
token lacks permissions (likely expired; OpenBao default token TTL is 32 days).

**To re-establish credentials:**

1. **OpenBao** (do this first — everything else comes from here):
   ```bash
   unset OPENBAO_TOKEN VAULT_TOKEN
   bao login -method=oidc
   # Then export the new token into shell rc files
   ```

2. **Kafka SCRAM password** (kept under `services/kafka:admin_password` in
   OpenBao per `/projects/hyperi-infra/ansible/inventories/prod/group_vars/all/vault.yml`):
   ```bash
   bao kv get -field=admin_password services/kafka
   # Update dfe-receiver/.env KAFKA_SASL_PASSWORD with value
   ```
   If the bao value is also stale, rotate at the broker:
   ```bash
   kubectl -n kafka exec -it my-cluster-kafka-0 -c kafka -- \
     bin/kafka-configs.sh --bootstrap-server localhost:9092 \
     --entity-type users --entity-name admin --alter \
     --add-config 'SCRAM-SHA-512=[password=<NEW_PW>]'
   bao kv put services/kafka admin_password=<NEW_PW>
   ```

3. **Consider dedicated test user**: create `dfe-receiver-test` SCRAM user
   with ACLs limited to `dfe-receiver-test-*` topics (principle of least
   privilege) instead of reusing `admin`.

**Impact on test suite today:** none. Tests use `kafka_backend()` which tries
live Kafka first, then auto-falls-back to a fresh testcontainers Kafka.
Live creds becoming valid again will make tests run faster (skip container
startup). Containers are dropped at test end — no leftover Docker state.

**Credentials policy going forward:**
- All credentials **only** in project `.env` (never in code, docs, or CLAUDE.md)
- `.env` is in `.gitignore`; `.env.example` may be committed with empty values
- Tests read from env + `.env` (via `dotenvy`), never hardcoded
- Gitleaks runs as part of `hyperi-ci check` — catches accidental commits

## Previously Active

### ~~Migrate to Single Versioning on Main~~ [DONE]

- [x] Replace `.releaserc.json` with `.releaserc.yaml` (main only, all commit types)
- [x] Create `scripts/set-version.py`, `.githooks/commit-msg`
- [x] Update `ci.yml` — workflow_dispatch tag input, publish-target: both
- [x] Fix VERSION + Cargo.toml to `1.14.4`, force-move tag
- [x] CI green, semantic-release created v1.14.5 (clean, no `-dev.N`)
- [x] Release branch deleted, stale merge branches cleaned
- [x] v1.14.5 published to GH Release + R2 (amd64 + arm64 binaries)

### ~~Consume hyperi-rustlib v1.20.0~~ [DONE]

- [x] Bump hyperi-rustlib to `>=1.20.0`, updated to `1.20.1`
- [x] Fix Transport trait split (TransportBase, TransportSender, TransportReceiver)
- [x] Add version check on startup

### ~~Code Review (v1.14.5)~~ [DONE]

- [x] Run code review — found 1 critical, 5 important, 4 suggestions
- [x] Remove `rust-version` from Cargo.toml (matches no-MSRV-pin decision)
- [x] Fix tautological test `test_pipeline_validation_reject`
- Tracked for later: request_duration_seconds histogram, active_connections gauge wiring

### ~~Remove dead plugin code~~ [DONE]

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

- [x] `tracing-throttle` layer, opt-in via `LOG_THROTTLE_ENABLED`
- [x] Helper functions: `log_state_change()`, `log_sampled()`, `log_debounced()`
- [x] Published as part of rustlib `v1.16.3`

#### Phase 2: rustlib — Standard DFE Metrics Framework [DONE]

- [x] `DfeMetrics` struct with `dfe_transport_*`, `dfe_pipeline_*`, `dfe_records_*`, `dfe_scaling_*`
- [x] Transport label support, tiered architecture
- [x] Published as part of rustlib `v1.16.3`

#### Phase 2.5: rustlib — Security Logging Framework [DONE]

- [x] `SecurityEvent` builder + 10 convenience functions (OWASP-aligned)
- [x] `target: "security"` routing, level-mapped (info/warn/error)
- [x] Data quality events: `record_dlq`, `data_quality_alert`
- [x] Service name + version in JSON log output (auto via DfeApp)
- [x] Published as part of rustlib `v1.16.3`

#### Phase 2.75: rustlib — Flat Env Override Helpers [DONE]

- [x] `ApplyFlatEnv` + `Normalize` traits
- [x] Runtime helpers: `flat_env_string`, `flat_env_list`, `flat_env_bool`, `flat_env_parsed`
- [x] `load_config<T>()` generic cascade function
- [x] Published as part of rustlib `v1.16.3`

#### Phase 3: dfe-receiver — Consume New rustlib [DONE]

- [x] Bumped rustlib to `>=1.16.3`, removed `[patch.crates-io]`
- [x] `DfeMetrics` dual-emit (old `receiver_*` + new `dfe_*`)
- [x] Log spam helpers wired into 5 hot spots
- [x] Security events wired into auth, TLS, config reload, validation
- [x] `ApplyFlatEnv` migration (removed 153 lines of bespoke env override code)
- [x] KEDA PromQL updated to `dfe_scaling_pressure`
- [x] 14 hardening tests added (rate limit, IP filter, backpressure, slowloris, metrics)
- [x] CI green, 404 tests passing

#### dfe-receiver Log Spam Fixes (remaining)

- [ ] `src/pipeline/mod.rs:205,315` — memory pressure warn → state-transition
- [ ] `src/sink/kafka/mod.rs:61` — Kafka send error → sampled (1/1000) + metric
- [ ] `src/sink/loader/mod.rs:95` — loader send error → sampled (1/1000) + metric
- [ ] `src/server/syslog/mod.rs:73` — UDP recv error → debounced (5s)
- [ ] `src/server/lumberjack/mod.rs:94,135` — frame parse error → sampled (1/100)

#### dfe-receiver Metrics (remaining)

- [ ] Add histograms: `dfe_transport_send_duration_seconds`
- [ ] Add `request_duration_seconds` histogram with `transport` label
- [ ] Wire `active_connections` gauge to Prometheus

### Bespoke Code Dedup (receiver vs rustlib) [PARTIAL]

- [x] `apply_env_overrides()` replaced with rustlib `ApplyFlatEnv` trait
- [x] Security logging via rustlib `SecurityEvent` (not bespoke)
- [x] Hand-rolled `Metrics` struct — replaced with metric groups + MetricsManager (v1.14.2)
- [x] `RateWindow` — replaced with rustlib `scaling::RateWindow` (v1.14.1)
- [x] `BufferManager` memory detection — replaced with rustlib `MemoryGuard` (v1.14.1)

### ~~Security Logging Standard (rustlib)~~ [DONE]

- [x] `SecurityEvent` builder + convenience functions in rustlib
- [x] Wired into dfe-receiver (auth, TLS, config reload, validation, token rotation)

### Smart Log Combining (rustlib enhancement)

- [ ] Enhance `tracing-throttle` integration — configure `exclude_fields` for high-cardinality fields
  - Already supported: `exclude_fields(&["request_id", "span_id", "topic"])` strips variable parts before signature hash
  - The message template (format string) becomes the grouping key — identical template = same group regardless of field values
- [ ] Add collapsed summary on throttle: "N occurrences suppressed in last Xs" (tracing-throttle may support this natively)
- [ ] Consider post-collection pattern detection for dashboards (Grafana Loki pattern detection, not code-level)

### Hot Path Optimisation

- [ ] Pre-compute topic strings in Router (eliminate `format!()` per-message)
- [ ] Pre-split field paths at Router/Validator construction (eliminate `.split('.').collect()` per-message)

### Internet-Facing Hardening [DONE]

- [x] Slowloris protection — refactored plain HTTP server from `axum::serve` to hyper low-level APIs; 5s `header_read_timeout` on all paths (TLS + plain)
- [x] Connection idle timeout — 60s idle timeout via hyper HTTP/1 keepalive + HTTP/2 keep_alive_timeout
- [x] Concurrency limit — `GlobalConcurrencyLimitLayer` with configurable `max_concurrent_requests` (default 10,000)
- [x] 503 backpressure on HTTP ingest — `pipeline.is_ready()` check before processing, returns 503 + `Retry-After: 5`
- [x] 503 backpressure on gRPC ingest — `pipeline.is_ready()` check, returns `Status::unavailable`
- [x] Hardened hyper builder (`hardened_http_builder()`) shared between TLS and plain paths
- [x] HARDENING.md — infrastructure fronting architecture doc (Envoy Gateway, Cloudflare, NLB, CrowdSec, cost analysis)
- [x] Per-IP rate limiting — tower-governor 0.8 (GCRA), SmartIpKeyExtractor (X-Forwarded-For aware), opt-in via `server.rate_limit`
- [x] IP allowlist/denylist — ipnet-trie CIDR matching, connection-level reject (before TLS handshake), opt-in via `server.ip_filter`
- [x] Fixed test_bearer_auth_file_refresh flake — fresh reqwest client after 5s sleep to avoid stale keep-alive killed by header_read_timeout

### Medium Priority

- [ ] Fix Helm `chart/templates/secret.yaml` — `bearer-tokens` hyphen in Go template field name
- [x] Remove `[patch.crates-io]` from Cargo.toml — done, building against crates.io `v1.16.3`
- [ ] `#[derive(FlatEnvOverrides)]` proc macro (Phase 2 of flat env spec — currently manual impls)

### Low Priority

- [x] Migrate hand-rolled Prometheus text render to rustlib MetricsManager (done in metrics migration v1.14.2)

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

---

## Rust Release-Track Optimisation (hyperi-ci Tier 1/2)

**Context:** hyperi-ci is shipping channel-gated build optimisations for Rust
binaries. See `hyperi-ai/standards/languages/RUST.md` — *Release-Track Build
Optimisation (hyperi-ci)* — and `hyperi-ai/standards/infrastructure/CI.md` —
*Channel-Tiered Build Optimisation*. Local `cargo build` is unaffected.

### Tier 1 prep (automatic at beta+/release once hyperi-ci ships)

Current state: **✅ READY — no source changes required.**

- [x] `Cargo.toml` has `[features] jemalloc` + `mimalloc` declared
- [x] `main.rs` wires `#[global_allocator]` under `#[cfg(feature = "jemalloc")]`
- [x] `default = ["otlp"]` — no allocator in default (clean)
- [x] `[profile.release] lto = "thin"` — CI overrides to `fat` on beta+

**Next release push will automatically build with:**
- `--features jemalloc` at `beta` / `release` channels
- `CARGO_PROFILE_RELEASE_LTO=fat` at `beta` / `release` channels

No action required from the project team. Just verify the next `release`-channel
binary is jemalloc-linked (`nm target/<target>/release/dfe-receiver | grep -i
jemalloc` should show symbols).

### Tier 2 opt-in (PGO + BOLT — release channel only)

Current state: **⚠️ NOT CONFIGURED — opt-in required.**

- [ ] Decide whether PGO is worth the +30-60 min release build time
- [ ] If yes: write `scripts/pgo-workload.sh` that performs **actual HTTP/gRPC
      traffic** against the receiver — at least 5 min sustained load with
      representative message mix (syslog, OTel, Vector protocols)
- [ ] **PGO workload MUST NOT be a port check or startup probe** — a bad
      workload causes NEGATIVE PGO gains (the compiler optimises for the wrong
      hot paths). Send realistic payloads at realistic rates.
- [ ] Add to `.hyperi-ci.yaml`:
  ```yaml
  build:
    rust:
      optimize:
        pgo:
          enabled: true
          workload_cmd: "bash scripts/pgo-workload.sh"
          duration_secs: 300
        bolt:
          enabled: true    # Linux only, +5-15% on top of PGO
  ```

---

## Role: Canary Test Project for hyperi-ci Release-Track Optimisation

**dfe-receiver is the designated end-to-end test project for the new hyperi-ci
channel-tiered build optimisation feature.** It was chosen because:

- It's a real shipped binary (not a library, not a dev tool)
- Its `Cargo.toml` + `main.rs` are already correctly prepared for Tier 1
- Its release pipeline (GH Releases + R2) is proven and stable
- Its workflow is representative of other DFE binaries — lessons learned here
  apply across dfe-loader, dfe-archiver, dfe-fetcher, etc.

### What this means in practice

- [ ] Once hyperi-ci ships the Tier 1 feature, push a trivial change
      (comment-only is fine) and verify the `release`-channel publish binary
      has jemalloc linked
- [ ] Validate: `nm target/<target>/release/dfe-receiver | grep -i jemalloc`
      should show jemalloc symbols on the published binary
- [ ] Validate: build logs should show
      `CARGO_PROFILE_RELEASE_LTO=fat` and `--features jemalloc` on the
      `release` channel build
- [ ] Any bugs found in the hyperi-ci optimisation handler (wrong flag, wrong
      env var, feature detection issues, etc.) are fixable directly in
      **hyperi-ci** — this project is authorised to change hyperi-ci's
      `src/hyperi_ci/languages/rust/build.py` and related code to unblock the
      test. Fixes should be committed to hyperi-ci main, published, then
      retested here.
- [ ] Record observations in this TODO.md under a new "Canary run notes"
      subsection as each test cycle runs

### Canary run notes

#### 2026-04-19 — Tier 2 v1.15.7 release: PGO green, BOLT silently skipped

Full release-channel dispatch through hyperi-ci v1.10.1 completed successfully end-to-end (Quality → Test → Build amd64 + arm64 → Container → Publish). Build log confirmed:

- `Rust build optimisation: channel=release, allocator=jemalloc, lto=fat, pgo=on, bolt=on`
- `cargo pgo build` instrumented build: 5m 31s
- Workload ran 300s (pgo-driver against real testcontainer Kafka)
- Profile size: 5.17 MiB (well above `min_profile_bytes` guard)
- `cargo pgo optimize` final build: 4m 04s ✓
- **BOLT step: `llvm-bolt not installed — skipping BOLT step`**

Shipped binary (`dfe-receiver-linux-amd64`, 11.9 MB):
- jemalloc strings present (`jemalloc_bg_thd`, `jemalloc`, `<jemalloc>` format markers) — **Tier 1 + PGO confirmed**
- BOLT-specific sections stripped (`strip = true`) — can't verify from binary, log is authoritative

Root cause of BOLT skip: Ubuntu noble's `llvm` metapackage doesn't ship the `llvm-bolt` binary. It lives in the separate `bolt-NN` package (post-link optimizer) at `/usr/bin/llvm-bolt-NN` with version suffix only — no unversioned symlink. `shutil.which("llvm-bolt")` returns None, so `_ensure_llvm_bolt_available()` returns False and BOLT was (correctly, non-fatally) skipped.

**Fix shipped to hyperi-ci v1.10.2**:
- `native-deps/rust.yaml`: replace `llvm` → `bolt-21` (latest LLVM/BOLT for Rust; C++/ClickHouse fork keeps its own pinned toolchain)
- `pgo.py`: `_ensure_llvm_bolt_available()` now falls back to versioned names (llvm-bolt-18..30), creates `~/.local/bin/llvm-bolt` symlink pointing at whichever it finds, and prepends to PATH so cargo-pgo's bolt subcommand resolves the unversioned name it invokes

Re-dispatch with hyperi-ci v1.10.2 expected to complete full Tier 2 (PGO + BOLT) with ~1.5 min additional build time from the BOLT apply step.

#### 2026-04-17 — Tier 1 local validation + PGO workload shipped (local)

Context: this session closed the canary loop from "ready in theory"
(v1.14.5-era claim) to "ready in practice with concrete numbers".
hyperi-ci v1.8.0 shipped the Tier 1+2 handler code around this work;
dfe-receiver's `.hyperi-ci.yaml` opt-in + PGO workload are committed
locally (not pushed — waiting for hyperi-ci to be production-released).

**Binary + symbol validation (on v1.14.10 source)**:

| Allocator | Binary size | Delta | Linkage detection |
|---|---|---|---|
| System (glibc) | 14,051,296 B (13.4 MB) | baseline | 0 strings |
| **jemalloc** | **14,542,432 B (13.9 MB)** | **+491 KB (+3.5%)** | 39 `jemalloc`/`je_mallctl` strings (static) |
| mimalloc | 14,185,664 B (13.5 MB) | +134 KB (+1.0%) | 7 `mimalloc`/`mi_option` strings (static) |

**Method note:** `strip = true` in `[profile.release]` means `nm` returns 0
symbols. Use `strings <binary> | grep -ciE 'jemalloc|je_mallctl'` (`> 0`
on a jemalloc build) or `strings | grep -ciE 'mimalloc|mi_option'`
(for mimalloc).

**Criterion micro-bench deltas** (jemalloc vs system, same hardware):

- `json_validation/small`:  −4.4%
- `json_validation/medium`: −1.9%
- `json_validation/large`:  **−7.2%**
- `field_extraction/nested`: −3.7%
- `field_extraction/top_level`: +4.5% (within noise)
- `router/route_default`: +1.2% (within noise)

Headline: jemalloc wins on allocation-heavy paths (json parse),
within noise on zero-alloc paths (router). Confirms standardising
on jemalloc (DFE-wide policy) over mimalloc, which showed smaller
wins on the same benches.

**PGO workload shape** (shipped in this session):

- `scripts/pgo-workload.sh` — bash orchestrator: spins up
  apache/kafka KRaft container, writes ephemeral all-listeners
  config, starts the instrumented binary, waits for `/health/ready`,
  runs the driver for `PGO_WORKLOAD_DURATION_SECS` (default 300,
  hard floor 60), cleans up on EXIT trap
- `src/bin/pgo-driver.rs` — feature-gated (`pgo-driver`) Rust binary,
  drives HTTP JSON, Prometheus RW (snappy+protobuf), Splunk HEC,
  OTLP HTTP (protobuf), Syslog UDP/TCP. Uses the project's own
  vendored OTLP + Prom RW proto types for zero schema duplication.
  6 MB release binary.
- `docs/PERFORMANCE.md` — full audit vs dfe-loader's PERFORMANCE.md
  with Applied/Applicable/Loader-specific/Tracked classification.

**Propagated to other DFE Rust projects** (local commits in each
repo): dfe-loader, dfe-archiver, dfe-fetcher, dfe-transform-wasm,
dfe-transform-vrl, dfe-transform-vector — each TODO.md now has the
dated lessons block pointing at the hyperi-ci consumer docs +
workload templates.

**Pending (deferred to hyperi-ci release readiness)**:

- [ ] Push dfe-receiver's locally committed opt-in (`.hyperi-ci.yaml`
      PGO/BOLT enabled; `docs/PERFORMANCE.md`; `scripts/pgo-workload.sh`;
      `src/bin/pgo-driver.rs`)
- [ ] Trigger a release-channel dfe-receiver build and verify:
      `--features jemalloc` in cargo invocation,
      `CARGO_PROFILE_RELEASE_LTO=fat` in env,
      `cargo pgo build` + `cargo pgo optimize` steps,
      (Linux) `cargo pgo bolt` step,
      `strings <binary> | grep jemalloc` non-empty on published binary

---

## POLICY UPDATE 2026-04-17 — Jemalloc at every channel, drop mimalloc

**Allocator policy changed:** DFE binaries now standardise on jemalloc at
**every** channel (spike/alpha/beta/release). mimalloc is no longer a
supported option in DFE projects. See:

- `hyperi-ai/standards/languages/RUST.md` — *Allocator Policy* section
- `hyperi-ai/standards/rules/rust.md` — updated tier table

### Action items for this project

- [ ] Remove `mimalloc = ["dep:mimalloc"]` from `[features]` in `Cargo.toml`
- [ ] Remove `mimalloc = { version = "0.1", optional = true }` from `[dependencies]` in `Cargo.toml`
- [ ] Remove the `#[cfg(all(feature = "mimalloc", not(feature = "jemalloc")))]` fallback block from `src/main.rs` — keep only the jemalloc wiring
- [ ] Run `cargo build --release --features jemalloc` to verify the crate still builds
- [ ] Update code comments that reference mimalloc as an alternative

### What changes in CI behaviour

Previously: jemalloc was only applied at `beta`/`release` channels (spike
and alpha used system allocator).
Now: jemalloc is applied at **every** channel including spike/alpha.
Trade-off: +10s extra compile time per spike/alpha CI run in exchange
for consistent perf-trace symbols and unified `jeprof` tooling across
all builds.

### Learnings reference

Binary-size + verification learnings captured from past receiver canary work
are in `hyperi-ci/docs/RUST-RELEASE-TRACK-OPTIMISATION.md` (upstream):

- **Binary size:** jemalloc adds ~491 KB (+3.5%) on a 14 MB stripped baseline
- **Verification:** use `strings <binary> | grep -ciE 'jemalloc|je_mallctl'`
  NOT `nm` — release binaries are stripped
- **Cross-compile:** PGO profiles are arch-specific; amd64 CI workers
  produce profiles that don't generalise to arm64. PGO on arm64 cross-compile
  targets is currently skipped by hyperi-ci (logged)
- Troubleshooting table in that doc covers "allocator requested but feature
  not declared", "PGO profile data too small", BOLT skip reasons, etc.

Supersedes the earlier validation guidance in this file's *Role: Canary
Test Project* section that said to use `nm | grep jemalloc` — use
`strings` instead.
