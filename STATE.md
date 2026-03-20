## Shared Build Host

This host runs multiple projects concurrently. **Never kill cargo processes** to
free the build lock — other projects may be building. Wait for the lock naturally.

---

# Project Context

**Project:** dfe-receiver
**Purpose:** High-performance HTTP/gRPC receiver for PB/s scale data ingestion

> **Note:** The `ai/` submodule provides standards and configuration - not code
> to import. Your project never imports or links to it.

---

## DO NOT ADD TO THIS FILE

**The following belong elsewhere:**

| Data | Correct Location |
|------|------------------|
| Version numbers | `VERSION` file, `git describe --tags` |
| Tasks/Progress | `TODO.md` |
| Session history | Git log (`git log --oneline -10`) |
| Changelog | `CHANGELOG.md` (semantic-release) |
| Dates | Git commit timestamps |

**This file is for static project context only.**

---

## CI Workflow Rules

> **ALWAYS run `hyperi-ci check` before pushing.**
>
> This runs quality (fmt, clippy) and tests locally. Fix all issues before pushing.
> Do NOT use CI as a build validator — it consumes shared runner time and creates noise.

---

## Project Overview

### Architecture

Native Rust receiver that:

1. Receives JSON data over HTTP(S) and gRPC from Vector sinks and other sources
2. Validates JSON format and optional required fields
3. Routes to Kafka topics OR direct to dfe-loader based on configurable expressions
4. Batches messages (10K / 8MiB / 20ms) with in-memory buffering
5. Supports header auth, bearer tokens, and mTLS authentication

### Key Components

1. **HTTP Server** - axum-based with TLS termination and auth middleware
2. **gRPC Server** - tonic-based Vector sink protocol with protobuf-to-JSON conversion
3. **OTLP Server** - gRPC (port 4317) + HTTP (port 4318) for OpenTelemetry logs/metrics/traces
4. **Lumberjack/Beats Server** - TCP/TLS listener (port 5044), Lumberjack v2 frame parser with zlib decompression
5. **Splunk HEC Server** - axum HTTP (port 8088), `/services/collector/event` + `/raw` + `/health`
6. **Syslog Server** - UDP (514) + TCP (514) + TLS/TCP (6514), RFC 5424+3164 auto-detect, RFC 6587 framing
7. **Fluent Forward Server** - TCP listener (port 24224), msgpack via rmpv, Message/Forward/PackedForward modes, chunk ACK
8. **GELF Server** - TCP listener (port 12201), null-byte delimited JSON, GELF 1.1 validation
9. **Prometheus Remote Write Server** - HTTP (port 9091), snappy+protobuf decode, native/otel/hyperdx modes
10. **Router** - Zero-copy JSON field extraction for topic routing
11. **TieredSink** - In-memory buffering with hyperi-rustlib CircuitBreaker
12. **BearerTokenProvider** - Dynamic token loading from secret managers
13. **ProtocolHandler trait** - Pluggable protocol handler abstraction (`src/server/traits.rs`)

### Tech Stack

- **Language:** Rust
- **HTTP Framework:** axum 0.8
- **gRPC Framework:** tonic 0.14
- **JSON Processing:** sonic-rs 0.5 (SIMD)
- **Kafka:** hyperi-rustlib KafkaProducer (wraps librdkafka internally)
- **Shared Library:** hyperi-rustlib (config, secrets, metrics, tiered-sink, kafka)

---

## Key Decisions

### No MSRV Pinning (Pre-OSS)

**Decision:** Remove `rust-version` from Cargo.toml — build against latest stable Rust
**Rationale:** While the project is internal/FSL-licensed, there's no benefit to pinning MSRV. It constrains dependency upgrades (e.g. sysinfo 0.38 requires 1.88) and prevents using new language features. MSRV will be pinned when the project is open-sourced and needs to support a wider range of toolchains.
**Alternatives considered:** Pinning to 1.75/1.82 (rejected — causes duplicate deps when rustlib uses newer crates)

### Use hyperi-rustlib CircuitBreaker

**Decision:** Use CircuitBreaker from hyperi-rustlib instead of custom implementation
**Rationale:** Reduces code duplication, maintains consistency across HyperI projects, provides well-tested half-open state support
**Alternatives considered:** Custom CircuitBreaker (initially implemented, then removed)

### Bearer Token Storage

**Decision:** Load bearer tokens from secret managers (OpenBao/Vault, AWS Secrets Manager, files)
**Rationale:** Production security requires dynamic token rotation without restarts; static tokens only for dev
**Alternatives considered:** Static config only (rejected for production use cases)

### Secret Source Format

**Decision:** Use "provider:path:key" format for secret sources
**Rationale:** Simple, parseable format that supports all providers; similar to URI schemes
**Examples:**

- `file:/etc/secrets/tokens`
- `vault:secret/data/auth:bearer_tokens`
- `aws:prod/auth/tokens:bearer`

### Disable hyperi-rustlib Secrets Cache for Token Refresh

**Decision:** Disable disk-persistent cache when loading bearer tokens via `SecretsManager`
**Rationale:** hyperi-rustlib v1.4.3 caches secrets to `~/.cache/hyperi-rustlib/secrets/` with 3600s TTL. Even new `SecretsManager` instances check this shared disk cache, returning stale data during token refresh. Setting `CacheConfig { enabled: false }` ensures each refresh reads the source fresh.
**Impact:** `BearerTokenProvider::load_from_secret()` in `src/server/auth.rs`

### Pluggable Protocol Handlers (ProtocolHandler Trait)

**Decision:** Define a `ProtocolHandler` trait in `src/server/traits.rs` that all protocol servers implement
**Rationale:** Enables adding new protocols (OTLP, Prometheus RW, Syslog, etc.) without modifying server orchestration. Each handler is independently enabled via config and spawned concurrently.
**Interface:** `name()`, `bind_address()`, `start(shutdown)`, `is_healthy()`

### OTLP Dual-Mode Conversion (Approach C)

**Decision:** OTLP conversion supports two modes -- `hyperdx` (default) and `generic`
**Rationale:** HyperDX mode produces JSON matching the OTel ClickHouse exporter schema (`otel_logs`, `otel_traces`, `otel_metrics_*` tables), allowing dfe-receiver to replace the Go OTel Collector in the HyperDX stack. Generic mode produces a normalised JSON envelope for custom pipeline routing.
**Config:** `otlp.mode: "hyperdx"` or `otlp.mode: "generic"`
**Routing:** Logs -> `otel_logs_land`, Traces -> `otel_traces_land`, Metrics -> `otel_metrics_land`

### Vendored OTLP Protos

**Decision:** Vendor opentelemetry-proto v1.5.0 files into `proto/opentelemetry/` and compile with tonic-build
**Rationale:** Avoids pulling in the full `opentelemetry-proto` crate which depends on the entire OTel SDK (opentelemetry, opentelemetry_sdk, etc.). Vendoring keeps the dependency tree minimal and matches the existing Vector proto pattern.
**Alternatives considered:** `opentelemetry-proto` crate (rejected -- brings in full OTel SDK as transitive deps)

### Optional Disk Spillover (Opt-In)

**Decision:** Disk spillover is available but disabled by default (`buffer.spillover.enabled: false`)
**Rationale:** In-memory buffering is the default for PB/s scale ingestion where K8s OOMKill + KEDA handles scaling. However, for deployments that need crash-resilient buffering or operate outside K8s, disk spillover via rustlib's `TieredSink` is available as opt-in.
**Implementation:** `SinkBackend` enum in `src/buffer/mod.rs` wraps either `InMemoryBuffer` (default) or rustlib's `TieredSink` with a `RustlibSinkAdapter` that encodes topic+payload as `[u32 LE topic_len][topic][payload]` for spool storage.
**Config:** `buffer.spillover.enabled`, `buffer.spillover.path`, `buffer.spillover.max_usage_percent`

---

## External Dependencies

- **hyperi-rustlib** - Shared library for config, secrets, metrics, tiered-sink, KafkaProducer (published to crates.io)
- **librdkafka** - Kafka producer (accessed via hyperi-rustlib KafkaProducer, not linked directly)
- **OpenBao/Vault** - Secret management (optional, via hyperi-rustlib)
- **AWS Secrets Manager** - Secret management (optional, via hyperi-rustlib)
- For unsupported protocols, use Vector as a sidecar pushing to gRPC ingest (:6000)

---

## Resources

**Documentation:**

- [config.example.yaml](config.example.yaml) - Configuration reference
- [docs/DESIGN.md](docs/DESIGN.md) - Architecture and design

**External Resources:**

- [hyperi-rustlib secrets module](/projects/hyperi-rustlib/src/secrets/) - Secret provider implementations
- [dfe-loader routing](/projects/dfe-loader/src/routing/) - Reference for zero-copy routing patterns

---

## Notes for AI Assistants

This file contains **static project context only**.

**DO NOT add:**

- Version numbers (use `git describe --tags`)
- Progress/tasks (use `TODO.md`)
- Dates or session history (use `git log`)
- "Current Session" or "Last Session" sections

**DO add:**

- Architecture decisions and rationale
- Key component descriptions
- External dependencies
- How things work (not what's happening)

When in doubt, ask: "Will this be true next week?" If no, it doesn't belong here.
