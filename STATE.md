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

## Project Overview

### Architecture

Native Rust receiver that:

1. Receives JSON data over HTTP(S) and gRPC from Vector sinks and other sources
2. Validates JSON format and optional required fields
3. Routes to Kafka topics OR direct to dfe-loader based on configurable expressions
4. Batches messages (10K / 8MiB / 20ms) with disk spillover
5. Supports header auth, bearer tokens, and mTLS authentication

### Key Components

1. **HTTP Server** - axum-based with TLS termination and auth middleware
2. **gRPC Server** - tonic-based for Vector sink protocol (planned)
3. **Router** - Zero-copy JSON field extraction for topic routing
4. **TieredSink** - Disk spillover using hyperi-rustlib CircuitBreaker
5. **BearerTokenProvider** - Dynamic token loading from secret managers

### Tech Stack

- **Language:** Rust
- **HTTP Framework:** axum 0.7
- **gRPC Framework:** tonic 0.12
- **JSON Processing:** sonic-rs (SIMD)
- **Kafka:** rdkafka
- **Shared Library:** hyperi-rustlib (config, secrets, metrics, tiered-sink)

---

## Key Decisions

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

### No Disk Spillover

**Decision:** Use in-memory buffering only, no disk spillover
**Rationale:** For PB/s scale ingestion:

1. K8s memory limits trigger OOMKill -> KEDA scales up (desired behavior)
2. Vector clients have their own disk buffer for retries
3. Circuit breaker + 503 responses propagate backpressure upstream
4. Disk I/O would bottleneck the hot path at scale
5. Simpler architecture, no persistent volumes needed

**Alternatives considered:** Disk spillover via hyperi-rustlib Spool (rejected - adds complexity without benefit when clients handle retries)

---

## External Dependencies

- **hyperi-rustlib** - Shared library for config, secrets, metrics, tiered-sink
- **rdkafka** - Kafka producer with batching support
- **OpenBao/Vault** - Secret management (optional, via hyperi-rustlib)
- **AWS Secrets Manager** - Secret management (optional, via hyperi-rustlib)

---

## Resources

**Documentation:**

- [SCOPE.md](SCOPE.md) - Project scope and requirements
- [config.example.yaml](config.example.yaml) - Configuration reference

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
