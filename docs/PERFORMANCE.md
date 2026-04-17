# Performance Optimisation Guide

Performance optimisation guide for dfe-receiver covering build optimisations,
memory allocators, runtime tuning, and profiling workflows.

> **CI integration:** The release-channel binary published by hyperi-ci uses
> Tier 1 (jemalloc + fat LTO) automatically when the project opts in via
> `.hyperi-ci.yaml`. Tier 2 (PGO + BOLT) is additionally opt-in. The
> manual `cargo pgo` / allocator commands below are for local profiling
> and one-off investigations.
>
> See:
> - `hyperi-ai/standards/languages/rust.md` — *Release-Track Build
>   Optimisation (hyperi-ci)*
> - hyperi-ci docs (`RUST-RELEASE-TRACK-OPTIMISATION.md`,
>   `PGO-WORKLOAD-GUIDE.md`) — once hyperi-ci v1.8+ is shipped
> - `TODO.md` → *Rust Release-Track Optimisation* — per-project opt-in steps

## Quick status

| Optimisation | Status | Detail |
|---|---|---|
| `lto = "thin"` (default release) | ✅ Applied | `Cargo.toml` |
| `codegen-units = 1` | ✅ Applied | `Cargo.toml` |
| `panic = "abort"` | ✅ Applied | `Cargo.toml` |
| `strip = true` | ✅ Applied | `Cargo.toml` |
| `opt-level = 3` | ✅ Applied | `Cargo.toml` |
| jemalloc feature declared | ✅ Applied | mutually-exclusive via `#[cfg]` in `main.rs` |
| mimalloc feature declared | ✅ Applied | mutually-exclusive via `#[cfg]` in `main.rs` |
| `default = ["otlp"]` (no allocator in default) | ✅ Applied | Clean — no surprise allocator in non-opt-in builds |
| `lto = "fat"` on release channel | 📋 Tier 1 | via hyperi-ci opt-in |
| PGO | 📋 Tier 2 | via hyperi-ci opt-in |
| BOLT | 📋 Tier 2 | via hyperi-ci opt-in (Linux only) |
| Pre-computed topic strings (hot path) | ⏳ Tracked | `TODO.md` → *Hot Path Optimisation* |
| Pre-split field paths (hot path) | ⏳ Tracked | `TODO.md` → *Hot Path Optimisation* |

## Build optimisations

### Link-Time Optimisation (LTO)

Already configured in `Cargo.toml`:

```toml
[profile.release]
lto = "thin"           # "fat" applied by hyperi-ci on release channel
codegen-units = 1      # Better optimisation
panic = "abort"        # Smaller binary, no unwind tables
strip = true           # Strip symbols from published binary
opt-level = 3
```

**thin LTO** (current default) — moderate build-time cost, most optimisation
benefit. **fat LTO** — 2-3× longer compile, an additional 2-5% runtime
improvement. hyperi-ci applies fat LTO automatically on `beta` / `release`
channels when Tier 1 is opted-in.

### Profile-Guided Optimisation (PGO)

PGO provides **10-20% improvement** by using runtime profile data to guide
compiler optimisations. See [cargo-pgo](https://github.com/Kobzol/cargo-pgo).

**Automated via hyperi-ci** on `release` channel when `.hyperi-ci.yaml` has
`build.rust.optimize.pgo.enabled: true`. Workload: `scripts/pgo-workload.sh`
(see `docs/PGO-WORKLOAD.md`).

#### Local (manual) setup

```bash
cargo install cargo-pgo
rustup component add llvm-tools-preview

# 1. Build instrumented binary (jemalloc recommended alongside PGO)
cargo pgo build -- --features jemalloc

# 2. Run workload (minimum 60s, 300s recommended)
bash scripts/pgo-workload.sh \
    ./target/x86_64-unknown-linux-gnu/release/dfe-receiver

# 3. Build PGO-optimised binary
cargo pgo optimize build -- --features jemalloc
```

#### Workload requirements

A PGO workload MUST exercise the receiver's data-processing hot paths —
parse → validate → route → produce to Kafka sink. Port checks, health
probes, or trivial one-shot sends produce **negative PGO gains** because
the compiler mis-optimises startup paths over production hot paths.

See `scripts/pgo-workload.sh` (the orchestrator) and `src/bin/pgo-driver.rs`
(the load generator). Full workload-writing rules for any consumer
project live in hyperi-ci's `docs/PGO-WORKLOAD-GUIDE.md`.

### BOLT post-link optimisation

BOLT adds **5-15%** on top of PGO by reordering the code layout in the
final binary. Linux-only (x86_64 and AArch64).

```bash
# After PGO (Linux + BOLT installed)
cargo pgo bolt optimize --with-pgo -- --features jemalloc
```

hyperi-ci runs BOLT automatically on release channel when
`build.rust.optimize.bolt.enabled: true` AND PGO is enabled. Non-Linux
builds skip BOLT with a warning.

## Memory allocators

dfe-receiver supports jemalloc and mimalloc via mutually-exclusive
cargo features. Both are declared optional in `Cargo.toml`:

```toml
[features]
jemalloc = ["dep:tikv-jemallocator"]
mimalloc = ["dep:mimalloc"]
```

Wired in `src/main.rs` with `#[cfg]` gating so jemalloc wins if both are
enabled (defensive against `--all-features`):

```rust
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "mimalloc", not(feature = "jemalloc")))]
#[global_allocator]
static GLOBAL_MIMALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

### Binary size impact (dfe-receiver v1.14.10, stripped release build)

| Allocator | Binary size | Delta | Linkage verification |
|---|---|---|---|
| System (glibc) | 14,051,296 B (13.4 MB) | baseline | 0 allocator-specific strings |
| **jemalloc** | **14,542,432 B (13.9 MB)** | **+491 KB (+3.5%)** | 39 `jemalloc` / `je_mallctl` strings (static) |
| mimalloc | 14,185,664 B (13.5 MB) | +134 KB (+1.0%) | 7 `mimalloc` / `mi_option` strings (static) |

Because release builds have `strip = true`, `nm` can't see symbols — use
`strings <binary> | grep -ciE 'jemalloc|mi_option'` to verify linkage.

### Benchmark deltas (criterion micro-benchmarks)

Measured on `cargo bench --bench throughput` with `--save-baseline` per
allocator. These micro-benchmarks exercise parse/route/metrics hot paths
that do **minimal allocation per iteration** — they are poor allocator
discriminators. Expect larger gains on production load (full protocol
decoders + Kafka batch assembly + async task allocation).

| Benchmark | System (ns) | jemalloc (ns) | mimalloc (ns) | jemalloc vs sys | mimalloc vs sys |
|---|---|---|---|---|---|
| json_validation / small | 113.96 | 108.99 | 110.06 | **−4.4%** | −3.4% |
| json_validation / medium | 191.23 | 187.65 | 190.74 | −1.9% | −0.3% |
| json_validation / large (8 KB) | 642.28 | 596.06 | 627.87 | **−7.2%** | −2.2% |
| field_extraction / nested | 142.93 | 137.71 | 141.71 | −3.7% | −0.9% |
| field_extraction / top_level | 74.24 | 77.59 | 77.68 | +4.5% | +4.6% |
| router / route_default | 31.17 | 31.55 | 30.91 | +1.2% | −0.8% |
| router / route_5_rules_miss | 2078.7 | 2061.5 | 2199.5 | −0.8% | +5.8% |
| metrics / inc_requests_total | 1511.1 | 1479.0 | 1503.8 | −2.1% | −0.5% |
| metrics / add_bytes_received | 61.35 | 62.16 | 62.16 | +1.3% | +1.3% |

**Headline:** jemalloc wins on the allocation-heavier benchmarks
(json/large −7.2%, json/small −4.4%, field nested −3.7%) and is within
noise elsewhere. mimalloc is near-neutral. Conclusion: **jemalloc is
the production default for Tier 1** (matches dfe-loader's choice).

### Reproducing the baselines

```bash
# Capture each
cargo bench --bench throughput -- --save-baseline system-allocator
cargo bench --bench throughput --features jemalloc -- --save-baseline jemalloc
cargo bench --bench throughput --features mimalloc -- --save-baseline mimalloc

# Diff
cargo bench --bench throughput -- --baseline system-allocator
```

### Applicability audit (vs dfe-loader's PERFORMANCE.md)

| Optimisation | dfe-loader | dfe-receiver |
|---|---|---|
| `lto = "thin"` | ✅ | ✅ Already applied |
| `lto = "fat"` (via CI) | ✅ Tier 1 | ✅ Tier 1 (pending hyperi-ci) |
| jemalloc | ✅ Tier 1 | ✅ Tier 1 (pending hyperi-ci) |
| mimalloc (alternative) | ✅ | ✅ Available, jemalloc preferred |
| PGO | ✅ Tier 2 | ✅ Tier 2 (pending hyperi-ci, workload: `scripts/pgo-workload.sh`) |
| BOLT | ✅ Tier 2 | ✅ Tier 2 (pending hyperi-ci) |
| ClickHouse batch tuning (`flush_rows`, `flush_bytes`, `flush_age_secs`) | ✅ | ❌ Loader-specific (receiver produces to Kafka, not CH) |
| RowBinary vs JSONEachRow insert format | ✅ | ❌ Loader-specific |
| `max_concurrent_inserts` | ✅ | ↔ Receiver analogue: `max_concurrent_requests` (already tuned, GlobalConcurrencyLimitLayer, default 10,000) |

## Hot-path tuning (receiver-specific)

These are tracked in `TODO.md` → *Hot Path Optimisation* for when the
allocator win is banked and we move to code-level tuning.

### Pre-compute topic strings in Router

**Current**: `src/routing/mod.rs:197` does `format!("{source}{}",
self.topic_suffix)` when a source is dynamically extracted but not in
the pre-computed `source_to_topic` map. `format!()` allocates per
message — visible in `dhat` heap profiles under message-routing hot
path.

**Fix**: Pre-compute topic strings eagerly for bounded source sets, or
add a concurrent LRU cache (`DashMap` with capacity bound) to amortise
the `format!()` cost for unbounded sets.

### Pre-split field paths

**Current**: Field-path strings like `"tags.event.category"` are split
on every routing decision via `.split('.').collect::<Vec<_>>()`, which
allocates per call.

**Fix**: Pre-split at Router/Validator construction time into
`Arc<[Arc<str>]>` and store in the config struct.

## Runtime tuning

### Connection/request concurrency

`server.max_concurrent_requests` (default 10,000) — `GlobalConcurrencyLimitLayer`
caps in-flight requests on the HTTP ingest path. Tune down if backpressure
to the Kafka sink can't keep up (OOM risk otherwise).

### Per-IP rate limiting (tower-governor)

`server.rate_limit` — GCRA-based per-IP limit. Opt-in; disabled by default.
Not a performance tuning dial — it's a hardening dial for internet-facing
deployments. See `docs/HARDENING.md`.

### Kafka producer batching

`hyperi-rustlib`'s `KafkaProducer::HighThroughput` profile sets librdkafka
batching to **256 KiB batches, 100 ms linger, LZ4 compression**. These are
the hot-path throughput defaults. Override per-env via
`kafka.librdkafka_overrides` in config YAML if a specific cluster needs
different tuning.

### Memory backpressure (MemoryGuard)

Cgroup-aware memory headroom (`MEMORY_PRESSURE_THRESHOLD` default 0.80,
`MEMORY_CGROUP_HEADROOM` default 0.85). When pressure > threshold, HTTP
ingest returns 503 + `Retry-After: 5`. Tune via env vars documented in
`.env` comments.

## Profiling

### CPU profiling (perf + flamegraph)

Use the `profiling` profile which inherits release + keeps debug info:

```bash
cargo build --profile profiling --features jemalloc
perf record -g --call-graph dwarf ./target/profiling/dfe-receiver --config test-config.yaml
# In another terminal, drive load via scripts/pgo-workload.sh
perf report
```

Or for a flame graph directly:

```bash
cargo install flamegraph
cargo flamegraph --profile profiling --features jemalloc --bin dfe-receiver \
    -- --config test-config.yaml
# Open flamegraph.svg
```

### Memory profiling (jemalloc + jeprof)

```bash
export MALLOC_CONF="prof:true,prof_prefix:jeprof.out"
./target/release/dfe-receiver --config test-config.yaml
# Drive load, then:
jeprof --svg ./target/release/dfe-receiver jeprof.out.*.heap > heap.svg
```

### Heap profiling (dhat)

Feature-gate `dhat-heap` in `Cargo.toml` (not currently enabled — add if
allocation profiling becomes ongoing need).

## Recommended production build

**For production releases: let hyperi-ci do it.** Push to a `release`-channel
project and CI applies the full optimisation pipeline automatically.

**For local full-optimisation build:**

```bash
cargo build --release --features jemalloc
# With PGO (after profiling):
cargo pgo optimize build -- --features jemalloc
# With PGO + BOLT:
cargo pgo optimize build -- --features jemalloc
cargo pgo bolt optimize --with-pgo -- --features jemalloc
```

## References

- [cargo-pgo](https://github.com/Kobzol/cargo-pgo)
- [The Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [LLVM BOLT](https://github.com/llvm/llvm-project/tree/main/bolt)
- [tikv-jemallocator](https://crates.io/crates/tikv-jemallocator)
- [mimalloc](https://crates.io/crates/mimalloc)
- dfe-loader's `docs/PERFORMANCE.md` — template this doc is modelled on
