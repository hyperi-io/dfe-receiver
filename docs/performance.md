# Performance Optimisation Guide

How the published binary is optimised, the dials that change throughput at run time, and how to profile the receiver. The allocator is jemalloc and nothing else.

## What CI applies

hyperi-ci builds the binary at an optimisation tier set by the release channel, with no opt-in. The rules are in hyperi-ci's [docs/runtime/pgo-bolt.md](https://github.com/hyperi-io/hyperi-ci/blob/main/docs/runtime/pgo-bolt.md).

| Channel | Allocator | LTO | PGO and BOLT |
|---|---|---|---|
| alpha | jemalloc | thin | no |
| beta | jemalloc | fat | no |
| release | jemalloc | fat | yes, while `scripts/pgo-workload.sh` exists |

`.hyperi-ci.yaml` sets only how the PGO driver is built (`build.rust.optimize.pgo.workload_setup_cmd`), and `build.rust.optimize` can override a channel's defaults key by key. hyperi-ci passes `--features jemalloc` itself, so a local `cargo build --release` links the system allocator unless it asks for jemalloc too.

`Cargo.toml`'s release profile, which every channel starts from:

```toml
[profile.release]
lto = "thin"           # hyperi-ci sets fat on beta and release
codegen-units = 1
strip = true
panic = "abort"
opt-level = 3
```

`jemalloc` is the only allocator feature, wired in `src/main.rs` as the `#[global_allocator]`. Release builds are stripped, so `nm` sees no symbols: `strings <binary> | grep -ci jemalloc` shows the allocator is linked.

## PGO and BOLT

A PGO workload MUST exercise the receiver's data-processing hot paths -- parse, validate, route, produce to the Kafka sink. Port checks, health probes or one-shot sends make PGO worse than not running it, because the compiler then optimises startup paths over production hot paths. `scripts/pgo-workload.sh` is the orchestrator and `src/bin/pgo_driver.rs` the load generator. The workload needs Docker, for its broker, and the driver binary at `target/release/pgo-driver` or `$PGO_DRIVER_PATH`.

To run it locally:

```bash
cargo install cargo-pgo
rustup component add llvm-tools-preview

# The load generator, then an instrumented receiver
cargo build --release --features pgo-driver --bin pgo-driver
cargo pgo build -- --features jemalloc

# Drive it (PGO_WORKLOAD_DURATION_SECS, default 300)
bash scripts/pgo-workload.sh ./target/x86_64-unknown-linux-gnu/release/dfe-receiver

# Build with the profile, then add BOLT (Linux only)
cargo pgo optimize build -- --features jemalloc
cargo pgo bolt optimize --with-pgo -- --features jemalloc
```

## Benchmarks

| Bench | What it times |
|---|---|
| `throughput` | JSON validation, field extraction, routing and the metric calls |
| `routing` | Per-record routing and header stamping, as the pipeline runs them |
| `json_depth` | The nesting-depth guard, alone and inside the per-record path |
| `flow_decode` | NetFlow v5 and sFlow v5 decode and envelope rendering |
| `syslog` | Framing and conversion over eight lines: five RFC 3164 device formats, two RFC 5424 and one CEF over syslog |
| `syslog_allocs` | Heap allocations per syslog message |

```bash
cargo bench --bench throughput -- --save-baseline system-allocator
cargo bench --bench throughput --features jemalloc -- --baseline system-allocator
cargo bench --bench syslog -- --save-baseline before   # on the old code
cargo bench --bench syslog -- --baseline before        # on the new code
cargo bench --bench syslog_allocs
```

Criterion micro-benchmarks do little allocation per iteration, so they say little about an allocator. Compare allocators on a load run instead.

## Run-time dials

- `server.max_concurrent_requests` (default 10,000) caps in-flight requests on each HTTP listener. Lower it if the destinations cannot keep up, or memory grows instead.
- `server.rate_limit` is a per-IP GCRA limit, off by default. It is a hardening dial, not a performance one ([hardening.md](hardening.md)).
- The Kafka producer runs scalo's `throughput` sizing profile: `batch.size` 128 KiB, `linger.ms` 20, zstd level 3, a 64 MiB queue. `kafka.librdkafka_overrides` changes any of it per deployment.
- The memory guard reads `DFE_RECEIVER_MEMORY_PRESSURE_THRESHOLD` (default 0.80) and `DFE_RECEIVER_MEMORY_CGROUP_HEADROOM` (default 0.85, the share of the cgroup limit it counts as available). Past the threshold, HTTP ingest answers 503 with `Retry-After: 5` and gRPC `UNAVAILABLE`.

## Profiling

The `profiling` profile inherits release and keeps debug info:

```bash
cargo build --profile profiling --features jemalloc
perf record -g --call-graph dwarf ./target/profiling/dfe-receiver --config config.yaml
# drive load with scripts/pgo-workload.sh, then
perf report

cargo install flamegraph
cargo flamegraph --profile profiling --features jemalloc --bin dfe-receiver -- --config config.yaml
```

Heap profiling with jeprof needs a build whose jemalloc has profiling compiled in: tikv-jemalloc-sys's `profiling` feature, which the receiver's `jemalloc` feature does not turn on. Such a build reads its options from `_RJEM_MALLOC_CONF`, the prefixed name, not `MALLOC_CONF`.

A production release is hyperi-ci's to build: a squash merge carrying `Release: true`, or a `from-head` dispatch, runs the pipeline above. A local build at the release tier is `cargo build --release --features jemalloc`, with the PGO and BOLT steps above when wanted.

## References

- [cargo-pgo](https://github.com/Kobzol/cargo-pgo)
- [The Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [LLVM BOLT](https://github.com/llvm/llvm-project/tree/main/bolt)
- [tikv-jemallocator](https://crates.io/crates/tikv-jemallocator)
