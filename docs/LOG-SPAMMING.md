# DFE Platform Log Spamming Protection

> **STATUS - HISTORICAL (out of date).** Platform-era planning doc that predates
> the scalo rename. The flood-control it proposes now ships in the library: the
> scalo `logger` feature pulls `tracing-throttle`, so the "Phase 1/3" plans
> below are largely implemented in-library. Kept for context; not current
> guidance.

Patterns and techniques for preventing log flooding in high-throughput DFE Rust
services.

> **Problem:** Under failure conditions (Kafka down, ClickHouse unavailable,
> disk full, memory pressure), error/warn log sites can produce thousands to
> millions of identical messages per second, saturating log aggregators,
> inflating costs, and burying actionable signals in noise.

---

## Current State (Problems)

### Audit Results Across All DFE Projects

**No project implements any form of log rate limiting or deduplication.**

| Project | Worst Spam Candidates | Impact |
|---------|----------------------|--------|
| **dfe-receiver** | Memory pressure warn per-request (`pipeline/mod.rs:205`), Kafka send error per-message (`sink/kafka/mod.rs:61`), UDP recv error in tight loop (`syslog/mod.rs:73`) | Unbounded warn/error volume under sustained failure |
| **dfe-loader** | Type coercion warn per-row (`transform/coerce.rs:103`), ClickHouse retry warn per-attempt per-batch (`clickhouse/inserter.rs:338`), DLQ channel full per-message (`pipeline/orchestrator.rs:783`) | At PB/s, a systematic schema mismatch produces millions of warn lines |
| **dfe-fetcher** | Container stderr warn per-line (`extractor/container/mod.rs:152`), scheduler not-ready warn in busy-wait (`scheduler/mod.rs:118`), transport send error per-message (`output.rs:143`) | Chatty container process saturates log pipeline |
| **dfe-archiver** | Routing failure warn per-message (`archiver.rs:168`), buffer push warn per-message under disk pressure (`archiver.rs:191`), Kafka recv error per-poll (`archiver.rs:134`) | Sustained disk pressure floods logs |
| **dfe-transform-vector** | Low risk — all error/warn at process lifecycle boundaries, not per-event | Acceptable |

### Common Anti-Patterns Found

1. **`warn!`/`error!` inside per-message loops** — fires on every failed message during an outage
2. **`warn!` inside tight `recv()` loops** — socket errors repeat at recv frequency
3. **No state-transition logging** — logs every poll cycle, not just on state change
4. **No `tracing::enabled!` guards** — expensive format strings constructed even when filtered

---

## Protection Techniques

### Technique 1: Rate-Limited Logging (`tracing-throttle`)

The `tracing-throttle` crate provides signature-based rate limiting as a
`tracing::Layer`. Events with identical signatures (level + message + target +
field values) are deduplicated and throttled together.

**Default behaviour:** 50 burst capacity, 1 token/sec (60/min), 10K max
signatures with LRU eviction.

```rust
use tracing_throttle::TracingRateLimitLayer;
use tracing_subscriber::{Registry, layer::SubscriberExt};

let throttle = TracingRateLimitLayer::new()
    .burst(10)           // allow 10 rapid-fire identical messages
    .refill_rate(1)      // then 1 per second
    .max_signatures(5000)
    .exclude_fields(&["request_id", "span_id"]);  // ignore high-cardinality

let subscriber = Registry::default()
    .with(throttle)
    .with(fmt_layer);
```

**When to use:** As a global safety net. Does not require per-site code changes.
Add as a layer in scalo's `logger::setup()`.

**Trade-off:** Adds ~50ns per event (lock-free sharded storage). Negligible for
logging but measurable if applied to `trace!`-level events at millions/sec.

### Technique 2: State-Transition Logging

Log only when a condition **changes**, not on every occurrence. This is the
most effective technique for reducing spam from sustained failure conditions.

```rust
use std::sync::atomic::{AtomicBool, Ordering};

static WAS_UNDER_PRESSURE: AtomicBool = AtomicBool::new(false);

fn check_pressure(pressure: f64) {
    let is_high = pressure > 0.8;
    let was_high = WAS_UNDER_PRESSURE.swap(is_high, Ordering::Relaxed);

    match (was_high, is_high) {
        (false, true) => warn!(pressure, "memory pressure HIGH — backpressure active"),
        (true, false) => info!(pressure, "memory pressure recovered"),
        _ => {} // no change, no log
    }
}
```

**When to use:** For any condition that persists (memory pressure, circuit
breaker state, disk full, transport unhealthy). Log the transition, not the
state.

**Pattern for circuit breaker:**

```rust
// Log once on open, once on close — not every failed send
if circuit_state_changed {
    match new_state {
        CircuitState::Open => warn!("circuit breaker OPEN — spooling messages"),
        CircuitState::HalfOpen => info!("circuit breaker half-open — probing"),
        CircuitState::Closed => info!("circuit breaker closed — recovered"),
    }
}
```

### Technique 3: Sampled Error Logging

For per-message errors where every occurrence matters for metrics but not for
logs, log a sample and count the rest.

```rust
use std::sync::atomic::{AtomicU64, Ordering};

static SEND_ERRORS: AtomicU64 = AtomicU64::new(0);

fn handle_send_error(error: &Error) {
    let count = SEND_ERRORS.fetch_add(1, Ordering::Relaxed) + 1;

    // Log the first occurrence, then every 1000th
    if count == 1 || count % 1000 == 0 {
        warn!(
            error = %error,
            total_errors = count,
            "transport send failed (showing 1 in 1000)"
        );
    }
    // Always increment the metric counter
    counter!("dfe_transport_send_errors_total", "transport" => "kafka").increment(1);
}
```

**When to use:** For per-message/per-row errors in the hot path (send failures,
validation failures, type coercion errors). The metric captures every event;
the log shows a representative sample.

### Technique 4: Debounced Logging

For conditions that flap, use a minimum interval between log emissions.

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static LAST_WARN_EPOCH_MS: AtomicU64 = AtomicU64::new(0);
const MIN_INTERVAL_MS: u64 = 5000; // 5 seconds

fn warn_throttled(message: &str) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let last = LAST_WARN_EPOCH_MS.load(Ordering::Relaxed);

    if now - last >= MIN_INTERVAL_MS {
        LAST_WARN_EPOCH_MS.store(now, Ordering::Relaxed);
        warn!(message);
    }
}
```

**When to use:** For periodic check loops (UDP recv errors, health check
failures, disk space checks). Prevents tight-loop spam while ensuring the
condition is still logged regularly.

### Technique 5: `tracing::enabled!` Guards

Prevent expensive format string construction when the log level is filtered.

```rust
// Bad — formats even when debug is off
debug!(payload = ?large_struct, "processing message");

// Good — skip formatting entirely when not needed
if tracing::enabled!(tracing::Level::DEBUG) {
    debug!(payload = ?large_struct, "processing message");
}
```

**When to use:** For any `debug!` or `trace!` call that formats large
structures, vectors, or does `Display`/`Debug` on non-trivial types. Not
needed for simple scalar fields — `tracing` already avoids allocation for
those.

---

## Recommended Implementation Strategy

### Phase 1: Global Safety Net (scalo)

Add `tracing-throttle` as a layer in `scalo::logger::setup()`.
This provides baseline protection for all DFE services with zero per-site
code changes.

**Configuration via env var:**

| Env Var | Default | Description |
|---------|---------|-------------|
| `LOG_THROTTLE_BURST` | `10` | Messages allowed in burst before throttling |
| `LOG_THROTTLE_RATE` | `1` | Sustained messages per second per signature |
| `LOG_THROTTLE_MAX_SIGNATURES` | `5000` | Max unique signatures tracked (LRU eviction) |
| `LOG_THROTTLE_ENABLED` | `true` | Disable entirely if needed for debugging |

**Apply to `warn!` and `error!` only.** Leave `info!`, `debug!`, `trace!`
unthrottled — these are already filtered by level in production.

### Phase 2: Targeted Fixes (per-project)

Fix the worst offenders identified in the audit:

#### dfe-receiver

| File | Line | Current | Fix |
|------|------|---------|-----|
| `src/pipeline/mod.rs` | 205,315 | `warn!` per-request under memory pressure | State-transition logging |
| `src/sink/kafka/mod.rs` | 61 | `error!` per failed Kafka send | Sampled (1 in 1000) + metric |
| `src/server/syslog/mod.rs` | 73 | `warn!` per UDP recv error in tight loop | Debounced (5s interval) |
| `src/server/lumberjack/mod.rs` | 94,135 | `warn!` per failed frame parse | Sampled (1 in 100) |

#### dfe-loader

| File | Line | Current | Fix |
|------|------|---------|-----|
| `dfe-loader/src/transform/coerce.rs` | 103 | `warn!` per row with coercion failure | Sampled (1 in 1000) + metric + log total on batch completion |
| `dfe-loader/src/clickhouse/inserter.rs` | 338,358 | `warn!` per retry attempt | State-transition (log first failure, log recovery) |
| `dfe-loader/src/pipeline/orchestrator.rs` | 783,786 | `warn!` per DLQ channel full | Debounced (5s) |

#### dfe-fetcher

| File | Line | Current | Fix |
|------|------|---------|-----|
| `dfe-fetcher/crates/fetcher/src/extractor/container/mod.rs` | 152 | `warn!` per container stderr line | Sampled (1 in 100) + count |
| `dfe-fetcher/crates/fetcher/src/scheduler/mod.rs` | 118 | `warn!` while source not ready in busy-wait | Debounced (10s) |
| `dfe-fetcher/crates/fetcher/src/output.rs` | 143 | `error!` per transport send failure | Sampled (1 in 1000) + metric |

#### dfe-archiver

| File | Line | Current | Fix |
|------|------|---------|-----|
| `crates/archiver/src/archiver.rs` | 168 | `warn!` per routing failure | Sampled (1 in 1000) + metric |
| `crates/archiver/src/archiver.rs` | 191 | `warn!` per buffer push under pressure | State-transition logging |
| `crates/core/src/buffer/tiered.rs` | 225,242 | `warn!` per spool full push | State-transition logging |

### Phase 3: Helpers in scalo

Add convenience macros/utilities to scalo for the common patterns:

```rust
// In scalo::logger

/// Log on state transition only. Returns true if state changed.
pub fn log_state_change(flag: &AtomicBool, new_state: bool) -> bool {
    flag.swap(new_state, Ordering::Relaxed) != new_state
}

/// Log every Nth occurrence. Returns true if this is a loggable occurrence.
pub fn log_sampled(counter: &AtomicU64, sample_rate: u64) -> bool {
    let count = counter.fetch_add(1, Ordering::Relaxed) + 1;
    count == 1 || count % sample_rate == 0
}

/// Log at most once per interval. Returns true if enough time has passed.
pub fn log_debounced(last_ms: &AtomicU64, min_interval_ms: u64) -> bool {
    let now = /* epoch ms */;
    let last = last_ms.load(Ordering::Relaxed);
    if now - last >= min_interval_ms {
        last_ms.store(now, Ordering::Relaxed);
        true
    } else {
        false
    }
}
```

---

## Decision Matrix: Which Technique to Use

| Scenario | Technique | Example |
|----------|-----------|---------|
| Sustained condition (memory pressure, circuit open, disk full) | State-transition | Log on open/close, not every check |
| Per-message error in hot path (send failure, validation failure) | Sampled + metric | Log 1 in 1000, count every one in metrics |
| Tight recv/poll loop error (UDP, Kafka consumer) | Debounced | Max once per 5-10 seconds |
| Flapping condition | Debounced | Max once per interval |
| Expensive debug formatting | `tracing::enabled!` guard | Skip `Debug` of large structs |
| Global baseline protection | `tracing-throttle` layer | All warn/error automatically |

---

## What NOT to Do

- **Never suppress `error!` entirely** — always emit at least periodically.
  Errors represent data loss risk; complete suppression hides incidents.
- **Never use log suppression as a substitute for fixing the root cause.**
  If a code path generates 10K warn/sec, fix the code path. Throttling is
  defence-in-depth, not a permanent workaround.
- **Never throttle `info!` at startup/shutdown** — these are bounded by
  definition and important for operational visibility.
- **Never drop logs required for audit trails** — compliance-relevant events
  must always be emitted regardless of rate.

---

## Testing Log Spam Protection

```rust
#[test]
fn test_sampled_logging() {
    let counter = AtomicU64::new(0);
    // First call should log
    assert!(log_sampled(&counter, 1000));
    // Next 999 should not
    for _ in 0..999 {
        assert!(!log_sampled(&counter, 1000));
    }
    // 1001st should log
    assert!(log_sampled(&counter, 1000));
}

#[test]
fn test_state_transition() {
    let flag = AtomicBool::new(false);
    // false → true: changed
    assert!(log_state_change(&flag, true));
    // true → true: no change
    assert!(!log_state_change(&flag, true));
    // true → false: changed
    assert!(log_state_change(&flag, false));
}
```

---

## References

- [tracing-throttle](https://crates.io/crates/tracing-throttle) — signature-based rate limiting for Rust tracing
- [tokio-rs/tracing Discussion #3006](https://github.com/tokio-rs/tracing/discussions/3006) — rate limiter for tracing
- [Fluent Bit Backpressure with Prometheus](https://docs.fluentbit.io/manual/administration/monitoring)
- [OTel Collector Backpressure (Axoflow)](https://axoflow.com/blog/opentelemetry-controller-outages-pipelines-backpressure)
- [Prometheus Best Practices (Better Stack)](https://betterstack.com/community/guides/monitoring/prometheus-best-practices/)
