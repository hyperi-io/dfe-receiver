# DFE Platform Metrics Standard

> **STATUS - HISTORICAL (out of date).** Platform-era planning doc that predates
> the scalo rename. The consolidation it proposes happened: the DFE apps now
> emit through scalo's `MetricsManager` (bare metric names, optional namespace
> prefix), so the "Current State (Problems)" table and migration plan below are
> largely resolved. Kept for context; not current guidance.
>
> The `dfe_` prefix this plan proposed was not adopted. Every metric name below is the one the runtime emits, except in the "Current State (Problems)" table, which records the names of its time.

Common metrics naming, structure, and implementation patterns for all DFE Rust
services. Designed for Prometheus scraping, Grafana dashboards, KEDA autoscaling,
and PagerDuty/OpsGenie alerting.

> **Scope:** This standard applies to dfe-receiver, dfe-loader, dfe-fetcher,
> dfe-archiver, and dfe-transform-vector.

---

## Current State (Problems)

| Issue | Impact |
|-------|--------|
| 3 different metric implementations across 5 projects | No shared dashboards, inconsistent naming |
| Prefix collision: `dfe_pipeline_ready` emitted by both dfe-fetcher and dfe-transform-vector | Prometheus conflation when scraped from same cluster |
| dfe-receiver and dfe-fetcher hand-roll Prometheus text; dfe-loader uses `prometheus` crate; dfe-archiver uses scalo `MetricsManager` | Maintenance burden, no code reuse |
| No histograms in dfe-receiver or dfe-fetcher | SLO tracking impossible without external APM |
| dfe-loader appends `scaling_pressure` as raw text after `Registry::gather()` | Invisible to registry-based tooling |
| No transport labels — separate metrics for Kafka vs gRPC vs loader | Can't build unified transport dashboards |

---

## Naming Convention

### Prefix

Metric names carry no prefix unless `metrics.namespace` sets one, and it is empty by default. Service differentiation comes from the Prometheus `job` label (set by scrape config), NOT from the metric name.

```
{domain}_{metric_name}_{unit}
```

| Component | Rule | Examples |
|-----------|------|---------|
| Prefix | None unless `metrics.namespace` sets one | |
| Domain | `transport`, `pipeline`, `records`, `scaling` | `transport_sent_total` |
| Unit suffix | `_total` (counters), `_bytes`, `_seconds` (histograms/gauges) | `pipeline_stall_seconds_total` |

### Labels

| Label | Values | When |
|-------|--------|------|
| `transport` | `kafka`, `grpc`, `loader`, `file` | All `transport_*` metrics |
| `reason` | `missing_header`, `invalid_token`, `invalid_json`, etc. | Failure counters |
| `table` | ClickHouse table name | dfe-loader insert metrics |
| `protocol` | `http`, `grpc`, `otlp`, `syslog`, `splunk_hec`, etc. | dfe-receiver ingest metrics |

**Cardinality rules:**
- Labels MUST have bounded cardinality (< 100 unique values per label)
- NEVER use request IDs, IP addresses, or user-supplied strings as label values
- Topic names are acceptable (bounded by Kafka topic count, typically < 50)

---

## Standard Metric Set

Every DFE service MUST emit the applicable metrics from each category.

### Transport Metrics (`transport_*`)

Metrics for downstream delivery (Kafka, gRPC, loader, file sinks).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `transport_sent_total` | counter | `transport` | Messages sent successfully |
| `transport_send_errors_total` | counter | `transport` | Messages that failed to send (fatal) |
| `transport_backpressured_total` | counter | `transport` | Send attempts rejected due to backpressure |
| `transport_refused_total` | counter | `transport` | Messages refused (queue full, no capacity) |
| `transport_healthy` | gauge | `transport` | Transport health: 1=healthy, 0=unhealthy |
| `transport_queue_size` | gauge | `transport` | Current messages in send queue |
| `transport_queue_capacity` | gauge | `transport` | Maximum send queue capacity |
| `transport_inflight` | gauge | `transport` | Messages in-flight (sent, awaiting ack) |
| `transport_send_duration_seconds` | histogram | `transport` | Per-send latency |

### Pipeline Metrics (`pipeline_*`)

Metrics for the processing pipeline itself.

| Metric | Type | Description |
|--------|------|-------------|
| `pipeline_ready` | gauge | Pipeline readiness: 1=ready, 0=backpressured/stalled |
| `pipeline_stall_seconds_total` | counter | Total seconds spent stalled due to backpressure |

### Record Metrics (`records_*`)

Metrics for data records flowing through the pipeline.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `records_received_total` | counter | `protocol` (receiver only) | Records received before filtering |
| `records_delivered_total` | counter | | Records delivered to output |
| `records_filtered_total` | counter | | Records dropped by filter expressions |
| `records_dlq_total` | counter | | Records routed to dead letter queue |

### Scaling Metrics (`scaling_*`)

Metrics for KEDA autoscaling. All services that run in K8s SHOULD emit these.

| Metric | Type | Description |
|--------|------|-------------|
| `scaling_pressure` | gauge | Composite pressure score 0-100 (from scalo `ScalingPressure`) |
| `scaling_circuit_open` | gauge | 1 if circuit breaker is open, 0 otherwise |
| `scaling_memory_pressure` | gauge | Memory usage as fraction of limit (0.0-1.0) |

### Spool/Buffer Metrics (`spool_*`)

For services with disk spillover.

| Metric | Type | Description |
|--------|------|-------------|
| `spool_bytes` | gauge | Current bytes in disk spool |
| `spool_messages` | gauge | Current messages in disk spool |
| `spool_disk_available` | gauge | 1 if disk has capacity, 0 if full |

### Process Metrics (automatic via scalo)

These are auto-registered by `MetricsManager` when `enable_process_metrics: true`, and are not yet in the manifest (scalo-rs#137):

| Metric | Type | Source |
|--------|------|--------|
| `process_cpu_seconds_total` | gauge | sysinfo |
| `process_resident_memory_bytes` | gauge | sysinfo |
| `process_virtual_memory_bytes` | gauge | sysinfo |
| `process_open_fds` | gauge | /proc/{pid}/fd |
| `process_start_time_seconds` | gauge | startup epoch |
| `container_memory_limit_bytes` | gauge | cgroup |
| `container_memory_usage_bytes` | gauge | cgroup |
| `container_cpu_limit_cores` | gauge | cgroup |

---

## Key Alerting Ratios

These PromQL expressions should be standardised across all DFE Grafana dashboards.

| Alert | Query | Threshold |
|-------|-------|-----------|
| Error rate | `rate(transport_send_errors_total[5m]) / rate(transport_sent_total[5m])` | > 0.01 (1%) |
| Backpressure rate | `rate(transport_backpressured_total[5m])` | > 0 sustained |
| Queue saturation | `transport_queue_size / transport_queue_capacity` | > 0.8 |
| Pipeline stall | `pipeline_ready == 0` | > 60s |
| Scaling pressure | `scaling_pressure` | > 70 (KEDA trigger) |
| Spool disk full | `spool_disk_available == 0` | > 0s |

---

## Implementation Standard

### Use scalo `MetricsManager`

All projects MUST use `scalo::metrics::MetricsManager`. No hand-rolled
Prometheus text output. No direct `prometheus` crate usage.

```rust
use scalo::metrics::MetricsManager;

let metrics = MetricsManager::new("");

// Register metrics
let sent = metrics.counter("transport_sent_total", "Messages sent successfully");
let queue = metrics.gauge("transport_queue_size", "Current queue depth");
let latency = metrics.histogram("transport_send_duration_seconds", "Send latency");

// Record
sent.increment(1);
queue.set(42.0);
latency.record(elapsed.as_secs_f64());
```

An empty namespace records bare names, and it is the `metrics.namespace` default the service runtime reads from the config cascade. Service differentiation comes from the Prometheus `job` label in scrape config.

### Transport Label Pattern

```rust
// Use metrics! macros with labels for transport discrimination
counter!("transport_sent_total", "transport" => "kafka").increment(1);
counter!("transport_sent_total", "transport" => "grpc").increment(1);
```

### Histogram Buckets

Use scalo's standard bucket helpers:

| Domain | Buckets | Helper |
|--------|---------|--------|
| Latency | 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s | `MetricsManager::latency_buckets()` |
| Size | 100B, 1KB, 10KB, 100KB, 1MB, 10MB | `MetricsManager::size_buckets()` |

### KEDA ScaledObject

```yaml
triggers:
  - type: prometheus
    metadata:
      query: "avg(scaling_pressure{job='dfe-receiver'})"
      threshold: "70"
```

---

## Migration Path

### Phase 1: Add `dfe_` metrics alongside existing names

Emit both old names (`receiver_*`, `loader_*`) and new names (`dfe_*`).
Update dashboards to use new names. No breaking changes.

### Phase 2: Remove old names

After all dashboards and alerts are migrated, remove the old metric names.
Coordinate across teams — one release cycle warning.

### Phase 3: Consolidate implementation

Migrate all projects to scalo `MetricsManager`. Remove hand-rolled
`render()` functions and direct `prometheus` crate usage.

---

## Per-Service Metric Map

Which standard metrics apply to which service:

| Metric | receiver | loader | fetcher | archiver | transform-vector |
|--------|----------|--------|---------|----------|-----------------|
| `transport_sent_total` | Yes | Yes | Yes | Yes | Via Vector |
| `transport_send_errors_total` | Yes | Yes | Yes | Yes | Via Vector |
| `transport_backpressured_total` | Yes | Yes | Yes | Yes | - |
| `transport_healthy` | Yes | Yes | Yes | Yes | Yes |
| `transport_queue_size` | Yes | Yes | - | Yes | Via Vector |
| `pipeline_ready` | Yes | Yes | Yes | Yes | Yes |
| `records_received_total` | Yes | Yes | Yes | Yes | Via Vector |
| `records_delivered_total` | Yes | Yes | Yes | Yes | Via Vector |
| `records_dlq_total` | Yes | Yes | - | Yes | - |
| `scaling_pressure` | Yes | Yes | Yes | Yes | - |
| `spool_bytes` | Yes | - | - | Yes | - |

---

## References

- [Prometheus Metric and Label Naming](https://prometheus.io/docs/practices/naming/)
- [OTel Collector Internal Telemetry](https://opentelemetry.io/docs/collector/internal-telemetry/)
- [KEDA Prometheus Scaler](https://keda.sh/docs/latest/scalers/prometheus/)
- [Monitor Collector Queue Depth and Backpressure](https://oneuptime.com/blog/post/2026-02-06-monitor-collector-queue-depth-backpressure/view)
- [OTel Collector Backpressure (Axoflow)](https://axoflow.com/blog/opentelemetry-controller-outages-pipelines-backpressure)
- [Prometheus Labels Best Practices (CNCF)](https://www.cncf.io/blog/2025/07/22/prometheus-labels-understanding-and-best-practices/)
- [Prometheus Best Practices (Better Stack)](https://betterstack.com/community/guides/monitoring/prometheus-best-practices/)
- [KEDA + Prometheus Autoscaling (Devtron)](https://devtron.ai/blog/keda-autoscaling-prometheus/)
