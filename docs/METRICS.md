# DFE Platform Metrics Standard

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
| dfe-receiver and dfe-fetcher hand-roll Prometheus text; dfe-loader uses `prometheus` crate; dfe-archiver uses rustlib `MetricsManager` | Maintenance burden, no code reuse |
| No histograms in dfe-receiver or dfe-fetcher | SLO tracking impossible without external APM |
| dfe-loader appends `scaling_pressure` as raw text after `Registry::gather()` | Invisible to registry-based tooling |
| No transport labels — separate metrics for Kafka vs gRPC vs loader | Can't build unified transport dashboards |

---

## Naming Convention

### Prefix

All metrics use the `dfe_` platform prefix. Service differentiation comes from
the Prometheus `job` label (set by scrape config), NOT from the metric name.

```
dfe_{domain}_{metric_name}_{unit}
```

| Component | Rule | Examples |
|-----------|------|---------|
| Prefix | Always `dfe_` | |
| Domain | `transport`, `pipeline`, `records`, `scaling` | `dfe_transport_sent_total` |
| Unit suffix | `_total` (counters), `_bytes`, `_seconds` (histograms/gauges) | `dfe_pipeline_stall_seconds_total` |

### Labels

| Label | Values | When |
|-------|--------|------|
| `transport` | `kafka`, `grpc`, `loader`, `file` | All `dfe_transport_*` metrics |
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

### Transport Metrics (`dfe_transport_*`)

Metrics for downstream delivery (Kafka, gRPC, loader, file sinks).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dfe_transport_sent_total` | counter | `transport` | Messages sent successfully |
| `dfe_transport_send_errors_total` | counter | `transport` | Messages that failed to send (fatal) |
| `dfe_transport_backpressured_total` | counter | `transport` | Send attempts rejected due to backpressure |
| `dfe_transport_refused_total` | counter | `transport` | Messages refused (queue full, no capacity) |
| `dfe_transport_healthy` | gauge | `transport` | Transport health: 1=healthy, 0=unhealthy |
| `dfe_transport_queue_size` | gauge | `transport` | Current messages in send queue |
| `dfe_transport_queue_capacity` | gauge | `transport` | Maximum send queue capacity |
| `dfe_transport_inflight` | gauge | `transport` | Messages in-flight (sent, awaiting ack) |
| `dfe_transport_send_duration_seconds` | histogram | `transport` | Per-send latency |

### Pipeline Metrics (`dfe_pipeline_*`)

Metrics for the processing pipeline itself.

| Metric | Type | Description |
|--------|------|-------------|
| `dfe_pipeline_ready` | gauge | Pipeline readiness: 1=ready, 0=backpressured/stalled |
| `dfe_pipeline_stall_seconds_total` | counter | Total seconds spent stalled due to backpressure |

### Record Metrics (`dfe_records_*`)

Metrics for data records flowing through the pipeline.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dfe_records_received_total` | counter | `protocol` (receiver only) | Records received before filtering |
| `dfe_records_delivered_total` | counter | | Records delivered to output |
| `dfe_records_filtered_total` | counter | | Records dropped by filter expressions |
| `dfe_records_dlq_total` | counter | | Records routed to dead letter queue |

### Scaling Metrics (`dfe_scaling_*`)

Metrics for KEDA autoscaling. All services that run in K8s SHOULD emit these.

| Metric | Type | Description |
|--------|------|-------------|
| `dfe_scaling_pressure` | gauge | Composite pressure score 0-100 (from rustlib `ScalingPressure`) |
| `dfe_scaling_circuit_open` | gauge | 1 if circuit breaker is open, 0 otherwise |
| `dfe_scaling_memory_pressure` | gauge | Memory usage as fraction of limit (0.0-1.0) |

### Spool/Buffer Metrics (`dfe_spool_*`)

For services with disk spillover.

| Metric | Type | Description |
|--------|------|-------------|
| `dfe_spool_bytes` | gauge | Current bytes in disk spool |
| `dfe_spool_messages` | gauge | Current messages in disk spool |
| `dfe_spool_disk_available` | gauge | 1 if disk has capacity, 0 if full |

### Process Metrics (automatic via rustlib)

These are auto-registered by `MetricsManager` when `enable_process_metrics: true`:

| Metric | Type | Source |
|--------|------|--------|
| `{ns}_process_cpu_seconds_total` | gauge | sysinfo |
| `{ns}_process_resident_memory_bytes` | gauge | sysinfo |
| `{ns}_process_virtual_memory_bytes` | gauge | sysinfo |
| `{ns}_process_open_fds` | gauge | /proc/{pid}/fd |
| `{ns}_process_start_time_seconds` | gauge | startup epoch |
| `{ns}_container_memory_limit_bytes` | gauge | cgroup |
| `{ns}_container_memory_usage_bytes` | gauge | cgroup |
| `{ns}_container_cpu_limit_cores` | gauge | cgroup |

---

## Key Alerting Ratios

These PromQL expressions should be standardised across all DFE Grafana dashboards.

| Alert | Query | Threshold |
|-------|-------|-----------|
| Error rate | `rate(dfe_transport_send_errors_total[5m]) / rate(dfe_transport_sent_total[5m])` | > 0.01 (1%) |
| Backpressure rate | `rate(dfe_transport_backpressured_total[5m])` | > 0 sustained |
| Queue saturation | `dfe_transport_queue_size / dfe_transport_queue_capacity` | > 0.8 |
| Pipeline stall | `dfe_pipeline_ready == 0` | > 60s |
| Scaling pressure | `dfe_scaling_pressure` | > 70 (KEDA trigger) |
| Spool disk full | `dfe_spool_disk_available == 0` | > 0s |

---

## Implementation Standard

### Use rustlib `MetricsManager`

All projects MUST use `hyperi_rustlib::metrics::MetricsManager`. No hand-rolled
Prometheus text output. No direct `prometheus` crate usage.

```rust
use hyperi_rustlib::metrics::MetricsManager;

let metrics = MetricsManager::new("dfe");

// Register metrics
let sent = metrics.counter("transport_sent_total", "Messages sent successfully");
let queue = metrics.gauge("transport_queue_size", "Current queue depth");
let latency = metrics.histogram("transport_send_duration_seconds", "Send latency");

// Record
sent.increment(1);
queue.set(42.0);
latency.record(elapsed.as_secs_f64());
```

The namespace is `dfe` (not service-specific). Service differentiation comes from
the Prometheus `job` label in scrape config.

### Transport Label Pattern

```rust
// Use metrics! macros with labels for transport discrimination
counter!("dfe_transport_sent_total", "transport" => "kafka").increment(1);
counter!("dfe_transport_sent_total", "transport" => "grpc").increment(1);
```

### Histogram Buckets

Use rustlib's standard bucket helpers:

| Domain | Buckets | Helper |
|--------|---------|--------|
| Latency | 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s | `MetricsManager::latency_buckets()` |
| Size | 100B, 1KB, 10KB, 100KB, 1MB, 10MB | `MetricsManager::size_buckets()` |

### KEDA ScaledObject

```yaml
triggers:
  - type: prometheus
    metadata:
      query: "avg(dfe_scaling_pressure{job='dfe-receiver'})"
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

Migrate all projects to rustlib `MetricsManager`. Remove hand-rolled
`render()` functions and direct `prometheus` crate usage.

---

## Per-Service Metric Map

Which standard metrics apply to which service:

| Metric | receiver | loader | fetcher | archiver | transform-vector |
|--------|----------|--------|---------|----------|-----------------|
| `dfe_transport_sent_total` | Yes | Yes | Yes | Yes | Via Vector |
| `dfe_transport_send_errors_total` | Yes | Yes | Yes | Yes | Via Vector |
| `dfe_transport_backpressured_total` | Yes | Yes | Yes | Yes | - |
| `dfe_transport_healthy` | Yes | Yes | Yes | Yes | Yes |
| `dfe_transport_queue_size` | Yes | Yes | - | Yes | Via Vector |
| `dfe_pipeline_ready` | Yes | Yes | Yes | Yes | Yes |
| `dfe_records_received_total` | Yes | Yes | Yes | Yes | Via Vector |
| `dfe_records_delivered_total` | Yes | Yes | Yes | Yes | Via Vector |
| `dfe_records_dlq_total` | Yes | Yes | - | Yes | - |
| `dfe_scaling_pressure` | Yes | Yes | Yes | Yes | - |
| `dfe_spool_bytes` | Yes | - | - | Yes | - |

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
