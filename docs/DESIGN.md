# dfe-receiver Design Document

## Overview

dfe-receiver is a high-performance HTTP/gRPC receiver for PB/s scale data ingestion. It replaces a Vector-based implementation with native Rust for maximum performance on the hot path.

## Requirements

### Functional Requirements

1. **Data Ingestion**
   - Receive JSON data over HTTP(S) and gRPC
   - Support Vector HTTP sink and Vector sink (gRPC) protocols
   - Accept data from any HTTP client (not limited to Vector)

2. **Validation**
   - Accept only valid JSON payloads
   - Optionally validate required fields (JSON path)
   - Route invalid data to DLQ or reject with error

3. **Routing**
   - Route to Kafka topics based on JSON field values
   - Support category-to-topic mapping
   - Support direct routing to dfe-loader
   - Configurable default topic for unmatched data

4. **Authentication**
   - Header-based authentication (static values)
   - Bearer token authentication (static or from secret manager)
   - mTLS client certificate validation
   - Certificates from file or secret manager (AWS, OpenBao/Vault)

5. **Resilience**
   - Disk spillover when downstream unavailable
   - Circuit breaker for failing sinks
   - Memory pressure detection and backpressure

### Non-Functional Requirements

1. **Performance** - PB/s daily throughput capability
2. **Latency** - Sub-millisecond hot path processing
3. **Resource Efficiency** - Minimal allocations on hot path
4. **Observability** - Prometheus metrics, structured logging
5. **Operability** - Kubernetes-native (KEDA scaling, health probes)

## Architecture

```
                                   ┌─────────────────┐
                                   │   Config File   │
                                   │  (7-layer       │
                                   │   cascade)      │
                                   └────────┬────────┘
                                            │
┌──────────────┐                  ┌─────────▼─────────┐
│ Vector/HTTP  │──── HTTPS ──────▶│   HTTP Server    │
│   Client     │                  │   (axum + TLS)   │
└──────────────┘                  └─────────┬─────────┘
                                            │
┌──────────────┐                  ┌─────────▼─────────┐
│ Vector gRPC  │──── gRPC ───────▶│   gRPC Server    │
│   Client     │                  │   (tonic)        │
└──────────────┘                  └─────────┬─────────┘
                                            │
                                  ┌─────────▼─────────┐
                                  │  Auth Middleware  │
                                  │ (header/bearer/   │
                                  │  mTLS)            │
                                  └─────────┬─────────┘
                                            │
                                  ┌─────────▼─────────┐
                                  │  JSON Validator   │
                                  │  (sonic_rs)       │
                                  └─────────┬─────────┘
                                            │
                                  ┌─────────▼─────────┐
                                  │     Router        │
                                  │ (zero-copy field │
                                  │  extraction)      │
                                  └─────────┬─────────┘
                                            │
                         ┌──────────────────┼──────────────────┐
                         │                  │                  │
               ┌─────────▼─────────┐ ┌──────▼──────┐ ┌─────────▼─────────┐
               │   Kafka Sink      │ │ DLQ Sink    │ │  Loader Sink      │
               │   (rdkafka)       │ │             │ │  (hs-rustlib)     │
               └─────────┬─────────┘ └─────────────┘ └───────────────────┘
                         │
               ┌─────────▼─────────┐
               │   TieredSink      │
               │ (circuit breaker  │
               │  + disk spool)    │
               └───────────────────┘
```

## Hot Path Design

The hot path is critical for PB/s throughput. Key optimizations:

### Zero-Copy Processing

```rust
// Payload stays as bytes::Bytes throughout
async fn ingest_handler(body: Bytes) -> StatusCode {
    // 1. Validate JSON without full parse (sonic_rs LazyValue)
    // 2. Extract routing field with zero-copy (Cow<str>)
    // 3. Send to batcher (Bytes reference)
}
```

### Memory Efficiency

- `bytes::Bytes` for payload (reference-counted, no copy on clone)
- `Cow<str>` for extracted fields (borrow when no escaping needed)
- `FxHashMap` for category lookups (faster than std HashMap)
- Pre-allocated batch vectors

### Minimal Allocations

- No `format!()` on hot path
- Avoid `String::to_string()` when `&str` suffices
- Use `#[inline]` on critical functions

## Component Details

### HTTP Server (axum)

- Tower middleware for auth layers
- Graceful shutdown support
- TLS termination with rustls
- Health endpoints for Kubernetes

### gRPC Server (tonic)

- Vector sink protocol compatibility
- Shared routing/batching pipeline with HTTP
- Same auth middleware (via Tower)

### JSON Validation (sonic_rs)

- SIMD-accelerated parsing
- `LazyValue` for validation without full DOM
- `get_from_slice()` for on-demand field extraction

### Router

Pattern from dfe-loader:

```rust
pub fn route(&self, payload: &[u8]) -> RouteResult {
    // Extract category field (zero-copy)
    let category = extract_field_cow(payload, &self.topic_fields)?;

    // Lookup destination
    let topic = self.category_to_topic
        .get(category.as_ref())
        .unwrap_or(&self.default_topic);

    RouteResult { topic, destination }
}
```

### Kafka Batching

Per-topic batching with configurable thresholds:

- **Size**: 8 MiB default
- **Count**: 10,000 messages default
- **Time**: 20ms linger

Uses rdkafka `FutureProducer` with zstd compression.

### TieredSink

Wraps primary sink with resilience:

1. **Circuit Breaker** (hs-rustlib) - Tracks consecutive failures, opens after threshold
2. **In-Memory Queue** - Buffers during circuit-open state
3. **Disk Spool** (planned) - Spillover when memory queue full
4. **Background Drain** - Retries queued messages when circuit closes

### Authentication

#### Bearer Token Provider

```rust
pub struct BearerTokenProvider {
    tokens: RwLock<HashSet<String>>,  // Thread-safe for hot reload
    shutdown_tx: broadcast::Sender<()>,
}
```

Supports:
- Static tokens (development)
- Dynamic loading from secret managers
- Background refresh with configurable interval
- Token rotation without restart

Secret source format: `provider:path:key`
- `file:/etc/secrets/tokens`
- `vault:secret/data/auth:bearer_tokens`
- `aws:prod/auth/tokens:bearer`

## Configuration

7-layer cascade (hs-rustlib pattern):

1. Compiled defaults
2. `/etc/dfe-receiver/config.yaml`
3. `./config.yaml`
4. `~/.config/dfe-receiver/config.yaml`
5. Environment variables (`DFE_RECEIVER_*`)
6. CLI arguments
7. Runtime overrides

### Example Configuration

```yaml
server:
  bind_address: "0.0.0.0:443"
  tls:
    enabled: true
    cert_file: "/etc/ssl/receiver.crt"
    key_file: "/etc/ssl/receiver.key"
  auth:
    mode: bearer
    bearer:
      secret_source: "vault:secret/data/auth:tokens"
      refresh_interval_secs: 300

validation:
  require_json: true
  required_fields: ["org_id"]
  dlq_on_invalid: true

routing:
  topic_fields: ["event.category", "event_type"]
  default_topic: "unmatched"
  topic_suffix: "_land"
  category_to_topic:
    auth: "logs_auth"
    network: "logs_network"
  dlq:
    enabled: true
    topic: "dlq_land"

kafka:
  brokers: ["kafka:9092"]
  producer:
    batch_size: 8388608
    batch_messages: 10000
    linger_ms: 20
    compression: zstd

buffer:
  memory_limit: 0  # Auto (67% of available)
  pressure_threshold: 0.8
  spool_path: "/var/spool/dfe-receiver"

metrics:
  address: "0.0.0.0:9090"
```

## Deployment

### Kubernetes with KEDA

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: dfe-receiver
spec:
  scaleTargetRef:
    name: dfe-receiver
  minReplicaCount: 2
  maxReplicaCount: 100
  triggers:
    - type: prometheus
      metadata:
        query: dfe_receiver_scaling_metric
        threshold: "0.7"
```

### Health Probes

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 8080
readinessProbe:
  httpGet:
    path: /health/ready
    port: 8080
```

## Metrics

### Request Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `dfe_receiver_requests_total` | Counter | Total requests by status |
| `dfe_receiver_bytes_received_total` | Counter | Total bytes ingested |
| `dfe_receiver_request_duration_seconds` | Histogram | Request latency |

### Kafka Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `dfe_receiver_kafka_messages_total` | Counter | Messages sent to Kafka |
| `dfe_receiver_kafka_bytes_total` | Counter | Bytes sent to Kafka |
| `dfe_receiver_kafka_batch_size` | Histogram | Batch sizes |

### Buffer Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `dfe_receiver_buffer_spilled_total` | Counter | Messages spilled |
| `dfe_receiver_buffer_drained_total` | Counter | Messages drained |
| `dfe_receiver_buffer_queue_size` | Gauge | Current queue size |
| `dfe_receiver_memory_pressure` | Gauge | Memory pressure (0-1) |

### Scaling Metric

Composite metric for KEDA scaling:

```
scaling_metric = max(
    memory_pressure,
    queue_depth / max_queue,
    request_rate / target_rate
)
```

## Testing Strategy

### Unit Tests

- JSON validation (valid, invalid, edge cases)
- Router field extraction and topic mapping
- Batcher flush triggers (size, count, time)
- Auth validation (header, bearer, mTLS)

### Integration Tests

- HTTP endpoint with test payloads
- gRPC endpoint with Vector protocol
- Kafka producer (testcontainers)
- TLS/mTLS validation
- Backpressure scenarios

### Performance Tests

```bash
# Benchmark hot path
cargo bench

# Load test
k6 run --vus 100 --duration 60s load-test.js

# Memory profiling
cargo run --release --features dhat-heap
```

## Future Work

- [ ] gRPC Vector sink protocol implementation
- [ ] Full disk spillover (currently in-memory only)
- [ ] Config hot-reload for all settings
- [ ] Expression language for routing rules
- [ ] Rebranding: hs-rustlib to hyperi-rustlib

## References

- [dfe-loader](https://github.com/hypersec-io/dfe-loader) - Reference implementation patterns
- [hs-rustlib](https://github.com/hypersec-io/hs-rustlib) - Shared library
- [Vector HTTP sink](https://vector.dev/docs/reference/configuration/sinks/http/)
- [Vector sink (gRPC)](https://vector.dev/docs/reference/configuration/sinks/vector/)
