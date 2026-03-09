# dfe-receiver

High-performance HTTP/gRPC receiver for PB/s scale data ingestion.

## Overview

dfe-receiver is a native Rust data ingestion service that accepts data from multiple
agent protocols, normalises it to JSON, and routes it to Kafka topics or dfe-loader.

**Supported protocols:**

| Protocol | Port | Source |
|---|---|---|
| HTTP(S) JSON | configurable | Vector, custom agents |
| gRPC Vector sink | configurable | Vector |
| OTLP gRPC | 4317 | OpenTelemetry collectors |
| OTLP HTTP | 4318 | OpenTelemetry collectors |
| Lumberjack/Beats | 5044 | Filebeat, Logstash Beats output |
| Splunk HEC | 8088 | Splunk forwarders, Fluentd HEC output |
| Syslog UDP/TCP/TLS | 514 / 6514 | syslogd, rsyslog, syslog-ng |
| Fluent Forward | 24224 | Fluentd, Fluent Bit |
| GELF TCP | 12201 | Graylog GELF output, Fluent Bit |
| Prometheus Remote Write | 9091 | Prometheus, VictoriaMetrics |

**Core behaviour:**

- Normalises all protocol data to JSON
- Validates JSON format and optional required fields
- Routes to Kafka topics based on configurable field extraction rules
- Buffers in-memory with backpressure via CircuitBreaker (no disk spillover by design)
- Supports header auth, bearer tokens, and mTLS authentication

## Quick Start

```bash
# Build
cargo build --release

# Run with config file
./target/release/dfe-receiver --config config.yaml

# Run with environment override
DFE_RECEIVER_SERVER__BIND_ADDRESS=0.0.0.0:8080 ./target/release/dfe-receiver
```

## Configuration

See [config.example.yaml](config.example.yaml) for full configuration reference.

### Minimal Configuration

```yaml
server:
  bind_address: "0.0.0.0:8080"
  auth:
    mode: none

kafka:
  brokers:
    - "localhost:9092"

routing:
  default_topic: "events"
```

### Authentication Modes

#### Header Authentication

```yaml
server:
  auth:
    mode: header
    accepted_headers:
      - name: x-api-key
        values: ["secret-key-1", "secret-key-2"]
```

#### Bearer Token Authentication

Static tokens (development):

```yaml
server:
  auth:
    mode: bearer
    bearer:
      tokens:
        - "dev-token-1"
        - "dev-token-2"
```

Dynamic tokens from secret manager (production):

```yaml
server:
  auth:
    mode: bearer
    bearer:
      secret_source: "vault:secret/data/auth:bearer_tokens"
      refresh_interval_secs: 300
```

Supported secret sources:

- `file:/path/to/tokens` - Local file (K8s secrets)
- `vault:secret/path:key` - OpenBao/Vault
- `aws:secret-name:key` - AWS Secrets Manager

#### mTLS Authentication

```yaml
server:
  tls:
    enabled: true
    cert_file: /etc/ssl/server.crt
    key_file: /etc/ssl/server.key
    ca_file: /etc/ssl/ca.crt
    client_auth: required
  auth:
    mode: mtls
```

## API Endpoints

### POST /ingest

Accepts JSON payloads for ingestion.

```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <YOUR_TOKEN>" \
  -d '{"event_type": "login", "user_id": "123"}'
```

**Response Codes:**

- `202 Accepted` - Message queued for delivery
- `400 Bad Request` - Invalid JSON or validation failure
- `401 Unauthorized` - Authentication failed
- `503 Service Unavailable` - Downstream unavailable or under pressure

### GET /health/live

Kubernetes liveness probe.

```bash
curl http://localhost:8080/health/live
# OK
```

### GET /health/ready

Kubernetes readiness probe. Returns 503 if downstream sinks are unavailable.

```bash
curl http://localhost:8080/health/ready
```

## Routing

Messages are routed to Kafka topics based on JSON field extraction:

```yaml
routing:
  topic_fields:
    - "event.category"
    - "event_type"
  default_topic: "unmatched"
  topic_suffix: "_land"
  category_to_topic:
    auth: "logs_auth"
    network: "logs_network"
  dlq:
    enabled: true
    topic: "dlq_land"
```

Given `{"event": {"category": "auth"}}`, routes to `logs_auth_land`.

## Metrics

Prometheus metrics available at the configured metrics endpoint:

```yaml
metrics:
  address: "0.0.0.0:9090"
```

Key metrics:

- `dfe_receiver_requests_total` - Total requests by status
- `dfe_receiver_bytes_received_total` - Total bytes ingested
- `dfe_receiver_kafka_messages_sent_total` - Messages sent to Kafka
- `dfe_receiver_memory_pressure` - Current memory pressure (0-1)

## Architecture

All protocols share the same core pipeline after normalisation:

```text
┌─────────────────────────────────────────────────────┐
│                  Protocol Handlers                  │
│  (each independently enabled, spawned in parallel)  │
│                                                     │
│  HTTP(S)          →  JSON passthrough               │
│  gRPC/Vector      →  protobuf → JSON                │
│  OTLP gRPC/HTTP   →  OTel proto → JSON              │
│  Lumberjack/Beats →  msgpack frames → JSON          │
│  Splunk HEC       →  HEC JSON → normalised JSON     │
│  Syslog UDP/TCP   →  RFC5424/3164 → JSON            │
│  Fluent Forward   →  msgpack → JSON                 │
│  GELF TCP         →  GELF JSON → normalised JSON    │
│  Prometheus RW    →  protobuf timeseries → JSON     │
└───────────────────────┬─────────────────────────────┘
                        │ bytes::Bytes (normalised JSON)
                        ▼
               Auth middleware
               (header / bearer token / mTLS)
                        │
                        ▼
               JSON validation
               (sonic_rs SIMD — optional field checks)
                        │
                        ▼
               Router
               (zero-copy field extraction → topic name)
                        │
                        ▼
               TieredSink
               (in-memory buffer + CircuitBreaker)
                 │                    │
                 ▼                    ▼
           Kafka topics          dfe-loader
           (librdkafka,          (direct Kafka
            batched/LZ4)          input topic)
```

## Development

```bash
# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run -- --config config.yaml

# Run integration tests (requires Kafka)
cargo test --test integration_kafka -- --ignored
```

## License

This project is licensed under the Functional Source License, Version 1.1,
Apache 2.0 Future License (FSL-1.1-ALv2). See [LICENSE](LICENSE) for details.

Copyright (c) 2026 HYPERI PTY LIMITED

For commercial licensing options, see [COMMERCIAL.md](COMMERCIAL.md).
