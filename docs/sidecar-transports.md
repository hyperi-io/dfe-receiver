# Sidecar Transport Pattern

dfe-receiver natively supports these ingest protocols:

| Protocol | Port | Format |
|----------|------|--------|
| HTTP JSON | 8080 | POST JSON body |
| gRPC (Vector-compat) | 6000 | Vector native protocol |
| Syslog (RFC 3164/5424) | 514 / 6514 | UDP/TCP / TLS syslog |
| GELF | 12201 | Graylog Extended Log Format |
| Splunk HEC | 8088 | Splunk HTTP Event Collector |
| OTLP | 4317 | OpenTelemetry Protocol |
| Fluent Forward | 24224 | Fluentd/Fluent Bit forward protocol |
| Lumberjack/Beats | 5044 | Elastic Beats protocol |
| Prometheus Remote Write | 9091 | Prometheus remote write |

For protocols not natively supported, use a **sidecar** — a lightweight
process deployed alongside receiver that collects data from the source
protocol and pushes it to receiver's ingest endpoint.

## Recommended: Vector Sidecar

[Vector](https://vector.dev/) is a high-performance observability data
pipeline with 50+ source types. It speaks receiver's native gRPC protocol
via the `vector` sink, making it the ideal sidecar for unsupported protocols.

Deploy Vector in the same Kubernetes pod or Docker Compose service.
It collects from the source protocol and pushes to receiver's gRPC
ingest port (`:6000`).

```text
[Source Protocol] --> Vector (sidecar) --> gRPC :6000 --> dfe-receiver --> Kafka
```

### Example: SNMP Traps

Collect SNMP traps and forward to receiver.

```yaml
# vector.yaml
sources:
  snmp_traps:
    type: socket
    mode: udp
    address: "0.0.0.0:162"
    decoding:
      codec: bytes

transforms:
  snmp_parse:
    type: remap
    inputs: ["snmp_traps"]
    source: |
      .source = "snmp"
      .timestamp = now()
      .source_address = .host

sinks:
  receiver:
    type: vector
    inputs: ["snmp_parse"]
    address: "localhost:6000"
```

### Example: Docker Container Logs

Collect logs from Docker containers via the Docker API.

```yaml
# vector.yaml
sources:
  docker:
    type: docker_logs
    include_containers:
      - "app-*"
      - "service-*"

transforms:
  enrich:
    type: remap
    inputs: ["docker"]
    source: |
      .source = "docker"
      .container = .container_name

sinks:
  receiver:
    type: vector
    inputs: ["enrich"]
    address: "dfe-receiver:6000"
```

### Example: AWS S3 / SQS

Collect logs from S3 buckets via SQS notifications.

```yaml
# vector.yaml
sources:
  s3_logs:
    type: aws_s3
    region: "ap-southeast-2"
    sqs:
      queue_url: "https://sqs.ap-southeast-2.amazonaws.com/123456789/log-notifications"

transforms:
  tag:
    type: remap
    inputs: ["s3_logs"]
    source: |
      .source = "aws_s3"

sinks:
  receiver:
    type: vector
    inputs: ["tag"]
    address: "localhost:6000"
```

### Example: StatsD / DogStatsD

Collect StatsD metrics on UDP :8125 and forward to receiver.

```toml
# vector.toml
[sources.statsd]
type = "statsd"
mode = "udp"
address = "0.0.0.0:8125"

[transforms.to_json]
type = "remap"
inputs = ["statsd"]
source = '''
.source = "statsd"
.timestamp = now()
'''

[sinks.receiver]
type = "vector"
inputs = ["to_json"]
address = "localhost:6000"
```

### Example: Windows Event Log

> **Reference only — requires a Windows Vector agent.** Run Vector on the
> Windows host and push to receiver over the network.

```toml
# vector.toml (Windows host)
[sources.windows_events]
type = "windows_event_log"
channels = ["Application", "Security", "System"]

[transforms.enrich]
type = "remap"
inputs = ["windows_events"]
source = '''
.source = "windows_event_log"
.host = get_hostname!()
'''

[sinks.receiver]
type = "vector"
inputs = ["enrich"]
address = "dfe-receiver.example.com:6000"
```

### Example: Kafka as Source

Useful for cross-cluster routing or reprocessing events from an existing
Kafka topic into a different pipeline.

```toml
# vector.toml
[sources.kafka_source]
type = "kafka"
bootstrap_servers = "kafka-broker:9092"
group_id = "dfe-reprocess"
topics = ["raw_events"]
auto_offset_reset = "earliest"

[transforms.tag]
type = "remap"
inputs = ["kafka_source"]
source = '''
.source = "kafka_reprocess"
'''

[sinks.receiver]
type = "vector"
inputs = ["tag"]
address = "localhost:6000"
```

## Alternative: Fluent Bit Sidecar

[Fluent Bit](https://fluentbit.io/) is a lightweight log processor that
can push to receiver's Fluent Forward endpoint (`:24224`).

```text
[Source Protocol] --> Fluent Bit (sidecar) --> Forward :24224 --> dfe-receiver --> Kafka
```

### Example: Tail Files + Forward

```ini
# fluent-bit.conf
[INPUT]
    Name   tail
    Path   /var/log/custom-app/*.log
    Tag    custom

[OUTPUT]
    Name   forward
    Match  *
    Host   localhost
    Port   24224
```

Fluent Bit is a good choice when:

- You need a very small memory footprint (<1 MB)
- You're already in a Fluent ecosystem (Fluentd/Fluent Bit)
- The source is file-based (tail, systemd) rather than network protocol

## Alternative: Custom HTTP Sidecar

For fully custom protocols where neither Vector nor Fluent Bit has a
source, write a small program in any language that speaks your protocol
and POSTs JSON to receiver's HTTP ingest.

```text
[Custom Protocol] --> your-collector --> POST :8080 --> dfe-receiver --> Kafka
```

### Example: Python Collector

```python
import httpx

def collect_and_forward(data: dict, receiver_url: str = "http://localhost:8080/ingest"):
    response = httpx.post(receiver_url, json=data)
    response.raise_for_status()
```

### Example: Go Collector

```go
func forward(data []byte) error {
    resp, err := http.Post("http://localhost:8080/ingest", "application/json", bytes.NewReader(data))
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("receiver returned %d", resp.StatusCode)
    }
    return nil
}
```

## Kubernetes Deployment

Deploy the sidecar in the same pod as receiver:

```yaml
# values.yaml (Helm)
extraContainers:
  - name: vector-sidecar
    image: timberio/vector:0.57.0-alpine
    args: ["--config", "/etc/vector/vector.yaml"]
    volumeMounts:
      - name: vector-config
        mountPath: /etc/vector
    resources:
      requests:
        cpu: 100m
        memory: 128Mi
      limits:
        cpu: 500m
        memory: 256Mi

extraVolumes:
  - name: vector-config
    configMap:
      name: vector-sidecar-config
```

The sidecar communicates with receiver via `localhost:6000` (gRPC) or
`localhost:8080` (HTTP) — no network hop, minimal latency.

## Choosing a Sidecar

| Criteria | Vector | Fluent Bit | Custom |
|----------|--------|------------|--------|
| Protocol coverage | 50+ sources | 30+ inputs | Unlimited |
| Memory footprint | ~30 MB | ~1 MB | Varies |
| Best transport to receiver | gRPC (Vector native) | Fluent Forward | HTTP JSON |
| Language | Rust | C | Any |
| When to use | Most cases | Minimal footprint, file tailing | Novel/proprietary protocols |

## Troubleshooting

### Connection refused

- Receiver may not be ready yet — add a startup delay or use an `initContainer` (see below).
- Wrong port — gRPC ingest is `:6000`, HTTP is `:8080`. Confirm the sidecar config matches.
- Firewall or Kubernetes NetworkPolicy blocking loopback or pod-to-pod traffic. Within a pod, `localhost` is always reachable — check NetworkPolicy only for cross-pod setups.

### Auth failures

- **Bearer token mismatch** — the token in the sidecar sink config must exactly match one of the tokens in receiver's `auth.bearer_tokens` list (or the secret source).
- **TLS certificate issues** — if receiver has TLS enabled, the sidecar must trust receiver's CA. Mount the CA cert and set `tls.ca_file` in the Vector sink.
- **mTLS** — receiver's `auth.mtls` requires a client certificate. Set `tls.crt_file` and `tls.key_file` in the sidecar sink config.

### Payload too large

Receiver's default `max_body_size` is 16 MiB per request. Large batches from Vector's `batch.max_bytes` can exceed this.

Options:

- Increase receiver's limit: set `server.max_body_size: "64MiB"` in receiver config.
- Reduce Vector's batch size: set `batch.max_bytes = 8388608` (8 MiB) in the sink.

### TLS between sidecar and receiver

```toml
# vector.toml — TLS sink config
[sinks.receiver]
type = "vector"
inputs = ["my_source"]
address = "localhost:6000"

[sinks.receiver.tls]
enabled = true
ca_file = "/etc/ssl/certs/receiver-ca.pem"   # Trust receiver's CA
# mTLS — only required if receiver has mtls enabled:
crt_file = "/etc/ssl/certs/sidecar-client.pem"
key_file = "/etc/ssl/private/sidecar-client.key"
```

## Performance Sizing

### When one sidecar is enough

A single Vector sidecar handles most workloads up to roughly:

- ~50,000 events/sec on a modern CPU core
- ~100 MB/s of log throughput

Beyond that, consider deploying Vector as a separate Deployment (DaemonSet for node-level collection, Deployment for protocol aggregation) rather than a per-pod sidecar.

### Sidecar container resource starting point

```yaml
resources:
  requests:
    cpu: 100m
    memory: 64Mi
  limits:
    cpu: 500m
    memory: 256Mi
```

Increase `cpu` limit if you see throttling under load. Increase `memory` limit if Vector buffers events to disk during backpressure spikes.

### Backpressure behaviour

When receiver is overloaded it returns `503 Service Unavailable`. Vector responds by:

1. Pausing event ingestion from the source.
2. Buffering events to its disk buffer (configure `data_dir` and `buffer.max_size`).
3. Retrying delivery with exponential backoff.

This means the sidecar is self-regulating — set an appropriate disk buffer size to absorb short bursts, and rely on KEDA / HPA to scale receiver for sustained high load.

## Health Check Integration

### Sidecar readiness depending on receiver

The sidecar should not be marked ready until receiver is accepting connections.
Use an `initContainer` to wait for receiver's health endpoint:

```yaml
# pod spec
initContainers:
  - name: wait-for-receiver
    image: curlimages/curl:8.6.0
    command:
      - sh
      - -c
      - |
        until curl -sf http://localhost:8080/livez; do
          echo "waiting for receiver..."
          sleep 2
        done

containers:
  - name: dfe-receiver
    # ...

  - name: vector-sidecar
    # ...
    readinessProbe:
      httpGet:
        path: /health
        port: 8686        # Vector's internal API port
      initialDelaySeconds: 5
      periodSeconds: 10
```

### Docker Compose ordering

```yaml
services:
  dfe-receiver:
    image: ghcr.io/hyperi-io/dfe-receiver:latest
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8080/livez"]
      interval: 5s
      start_period: 10s

  vector-sidecar:
    image: timberio/vector:0.57.0-alpine
    depends_on:
      dfe-receiver:
        condition: service_healthy
```
