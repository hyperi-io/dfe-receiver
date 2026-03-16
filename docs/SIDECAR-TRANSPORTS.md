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

```
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

## Alternative: Fluent Bit Sidecar

[Fluent Bit](https://fluentbit.io/) is a lightweight log processor that
can push to receiver's Fluent Forward endpoint (`:24224`).

```
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

```
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
    image: timberio/vector:0.54.0-alpine
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
