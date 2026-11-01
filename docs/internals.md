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
   - Refuse a body that is not JSON with a 400, counted on `receiver_validation_failures_total{reason="invalid_json"}`; route a record missing a required field to the DLQ or refuse it, per `dlq_on_invalid`
   - Refuse JSON nested deeper than 64 levels with a 400, before any parse reads it
   - Refuse valid JSON that is not an object (an array, string, number, bool or null) with a 400, counted as `reason="not_an_object"`

   JSON is the only payload format. MessagePack, supported in DFE/XDR 2.0 and 2.1, is deprecated in DFE 2.2 and no longer accepted: the JSON path (SIMD parsing with sonic-rs, zstd on the wire) is fast enough that MessagePack gave no CPU saving.

   Fluent Forward input (fluentd) is still accepted: it is converted to JSON at the receiver.

3. **Routing and tenancy**
   - A scalo rule set (`rules`) sets `_source`, then `_org_id`, first match per field
   - `_org_id` is clamped to the orgs the sender's credential or listener is bound to (`ingest_bindings`), unbound senders to `routing.default_org_id`
   - Source-to-topic remapping, destinations by source, and default source ("main")
   - Support direct routing to dfe-loader
   - `_timestamp_received` enrichment (epoch ms injection): the receiver is the first hop and always writes its own, keeping a sender's as `_timestamp_received_original`
   - `usage_ingested_bytes_total{org_id}` counts the bytes each org's accepted records carried

4. **Authentication**
   - Header-based authentication (static values)
   - Bearer token authentication (static or from secret manager)
   - mTLS client certificate validation
   - Certificates from file or secret manager (AWS, OpenBao/Vault)

5. **Resilience**
   - A sender is answered once every destination confirmed its records (`acknowledgements.enabled`, default on)
   - For answers given at enqueue: in-memory buffering with backpressure, disk spillover opt-in
   - Circuit breaker for failing sinks
   - Memory pressure detection and backpressure

### Non-Functional Requirements

1. **Performance** - PB/s daily throughput capability
2. **Latency** - Sub-millisecond hot path processing
3. **Resource Efficiency** - Minimal allocations on hot path
4. **Observability** - Prometheus metrics, structured logging
5. **Operability** - Kubernetes-native (KEDA scaling, health probes)

## Architecture

```text
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
               │   Kafka Sink      │ │ DLQ Sink    │ │  gRPC Sink        │
               │   (rdkafka)       │ │             │ │  (scalo Push)     │
               └─────────┬─────────┘ └─────────────┘ └───────────────────┘
                         │
               ┌─────────▼─────────┐
               │   TieredSink      │
               │ (circuit breaker  │
               │  + memory buffer)    │
               └───────────────────┘
```

## Where parsing happens

We parse mechanical transport at max efficiency at the edge. The receiver parses the transport layer of each protocol at full efficiency and emits JSON. For syslog that is the header (PRI, timestamp, host, app name), for beats and HEC the envelope, for OTLP the framing. The payload goes in `message` and the parsed header fields sit beside it.

An RFC 3164 token is the app name only when it reads `name:` or `name[pid]:`, the name drawn from ASCII alphanumerics and `-_./`. Anything else stays at the front of `message`, so a tagless PAN-OS line keeps its whole CSV body and FortiGate keeps its leading `date=`. A Cisco stamp's `.407 EST:` tail goes into the timestamp's fraction, with the zone abbreviation dropped.

Parsing a vendor's message body (grok, dissect, a pipeline) is the transforms' job. They read `message` plus the header fields.

Raw capture (`raw_capture.enabled`, default `false`) keeps the wire bytes in `_raw`. It doubles every record, so it is an operator fallback for a single source and never the normal input to a transform. A record parsed wrongly here is fixed in the receiver, not by turning capture on downstream. See [Raw payload retention](#raw-payload-retention) and the fleet rule in [design-principles.md](https://github.com/hyperi-io/dfe-infra/blob/main/docs/design-principles.md).

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
- `FxHashMap` for source-to-topic lookups (faster than std HashMap)
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
- A Vector metric becomes a record with `_vector_type: "metric"`, its name, namespace, kind, tags (`tags_v2` where sent, a multi-valued tag as an array) and `interval_ms`, and its value under its kind: `counter`, `gauge`, `set`, `distribution`, `aggregated_histogram`, `aggregated_summary` or `sketch`. One with no value is refused with `INVALID_ARGUMENT`
- Shared routing/batching pipeline with HTTP
- Same auth middleware (via Tower)

### JSON Validation (sonic_rs)

- SIMD-accelerated parsing
- `LazyValue` for validation without full DOM
- `get_from_slice()` for on-demand field extraction

### Router

One pass per record (`src/routing/mod.rs`):

1. The sender's `_org_id` is removed under `routing.source_org_id: disallow`.
2. The rule set runs over the record's fields, read lazily from the bytes: the first `_source` row that matches, then the first `_org_id` row, which can test the `_source` just chosen.
3. The clamp holds `_org_id` to the orgs the request's principal is bound to. A value outside them is replaced by the binding's default org and counted on `rules_clamped_total`. The record is never refused for it.
4. The `_source` a row set, else `routing.default_source`, picks the destinations (`destinations.sources`, else `destinations.default`) and the topic.
5. `_timestamp_received`, `_source` and `_org_id` are written back in one pass: appended before the closing brace when the record lacks them, a serde rewrite when one must be replaced or its key is spelled twice or with an escape. A sender's `_timestamp_received` is kept as `_timestamp_received_original`, swapped in the bytes with no parse when its key is spelled once and nothing else needs the rewrite. The `_source` written is the row's, else the listener's own stamp (`syslog`, `fluent`, `gelf`, the flow protocol, or `prometheus` on a series with no `_source` label), else the default. Any other `_source` the record carries is the sender's, a remote-write `_source` label included, and is kept as `_source_original` when this replaces it.

The principal is a bearer token's credential id, an unnamed token, or the listener itself for a request with no credential the receiver can name. mTLS client identity is not read: such a request takes its listener's binding.

### Kafka Authentication

SASL-SCRAM-SHA-512 is the standard mechanism for all production deployments.
It works across Apache Kafka, AutoMQ, AWS MSK, Confluent Cloud, and Redpanda
with no code changes. Certificate-based auth (mTLS) and AWS IAM have high
variance between platforms — avoid for cross-platform workloads.

| Scenario | Protocol |
|----------|----------|
| External / internet-facing | `SASL_SSL` |
| Internal K8s (pod-to-pod) | `SASL_PLAINTEXT` |
| Local dev only | `PLAINTEXT` (no auth, no TLS) |

### Kafka Batching

Per-topic batching with configurable thresholds:

- **Size**: 8 MiB default
- **Count**: 10,000 messages default
- **Time**: 20ms linger

Uses rdkafka `FutureProducer` with zstd compression.

### TieredSink

Wraps primary sink with resilience:

1. **Circuit Breaker** (scalo) - Tracks consecutive failures, opens after threshold
2. **In-Memory Queue** - Buffers during circuit-open state
3. **Background Drain** - Retries queued messages when circuit closes

### What a sender is told

A record the pipeline could not take -- memory pressure, a full queue, a destination down, a route with no sink -- is answered with the protocol's retry signal. One refused for good -- malformed, over a destination's size ceiling -- gets its non-retryable answer. A batch stops at the first record not taken, and the sender resends the whole request, so records taken before it arrive twice. With acknowledgements on, a request whose records were not all confirmed within the hold gets the same retry signal for the whole request.

| Listener | Could not take | Refused for good | The protocol rule behind it |
|---|---|---|---|
| HTTP `/ingest`, webhook | 503, `Retry-After: 5` | 400 | RFC 9110 15.6.4: 503 is a temporary condition, and `Retry-After` says when to come back |
| Splunk HEC | 503, code 9 "Server is busy", `Retry-After` | 400, code 6 "Invalid data format" | Splunk answers 503 code 9 when its queue cannot take a payload; HEC senders such as the OpenTelemetry Collector's exporter retry 429 and 503, honouring `Retry-After`, and treat 400, 401 and 403 as permanent |
| Prometheus Remote Write | 503, `Retry-After` | 400 | Remote Write 1.0: senders MUST retry a 5xx and MUST NOT retry a 2xx or a 4xx other than 429 |
| OTLP gRPC | `UNAVAILABLE` | OK with `partial_success`, or `INVALID_ARGUMENT` when every record is refused | OTLP: `UNAVAILABLE` is retryable, `INTERNAL` is not, and a client MUST NOT retry an export whose `partial_success` is populated |
| gRPC push (`vector.Vector/PushEvents`) | `UNAVAILABLE` | `INVALID_ARGUMENT` | the protocol's peer, Vector's `vector` sink, retries every code but `NotFound`, `InvalidArgument`, `AlreadyExists`, `PermissionDenied`, `OutOfRange`, `Unimplemented`, `Unauthenticated` and `DataLoss` -- so `INTERNAL`, the old answer to everything, was retried even for a record that can never land |
| OTLP HTTP | 503, `Retry-After` | 200 with `partial_success`, or 400 when every record is refused | OTLP: 429, 502, 503 and 504 SHOULD be retried, every other 4xx and 5xx MUST NOT be, and `Retry-After` SHOULD be honoured |
| Lumberjack (Beats) | ACK for the events taken, then the connection closes | acknowledged, counted dropped | go-lumber reads a partial ACK as progress and a closed connection as an error, on which Beats re-queues the window's unacknowledged events |
| Fluent Forward with `chunk` | no ack, then the connection closes | acknowledged, counted dropped | Forward v1: a client SHOULD resend a chunk whose request got no `ack` |
| Syslog TCP/TLS, GELF TCP, Fluent Forward without `chunk` | the socket is held unread and the record offered again, backing off to 2s | counted dropped | none of these carries an acknowledgement, so TCP flow control is the only signal a sender reads |
| Syslog UDP | counted dropped | counted dropped | a datagram carries no answer |

No answer carries the receiver's internals: a refused record is told why only when the record itself is at fault, and every other cause is logged and answered with the protocol's generic wording.

"Counted dropped" is `receiver_records_dropped_total{transport,reason}`: a record gone with no way to tell its sender, `reason` one of `unavailable`, `rejected` or `shutdown` (a held record dropped at shutdown). The flow listeners count their drops in `transport_drops_total`.

With acknowledgements on (the default), each answer above is given once the destinations confirmed the records; with them off, once the record is in the receiver's buffer. See [architecture.md](architecture.md#delivery-the-answer-waits-for-the-destination).

### Authentication

#### Bearer Token Provider

```rust
pub struct BearerTokenProvider {
    // SHA-256 of each token, with the credential id its entry names, swapped on reload
    token_hashes: RwLock<HashMap<TokenHash, Option<Arc<str>>>>,
    entries: Entries,
    shutdown_tx: broadcast::Sender<()>,
}
```

Static tokens, `accepted_headers[].values` and `header_values` are `SensitiveString`: redacted when the config is serialised, and marked `x-dfe-secret` in the config schema. A presented credential is compared by its SHA-256 hash.

A bearer entry is `<credential-id>:<token>` or a bare `<token>`. The id is not secret: `ingest_bindings.credentials` binds it to orgs, so no token or hash ever sits in the binding config. An entry that does not parse refuses the config, and a secret refresh that does not parse keeps the tokens in place. A webhook caller's header secret is read whole, colons included.

Supports:

- Static tokens (development)
- Dynamic loading from secret managers
- Background refresh with configurable interval
- Token rotation without restart

A config reload replaces a listener's whole auth section at once: mode, static tokens, accepted headers and secret source, with the replaced secret refresh stopped. It reaches every listener that authenticates -- `server`, `splunk_hec`, `prometheus_rw`, `otlp` (both ports) and `grpc` -- as soon as the reload lands, so a revoked credential is refused on the next request. A reloaded section that does not build, a secret source that will not load say, keeps the current one and logs an error. A gRPC or OTLP gRPC port started with `auth.mode: none` has no interceptor to reload, so turning its auth on or off takes a restart. A webhook caller's secret rotates on its own `refresh_interval_secs`.

Secret source format: `provider:path[:key]`, resolved by
`scalo::secrets::resolve` -- the receiver has no resolver of its own.

- `file:/var/run/secrets/dfe/ingest-bearer-tokens/http`
- `vault:secret/data/auth:bearer_tokens`
- `aws:dfe/ingest/bearer-tokens/http`

AWS Secrets Manager is served by scalo's `secrets-aws` provider, which the receiver builds. An `aws:` spec names the secret, never its ARN, and `aws:name:key` picks one key out of a JSON secret.

## Configuration

7-layer cascade (scalo pattern):

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
  required_fields: ["org_id"]
  dlq_on_invalid: true

rules:
  rules:
    - id: crates-audit
      match: { field: _source, op: equals, value: crates_audit }
      set: { _source: { value: crates_audit } }

ingest_bindings:
  credentials:
    acme-ingest:
      orgs: [acme]

routing:
  default_source: "main"
  default_org_id: "default"
  topic_suffix: "_land"
  # source_to_topic:
  #   auth: "logs_auth"
  dlq:
    enabled: true
    topic: "dfe_receiver_dlq"

kafka:
  brokers: ["kafka.example.com:9094"]
  sasl:
    enabled: true
    mechanism: scram_sha_512   # Works for Apache Kafka, AutoMQ, MSK, Confluent Cloud
    username: dfe-receiver
    password: "${KAFKA_PASSWORD}"
  tls:
    enabled: true              # SASL_SSL for external listeners; omit for internal K8s
  librdkafka_overrides:        # producer tuning over scalo's high-throughput profile
    linger.ms: "20"

buffer:
  memory_limit: 0          # sizes the destination queues; 0 = 1000 records each
  pressure_threshold: 0.8  # share of memory_limit a queue may hold

metrics:
  address: "0.0.0.0:9090"
```

### Raw payload retention

Opt-in retention of the original wire payload in the dfe-schemas common-header
`_raw` field. The parsed event is unchanged -- `_raw` is an addition to it.

`_raw` holds raw data the source provided: a syslog line, a HEC `/raw` line, a
flow record. A JSON payload lives in `_json` alone, so the JSON transports
never write `_raw`. dfe-loader never overwrites a `_raw` the record carries, so
a receiver-populated value is stored as it is. Without one, the legacy flatten
path renames the first of `raw_source_fields` (default `["logoriginal"]`) and
the `json_primary` path reads a field named `_raw`, or whatever the column's
`@renamed` / `@source` names. The reason to populate it here is fidelity the
loader cannot recover: for syslog its fallback lands `message`, the parsed body, after the
PRI, the header and the exact wire spacing are gone. `syslog_loose` never
errors either, so a malformed line silently becomes `message = <whole line>`
and is indistinguishable from a clean parse. Only `_raw` separates them.

Capture is off by default for two reasons. It roughly doubles produced bytes
for text protocols, on top of a full-text index downstream. And `_raw` is the
payload *before* parsing, so redaction, masking or field-dropping applied
downstream to the parsed fields does not reach it -- a credential or PII value
stripped from `message` still sits in `_raw`, indexed for full-text search.
Enable capture for a source only when retaining its raw payload at that
sensitivity is acceptable, and redact `_raw` explicitly wherever the parsed
fields are redacted.

```yaml
raw_capture:              # common default for syslog, HEC /raw and flow
  enabled: false
  max_bytes: 65536        # 0 = unlimited
  on_oversize: truncate   # truncate | omit
syslog:
  raw_capture:
    enabled: true         # per-transport override, inherits the rest
```

Every field is optional at both levels: an unset field inherits, so "off" and
"not configured" stay distinguishable. Flat env vars work at both levels
(`DFE_RECEIVER_RAW_CAPTURE_ENABLED`,
`DFE_RECEIVER_SYSLOG_RAW_CAPTURE_ENABLED`).

| Transport | What `_raw` holds |
|-----------|-------------------|
| `syslog` | the wire line verbatim, PRI and header included |
| `splunk_hec` | `/raw`: the original line bytes |
| `flow` | the decoder's verbatim records -- a JSON array in `canonical`, the single record in `exploded` |

`gelf`, `fluent`, `otlp`, `prometheus_rw`, the HEC `/event` endpoint, `http`,
`grpc` and `lumberjack` do not offer the knob: their payload is JSON (or passes
through untouched), which the loader keeps whole in `_json`, so `_raw` would be
a second copy of the event.

Two markers travel with the value when they apply, so a mangled capture cannot
be mistaken for a faithful one:

- `_raw_truncated: true` -- the tail was dropped to respect `max_bytes`
- `_raw_lossy: true` -- invalid UTF-8 was replaced with U+FFFD

`_raw` exists only in the `timeseries` common-header profile. A source using
`minimal` or `passthrough` pays for capture at the receiver and has the field
dropped at the loader; the receiver cannot see the destination profile, so
this is not checked.

## Webhook intake

`POST /webhook/{caller}` is the door for products that push events -- an alert
rule, a SaaS notification hook -- rather than agents that stream them. It is
its own protocol handler (`src/server/webhook/`), not a bolt-on to `/ingest`:
`/ingest` checks one server-wide credential in middleware before the body is
read, a product is identified per caller and a signed request needs the body.
A product's payload shape is its own, so the `rules` rows cannot pick its
topic. And an own listener lets a deployment expose only this port to the
product's egress.

```mermaid
flowchart LR
    P["Product<br/>(alert rule, hook)"] -->|"POST /webhook/{caller}"| L["Listener<br/>shared ingest port, or webhook.bind_address"]
    L --> LIM["Body limit (webhook.max_body_size)<br/>413 + body_size_rejected"]
    LIM --> CALLER["Caller lookup<br/>404 unknown_caller"]
    CALLER --> AUTH["Per-caller auth<br/>hmac: HMAC-SHA256 over ts.body, replay window<br/>header: hashed shared secret"]
    AUTH -->|401 reason| P
    AUTH --> READY["Readiness<br/>503 + retry-after"]
    READY --> SPLIT["body: single | array"]
    SPLIT --> FILTER["CEL filter<br/>compiled at load, false drops"]
    FILTER --> STAMP["_source = caller<br/>_timestamp_received"]
    STAMP --> TOPIC["process_batch_to_topic(caller.topic)<br/>_org_id held to the caller's binding"]
    TOPIC --> KAFKA[("Kafka")]
```

Two authentication modes, chosen per caller:

| Mode | What the product sends | What it proves | Replay |
|---|---|---|---|
| `hmac` | `X-Signature: sha256=HMAC(secret, "{ts}.{body}")`, `X-Timestamp: <unix secs>` | The body is unmodified and was signed by the secret holder | Refused outside `tolerance_secs` (default 300) as `stale_signature` |
| `header` | A fixed header carrying the shared secret | The sender holds the secret | None -- restrict the source with `server.ip_filter` |

`header` exists because some products can only attach static headers to a
webhook (runZero alert rules are the first caller and the reason); it is the
weaker mode and opt-in per caller. In both modes the secret is a
`provider:path[:key]` reference resolved through the same reader as bearer
tokens, refreshed on an interval, never a literal in the config file.

The path names the caller, so a wrong secret is only tried against one secret
set and callers never share a credential. Authentication runs before the
readiness check, so an unauthenticated client learns nothing about the
pipeline. Failures log at debug and count under
`receiver_auth_failures_total{reason}`; a credential spray writes no warn
line per attempt.

Delivery goes through `process_batch_to_topic`, which validates and
back-pressures but skips the router's enrichment, so the handler stamps
`_source` (the caller name, which wins over a sender-supplied value) and
`_timestamp_received` itself. The org pass still runs: the caller's name is its
credential id, so `_org_id` is held to `ingest_bindings.credentials.<caller>`,
or the default org when it has none. In `body: array` mode every element is checked, filtered and stamped
before any is delivered, so one element that is not an object refuses the
whole request as a 400 with nothing on the topic and the sender's retry
duplicates nothing. An oversize body is a 413 plus `body_size_rejected_total`,
never a DLQ entry: unauthenticated bytes do not enter Kafka.

`webhook.bind_address` unset merges the routes into the ingest listener after
its auth middleware and body limit have been applied to the ingest routes, so
the webhook keeps its own per-caller auth and smaller body limit while sharing
`server.tls`, `server.ip_filter`, `server.rate_limit` and the concurrency cap.
Set, the intake runs on its own port through the same hardened accept loops
and the same rate limit and concurrency cap, under `webhook.tls`.

Configuring a caller end to end -- the authentication choice, the secret
reference, the replay window, the body shape and the filter, with worked
examples and the statuses a sender sees -- is in
[webhook-setup.md](webhook-setup.md).

## Deployment

### Kubernetes with KEDA

The chart a release assembles from the deployment contract scales on CPU alone: 80% utilisation, 1 to 10 replicas, from the contract's `keda` block. A deployment adds the ScalingPressure trigger with the library's `keda.extraTriggers` value. The receiver never scales on raw Kafka consumer lag.

```yaml
triggers:
  - type: cpu
    metricType: Utilization
    metadata:
      value: "80"
```

### Health Probes

Probes hit the metrics listener (9090, port name `metrics`), not the ingest
port, so a saturated intake does not fail its own liveness check.

`/readyz` answers 503 until every listener an enabled handler binds is serving, again once any of them stops, and under memory pressure. A destination outage does not fail it, and ingest sheds per request with 503 instead. The ingest port's `/readyz` gives the same answer.

```yaml
livenessProbe:
  httpGet:
    path: /livez
    port: metrics
readinessProbe:
  httpGet:
    path: /readyz
    port: metrics
```

## Metrics

### Request Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `receiver_requests_total` | Counter | Total requests received |
| `receiver_requests_success_total` | Counter | Total successful requests |
| `receiver_requests_error_total` | Counter | Total failed requests |
| `receiver_bytes_received_total` | Counter | Total bytes ingested |
| `receiver_records_dropped_total` | Counter | Records dropped with no way to tell the sender, by `transport` and `reason` (`unavailable`, `rejected`, `shutdown`) |
| `records_received_total` | Counter | Records, counted once when a listener first offers each to the pipeline |
| `records_processed_total` / `records_delivered_total` | Counter | Records the pipeline took |
| `records_error_total` | Counter | Records refused for good |
| `receiver_messages_spilled_total` | Counter | Records a destination's buffer held back, in memory or on disk |
| `receiver_messages_drained_total` | Counter | Held-back records since sent on |

### Kafka Metrics

Enqueue and delivery are separate counts. librdkafka takes a record into its
own queue first and a broker answers for it later, so a record can be enqueued
and never delivered -- the delivery counters are the ones that say a broker
holds it.

| Metric | Type | Description |
|--------|------|-------------|
| `receiver_kafka_sends_total` | Counter | Messages librdkafka queued |
| `receiver_kafka_bytes_sent_total` | Counter | Bytes queued to Kafka |
| `receiver_kafka_send_errors_total` | Counter | Messages librdkafka refused to queue |
| `receiver_kafka_delivered_total` | Counter | Messages a broker acknowledged |
| `receiver_kafka_delivery_failures_total` | Counter | Messages no broker confirmed, by `reason` (librdkafka error code); a timed-out message may still have been written |
| `receiver_kafka_resends_total` | Counter | Messages answered at enqueue that librdkafka gave up on with a message timeout, queued again |
| `records_dlq_total` | Counter | Messages sent to DLQ (scalo emits this one) |
| `receiver_dlq_start_failures_total` | Counter | DLQ starts that failed, by `backend` (`kafka`, `file` or `other`); the receiver runs on without a DLQ and logs the reason at ERROR |
| `receiver_source_org_id_stripped_total` | Counter | Records whose sender-supplied `_org_id` was removed because `routing.source_org_id` is `disallow` (the default) |
| `receiver_records_rejected_total` | Counter | Records a destination can never take (over its size ceiling), on any sink, by `outcome`: `dead_lettered`, `dropped` when no DLQ is configured or no DLQ backend can hold the entry (also counted in `pipeline_dead_letters_dropped_total{reason}`), or `dlq_refused` when the DLQ did not confirm the write, so the record stays buffered or its sender is told to retry |

### gRPC Destination Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `receiver_destination_send_failures_total` | Counter | Records a gRPC destination did not take, by `reason`: `unavailable` (down, refusing connections, busy or past its deadline) or `failed` (it answered with an error). Both are retried, and each class logs at most once per 10 s per destination |
| `receiver_destination_build_failures_total` | Counter | Destinations a config reload asked for whose sender failed to build, logged at ERROR with the reason. Their records are answered unavailable until a reload builds the sender |
| `receiver_retired_records_total` | Counter | Records held by a destination a config reload retired, by `outcome`: `sent` to it, `dead_lettered`, `spooled` (left in its disk spool), or `dropped` (no DLQ backend could hold the entry, or shutdown came first) |

### Scaling Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `scaling_pressure` | Gauge | Weighted scaling score (0-100), also served at `/scaling/pressure` on the metrics port (scalo emits this one) |
| `scaling_memory_pressure` | Gauge | Memory the runtime guard counts, over its limit (0.0-1.0) |
| `scaling_circuit_open` | Gauge | 1 while the Kafka sink's circuit is open, else 0 |

The score is scalo's `ScalingPressure`, refreshed once a second. Each `scaling.*` component adds its weight in proportion to value over saturation, capped at its full weight. An open Kafka circuit forces 0, since more replicas cannot help. Otherwise memory at or past `scaling.memory_gate_threshold` forces 100. `config.example.yaml` lists the components and what feeds each.

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

## Security Architecture

### Deployment Requirements

> **CRITICAL**: dfe-receiver MUST NOT be directly exposed to the public internet.
> It should always be deployed behind edge protection infrastructure.

The receiver is designed as the **final layer** in a defense-in-depth architecture. Edge infrastructure handles volumetric attacks, bot filtering, and rate limiting, while the receiver focuses on authenticated payload processing.

### Required Deployment Architecture

```mermaid
flowchart TB
    subgraph Internet["Public Internet"]
        Attackers["Attackers / Bots"]
        Legitimate["Legitimate Clients"]
    end

    subgraph Edge["Edge Protection Layer"]
        CDN["CDN / DDoS Protection<br/>(Cloudflare, CloudFront)"]
        WAF["Web Application Firewall<br/>(OWASP rules)"]
        RateLimit["Rate Limiting<br/>(per-IP, per-token)"]
        GeoBlock["Geographic Restrictions<br/>(optional)"]
    end

    subgraph Internal["Internal Network"]
        LB["Load Balancer<br/>(K8s Ingress / ALB)"]
        subgraph Receivers["dfe-receiver Pods"]
            R1["Receiver Pod 1"]
            R2["Receiver Pod 2"]
            R3["Receiver Pod N"]
        end
        Kafka["Kafka Cluster"]
    end

    Attackers --> CDN
    Legitimate --> CDN
    CDN --> WAF
    WAF --> RateLimit
    RateLimit --> GeoBlock
    GeoBlock --> LB
    LB --> R1 & R2 & R3
    R1 & R2 & R3 --> Kafka

    style Attackers fill:#ff6b6b,color:#fff
    style CDN fill:#4ecdc4,color:#fff
    style WAF fill:#4ecdc4,color:#fff
    style RateLimit fill:#4ecdc4,color:#fff
    style GeoBlock fill:#4ecdc4,color:#fff
    style R1 fill:#667eea,color:#fff
    style R2 fill:#667eea,color:#fff
    style R3 fill:#667eea,color:#fff
```

### Security Controls by Layer

```mermaid
flowchart LR
    subgraph Edge["Edge Layer<br/>(CDN/WAF)"]
        E1["DDoS mitigation"]
        E2["Bot detection"]
        E3["Rate limiting"]
        E4["Geo-blocking"]
        E5["WAF rules"]
        E6["TLS termination"]
    end

    subgraph LB["Load Balancer"]
        L1["Connection limits"]
        L2["Health checks"]
        L3["Traffic distribution"]
    end

    subgraph Receiver["dfe-receiver"]
        R1["Request timeout<br/>(slow loris)"]
        R2["Body size limit<br/>(memory DoS)"]
        R3["TLS handshake timeout<br/>(TLS attacks)"]
        R4["Auth middleware<br/>(bearer/header/mTLS)"]
        R5["JSON validation"]
        R6["Structured logging"]
    end

    Edge --> LB --> Receiver
```

### Layer Responsibilities

| Attack Vector | Edge Layer | Load Balancer | dfe-receiver |
|--------------|------------|---------------|--------------|
| **DDoS / Volumetric** | Absorbs | - | - |
| **Bot Traffic** | Blocks | - | - |
| **Rate Abuse** | Limits | - | - |
| **Geographic** | Blocks | - | - |
| **WAF Signatures** | Blocks | - | - |
| **Connection Exhaustion** | - | Limits | - |
| **Slow Loris** | - | - | Request timeout |
| **Memory DoS (large body)** | - | - | Body size limit |
| **Slow TLS** | - | - | Handshake timeout |
| **Unauthenticated Access** | - | - | Auth middleware |
| **Malformed JSON** | - | - | Validation |
| **Invalid Payloads** | - | - | DLQ routing |

### Request Processing Order

The receiver applies security controls in a specific order to minimize resource usage for malicious requests:

```mermaid
sequenceDiagram
    participant Client
    participant Timeout as TimeoutLayer
    participant BodyLimit as RequestBodyLimitLayer
    participant Auth as Auth Middleware
    participant Handler as Ingest Handler
    participant Kafka

    Client->>Timeout: HTTP Request

    alt Request too slow
        Timeout-->>Client: 408 Request Timeout
    else Within timeout
        Timeout->>BodyLimit: Forward request
    end

    alt Body too large
        BodyLimit-->>Client: 413 Payload Too Large
    else Body within limit
        BodyLimit->>Auth: Forward request
    end

    alt Auth failed
        Auth-->>Client: 401 Unauthorized
    else Auth passed
        Auth->>Handler: Forward request
    end

    Handler->>Handler: Validate JSON
    alt Invalid JSON
        Handler-->>Client: 400 Bad Request
    else Valid JSON
        Handler->>Kafka: Route to topic
        alt Delivery report within the hold
            Kafka-->>Handler: Delivered
            Handler-->>Client: 202 Accepted
        else Refused for good, DLQ holds it
            Kafka-->>Handler: Refused
            Handler-->>Client: 202 Accepted
        else Refused for good, no DLQ
            Kafka-->>Handler: Refused
            Handler-->>Client: 400 Bad Request
        else Not confirmed
            Handler-->>Client: 503 Service Unavailable, Retry-After
        end
    end
```

### Security Configuration

```yaml
server:
  # Request limits (applied BEFORE auth to minimize cost)
  max_body_size: 10485760        # 10MB - reject larger payloads
  request_timeout_ms: 30000      # 30s - reject slow clients

  tls:
    enabled: true
    cert_file: "/etc/ssl/receiver.crt"
    key_file: "/etc/ssl/receiver.key"
    ca_file: "/etc/ssl/ca.crt"      # For mTLS
    client_auth: required            # none, optional, required

  auth:
    mode: both                       # none, header, bearer, mtls, both
    accepted_headers:
      - name: "x-api-key"
        values: ["production-key-1", "production-key-2"]
    bearer:
      secret_source: "vault:secret/data/auth:tokens"
      refresh_interval_secs: 300     # Hot-reload tokens
```

### TLS Hardening

The receiver includes TLS hardening for mTLS deployments:

- **Handshake timeout**: 10 seconds (prevents slow TLS attacks)
- **Modern cipher suites**: Via rustls (no legacy ciphers)
- **Client certificate validation**: Optional or required mTLS
- **Certificate refresh**: Supports secret manager integration

### Auth Middleware Order

Authentication is enforced **before** body processing to minimize resource usage:

1. **TimeoutLayer** - Rejects slow requests (slow loris protection)
2. **RequestBodyLimitLayer** - Rejects oversized requests before reading
3. **Auth Middleware** - Rejects unauthenticated requests before processing
4. **Handler** - Only reached by authenticated, properly-sized, timely requests

This order ensures minimal CPU/memory usage for bot scans and unauthenticated probes.

### Admission Controls in dfe-receiver

The receiver carries its own admission controls, so a listener exposed without
an edge layer still has them. An edge layer in front is still the place for
volumetric and reputation-based defence.

| Control | Where it runs | Config |
|---------|---------------|--------|
| IP allowlist/denylist | Every accept loop the receiver owns, before TLS or any protocol work; syslog UDP per datagram | `server.ip_filter` |
| Per-client rate limit | A GCRA limiter per HTTP listener (ingest, webhook, HEC, remote write, OTLP HTTP), keyed on the TCP peer or the client a trusted proxy names, answering 429 | `server.rate_limit`, `server.trusted_proxies` |
| In-flight request cap | Per HTTP listener | `server.max_concurrent_requests` |
| Body size and request timeout | Every HTTP listener, answering 413 and 408 | `server.*`, `splunk_hec.*`, `prometheus_rw.*`, `webhook.*` |

The rate limiter drops a client's entry once its budget has refilled, checked
every 5 seconds, so its memory follows the clients seen recently, not every
client ever seen.

These stay with the edge layer:

| Feature | Reason |
|---------|--------|
| Connection limits | Kubernetes or load balancer handles this |
| Access logging | Edge provides this; duplicating wastes resources |
| Bot detection | WAF/CDN provides sophisticated detection |
| Geographic blocking | Edge layer handles with IP geolocation |

### Monitoring and Alerting

Key security metrics to monitor. Names are bare unless `metrics.namespace`
sets a prefix.

```yaml
# Prometheus alerts
groups:
  - name: dfe-receiver-security
    rules:
      - alert: HighAuthFailureRate
        expr: sum(rate(receiver_auth_failures_total[5m])) > 100
        labels:
          severity: warning
        annotations:
          summary: "High authentication failure rate"

      - alert: HighValidationFailureRate
        expr: sum(rate(receiver_validation_failures_total[5m])) > 50
        labels:
          severity: warning
        annotations:
          summary: "High validation failure rate - check DLQ"

      - alert: RequestTimeoutSpike
        expr: sum by (transport) (rate(receiver_request_timeouts_total[5m])) > 10
        labels:
          severity: info
        annotations:
          summary: "Request timeout spike - possible slow loris attempt"

      - alert: OversizeBodyProbe
        expr: sum by (transport) (rate(receiver_body_size_rejected_total[5m])) > 10
        labels:
          severity: info
        annotations:
          summary: "Requests refused for their size"

      - alert: IpFilterRejecting
        expr: sum by (transport) (rate(receiver_ip_filter_rejected_total[5m])) > 0
        for: 15m
        labels:
          severity: info
        annotations:
          summary: "The IP filter is refusing connections - check server.ip_filter covers every sender"
```

A refused credential is also written as a `security` target event, at most
one line per failure reason every 5 seconds; the counter carries every one.

## Flow (NetFlow + sFlow) -- EXPERIMENTAL

Native UDP ingestion for NetFlow v5/v9, IPFIX, and sFlow v5 with autosense
dispatch and configurable output modes.

**Status:** EXPERIMENTAL when first enabled. Handler emits a startup `WARN`
log and sets `handler_experimental{handler="flow"} 1`. Set
`flow.experimental: false` once stability is proven.

### Ports

| Port | Convention | Accepts |
|---|---|---|
| 2055/udp | NetFlow historic | v5/v9/IPFIX/sFlow (autosense) |
| 4739/udp | IPFIX IANA | v5/v9/IPFIX/sFlow (autosense) |
| 6343/udp | sFlow IANA | v5/v9/IPFIX/sFlow (autosense) |

### Output modes

- `canonical` (default) -- one JSON envelope per UDP datagram with `flows: [...]` array
- `exploded` -- one JSON envelope per flow record (amplifies events; use for per-flow analytics)

Raw record retention is no longer a mode. `flow.raw_capture.enabled` adds the
decoder's verbatim output to the event's common-header `_raw` field, and works
with either mode -- a JSON array of every record in `canonical`, the single
record in `exploded`. See [Raw payload retention](#raw-payload-retention).

### Template cache

Each v9 and IPFIX exporter keeps its own templates, at most `flow.netflow.template_cache.max_per_exporter` (default 1000) of them, v9 and IPFIX together. A template past the cap evicts that exporter's oldest, counted on `flow_template_evicted_total`, so a flooding exporter holds bounded memory. An exporter whose evicted template it still uses sees template misses until it resends the template, which exporters do on a timer. At most `max_exporters` exporters are kept.

### Record kinds

- `flow` -- standard NetFlow / sFlow record
- `counter` -- sFlow counter sample (interface statistics)
- `security_event` -- NSEL firewall event (Cisco ASA firewallEvent / NF_F_FW_EVENT)
- `nat_translation` -- CGNAT NAT44 translation event (natEvent IE)

### Modes

- **Unified** (default): one listener per port accepts all flow protocols
- **Split**: opt-in via `flow.split:` -- separate NetFlow-only and sFlow-only listeners

### Configuration

See `config.example.yaml` for the full `flow:` block.

### Known limitations (v1)

- **NetFlow v7 is not supported.** v7 is a Cisco-proprietary variant for
  early-2000s Catalyst switches with hybrid L2+L3 flow tracking. Never widely
  adopted; current Cisco gear emits v9 or IPFIX. A v7 packet hitting our
  listener returns a parse error visible in
  `transport_decode_err_total{transport="netflow", reason="parse_err"}`.
  If a customer ever reports v7 exporters, support can be added reactively
  (~1 day of hand-rolled wire decode like our v5 implementation).
- Template-miss errors currently conflated with parse errors in metrics
  (`transport_decode_err_total{reason="template_miss"}` will be 0)
- `t_flow_start`/`t_flow_end` carry relative `sysup:<ms>` strings rather than
  absolute RFC 3339 timestamps (sysUpTime anchor resolution deferred)
- `recvmmsg(2)` batch syscall deferred. Linux listener uses `AsyncFd::readable`
  \+ per-packet `recv_from` drain loop (capped at 256 packets per readiness
  event). Functionally equivalent below ~100K pps.

## Completed Milestones

- [x] gRPC Vector sink protocol
- [x] Sidecar transport pattern (see `docs/sidecar-transports.md`)
- [x] Config hot-reload via SIGHUP (routing, validation, enrichment)
- [x] Rule-set routing with credential-bound tenancy (`rules`, `ingest_bindings`)
- [x] `_timestamp_received` enrichment
- [x] 9-protocol multi-protocol ingestion (HTTP, gRPC, OTLP, Lumberjack, Splunk HEC, Syslog, Fluent, GELF, Prometheus RW)
- [x] Flow handler (NetFlow v5/v9/IPFIX + sFlow v5 + NSEL + NAT44, autosense UDP, EXPERIMENTAL)
- [x] Webhook intake (`POST /webhook/{caller}`, per-caller HMAC or static-header auth, per-caller topic)

## References

- [dfe-loader](https://github.com/hyperi-io/dfe-loader) - Reference implementation patterns
- [scalo](https://github.com/hyperi-io/scalo-rs) - Shared data-plane runtime (crate: `scalo`)
- [Vector HTTP sink](https://vector.dev/docs/reference/configuration/sinks/http/)
- [Vector sink (gRPC)](https://vector.dev/docs/reference/configuration/sinks/vector/)
