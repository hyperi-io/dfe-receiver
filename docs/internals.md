# dfe-receiver internals

How the receiver's shared path works: what it rejects, how a record is routed, what each listener tells its sender, auth, Kafka and deployment. Why it is built this way, and the component diagram, are in [architecture.md](architecture.md). What each protocol parses is in [protocols.md](protocols.md), delivery and buffering in [delivery.md](delivery.md), the webhook intake in [webhook-setup.md](webhook-setup.md), the metrics in [metrics.md](metrics.md), and the hardening and fronting in [hardening.md](hardening.md).

## What the receiver rejects

- A body that is not JSON: 400, counted on `receiver_validation_failures_total{reason="invalid_json"}`.
- JSON nested deeper than 64 levels: 400, before any parse reads it.
- Valid JSON that is not an object (an array, string, number, bool or null): 400, counted as `reason="not_an_object"`.
- A record missing a field `validation.required_fields` names: to the DLQ or refused, per `validation.dlq_on_invalid`.

JSON is the one payload format. MessagePack, supported in DFE/XDR 2.0 and 2.1, was dropped in DFE 2.2: the JSON path (SIMD parsing with sonic-rs, zstd on the wire) left it no CPU saving. Fluent Forward is still accepted, and converted to JSON here.

Validation reads the payload with sonic-rs `LazyValue` and `get_from_slice()`, never a full DOM, so it proves the payload is a JSON object carrying the required fields and nothing more.

## Routing a record

One pass per record (`src/routing/mod.rs`):

1. The sender's `_org_id` is removed under `routing.source_org_id: disallow`.
2. The rule set runs over the record's fields, read lazily from the bytes: the first `_source` row that matches, then the first `_org_id` row, which can test the `_source` just chosen.
3. The clamp holds `_org_id` to the orgs the request's principal is bound to. A value outside them is replaced by the binding's default org and counted on `rules_clamped_total`. The record is never refused for it.
4. The `_source` a row set, else `routing.default_source`, picks the destinations (`destinations.sources`, else `destinations.default`) and the topic.
5. `_timestamp_received`, `_source` and `_org_id` are written back in one pass: appended before the closing brace when the record lacks them, a serde rewrite when one must be replaced or its key is spelled twice or with an escape. A sender's `_timestamp_received` is kept as `_timestamp_received_original`, swapped in the bytes with no parse when its key is spelled once and nothing else needs the rewrite. The `_source` written is the row's, else the listener's stamp (`syslog`, `fluent`, `gelf`, the flow protocol, or `prometheus` on a series with no `_source` label), else the default. Any other `_source` in the record is the sender's, a remote-write `_source` label included, and is kept as `_source_original` when this replaces it, or as the next free `_source_original_<n>` when the record has one.

The principal is a bearer token's credential id, an unnamed credential (a token listed with no id, or an accepted header value), or the listener itself for a request with no credential the receiver can name. mTLS client identity is not read: such a request takes its listener's binding.

## What a sender is told

A record the pipeline could not take -- memory pressure, a full queue, a destination down, a route with no sink -- is answered with the protocol's retry signal. One refused for good -- malformed, over a destination's size ceiling -- gets its non-retryable answer. A batch stops at the first record not taken, and the sender resends the whole request, so records taken before it arrive twice. With acknowledgements on, a request with any record not confirmed within the hold gets the same retry signal for the whole request.

| Listener | Could not take | Refused for good | The protocol rule behind it |
|---|---|---|---|
| HTTP `/ingest`, webhook | 503, `Retry-After: 5` | 400 | RFC 9110 15.6.4: 503 is a temporary condition, and `Retry-After` says when to come back |
| Splunk HEC | 503, code 9 "Server is busy", `Retry-After` | 400, code 6 "Invalid data format" | Splunk answers 503 code 9 when its queue cannot take a payload. HEC senders such as the OpenTelemetry Collector's exporter retry 429 and 503, honouring `Retry-After`, and treat 400, 401 and 403 as permanent |
| Prometheus Remote Write | 503, `Retry-After` | 400 | Remote Write 1.0: senders MUST retry a 5xx and MUST NOT retry a 2xx or a 4xx other than 429 |
| OTLP gRPC | `UNAVAILABLE` | OK with `partial_success`, or `INVALID_ARGUMENT` when every record is refused | OTLP: `UNAVAILABLE` is retryable, `INTERNAL` is not, and a client MUST NOT retry an export with `partial_success` populated |
| gRPC push (`vector.Vector/PushEvents`) | `UNAVAILABLE` | `INVALID_ARGUMENT` | the protocol's peer, Vector's `vector` sink, retries every code but `NotFound`, `InvalidArgument`, `AlreadyExists`, `PermissionDenied`, `OutOfRange`, `Unimplemented`, `Unauthenticated` and `DataLoss` |
| OTLP HTTP | 503, `Retry-After` | 200 with `partial_success`, or 400 when every record is refused | OTLP: 429, 502, 503 and 504 SHOULD be retried, every other 4xx and 5xx MUST NOT be, and `Retry-After` SHOULD be honoured |
| Lumberjack (Beats) | ACK for the events taken, then the connection closes | acknowledged, counted dropped | go-lumber reads a partial ACK as progress and a closed connection as an error, on which Beats re-queues the window's unacknowledged events |
| Fluent Forward with `chunk` | no ack, then the connection closes | acknowledged, counted dropped | Forward v1: a client SHOULD resend a chunk if its request got no `ack` |
| Syslog TCP/TLS, GELF TCP, Fluent Forward without `chunk` | the socket is held unread and the record offered again, backing off to 2s | counted dropped | none of these has an acknowledgement, so TCP flow control is the one signal a sender reads |
| Syslog UDP | counted dropped | counted dropped | a datagram has no answer |

No answer reveals the receiver's internals: a refused record is told why when the record itself is at fault. Every other cause is logged and answered with the protocol's generic wording.

"Counted dropped" is `receiver_records_dropped_total{transport,reason}`: a record gone with no way to tell its sender, `reason` one of `unavailable`, `rejected`, `shutdown` (a held record dropped at shutdown), `oversize` or `kernel`. The flow listeners count their drops in `transport_drops_total`.

With acknowledgements on (the default), each answer above is given once the destinations confirmed the records, and with them off, once the record is in the receiver's buffer. See [delivery.md](delivery.md).

## Authentication

```rust
pub struct BearerTokenProvider {
    // SHA-256 of each token, with the credential id its entry names, swapped on reload
    token_hashes: RwLock<HashMap<TokenHash, Option<Arc<str>>>>,
    entries: Entries,
    shutdown_tx: broadcast::Sender<()>,
}
```

Static tokens, `accepted_headers[].values` and `header_values` are `SensitiveString`: redacted when the config is serialised, and marked `x-scalo-secret` in the config schema. A presented credential is compared by its SHA-256 hash. A bearer entry is `<credential-id>:<token>` or a bare `<token>`. The id is not secret: `ingest_bindings.credentials` binds it to orgs, so no token or hash ever sits in the binding config. An entry that does not parse rejects the config, and a secret refresh that does not parse keeps the tokens in place. A webhook caller's header secret is read whole, colons included.

Tokens come from the config (development) or a secret source, refreshed every `auth.bearer.refresh_interval_secs`, so a token rotates without a restart. A config reload replaces a listener's whole auth section at once: mode, static tokens, accepted headers and secret source, with the replaced secret refresh stopped. It reaches every listener that authenticates -- `server`, `splunk_hec`, `prometheus_rw`, `otlp` (both ports) and `grpc` -- as soon as the reload lands, so a revoked credential is refused on the next request. A reloaded section that does not build, a secret source that will not load say, keeps the current one and logs an error. A webhook caller's secret rotates on the caller's `refresh_interval_secs`.

The certificate, key and client CA of every TLS listener, from a file or a secret reference, are re-read every `tls.refresh_interval_secs`, and a rotated certificate is served from the next handshake. A certificate, key or CA is set as a file or as a secret, never both: startup refuses both. Everything else under `tls.*`, the client-certificate policy included, is read once at start. A change to it needs a restart. A reload into `mtls` or `both` is refused on a listener with a running handshake that admits clients with no certificate: the current auth stays and an error is logged.

The gRPC and OTLP gRPC ports authenticate in a tonic interceptor that checks bearer tokens and nothing else, so `header` and `both` are refused at startup there. Under `mtls` the interceptor checks no token, because the handshake verified the client certificate before it. Startup rejects `mtls` there without `tls.client_auth: required` and the CA. A port started with `auth.mode: none` has no interceptor to reload, so turning its auth on or off takes a restart.

Secret source format: `provider:path[:key]`, resolved by `scalo::secrets::resolve` -- the receiver resolves no secret itself. Examples: `file:/var/run/secrets/dfe/ingest-bearer-tokens/http`, `vault:secret/data/auth:bearer_tokens`, `aws:dfe/ingest/bearer-tokens/http`. AWS Secrets Manager is served by scalo's `secrets-aws` provider, which the receiver builds. An `aws:` spec names the secret, never its ARN, and `aws:name:key` picks one key out of a JSON secret.

## Kafka

SASL-SCRAM-SHA-512 is the mechanism for production. It works with no code change on Apache Kafka, AutoMQ, AWS MSK and Redpanda. Confluent Cloud takes SASL/PLAIN with an API key and secret, or OAUTHBEARER, and no SCRAM. Certificate-based auth (mTLS) and AWS IAM vary between platforms, so avoid them for cross-platform workloads.

| Scenario | Protocol |
|----------|----------|
| External / internet-facing | `SASL_SSL` |
| Internal K8s (pod-to-pod) | `SASL_PLAINTEXT` |
| Local dev | `PLAINTEXT` (no auth, no TLS) |

librdkafka batches per scalo's `throughput` sizing profile: `batch.size` 128 KiB, `linger.ms` 20, zstd level 3 and a 64 MiB producer queue. The producer is an rdkafka `ThreadedProducer`, built by the receiver for its delivery reports. Tune it through `kafka.librdkafka_overrides`.

## Configuration

There are two ways in.

- Without `--config` it's scalo's cascade: compiled defaults, then `defaults.yaml`, `settings.yaml` and `settings.{env}.yaml`, each searched in `./`, `./config/`, `/config/` and `~/.config/dfe-receiver/`, then `./.env`, then `DFE_RECEIVER_*` environment variables, then CLI arguments.
- The container passes `--config /etc/dfe-receiver/config.yaml`, which reads that mounted file over the compiled defaults, then the flat `DFE_RECEIVER_*` variables.

Routing, validation and enrichment reload on SIGHUP and when the config file changes, and so do the gRPC destinations. A reload builds a sender for a destination it adds. It retires one it removes, sending that destination's buffered records to it, or to the DLQ when it can't. With disk spillover on, a destination whose address changed moves to the new one on the same spool once no request is using its old sender, and replays what that sender spooled. The Kafka producer and the DLQ need a restart to change.

Bearer tokens, TLS material and webhook caller secrets can be `provider:path[:key]` references -- `file:`, `vault:`, `aws:`, `env:` -- resolved through `scalo::secrets::resolve`. A reference field rejects a value that names no provider, and a webhook caller's secret is always a reference. Static bearer tokens and accepted header values can instead be literals in the file. The Kafka SASL credential is a plain value, set by `DFE_RECEIVER_KAFKA_SASL_PASSWORD` from the Kafka Secret. The receiver resolves no secret itself. It builds scalo's `secrets-vault` and `secrets-aws` features, so the OpenBao and AWS Secrets Manager providers are in the binary.

Every key and its default is in [config-schema.yaml](config-schema.yaml), and [config.example.yaml](../config.example.yaml) is an annotated starting point. An example for a TLS listener with bearer tokens and one source rule:

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

# A rule keys on the identifier the shipper already sends, for Elastic data_stream.dataset.
rules:
  rules:
    - id: cisco-ios
      match: { field: data_stream.dataset, op: equals, value: cisco_ios.log }
      set: { _source: { value: cisco_ios } }

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
    mechanism: scram_sha_512   # Apache Kafka, AutoMQ, MSK, Redpanda
    username: dfe-receiver
    # password stays out of the file: set DFE_RECEIVER_KAFKA_SASL_PASSWORD
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

## Deployment

The chart a release assembles from the deployment contract scales on CPU alone: 80% use, 1 to 10 replicas, from the contract's `keda` block. A deployment adds the ScalingPressure trigger with the library's `keda.extraTriggers` value. The receiver never scales on raw Kafka consumer lag.

In a DFE deployment, dfe-infra's values set the CPU target to 70% and add the ScalingPressure trigger (target 70, read through dfe-keda-shim) by default. The slim profile clears the trigger, so slim scales on CPU alone. The engine's [KEDA scaling page](https://github.com/hyperi-io/dfe-engine/blob/main/docs/deployment/keda-scaling.md) covers the triggers for every app.

Probes hit the metrics listener (9090, port name `metrics`), not the ingest port, so a saturated intake does not fail the pod's liveness check. `/readyz` answers 503 until every listener an enabled handler binds is serving, again once any of them stops, and under memory pressure. A destination outage does not fail it, and ingest sheds per request with 503 instead. The ingest port's `/readyz` gives the same answer.

Further reading:

- [scalo](https://github.com/hyperi-io/scalo-rs) - Shared data-plane runtime (crate: `scalo`)
- [Vector HTTP sink](https://vector.dev/docs/reference/configuration/sinks/http/)
- [Vector sink (gRPC)](https://vector.dev/docs/reference/configuration/sinks/vector/)
