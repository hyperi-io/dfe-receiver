# dfe-receiver architecture

Why the service is shaped the way it is, and the rules you cannot read off the
code. The configuration reference is [config.example.yaml](../config.example.yaml);
the component-by-component detail is in [DESIGN.md](DESIGN.md).

## The problem

Agents do not speak one protocol. A site already running Filebeat, an
OpenTelemetry collector, rsyslog, a Splunk forwarder and a Prometheus server has
five wire formats on the floor and no appetite for changing any of them. The DFE
pipeline behind wants one thing: JSON on a Kafka topic.

Three ways to close that gap. Put a translator in front of each agent, and the
deployment count multiplies. Teach dfe-loader the five protocols, and
network-facing framing and decode move into the middle of the pipeline, next to
the enrichment and schema logic. Or terminate every protocol once, at the edge,
and normalise before anything else sees the data.

dfe-receiver is the third option. Everything below follows from it.

## What that makes it

The one external door. dfe-receiver and dfe-ui are the only two components in
the stack that read untrusted input -- the loader, archiver and transforms have
no listener an outsider reaches, and dfe-fetcher dials out rather than being
dialled. So a dependency advisory here is not the same finding as the identical
advisory in dfe-loader. Grade reachability first, severity second. The exposure
model is dfe-infra's `docs/INGEST-EDGE.md`, which calls the receiver a
single-homed airlock: traffic from outside, produce to local Kafka, and Kafka
never faces the network.

It is not a transform stage. The receiver decides a payload is JSON, stamps
`_source` and `_timestamp_receiver`, picks a topic and hands over. Parsing,
enrichment and schema work are dfe-loader's. The one deliberate exception is
`_raw`: the receiver can retain the original wire bytes because for syslog and
friends that fidelity is gone by the time the loader sees the record.

## Shape

```mermaid
flowchart TB
    AG["Agents and collectors<br/>Beats, OTel, Splunk, syslog, Fluent, GELF, Prometheus, NetFlow"]
    PR["Products<br/>alert rules, SaaS hooks"]

    AG --> H["11 protocol handlers<br/>own listener, framing and decode<br/>each normalises to JSON"]
    PR --> W["Webhook intake<br/>per-caller auth, topic and filter"]

    H --> CORE
    W --> CORE

    subgraph CORE["Shared core path"]
        direction TB
        VAL["Validate<br/>sonic-rs, no full parse"]
        RT["Route<br/>source rules -> topic"]
        BUF["Buffer<br/>in-memory + circuit breaker"]
        VAL --> RT --> BUF
    end

    BUF --> K[("Kafka topics")]
    BUF --> G["gRPC destinations<br/>dfe-loader, transforms, archiver"]
    BUF -. invalid .-> D[("DLQ topic")]
```

Eleven handlers, one core path. A handler owns its socket, its framing and its
decode, and nothing else. The moment it holds JSON bytes it calls the same
pipeline every other handler calls, so validation, routing, backpressure and
metrics exist once rather than eleven times.

`ProtocolHandler` (`src/server/traits.rs`) is where the two halves meet:
`name()`, `bind_address()`, `start(shutdown)`, `is_healthy()`.
`Server::build_handlers` (`src/server/mod.rs:93`) collects the enabled handlers
and spawns each one. Adding a protocol is a directory under `src/server/` plus a
block in `build_handlers` -- it does not touch the core path.

HTTP is the exception to opt-in. It always runs, because `/livez` and `/readyz`
ride its listener, so exposing ingest exposes both probe paths.

The webhook intake is its own handler rather than a route on `/ingest`, for
reasons worth keeping. `/ingest` checks one server-wide credential in middleware
before the body is read, where a product is identified per caller and a signed
request needs the body to verify. A product's payload shape is its own, so
`routing.source_rules` cannot pick its topic. And an own listener lets a
deployment expose only that port to the product's egress.

## The hot path, and what it costs

PB/s-a-day throughput sets the rules for the core path. The payload stays
`bytes::Bytes` from socket to sink -- reference-counted, so a clone copies
nothing. Field extraction for routing borrows out of the payload (`Cow<str>`)
rather than building a DOM. Topic lookups use `FxHashMap`. No `format!()` on the
path.

The consequence a reader should know: validation proves the payload IS JSON and
that any required fields are present. It does not prove the payload is
well-shaped for downstream. That is deliberate -- a full parse at the door would
cost the throughput the design exists for.

## Delivery, and the acknowledgement that is not one

`POST /ingest` answers `202 Accepted` when the record is accepted into the
producer's queue, not when a broker has it. Those are two different events, and
since #110 they are counted separately: `receiver_kafka_sends_total` is what
librdkafka queued, `receiver_kafka_delivered_total` is what a broker
acknowledged, and `receiver_kafka_delivery_failures_total{reason}` is what no
broker took. A gap between the first two is the signal worth alerting on.

The sink builds its own `ThreadedProducer` instead of using scalo's
`KafkaProducer`, because scalo hard-codes a `ProducerContext` with no injection
point and a delivery report needs one. The cost is `producer_client_config()`
(`src/sink/kafka/mod.rs:141`), a hand copy of scalo's config assembly and its
precedence -- profile defaults, then `librdkafka_overrides`, then the sizing
surface, which wins. It goes wrong quietly if scalo reorders or adds a key.
scalo-rs#26 is the long-term fix.

## Buffering: memory by default, disk by choice

The default backend is an in-memory buffer behind a circuit breaker, with no
disk involved. For the Kubernetes deployment that is the right trade -- OOMKill
plus KEDA answers a backlog by adding pods, and a spool on an ephemeral volume
buys little.

Disk spillover exists for deployments that want crash-resilient buffering or run
outside Kubernetes. `buffer.spillover.enabled` swaps the backend for scalo's
`TieredSink` with a disk spool. `SinkBackend` (`src/buffer/mod.rs:30`) is the
enum holding one or the other, and the two branches differ in their shutdown
behaviour -- see invariant 1.

## Configuration

A cascade, scalo's: compiled defaults, then `/etc/dfe-receiver/config.yaml`,
then `./config.yaml`, then `~/.config/dfe-receiver/config.yaml`, then
`DFE_RECEIVER_*` environment variables, then CLI arguments, then runtime
overrides. Routing, validation and enrichment reload on SIGHUP.

Secrets are never literals in the config file. Every credential is a
`provider:path[:key]` reference -- `file:`, `vault:`, `env:` -- resolved through
`scalo::secrets::resolve`. The receiver has no resolver of its own, and that is
deliberate: an `aws:` reference is refused at startup naming the scalo feature it
would need, rather than pulling the AWS SDK in for a path nothing uses.

## Invariants

The rules a reader cannot infer, in rough order of how much damage getting them
wrong does.

1. **A 202 is not a delivery receipt.** The client has already been answered by
   the time a broker sees the record. Two open issues live in that window: #132,
   where the buffer's drain tasks and the metrics loop only start at SIGTERM, and
   #130, where spillover being on means the Kafka producer's queue is never
   flushed at shutdown. Neither is fixed. Read both before changing anything
   under `src/buffer/` or the startup order in `main.rs`.

2. **`chart/` and `Dockerfile` are generated, not written.** Both come from the
   deployment contract in `src/deployment.rs`, and two unit tests assert the
   committed files still match the generator. A hand edit is reverted the next
   time anything regenerates. Regenerate with `dfe-receiver --emit-helm chart`
   and `dfe-receiver --emit-dockerfile Dockerfile`. A scalo bump can change the
   generator, so a scalo bump without a regenerate fails this repo's own suite --
   that is the guard working, not a spurious failure.

3. **`chart/` is not the chart that deploys this service.** It is the standalone
   artefact. The suite deploys the receiver from dfe-infra's own chart, which
   pins the built image.

4. **dfe-engine hand-mirrors `Config::validate()`.** The copy lives at
   `dfe-engine/src/dfe_engine/services/plugins_builtin/receiver.py` and no script
   compares the two. They have already drifted both ways: the engine refuses a
   config where gRPC and HTTP share a `bind_address`, which this repo does not
   check anywhere, and this repo has since grown destination, webhook,
   rate-limit and spillover rules the engine copy has never heard of. A
   validation change here is only half the change.

5. **`auth.mode` defaults to `none`.** A deployment that has not configured auth
   accepts unauthenticated posts. Body size, request timeout, the IP filter and
   the rate limiter are the only other limits, and a default deploy is
   internet-facing with an empty `loadBalancerSourceRanges`, which means every
   address on earth. The receiver is built to sit behind edge protection, never
   directly on the internet.

6. **`_raw` holds the payload before redaction.** Anything stripped from the
   parsed fields downstream is still in `_raw`, indexed for full-text search.
   Capture is off by default for that reason as much as for the doubled bytes.

7. **`Config::validate()` only runs on the cascade path.** A test that builds a
   `Config` struct directly never reaches it, which is how the brokerless test
   configurations work. Do not read a passing test as proof that a YAML file
   would be accepted.
