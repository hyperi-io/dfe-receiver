# dfe-receiver architecture

dfe-receiver is DFE's front door for data. Agents and products send to it in whatever protocol they already speak, and it hands JSON on to Kafka or straight to the loader.

```mermaid
---
config:
  theme: base
  look: neo
  fontFamily: Inter, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif
  flowchart:
    curve: linear
    nodeSpacing: 40
    rankSpacing: 64
    padding: 18
    wrappingWidth: 280
    htmlLabels: true
  elk:
    preset: modelOrder
  themeVariables:
    fontSize: 15px
    lineColor: "#64748B"
    primaryTextColor: "#0F172A"
    clusterBorder: "#0072B2"
    edgeLabelBackground: "#FFFFFF"
  themeCSS: >-
    p{margin:0}
    .node[data-look] rect,.node[data-look] path,.node[data-look] polygon,.node[data-look] .outer-path{filter:drop-shadow(0 1px 2px rgba(15,23,42,.10)) drop-shadow(0 6px 14px rgba(15,23,42,.10))}
    .node rect{rx:10px;ry:10px}
    .cluster rect{rx:16px;ry:16px}
    .node .label div{min-width:150px}
    .node.decision .label div{min-width:0}
    .cluster[data-look] rect{filter:none}
    .nodeLabel{line-height:1.35}
    .nodeLabel b{font-weight:600;font-size:15px;letter-spacing:-0.01em}
    .nodeLabel span{font-size:12.5px;font-weight:400;color:#334155}
    .cluster-label span.nodeLabel{display:inline-block;background:#FFFFFF;color:#0B2A3F;border:1.5px solid #0072B2;border-radius:999px;padding:2px 12px;font-size:11.5px;font-weight:700;letter-spacing:.09em;text-transform:uppercase}
    .flowchart-link{stroke-width:1.75px}
    .labelBkg{background:transparent!important}
    span.edgeLabel{display:inline-block;background:#FFFFFF!important;color:#1E293B;border:1px solid #94A3B8;border-radius:999px;padding:2px 10px;font-size:12px;font-weight:500}
    span.edgeLabel p{background:transparent!important}
    .label-icon{width:20px;height:20px;vertical-align:-4px;margin-right:6px}
    .ingest .label-icon{color:#0E8A7D}
    .xform .label-icon{color:#7B4FB0}
    .load .label-icon{color:#0072B2}
    .bus .label-icon{color:#A8517F}
    .store .label-icon{color:#00785A}
    .ui .label-icon{color:#D9480F}
    .ext .label-icon{color:#64748B}
---
flowchart TB
    accTitle: L3 Components -- Receiver
    accDescr: Agents stream events and products push webhooks. Inside dfe-receiver the protocol handlers and the webhook intake take them in, then validation, routing and stamping, and delivery handle each record. Delivery writes to Kafka topics or gRPC destinations, and sends refused records to the DLQ.

    agents("<b>Agents</b><br/><span>Filebeat, OTel, rsyslog</span>")
    products("<b>Products</b><br/><span>push webhooks</span>")
    subgraph receiver["dfe-receiver"]
        direction TB
        subgraph TAKE["Take in"]
            direction LR
            handlers("<b>Protocol handlers</b><br/><span>framing into JSON</span>")
            webhook("<b>Webhook intake</b><br/><span>per-caller auth and filter</span>")
            handlers ~~~ webhook
        end
        subgraph PROC["Every record"]
            direction LR
            validate("<b>Validate</b><br/><span>shape and required fields</span>")
            route("<b>Route and stamp</b><br/><span>topic, _source, _org_id</span>")
            delivery("<b>Delivery</b><br/><span>sinks, buffer, answers</span>")
            validate --> route --> delivery
        end
        TAKE --> PROC
    end
    kafka[("<b>Kafka topics</b>")]
    grpc("<b>gRPC destinations</b><br/><span>loader, transforms, archiver</span>")
    dlq[("<b>DLQ</b><br/><span>dead-letter queue</span>")]
    agents -->|events| receiver
    products -->|webhooks| receiver
    receiver -->|records| kafka
    receiver -->|records| grpc
    receiver -.->|dead letters| dlq
  class agents,products ext
  class handlers,webhook,validate,route,delivery part
  class kafka,dlq bus
  class grpc load
  classDef ext fill:#F4F6F8,stroke:#94A3B8,stroke-width:1.5px,stroke-dasharray:5 4,color:#0F172A
  classDef ingest fill:#FFFFFF,stroke:#0E8A7D,stroke-width:3px,color:#0F172A
  classDef xform fill:#FFFFFF,stroke:#7B4FB0,stroke-width:3px,color:#0F172A
  classDef load fill:#FFFFFF,stroke:#0072B2,stroke-width:3px,color:#0F172A
  classDef bus fill:#FFFFFF,stroke:#A8517F,stroke-width:3px,color:#0F172A
  classDef store fill:#FFFFFF,stroke:#00785A,stroke-width:3px,color:#0F172A
  classDef ui fill:#FFFFFF,stroke:#D9480F,stroke-width:3px,color:#0F172A
  classDef decision fill:#FFFFFF,stroke:#9A6700,stroke-width:3px,color:#0F172A
  classDef error fill:#FFFFFF,stroke:#C2410C,stroke-width:3px,color:#0F172A
  classDef control fill:#FFFFFF,stroke:#334155,stroke-width:3px,color:#0F172A
  classDef part fill:#FFFFFF,stroke:#94A3B8,stroke-width:1.5px,color:#0F172A
  style receiver fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style TAKE fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style PROC fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L3 Components -- Receiver. Dashed boxes are outside DFE, and the dotted arrow is the dead-letter path.* Up: [L2 Services -- DFE deployment](https://github.com/hyperi-io/dfe-infra/blob/main/docs/architecture.md). Down: [L4 Detail -- Protocol handlers](protocol-handlers.md), [L4 Detail -- Webhook intake](webhook-setup.md), [L4 Detail -- Delivery](delivery.md).

## How data flows

1. A protocol handler owns its socket, its framing and its decoding, and nothing else. Once it has JSON bytes it calls the same pipeline every other handler calls, so validation, routing, backpressure and metrics exist once, not once per protocol.
2. The webhook intake takes `POST /webhook/{caller}` from a product. It checks that caller's own secret, splits the body into records and runs the caller's [CEL](https://github.com/cel-expr/cel-spec/blob/master/doc/langdef.md) filter.
3. Validate checks each record is a JSON object no deeper than 64 levels, with the fields `validation.required_fields` names. A record missing one goes to the DLQ or is refused, per `validation.dlq_on_invalid`.
4. Route and stamp runs the rule set, holds `_org_id` to the orgs the sender is bound to, and picks the topic and destinations. It writes `_source`, `_org_id` and `_timestamp_received` back into the record.
5. Delivery hands the record to the Kafka producer, or to one gRPC sender per destination, and answers the sender. A record a destination refuses for good goes to the DLQ: by default the `dfe_receiver_dlq` topic, and a local file when Kafka doesn't take the entry.

## Components

| Part | Role | Page |
|---|---|---|
| Protocol handlers | Each protocol's listeners, framing, decode and auth, handing JSON to the pipeline | [protocol-handlers.md](protocol-handlers.md) |
| Webhook intake | Per-caller auth, body split and CEL filter, then the caller's topic | [webhook-setup.md](webhook-setup.md) |
| Validate | Nesting depth, JSON object, required fields, read lazily with sonic-rs | -- |
| Route and stamp | The rule set, the org clamp, the topic and destinations, and the stamps | -- |
| Delivery | The Kafka producer and the gRPC senders, the buffer and spool, and when a sender is answered | [delivery.md](delivery.md) |

Validate and Route and stamp have no page of their own. [internals.md](internals.md) has what the receiver rejects and how a record is routed.

## External dependencies

| Dependency | When it's needed | What can stand in |
|---|---|---|
| Kafka | The default transport, and the default DLQ | Apache Kafka, AutoMQ, AWS MSK and Redpanda on SASL-SCRAM-SHA-512. Confluent Cloud on SASL/PLAIN or OAUTHBEARER |
| gRPC destinations | `transport: grpc`, or a `destinations.*` entry | Any scalo Push listener: dfe-loader, the transforms, the archiver |
| Secrets store | A credential or certificate given as a `provider:path[:key]` reference | `file:`, `vault:` (OpenBao), `aws:` (AWS Secrets Manager), `env:` |
| A volume | `buffer.spillover.enabled` alone | Any writable path |

The receiver resolves no secret itself. scalo does, through `scalo::secrets::resolve`, and the binary builds its OpenBao and AWS Secrets Manager providers.

## Deployment variants

Records land on Kafka by default. `transport: grpc` (or `DFE_RECEIVER_TRANSPORT=grpc`), the same key dfe-loader and dfe-archiver read, sends them to the loader's Push listener with no broker in between. Under it an unset `destinations.default` becomes `loader` and an unset `loader.transport` becomes `grpc`, and a config that points either at the bus refuses to start. dfe-infra's slim and mesh profiles set both keys themselves.

The public door differs by how the receiver is installed.

- In a DFE deployment the public door is `exposure.groups.ingest` (`dfe-infra/docs/ingest-edge.md`). A load balancer renders once its allow list names someone, so an empty list means no public door at all. `publicService` stays off, and is refused if switched on.
- The standalone thin chart defaults to an in-cluster Service alone. Setting `publicService.enabled` adds a load balancer, and an empty `publicService.loadBalancerSourceRanges` leaves it open to every address.

The released chart scales on CPU alone: 80% use, 1 to 10 replicas. In a DFE deployment, dfe-infra's values set the CPU target to 70% and add the ScalingPressure trigger, and the slim profile clears that trigger again. The receiver never scales on raw Kafka consumer lag.

## Design considerations

**Security.** Only a few DFE components take input from outside: the receiver, dfe-ui, dfe-hyperdx (behind the engine's token) and culvert where it's deployed. So a dependency advisory here isn't the same finding as the same advisory in dfe-loader. Grade reachability first and severity second, as `dfe-infra/docs/threat-model.md` sets out.

`auth.mode` defaults to `none`. A receiver nobody has configured auth on accepts unauthenticated posts, limited by nothing more than body size, request timeout, the concurrency and connection caps, the IP filter and the rate limiter. So whatever sits in front of it on the internet has to limit who reaches the ports. [hardening.md](hardening.md) has the fronting options.

**Reliability.** What a sender is told depends on whether its protocol has an answer to give.

| Listeners | The sender is answered | A record that isn't confirmed |
|---|---|---|
| `server`, `grpc`, `otlp`, `splunk_hec`, `lumberjack`, `fluent`, `webhook`, `prometheus_rw`, with `acknowledgements.enabled` (the default) | Once every destination confirms the request's records, within the hold | Gets the protocol's retry signal, and the sender keeps it and sends it again |
| syslog, GELF and flow, which have no answer to hold, and any listener with acknowledgements off | As soon as the record is queued | Is kept by the receiver, in its buffer and, with spillover on, its spool |

A retry can deliver a record twice, because a destination can take a record and then miss the hold. Duplicates, never loss. An answer given at enqueue isn't a delivery receipt: a kill loses whatever the producer and the buffer held.

**Operations.** Routing, validation, enrichment and the gRPC destinations reload on SIGHUP and when the config file changes. The Kafka producer and the DLQ need a restart to change. A bearer token rotates without a restart, refreshed from its secret source. [internals.md](internals.md#configuration) has the config cascade and what a reload does to each destination.

**Performance.** The receiver is designed for petabytes a day, so the core path copies and parses as little as it can. A payload stays `bytes::Bytes` from socket to sink, and routing reads its fields straight out of the payload instead of building a parsed tree. [performance.md](performance.md) has the tuning.

## Why it's built this way

**One front door for every protocol.** A site running Filebeat, an OpenTelemetry collector, rsyslog, a Splunk forwarder and a Prometheus server has five wire formats on the floor, and nobody wants to change any of them. A translator in front of each agent multiplies what there is to deploy. Teaching dfe-loader all five puts network-facing decoding in the middle of the pipeline, next to the enrichment and schema logic. So every protocol is terminated once, at the edge, and turned into JSON before anything else sees the data.

**It stops at the transport framing.** Parsing the message body is the dfe-transform-* apps' job, and fitting a record to its table is dfe-loader's.

**Validation checks the shape, not the values.** It checks the payload is JSON, that it's an object, and that the required fields are there. It doesn't check the fields hold what downstream expects. A full parse at the door would cost the throughput the receiver is built for.

**Webhooks get their own routes.** `/ingest` checks one server-wide credential before reading the body, but a product is identified per caller, and a signed request needs the body to verify it. Each product picks its own payload format, so the `rules` rows can't pick its topic. And with `webhook.bind_address` set, a deployment can expose just that port to the product.

**Raw capture is off by default.** With it on, `_raw` keeps a copy of the original. For syslog and similar protocols it's the only copy: the wire bytes are gone by the time the loader sees the record. `_raw` is taken before redaction, so anything stripped from the parsed fields downstream is still there, indexed for full-text search. That's as much why capture is off as the doubled bytes.

## Diagram map

- Figure 1. L3 Components -- Receiver (this page)
  - [L4 Detail -- Protocol handlers](protocol-handlers.md)
  - [L4 Detail -- Webhook intake](webhook-setup.md)
  - [L4 Detail -- Delivery](delivery.md)
  - [L4 Detail -- HTTP request order](hardening.md), and the fronting diagrams on the same page
  - [L4 Detail -- Sidecar transports](sidecar-transports.md)

## What's next

- [config-schema.yaml](config-schema.yaml): every setting and its default
- [internals.md](internals.md): what the receiver rejects, routing, auth and Kafka in detail
- [metrics.md](metrics.md): every series and alert
- [hardening.md](hardening.md): fronting the receiver on the internet
- [CONTRIBUTING.md](../CONTRIBUTING.md#context): where the code lives and what tends to bite
