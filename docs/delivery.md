# Delivery

Delivery is the last stage of [dfe-receiver](architecture.md): it sends each record to Kafka or a gRPC destination, and decides when the sender is answered.

| | |
|---|---|
| Repo | [hyperi-io/dfe-receiver](https://github.com/hyperi-io/dfe-receiver) |
| Image and chart | `ghcr.io/hyperi-io/dfe-receiver`, and the thin chart released beside it |
| Config reference | `acknowledgements` on each listener, `kafka`, `destinations`, `loader`, `buffer` and `routing.dlq` in [config-schema.yaml](config-schema.yaml) |
| Endpoints | None of its own. It answers through the listener that took the record |
| Metrics | `receiver_kafka_*`, `receiver_destination_*`, `transport_ack_*`, `pipeline_delivery_guarantee`, in [metrics.md](metrics.md#kafka) |

Which path a record takes is set by the listener that took it, not by the record.

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
    accTitle: L4 Detail -- Delivery
    accDescr: Routed records reach delivery. A record whose listener holds its answer goes straight to the sinks, and one answered at enqueue goes through the buffer and spool first. The sinks write to Kafka topics and gRPC destinations, and send records refused for good to the DLQ.

    route("<b>Route and stamp</b><br/><span>topic and destinations</span>")
    subgraph delivery["Delivery"]
        direction LR
        hold("<b>Answer hold</b><br/><span>held answers</span>")
        buffer("<b>Buffer and spool</b><br/><span>answers at enqueue</span>")
        sinks("<b>Sinks</b><br/><span>Kafka producer, gRPC senders</span>")
        hold --> sinks
        buffer --> sinks
    end
    kafka[("<b>Kafka topics</b>")]
    grpc("<b>gRPC destinations</b><br/><span>loader, transforms, archiver</span>")
    dlq[("<b>DLQ</b><br/><span>dead-letter queue</span>")]
    route --> delivery
    delivery --> kafka
    delivery --> grpc
    delivery -.-> dlq
  class route,hold,buffer,sinks part
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
  style delivery fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Delivery. The dotted arrow is the dead-letter path.* Up: [L3 Components -- Receiver](architecture.md). Down: [L4 Detail -- When a sender is answered](#the-answer-waits-for-the-destination).

Each listener's answer, and the protocol rule behind it, is in [internals.md](internals.md#what-a-sender-is-told).

## The answer waits for the destination

A listener with an answer in its protocol holds it until every destination confirmed the request's records: a Kafka delivery report, a gRPC destination's answer, or a dead-letter write the DLQ confirmed. That is `acknowledgements.enabled`, default on, per listener: `server`, `grpc`, `otlp`, `splunk_hec`, `lumberjack`, `fluent`, `webhook`, `prometheus_rw`. Syslog, GELF and flow have no answer to hold.

A held answer waits for the destination and bypasses the buffer, and an enqueue answer is given at once.

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
    accTitle: L4 Detail -- When a sender is answered
    accDescr: A routed record whose listener holds its answer goes straight to its sink. The sender is told it was accepted when the destination confirms within the hold, and told to retry when it does not. A record whose listener answers at enqueue goes into the buffer, and the sender is told it was accepted at once.
    subgraph ans["Answer"]
        direction TB
        held{"<b>Answer held?</b>"}
        subgraph HELD["Held"]
            direction LR
            send("<b>Send to sink</b>")
            confirmed{"<b>Confirmed<br/>in the hold?</b>"}
            ok(["<b>Accepted</b>"])
            retry(["<b>Told to retry</b>"])
            send --> confirmed
            confirmed -->|yes| ok
            confirmed -.->|no| retry
        end
        subgraph ENQ["Answered at enqueue"]
            direction LR
            buffer[("<b>Buffer</b>")]
            queued(["<b>Accepted<br/>at enqueue</b>"])
            buffer --> queued
        end
        held -->|yes| HELD
        HELD ~~~ ENQ
        held -->|no| ENQ
    end
  class held,confirmed decision
  class send,buffer,queued,ok part
  class retry error
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
  style ans fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style HELD fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style ENQ fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 2. L4 Detail -- When a sender is answered. The dotted arrow is the retry path.* Up: [L4 Detail -- Delivery](delivery.md).

A buffered record drains to the same sink later, and the sender has no copy of it by then. A record refused for good is answered as the next section says, whichever way it went.

A record not confirmed within the hold gets the protocol's retryable refusal (503 with `Retry-After`, `UNAVAILABLE`, no ack), and the sender keeps it. That means "not confirmed", not "not written": a Kafka message timeout can expire with a produce request in flight, so a resend can deliver a record twice. Duplicates, never loss.

The hold sits short of the listener's request timeout or a gRPC sender's `grpc-timeout` by a margin. OTLP HTTP exports have no deadline, so their hold stays inside the OTel exporters' default timeout. The held producer's message timeout and a gRPC destination's send deadline both settle inside the hold. A send still running when a shorter hold ends is cut off and answered retryable.

| Bound | Default | Source |
|---|---|---|
| The hold | 25 s | `MAX_HOLD`, `src/pipeline/acks.rs` |
| The hold for OTLP HTTP exports | 9 s, under the OTel exporters' 10 s | `otlp.http_max_hold_ms` |
| `message.timeout.ms` of the producer for held answers, unless the operator set one | 20 s | `HELD_MESSAGE_TIMEOUT`, `src/pipeline/acks.rs` |
| A gRPC destination's send deadline | 20 s | `NEXT_HOP_DEADLINE_MS`, and `loader.timeout_ms` for the loader |

A listener holds at most a quarter of the memory limit in unanswered bytes, 256 MiB when the guard reports no limit, and answers a request past it with the retry signal. Admission, that ceiling and the `transport_ack_*` metrics are scalo's `Tickets`, behind the pipeline's pressure brake and memory lease. The lease answers the retry signal to a request the brake let through whose bytes would take the memory guard's usage reading past the memory limit.

## What is final, and what is retried

No listener answers success for a record the pipeline did not take: a sender that reads a false success discards the record, and nothing downstream can recover it. A failure is final only when the record itself is at fault -- malformed, over a destination's size ceiling, or sent without valid credentials. Anything else tells the sender to retry, in its protocol's terms, and a batch stops at the first record not taken. The sender resends the whole request, so the records taken before it arrive twice: duplicates, never loss.

A delivery report that rejects the record itself for good -- the same codes that take a spooled record off the spool, below -- is answered exactly as librdkafka refusing it at enqueue is: accepted once the DLQ holds it, refused for good (400 on HTTP) where no DLQ can, and retryable where the DLQ rejects the write. Neither path is retried forever.

## Answers given at enqueue

With `acknowledgements.enabled: false` a listener answers once the record is queued. `receiver_kafka_sends_total` is what librdkafka queued, `receiver_kafka_delivered_total` what a broker acknowledged, and `receiver_kafka_delivery_failures_total{reason}` what no broker confirmed. `pipeline_delivery_guarantee{listener,guarantee,reason}` reports what each listener gives: its weakest reachable leg, so a gRPC destination declared `confirms_delivery: false` (the archiver's direct listener answers on receipt) makes every held listener `best_effort`.

Records answered at enqueue have no resend behind them, so beside a held listener they get a separate producer on librdkafka's default timeout. The sink queues again every one librdkafka gives up on with a message timeout, a topic the cluster does not serve or an authorisation failure (`receiver_kafka_resends_total`), and later records wait in the buffer in front. Any other broker refusal is final, and what is still queued at exit is lost.

The sink builds a `ThreadedProducer` instead of using scalo's `KafkaProducer`, because scalo hard-codes a `ProducerContext` with no injection point and a delivery report needs one. Its librdkafka config still comes from scalo's `producer_client_config`, over the `PRODUCER_HIGH_THROUGHPUT` profile, so the precedence is scalo's -- profile defaults, then the sizing surface, then `librdkafka_overrides`, which wins.

## Buffering is for answers given at enqueue

A held answer sends straight to the destination's sink: the sender still has the record, so nothing buffers or spools it. A buffer, and its spool, are built when some enabled listener answers at enqueue -- one with `acknowledgements.enabled: false`, or syslog, GELF or flow. With every listener holding, `buffer.spillover.enabled` opens no spool.

For answers given at enqueue, the default backend is an in-memory buffer behind a circuit breaker, with no disk involved. For the Kubernetes deployment that is the right trade -- OOMKill plus KEDA answers a backlog by adding pods, and a spool on an ephemeral volume buys little. Admission does not wait on a destination: the buffer queues while its sink is down and answers the retry signal once it is full, and a held record bound for a down destination is refused at once, unsent. Records bound for other destinations are taken meanwhile.

Disk spillover exists for deployments that want crash-resilient buffering or run outside Kubernetes. `buffer.spillover.enabled` swaps the backend for scalo's `TieredSink` with a disk spool, one directory per sink under `buffer.spillover.path`: `kafka/` for the bus and `grpc/<name>/` for each gRPC destination. Two sinks cannot share a spool. A spool that won't open at startup (its directory can't be created, or scalo refuses it) doesn't stop the receiver: that sink gets the in-memory buffer instead, logged at ERROR and shown as `receiver_spool_available` at `0`, until a restart opens the spool. `SinkBackend` (`src/buffer/mod.rs`) is the enum holding one or the other. Either way the shutdown drains into the sink and then flushes it.

## The spool keeps a record until it is confirmed

A spooled record stays on the spool until its destination confirms it: for the bus, its Kafka delivery report, and for a gRPC destination, the RPC's answer. A crash before then replays it from disk on the next start rather than losing it. A record taken on the hot path is answered once librdkafka queues it, as its sender is. The drain waits for one confirmation per record, because scalo's drain offers the spool one record at a time. A confirmation that does not come within the spool's `send_timeout_ms` leaves the record spooled and offers it again, so a slow broker can be sent a spooled record more than once: duplicates, never loss.

A delivery report that rejects the record itself for good (invalid record or timestamp, invalid topic, unknown partition) takes it off the spool through the DLQ like any other refused record, and any other failed report keeps it spooled. A topic or cluster authorisation failure is the receiver's ACL, not the record, so it keeps the record as well. A topic the cluster does not serve is not refused for good either, because a just-created or unpropagated topic reads the same as one that never exists. Its records stay spooled, and a record answered at enqueue is queued again, until the topic is served. A topic that never exists shows as a sampled ERROR log, `receiver_kafka_delivery_failures_total{reason="UnknownTopicOrPartition"}` and the sink reading unhealthy, with no give-up.

A broker's size refusal is not one of them: it fails the whole batch librdkafka sent, records that fit included, so each is offered again. librdkafka rejects a record over its `message.max.bytes` at enqueue, so a broker limit below the producer's shows as delivery failures until the two agree. A buffered record a destination rejects for good leaves once the DLQ confirms the write. Until the DLQ takes it, it stays at the front and holds up the records behind it: its sender was answered at enqueue, so dropping it would be loss.
