# Protocol handlers

The protocol handlers are the listeners of [dfe-receiver](architecture.md): each one takes a sender's connection and turns what it sent into JSON for the shared pipeline.

| | |
|---|---|
| Repo | [hyperi-io/dfe-receiver](https://github.com/hyperi-io/dfe-receiver) |
| Image and chart | `ghcr.io/hyperi-io/dfe-receiver`, and the thin chart released beside it |
| Config reference | `server`, `grpc`, `otlp`, `lumberjack`, `splunk_hec`, `syslog`, `fluent`, `gelf`, `prometheus_rw` and `flow` in [config-schema.yaml](config-schema.yaml) |
| Endpoints | One port per protocol, listed in the [README](../README.md#what-it-does). `/livez` and `/readyz` on the ingest and metrics ports |
| Metrics | `receiver_requests_total`, `receiver_parse_failures_total`, `receiver_records_dropped_total`, in [metrics.md](metrics.md#requests-and-records) |

Every handler runs the same three steps, whatever its protocol.

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
    nodeBorder: "#334155"
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
    .icon-shape path[stroke-width="0"][fill="none"]{fill:#FFFFFF;stroke:#FFFFFF;stroke-width:12px;stroke-linejoin:round}
    .icon-shape p{background:transparent!important;color:#0F172A}
    .ingest .label-icon{color:#0E8A7D}
    .xform .label-icon{color:#7B4FB0}
    .load .label-icon{color:#0072B2}
    .bus .label-icon{color:#A8517F}
    .store .label-icon{color:#00785A}
    .ui .label-icon{color:#D9480F}
    .ext .label-icon{color:#64748B}
---
flowchart TB
    accTitle: L4 Detail -- Protocol handlers
    accDescr: Senders connect to a handler's listener. The listener applies its limits, auth checks the credential where the protocol has one, and decode turns the framing into JSON. The JSON goes on to validation.

    senders("<b>Senders</b><br/><span>agents, collectors, forwarders</span>")
    subgraph handlers["Protocol handlers"]
        direction LR
        listen("<b>Listener</b><br/><span>socket, TLS, limits</span>")
        auth("<b>Auth</b><br/><span>bearer, header, mTLS</span>")
        decode("<b>Decode</b><br/><span>framing into JSON</span>")
        listen --> auth --> decode
    end
    validate("<b>Validate</b><br/><span>the shared pipeline</span>")
    senders --> handlers
    handlers -->|JSON| validate
  class senders ext
  class listen,auth,decode,validate part
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
  style handlers fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Protocol handlers. Senders is outside DFE.* Up: [L3 Components -- Receiver](architecture.md). Down: none.

## How it works

1. Every protocol is switched on in config, except HTTP, which always runs. HTTP serves `/livez` and `/readyz` beside `/ingest`, so exposing ingest exposes both probes. The chart's probes use the same paths on the metrics port.
2. The listener applies its limits before anything else: body size, request timeout, the concurrency and connection caps, the IP filter and the rate limiter. [hardening.md](hardening.md#which-listener-gets-which-control) says which listener gets which.
3. Auth checks the credential on the listeners whose protocol carries one. Syslog, GELF and flow have none. [internals.md](internals.md#authentication) has the modes and how a token rotates.
4. Decode parses the transport framing into JSON and leaves the message body alone. [protocols.md](protocols.md) has what each protocol gives.

## Interfaces

Each handler takes its protocol on its own port and hands JSON bytes to the pipeline. Nothing else crosses: a new protocol adds a handler and a port, never a change to the core path.

`grpc.enabled` defaults to `false`, but a DFE deployment turns it on. dfe-infra's values put the listener on `0.0.0.0:6000`, where it takes Vector's gRPC sink (`PushEvents`). It speaks Vector's protocol, not scalo's Push.

## Scaling and failure

`/readyz` answers 503 until every enabled listener is serving, and again once any of them stops. When a listener can't bind, or stops before shutdown, its handler stops all its listeners. The process logs `Protocol handler failed` and stays up, the ingest port's `/readyz` 503 body names each failed handler, and the error stays in the log. An enabled handler that can't be built fails the same way.

A handler that can't hand a record on, because of memory pressure or a full queue, gives its sender the protocol's retry signal. The table in [internals.md](internals.md#what-a-sender-is-told) has each protocol's answer.

## Key metrics

- `receiver_requests_total` and `receiver_request_duration_seconds`, by `transport`: traffic and answer time per protocol.
- `receiver_parse_failures_total`: bodies a decoder couldn't read.
- `receiver_auth_failures_total`: refused credentials.
- `receiver_records_dropped_total`: records gone with no way to tell the sender, on the protocols with no answer.

## Related

- [protocols.md](protocols.md): what each protocol parses, raw capture and flow
- [hardening.md](hardening.md): fronting the listeners on the internet
- [sidecar-transports.md](sidecar-transports.md): a collector in front for protocols the receiver doesn't take
