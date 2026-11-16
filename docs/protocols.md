# Protocol parsing

What the receiver parses out of each protocol, and the records it builds from them. How a record is then routed and answered is in [internals.md](internals.md).

## Where parsing happens

The receiver parses the transport layer of each protocol and emits JSON: for syslog the header (PRI, timestamp, host, app name), for Beats and HEC the envelope, for OTLP the framing. A syslog body goes in `message` with the parsed header fields beside it. OTLP records carry signal fields, a log's body as `body` (`Body` in `hyperdx` mode), and the JSON transports keep the sender's fields.

Parsing a message body (grok, dissect, a pipeline) is the transforms' job. They read `message` plus the header fields.

Raw capture (`raw_capture.enabled`, default `false`) keeps the source's raw form in `_raw`, for syslog, HEC `/raw` and flow. It roughly doubles each record it applies to, so it is an operator fallback for a single source and never the normal input to a transform. A record parsed wrongly here is fixed in the receiver, not by turning capture on downstream. See [Raw payload retention](#raw-payload-retention) and the fleet rule in [design-principles.md](https://github.com/hyperi-io/dfe-infra/blob/main/docs/design-principles.md).

## Syslog parsing

An RFC 3164 token is the app name only when it reads `name:` or `name[pid]:`, the name drawn from ASCII alphanumerics and `-_./`. Anything else stays at the front of `message`, so a tagless PAN-OS line keeps its whole CSV body and FortiGate keeps its leading `date=`. A Cisco stamp's `.407 EST:` tail goes into the timestamp's fraction, with the zone abbreviation dropped, and its `*` or `.` clock marker and its sequence number (`123456:`) are read past. Fractional seconds are kept on any 3164 stamp, host or no host. A stamp with no zone of its own is read in `syslog.timezone` (default `UTC`), never the receiver's local zone. A PRI with no parseable header after it still gives the facility and severity, and stays out of `message`.

RFC 5424 structured data that does not parse, like `[x@1 k=v]`, stays in `message` as sent, and the elements that did parse still land in `structured_data`. A byte order mark at the start of the message and NUL bytes at its end are dropped.

On TCP and TLS a frame is octet-counted (RFC 6587) only when it opens with a count, one space and the `<` of a PRI. Anything else is an LF-terminated line, so a line opening with digits, like `2026 Oct 11 ...`, is never read as a count. A message over `syslog.max_message_size` is skipped and counted as dropped (reason `oversize`), and the connection carries on: an octet-counted one by its count, a line up to its LF. A count over `syslog.max_frame_size` (default 256 KiB) is not trusted as a count, and that frame is read as a line.

Syslog UDP reads every datagram ready on a wake, up to `syslog.udp_batch_size` (default 64), in batched receive calls, and hands them to the pipeline as one batch. Its socket asks for a `syslog.udp_recv_buffer_size` receive buffer (default 8 MiB), and logs a warning when `net.core.rmem_max` grants less. Datagrams the kernel drops for a full buffer are read from `/proc/net/udp` every 10 seconds and counted as dropped, reason `kernel`. Syslog TCP settles every frame one read brought in with a single hold on the pipeline.

## Vector and OTLP metric records

The gRPC listener speaks Vector's sink protocol. A Vector metric becomes a record with `_vector_type: "metric"`, its name, namespace, kind, tags (`tags_v2` where sent, a multi-valued tag as an array) and `interval_ms`, and its value under its kind: `counter`, `gauge`, `set`, `distribution`, `aggregated_histogram`, `aggregated_summary` or `sketch`. One with no value is refused with `INVALID_ARGUMENT`.

Each OTLP data point is one record. An exponential histogram carries its buckets either side of zero as `positive_offset` and `positive_bucket_counts` and the `negative_` pair (`PositiveOffset`, `PositiveBucketCounts` and so on in `hyperdx` mode), with `zero_threshold` in `generic`. A data point's exemplars ride on its record: `exemplars` in `generic`, the `Exemplars.*` arrays in `hyperdx`, trace and span ids as hex. Remote write builds its records the same way, and both modes hold every OTLP exemplar and bucket.

## Prometheus Remote Write records

`prometheus_rw.mode: native` (default) writes each sample, each exemplar (`_type: "exemplar"`) and each native histogram (`_type: "histogram"`) as its own record, with the series labels as top-level fields. A histogram record carries `count`, `sum`, `schema`, `zero_threshold`, `zero_count`, `reset_hint`, its buckets as `positive_offset` and `positive_bucket_counts` (and the `negative_` pair), the Prometheus index of the first bucket and every count from there with gaps filled, and `custom_values` for a custom-bucket histogram.

`otel` and `hyperdx` write the OTLP converter's metric types: an exponential schema as `exponential_histogram` (the OTel offset is the Prometheus index less one), a custom-bucket histogram as `histogram` with its bounds as `explicit_bounds`. A series' exemplars ride on its last record: `exemplars` in `otel`, the `Exemplars.*` arrays in `hyperdx`, with the `trace_id` and `span_id` labels lifted out.

What those types cannot hold is counted on `receiver_prometheus_rw_unrecorded_total{kind}`, never dropped silently: `exemplar` for exemplars sent with no sample of their series (Prometheus sends them in a series of their own), `float_histogram`, whose counts OTel keeps as integers, and `unknown_schema`. `mode: native` records all three. A histogram whose spans name more than 4096 buckets, a negative gap, or more buckets than it has counts is refused as malformed with a 400.

## Raw payload retention

`_raw`, a dfe-schemas common-header field, holds raw data the source provided: a syslog line, a HEC `/raw` line, a flow record. The parsed event is unchanged. A JSON payload lives in `_json` alone, so the JSON transports never write `_raw`. dfe-loader never overwrites a `_raw` the record carries. Without one, `_raw` takes the first field the loader's `metadata.raw_source_fields` names (default none) while `metadata.capture_raw` is on (the default), or what an `@renamed` or `@source` on the `_raw` column names.

The reason to populate it here is fidelity the loader cannot recover. For syslog the loader sees `message`, the body after the PRI and header, and the exact wire spacing is gone. A line whose header the parser does not recognise becomes `message = <whole line>`, which reads the same as a clean parse. Only `_raw` separates them.

Capture is off by default for two reasons. It roughly doubles produced bytes for text protocols, on top of a full-text index downstream. And `_raw` is the payload *before* parsing, so redaction, masking or field-dropping applied downstream to the parsed fields does not reach it. Enable capture for a source only when retaining its raw payload at that sensitivity is acceptable, and redact `_raw` wherever the parsed fields are redacted.

```yaml
raw_capture:              # common default for syslog, HEC /raw and flow
  enabled: false
  max_bytes: 65536        # 0 = unlimited
  on_oversize: truncate   # truncate | omit
syslog:
  raw_capture:
    enabled: true         # per-transport override, inherits the rest
```

Every field is optional at both levels: an unset field inherits, so "off" and "not configured" stay distinguishable. Flat env vars work at both levels (`DFE_RECEIVER_RAW_CAPTURE_ENABLED`, `DFE_RECEIVER_SYSLOG_RAW_CAPTURE_ENABLED`). `_raw_truncated: true` marks a value cut to `max_bytes`, and `_raw_lossy: true` a HEC or flow value whose invalid UTF-8 was replaced with U+FFFD.

| Transport | What `_raw` holds |
|-----------|-------------------|
| `syslog` | the received line, PRI and header included, its invalid UTF-8 already replaced and counted on `receiver_lossy_decodes_total`, so it never carries `_raw_lossy` |
| `splunk_hec` | `/raw`: the original line bytes |
| `flow` | the decoder's verbatim records -- a JSON array in `canonical`, the single record in `exploded` |

The other listeners do not offer the knob: their payload is JSON or passes through untouched, which the loader keeps whole in `_json`. `_raw` exists only in the `timeseries` common-header profile. A source using `minimal` or `passthrough` pays for capture at the receiver and has the field dropped at the loader, and the receiver cannot see the destination profile to warn.

## Flow (NetFlow + sFlow) is experimental

Status: experimental. `flow.experimental` defaults to `true`, and while it is the handler logs a startup WARN and sets `handler_experimental{handler="flow"} 1`. The generated capability catalogue lists flow as stable, and [#254](https://github.com/hyperi-io/dfe-receiver/issues/254) tracks settling the two.

Native UDP ingestion for NetFlow v5/v9, IPFIX and sFlow v5, with autosense dispatch on each of the default ports (2055, 4739, 6343). `canonical` output (default) is one JSON envelope per UDP datagram with a `flows: [...]` array, and `exploded` one envelope per flow record, which multiplies the events. A record's kind is `flow`, `counter` (an sFlow counter sample), `security_event` (an NSEL firewall event, Cisco ASA firewallEvent / NF_F_FW_EVENT) or `nat_translation` (a CGNAT NAT44 natEvent). One listener per port takes every flow protocol by default, and `flow.split:` runs separate NetFlow-only and sFlow-only listeners instead.

Each observation domain of each v9 and IPFIX exporter keeps its own templates, since a template id is unique only within its domain. It holds at most `flow.netflow.template_cache.max_per_exporter` (default 1000) of them, v9 and IPFIX together. A template past the cap evicts that exporter's oldest, counted on `flow_template_evicted_total`, and the exporter sees template misses until it resends the template. At most `max_exporters` exporter and domain pairs are kept.

A flow record no rules row routes goes to its listener's topic: `flow.netflow.topic` and `flow.sflow.topic`, or the `topic` of each `flow.split` side. A topic left empty is the listener's source plus `routing.topic_suffix`, so `netflow_land` and `sflow_land` by default. A row that assigns the record a `_source` routes it as any other record. Every `flow:` key is in [config-schema.yaml](config-schema.yaml).

What is not built yet:

- NetFlow v7, a Cisco variant for early-2000s Catalyst switches, is refused as a parse error, counted on `transport_decode_err_total{transport="netflow", reason="parse_err"}`.
- Template misses count as parse errors, so `transport_decode_err_total{reason="template_miss"}` stays 0.
- A v9 or IPFIX sysUpTime field gives `t_flow_start` and `t_flow_end` as relative `sysup:<ms>` strings, because the anchor is not resolved. An absolute millisecond field gives RFC 3339.
- The Linux listener drains up to 256 datagrams per readiness wake with `try_recv_from`, one syscall each, with no `recvmmsg` batching.
