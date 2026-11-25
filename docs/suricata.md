# Suricata

Suricata writes its alerts, flows and protocol events to one JSON file, `eve.json`. Ship it to dfe-receiver with a file shipper and no receiver changes beyond switching a listener on.

| Shipper | dfe-receiver listener | What lands |
|---|---|---|
| Filebeat `suricata` module, `output.logstash` | `lumberjack`, port 5044 | Filebeat's parse of each event, which dfe-transform-elastic reads through its `suricata_eve` source |
| Fluent Bit `tail`, `forward` output | `fluent`, port 24224 | the raw eve record, with no Elastic pipeline mapping |

Start with Filebeat. Its module marks every event as `suricata.eve`, so a routing rule keys on a field Filebeat already sends:

```yaml
field:    event.dataset
operator: equals
value:    suricata.eve
```

Suricata has other `eve-log` outputs (`syslog`, `unix_dgram`, `unix_stream` and `redis`). None of them was run. `syslog` hands each event to a local syslog daemon, and the others need a reader on the far end, so a file and a shipper is the shortest path.

## Filebeat

```yaml
filebeat.modules:
  - module: suricata
    eve:
      enabled: true
      var.paths: [/var/log/suricata/eve.json]

output.logstash:
  hosts: ["dfe-receiver.example.com:5044"]
```

On the receiver:

```yaml
lumberjack:
  enabled: true
  accept_unauthenticated: true

rules:
  rules:
    - id: suricata-eve
      match: { field: event.dataset, op: equals, value: suricata.eve }
      set: { _source: { value: suricata_eve } }
```

The listener is off by default. An enabled one refuses to start unless `lumberjack.accept_unauthenticated: true` is set, or `lumberjack.tls.enabled: true` is set with `lumberjack.tls.client_auth: required` and a `lumberjack.tls.ca_file`. `accept_unauthenticated` takes every sender that reaches the port, so pair it with `server.ip_filter.cidrs`. The Filebeat side of TLS is `output.logstash.ssl.*`, which wasn't run for this page.

`suricata_eve` is dfe-transform-elastic's source name, so a source defined under it takes the Filebeat shape as it arrives. The run below stopped at the receiver and the loader, so the transform wasn't in the path.

What the run showed:

- The module sends `event.dataset: suricata.eve`, `fileset.name: eve` and `agent.type: filebeat`, and nothing under `data_stream`. A rule on `data_stream.dataset` never matches it. An Elastic Agent integration sends `data_stream.dataset` instead, and Agent wasn't run here.
- Every eve event type shares the one dataset, so one rule and one source cover alerts, flows, DNS and the rest.
- A rule's source needs its Kafka topic. Pointed at a broker that doesn't create topics, the receiver left each Lumberjack window unacknowledged because `suricata_eve_land` didn't exist, and Filebeat logged `EOF` and kept resending. Define the source before pointing Filebeat at it.
- Filebeat warns that it can't load ingest pipelines because the output isn't Elasticsearch. That is expected here, since dfe-transform-elastic runs them.

## Fluent Bit

Use this where Filebeat isn't an option. The record arrives as the JSON Suricata wrote, so nothing is mapped to the Elastic shape.

```yaml
parsers:
  - name: eve
    format: json
    time_key: timestamp
    time_format: "%Y-%m-%dT%H:%M:%S.%L%z"

pipeline:
  inputs:
    - name: tail
      path: /var/log/suricata/eve.json
      tag: suricata
      parser: eve
      read_from_head: true
  outputs:
    - name: forward
      match: "*"
      host: dfe-receiver.example.com
      port: 24224
      require_ack_response: true
```

```yaml
fluent:
  enabled: true
  accept_unauthenticated: true

rules:
  rules:
    - id: suricata-eve
      match: { field: event_type, op: exists }
      set: { _source: { value: suricata_eve } }
```

Every eve record has `event_type`, so that is the key. The `fluent` listener refuses to start on the same terms as `lumberjack` above.

The `time_key` lines matter. The listener sets `timestamp` from the time Fluent Bit attaches to the record, and replaces any `timestamp` the record carried. Eve's own `timestamp` is the event time, and without the parser the row holds the time Fluent Bit read the line. With it, `timestamp` is the eve time as an epoch float. The listener also adds `tag`.

The run added `exit_on_eof: true` to the input so Fluent Bit stopped after a finished file, which a live log doesn't want.

Both paths ran against a live stack. Suricata read a 78 packet capture of HTTP, DNS and WebSocket traffic, assembled from the Zeek source tree's test traces, with no rule set loaded. It wrote 27 events (15 `flow`, 9 `websocket`, 1 each of `dns`, `http` and `stats`) and no alerts. Each path sent them through a throwaway receiver with the rules above, and the rows were counted in ClickHouse.

| Path | Events in | Rows counted |
|---|---|---|
| Filebeat to `lumberjack` | 27 `suricata.eve` | 27 in `suricata_eve` |
| Fluent Bit to `fluent` | 27 | 27 in `suricata_eve` |
