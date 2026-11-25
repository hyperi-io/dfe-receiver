# Zeek

Zeek writes one log file per protocol. Ship them to dfe-receiver with a file shipper and no receiver changes beyond switching a listener on.

| Shipper | dfe-receiver listener | What lands |
|---|---|---|
| Filebeat `zeek` module, `output.logstash` | `lumberjack`, port 5044 | Filebeat's parse of each log, which dfe-transform-elastic reads through its `zeek_*` sources |
| Fluent Bit `tail`, `forward` output | `fluent`, port 24224 | the raw JSON record, with no Elastic pipeline mapping |

Start with Filebeat. Its module marks every event with the log it came from, so a routing rule keys on a field Filebeat already sends:

```yaml
field:    event.dataset
operator: equals
value:    zeek.connection
```

Both paths read JSON, so Zeek has to write it. Add `@load policy/tuning/json-logs` to `local.zeek`, or run `zeek` with `LogAscii::use_json=T`.

## Filebeat

```yaml
filebeat.modules:
  - module: zeek
    connection:
      enabled: true
      var.paths: [/opt/zeek/logs/current/conn.log]
    dns:
      enabled: true
      var.paths: [/opt/zeek/logs/current/dns.log]
    http:
      enabled: true
      var.paths: [/opt/zeek/logs/current/http.log]

output.logstash:
  hosts: ["dfe-receiver.example.com:5044"]
```

Only the filesets you list run, so add `dhcp`, `ssl`, `files` and the rest the same way. On the receiver:

```yaml
lumberjack:
  enabled: true
  accept_unauthenticated: true

rules:
  rules:
    - id: zeek-connection
      match: { field: event.dataset, op: equals, value: zeek.connection }
      set: { _source: { value: zeek_connection } }
    - id: zeek-dns
      match: { field: event.dataset, op: equals, value: zeek.dns }
      set: { _source: { value: zeek_dns } }
```

The listener is off by default. An enabled one refuses to start unless `lumberjack.accept_unauthenticated: true` is set, or `lumberjack.tls.enabled: true` is set with `lumberjack.tls.client_auth: required` and a `lumberjack.tls.ca_file`. `accept_unauthenticated` takes every sender that reaches the port, so pair it with `server.ip_filter.cidrs`. The Filebeat side of TLS is `output.logstash.ssl.*`, which wasn't run for this page.

The source names match dfe-transform-elastic's `zeek_connection` and `zeek_dns`, so a source defined under one of them takes the Filebeat shape as it arrives. The run below stopped at the receiver and the loader, so the transform wasn't in the path.

What the run showed:

- The module sends `event.dataset` (`zeek.connection`, `zeek.dns`, `zeek.http`), `fileset.name` and `agent.type: filebeat`, and nothing under `data_stream`. A rule on `data_stream.dataset` never matches it. An Elastic Agent integration sends `data_stream.dataset` instead, and Agent wasn't run here.
- A log with no rule lands in `routing.default_source`. The `http` events above have none.
- A rule's source needs its Kafka topic. Pointed at a broker that doesn't create topics, the receiver left each Lumberjack window unacknowledged because `zeek_connection_land` didn't exist, and Filebeat logged `EOF` and kept resending. Define the source before pointing Filebeat at it.
- Filebeat warns that it can't load ingest pipelines because the output isn't Elasticsearch. That is expected here, since dfe-transform-elastic runs them.

## Fluent Bit

Use this where Filebeat isn't an option. The record arrives as the JSON Zeek wrote, so nothing is mapped to the Elastic shape.

```yaml
parsers:
  - name: json
    format: json

pipeline:
  inputs:
    - name: tail
      path: /opt/zeek/logs/current/conn.log,/opt/zeek/logs/current/dns.log
      path_key: log_path
      tag: zeek
      parser: json
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
    - id: zeek-connection
      match: { field: log_path, op: ends_with, value: /conn.log }
      set: { _source: { value: zeek_connection } }
    - id: zeek-dns
      match: { field: log_path, op: ends_with, value: /dns.log }
      set: { _source: { value: zeek_dns } }
```

A Zeek JSON record has no field naming its log. `path_key` puts the file path in `log_path`, and the rule reads the file name off the end of it. The `fluent` listener refuses to start on the same terms as `lumberjack` above.

The listener adds `tag` and sets `timestamp` to the time Fluent Bit read the line. Zeek's own time stays in `ts` as an epoch float, and dotted names like `id.orig_h` land as nested objects. The run added `exit_on_eof: true` to the input so Fluent Bit stopped after a finished file, which a live log doesn't want.

Filebeat's `filestream` input, and rsyslog's `imfile` into the `syslog` listener, read the same files. Neither was run.

## What was run

Zeek read a 78 packet capture of HTTP, DNS and WebSocket traffic, assembled from the Zeek source tree's test traces, and wrote `conn.log`, `dns.log` and `http.log`. Each path sent them through a throwaway receiver with the rules above, and the rows were counted in ClickHouse.

| Path | Events in | Rows counted |
|---|---|---|
| Filebeat to `lumberjack` | 15 `zeek.connection`, 1 `zeek.dns`, 2 `zeek.http` | 15 in `zeek_connection`, 1 in `zeek_dns`, 2 in the default source |
| Fluent Bit to `fluent` | 15 `conn.log`, 1 `dns.log` | 15 in `zeek_connection`, 1 in `zeek_dns` |
