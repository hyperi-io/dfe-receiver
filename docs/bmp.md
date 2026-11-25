# BGP over BMP

BGP route data reaches DFE through the BGP Monitoring Protocol (BMP, RFC 7854), with no receiver changes. Routers stream BMP to pmacct's `pmbmpd`, which decodes it and writes one JSON object per message. That JSON takes one of two ways in.

| pmbmpd output | Into DFE | Needs |
|---|---|---|
| Kafka, `bmp_daemon_msglog_kafka_*` | straight to a source's landing topic, as an [external origin](https://github.com/hyperi-io/dfe-engine/blob/main/docs/data-plane/external-pipelines.md) | a bus deployment and a pmacct build with Kafka |
| a file, `bmp_daemon_msglog_file` | Fluent Bit tails it and posts to HTTP `POST /ingest`, port 8080 | any deployment, Kafka or not |

The router dials the collector, never the receiver. Run `pmbmpd` close to the routers on TCP 1790, or whichever port you give it.

Every record carries `bmp_router`, the address of the router that sent it, and `bmp_msg_type` on every BMP message (`init`, `peer_up`, `peer_down`, `route_monitor`, `stats` and so on). A source on the HTTP path routes on the first, which `pmbmpd`'s own `log_init` and `log_close` lines carry as well:

```yaml
field:    bmp_router
operator: exists
```

`bmp_msg_type` misses those two lines, so they'd land in the default source. A route record looks like this:

```json
{
  "event_type": "log",
  "bmp_msg_type": "route_monitor",
  "bmp_router": "192.0.2.11",
  "bmp_rib_type": "Adj-Rib-In Post-Policy",
  "peer_ip": "192.0.2.12",
  "peer_asn": 65002,
  "log_type": "update",
  "ip_prefix": "203.0.113.0/24",
  "as_path": "65002",
  "origin": "i",
  "timestamp": "2026-11-25 21:28:18.710213",
  "timestamp_arrival": "2026-11-25 21:31:46.986861"
}
```

`timestamp` is the router's time for the message, read into `_timestamp`. A withdrawal is `log_type: withdraw` with the prefix and no path. Each table the router monitors (`Adj-Rib-In Pre-Policy`, `Adj-Rib-In Post-Policy`, `Loc-Rib`) gives its own copy, so filter on `bmp_rib_type` before counting routes.

Any router that speaks BMP will do. On FRR, `bgpd` has to start with the BMP module (`-M bmp` in `bgpd_options` in `/etc/frr/daemons`), then:

```text
router bgp 65001
 bmp targets collectors
  bmp connect bmp-collector.example.com port 1790 min-retry 1000 max-retry 5000
  bmp monitor ipv4 unicast pre-policy
  bmp monitor ipv4 unicast post-policy
  bmp stats interval 30000
 exit
```

On connect the router sends an initiation message, a peer up per BGP session and its whole table as route monitoring records. A full table arrives as one burst each time the BMP session comes up.

## Kafka

```text
bmp_daemon: true
bmp_daemon_ip: 0.0.0.0
bmp_daemon_port: 1790
bmp_daemon_msglog_output: json
bmp_daemon_msglog_kafka_broker_host: kafka.example.com
bmp_daemon_msglog_kafka_broker_port: 9092
bmp_daemon_msglog_kafka_topic: bgp-bmp_land
bmp_daemon_msglog_kafka_config_file: /etc/pmacct/librdkafka.csv
```

The topic is the source's `<source>_land`. The source is an external origin with no match rule, and its `connection` block gives the topic and the bootstrap address:

```yaml
source: bgp-bmp
external:
  credential_ref: vault:kv/acme/kafka/pmacct
```

A DFE-owned broker takes SASL with SCRAM-SHA-512. `bmp_daemon_msglog_kafka_config_file` points at a CSV of librdkafka settings, one `global, <key>, <value>` line each, which is where `security.protocol`, `sasl.mechanism`, `sasl.username` and `sasl.password` go. The proof ran against a plaintext listener, and this page doesn't cover the SASL settings beyond naming them.

`pmbmpd` writes the BMP initiation message with `"timestamp": "0000-00-00 00:00:00.000000"`, because that message has no per-peer header to take a time from. A router can send the same zero time on route monitoring records. The loader rejects a `timestamp` it can't read, so on the Kafka path those records go to the loader's dead letter queue. Everything else lands. The HTTP path below removes the field first.

## HTTP

```text
bmp_daemon: true
bmp_daemon_ip: 0.0.0.0
bmp_daemon_port: 1790
bmp_daemon_msglog_output: json
bmp_daemon_msglog_file: /var/log/pmacct/bmp.json
```

```yaml
service:
  flush: 2
  parsers_file: /fluent-bit/etc/parsers.conf
pipeline:
  inputs:
    - name: tail
      path: /var/log/pmacct/bmp.json
      parser: json
  filters:
    - name: modify
      match: '*'
      condition: Key_value_matches timestamp ^0000-
      remove: timestamp
  outputs:
    - name: http
      match: '*'
      host: dfe-receiver.example.com
      port: 8080
      uri: /ingest
      format: json
      json_date_key: false
      tls: on
      header: Authorization Bearer ${DFE_INGEST_TOKEN}
```

Mint an ingest credential for the receiver's HTTP listener and hand Fluent Bit its token in `DFE_INGEST_TOKEN`. `format: json` posts a JSON array and the receiver splits it into one record each. `json_date_key: false` stops Fluent Bit adding its own `date` field.

The `modify` filter drops a zero `timestamp`, so the loader takes the load time for those records instead of rejecting them. The `pmbmpd` image writes the file as root with mode 0600, so Fluent Bit reading it from a shared volume runs as root too. Nothing trims the file, and every stats interval adds a line per peer.
