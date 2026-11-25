# SNMP

SNMP reaches dfe-receiver through Telegraf, with no receiver changes. Telegraf polls the devices with `inputs.snmp`, takes traps and informs with `inputs.snmp_trap`, and posts both to the HTTP listener as JSON. The receiver never polls a device and has no trap port of its own.

| Telegraf input | What it does | dfe-receiver listener |
|---|---|---|
| `inputs.snmp` | polls agents on UDP 161 every `interval` | HTTP `POST /ingest`, port 8080 |
| `inputs.snmp_trap` | listens for traps and informs, UDP 162 by default | HTTP `POST /ingest`, port 8080 |

Every Telegraf record carries `name`, the measurement. `inputs.snmp` sets it to `snmp` for scalar fields and to the table's `name` for each table, and `inputs.snmp_trap` always sets `snmp_trap`. A source routes on it with nothing added at the collector:

```yaml
match:
  any:
    - { field: name, operator: equals, value: snmp }
    - { field: name, operator: equals, value: snmp_interface }
```

A trap source keys on `name` equals `snmp_trap`. Keep polling and traps in separate sources, because a trap is an event and a poll is a counter sample.

## Polling and traps

```toml
[agent]
  interval = "60s"
  flush_interval = "10s"
  omit_hostname = true

[[inputs.snmp]]
  agents = ["udp://router1.example.com:161", "udp://router2.example.com:161"]
  version = 2
  community = "public"
  agent_host_tag = "source"

  [[inputs.snmp.field]]
    oid = ".1.3.6.1.2.1.1.5.0"
    name = "sysName"
    is_tag = true

  [[inputs.snmp.field]]
    oid = ".1.3.6.1.2.1.1.3.0"
    name = "sysUpTime"

  [[inputs.snmp.table]]
    name = "snmp_interface"
    inherit_tags = ["sysName"]

    [[inputs.snmp.table.field]]
      oid = ".1.3.6.1.2.1.2.2.1.2"
      name = "ifDescr"
      is_tag = true

    [[inputs.snmp.table.field]]
      oid = ".1.3.6.1.2.1.2.2.1.10"
      name = "ifInOctets"

    [[inputs.snmp.table.field]]
      oid = ".1.3.6.1.2.1.2.2.1.16"
      name = "ifOutOctets"

[[inputs.snmp_trap]]
  service_address = "udp://:162"

[[outputs.http]]
  url = "https://dfe-receiver.example.com:8080/ingest"
  method = "POST"
  data_format = "json"
  json_timestamp_units = "1ms"
  json_timestamp_format = "2006-01-02T15:04:05.000Z07:00"
  json_transformation = "metrics[]"
  [outputs.http.headers]
    Content-Type = "application/json"
    Authorization = "Bearer ${DFE_INGEST_TOKEN}"
```

UDP 162 is a privileged port and the Telegraf image runs as the `telegraf` user. Give the container `CAP_NET_BIND_SERVICE`, or listen on a high port and map 162 to it. Point each device's trap destination at the Telegraf host, not the receiver.

Mint an ingest credential for the receiver's HTTP listener and hand Telegraf its token in `DFE_INGEST_TOKEN`. Without it the receiver answers 401 `invalid_bearer_token` and Telegraf keeps the batch and retries.

`outputs.http` sends a batch as `{"metrics": [...]}`. `json_transformation = "metrics[]"` turns that into a JSON array, and the receiver splits an array into one record per metric. Without it a whole batch lands as one row.

Telegraf's default timestamp is Unix seconds. `json_timestamp_format` makes it RFC 3339 with milliseconds, which the loader reads into `_timestamp`. Telegraf's `tags` object lands in `_tags`, so `source`, `sysName` and `ifDescr` are filterable without parsing the payload.

A polled interface row lands like this:

```json
{
  "name": "snmp_interface",
  "fields": { "ifInOctets": 11586, "ifOutOctets": 19016 },
  "tags": { "ifDescr": "eth0", "source": "router1.example.com", "sysName": "edge-r1" },
  "timestamp": "2026-11-25T21:25:00.000Z"
}
```

## Trap MIBs

A table given by its own OID, such as `oid = ".1.3.6.1.2.1.2.2"` on `[[inputs.snmp.table]]`, needs the MIB that defines it, because Telegraf reads the column list from the MIB. Without it Telegraf won't start (`initializing table ... Unknown Object Identifier`). Name each column as a `[[inputs.snmp.table.field]]` with a numeric OID, as above, and no MIB is needed for polling.

Traps always need them. `inputs.snmp_trap` resolves every trap OID and varbind against the MIBs on its `path`, `/usr/share/snmp/mibs` by default. A trap it can't resolve is logged as `Error resolving OID` and dropped, so nothing reaches the receiver. The Telegraf image carries no MIBs. Mount the IETF set (`SNMPv2-SMI`, `SNMPv2-TC`, `SNMPv2-CONF`, `SNMPv2-MIB`, `IF-MIB`, `IANAifType-MIB`) plus the MIB of every device model that sends traps.

A resolved trap lands with its MIB name and OID as tags:

```json
{
  "name": "snmp_trap",
  "fields": {
    "sysUpTime": { "0": 120530975 },
    "ifIndex": { "2": 2 },
    "ifDescr": { "2": "eth1" },
    "ifOperStatus": { "2": 2 }
  },
  "tags": { "community": "public", "mib": "IF-MIB", "name": "linkDown", "oid": ".1.3.6.1.6.3.1.1.5.3", "source": "192.0.2.21", "version": "2c" }
}
```

Telegraf names a varbind field after its OID with the instance suffix, `ifDescr.2`. ClickHouse's JSON type reads the dot as a path, so the field lands as `ifDescr` holding `{"2": "eth1"}`. Quote a numeric segment in a query, ``_json.fields.ifDescr.`2` ``.
