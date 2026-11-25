# Siemens and industrial OT

Plant data reaches dfe-receiver through Telegraf, with no receiver changes. Telegraf reads the PLC or OPC UA server on the plant network and posts JSON to the HTTP listener. The receiver never dials a PLC, so only Telegraf needs a route into the cell.

| Protocol | Siemens gear | Telegraf input | Telegraf `name` |
|---|---|---|---|
| OPC UA | SIMATIC S7-1500, WinCC Unified, and most other OPC UA servers | `inputs.opcua` polls, `inputs.opcua_listener` subscribes | `opcua` |
| S7comm | SIMATIC S7-300, S7-400, S7-1200 and S7-1500 over PUT/GET | `inputs.s7comm` polls | `s7comm` |
| Modbus TCP | SENTRON PAC meters, drives and anything else with a Modbus TCP server | `inputs.modbus` polls | `modbus` |

Prefer OPC UA wherever the PLC offers it. It sends the value's type, a quality code and the PLC's timestamp. S7comm and Modbus send the value alone, so Telegraf stamps the time it read it.

## One record per signal

Every config on this page gives each signal a record, with the same shape for all three protocols:

- `fields` holds one value under a readable name (`oven_temperature`, not `ns=1;s=Line1.Oven.Temperature`), typed as the PLC holds it: a REAL or Double as a JSON number, a BOOL as `true` or `false`, an integer as an integer, a STRING as a string.
- `tags` keeps the raw address the value came from (`id` for OPC UA, `address` for S7comm and Modbus), the engineering unit in `unit`, and the PLC in `plc`.
- `name` is the Telegraf measurement, which a source routes on.

A source routes on `name`, which Telegraf sends with every record:

```yaml
field:    name
operator: equals
value:    opcua
```

Use one source per protocol, `opcua`, `s7comm` or `modbus`. Until an OT meta schema exists, a source needs a placeholder schema to deploy, and the record lands in `_json` and `_tags`. The address tags land in `_tags` as well as `_json`, so `_tags.address` and `_tags.unit` filter without parsing the payload.

ClickHouse's JSON type stores each value with the type it arrived as. Telegraf writes a float with no fraction as `180`, not `180.0`, so a whole-number reading of a float signal is stored as `Int64` beside the `Float64` ones. Read a float signal with `toFloat64(_json.fields.oven_setpoint)`, never the typed subcolumn `_json.fields.oven_setpoint.:Float64`, which returns NULL for the whole-number rows.

`outputs.http` is the same in every config here and matches the [SNMP](snmp.md) page. Mint an ingest credential for the HTTP listener and hand Telegraf its token in `DFE_INGEST_TOKEN`. `json_transformation = "metrics[]"` sends a batch as a JSON array, which the receiver splits into one record per metric, and `json_timestamp_format` gives the loader an RFC 3339 time for `_timestamp`.

## OPC UA

```toml
[agent]
  interval = "10s"
  flush_interval = "5s"
  omit_hostname = true
  precision = "1ms"

[[inputs.opcua]]
  name = "opcua"
  endpoint = "opc.tcp://plc1.example.com:4840"
  security_policy = "None"
  security_mode = "None"
  auth_method = "Anonymous"
  timestamp = "source"
  optional_fields = ["DataType"]
  [inputs.opcua.tags]
    plc = "line1"

  [[inputs.opcua.nodes]]
    name = "oven_temperature"
    id = "ns=1;s=Line1.Oven.Temperature"
    default_tags = { unit = "degC" }
  [[inputs.opcua.nodes]]
    name = "oven_setpoint"
    id = "ns=1;s=Line1.Oven.Setpoint"
    default_tags = { unit = "degC" }
  [[inputs.opcua.nodes]]
    name = "conveyor_speed"
    id = "ns=1;s=Line1.Conveyor.Speed"
    default_tags = { unit = "m/min" }
  [[inputs.opcua.nodes]]
    name = "parts_good"
    id = "ns=1;s=Line1.Counter.PartsGood"
    default_tags = { unit = "count" }

[[inputs.opcua_listener]]
  name = "opcua"
  endpoint = "opc.tcp://plc1.example.com:4840"
  security_policy = "None"
  security_mode = "None"
  auth_method = "Anonymous"
  timestamp = "source"
  subscription_interval = "1s"
  optional_fields = ["DataType"]
  connect_fail_behavior = "retry"
  [inputs.opcua_listener.tags]
    plc = "line1"

  [[inputs.opcua_listener.nodes]]
    name = "oven_pressure"
    id = "ns=1;s=Line1.Oven.Pressure"
    default_tags = { unit = "bar" }
  [[inputs.opcua_listener.nodes]]
    name = "conveyor_running"
    id = "ns=1;s=Line1.Conveyor.Running"

  [inputs.opcua_listener.workarounds]
    additional_valid_status_codes = ["0x40900000"]

[[processors.regex]]
  namepass = ["opcua"]
  [[processors.regex.fields]]
    key = "Quality"
    pattern = '^.*Status(\w+) \((0x[0-9A-Fa-f]+)\)$'
    replacement = "${2}"
    result_key = "status_code"
  [[processors.regex.fields]]
    key = "Quality"
    pattern = '^.*Status(\w+) \((0x[0-9A-Fa-f]+)\)$'
    replacement = "${1}"

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

`inputs.opcua` reads every node each `interval`. `inputs.opcua_listener` subscribes, and the server sends a record each time a value changes, which suits fast or rarely changing signals. Give both inputs the same `name` so one source takes them. Don't subscribe and poll the same node, or it lands twice.

The node ids are the server's. On an S7-1500 they come from the CPU's OPC UA server in TIA Portal, and an OPC UA browser such as UaExpert shows each one. `name` gives the signal its field name and `id` lands in `tags.id`, so every row keeps the node it came from. Telegraf doesn't read a node's `EngineeringUnits` property, so set the unit in `default_tags`.

`timestamp = "source"` stamps each record with the PLC's source timestamp. Telegraf rounds a polled record's timestamp to the agent's `precision`, and left unset it dropped the milliseconds, so set `precision = "1ms"` to keep them. A subscribed record keeps them either way. A polled node that hasn't changed lands again with its old source timestamp, so count reads by `_timestamp_received`.

Every record has the OPC UA status in `Quality`. Telegraf writes it as a sentence (`The operation succeeded. StatusGood (0x0)`), and the two regex rules cut it to the status name in `Quality` (`Good`) and the code in `status_code` (`0x0`). Telegraf logs any status that isn't Good as an error and keeps the node's last value. List a status in `additional_valid_status_codes` to take its values anyway, as the config does for `UncertainLastUsableValue`.

A polled node lands like this:

```json
{
  "name": "opcua",
  "fields": { "DataType": "Double", "Quality": "Good", "oven_temperature": 180.8892506626224, "status_code": "0x0" },
  "tags": { "id": "ns=1;s=Line1.Oven.Temperature", "plc": "line1", "unit": "degC" },
  "timestamp": "2026-11-25T22:44:49.365Z"
}
```

A subscribed node with an uncertain status keeps its value and the status:

```json
{
  "name": "opcua",
  "fields": { "DataType": "Double", "Quality": "UncertainLastUsableValue", "oven_pressure": 1.24, "status_code": "0x40900000" },
  "tags": { "id": "ns=1;s=Line1.Oven.Pressure", "plc": "line1", "unit": "bar" },
  "timestamp": "2026-11-25T22:42:02.364Z"
}
```

`security_policy = "None"` and anonymous sign-in suit a lab server only. On a plant network use `security_policy = "Basic256Sha256"` and `security_mode = "SignAndEncrypt"`, give Telegraf a `certificate` and `private_key` that persist, and trust that certificate in the PLC's OPC UA server. Without them Telegraf makes a new certificate at every restart, which the server has to trust again.

`inputs.opcua_listener` subscribes to OPC UA events too, which hold alarms. Those land with `name` set to `opcua_event`, so they need a separate source.

## S7comm

S7comm is the protocol STEP 7 and TIA Portal speak. It reads data blocks by absolute address, so it suits older S7-300 and S7-400 CPUs, and S7-1200 and S7-1500 CPUs with no OPC UA server licensed.

```toml
[agent]
  interval = "10s"
  flush_interval = "5s"
  omit_hostname = true

[[inputs.s7comm]]
  server = "plc2.example.com:102"
  rack = 0
  slot = 1
  [inputs.s7comm.tags]
    plc = "press7"

  [[inputs.s7comm.metric]]
    name = "s7comm"
    fields = [{ name = "hydraulic_temperature", address = "DB1.R0" }]
    tags = { address = "DB1.DBD0", unit = "degC" }
  [[inputs.s7comm.metric]]
    name = "s7comm"
    fields = [{ name = "press_running", address = "DB1.X4.0" }]
    tags = { address = "DB1.DBX4.0" }
  [[inputs.s7comm.metric]]
    name = "s7comm"
    fields = [{ name = "hydraulic_pressure", address = "DB1.I6" }]
    tags = { address = "DB1.DBW6", unit = "bar" }
  [[inputs.s7comm.metric]]
    name = "s7comm"
    fields = [{ name = "stroke_count", address = "DB1.DI8" }]
    tags = { address = "DB1.DBD8", unit = "count" }
  [[inputs.s7comm.metric]]
    name = "s7comm"
    fields = [{ name = "tool_id", address = "DB1.S12.16" }]
    tags = { address = "DB1.DBB12" }

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

Telegraf's address names the data type, and STEP 7's names the width. The config reads with Telegraf's form and tags each record with the STEP 7 form, which is what the PLC programmer's symbol table shows:

| PLC type | Telegraf `address` | STEP 7 address in `tags.address` | Lands as |
|---|---|---|---|
| REAL | `DB1.R0` | `DB1.DBD0` | JSON number, `Float64` |
| BOOL | `DB1.X4.0` | `DB1.DBX4.0` | `true` or `false`, `Bool` |
| INT | `DB1.I6` | `DB1.DBW6` | integer, `Int64` |
| DINT | `DB1.DI8` | `DB1.DBD8` | integer, `Int64` |
| STRING[16] | `DB1.S12.16` | `DB1.DBB12` | string, `String` |

Each `[[inputs.s7comm.metric]]` holds one field so each record can keep that field's address. Telegraf still reads them together, up to `pdu_size` fields a request.

An S7-1200 or S7-1500 needs two settings in TIA Portal before it answers: "Permit access with PUT/GET communication" on the CPU, and "Optimized block access" off on each data block you read, since an optimised block has no absolute addresses. Use rack 0 and slot 1 for an S7-1200 or S7-1500 and slot 2 for an S7-300.

S7comm has no timestamp or quality. `timestamp` is when Telegraf read the block. A failed read sends nothing for that interval: Telegraf logs the error and reconnects.

A REAL lands like this:

```json
{
  "name": "s7comm",
  "fields": { "hydraulic_temperature": 70.83665466308594 },
  "tags": { "address": "DB1.DBD0", "plc": "press7", "unit": "degC" },
  "timestamp": "2026-11-25T22:42:00.000Z"
}
```

A REAL is 32-bit and Telegraf widens it to 64, so `70.83665466308594` is the exact value the PLC holds.

## Modbus TCP

```toml
[agent]
  interval = "10s"
  flush_interval = "5s"
  omit_hostname = true

[[inputs.modbus]]
  name = "meter1"
  controller = "tcp://meter1.example.com:502"
  configuration_type = "metric"
  exclude_register_type_tag = true

  [[inputs.modbus.metric]]
    slave_id = 1
    byte_order = "ABCD"
    measurement = "modbus"
    fields = [{ register = "holding", address = 0, name = "voltage_l1_n", type = "FLOAT32" }]
    tags = { address = "holding:0", unit = "V" }
  [[inputs.modbus.metric]]
    slave_id = 1
    byte_order = "ABCD"
    measurement = "modbus"
    fields = [{ register = "holding", address = 2, name = "current_l1", type = "FLOAT32" }]
    tags = { address = "holding:2", unit = "A" }
  [[inputs.modbus.metric]]
    slave_id = 1
    byte_order = "ABCD"
    measurement = "modbus"
    fields = [{ register = "holding", address = 4, name = "active_power", type = "FLOAT32" }]
    tags = { address = "holding:4", unit = "W" }
  [[inputs.modbus.metric]]
    slave_id = 1
    byte_order = "ABCD"
    measurement = "modbus"
    fields = [{ register = "holding", address = 6, name = "frequency", type = "FLOAT32" }]
    tags = { address = "holding:6", unit = "Hz" }
  [[inputs.modbus.metric]]
    slave_id = 1
    byte_order = "ABCD"
    measurement = "modbus"
    fields = [{ register = "holding", address = 8, name = "energy_import", type = "UINT32", scale = 0.001 }]
    tags = { address = "holding:8", unit = "kWh" }
  [[inputs.modbus.metric]]
    slave_id = 1
    measurement = "modbus"
    fields = [{ register = "coil", address = 16, name = "breaker_closed", output = "BOOL" }]
    tags = { address = "coil:16" }

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

The register map is an example. Take the device's map from its manual, and check how it numbers registers. Telegraf's `address` is the zero-based address on the wire, so a manual that lists holding register 40001 or 1 means `address = 0`.

Modbus registers are 16-bit words, so `type` says how to read them: `FLOAT32` and `UINT32` take two registers, and `byte_order` says which word comes first. A scaled value (`scale = 0.001` turns Wh into kWh) lands as a float. A coil read with `output = "BOOL"` lands as `true` or `false` instead of 0 or 1.

`measurement` sets `name` to `modbus` for every record. Telegraf adds the device's `name` (`meter1` here) and `slave_id` as tags. `exclude_register_type_tag` drops a third tag, since `tags.address` names the register type already.

Modbus has no timestamp or quality. `timestamp` is when Telegraf read the registers. A failed read sends nothing for that device and interval: Telegraf logs the error and reconnects.

A FLOAT32 holding register lands like this:

```json
{
  "name": "modbus",
  "fields": { "voltage_l1_n": 228.08157348632812 },
  "tags": { "address": "holding:0", "name": "meter1", "slave_id": "1", "unit": "V" },
  "timestamp": "2026-11-25T22:45:00.000Z"
}
```

## Siemens paths that already work

These need no Telegraf:

- RUGGEDCOM and SCALANCE network gear, SINEC NMS and WinCC alarm forwarding send syslog and SNMP traps. Point syslog at the receiver's `syslog` listener ([parsing](protocols.md#syslog-parsing)) and traps at Telegraf as the [SNMP](snmp.md) page describes.
- Where a SINEC or WinCC event store is a SQL Server or PostgreSQL database, dfe-fetcher's `sources.db` tails it. dfe-fetcher's `docs/reference/db-drivers.md` lists the drivers and what each needs.

## Not covered

IEC 61850 (MMS and GOOSE from SIPROTEC protection relays) has no widely used open collector, and Telegraf has no input for it. Reading it would take an MMS client built on libIEC61850 that maps logical nodes to records and posts them to the HTTP listener.
