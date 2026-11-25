# gNMI streaming telemetry

gNMI telemetry reaches dfe-receiver through Telegraf, with no receiver changes. Telegraf's `inputs.gnmi` dials each device's gNMI server and subscribes to the paths you name. `outputs.http` then posts the samples to the HTTP listener as JSON. The receiver never dials a device.

Each subscription's `name` becomes the record's `name`, the Telegraf measurement. A source routes on it:

```yaml
field:    name
operator: starts_with
value:    gnmi_
```

Every record also carries the device's own response path in `tags.path` (`srl_nokia-interfaces:/interface/statistics`) and the device address in `tags.source`.

```toml
[agent]
  interval = "10s"
  flush_interval = "5s"
  omit_hostname = true

[[inputs.gnmi]]
  addresses = ["leaf1.example.com:57400"]
  username = "${GNMI_USERNAME}"
  password = "${GNMI_PASSWORD}"
  encoding = "json_ietf"
  redial = "10s"
  tls_enable = true
  tls_ca = "/etc/telegraf/gnmi-ca.pem"
  path_guessing_strategy = "subscription"

  [inputs.gnmi.aliases]
    gnmi_interface = "srl_nokia-interfaces:/interface/statistics"
    gnmi_system = "srl_nokia-system:/system/information"

  [[inputs.gnmi.subscription]]
    name = "gnmi_interface"
    origin = "srl_nokia"
    path = "/interface[name=*]/statistics"
    subscription_mode = "sample"
    sample_interval = "10s"

  [[inputs.gnmi.subscription]]
    name = "gnmi_system"
    origin = "srl_nokia"
    path = "/system/information"
    subscription_mode = "sample"
    sample_interval = "30s"

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

The example targets Nokia SR Linux native paths. On another platform, swap the subscriptions and aliases for its own paths or the OpenConfig ones it supports.

`outputs.http` and its JSON settings are the same as on the [SNMP](snmp.md) page: mint an ingest credential and hand Telegraf the token in `DFE_INGEST_TOKEN`. `json_transformation = "metrics[]"` sends a batch as a JSON array, and `json_timestamp_format` gives the loader an RFC 3339 time for `_timestamp`.

The aliases matter. SR Linux answers with the module namespace in the path (`srl_nokia-interfaces:`), not the subscription's origin, so Telegraf can't tie the response to a subscription. It logs `Got empty metric-name for response` and sends the record with an empty `name`, which no `name` rule matches and the record lands in the default source. An alias per response path gives it the name back. Check the Telegraf log for that warning whenever you add a subscription.

An interface sample lands like this:

```json
{
  "name": "gnmi_interface",
  "fields": {
    "in_octets": "16407",
    "in_packets": "110",
    "in_error_packets": "0",
    "out_octets": "25696",
    "out_packets": "90"
  },
  "tags": { "name": "mgmt0", "path": "srl_nokia-interfaces:/interface/statistics", "source": "leaf1.example.com" },
  "timestamp": "2026-11-25T22:04:40.768Z"
}
```

`json_ietf` encodes 64-bit counters as strings, as RFC 7951 requires, so the counters land as strings. Cast them in a query, `toUInt64(_json.fields.in_octets)`.

The gNMI server's TLS certificate is the device's. Give Telegraf the CA that signed it in `tls_ca`. `insecure_skip_verify = true` works against a lab device's self-signed certificate and checks nothing.
