# PGO workload events

`beats-events.ndjson` is what `pgo-driver` sends over HTTP and lumberjack: one Elastic-shipped event per line, the `event` object of each dfe-transform-elastic envelope fixture, compacted and otherwise unchanged. Those fixtures record where each shape was read from.

| line | dfe-transform-elastic fixture | shape |
|---|---|---|
| 1 | `tests/envelopes/beats/agent_cisco_ios.json` | Elastic Agent 8.0.0, `data_stream.dataset: cisco_ios.log` |
| 2 | `tests/envelopes/beats/agent.json` | Elastic Agent 8.1.3, `data_stream.dataset: cylance.protect` |
| 3 | `tests/envelopes/beats/module.json` | filebeat 8.13.2 panw module, `event.dataset: panw.panos`, no `data_stream` |
| 4 | `tests/envelopes/beats/bare.json` | bare `{message, tags}`, no routing marker |

The workload config routes lines 1 and 2 on `data_stream.dataset`, the key a deployed receiver's source rules match on, line 3 on `event.dataset`, and leaves line 4 to the default source.

Copied from dfe-transform-elastic `50dcff5d`.
