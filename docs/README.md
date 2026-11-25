# dfe-receiver documentation

How dfe-receiver is built, hardened and tuned, and how to point a caller at it.

- [dfe-receiver architecture](architecture.md)
- [Protocol handlers](protocol-handlers.md)
- [Webhook intake](webhook-setup.md)
- [NXLog](nxlog.md) -- CE and EE configs for the HTTP, syslog and GELF listeners
- [Zeek](zeek.md) -- Filebeat to lumberjack, Fluent Bit to fluent
- [Suricata](suricata.md) -- Filebeat to lumberjack, Fluent Bit to fluent
- [SNMP](snmp.md) -- polling and traps through Telegraf to the HTTP listener
- [gNMI streaming telemetry](gnmi.md) -- Telegraf subscriptions to the HTTP listener
- [BGP over BMP](bmp.md) -- pmacct's pmbmpd to a landing topic, or through Fluent Bit to the HTTP listener
- [Siemens and industrial OT](ot.md) -- OPC UA, S7comm and Modbus TCP through Telegraf to the HTTP listener
- [Delivery](delivery.md)
- [dfe-receiver internals](internals.md)
- [Protocol parsing](protocols.md)
- [Internet-Facing Hardening & Fronting Architecture](hardening.md)
- [dfe-receiver metrics](metrics.md)
- [Keeping failure logs bounded](log-spamming.md)
- [Performance Optimisation Guide](performance.md)
- [Sidecar Transport Pattern](sidecar-transports.md)
- [Testing](testing.md) -- the core and full tiers, and the containers a run shares
- [config-schema.yaml](config-schema.yaml) -- the configuration as a JSON Schema (YAML)
- [config-schema.json](config-schema.json) -- the configuration as a JSON Schema (JSON)
- [capability-catalog.yaml](capability-catalog.yaml) -- what the app can do, as a catalogue (YAML)
- [capability-catalog.json](capability-catalog.json) -- what the app can do, as a catalogue (JSON)
