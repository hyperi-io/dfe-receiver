# Keeping failure logs bounded

An outage repeats one failure per record, so a log line per record would bury the signal under its own volume. Every per-record failure in the receiver is logged a bounded number of times, and a counter carries every occurrence. The helpers are scalo's `log_sampled`, `log_debounced` and `log_state_change`. scalo's logging page ([core-pillars/logging.md](https://github.com/hyperi-io/scalo-rs/blob/main/docs/core-pillars/logging.md)) describes them and the global throttle.

| Failure | How often it logs | Metric |
|---|---|---|
| Kafka enqueue error | 1 in 1000 | `receiver_kafka_send_errors_total` |
| A topic the cluster does not serve, or an ACL refusal | 1 in 1000 each | `receiver_kafka_delivery_failures_total{reason}` |
| Any other delivery failure | the first on each topic | `receiver_kafka_delivery_failures_total{reason}` |
| A refused credential | once per 5 s per reason | `receiver_auth_failures_total{reason}` |
| An IP-filter refusal | once per 5 s per listener | `receiver_ip_filter_rejected_total{transport}` |
| A validation refusal | once per 5 s per reason | `receiver_validation_failures_total{reason}` |
| A gRPC destination down, or answering an error | once per 10 s per class per destination | `receiver_destination_send_failures_total{reason}` |
| Memory pressure, on the pipeline and on each flow listener | on each change of state | `scaling_memory_pressure` |
| A HEC conversion, Lumberjack frame, remote-write empty decode, DLQ refusal or webhook filter error | 1 in 100 | the listener's own counter |
| A syslog UDP receive error | once per 5 s | |
| A remote-write series no metric type can hold | once per 5 s | `receiver_prometheus_rw_unrecorded_total{kind}` |

On top of these, scalo's logger can deduplicate identical events by signature with `tracing-throttle`. It is off by default: `LOG_THROTTLE_ENABLED=1` turns it on, with `LOG_THROTTLE_BURST` (default 50) events allowed before throttling and `LOG_THROTTLE_RATE` (default 1) a second after that.

A new per-record log site in the receiver takes one of these forms. A condition that persists logs on change of state, a per-record error is sampled, and a tight receive loop is debounced. Errors are never suppressed outright, and startup and shutdown lines are never throttled.
