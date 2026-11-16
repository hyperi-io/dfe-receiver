# dfe-receiver metrics

The receiver serves Prometheus metrics on its metrics listener (`metrics.address`, default `0.0.0.0:9090`) at `/metrics`. Names are bare unless scalo's `metrics.namespace` sets a prefix, and it is empty by default, so a scrape tells services apart by its `job` label. `dfe-receiver metrics-manifest` prints every registered series with its type, labels and description. scalo serves process and container gauges and a few DLQ counters as well, which the manifest does not list ([scalo-rs#137](https://github.com/hyperi-io/scalo-rs/issues/137)).

## Requests and records

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `receiver_requests_total` | Counter | `transport` | Requests received |
| `receiver_requests_success_total` / `receiver_requests_error_total` | Counter | `transport` | Requests answered as taken, and as not |
| `receiver_request_duration_seconds` | Histogram | `transport` | Time from receipt to the answer |
| `receiver_bytes_received_total` | Counter | `transport` | Bytes ingested |
| `receiver_active_connections` | Gauge | `transport` | Open inbound connections |
| `receiver_connections_closed_total` | Counter | `transport`, `reason` | Connections closed for a limit: `over_limit`, `no_first_byte`, `frame_timeout`, `idle` |
| `receiver_parse_failures_total` | Counter | `transport` | Bodies a protocol decoder could not read |
| `receiver_empty_decodes_total` | Counter | `transport` | Non-empty bodies that decoded to no records, answered as accepted |
| `receiver_lossy_decodes_total` | Counter | `transport` | Syslog messages and Fluent Forward records kept with invalid UTF-8 replaced by U+FFFD |
| `receiver_validation_failures_total` | Counter | `reason` | Records refused by validation: `invalid_json`, `missing_field`, `nesting_too_deep`, `not_an_object` |
| `receiver_auth_failures_total` | Counter | `reason` | Refused credentials |
| `receiver_request_timeouts_total`, `receiver_body_size_rejected_total` | Counter | `transport` | 408s and 413s |
| `receiver_rate_limited_total` | Counter | `transport` | 429s from `server.rate_limit` |
| `receiver_ip_filter_rejected_total` | Counter | `transport` | Connections and datagrams the IP filter refused |
| `receiver_unsupported_encoding_total` | Counter | `transport` | 415s for a `Content-Encoding` or media type the receiver does not read |
| `receiver_tls_handshake_failures_total` | Counter | | Handshakes that failed or timed out |
| `receiver_records_dropped_total` | Counter | `transport`, `reason` | Records dropped with no way to tell the sender: `unavailable`, `rejected`, `shutdown`, `oversize`, `kernel` |
| `records_received_total` | Counter | | Records, counted once when a listener first offers each to the pipeline |
| `records_processed_total` / `records_delivered_total` | Counter | | Records the pipeline took |
| `records_error_total` | Counter | | Records refused for good |
| `usage_ingested_bytes_total` | Counter | `org_id` | Payload bytes accepted, by the org each record was stamped with |
| `rules_hits_total`, `rules_clamped_total` | Counter | `rule_set`, `rule` | Rows that set a target, and clamped values the binding's default replaced |
| `receiver_source_org_id_stripped_total` | Counter | | Records with a sender-supplied `_org_id` removed under `routing.source_org_id: disallow` (the default) |
| `receiver_original_stripped_total` | Counter | | Records with the `event.original` and `log.original` copy of the raw line removed because `message` has it. A record tagged `preserve_original_event`, or with no `message`, keeps it |

## Kafka

Enqueue and delivery are separate counts. librdkafka takes a record into its queue first and a broker answers for it later, so a record can be enqueued and never delivered -- the delivery counters are the ones that say a broker holds it.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `receiver_kafka_sends_total` | Counter | | Messages librdkafka queued |
| `receiver_kafka_send_duration_seconds` | Histogram | | Time to queue one send |
| `receiver_kafka_bytes_sent_total` | Counter | | Bytes queued to Kafka |
| `receiver_kafka_send_errors_total` | Counter | | Messages librdkafka refused to queue |
| `receiver_kafka_delivered_total` | Counter | | Messages a broker acknowledged |
| `receiver_kafka_delivery_failures_total` | Counter | `reason` | Messages no broker confirmed, by librdkafka error code. A timed-out message may still have been written |
| `receiver_kafka_resends_total` | Counter | | Messages answered at enqueue that librdkafka gave up on with a message timeout, a topic the cluster does not serve or an authorisation failure, queued again |
| `records_dlq_total` | Counter | | Messages sent to the DLQ (scalo emits this one) |
| `dlq_backend_start_failures_total` | Counter | `backend` | DLQ backends that failed to start (scalo emits this one). The receiver runs on with the others, and with none a held dead letter is not taken |
| `receiver_records_rejected_total` | Counter | `outcome` | Records a destination can never take (over its size ceiling), on any sink: `dead_lettered`, `dropped` when no DLQ is configured or no DLQ backend can hold the entry (counted in `pipeline_dead_letters_dropped_total{reason}` as well), or `dlq_refused` when the DLQ did not confirm the write, so the record stays buffered or its sender is told to retry |
| `receiver_messages_spilled_total` / `receiver_messages_drained_total` | Counter | | Records a destination's buffer held back, in memory or on disk, and those since sent on |

## gRPC destinations

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `receiver_destination_send_failures_total` | Counter | `reason` | Records a gRPC destination did not take: `unavailable` (down, refusing connections, busy or past its deadline) or `failed` (it answered with an error). Both are retried, and each class logs at most once per 10 s per destination |
| `receiver_destination_build_failures_total` | Counter | | Destinations a config reload asked for with a sender that failed to build, logged at ERROR with the reason. Their records are answered unavailable until a reload builds the sender |
| `receiver_retired_records_total` | Counter | `outcome` | Records held by a destination a config reload retired: `sent` to it, `dead_lettered`, `spooled` (left in its disk spool), or `dropped` (no DLQ backend could hold the entry, or shutdown came first) |

## Delivery and scaling

`pipeline_delivery_guarantee{listener,guarantee,reason}` is 1 for what each listener gives, its weakest reachable leg ([delivery.md](delivery.md)). The `transport_ack_*` series are scalo's held answers by `transport`: `transport_ack_held` and `transport_ack_held_bytes` (gauges), `transport_ack_released_total{outcome}`, `transport_ack_refused_total{reason}` and `transport_ack_latency_seconds{outcome}`.

| Metric | Type | Description |
|--------|------|-------------|
| `scaling_pressure` | Gauge | Weighted scaling score (0-100), served at `/scaling/pressure` on the metrics port as well (scalo emits this one) |
| `scaling_memory_pressure` | Gauge | Memory the runtime guard counts, over its limit (0.0-1.0) |
| `scaling_circuit_open` | Gauge | 1 when the Kafka sink's circuit is open, else 0 |

The score is scalo's `ScalingPressure`, refreshed once a second. Each `scaling.*` component adds its weight in proportion to value over saturation, capped at its full weight. An open Kafka circuit forces 0, since more replicas cannot help. Otherwise memory at or past `scaling.memory_gate_threshold` forces 100. The `queue_depth` component is the outbound backlog: the records each Kafka producer holds that no broker has confirmed yet, held answers' included, plus what every buffer and spool in front of a destination holds. `config.example.yaml` lists the components and what feeds each.

A DFE deployment scales on that score through dfe-keda-shim: dfe-infra adds a `metrics-api` trigger on it, target 70, beside the chart's CPU trigger.

## Series scalo serves for every service

| Family | What it covers |
|---|---|
| `transport_*` | Delivery by `transport`: `transport_sent_total`, `transport_send_errors_total`, `transport_backpressured_total`, `transport_refused_total`, `transport_healthy`, `transport_queue_size`, `transport_queue_capacity`, `transport_inflight`, `transport_send_duration_seconds` |
| `pipeline_*` | `pipeline_ready` (1 ready, 0 not), `pipeline_stall_seconds_total`, `pipeline_retries_total{stage}`, `pipeline_idle` |
| `spool_*` | The disk spool, with spillover on: `spool_bytes`, `spool_messages`, `spool_disk_available` and the rest |
| `worker_pool_*` | scalo's worker pool |
| flow | `flow_records_emitted_total{transport,mode}`, `flow_rate_limited_total{transport}`, `flow_template_evicted_total`, `flow_kernel_drops_total`, `transport_decode_err_total{transport,reason}`, `transport_drops_total{transport,reason}`, `handler_experimental{handler}` |
| `process_*`, `container_*` | Resident and virtual memory, open file descriptors, start time, and the cgroup memory and CPU limits, not in the manifest |

## Alerts

```yaml
groups:
  - name: dfe-receiver-security
    rules:
      - alert: HighAuthFailureRate
        expr: sum(rate(receiver_auth_failures_total[5m])) > 100
        labels:
          severity: warning
      - alert: HighValidationFailureRate
        expr: sum(rate(receiver_validation_failures_total[5m])) > 50
        labels:
          severity: warning
      - alert: RequestTimeoutSpike
        expr: sum by (transport) (rate(receiver_request_timeouts_total[5m])) > 10
        labels:
          severity: info
      - alert: OversizeBodyProbe
        expr: sum by (transport) (rate(receiver_body_size_rejected_total[5m])) > 10
        labels:
          severity: info
      - alert: IpFilterRejecting
        expr: sum by (transport) (rate(receiver_ip_filter_rejected_total[5m])) > 0
        for: 15m
        labels:
          severity: info
```

A refused credential is written as a `security` target event as well, at most one line per failure reason every 5 seconds, and the counter has every one.

| Condition | Query | Threshold |
|-------|-------|-----------|
| Error rate | `rate(transport_send_errors_total[5m]) / rate(transport_sent_total[5m])` | > 0.01 |
| Backpressure | `rate(transport_backpressured_total[5m])` | > 0 sustained |
| Queue saturation | `transport_queue_size / transport_queue_capacity` | > 0.8 |
| Not ready | `pipeline_ready == 0` | > 60s |
| Unconfirmed deliveries | `rate(receiver_kafka_delivery_failures_total[5m])` | > 0 sustained |
