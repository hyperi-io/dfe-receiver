# dfe-receiver Request Batching — Design Spec

**Date:** 2026-04-01
**Status:** Draft — needs implementation in Phase 2
**Priority:** High (single-message-per-request is a throughput bottleneck)

## Problem

Every HTTP request body is processed as a single message through the pipeline:
`POST /ingest` → `pipeline.process(body)` → one Kafka produce.

At high request rates (10K+ req/sec), this creates thousands of individual Kafka
produce calls instead of batched sends. rdkafka batches internally but the pipeline
doesn't — each request goes through validate → route → produce independently.

Additionally, the Splunk HEC handler splits batch events but processes them
sequentially in a `for event in events` loop.

## Solution

### HTTP Request Accumulator

Add a bounded channel between HTTP handlers and the pipeline. Handlers push
payloads into the channel; a background task drains batches:

```
HTTP handler → accumulator channel → batch processor → Kafka
                                     (parallel validate+route)
```

The accumulator drains when:
- Batch reaches N items (e.g. 100)
- Time threshold expires (e.g. 10ms)
- Bytes threshold reached (e.g. 1MB)

### Batch Processing with Worker Pool

Once a batch is accumulated, process all items in parallel:

```rust
let batch = accumulator.drain();
let results = worker_pool.process_batch(&batch, |payload| {
    let validation = validator.validate(payload);
    let route = router.route(payload);
    Ok((payload, route))
});
// Then batch-produce to Kafka
```

### Splunk HEC Parallel Events

The HEC handler at `splunk_hec/mod.rs:321` already has a `for event in events`
loop. Replace with `fan_out_async` for parallel processing.

## Files to Change

- `src/pipeline/mod.rs` — add `process_batch()` method alongside `process()`
- `src/server/http/mod.rs` — wire accumulator or batch-process inline
- `src/server/splunk_hec/mod.rs` — replace sequential event loop with fan_out_async
- New: `src/pipeline/accumulator.rs` — bounded channel + drain logic

## Constraints

- Must not break existing single-message API (gRPC, syslog use it)
- Latency: batching adds delay. Max 10ms accumulation window.
- Backpressure: channel bounded, handlers get 503 when full
