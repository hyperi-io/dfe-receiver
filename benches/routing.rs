// Project:   dfe-receiver
// File:      benches/routing.rs
// Purpose:   Per-record routing and header stamping, as the pipeline runs them
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The routing hot path for one record: the sender `_org_id` strip, the rules,
//! the tenancy clamp, and the header fields written back into the record.

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use dfe_receiver::config::Config;
use dfe_receiver::routing::{self, Router};
use dfe_receiver::tenancy::{Ingest, Principal};
use scalo::rules::{MatchConfig, RuleConfig, SetAction};

/// A fixed arrival time, so every iteration writes the same bytes.
const NOW_MS: u128 = 1_760_000_000_000;

/// An HTTP sender's record: no header fields of its own.
const SENDER: &str = r#"{"@timestamp":"2026-10-21T01:02:03.456Z","host":{"name":"web-01","ip":"10.1.2.3"},"event":{"dataset":"nginx.access","kind":"event"},"http":{"request":{"method":"GET"},"response":{"status_code":200}},"url":{"path":"/api/v1/items"},"message":"GET /api/v1/items 200"}"#;

/// A record dfe-fetcher relayed: it carries its own `_source` and received time.
const FETCHED: &str = r#"{"crate":"dfe-fetcher","downloads":42,"_timestamp_fetcher":1757000000000,"_timestamp_received":1757000000000,"_source":"crates_audit","_source_fetcher":"crates_io.crates"}"#;

/// A row setting `_source` to `source` on a record whose `field` equals `value`.
fn rule(id: &str, field: &str, value: &str, source: &str) -> RuleConfig {
    RuleConfig::new(id, "_source", SetAction::value(source))
        .matching(MatchConfig::equals(field, value))
}

fn router(rules: Vec<RuleConfig>) -> Option<Router> {
    let mut config = Config::default();
    config.rules.rules = rules;
    Router::new(&config, &config.destinations).ok()
}

/// Strip, route and stamp one record, with the tally a one-record request gets.
fn route_one(router: &Router, record: &Bytes, ingest: &Ingest) -> Bytes {
    let payload = if router.strips_org_id() {
        routing::strip_org_id(record.clone()).unwrap_or_default()
    } else {
        record.clone()
    };
    let mut tally = router.tally();
    match router.route(payload, ingest, NOW_MS, &mut tally) {
        Ok(done) => {
            black_box(&done.route);
            done.payload
        }
        Err(_) => Bytes::new(),
    }
}

fn routing_benchmark(c: &mut Criterion) {
    let sender = Bytes::from(SENDER);
    let relayed = Bytes::from(FETCHED);
    let ingest = Ingest::sent(Principal::Listener("http"));

    let (Some(no_rules), Some(fetcher), Some(twenty_misses)) = (
        router(Vec::new()),
        router(vec![rule(
            "crates_audit",
            "_source",
            "crates_audit",
            "crates_audit",
        )]),
        router(
            (0..20)
                .map(|i| {
                    rule(
                        &format!("absent-{i}"),
                        &format!("absent_{i}"),
                        "x",
                        &format!("source_{i}"),
                    )
                })
                .collect(),
        ),
    ) else {
        return;
    };

    let mut group = c.benchmark_group("routing_hot_path");
    group.throughput(Throughput::Elements(1));
    group.bench_function("no_rules", |b| {
        b.iter(|| route_one(&no_rules, black_box(&sender), &ingest));
    });
    group.bench_function("fetched_record", |b| {
        b.iter(|| route_one(&fetcher, black_box(&relayed), &ingest));
    });
    group.bench_function("twenty_rules_miss", |b| {
        b.iter(|| route_one(&twenty_misses, black_box(&sender), &ingest));
    });
    group.finish();
}

criterion_group!(benches, routing_benchmark);
criterion_main!(benches);
