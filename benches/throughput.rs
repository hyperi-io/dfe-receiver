// Project:   dfe-receiver
// File:      benches/throughput.rs
// Purpose:   Throughput benchmarks for hot path validation and routing
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn json_validation_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_validation");

    let small_payload = r#"{"org_id":"test","event_category":"auth"}"#;
    let medium_payload = r#"{"org_id":"test","event_category":"auth","data":{"field1":"value1","field2":"value2","field3":"value3"}}"#;
    let large_payload = format!(
        r#"{{"org_id":"test","event_category":"auth","data":{{"field":"{}"}} }}"#,
        "x".repeat(8192)
    );

    for (name, payload) in [
        ("small", small_payload.to_string()),
        ("medium", medium_payload.to_string()),
        ("large", large_payload),
    ] {
        group.throughput(Throughput::Bytes(payload.len() as u64));
        group.bench_with_input(BenchmarkId::new("validate", name), &payload, |b, p| {
            b.iter(|| {
                // Validate JSON is parseable using sonic-rs LazyValue
                let _ = sonic_rs::from_slice::<sonic_rs::LazyValue>(black_box(p.as_bytes()));
            });
        });
    }

    group.finish();
}

fn field_extraction_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("field_extraction");

    let payload =
        r#"{"org_id":"test","tags":{"event":{"category":"authentication"}},"data":"value"}"#;

    group.throughput(Throughput::Bytes(payload.len() as u64));

    group.bench_function("extract_nested_field", |b| {
        b.iter(|| {
            let bytes = payload.as_bytes();
            // Extract nested field using sonic-rs pointer
            let _ = sonic_rs::get_from_slice(black_box(bytes), &["tags", "event", "category"]);
        });
    });

    group.bench_function("extract_top_level_field", |b| {
        b.iter(|| {
            let bytes = payload.as_bytes();
            let _ = sonic_rs::get_from_slice(black_box(bytes), &["org_id"]);
        });
    });

    group.finish();
}

fn router_benchmark(c: &mut Criterion) {
    use bytes::Bytes;
    use dfe_receiver::config::Config;
    use dfe_receiver::routing::Router;
    use dfe_receiver::tenancy::{Ingest, Principal};
    use scalo::rules::{MatchConfig, RuleConfig, SetAction};

    let mut group = c.benchmark_group("router");

    let payload =
        Bytes::from(r#"{"org_id":"acme","event_category":"auth","source":"syslog","data":"test"}"#);
    let ingest = Ingest::sent(Principal::Listener("http"));
    let build = |config: &Config| Router::new(config, &config.destinations).ok();
    // A record routed and its header written, one request's tally per record.
    let route = |router: &Router| {
        let mut tally = router.tally();
        router
            .route(
                black_box(payload.clone()),
                &ingest,
                1_760_000_000_000,
                &mut tally,
            )
            .map(|routed| routed.payload)
            .unwrap_or_default()
    };

    // No rules: the default path.
    let Some(router) = build(&Config::default()) else {
        return;
    };

    group.throughput(Throughput::Elements(1));
    group.bench_function("route_default", |b| {
        b.iter(|| route(&router));
    });

    // 5 rows, none matching: every row is evaluated.
    let mut config_5 = Config::default();
    config_5.rules.rules = (0..5)
        .map(|i| {
            RuleConfig::new(
                format!("miss-{i}"),
                "_source",
                SetAction::value(format!("source_{i}")),
            )
            .matching(MatchConfig::equals(
                format!("nonexistent_field_{i}"),
                format!("value_{i}"),
            ))
        })
        .collect();
    let Some(router_5) = build(&config_5) else {
        return;
    };

    group.bench_function("route_5_rules_miss", |b| {
        b.iter(|| route(&router_5));
    });

    group.finish();
}

fn metrics_increment_benchmark(c: &mut Criterion) {
    use dfe_receiver::metrics::Metrics;

    let mut group = c.benchmark_group("metrics_increment");

    let metrics = Metrics::default();

    group.throughput(Throughput::Elements(1));
    group.bench_function("inc_requests_total", |b| {
        b.iter(|| metrics.inc_requests_total(black_box("http")));
    });

    group.bench_function("add_bytes_received", |b| {
        b.iter(|| metrics.add_bytes_received(black_box("http"), 1024));
    });

    group.finish();
}

criterion_group!(
    benches,
    json_validation_benchmark,
    field_extraction_benchmark,
    router_benchmark,
    metrics_increment_benchmark,
);
criterion_main!(benches);
