// Project:   dfe-receiver
// File:      benches/throughput.rs
// Purpose:   Throughput benchmarks for hot path validation and routing
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

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
    use dfe_receiver::config::{DestinationsConfig, RoutingConfig, SourceRule};
    use dfe_receiver::routing::Router;

    let mut group = c.benchmark_group("router");

    let payload =
        Bytes::from(r#"{"org_id":"acme","event_category":"auth","source":"syslog","data":"test"}"#);

    // No source rules — fast default path
    let config = RoutingConfig::default();
    let destinations = DestinationsConfig::default();
    let router = Router::new(&config, &destinations, true);

    group.throughput(Throughput::Elements(1));
    group.bench_function("route_default", |b| {
        b.iter(|| router.route(black_box(&payload)));
    });

    // 5 source rules — worst case miss (evaluates all rules)
    let mut config_5 = RoutingConfig::default();
    for i in 0..5 {
        config_5.source_rules.push(SourceRule {
            field: format!("nonexistent_field_{i}"),
            mode: "key_value_set".into(),
            match_value: Some(format!("value_{i}")),
            source: Some(format!("source_{i}")),
        });
    }
    let router_5 = Router::new(&config_5, &destinations, true);

    group.bench_function("route_5_rules_miss", |b| {
        b.iter(|| router_5.route(black_box(&payload)));
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
