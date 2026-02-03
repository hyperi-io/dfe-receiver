// Project:   dfe-receiver
// File:      benches/throughput.rs
// Purpose:   Throughput benchmarks for hot path validation and routing
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

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
                let _ = sonic_rs::LazyValue::from_str(p);
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
            let _ = sonic_rs::get_from_slice(bytes, &["tags", "event", "category"]);
        });
    });

    group.bench_function("extract_top_level_field", |b| {
        b.iter(|| {
            let bytes = payload.as_bytes();
            let _ = sonic_rs::get_from_slice(bytes, &["org_id"]);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    json_validation_benchmark,
    field_extraction_benchmark
);
criterion_main!(benches);
