// Project:   dfe-receiver
// File:      benches/json_depth.rs
// Purpose:   JSON depth guard cost, alone and inside the per-record ingress path
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! JSON depth guard benchmarks on real payloads.
//!
//! The corpus is a directory of JSON fixtures named by `JSON_DEPTH_CORPUS`.
//! A `*-expected.json` file holding an `expected` array contributes its first
//! ten objects (the dfe-transform-elastic `tests/fixtures` layout), and any
//! other object file contributes itself. Unset, it falls back to this crate's
//! `tests/fixtures`.
//!
//! ```text
//! env JSON_DEPTH_CORPUS=/path/to/dfe-transform-elastic/tests/fixtures cargo bench --bench json_depth
//! ```

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use dfe_receiver::config::{DestinationsConfig, RoutingConfig, SourceRule, ValidationConfig};
use dfe_receiver::routing::Router;
use dfe_receiver::validation::depth::{MAX_BATCH_DEPTH, MAX_PARSE_DEPTH, json_depth_within};
use dfe_receiver::validation::{ValidationResult, Validator};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

/// Objects taken from each `expected` array.
const DOCS_PER_FIXTURE: usize = 10;

/// Records in the hot sample, small enough to stay in one core's L2 between iterations.
const HOT_RECORDS: usize = 96;

/// Size of each large body.
const LARGE: usize = 1 << 20;

/// Byte-at-a-time guard with the same verdicts, the baseline the block scan is measured against.
fn byte_guard(payload: &[u8], max: usize) -> bool {
    let mut depth: usize = 0;
    let mut in_string = false;
    let mut escaped = false;
    for &b in payload {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > max {
                    return false;
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    true
}

/// A guard under test, or none.
type Guard = Option<fn(&[u8], usize) -> bool>;

const GUARDS: [(&str, Guard); 3] = [
    ("byte_guard", Some(byte_guard)),
    ("block_guard", Some(json_depth_within)),
    ("no_guard", None),
];

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().is_some_and(|e| e == "json") {
            out.push(path);
        }
    }
}

fn corpus() -> Vec<Bytes> {
    let root = std::env::var_os("JSON_DEPTH_CORPUS").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures"),
        PathBuf::from,
    );
    let mut files = Vec::new();
    walk(&root, &mut files);
    let mut records = Vec::new();
    for file in files {
        let raw = std::fs::read(&file).expect("read a corpus file");
        let Ok(value) = sonic_rs::from_slice::<sonic_rs::Value>(&raw) else {
            continue;
        };
        if let Some(expected) = value.get("expected").and_then(|e| e.as_array()) {
            for doc in expected
                .iter()
                .filter(JsonValueTrait::is_object)
                .take(DOCS_PER_FIXTURE)
            {
                records.push(Bytes::from(sonic_rs::to_vec(doc).expect("re-encode")));
            }
        } else if value.is_object() {
            records.push(Bytes::from(sonic_rs::to_vec(&value).expect("re-encode")));
        }
    }
    assert!(
        !records.is_empty(),
        "no JSON records under {}",
        root.display()
    );
    records
}

/// Records joined with `sep` inside `open` and `close` until the body reaches [`LARGE`].
fn large_body(records: &[Bytes], open: &[u8], sep: &[u8], close: &[u8]) -> Vec<u8> {
    let mut body = open.to_vec();
    for record in records.iter().cycle() {
        if body.len() >= LARGE {
            break;
        }
        if body.len() > open.len() {
            body.extend_from_slice(sep);
        }
        body.extend_from_slice(record);
    }
    body.extend_from_slice(close);
    body
}

/// One record carrying a single string value of [`LARGE`] bytes.
fn long_string_body() -> Vec<u8> {
    let text = "a \\\"quoted\\\" [bracket] {brace} line ".repeat(LARGE / 36);
    format!(r#"{{"data_stream":{{"dataset":"bench.log"}},"message":"{text}"}}"#).into_bytes()
}

/// The receiver's validator and router as a default ingest listener configures them.
fn ingress() -> (Validator, Router) {
    let validator = Validator::new(ValidationConfig {
        require_json: true,
        required_fields: Vec::new(),
        dlq_on_invalid: true,
    });
    let routing = RoutingConfig {
        source_rules: vec![SourceRule {
            field: "data_stream.dataset".to_string(),
            mode: "key_value_use".to_string(),
            match_value: None,
            source: None,
        }],
        ..RoutingConfig::default()
    };
    let router = Router::new(&routing, &DestinationsConfig::default(), true);
    (validator, router)
}

/// Guard, lazy validate and route one record, as `Pipeline::process_inner` does.
fn ingest(guard: Guard, validator: &Validator, router: &Router, record: &Bytes) -> bool {
    if let Some(guard) = guard
        && !guard(record, MAX_PARSE_DEPTH)
    {
        return false;
    }
    if validator.validate(record) != ValidationResult::Valid {
        return false;
    }
    let (_, source) = router.route_with_source(record);
    source.is_some()
}

fn bench_guard(c: &mut Criterion, name: &str, records: &[Bytes]) {
    let bytes: usize = records.iter().map(Bytes::len).sum();
    let mut group = c.benchmark_group(format!("json_depth/{name}"));
    group.throughput(Throughput::Bytes(bytes as u64));
    for (label, guard) in GUARDS {
        let Some(guard) = guard else { continue };
        group.bench_function(label, |b| {
            b.iter(|| {
                records
                    .iter()
                    .filter(|r| guard(black_box(r), MAX_PARSE_DEPTH))
                    .count()
            });
        });
    }
    let (validator, _) = ingress();
    group.bench_function("lazy_validate", |b| {
        b.iter(|| {
            records
                .iter()
                .filter(|r| validator.validate(black_box(r)) == ValidationResult::Valid)
                .count()
        });
    });
    group.finish();
}

fn bench_ingress(c: &mut Criterion, name: &str, records: &[Bytes]) {
    let (validator, router) = ingress();
    let mut group = c.benchmark_group(format!("ingress/{name}"));
    group.throughput(Throughput::Elements(records.len() as u64));
    for (label, guard) in GUARDS {
        group.bench_function(label, |b| {
            b.iter(|| {
                records
                    .iter()
                    .filter(|r| ingest(guard, &validator, &router, black_box(r)))
                    .count()
            });
        });
    }
    group.finish();
}

fn benches(c: &mut Criterion) {
    let records = corpus();
    let bytes: usize = records.iter().map(Bytes::len).sum();
    let step = (records.len() / HOT_RECORDS).max(1);
    let hot: Vec<Bytes> = records.iter().step_by(step).cloned().collect();
    let hot_bytes: usize = hot.iter().map(Bytes::len).sum();
    eprintln!(
        "corpus: {} records, {bytes} bytes, mean {}; hot sample: {} records, {hot_bytes} bytes",
        records.len(),
        bytes / records.len(),
        hot.len()
    );

    bench_guard(c, "corpus", &records);
    bench_guard(c, "corpus_hot", &hot);
    bench_ingress(c, "corpus", &records);
    bench_ingress(c, "corpus_hot", &hot);

    let large = [
        ("ndjson_1mib", large_body(&records, b"", b"\n", b"")),
        ("array_1mib", large_body(&records, b"[", b",", b"]")),
        ("long_string_1mib", long_string_body()),
    ];
    let mut group = c.benchmark_group("json_depth/large");
    for (name, body) in &large {
        group.throughput(Throughput::Bytes(body.len() as u64));
        for (label, guard) in GUARDS {
            let Some(guard) = guard else { continue };
            group.bench_function(format!("{name}/{label}"), |b| {
                b.iter(|| guard(black_box(body), MAX_BATCH_DEPTH));
            });
        }
    }
    group.finish();
}

criterion_group!(json_depth, benches);
criterion_main!(json_depth);
