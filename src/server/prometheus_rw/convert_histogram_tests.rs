// Project:   dfe-receiver
// File:      src/server/prometheus_rw/convert_histogram_tests.rs
// Purpose:   Remote Write native histograms and exemplars reach the record
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

use serde_json::{Value, json};

use super::convert::{PrometheusRwMode, write_request_to_json};
use super::proto;
use crate::buffer::rejects::CountedKeys;

fn label(name: &str, value: &str) -> proto::Label {
    proto::Label {
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn span(offset: i32, length: u32) -> proto::BucketSpan {
    proto::BucketSpan { offset, length }
}

/// An exponential native histogram: schema 0, buckets at Prometheus indices
/// 0, 1 and 3 counting 2, 3 and 1, delta-encoded as 2, +1, -2.
fn exponential() -> proto::Histogram {
    proto::Histogram {
        count: Some(proto::histogram::Count::CountInt(7)),
        sum: 12.5,
        schema: 0,
        zero_threshold: 0.001,
        zero_count: Some(proto::histogram::ZeroCount::ZeroCountInt(1)),
        positive_spans: vec![span(0, 2), span(1, 1)],
        positive_deltas: vec![2, 1, -2],
        timestamp: 1_709_540_000_000,
        ..proto::Histogram::default()
    }
}

/// A native histogram with custom buckets: bounds 1, 5 and 10, counts 4, 0,
/// 2 and 1 for the last, open bucket.
fn custom_buckets() -> proto::Histogram {
    proto::Histogram {
        count: Some(proto::histogram::Count::CountInt(7)),
        sum: 20.0,
        schema: -53,
        positive_spans: vec![span(0, 1), span(1, 2)],
        positive_deltas: vec![4, -2, -1],
        custom_values: vec![1.0, 5.0, 10.0],
        timestamp: 1_709_540_000_000,
        ..proto::Histogram::default()
    }
}

fn series(
    samples: Vec<proto::Sample>,
    exemplars: Vec<proto::Exemplar>,
    histograms: Vec<proto::Histogram>,
) -> proto::TimeSeries {
    proto::TimeSeries {
        labels: vec![label("__name__", "latency_seconds"), label("job", "api")],
        samples,
        exemplars,
        histograms,
    }
}

fn exemplar() -> proto::Exemplar {
    proto::Exemplar {
        labels: vec![label("trace_id", "abc123"), label("span_id", "def456")],
        value: 0.95,
        timestamp: 1_709_540_000_500,
    }
}

fn records(timeseries: Vec<proto::TimeSeries>, mode: PrometheusRwMode) -> Vec<Value> {
    let request = proto::WriteRequest {
        timeseries,
        metadata: vec![],
    };
    write_request_to_json(request, mode)
        .expect("converts")
        .iter()
        .map(|record| serde_json::from_slice(record).expect("JSON"))
        .collect()
}

/// Each native histogram is one record in every mode, with its buckets
/// expanded from spans and deltas into dense counts.
#[test]
fn a_native_histogram_is_one_record_with_its_buckets() {
    let cases: [(&str, PrometheusRwMode, proto::Histogram, Value); 5] = [
        (
            "native, exponential",
            PrometheusRwMode::Native,
            exponential(),
            json!({
                "_type": "histogram", "count": 7.0, "sum": 12.5, "schema": 0,
                "zero_threshold": 0.001, "zero_count": 1.0,
                "positive_offset": 0, "positive_bucket_counts": [2.0, 3.0, 0.0, 1.0],
                "negative_offset": 0, "negative_bucket_counts": [],
            }),
        ),
        (
            "native, custom buckets",
            PrometheusRwMode::Native,
            custom_buckets(),
            json!({
                "_type": "histogram", "count": 7.0, "schema": -53,
                "positive_offset": 0, "positive_bucket_counts": [4.0, 0.0, 2.0, 1.0],
                "custom_values": [1.0, 5.0, 10.0],
            }),
        ),
        (
            "otel, exponential",
            PrometheusRwMode::OTel,
            exponential(),
            json!({
                "metric_type": "exponential_histogram", "count": 7, "sum": 12.5,
                "scale": 0, "zero_count": 1,
                "positive_offset": -1, "positive_bucket_counts": [2, 3, 0, 1],
                "negative_offset": 0, "negative_bucket_counts": [],
            }),
        ),
        (
            "otel, custom buckets",
            PrometheusRwMode::OTel,
            custom_buckets(),
            json!({
                "metric_type": "histogram", "count": 7, "sum": 20.0,
                "bucket_counts": [4, 0, 2, 1], "explicit_bounds": [1.0, 5.0, 10.0],
            }),
        ),
        (
            "hyperdx, exponential",
            PrometheusRwMode::HyperDx,
            exponential(),
            json!({
                "_otel_metric_type": "exponential_histogram", "Count": 7, "Sum": 12.5,
                "Scale": 0, "ZeroCount": 1,
                "PositiveOffset": -1, "PositiveBucketCounts": [2, 3, 0, 1],
                "NegativeOffset": 0, "NegativeBucketCounts": [],
            }),
        ),
    ];
    for (case, mode, histogram, want) in cases {
        let got = records(vec![series(vec![], vec![], vec![histogram])], mode);
        assert_eq!(got.len(), 1, "{case}: {got:?}");
        for (key, value) in want.as_object().unwrap() {
            assert_eq!(&got[0][key], value, "{case}: {key} in {}", got[0]);
        }
    }
}

/// An exemplar reaches the record in the OTel modes, beside its series'
/// sample. Native mode gives it a record of its own.
#[test]
fn an_exemplar_reaches_the_record_in_the_otel_modes() {
    let sample = proto::Sample {
        value: 1.0,
        timestamp: 1_709_540_000_000,
    };
    let generic = records(
        vec![series(vec![sample], vec![exemplar()], vec![])],
        PrometheusRwMode::OTel,
    );
    assert_eq!(generic.len(), 1, "{generic:?}");
    assert_eq!(generic[0]["exemplars"][0]["value"], 0.95);
    assert_eq!(generic[0]["exemplars"][0]["trace_id"], "abc123");
    assert_eq!(generic[0]["exemplars"][0]["span_id"], "def456");

    let hyperdx = records(
        vec![series(vec![sample], vec![exemplar()], vec![])],
        PrometheusRwMode::HyperDx,
    );
    assert_eq!(hyperdx[0]["Exemplars.Value"], json!([0.95]));
    assert_eq!(hyperdx[0]["Exemplars.TraceId"], json!(["abc123"]));
    assert_eq!(hyperdx[0]["Exemplars.SpanId"], json!(["def456"]));
    assert_eq!(
        hyperdx[0]["Exemplars.TimeUnix"],
        json!(["2024-03-04 08:13:20.500000000"])
    );
}

/// What an OTel-shaped record cannot hold is counted, never dropped silently:
/// an exemplar sent with no sample of its series, and a float histogram,
/// whose counts the OTel shape keeps as integers.
#[test]
fn what_the_otel_shape_cannot_hold_is_counted() {
    let float = proto::Histogram {
        count: Some(proto::histogram::Count::CountFloat(2.5)),
        positive_spans: vec![span(0, 1)],
        positive_deltas: vec![],
        positive_counts: vec![2.5],
        ..exponential()
    };
    for mode in [PrometheusRwMode::OTel, PrometheusRwMode::HyperDx] {
        let recorder = CountedKeys::default();
        let _local = metrics::set_default_local_recorder(&recorder);
        let got = records(
            vec![
                series(vec![], vec![exemplar()], vec![]),
                series(vec![], vec![], vec![float.clone()]),
            ],
            mode,
        );
        assert!(got.is_empty(), "{mode:?}: {got:?}");
        assert_eq!(
            recorder.get("receiver_prometheus_rw_unrecorded_total{kind=exemplar}"),
            1,
            "{mode:?}"
        );
        assert_eq!(
            recorder.get("receiver_prometheus_rw_unrecorded_total{kind=float_histogram}"),
            1,
            "{mode:?}"
        );
    }
}

/// A histogram whose spans ask for more buckets than one may hold, or name
/// more buckets than it carries counts, is refused as malformed in every mode.
#[test]
fn a_malformed_histogram_is_refused() {
    let cases = [
        (
            "a span gap of i32::MAX",
            vec![span(0, 1), span(i32::MAX, 1)],
            vec![1, 0],
        ),
        ("more buckets than counts", vec![span(0, 3)], vec![1]),
        (
            "a negative span gap",
            vec![span(0, 1), span(-1, 1)],
            vec![1, 0],
        ),
    ];
    for (case, spans, deltas) in cases {
        for mode in [
            PrometheusRwMode::Native,
            PrometheusRwMode::OTel,
            PrometheusRwMode::HyperDx,
        ] {
            let histogram = proto::Histogram {
                positive_spans: spans.clone(),
                positive_deltas: deltas.clone(),
                ..exponential()
            };
            let request = proto::WriteRequest {
                timeseries: vec![series(vec![], vec![], vec![histogram])],
                metadata: vec![],
            };
            let refused = write_request_to_json(request, mode);
            assert!(
                matches!(refused, Err(crate::error::Error::Validation(_))),
                "{case}, {mode:?}: {refused:?}"
            );
        }
    }
}
