// Project:   dfe-receiver
// File:      src/server/otlp/convert_metric_detail_tests.rs
// Purpose:   OTLP exponential-histogram buckets and exemplars reach the record
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

use serde_json::{Value, json};

use super::convert::{OtlpMode, convert_metrics};
use super::pb;
use super::pb::metrics::v1::{
    Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    exponential_histogram_data_point::Buckets, metric::Data, number_data_point,
};

const NOW: u64 = 1_771_459_200_000_000_000;

fn exemplar() -> Exemplar {
    Exemplar {
        filtered_attributes: vec![pb::common::v1::KeyValue {
            key: "route".to_string(),
            value: Some(pb::common::v1::AnyValue {
                value: Some(pb::common::v1::any_value::Value::StringValue("/api".into())),
            }),
        }],
        time_unix_nano: NOW + 500_000_000,
        span_id: vec![0xde, 0xf4, 0x56, 0x00, 0x00, 0x00, 0x00, 0x01],
        trace_id: vec![0xab; 16],
        value: Some(pb::metrics::v1::exemplar::Value::AsDouble(0.95)),
    }
}

fn number_point() -> NumberDataPoint {
    NumberDataPoint {
        time_unix_nano: NOW,
        exemplars: vec![exemplar()],
        value: Some(number_data_point::Value::AsDouble(1.0)),
        ..NumberDataPoint::default()
    }
}

fn records(data: Data, mode: OtlpMode) -> Vec<Value> {
    let request = pb::collector::metrics::v1::ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "latency_seconds".to_string(),
                    data: Some(data),
                    ..Metric::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    convert_metrics(&request, mode)
        .expect("converts")
        .iter()
        .map(|payload| serde_json::from_slice(&payload.json).expect("JSON"))
        .collect()
}

/// An exponential histogram's buckets, either side of zero, and its zero
/// threshold reach the record in both modes, keyed as remote write keys them.
#[test]
fn an_exponential_histogram_carries_its_buckets() {
    let point = ExponentialHistogramDataPoint {
        time_unix_nano: NOW,
        count: 7,
        sum: Some(12.5),
        scale: 2,
        zero_count: 1,
        zero_threshold: 0.001,
        positive: Some(Buckets {
            offset: 3,
            bucket_counts: vec![2, 0, 3],
        }),
        negative: Some(Buckets {
            offset: -1,
            bucket_counts: vec![1],
        }),
        ..ExponentialHistogramDataPoint::default()
    };
    let data = || {
        Data::ExponentialHistogram(ExponentialHistogram {
            data_points: vec![point.clone()],
            aggregation_temporality: 0,
        })
    };
    let cases = [
        (
            OtlpMode::Generic,
            json!({
                "positive_offset": 3, "positive_bucket_counts": [2, 0, 3],
                "negative_offset": -1, "negative_bucket_counts": [1],
                "zero_threshold": 0.001,
            }),
        ),
        (
            OtlpMode::HyperDx,
            json!({
                "PositiveOffset": 3, "PositiveBucketCounts": [2, 0, 3],
                "NegativeOffset": -1, "NegativeBucketCounts": [1],
            }),
        ),
    ];
    for (mode, want) in cases {
        let got = records(data(), mode);
        assert_eq!(got.len(), 1, "{mode:?}");
        for (key, value) in want.as_object().unwrap() {
            assert_eq!(&got[0][key], value, "{mode:?}: {key} in {}", got[0]);
        }
    }
}

/// Every data point that carries exemplars keeps them on its record: the
/// trace and span ids as hex, the filtered attributes beside them.
#[test]
fn every_kind_of_data_point_keeps_its_exemplars() {
    let kinds = [
        (
            "gauge",
            Data::Gauge(Gauge {
                data_points: vec![number_point()],
            }),
        ),
        (
            "sum",
            Data::Sum(Sum {
                data_points: vec![number_point()],
                ..Sum::default()
            }),
        ),
        (
            "histogram",
            Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: NOW,
                    count: 1,
                    exemplars: vec![exemplar()],
                    ..HistogramDataPoint::default()
                }],
                aggregation_temporality: 0,
            }),
        ),
        (
            "exponential histogram",
            Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: NOW,
                    count: 1,
                    exemplars: vec![exemplar()],
                    ..ExponentialHistogramDataPoint::default()
                }],
                aggregation_temporality: 0,
            }),
        ),
    ];
    let trace_id = "ab".repeat(16);
    for (kind, data) in kinds {
        let generic = records(data.clone(), OtlpMode::Generic);
        let exemplars = &generic[0]["exemplars"];
        assert_eq!(exemplars[0]["value"], 0.95, "{kind}: {}", generic[0]);
        assert_eq!(exemplars[0]["trace_id"], trace_id, "{kind}");
        assert_eq!(exemplars[0]["span_id"], "def4560000000001", "{kind}");
        assert_eq!(exemplars[0]["attributes"]["route"], "/api", "{kind}");

        let hyperdx = records(data, OtlpMode::HyperDx);
        assert_eq!(hyperdx[0]["Exemplars.Value"], json!([0.95]), "{kind}");
        assert_eq!(hyperdx[0]["Exemplars.TraceId"], json!([trace_id]), "{kind}");
        assert_eq!(
            hyperdx[0]["Exemplars.SpanId"],
            json!(["def4560000000001"]),
            "{kind}"
        );
        assert_eq!(
            hyperdx[0]["Exemplars.FilteredAttributes"],
            json!([{"route": "/api"}]),
            "{kind}"
        );
        assert_eq!(
            hyperdx[0]["Exemplars.TimeUnix"],
            json!(["2026-02-19 00:00:00.500000000"]),
            "{kind}"
        );
    }
}
