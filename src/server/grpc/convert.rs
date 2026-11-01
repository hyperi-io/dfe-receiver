// Project:   dfe-receiver
// File:      src/server/grpc/convert.rs
// Purpose:   Protobuf Value to JSON conversion for Vector events
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Converts Vector protobuf events to JSON for the processing pipeline.
//!
//! Vector sends events as protobuf `EventWrapper` messages containing
//! `Log`, `Metric`, or `Trace` variants. The pipeline expects JSON bytes,
//! so this module converts the recursive `Value` proto type to
//! `serde_json::Value` and serialises to bytes.

use bytes::Bytes;
use serde_json::json;

use crate::error::{Error, Result};

use super::pb::event;

/// Convert a Vector `EventWrapper` to JSON bytes for the pipeline.
pub fn event_wrapper_to_json(wrapper: &event::EventWrapper) -> Result<Bytes> {
    let event = wrapper
        .event
        .as_ref()
        .ok_or_else(|| Error::Validation("empty event wrapper: no log, metric, or trace".into()))?;

    let json_value = match event {
        event::event_wrapper::Event::Log(log) => log_to_json(log),
        event::event_wrapper::Event::Metric(metric) => metric_to_json(metric)?,
        event::event_wrapper::Event::Trace(trace) => trace_to_json(trace),
    };

    let json_bytes =
        serde_json::to_vec(&json_value).map_err(|e| Error::Validation(e.to_string()))?;

    Ok(Bytes::from(json_bytes))
}

/// Convert a Log event to JSON.
///
/// Prefers the `value` field (current Vector format). Falls back to
/// deprecated `fields` map for backwards compatibility.
fn log_to_json(log: &event::Log) -> serde_json::Value {
    // Prefer the new `value` field if present
    if let Some(ref value) = log.value {
        return proto_value_to_json(value);
    }

    // Fall back to deprecated `fields` map
    if !log.fields.is_empty() {
        return fields_map_to_json(&log.fields);
    }

    // Empty log
    json!({})
}

/// Convert a Metric event to a JSON envelope.
///
/// Wraps metric data with a `_vector_type` tag so the router
/// can direct it to a metrics-specific topic. The value goes under its kind,
/// the proto's oneof field name without its encoding version: `counter`,
/// `gauge`, `set`, `distribution`, `aggregated_histogram`,
/// `aggregated_summary` or `sketch`.
///
/// # Errors
///
/// [`Error::Validation`] for a metric with no value or an empty sketch: a
/// record of its name alone would be answered delivered with the
/// measurement lost.
fn metric_to_json(metric: &event::Metric) -> Result<serde_json::Value> {
    let Some((kind, value)) = metric.value.as_ref().and_then(metric_value_to_json) else {
        return Err(Error::Validation(format!(
            "Vector metric '{}' carries no value",
            metric.name
        )));
    };
    let mut obj = serde_json::Map::new();
    obj.insert(
        "_vector_type".into(),
        serde_json::Value::String("metric".into()),
    );
    obj.insert(
        "name".into(),
        serde_json::Value::String(metric.name.clone()),
    );

    if !metric.namespace.is_empty() {
        obj.insert(
            "namespace".into(),
            serde_json::Value::String(metric.namespace.clone()),
        );
    }

    if let Some(ref ts) = metric.timestamp {
        obj.insert("timestamp".into(), timestamp_to_json(ts));
    }

    // tags_v1 holds one value per tag, so tags_v2 wins where both are sent.
    let tags: serde_json::Map<String, serde_json::Value> = if metric.tags_v2.is_empty() {
        metric
            .tags_v1
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect()
    } else {
        metric
            .tags_v2
            .iter()
            .map(|(k, v)| (k.clone(), tag_values_to_json(v)))
            .collect()
    };
    if !tags.is_empty() {
        obj.insert("tags".into(), serde_json::Value::Object(tags));
    }

    if metric.interval_ms != 0 {
        obj.insert("interval_ms".into(), json!(metric.interval_ms));
    }
    obj.insert(kind.into(), value);

    obj.insert(
        "kind".into(),
        serde_json::Value::String(
            match metric.kind {
                k if k == event::metric::Kind::Incremental as i32 => "incremental",
                k if k == event::metric::Kind::Absolute as i32 => "absolute",
                _ => "unknown",
            }
            .into(),
        ),
    );

    Ok(serde_json::Value::Object(obj))
}

/// One tag's values: a single value as a string, several as an array, and a
/// bare tag (no value) as null.
fn tag_values_to_json(tag: &event::TagValues) -> serde_json::Value {
    let value = |v: &event::TagValue| {
        v.value
            .as_deref()
            .map_or(serde_json::Value::Null, |s| json!(s))
    };
    match tag.values.as_slice() {
        [one] => value(one),
        many => serde_json::Value::Array(many.iter().map(value).collect()),
    }
}

/// A distribution's statistic by name.
fn statistic_name(statistic: i32) -> &'static str {
    match event::StatisticKind::try_from(statistic) {
        Ok(event::StatisticKind::Histogram) => "histogram",
        Ok(event::StatisticKind::Summary) => "summary",
        Err(_) => "unknown",
    }
}

/// A metric value as its kind and its JSON, `None` for a sketch with no
/// sketch inside it.
fn metric_value_to_json(value: &event::metric::Value) -> Option<(&'static str, serde_json::Value)> {
    use event::metric::Value as V;
    let bucket = |upper_limit: f64, count: u64| json!({"upper_limit": upper_limit, "count": count});
    let quantile = |q: &event::SummaryQuantile| json!({"quantile": q.quantile, "value": q.value});
    let converted = match value {
        V::Counter(c) => ("counter", json!({"value": c.value})),
        V::Gauge(g) => ("gauge", json!({"value": g.value})),
        V::Set(s) => ("set", json!({"values": s.values})),
        V::Distribution1(d) => {
            let samples: Vec<serde_json::Value> = d
                .values
                .iter()
                .zip(&d.sample_rates)
                .map(|(value, rate)| json!({"value": value, "rate": rate}))
                .collect();
            (
                "distribution",
                json!({"samples": samples, "statistic": statistic_name(d.statistic)}),
            )
        }
        V::Distribution2(d) => {
            let samples: Vec<serde_json::Value> = d
                .samples
                .iter()
                .map(|s| json!({"value": s.value, "rate": s.rate}))
                .collect();
            (
                "distribution",
                json!({"samples": samples, "statistic": statistic_name(d.statistic)}),
            )
        }
        V::AggregatedHistogram1(h) => {
            let buckets: Vec<serde_json::Value> = h
                .buckets
                .iter()
                .zip(&h.counts)
                .map(|(&upper, &count)| bucket(upper, u64::from(count)))
                .collect();
            (
                "aggregated_histogram",
                json!({"buckets": buckets, "count": h.count, "sum": h.sum}),
            )
        }
        V::AggregatedHistogram2(h) => {
            let buckets: Vec<serde_json::Value> = h
                .buckets
                .iter()
                .map(|b| bucket(b.upper_limit, u64::from(b.count)))
                .collect();
            (
                "aggregated_histogram",
                json!({"buckets": buckets, "count": h.count, "sum": h.sum}),
            )
        }
        V::AggregatedHistogram3(h) => {
            let buckets: Vec<serde_json::Value> = h
                .buckets
                .iter()
                .map(|b| bucket(b.upper_limit, b.count))
                .collect();
            (
                "aggregated_histogram",
                json!({"buckets": buckets, "count": h.count, "sum": h.sum}),
            )
        }
        V::AggregatedSummary1(s) => {
            let quantiles: Vec<serde_json::Value> = s
                .quantiles
                .iter()
                .zip(&s.values)
                .map(|(q, v)| json!({"quantile": q, "value": v}))
                .collect();
            (
                "aggregated_summary",
                json!({"quantiles": quantiles, "count": s.count, "sum": s.sum}),
            )
        }
        V::AggregatedSummary2(s) => {
            let quantiles: Vec<serde_json::Value> = s.quantiles.iter().map(quantile).collect();
            (
                "aggregated_summary",
                json!({"quantiles": quantiles, "count": s.count, "sum": s.sum}),
            )
        }
        V::AggregatedSummary3(s) => {
            let quantiles: Vec<serde_json::Value> = s.quantiles.iter().map(quantile).collect();
            (
                "aggregated_summary",
                json!({"quantiles": quantiles, "count": s.count, "sum": s.sum}),
            )
        }
        V::Sketch(sketch) => match sketch.sketch.as_ref()? {
            event::sketch::Sketch::AgentDdSketch(dd) => (
                "sketch",
                json!({"agent_dd_sketch": {
                    "count": dd.count,
                    "min": dd.min,
                    "max": dd.max,
                    "sum": dd.sum,
                    "avg": dd.avg,
                    "k": dd.k,
                    "n": dd.n,
                }}),
            ),
        },
    };
    Some(converted)
}

/// Convert a Trace event to a JSON envelope.
fn trace_to_json(trace: &event::Trace) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "_vector_type".into(),
        serde_json::Value::String("trace".into()),
    );

    // Convert fields map
    if !trace.fields.is_empty() {
        let fields_json = fields_map_to_json(&trace.fields);
        if let serde_json::Value::Object(fields) = fields_json {
            for (k, v) in fields {
                obj.insert(k, v);
            }
        }
    }

    serde_json::Value::Object(obj)
}

/// Convert a proto `Value` to `serde_json::Value`.
///
/// Handles the recursive oneof structure:
/// - `raw_bytes` -> UTF-8 string (with base64 fallback for non-UTF-8)
/// - `timestamp` -> ISO 8601 string
/// - `integer` -> JSON number
/// - `float` -> JSON number
/// - `boolean` -> JSON boolean
/// - `map` -> JSON object (recursive)
/// - `array` -> JSON array (recursive)
/// - `null` -> JSON null
fn proto_value_to_json(value: &event::Value) -> serde_json::Value {
    let Some(ref kind) = value.kind else {
        return serde_json::Value::Null;
    };

    match kind {
        event::value::Kind::RawBytes(bytes) => {
            // Try UTF-8 first, fall back to base64 for binary data
            match std::str::from_utf8(bytes) {
                Ok(s) => serde_json::Value::String(s.to_string()),
                Err(_) => {
                    use base64::Engine;
                    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
                    serde_json::Value::String(encoded)
                }
            }
        }
        event::value::Kind::Timestamp(ts) => timestamp_to_json(ts),
        event::value::Kind::Integer(n) => json!(*n),
        event::value::Kind::Float(f) => json!(*f),
        event::value::Kind::Boolean(b) => json!(*b),
        event::value::Kind::Map(map) => fields_map_to_json(&map.fields),
        event::value::Kind::Array(arr) => {
            let items: Vec<serde_json::Value> = arr.items.iter().map(proto_value_to_json).collect();
            serde_json::Value::Array(items)
        }
        event::value::Kind::Null(_) => serde_json::Value::Null,
    }
}

/// Convert a proto fields map to JSON object.
fn fields_map_to_json(
    fields: &std::collections::HashMap<String, event::Value>,
) -> serde_json::Value {
    let obj: serde_json::Map<String, serde_json::Value> = fields
        .iter()
        .map(|(k, v)| (k.clone(), proto_value_to_json(v)))
        .collect();
    serde_json::Value::Object(obj)
}

/// Convert a protobuf Timestamp to an ISO 8601 JSON string.
fn timestamp_to_json(ts: &prost_types::Timestamp) -> serde_json::Value {
    let secs = ts.seconds;
    let nanos = ts.nanos as u32;

    // Use chrono for proper ISO 8601 formatting
    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
        serde_json::Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
    } else {
        // Fallback: raw seconds
        json!(secs)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn make_string_value(s: &str) -> event::Value {
        event::Value {
            kind: Some(event::value::Kind::RawBytes(s.as_bytes().to_vec())),
        }
    }

    fn make_int_value(n: i64) -> event::Value {
        event::Value {
            kind: Some(event::value::Kind::Integer(n)),
        }
    }

    fn make_bool_value(b: bool) -> event::Value {
        event::Value {
            kind: Some(event::value::Kind::Boolean(b)),
        }
    }

    fn make_null_value() -> event::Value {
        event::Value {
            kind: Some(event::value::Kind::Null(event::ValueNull::NullValue as i32)),
        }
    }

    fn make_float_value(f: f64) -> event::Value {
        event::Value {
            kind: Some(event::value::Kind::Float(f)),
        }
    }

    fn make_map_value(fields: std::collections::HashMap<String, event::Value>) -> event::Value {
        event::Value {
            kind: Some(event::value::Kind::Map(event::ValueMap { fields })),
        }
    }

    fn make_array_value(items: Vec<event::Value>) -> event::Value {
        event::Value {
            kind: Some(event::value::Kind::Array(event::ValueArray { items })),
        }
    }

    fn make_timestamp_value(secs: i64, nanos: i32) -> event::Value {
        event::Value {
            kind: Some(event::value::Kind::Timestamp(prost_types::Timestamp {
                seconds: secs,
                nanos,
            })),
        }
    }

    #[test]
    fn test_string_value() {
        let v = make_string_value("hello");
        let json = proto_value_to_json(&v);
        assert_eq!(json, json!("hello"));
    }

    #[test]
    fn test_integer_value() {
        let v = make_int_value(42);
        let json = proto_value_to_json(&v);
        assert_eq!(json, json!(42));
    }

    #[test]
    fn test_float_value() {
        let v = make_float_value(1.5);
        let json = proto_value_to_json(&v);
        assert_eq!(json, json!(1.5));
    }

    #[test]
    fn test_boolean_value() {
        let v = make_bool_value(true);
        let json = proto_value_to_json(&v);
        assert_eq!(json, json!(true));
    }

    #[test]
    fn test_null_value() {
        let v = make_null_value();
        let json = proto_value_to_json(&v);
        assert!(json.is_null());
    }

    #[test]
    fn test_timestamp_value() {
        // 2024-01-15T09:50:00Z
        let v = make_timestamp_value(1_705_312_200, 0);
        let json = proto_value_to_json(&v);
        let s = json.as_str().unwrap();
        assert!(s.starts_with("2024-01-15T09:50:00"));
    }

    #[test]
    fn test_map_value() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("key".to_string(), make_string_value("value"));
        fields.insert("num".to_string(), make_int_value(123));

        let v = make_map_value(fields);
        let json = proto_value_to_json(&v);

        assert_eq!(json["key"], json!("value"));
        assert_eq!(json["num"], json!(123));
    }

    #[test]
    fn test_array_value() {
        let items = vec![
            make_string_value("a"),
            make_int_value(1),
            make_bool_value(false),
        ];

        let v = make_array_value(items);
        let json = proto_value_to_json(&v);

        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], json!("a"));
        assert_eq!(arr[1], json!(1));
        assert_eq!(arr[2], json!(false));
    }

    #[test]
    fn test_nested_map() {
        let mut inner = std::collections::HashMap::new();
        inner.insert("nested_key".to_string(), make_string_value("nested_value"));

        let mut outer = std::collections::HashMap::new();
        outer.insert("inner".to_string(), make_map_value(inner));
        outer.insert("top".to_string(), make_int_value(1));

        let v = make_map_value(outer);
        let json = proto_value_to_json(&v);

        assert_eq!(json["inner"]["nested_key"], json!("nested_value"));
        assert_eq!(json["top"], json!(1));
    }

    #[test]
    fn test_empty_kind() {
        let v = event::Value { kind: None };
        let json = proto_value_to_json(&v);
        assert!(json.is_null());
    }

    #[test]
    fn test_binary_raw_bytes_base64() {
        // Non-UTF-8 bytes should be base64 encoded
        let v = event::Value {
            kind: Some(event::value::Kind::RawBytes(vec![0xFF, 0xFE, 0x00, 0x01])),
        };
        let json = proto_value_to_json(&v);
        let s = json.as_str().unwrap();
        // Should be valid base64
        use base64::Engine;
        assert!(base64::engine::general_purpose::STANDARD.decode(s).is_ok());
    }

    #[test]
    fn test_log_event_with_value() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("message".to_string(), make_string_value("hello world"));
        fields.insert("level".to_string(), make_string_value("info"));

        let log = event::Log {
            fields: std::collections::HashMap::new(),
            value: Some(make_map_value(fields)),
            ..Default::default()
        };

        let json = log_to_json(&log);
        assert_eq!(json["message"], json!("hello world"));
        assert_eq!(json["level"], json!("info"));
    }

    #[test]
    fn test_log_event_with_deprecated_fields() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("host".to_string(), make_string_value("server1"));

        let log = event::Log {
            fields,
            ..Default::default()
        };

        let json = log_to_json(&log);
        assert_eq!(json["host"], json!("server1"));
    }

    #[test]
    fn test_event_wrapper_log() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("msg".to_string(), make_string_value("test"));

        let wrapper = event::EventWrapper {
            event: Some(event::event_wrapper::Event::Log(event::Log {
                fields: std::collections::HashMap::new(),
                value: Some(make_map_value(fields)),
                ..Default::default()
            })),
        };

        let bytes = event_wrapper_to_json(&wrapper).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["msg"], json!("test"));
    }

    #[test]
    fn test_event_wrapper_metric() {
        let wrapper = event::EventWrapper {
            event: Some(event::event_wrapper::Event::Metric(event::Metric {
                name: "cpu_usage".to_string(),
                namespace: "system".to_string(),
                kind: event::metric::Kind::Absolute as i32,
                interval_ms: 10_000,
                value: Some(event::metric::Value::Gauge(event::Gauge { value: 0.25 })),
                ..Default::default()
            })),
        };

        let bytes = event_wrapper_to_json(&wrapper).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["_vector_type"], json!("metric"));
        assert_eq!(json["name"], json!("cpu_usage"));
        assert_eq!(json["namespace"], json!("system"));
        assert_eq!(json["kind"], json!("absolute"));
        assert_eq!(json["interval_ms"], json!(10_000));
        assert_eq!(json["gauge"], json!({"value": 0.25}));
    }

    fn metric_json(value: Option<event::metric::Value>) -> Result<serde_json::Value> {
        let wrapper = event::EventWrapper {
            event: Some(event::event_wrapper::Event::Metric(event::Metric {
                name: "m".to_string(),
                value,
                ..Default::default()
            })),
        };
        event_wrapper_to_json(&wrapper).map(|bytes| serde_json::from_slice(&bytes).unwrap())
    }

    /// Each metric value variant carries its measurement into the record, the
    /// legacy and current encodings of one kind under the same key, and a
    /// metric with no value is refused rather than forwarded as a name alone.
    #[test]
    fn a_metric_carries_its_value() {
        use event::metric::Value as V;
        let bucket = |upper_limit, count| event::HistogramBucket { upper_limit, count };
        let cases: [(&str, Option<V>, &str, serde_json::Value); 9] = [
            (
                "counter",
                Some(V::Counter(event::Counter { value: 3.0 })),
                "counter",
                json!({"value": 3.0}),
            ),
            (
                "gauge",
                Some(V::Gauge(event::Gauge { value: -1.5 })),
                "gauge",
                json!({"value": -1.5}),
            ),
            (
                "set",
                Some(V::Set(event::Set {
                    values: vec!["a".into(), "b".into()],
                })),
                "set",
                json!({"values": ["a", "b"]}),
            ),
            (
                "distribution, legacy encoding",
                Some(V::Distribution1(event::Distribution1 {
                    values: vec![1.0, 2.0],
                    sample_rates: vec![1, 4],
                    statistic: event::StatisticKind::Summary as i32,
                })),
                "distribution",
                json!({"samples": [{"value": 1.0, "rate": 1}, {"value": 2.0, "rate": 4}], "statistic": "summary"}),
            ),
            (
                "distribution",
                Some(V::Distribution2(event::Distribution2 {
                    samples: vec![event::DistributionSample {
                        value: 1.0,
                        rate: 2,
                    }],
                    statistic: event::StatisticKind::Histogram as i32,
                })),
                "distribution",
                json!({"samples": [{"value": 1.0, "rate": 2}], "statistic": "histogram"}),
            ),
            (
                "aggregated histogram, legacy encoding",
                Some(V::AggregatedHistogram1(event::AggregatedHistogram1 {
                    buckets: vec![0.5, 1.0],
                    counts: vec![2, 3],
                    count: 5,
                    sum: 3.5,
                })),
                "aggregated_histogram",
                json!({"buckets": [{"upper_limit": 0.5, "count": 2}, {"upper_limit": 1.0, "count": 3}], "count": 5, "sum": 3.5}),
            ),
            (
                "aggregated histogram",
                Some(V::AggregatedHistogram2(event::AggregatedHistogram2 {
                    buckets: vec![bucket(1.0, 7)],
                    count: 7,
                    sum: 2.0,
                })),
                "aggregated_histogram",
                json!({"buckets": [{"upper_limit": 1.0, "count": 7}], "count": 7, "sum": 2.0}),
            ),
            (
                "aggregated summary",
                Some(V::AggregatedSummary3(event::AggregatedSummary3 {
                    quantiles: vec![event::SummaryQuantile {
                        quantile: 0.99,
                        value: 9.0,
                    }],
                    count: 10,
                    sum: 40.0,
                })),
                "aggregated_summary",
                json!({"quantiles": [{"quantile": 0.99, "value": 9.0}], "count": 10, "sum": 40.0}),
            ),
            (
                "sketch",
                Some(V::Sketch(event::Sketch {
                    sketch: Some(event::sketch::Sketch::AgentDdSketch(
                        event::sketch::AgentDdSketch {
                            count: 2,
                            min: 1.0,
                            max: 3.0,
                            sum: 4.0,
                            avg: 2.0,
                            k: vec![-1, 4],
                            n: vec![1, 1],
                        },
                    )),
                })),
                "sketch",
                json!({"agent_dd_sketch": {"count": 2, "min": 1.0, "max": 3.0, "sum": 4.0, "avg": 2.0, "k": [-1, 4], "n": [1, 1]}}),
            ),
        ];
        for (name, value, key, want) in cases {
            let json = metric_json(value).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(json[key], want, "{name}: {json}");
        }

        let err = metric_json(None).expect_err("a metric with no value");
        assert!(matches!(err, Error::Validation(_)), "{err:?}");
    }

    /// `tags_v2` carries every value of a multi-valued tag and a bare tag as
    /// null, and wins over `tags_v1`, which holds one value per tag.
    #[test]
    fn a_metric_carries_its_v2_tags() {
        let tag = |values: &[Option<&str>]| event::TagValues {
            values: values
                .iter()
                .map(|v| event::TagValue {
                    value: v.map(str::to_string),
                })
                .collect(),
        };
        let wrapper = event::EventWrapper {
            event: Some(event::event_wrapper::Event::Metric(event::Metric {
                name: "m".to_string(),
                tags_v1: [("host".to_string(), "b".to_string())].into(),
                tags_v2: [
                    ("host".to_string(), tag(&[Some("a"), Some("b")])),
                    ("env".to_string(), tag(&[Some("prod")])),
                    ("bare".to_string(), tag(&[None])),
                ]
                .into(),
                value: Some(event::metric::Value::Counter(event::Counter { value: 1.0 })),
                ..Default::default()
            })),
        };
        let bytes = event_wrapper_to_json(&wrapper).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            json["tags"],
            json!({"host": ["a", "b"], "env": "prod", "bare": null})
        );
    }

    #[test]
    fn test_event_wrapper_empty() {
        let wrapper = event::EventWrapper { event: None };
        let result = event_wrapper_to_json(&wrapper);
        assert!(result.is_err());
    }
}
