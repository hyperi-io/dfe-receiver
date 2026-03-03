// Project:   dfe-receiver
// File:      src/server/prometheus_rw/convert.rs
// Purpose:   Prometheus Remote Write protobuf to JSON conversion
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Converts Prometheus Remote Write v1 `WriteRequest` protobuf into
//! pipeline-ready JSON events.
//!
//! Each `TimeSeries` + `Sample` pair produces one JSON event with labels
//! flattened to top-level fields.

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::error::{Error, Result};

use super::proto;

/// Convert a decoded `WriteRequest` into a vector of JSON `Bytes` for the pipeline.
///
/// Each `TimeSeries` with N samples produces N JSON events. Labels are flattened
/// to top-level fields. `_source` is set to `"prometheus"` for routing.
pub fn write_request_to_json(request: proto::WriteRequest) -> Result<Vec<Bytes>> {
    let mut events = Vec::new();

    for ts in request.timeseries {
        // Extract labels into a reusable map
        let mut labels = serde_json::Map::with_capacity(ts.labels.len() + 3);
        for label in &ts.labels {
            labels.insert(
                label.name.clone(),
                serde_json::Value::String(label.value.clone()),
            );
        }

        // Always set _source for routing
        labels
            .entry("_source")
            .or_insert_with(|| serde_json::Value::String("prometheus".to_string()));

        // Emit one event per sample
        for sample in &ts.samples {
            let mut obj = labels.clone();
            obj.insert("value".to_string(), serde_json::json!(sample.value));
            obj.insert(
                "timestamp".to_string(),
                serde_json::Value::String(epoch_ms_to_rfc3339(sample.timestamp)),
            );

            let json = serde_json::to_vec(&obj)
                .map_err(|e| Error::Validation(format!("JSON serialisation failed: {e}")))?;
            events.push(Bytes::from(json));
        }

        // Emit one event per exemplar
        for exemplar in &ts.exemplars {
            let mut obj = labels.clone();
            obj.insert(
                "exemplar_value".to_string(),
                serde_json::json!(exemplar.value),
            );
            obj.insert(
                "timestamp".to_string(),
                serde_json::Value::String(epoch_ms_to_rfc3339(exemplar.timestamp)),
            );
            obj.insert("_type".to_string(), serde_json::json!("exemplar"));

            // Include exemplar labels
            if !exemplar.labels.is_empty() {
                let exemplar_labels: serde_json::Map<String, serde_json::Value> = exemplar
                    .labels
                    .iter()
                    .map(|l| (l.name.clone(), serde_json::Value::String(l.value.clone())))
                    .collect();
                obj.insert(
                    "exemplar_labels".to_string(),
                    serde_json::Value::Object(exemplar_labels),
                );
            }

            let json = serde_json::to_vec(&obj)
                .map_err(|e| Error::Validation(format!("JSON serialisation failed: {e}")))?;
            events.push(Bytes::from(json));
        }
    }

    Ok(events)
}

/// Convert epoch milliseconds to RFC 3339 timestamp string.
fn epoch_ms_to_rfc3339(epoch_ms: i64) -> String {
    let secs = epoch_ms / 1000;
    let nanos = ((epoch_ms % 1000) * 1_000_000) as u32;
    match DateTime::from_timestamp(secs, nanos) {
        Some(dt) => dt
            .with_timezone(&Utc)
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        None => epoch_ms.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn make_label(name: &str, value: &str) -> proto::Label {
        proto::Label {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn make_sample(value: f64, timestamp: i64) -> proto::Sample {
        proto::Sample { value, timestamp }
    }

    fn make_timeseries(
        labels: Vec<proto::Label>,
        samples: Vec<proto::Sample>,
    ) -> proto::TimeSeries {
        proto::TimeSeries {
            labels,
            samples,
            exemplars: vec![],
            histograms: vec![],
        }
    }

    #[test]
    fn test_single_timeseries_single_sample() {
        let request = proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![
                    make_label("__name__", "http_requests_total"),
                    make_label("method", "GET"),
                ],
                vec![make_sample(42.0, 1709540000000)],
            )],
            metadata: vec![],
        };

        let events = write_request_to_json(request).unwrap();
        assert_eq!(events.len(), 1);

        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        assert_eq!(obj["__name__"], "http_requests_total");
        assert_eq!(obj["method"], "GET");
        assert_eq!(obj["value"], 42.0);
        assert_eq!(obj["_source"], "prometheus");
        // Timestamp should be RFC 3339
        let ts = obj["timestamp"].as_str().unwrap();
        assert!(ts.ends_with('Z'), "timestamp should be UTC: {ts}");
    }

    #[test]
    fn test_multiple_samples_per_timeseries() {
        let request = proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![make_label("__name__", "cpu_usage")],
                vec![
                    make_sample(0.5, 1709540000000),
                    make_sample(0.7, 1709540001000),
                    make_sample(0.3, 1709540002000),
                ],
            )],
            metadata: vec![],
        };

        let events = write_request_to_json(request).unwrap();
        assert_eq!(events.len(), 3);

        let obj0: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        let obj1: serde_json::Value = serde_json::from_slice(&events[1]).unwrap();
        let obj2: serde_json::Value = serde_json::from_slice(&events[2]).unwrap();
        assert_eq!(obj0["value"], 0.5);
        assert_eq!(obj1["value"], 0.7);
        assert_eq!(obj2["value"], 0.3);
    }

    #[test]
    fn test_multiple_timeseries() {
        let request = proto::WriteRequest {
            timeseries: vec![
                make_timeseries(
                    vec![make_label("__name__", "metric_a")],
                    vec![make_sample(1.0, 1709540000000)],
                ),
                make_timeseries(
                    vec![make_label("__name__", "metric_b")],
                    vec![make_sample(2.0, 1709540000000)],
                ),
            ],
            metadata: vec![],
        };

        let events = write_request_to_json(request).unwrap();
        assert_eq!(events.len(), 2);

        let obj0: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        let obj1: serde_json::Value = serde_json::from_slice(&events[1]).unwrap();
        assert_eq!(obj0["__name__"], "metric_a");
        assert_eq!(obj1["__name__"], "metric_b");
    }

    #[test]
    fn test_empty_timeseries_no_samples() {
        let request = proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![make_label("__name__", "no_samples")],
                vec![],
            )],
            metadata: vec![],
        };

        let events = write_request_to_json(request).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_empty_request() {
        let request = proto::WriteRequest {
            timeseries: vec![],
            metadata: vec![],
        };

        let events = write_request_to_json(request).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_source_field_always_set() {
        let request = proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![make_label("__name__", "test")],
                vec![make_sample(1.0, 1709540000000)],
            )],
            metadata: vec![],
        };

        let events = write_request_to_json(request).unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        assert_eq!(obj["_source"], "prometheus");
    }

    #[test]
    fn test_source_not_overwritten_if_present() {
        let request = proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![
                    make_label("__name__", "test"),
                    make_label("_source", "custom"),
                ],
                vec![make_sample(1.0, 1709540000000)],
            )],
            metadata: vec![],
        };

        let events = write_request_to_json(request).unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        assert_eq!(obj["_source"], "custom");
    }

    #[test]
    fn test_timestamp_conversion() {
        let ts = epoch_ms_to_rfc3339(1709540000000);
        assert_eq!(ts, "2024-03-04T08:13:20.000Z");
    }

    #[test]
    fn test_timestamp_with_millis() {
        let ts = epoch_ms_to_rfc3339(1709540000123);
        assert_eq!(ts, "2024-03-04T08:13:20.123Z");
    }

    #[test]
    fn test_exemplar_events() {
        let request = proto::WriteRequest {
            timeseries: vec![proto::TimeSeries {
                labels: vec![make_label("__name__", "request_duration")],
                samples: vec![make_sample(0.5, 1709540000000)],
                exemplars: vec![proto::Exemplar {
                    labels: vec![make_label("trace_id", "abc123")],
                    value: 0.95,
                    timestamp: 1709540000500,
                }],
                histograms: vec![],
            }],
            metadata: vec![],
        };

        let events = write_request_to_json(request).unwrap();
        // 1 sample + 1 exemplar = 2 events
        assert_eq!(events.len(), 2);

        let exemplar: serde_json::Value = serde_json::from_slice(&events[1]).unwrap();
        assert_eq!(exemplar["_type"], "exemplar");
        assert_eq!(exemplar["exemplar_value"], 0.95);
        assert_eq!(exemplar["exemplar_labels"]["trace_id"], "abc123");
    }
}
