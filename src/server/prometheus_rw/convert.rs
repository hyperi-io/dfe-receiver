// Project:   dfe-receiver
// File:      src/server/prometheus_rw/convert.rs
// Purpose:   Prometheus Remote Write protobuf to JSON conversion
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Converts Prometheus Remote Write v1 `WriteRequest` protobuf into
//! pipeline-ready JSON events.
//!
//! Supports three output modes:
//!
//! - **`native`** (default): Flat JSON with labels as top-level fields.
//!   Each `TimeSeries` + `Sample` pair produces one JSON event.
//!
//! - **`otel`**: Generic OTel JSON envelope with snake_case fields,
//!   RFC 3339 timestamps, and native JSON types.
//!
//! - **`hyperdx`**: HyperDX ClickHouse-compatible JSON with PascalCase
//!   fields and DateTime64 timestamps.

use std::collections::HashMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::config::RawCapture;
use crate::error::{Error, Result};
use crate::server::raw_capture;

use super::proto;

// ---------------------------------------------------------------------------
// Output mode
// ---------------------------------------------------------------------------

/// Prometheus Remote Write output mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PrometheusRwMode {
    /// Flat JSON with labels as top-level fields (default).
    #[default]
    Native,
    /// Generic OTel JSON envelope (matches OTLP generic mode).
    OTel,
    /// HyperDX ClickHouse-compatible JSON (matches OTLP HyperDX mode).
    HyperDx,
}

impl PrometheusRwMode {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "otel" | "generic" => Self::OTel,
            "hyperdx" => Self::HyperDx,
            _ => Self::Native,
        }
    }
}

// ---------------------------------------------------------------------------
// Timestamp helpers
// ---------------------------------------------------------------------------

/// Convert epoch milliseconds to RFC 3339 timestamp string (millisecond precision).
/// Used by native mode.
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

/// Convert epoch milliseconds to RFC 3339 with nanosecond precision.
/// Used by OTel generic mode to match OTLP convention.
fn epoch_ms_to_rfc3339_nanos(epoch_ms: i64) -> String {
    let secs = epoch_ms / 1000;
    let nanos = ((epoch_ms % 1000) * 1_000_000) as u32;
    match DateTime::from_timestamp(secs, nanos) {
        Some(dt) => dt
            .with_timezone(&Utc)
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        None => epoch_ms.to_string(),
    }
}

/// Convert epoch milliseconds to ClickHouse DateTime64(9) format.
/// Used by HyperDX mode. Format: "2024-03-04 08:13:20.000000000"
fn epoch_ms_to_ch_datetime(epoch_ms: i64) -> String {
    let secs = epoch_ms / 1000;
    let nanos = ((epoch_ms % 1000) * 1_000_000) as u32;
    match DateTime::from_timestamp(secs, nanos) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
        None => epoch_ms.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Conversion
// ---------------------------------------------------------------------------

/// Convert a decoded `WriteRequest` into a vector of JSON `Bytes` for the pipeline.
pub fn write_request_to_json(
    request: proto::WriteRequest,
    mode: PrometheusRwMode,
    raw_capture: RawCapture,
) -> Result<Vec<Bytes>> {
    match mode {
        PrometheusRwMode::Native => convert_native(request, raw_capture),
        PrometheusRwMode::OTel | PrometheusRwMode::HyperDx => {
            convert_otel(&request, mode, raw_capture)
        }
    }
}

// ---------------------------------------------------------------------------
// Native mode (existing behaviour)
// ---------------------------------------------------------------------------

/// Labels of a series as flat JSON fields, plus the `_source` routing tag.
///
/// This is the native shape's base, and also what `_raw` carries in the
/// otel and hyperdx modes -- native is the least-shaped decode we produce.
fn native_labels(ts: &proto::TimeSeries) -> serde_json::Map<String, serde_json::Value> {
    let mut labels = serde_json::Map::with_capacity(ts.labels.len() + 3);
    for label in &ts.labels {
        labels.insert(
            label.name.clone(),
            serde_json::Value::String(label.value.clone()),
        );
    }
    labels
        .entry("_source")
        .or_insert_with(|| serde_json::Value::String("prometheus".to_string()));
    labels
}

/// One native-shape sample event: the series labels plus value and timestamp.
fn native_sample_obj(
    labels: &serde_json::Map<String, serde_json::Value>,
    value: f64,
    timestamp: i64,
) -> serde_json::Map<String, serde_json::Value> {
    let mut obj = labels.clone();
    obj.insert("value".to_string(), serde_json::json!(value));
    obj.insert(
        "timestamp".to_string(),
        serde_json::Value::String(epoch_ms_to_rfc3339(timestamp)),
    );
    obj
}

/// Native conversion: flat JSON with labels as top-level fields.
fn convert_native(request: proto::WriteRequest, raw_capture: RawCapture) -> Result<Vec<Bytes>> {
    let mut events = Vec::new();

    for ts in request.timeseries {
        let labels = native_labels(&ts);

        // Emit one event per sample
        for sample in &ts.samples {
            let mut obj = native_sample_obj(&labels, sample.value, sample.timestamp);
            attach_native_raw(&mut obj, raw_capture)?;

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

            attach_native_raw(&mut obj, raw_capture)?;

            let json = serde_json::to_vec(&obj)
                .map_err(|e| Error::Validation(format!("JSON serialisation failed: {e}")))?;
            events.push(Bytes::from(json));
        }
    }

    Ok(events)
}

/// Attach `_raw` to a native-shape event.
///
/// In native mode the event already IS the verbatim decode, so `_raw` is a
/// copy of it. That is documented on `prometheus_rw.raw_capture` and warned
/// about at startup; the field is still emitted so downstream sees the same
/// schema whichever mode the receiver runs in.
fn attach_native_raw(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    raw_capture: RawCapture,
) -> Result<()> {
    let prepared = raw_capture::prepare_serialised(obj, raw_capture)
        .map_err(|e| Error::Validation(format!("Prometheus raw capture failed: {e}")))?;
    if let Some(prepared) = prepared {
        raw_capture::attach_prepared_to_map(obj, prepared);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// OTel mode (generic + hyperdx)
// ---------------------------------------------------------------------------

/// OTel conversion: structured JSON matching OTLP handler output.
fn convert_otel(
    request: &proto::WriteRequest,
    mode: PrometheusRwMode,
    raw_capture: RawCapture,
) -> Result<Vec<Bytes>> {
    // Build metadata lookup for metric type determination
    // MetricType::COUNTER (1) → "sum", everything else → "gauge"
    let metadata: HashMap<&str, i32> = request
        .metadata
        .iter()
        .map(|m| (m.metric_family_name.as_str(), m.r#type))
        .collect();

    let mut events = Vec::new();
    let empty_resource = serde_json::Map::new();

    for ts in &request.timeseries {
        // Extract __name__ label as metric name; remaining labels are attributes
        let mut metric_name = "";
        let mut attrs = serde_json::Map::with_capacity(ts.labels.len());

        for label in &ts.labels {
            if label.name == "__name__" {
                metric_name = &label.value;
            } else {
                attrs.insert(
                    label.name.clone(),
                    serde_json::Value::String(label.value.clone()),
                );
            }
        }

        // Determine OTel metric type from metadata
        let metric_type = match metadata.get(metric_name) {
            Some(&1) => "sum", // COUNTER
            _ => "gauge",
        };

        // The native rendering is what _raw carries here, so build the label
        // base once per series rather than per sample.
        let raw_labels = if raw_capture.enabled {
            Some(native_labels(ts))
        } else {
            None
        };

        // Emit one event per sample
        for sample in &ts.samples {
            let mut json_value = match mode {
                PrometheusRwMode::HyperDx => serde_json::json!({
                    "TimeUnix": epoch_ms_to_ch_datetime(sample.timestamp),
                    "MetricName": metric_name,
                    "Value": sample.value,
                    "Attributes": attrs,
                    "ResourceAttributes": empty_resource,
                    "_otel_metric_type": metric_type,
                }),
                _ => serde_json::json!({
                    "_signal": "metric",
                    "_timestamp": epoch_ms_to_rfc3339_nanos(sample.timestamp),
                    "metric_name": metric_name,
                    "metric_type": metric_type,
                    "value": sample.value,
                    "attributes": attrs,
                    "resource": empty_resource,
                }),
            };

            if let (Some(labels), Some(obj)) = (&raw_labels, json_value.as_object_mut()) {
                let native = native_sample_obj(labels, sample.value, sample.timestamp);
                let prepared =
                    raw_capture::prepare_serialised(&native, raw_capture).map_err(|e| {
                        Error::Validation(format!("Prometheus raw capture failed: {e}"))
                    })?;
                if let Some(prepared) = prepared {
                    raw_capture::attach_prepared_to_map(obj, prepared);
                }
            }

            let json = serde_json::to_vec(&json_value)
                .map_err(|e| Error::Validation(format!("JSON serialisation failed: {e}")))?;
            events.push(Bytes::from(json));
        }
    }

    Ok(events)
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

    // -----------------------------------------------------------------------
    // Mode parsing
    // -----------------------------------------------------------------------

    #[test]
    fn test_mode_from_str() {
        assert_eq!(
            PrometheusRwMode::from_str("native"),
            PrometheusRwMode::Native
        );
        assert_eq!(PrometheusRwMode::from_str("otel"), PrometheusRwMode::OTel);
        assert_eq!(
            PrometheusRwMode::from_str("generic"),
            PrometheusRwMode::OTel
        );
        assert_eq!(
            PrometheusRwMode::from_str("hyperdx"),
            PrometheusRwMode::HyperDx
        );
        assert_eq!(
            PrometheusRwMode::from_str("HYPERDX"),
            PrometheusRwMode::HyperDx
        );
        assert_eq!(
            PrometheusRwMode::from_str("unknown"),
            PrometheusRwMode::Native
        );
    }

    // -----------------------------------------------------------------------
    // Timestamp helpers
    // -----------------------------------------------------------------------

    #[test]
    fn test_timestamp_conversion() {
        let ts = epoch_ms_to_rfc3339(1_709_540_000_000);
        assert_eq!(ts, "2024-03-04T08:13:20.000Z");
    }

    #[test]
    fn test_timestamp_with_millis() {
        let ts = epoch_ms_to_rfc3339(1_709_540_000_123);
        assert_eq!(ts, "2024-03-04T08:13:20.123Z");
    }

    #[test]
    fn test_timestamp_rfc3339_nanos() {
        let ts = epoch_ms_to_rfc3339_nanos(1_709_540_000_123);
        assert_eq!(ts, "2024-03-04T08:13:20.123000000Z");
    }

    #[test]
    fn test_timestamp_ch_datetime() {
        let ts = epoch_ms_to_ch_datetime(1_709_540_000_123);
        assert_eq!(ts, "2024-03-04 08:13:20.123000000");
    }

    // -----------------------------------------------------------------------
    // Native mode tests
    // -----------------------------------------------------------------------

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

        let events =
            write_request_to_json(request, PrometheusRwMode::Native, RawCapture::OFF).unwrap();
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

        let events =
            write_request_to_json(request, PrometheusRwMode::Native, RawCapture::OFF).unwrap();
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

        let events =
            write_request_to_json(request, PrometheusRwMode::Native, RawCapture::OFF).unwrap();
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

        let events =
            write_request_to_json(request, PrometheusRwMode::Native, RawCapture::OFF).unwrap();
        assert_eq!(events, [] as [bytes::Bytes; 0]);
    }

    #[test]
    fn test_empty_request() {
        let request = proto::WriteRequest {
            timeseries: vec![],
            metadata: vec![],
        };

        let events =
            write_request_to_json(request, PrometheusRwMode::Native, RawCapture::OFF).unwrap();
        assert_eq!(events, [] as [bytes::Bytes; 0]);
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

        let events =
            write_request_to_json(request, PrometheusRwMode::Native, RawCapture::OFF).unwrap();
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

        let events =
            write_request_to_json(request, PrometheusRwMode::Native, RawCapture::OFF).unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        assert_eq!(obj["_source"], "custom");
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

        let events =
            write_request_to_json(request, PrometheusRwMode::Native, RawCapture::OFF).unwrap();
        // 1 sample + 1 exemplar = 2 events
        assert_eq!(events.len(), 2);

        let exemplar: serde_json::Value = serde_json::from_slice(&events[1]).unwrap();
        assert_eq!(exemplar["_type"], "exemplar");
        assert_eq!(exemplar["exemplar_value"], 0.95);
        assert_eq!(exemplar["exemplar_labels"]["trace_id"], "abc123");
    }

    // -----------------------------------------------------------------------
    // OTel generic mode tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_otel_generic_single_sample() {
        let request = proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![
                    make_label("__name__", "http_requests_total"),
                    make_label("job", "api-server"),
                    make_label("method", "GET"),
                ],
                vec![make_sample(42.0, 1709540000000)],
            )],
            metadata: vec![],
        };

        let events =
            write_request_to_json(request, PrometheusRwMode::OTel, RawCapture::OFF).unwrap();
        assert_eq!(events.len(), 1);

        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        assert_eq!(obj["_signal"], "metric");
        assert_eq!(obj["metric_name"], "http_requests_total");
        assert_eq!(obj["metric_type"], "gauge");
        assert_eq!(obj["value"], 42.0);
        assert_eq!(obj["attributes"]["job"], "api-server");
        assert_eq!(obj["attributes"]["method"], "GET");
        // __name__ should NOT appear in attributes
        assert!(obj["attributes"]["__name__"].is_null());
        // Timestamp should be RFC 3339 with nanos
        let ts = obj["_timestamp"].as_str().unwrap();
        assert!(ts.ends_with('Z'));
        assert!(ts.contains("000000000Z"));
        // resource should be empty object
        assert!(obj["resource"].is_object());
    }

    #[test]
    fn test_otel_generic_empty_request() {
        let request = proto::WriteRequest {
            timeseries: vec![],
            metadata: vec![],
        };

        let events =
            write_request_to_json(request, PrometheusRwMode::OTel, RawCapture::OFF).unwrap();
        assert_eq!(events, [] as [bytes::Bytes; 0]);
    }

    // -----------------------------------------------------------------------
    // HyperDX mode tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_hyperdx_single_sample() {
        let request = proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![
                    make_label("__name__", "cpu_usage"),
                    make_label("host", "server-1"),
                ],
                vec![make_sample(0.75, 1709540000123)],
            )],
            metadata: vec![],
        };

        let events =
            write_request_to_json(request, PrometheusRwMode::HyperDx, RawCapture::OFF).unwrap();
        assert_eq!(events.len(), 1);

        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        assert_eq!(obj["MetricName"], "cpu_usage");
        assert_eq!(obj["Value"], 0.75);
        assert_eq!(obj["_otel_metric_type"], "gauge");
        assert_eq!(obj["Attributes"]["host"], "server-1");
        // __name__ should NOT appear in Attributes
        assert!(obj["Attributes"]["__name__"].is_null());
        // Timestamp should be ClickHouse DateTime64 format
        let ts = obj["TimeUnix"].as_str().unwrap();
        assert!(ts.contains("08:13:20.123000000"));
        // ResourceAttributes should be empty object
        assert!(obj["ResourceAttributes"].is_object());
    }

    // -----------------------------------------------------------------------
    // Metric type from metadata
    // -----------------------------------------------------------------------

    #[test]
    fn test_otel_counter_metadata() {
        let request = proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![
                    make_label("__name__", "http_requests_total"),
                    make_label("method", "GET"),
                ],
                vec![make_sample(100.0, 1709540000000)],
            )],
            metadata: vec![proto::MetricMetadata {
                r#type: 1, // COUNTER
                metric_family_name: "http_requests_total".to_string(),
                help: String::new(),
                unit: String::new(),
            }],
        };

        let events =
            write_request_to_json(request, PrometheusRwMode::OTel, RawCapture::OFF).unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        assert_eq!(obj["metric_type"], "sum");
    }

    #[test]
    fn test_otel_no_metadata_defaults_gauge() {
        let request = proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![make_label("__name__", "temperature")],
                vec![make_sample(23.5, 1709540000000)],
            )],
            metadata: vec![],
        };

        let events =
            write_request_to_json(request, PrometheusRwMode::OTel, RawCapture::OFF).unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        assert_eq!(obj["metric_type"], "gauge");
    }

    #[test]
    fn test_hyperdx_counter_metadata() {
        let request = proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![make_label("__name__", "bytes_sent_total")],
                vec![make_sample(999.0, 1709540000000)],
            )],
            metadata: vec![proto::MetricMetadata {
                r#type: 1, // COUNTER
                metric_family_name: "bytes_sent_total".to_string(),
                help: String::new(),
                unit: String::new(),
            }],
        };

        let events =
            write_request_to_json(request, PrometheusRwMode::HyperDx, RawCapture::OFF).unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        assert_eq!(obj["_otel_metric_type"], "sum");
    }

    #[test]
    fn test_otel_metric_name_extraction() {
        let request = proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![
                    make_label("__name__", "process_cpu_seconds_total"),
                    make_label("instance", "localhost:9090"),
                ],
                vec![make_sample(1234.5, 1709540000000)],
            )],
            metadata: vec![],
        };

        let events =
            write_request_to_json(request, PrometheusRwMode::OTel, RawCapture::OFF).unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();

        // __name__ becomes metric_name, not an attribute
        assert_eq!(obj["metric_name"], "process_cpu_seconds_total");
        assert!(obj["attributes"]["__name__"].is_null());
        assert_eq!(obj["attributes"]["instance"], "localhost:9090");
    }

    #[test]
    fn test_otel_multiple_samples() {
        let request = proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![make_label("__name__", "temp"), make_label("sensor", "a")],
                vec![
                    make_sample(20.0, 1709540000000),
                    make_sample(21.0, 1709540001000),
                ],
            )],
            metadata: vec![],
        };

        let events =
            write_request_to_json(request, PrometheusRwMode::OTel, RawCapture::OFF).unwrap();
        assert_eq!(events.len(), 2);

        let obj0: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        let obj1: serde_json::Value = serde_json::from_slice(&events[1]).unwrap();
        assert_eq!(obj0["value"], 20.0);
        assert_eq!(obj1["value"], 21.0);
        assert_eq!(obj0["metric_name"], "temp");
        assert_eq!(obj1["metric_name"], "temp");
    }

    // -----------------------------------------------------------------------
    // Raw capture
    // -----------------------------------------------------------------------

    fn one_sample_request() -> proto::WriteRequest {
        proto::WriteRequest {
            timeseries: vec![make_timeseries(
                vec![
                    make_label("__name__", "http_requests_total"),
                    make_label("job", "api"),
                ],
                vec![make_sample(42.0, 1_709_540_000_000)],
            )],
            metadata: vec![],
        }
    }

    #[test]
    fn capture_off_emits_no_raw_field() {
        let events = write_request_to_json(
            one_sample_request(),
            PrometheusRwMode::Native,
            RawCapture::OFF,
        )
        .unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        assert!(obj.get("_raw").is_none());
    }

    #[test]
    fn hyperdx_capture_carries_the_native_rendering() {
        let events = write_request_to_json(
            one_sample_request(),
            PrometheusRwMode::HyperDx,
            RawCapture::on(),
        )
        .unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();

        // The HyperDX event is shaped as before.
        assert_eq!(obj["MetricName"], "http_requests_total");
        assert_eq!(obj["Value"], 42.0);

        // _raw is the native decode of the same sample -- flat labels.
        let captured: serde_json::Value =
            serde_json::from_str(obj["_raw"].as_str().unwrap()).unwrap();
        assert_eq!(captured["__name__"], "http_requests_total");
        assert_eq!(captured["job"], "api");
        assert_eq!(captured["value"], 42.0);
        assert_eq!(captured["_source"], "prometheus");
    }

    #[test]
    fn otel_capture_carries_the_native_rendering() {
        let events = write_request_to_json(
            one_sample_request(),
            PrometheusRwMode::OTel,
            RawCapture::on(),
        )
        .unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();

        assert_eq!(obj["metric_name"], "http_requests_total");
        let captured: serde_json::Value =
            serde_json::from_str(obj["_raw"].as_str().unwrap()).unwrap();
        assert_eq!(captured["__name__"], "http_requests_total");
        assert_eq!(captured["value"], 42.0);
    }

    #[test]
    fn native_capture_emits_raw_even_though_it_duplicates() {
        // Documented behaviour, warned about at startup: the schema stays
        // the same whichever mode the receiver runs in.
        let events = write_request_to_json(
            one_sample_request(),
            PrometheusRwMode::Native,
            RawCapture::on(),
        )
        .unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();

        let captured: serde_json::Value =
            serde_json::from_str(obj["_raw"].as_str().unwrap()).unwrap();
        assert_eq!(captured["value"], obj["value"]);
        assert_eq!(captured["job"], obj["job"]);
        // The copy does not contain itself.
        assert!(captured.get("_raw").is_none());
    }
}
