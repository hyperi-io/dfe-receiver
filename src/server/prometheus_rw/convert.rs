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
//!   Each `TimeSeries` + `Sample` pair produces one JSON event, and so does
//!   each exemplar and each native histogram.
//!
//! - **`otel`**: Generic OTel JSON envelope with snake_case fields,
//!   RFC 3339 timestamps, and native JSON types.
//!
//! - **`hyperdx`**: HyperDX ClickHouse-compatible JSON with PascalCase
//!   fields and DateTime64 timestamps.

use std::collections::HashMap;

use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use scalo::logger::log_debounced;
use tracing::warn;

use crate::error::{Error, Result};

use super::proto;

/// The `_source` this listener stamps on a series that carries no `_source` label.
pub const SOURCE: &str = "prometheus";

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
// Native histograms and exemplars
// ---------------------------------------------------------------------------

/// Buckets one native histogram may expand to, across both sides of zero. A
/// histogram past it is refused as malformed: Prometheus emits at most 160 a
/// side by default, and a span offset could otherwise ask for billions.
const MAX_HISTOGRAM_BUCKETS: usize = 4096;

/// The schema a native histogram with custom bucket bounds carries.
const CUSTOM_BUCKETS_SCHEMA: i32 = -53;

/// The exponential schemas, which OTel calls the scale.
const EXPONENTIAL_SCHEMAS: std::ops::RangeInclusive<i32> = -4..=8;

/// Shortest gap between two log lines about what an OTel record cannot hold.
const UNRECORDED_LOG_INTERVAL_MS: u64 = 5_000;

/// When the listener last logged what an OTel record cannot hold.
static UNRECORDED_LOGGED: AtomicU64 = AtomicU64::new(0);

/// One side of a native histogram: the Prometheus index of its first bucket
/// and every bucket's count from there, gaps filled with zeros.
struct Buckets {
    offset: i32,
    counts: Vec<f64>,
}

impl Buckets {
    /// Expand `spans` over `deltas` (an integer histogram, each count the
    /// previous plus its delta) or `counts` (a float histogram), adding to
    /// `budget` the buckets it took.
    fn expand(
        spans: &[proto::BucketSpan],
        deltas: &[i64],
        counts: &[f64],
        float: bool,
        budget: &mut usize,
    ) -> Result<Self> {
        let mut values: Box<dyn Iterator<Item = f64> + '_> = if float {
            Box::new(counts.iter().copied())
        } else {
            Box::new(deltas.iter().scan(0_i64, |count, delta| {
                *count = count.saturating_add(*delta);
                Some(*count as f64)
            }))
        };
        let mut out = Vec::new();
        let mut offset = 0;
        for (index, span) in spans.iter().enumerate() {
            let gap = if index == 0 {
                offset = span.offset;
                0
            } else {
                usize::try_from(span.offset).map_err(|_| malformed("a negative span gap"))?
            };
            let taken = gap.saturating_add(span.length as usize);
            *budget = budget.saturating_add(taken);
            if *budget > MAX_HISTOGRAM_BUCKETS {
                return Err(malformed("more buckets than one histogram may hold"));
            }
            out.extend(std::iter::repeat_n(0.0, gap));
            for _ in 0..span.length {
                out.push(
                    values
                        .next()
                        .ok_or_else(|| malformed("fewer counts than its spans"))?,
                );
            }
        }
        Ok(Self {
            offset,
            counts: out,
        })
    }

    /// The counts as OTel's integers.
    fn whole(&self) -> Vec<u64> {
        self.counts
            .iter()
            .map(|count| count.max(0.0) as u64)
            .collect()
    }

    /// The OTel offset: OTel bucket `i` is Prometheus bucket `i + 1`.
    fn otel_offset(&self) -> i32 {
        if self.counts.is_empty() {
            0
        } else {
            self.offset - 1
        }
    }
}

fn malformed(why: &str) -> Error {
    Error::Validation(format!("remote write native histogram: {why}"))
}

/// One native histogram, decoded.
struct NativeHistogram {
    float: bool,
    count: f64,
    zero_count: f64,
    positive: Buckets,
    negative: Buckets,
}

impl NativeHistogram {
    fn decode(h: &proto::Histogram) -> Result<Self> {
        use proto::histogram::{Count, ZeroCount};
        let float = matches!(h.count, Some(Count::CountFloat(_)));
        let count = match h.count {
            Some(Count::CountInt(n)) => n as f64,
            Some(Count::CountFloat(n)) => n,
            None => 0.0,
        };
        let zero_count = match h.zero_count {
            Some(ZeroCount::ZeroCountInt(n)) => n as f64,
            Some(ZeroCount::ZeroCountFloat(n)) => n,
            None => 0.0,
        };
        let mut budget = 0;
        let positive = Buckets::expand(
            &h.positive_spans,
            &h.positive_deltas,
            &h.positive_counts,
            float,
            &mut budget,
        )?;
        let negative = Buckets::expand(
            &h.negative_spans,
            &h.negative_deltas,
            &h.negative_counts,
            float,
            &mut budget,
        )?;
        Ok(Self {
            float,
            count,
            zero_count,
            positive,
            negative,
        })
    }

    /// The explicit-bucket counts of a custom-bucket histogram, one per bound
    /// and one past the last.
    fn custom_counts(&self, bounds: usize) -> Result<Vec<u64>> {
        let mut counts = vec![0_u64; bounds + 1];
        let start = usize::try_from(self.positive.offset)
            .map_err(|_| malformed("a custom bucket before the first bound"))?;
        for (index, count) in self.positive.whole().into_iter().enumerate() {
            *counts
                .get_mut(start + index)
                .ok_or_else(|| malformed("a custom bucket past the last bound"))? = count;
        }
        Ok(counts)
    }
}

/// Count `n` of `kind` that an OTel-shaped record cannot hold.
fn count_unrecorded(kind: &'static str, n: usize) {
    if n == 0 {
        return;
    }
    metrics::counter!("receiver_prometheus_rw_unrecorded_total", "kind" => kind)
        .increment(n as u64);
    if log_debounced(&UNRECORDED_LOGGED, UNRECORDED_LOG_INTERVAL_MS) {
        warn!(
            kind,
            count = n,
            "Remote write content an OTel-shaped record cannot hold was not recorded; \
             mode: native records it"
        );
    }
}

/// A series' exemplars as the OTel modes carry them on a record.
fn otel_exemplars(
    exemplars: &[proto::Exemplar],
    mode: PrometheusRwMode,
) -> Vec<(String, serde_json::Value)> {
    let split = |exemplar: &proto::Exemplar| {
        let mut trace_id = String::new();
        let mut span_id = String::new();
        let mut attrs = serde_json::Map::new();
        for label in &exemplar.labels {
            match label.name.as_str() {
                "trace_id" => trace_id.clone_from(&label.value),
                "span_id" => span_id.clone_from(&label.value),
                _ => {
                    attrs.insert(label.name.clone(), label.value.clone().into());
                }
            }
        }
        (trace_id, span_id, attrs)
    };
    match mode {
        PrometheusRwMode::HyperDx => {
            let mut filtered = Vec::new();
            let mut times = Vec::new();
            let mut values = Vec::new();
            let mut span_ids = Vec::new();
            let mut trace_ids = Vec::new();
            for exemplar in exemplars {
                let (trace_id, span_id, attrs) = split(exemplar);
                filtered.push(serde_json::Value::Object(attrs));
                times.push(serde_json::json!(epoch_ms_to_ch_datetime(
                    exemplar.timestamp
                )));
                values.push(serde_json::json!(exemplar.value));
                span_ids.push(serde_json::json!(span_id));
                trace_ids.push(serde_json::json!(trace_id));
            }
            vec![
                ("Exemplars.FilteredAttributes".to_string(), filtered.into()),
                ("Exemplars.TimeUnix".to_string(), times.into()),
                ("Exemplars.Value".to_string(), values.into()),
                ("Exemplars.SpanId".to_string(), span_ids.into()),
                ("Exemplars.TraceId".to_string(), trace_ids.into()),
            ]
        }
        PrometheusRwMode::OTel | PrometheusRwMode::Native => {
            let list: Vec<serde_json::Value> = exemplars
                .iter()
                .map(|exemplar| {
                    let (trace_id, span_id, attrs) = split(exemplar);
                    serde_json::json!({
                        "timestamp": epoch_ms_to_rfc3339_nanos(exemplar.timestamp),
                        "value": exemplar.value,
                        "trace_id": trace_id,
                        "span_id": span_id,
                        "attributes": attrs,
                    })
                })
                .collect();
            vec![("exemplars".to_string(), list.into())]
        }
    }
}

// ---------------------------------------------------------------------------
// Conversion
// ---------------------------------------------------------------------------

/// Convert a decoded `WriteRequest` into a vector of JSON `Bytes` for the pipeline.
pub fn write_request_to_json(
    request: proto::WriteRequest,
    mode: PrometheusRwMode,
) -> Result<Vec<Bytes>> {
    match mode {
        PrometheusRwMode::Native => convert_native(request),
        PrometheusRwMode::OTel | PrometheusRwMode::HyperDx => convert_otel(&request, mode),
    }
}

// ---------------------------------------------------------------------------
// Native mode (existing behaviour)
// ---------------------------------------------------------------------------

/// Labels of a series as flat JSON fields, plus the `_source` routing tag.
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
        .or_insert_with(|| serde_json::Value::String(SOURCE.to_string()));
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
fn convert_native(request: proto::WriteRequest) -> Result<Vec<Bytes>> {
    let mut events = Vec::new();

    for ts in request.timeseries {
        let labels = native_labels(&ts);

        // Emit one event per sample
        for sample in &ts.samples {
            let obj = native_sample_obj(&labels, sample.value, sample.timestamp);

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

        // Emit one event per native histogram
        for histogram in &ts.histograms {
            let obj = native_histogram_obj(&labels, histogram)?;
            let json = serde_json::to_vec(&obj)
                .map_err(|e| Error::Validation(format!("JSON serialisation failed: {e}")))?;
            events.push(Bytes::from(json));
        }
    }

    Ok(events)
}

/// One native-shape histogram event: the series labels plus every field the
/// histogram carries, its buckets expanded to dense counts.
fn native_histogram_obj(
    labels: &serde_json::Map<String, serde_json::Value>,
    histogram: &proto::Histogram,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let decoded = NativeHistogram::decode(histogram)?;
    let mut obj = labels.clone();
    obj.insert("_type".to_string(), serde_json::json!("histogram"));
    obj.insert(
        "timestamp".to_string(),
        serde_json::Value::String(epoch_ms_to_rfc3339(histogram.timestamp)),
    );
    obj.insert("count".to_string(), serde_json::json!(decoded.count));
    obj.insert("sum".to_string(), serde_json::json!(histogram.sum));
    obj.insert("schema".to_string(), serde_json::json!(histogram.schema));
    obj.insert(
        "zero_threshold".to_string(),
        serde_json::json!(histogram.zero_threshold),
    );
    obj.insert(
        "zero_count".to_string(),
        serde_json::json!(decoded.zero_count),
    );
    for (side, buckets) in [
        ("positive", &decoded.positive),
        ("negative", &decoded.negative),
    ] {
        obj.insert(format!("{side}_offset"), serde_json::json!(buckets.offset));
        obj.insert(
            format!("{side}_bucket_counts"),
            serde_json::json!(buckets.counts),
        );
    }
    obj.insert(
        "reset_hint".to_string(),
        serde_json::json!(histogram.reset_hint().as_str_name()),
    );
    if !histogram.custom_values.is_empty() {
        obj.insert(
            "custom_values".to_string(),
            serde_json::json!(histogram.custom_values),
        );
    }
    Ok(obj)
}

// ---------------------------------------------------------------------------
// OTel mode (generic + hyperdx)
// ---------------------------------------------------------------------------

/// OTel conversion: structured JSON matching OTLP handler output.
fn convert_otel(request: &proto::WriteRequest, mode: PrometheusRwMode) -> Result<Vec<Bytes>> {
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

        let mut series_records: Vec<serde_json::Value> = Vec::new();

        // One record per sample
        for sample in &ts.samples {
            let json_value = match mode {
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
            series_records.push(json_value);
        }

        // One record per native histogram
        for histogram in &ts.histograms {
            if let Some(record) =
                otel_histogram(histogram, metric_name, &attrs, &empty_resource, mode)?
            {
                series_records.push(record);
            }
        }

        // The series' exemplars ride on its last record, or on none.
        if !ts.exemplars.is_empty() {
            match series_records
                .last_mut()
                .and_then(serde_json::Value::as_object_mut)
            {
                Some(record) => record.extend(otel_exemplars(&ts.exemplars, mode)),
                None => count_unrecorded("exemplar", ts.exemplars.len()),
            }
        }

        for record in &series_records {
            let json = serde_json::to_vec(record)
                .map_err(|e| Error::Validation(format!("JSON serialisation failed: {e}")))?;
            events.push(Bytes::from(json));
        }
    }

    Ok(events)
}

/// One native histogram as an OTel record: an exponential histogram for an
/// exponential schema, an explicit-bucket histogram for custom buckets.
/// `None`, counted, for what the OTel shape cannot hold.
fn otel_histogram(
    histogram: &proto::Histogram,
    metric_name: &str,
    attrs: &serde_json::Map<String, serde_json::Value>,
    resource: &serde_json::Map<String, serde_json::Value>,
    mode: PrometheusRwMode,
) -> Result<Option<serde_json::Value>> {
    let decoded = NativeHistogram::decode(histogram)?;
    if decoded.float {
        count_unrecorded("float_histogram", 1);
        return Ok(None);
    }
    let count = decoded.count as u64;
    let hyperdx = mode == PrometheusRwMode::HyperDx;
    let record = if histogram.schema == CUSTOM_BUCKETS_SCHEMA {
        let counts = decoded.custom_counts(histogram.custom_values.len())?;
        if hyperdx {
            serde_json::json!({
                "TimeUnix": epoch_ms_to_ch_datetime(histogram.timestamp),
                "MetricName": metric_name,
                "Count": count,
                "Sum": histogram.sum,
                "Min": 0.0,
                "Max": 0.0,
                "BucketCounts": counts,
                "ExplicitBounds": histogram.custom_values,
                "Attributes": attrs,
                "ResourceAttributes": resource,
                "_otel_metric_type": "histogram",
            })
        } else {
            serde_json::json!({
                "_signal": "metric",
                "_timestamp": epoch_ms_to_rfc3339_nanos(histogram.timestamp),
                "metric_name": metric_name,
                "metric_type": "histogram",
                "count": count,
                "sum": histogram.sum,
                "min": null,
                "max": null,
                "bucket_counts": counts,
                "explicit_bounds": histogram.custom_values,
                "attributes": attrs,
                "resource": resource,
            })
        }
    } else if EXPONENTIAL_SCHEMAS.contains(&histogram.schema) {
        let zero_count = decoded.zero_count as u64;
        if hyperdx {
            serde_json::json!({
                "TimeUnix": epoch_ms_to_ch_datetime(histogram.timestamp),
                "MetricName": metric_name,
                "Count": count,
                "Sum": histogram.sum,
                "Min": 0.0,
                "Max": 0.0,
                "Scale": histogram.schema,
                "ZeroCount": zero_count,
                "PositiveOffset": decoded.positive.otel_offset(),
                "PositiveBucketCounts": decoded.positive.whole(),
                "NegativeOffset": decoded.negative.otel_offset(),
                "NegativeBucketCounts": decoded.negative.whole(),
                "Attributes": attrs,
                "ResourceAttributes": resource,
                "_otel_metric_type": "exponential_histogram",
            })
        } else {
            serde_json::json!({
                "_signal": "metric",
                "_timestamp": epoch_ms_to_rfc3339_nanos(histogram.timestamp),
                "metric_name": metric_name,
                "metric_type": "exponential_histogram",
                "count": count,
                "sum": histogram.sum,
                "min": null,
                "max": null,
                "scale": histogram.schema,
                "zero_count": zero_count,
                "zero_threshold": histogram.zero_threshold,
                "positive_offset": decoded.positive.otel_offset(),
                "positive_bucket_counts": decoded.positive.whole(),
                "negative_offset": decoded.negative.otel_offset(),
                "negative_bucket_counts": decoded.negative.whole(),
                "attributes": attrs,
                "resource": resource,
            })
        }
    } else {
        count_unrecorded("unknown_schema", 1);
        return Ok(None);
    };
    Ok(Some(record))
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

        let events = write_request_to_json(request, PrometheusRwMode::Native).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::Native).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::Native).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::Native).unwrap();
        assert_eq!(events, [] as [bytes::Bytes; 0]);
    }

    #[test]
    fn test_empty_request() {
        let request = proto::WriteRequest {
            timeseries: vec![],
            metadata: vec![],
        };

        let events = write_request_to_json(request, PrometheusRwMode::Native).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::Native).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::Native).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::Native).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::OTel).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::OTel).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::HyperDx).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::OTel).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::OTel).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::HyperDx).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::OTel).unwrap();
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

        let events = write_request_to_json(request, PrometheusRwMode::OTel).unwrap();
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

    /// The event is JSON, which the loader keeps whole in `_json`. A native or
    /// otel rendering of the same sample in `_raw` would be stored and
    /// text-indexed twice.
    #[test]
    fn no_mode_emits_a_raw_field() {
        for mode in [
            PrometheusRwMode::Native,
            PrometheusRwMode::OTel,
            PrometheusRwMode::HyperDx,
        ] {
            let events = write_request_to_json(one_sample_request(), mode).unwrap();
            let obj: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
            assert!(obj.get("_raw").is_none(), "{mode:?}: {obj}");
        }
    }
}
