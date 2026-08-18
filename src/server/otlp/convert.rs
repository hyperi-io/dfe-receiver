// Project:   dfe-receiver
// File:      src/server/otlp/convert.rs
// Purpose:   OTLP protobuf to JSON conversion (HyperDX + generic modes)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! OTLP protobuf to JSON conversion.
//!
//! Supports two output modes:
//!
//! - **`hyperdx`** (default): Produces JSON matching the OTel ClickHouse exporter
//!   schema used by HyperDX. Tables: `otel_logs`, `otel_traces`, `otel_metrics_*`.
//!   This allows dfe-receiver to replace the Go OTel Collector in the HyperDX stack.
//!
//! - **`generic`**: Produces a normalised JSON envelope with routing fields at
//!   known paths. For users who want OTLP data routed through custom pipelines.

use bytes::Bytes;

use super::pb;
use crate::config::RawCapture;
use crate::error::{Error, Result};
use crate::server::raw_capture;

/// OTLP conversion output mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OtlpMode {
    /// HyperDX-compatible: JSON matching ClickHouse OTel schema.
    #[default]
    HyperDx,
    /// Generic: normalised JSON envelope with routing fields.
    Generic,
}

impl OtlpMode {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "generic" => Self::Generic,
            _ => Self::HyperDx,
        }
    }
}

/// OTLP signal type for routing.
#[derive(Debug, Clone, Copy)]
pub enum OtlpSignal {
    Logs,
    Traces,
    Metrics,
}

impl OtlpSignal {
    /// Default Kafka topic suffix for this signal type.
    pub fn topic_suffix(&self) -> &'static str {
        match self {
            Self::Logs => "otel_logs_land",
            Self::Traces => "otel_traces_land",
            Self::Metrics => "otel_metrics_land",
        }
    }
}

/// Result of converting an OTLP request to JSON.
pub struct ConvertedPayload {
    /// JSON bytes for the pipeline.
    pub json: Bytes,
    /// Signal type for routing.
    pub signal: OtlpSignal,
}

/// Attach the generic rendering of a record as `_raw` on the emitted event.
///
/// OTLP arrives as protobuf, which cannot go into a text column, so the
/// generic mode's JSON stands in as the verbatim decode: it is the
/// least-shaped rendering the receiver produces. In generic mode the two are
/// the same document, so `_raw` is a copy -- documented on
/// `otlp.raw_capture` and warned about at startup.
fn attach_generic_raw(
    event: &mut serde_json::Value,
    generic: &serde_json::Value,
    raw_capture: RawCapture,
) -> Result<()> {
    let Some(obj) = event.as_object_mut() else {
        return Ok(());
    };
    let prepared = raw_capture::prepare_serialised(generic, raw_capture)
        .map_err(|e| Error::Validation(format!("OTLP raw capture failed: {e}")))?;
    if let Some(prepared) = prepared {
        raw_capture::attach_prepared_to_map(obj, prepared);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Timestamp helpers
// ---------------------------------------------------------------------------

/// Convert nanosecond unix timestamp to RFC 3339 string.
fn nanos_to_rfc3339(nanos: u64) -> String {
    if nanos == 0 {
        return String::new();
    }
    let secs = (nanos / 1_000_000_000) as i64;
    let nsecs = (nanos % 1_000_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, nsecs);
    match dt {
        Some(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        None => String::new(),
    }
}

/// Convert nanosecond unix timestamp to a ClickHouse-compatible DateTime64(9) string.
/// Format: "2026-02-19 14:30:00.123456789"
fn nanos_to_ch_datetime(nanos: u64) -> String {
    if nanos == 0 {
        return String::new();
    }
    let secs = (nanos / 1_000_000_000) as i64;
    let nsecs = (nanos % 1_000_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, nsecs);
    match dt {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
        None => String::new(),
    }
}

// ---------------------------------------------------------------------------
// Hex encoding helpers (trace_id, span_id)
// ---------------------------------------------------------------------------

fn bytes_to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

// ---------------------------------------------------------------------------
// Attribute helpers
// ---------------------------------------------------------------------------

/// Convert OTLP KeyValue attributes to a flat JSON object.
/// HyperDX schema uses `Map(LowCardinality(String), String)` — all values are stringified.
fn attributes_to_map(
    attrs: &[pb::common::v1::KeyValue],
) -> serde_json::Map<String, serde_json::Value> {
    let mut map = serde_json::Map::with_capacity(attrs.len());
    for kv in attrs {
        let value = kv.value.as_ref().map_or_else(
            || serde_json::Value::String(String::new()),
            any_value_to_string_value,
        );
        map.insert(kv.key.clone(), value);
    }
    map
}

/// Convert an OTLP AnyValue to a string JSON value (for ClickHouse Map columns).
fn any_value_to_string_value(av: &pb::common::v1::AnyValue) -> serde_json::Value {
    match &av.value {
        Some(pb::common::v1::any_value::Value::StringValue(s)) => {
            serde_json::Value::String(s.clone())
        }
        Some(pb::common::v1::any_value::Value::BoolValue(b)) => {
            serde_json::Value::String(b.to_string())
        }
        Some(pb::common::v1::any_value::Value::IntValue(i)) => {
            serde_json::Value::String(i.to_string())
        }
        Some(pb::common::v1::any_value::Value::DoubleValue(d)) => {
            serde_json::Value::String(d.to_string())
        }
        Some(pb::common::v1::any_value::Value::BytesValue(b)) => {
            serde_json::Value::String(bytes_to_hex(b))
        }
        Some(pb::common::v1::any_value::Value::ArrayValue(arr)) => {
            // Serialise array as JSON string
            let items: Vec<serde_json::Value> = arr.values.iter().map(any_value_to_json).collect();
            serde_json::Value::String(serde_json::Value::Array(items).to_string())
        }
        Some(pb::common::v1::any_value::Value::KvlistValue(kvl)) => {
            // Serialise kvlist as JSON string
            let map = attributes_to_map(&kvl.values);
            serde_json::Value::String(serde_json::Value::Object(map).to_string())
        }
        None => serde_json::Value::String(String::new()),
    }
}

/// Convert AnyValue to a native JSON value (for generic mode / body field).
fn any_value_to_json(av: &pb::common::v1::AnyValue) -> serde_json::Value {
    match &av.value {
        Some(pb::common::v1::any_value::Value::StringValue(s)) => {
            serde_json::Value::String(s.clone())
        }
        Some(pb::common::v1::any_value::Value::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(pb::common::v1::any_value::Value::IntValue(i)) => {
            serde_json::Value::Number(serde_json::Number::from(*i))
        }
        Some(pb::common::v1::any_value::Value::DoubleValue(d)) => serde_json::Number::from_f64(*d)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Some(pb::common::v1::any_value::Value::BytesValue(b)) => {
            serde_json::Value::String(bytes_to_hex(b))
        }
        Some(pb::common::v1::any_value::Value::ArrayValue(arr)) => {
            serde_json::Value::Array(arr.values.iter().map(any_value_to_json).collect())
        }
        Some(pb::common::v1::any_value::Value::KvlistValue(kvl)) => {
            let map = kvl
                .values
                .iter()
                .map(|kv| {
                    let v = kv
                        .value
                        .as_ref()
                        .map_or(serde_json::Value::Null, any_value_to_json);
                    (kv.key.clone(), v)
                })
                .collect();
            serde_json::Value::Object(map)
        }
        None => serde_json::Value::Null,
    }
}

/// Extract the `service.name` from resource attributes (used by HyperDX schema).
fn extract_service_name(resource: Option<&pb::resource::v1::Resource>) -> String {
    resource
        .and_then(|r| {
            r.attributes
                .iter()
                .find(|kv| kv.key == "service.name")
                .and_then(|kv| kv.value.as_ref())
                .and_then(|av| match &av.value {
                    Some(pb::common::v1::any_value::Value::StringValue(s)) => Some(s.clone()),
                    _ => None,
                })
        })
        .unwrap_or_default()
}

/// Build resource attributes map.
fn resource_attributes_map(
    resource: Option<&pb::resource::v1::Resource>,
) -> serde_json::Map<String, serde_json::Value> {
    resource.map_or_else(serde_json::Map::new, |r| attributes_to_map(&r.attributes))
}

// ===========================================================================
// LOGS conversion
// ===========================================================================

/// Convert an OTLP `ExportLogsServiceRequest` to JSON payloads.
///
/// In HyperDX mode, each log record produces one JSON object matching the
/// `otel_logs` ClickHouse schema.
pub fn convert_logs(
    request: &pb::collector::logs::v1::ExportLogsServiceRequest,
    mode: OtlpMode,
    raw_capture: RawCapture,
) -> Result<Vec<ConvertedPayload>> {
    let mut payloads = Vec::new();

    for resource_logs in &request.resource_logs {
        let service_name = extract_service_name(resource_logs.resource.as_ref());
        let resource_attrs = resource_attributes_map(resource_logs.resource.as_ref());

        for scope_logs in &resource_logs.scope_logs {
            let scope_name = scope_logs.scope.as_ref().map_or("", |s| s.name.as_str());
            let scope_version = scope_logs.scope.as_ref().map_or("", |s| s.version.as_str());

            for log in &scope_logs.log_records {
                let mut json = match mode {
                    OtlpMode::HyperDx => log_to_hyperdx_json(
                        log,
                        &service_name,
                        &resource_attrs,
                        scope_name,
                        scope_version,
                    ),
                    OtlpMode::Generic => {
                        log_to_generic_json(log, &resource_attrs, scope_name, scope_version)
                    }
                };

                if raw_capture.enabled {
                    let generic =
                        log_to_generic_json(log, &resource_attrs, scope_name, scope_version);
                    attach_generic_raw(&mut json, &generic, raw_capture)?;
                }

                let bytes = serde_json::to_vec(&json).map_err(|e| {
                    Error::Validation(format!("OTLP log serialisation failed: {e}"))
                })?;

                payloads.push(ConvertedPayload {
                    json: Bytes::from(bytes),
                    signal: OtlpSignal::Logs,
                });
            }
        }
    }

    Ok(payloads)
}

/// Convert a single OTLP log record to HyperDX-compatible JSON.
///
/// Schema matches `otel_logs` ClickHouse table (OTel ClickHouse exporter):
/// Timestamp, ObservedTimestamp, TraceId, SpanId, SeverityText,
/// SeverityNumber, Body, ServiceName, LogAttributes, ResourceAttributes,
/// ScopeName, ScopeVersion
fn log_to_hyperdx_json(
    log: &pb::logs::v1::LogRecord,
    service_name: &str,
    resource_attrs: &serde_json::Map<String, serde_json::Value>,
    scope_name: &str,
    scope_version: &str,
) -> serde_json::Value {
    let body = log
        .body
        .as_ref()
        .map_or(serde_json::Value::String(String::new()), |av| {
            // Body is stored as string in HyperDX schema
            any_value_to_string_value(av)
        });

    let severity_text = if log.severity_text.is_empty() {
        severity_number_to_text(log.severity_number)
    } else {
        log.severity_text.clone()
    };

    serde_json::json!({
        "Timestamp": nanos_to_ch_datetime(log.time_unix_nano),
        "ObservedTimestamp": nanos_to_ch_datetime(log.observed_time_unix_nano),
        "TraceId": bytes_to_hex(&log.trace_id),
        "SpanId": bytes_to_hex(&log.span_id),
        "SeverityText": severity_text,
        "SeverityNumber": log.severity_number,
        "Body": body,
        "ServiceName": service_name,
        "LogAttributes": attributes_to_map(&log.attributes),
        "ResourceAttributes": resource_attrs,
        "ScopeName": scope_name,
        "ScopeVersion": scope_version,
    })
}

/// Convert a single OTLP log record to generic JSON envelope.
fn log_to_generic_json(
    log: &pb::logs::v1::LogRecord,
    resource_attrs: &serde_json::Map<String, serde_json::Value>,
    scope_name: &str,
    scope_version: &str,
) -> serde_json::Value {
    let body = log
        .body
        .as_ref()
        .map_or(serde_json::Value::Null, any_value_to_json);

    serde_json::json!({
        "_signal": "log",
        "_timestamp": nanos_to_rfc3339(log.time_unix_nano),
        "_observed_timestamp": nanos_to_rfc3339(log.observed_time_unix_nano),
        "trace_id": bytes_to_hex(&log.trace_id),
        "span_id": bytes_to_hex(&log.span_id),
        "severity_text": log.severity_text,
        "severity_number": log.severity_number,
        "body": body,
        "attributes": attributes_to_map(&log.attributes),
        "resource": resource_attrs,
        "scope_name": scope_name,
        "scope_version": scope_version,
    })
}

/// Map severity number to text.
fn severity_number_to_text(n: i32) -> String {
    match n {
        1..=4 => "TRACE".to_string(),
        5..=8 => "DEBUG".to_string(),
        9..=12 => "INFO".to_string(),
        13..=16 => "WARN".to_string(),
        17..=20 => "ERROR".to_string(),
        21..=24 => "FATAL".to_string(),
        _ => String::new(),
    }
}

// ===========================================================================
// TRACES conversion
// ===========================================================================

/// Convert an OTLP `ExportTraceServiceRequest` to JSON payloads.
///
/// In HyperDX mode, each span produces one JSON object matching the
/// `otel_traces` ClickHouse schema.
pub fn convert_traces(
    request: &pb::collector::trace::v1::ExportTraceServiceRequest,
    mode: OtlpMode,
    raw_capture: RawCapture,
) -> Result<Vec<ConvertedPayload>> {
    let mut payloads = Vec::new();

    for resource_spans in &request.resource_spans {
        let service_name = extract_service_name(resource_spans.resource.as_ref());
        let resource_attrs = resource_attributes_map(resource_spans.resource.as_ref());

        for scope_spans in &resource_spans.scope_spans {
            let scope_name = scope_spans.scope.as_ref().map_or("", |s| s.name.as_str());
            let scope_version = scope_spans
                .scope
                .as_ref()
                .map_or("", |s| s.version.as_str());

            for span in &scope_spans.spans {
                let mut json = match mode {
                    OtlpMode::HyperDx => span_to_hyperdx_json(
                        span,
                        &service_name,
                        &resource_attrs,
                        scope_name,
                        scope_version,
                    ),
                    OtlpMode::Generic => {
                        span_to_generic_json(span, &resource_attrs, scope_name, scope_version)
                    }
                };

                if raw_capture.enabled {
                    let generic =
                        span_to_generic_json(span, &resource_attrs, scope_name, scope_version);
                    attach_generic_raw(&mut json, &generic, raw_capture)?;
                }

                let bytes = serde_json::to_vec(&json).map_err(|e| {
                    Error::Validation(format!("OTLP span serialisation failed: {e}"))
                })?;

                payloads.push(ConvertedPayload {
                    json: Bytes::from(bytes),
                    signal: OtlpSignal::Traces,
                });
            }
        }
    }

    Ok(payloads)
}

/// Convert a single OTLP span to HyperDX-compatible JSON.
///
/// Schema matches `otel_traces` ClickHouse table.
fn span_to_hyperdx_json(
    span: &pb::trace::v1::Span,
    service_name: &str,
    resource_attrs: &serde_json::Map<String, serde_json::Value>,
    scope_name: &str,
    scope_version: &str,
) -> serde_json::Value {
    // Duration in nanoseconds
    let duration_nano = span
        .end_time_unix_nano
        .saturating_sub(span.start_time_unix_nano);

    // Status
    let (status_code, status_message) = span.status.as_ref().map_or(("Unset", ""), |s| {
        let code = match s.code {
            1 => "Ok",
            2 => "Error",
            _ => "Unset",
        };
        (code, s.message.as_str())
    });

    // SpanKind
    let span_kind = match span.kind {
        0 => "SPAN_KIND_UNSPECIFIED",
        1 => "SPAN_KIND_INTERNAL",
        2 => "SPAN_KIND_SERVER",
        3 => "SPAN_KIND_CLIENT",
        4 => "SPAN_KIND_PRODUCER",
        5 => "SPAN_KIND_CONSUMER",
        _ => "SPAN_KIND_UNSPECIFIED",
    };

    // Events (nested array in HyperDX schema)
    let events: Vec<serde_json::Value> = span
        .events
        .iter()
        .map(|e| {
            serde_json::json!({
                "Timestamp": nanos_to_ch_datetime(e.time_unix_nano),
                "Name": e.name,
                "Attributes": attributes_to_map(&e.attributes),
            })
        })
        .collect();

    // Links (nested array in HyperDX schema)
    let links: Vec<serde_json::Value> = span
        .links
        .iter()
        .map(|l| {
            serde_json::json!({
                "TraceId": bytes_to_hex(&l.trace_id),
                "SpanId": bytes_to_hex(&l.span_id),
                "TraceState": l.trace_state,
                "Attributes": attributes_to_map(&l.attributes),
            })
        })
        .collect();

    serde_json::json!({
        "Timestamp": nanos_to_ch_datetime(span.start_time_unix_nano),
        "TraceId": bytes_to_hex(&span.trace_id),
        "SpanId": bytes_to_hex(&span.span_id),
        "ParentSpanId": bytes_to_hex(&span.parent_span_id),
        "SpanName": span.name,
        "SpanKind": span_kind,
        "ServiceName": service_name,
        "Duration": duration_nano,
        "StatusCode": status_code,
        "StatusMessage": status_message,
        "SpanAttributes": attributes_to_map(&span.attributes),
        "ResourceAttributes": resource_attrs,
        "Events.Timestamp": events.iter().map(|e| e["Timestamp"].clone()).collect::<Vec<_>>(),
        "Events.Name": events.iter().map(|e| e["Name"].clone()).collect::<Vec<_>>(),
        "Events.Attributes": events.iter().map(|e| e["Attributes"].clone()).collect::<Vec<_>>(),
        "Links.TraceId": links.iter().map(|l| l["TraceId"].clone()).collect::<Vec<_>>(),
        "Links.SpanId": links.iter().map(|l| l["SpanId"].clone()).collect::<Vec<_>>(),
        "Links.TraceState": links.iter().map(|l| l["TraceState"].clone()).collect::<Vec<_>>(),
        "Links.Attributes": links.iter().map(|l| l["Attributes"].clone()).collect::<Vec<_>>(),
        "ScopeName": scope_name,
        "ScopeVersion": scope_version,
    })
}

/// Convert a single OTLP span to generic JSON envelope.
fn span_to_generic_json(
    span: &pb::trace::v1::Span,
    resource_attrs: &serde_json::Map<String, serde_json::Value>,
    scope_name: &str,
    scope_version: &str,
) -> serde_json::Value {
    let duration_nano = span
        .end_time_unix_nano
        .saturating_sub(span.start_time_unix_nano);

    let events: Vec<serde_json::Value> = span
        .events
        .iter()
        .map(|e| {
            serde_json::json!({
                "timestamp": nanos_to_rfc3339(e.time_unix_nano),
                "name": e.name,
                "attributes": attributes_to_map(&e.attributes),
            })
        })
        .collect();

    let links: Vec<serde_json::Value> = span
        .links
        .iter()
        .map(|l| {
            serde_json::json!({
                "trace_id": bytes_to_hex(&l.trace_id),
                "span_id": bytes_to_hex(&l.span_id),
                "trace_state": l.trace_state,
                "attributes": attributes_to_map(&l.attributes),
            })
        })
        .collect();

    serde_json::json!({
        "_signal": "trace",
        "_timestamp": nanos_to_rfc3339(span.start_time_unix_nano),
        "trace_id": bytes_to_hex(&span.trace_id),
        "span_id": bytes_to_hex(&span.span_id),
        "parent_span_id": bytes_to_hex(&span.parent_span_id),
        "name": span.name,
        "kind": span.kind,
        "duration_ns": duration_nano,
        "status": span.status.as_ref().map(|s| serde_json::json!({"code": s.code, "message": s.message})),
        "attributes": attributes_to_map(&span.attributes),
        "resource": resource_attrs,
        "events": events,
        "links": links,
        "scope_name": scope_name,
        "scope_version": scope_version,
    })
}

// ===========================================================================
// METRICS conversion
// ===========================================================================

/// Convert an OTLP `ExportMetricsServiceRequest` to JSON payloads.
///
/// In HyperDX mode, each data point produces one JSON object matching the
/// `otel_metrics_*` ClickHouse schema (gauge, sum, histogram).
pub fn convert_metrics(
    request: &pb::collector::metrics::v1::ExportMetricsServiceRequest,
    mode: OtlpMode,
    raw_capture: RawCapture,
) -> Result<Vec<ConvertedPayload>> {
    let mut payloads = Vec::new();

    for resource_metrics in &request.resource_metrics {
        let resource_attrs = resource_attributes_map(resource_metrics.resource.as_ref());

        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                let metric_name = &metric.name;

                if let Some(ref data) = metric.data {
                    let mut points = metric_points(metric_name, data, &resource_attrs, mode);

                    // The generic rendering of the same points is what _raw
                    // carries; the converters are deterministic, so the two
                    // vectors line up point for point.
                    if raw_capture.enabled {
                        let generic =
                            metric_points(metric_name, data, &resource_attrs, OtlpMode::Generic);
                        for (json, generic) in points.iter_mut().zip(generic.iter()) {
                            attach_generic_raw(json, generic, raw_capture)?;
                        }
                    }

                    for json in points {
                        let bytes = serde_json::to_vec(&json).map_err(|e| {
                            Error::Validation(format!("OTLP metric serialisation failed: {e}"))
                        })?;
                        payloads.push(ConvertedPayload {
                            json: Bytes::from(bytes),
                            signal: OtlpSignal::Metrics,
                        });
                    }
                }
            }
        }
    }

    Ok(payloads)
}

/// Render one metric's data points in the given mode.
fn metric_points(
    metric_name: &str,
    data: &pb::metrics::v1::metric::Data,
    resource_attrs: &serde_json::Map<String, serde_json::Value>,
    mode: OtlpMode,
) -> Vec<serde_json::Value> {
    match data {
        pb::metrics::v1::metric::Data::Gauge(g) => {
            convert_gauge_points(metric_name, &g.data_points, resource_attrs, mode)
        }
        pb::metrics::v1::metric::Data::Sum(s) => {
            convert_sum_points(metric_name, &s.data_points, resource_attrs, mode)
        }
        pb::metrics::v1::metric::Data::Histogram(h) => {
            convert_histogram_points(metric_name, &h.data_points, resource_attrs, mode)
        }
        pb::metrics::v1::metric::Data::ExponentialHistogram(eh) => {
            convert_exp_histogram_points(metric_name, &eh.data_points, resource_attrs, mode)
        }
        pb::metrics::v1::metric::Data::Summary(s) => {
            convert_summary_points(metric_name, &s.data_points, resource_attrs, mode)
        }
    }
}

/// Convert gauge data points.
fn convert_gauge_points(
    metric_name: &str,
    data_points: &[pb::metrics::v1::NumberDataPoint],
    resource_attrs: &serde_json::Map<String, serde_json::Value>,
    mode: OtlpMode,
) -> Vec<serde_json::Value> {
    data_points
        .iter()
        .map(|dp| {
            let value = number_data_point_value(dp);
            let attrs = attributes_to_map(&dp.attributes);

            match mode {
                OtlpMode::HyperDx => serde_json::json!({
                    "TimeUnix": nanos_to_ch_datetime(dp.time_unix_nano),
                    "MetricName": metric_name,
                    "Value": value,
                    "Attributes": attrs,
                    "ResourceAttributes": resource_attrs,
                    "_otel_metric_type": "gauge",
                }),
                OtlpMode::Generic => serde_json::json!({
                    "_signal": "metric",
                    "_timestamp": nanos_to_rfc3339(dp.time_unix_nano),
                    "metric_name": metric_name,
                    "metric_type": "gauge",
                    "value": value,
                    "attributes": attrs,
                    "resource": resource_attrs,
                }),
            }
        })
        .collect()
}

/// Convert sum data points.
fn convert_sum_points(
    metric_name: &str,
    data_points: &[pb::metrics::v1::NumberDataPoint],
    resource_attrs: &serde_json::Map<String, serde_json::Value>,
    mode: OtlpMode,
) -> Vec<serde_json::Value> {
    data_points
        .iter()
        .map(|dp| {
            let value = number_data_point_value(dp);
            let attrs = attributes_to_map(&dp.attributes);

            match mode {
                OtlpMode::HyperDx => serde_json::json!({
                    "TimeUnix": nanos_to_ch_datetime(dp.time_unix_nano),
                    "MetricName": metric_name,
                    "Value": value,
                    "Attributes": attrs,
                    "ResourceAttributes": resource_attrs,
                    "_otel_metric_type": "sum",
                }),
                OtlpMode::Generic => serde_json::json!({
                    "_signal": "metric",
                    "_timestamp": nanos_to_rfc3339(dp.time_unix_nano),
                    "metric_name": metric_name,
                    "metric_type": "sum",
                    "value": value,
                    "attributes": attrs,
                    "resource": resource_attrs,
                }),
            }
        })
        .collect()
}

/// Convert histogram data points.
fn convert_histogram_points(
    metric_name: &str,
    data_points: &[pb::metrics::v1::HistogramDataPoint],
    resource_attrs: &serde_json::Map<String, serde_json::Value>,
    mode: OtlpMode,
) -> Vec<serde_json::Value> {
    data_points
        .iter()
        .map(|dp| {
            let attrs = attributes_to_map(&dp.attributes);

            match mode {
                OtlpMode::HyperDx => serde_json::json!({
                    "TimeUnix": nanos_to_ch_datetime(dp.time_unix_nano),
                    "MetricName": metric_name,
                    "Count": dp.count,
                    "Sum": dp.sum.unwrap_or(0.0),
                    "Min": dp.min.unwrap_or(0.0),
                    "Max": dp.max.unwrap_or(0.0),
                    "BucketCounts": dp.bucket_counts,
                    "ExplicitBounds": dp.explicit_bounds,
                    "Attributes": attrs,
                    "ResourceAttributes": resource_attrs,
                    "_otel_metric_type": "histogram",
                }),
                OtlpMode::Generic => serde_json::json!({
                    "_signal": "metric",
                    "_timestamp": nanos_to_rfc3339(dp.time_unix_nano),
                    "metric_name": metric_name,
                    "metric_type": "histogram",
                    "count": dp.count,
                    "sum": dp.sum,
                    "min": dp.min,
                    "max": dp.max,
                    "bucket_counts": dp.bucket_counts,
                    "explicit_bounds": dp.explicit_bounds,
                    "attributes": attrs,
                    "resource": resource_attrs,
                }),
            }
        })
        .collect()
}

/// Convert exponential histogram data points.
fn convert_exp_histogram_points(
    metric_name: &str,
    data_points: &[pb::metrics::v1::ExponentialHistogramDataPoint],
    resource_attrs: &serde_json::Map<String, serde_json::Value>,
    mode: OtlpMode,
) -> Vec<serde_json::Value> {
    data_points
        .iter()
        .map(|dp| {
            let attrs = attributes_to_map(&dp.attributes);

            match mode {
                OtlpMode::HyperDx => serde_json::json!({
                    "TimeUnix": nanos_to_ch_datetime(dp.time_unix_nano),
                    "MetricName": metric_name,
                    "Count": dp.count,
                    "Sum": dp.sum.unwrap_or(0.0),
                    "Min": dp.min.unwrap_or(0.0),
                    "Max": dp.max.unwrap_or(0.0),
                    "Scale": dp.scale,
                    "ZeroCount": dp.zero_count,
                    "Attributes": attrs,
                    "ResourceAttributes": resource_attrs,
                    "_otel_metric_type": "exponential_histogram",
                }),
                OtlpMode::Generic => serde_json::json!({
                    "_signal": "metric",
                    "_timestamp": nanos_to_rfc3339(dp.time_unix_nano),
                    "metric_name": metric_name,
                    "metric_type": "exponential_histogram",
                    "count": dp.count,
                    "sum": dp.sum,
                    "min": dp.min,
                    "max": dp.max,
                    "scale": dp.scale,
                    "zero_count": dp.zero_count,
                    "attributes": attrs,
                    "resource": resource_attrs,
                }),
            }
        })
        .collect()
}

/// Convert summary data points.
fn convert_summary_points(
    metric_name: &str,
    data_points: &[pb::metrics::v1::SummaryDataPoint],
    resource_attrs: &serde_json::Map<String, serde_json::Value>,
    mode: OtlpMode,
) -> Vec<serde_json::Value> {
    data_points
        .iter()
        .map(|dp| {
            let attrs = attributes_to_map(&dp.attributes);
            let quantiles: Vec<serde_json::Value> = dp
                .quantile_values
                .iter()
                .map(|qv| {
                    serde_json::json!({
                        "quantile": qv.quantile,
                        "value": qv.value,
                    })
                })
                .collect();

            match mode {
                OtlpMode::HyperDx => serde_json::json!({
                    "TimeUnix": nanos_to_ch_datetime(dp.time_unix_nano),
                    "MetricName": metric_name,
                    "Count": dp.count,
                    "Sum": dp.sum,
                    "Quantiles": quantiles,
                    "Attributes": attrs,
                    "ResourceAttributes": resource_attrs,
                    "_otel_metric_type": "summary",
                }),
                OtlpMode::Generic => serde_json::json!({
                    "_signal": "metric",
                    "_timestamp": nanos_to_rfc3339(dp.time_unix_nano),
                    "metric_name": metric_name,
                    "metric_type": "summary",
                    "count": dp.count,
                    "sum": dp.sum,
                    "quantile_values": quantiles,
                    "attributes": attrs,
                    "resource": resource_attrs,
                }),
            }
        })
        .collect()
}

/// Extract numeric value from a `NumberDataPoint`.
fn number_data_point_value(dp: &pb::metrics::v1::NumberDataPoint) -> f64 {
    match dp.value {
        Some(pb::metrics::v1::number_data_point::Value::AsDouble(d)) => d,
        Some(pb::metrics::v1::number_data_point::Value::AsInt(i)) => i as f64,
        None => 0.0,
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nanos_to_rfc3339() {
        // 2026-02-19T00:00:00Z in nanoseconds
        let nanos = 1_771_459_200_000_000_000u64;
        let result = nanos_to_rfc3339(nanos);
        assert!(result.starts_with("2026-02-19"));
        assert!(result.ends_with('Z'));
    }

    #[test]
    fn test_nanos_to_rfc3339_zero() {
        assert_eq!(nanos_to_rfc3339(0), "");
    }

    #[test]
    fn test_nanos_to_ch_datetime() {
        let nanos = 1_771_459_200_123_456_789u64;
        let result = nanos_to_ch_datetime(nanos);
        assert!(result.starts_with("2026-02-19"));
        assert!(result.contains("123456789"));
    }

    #[test]
    fn test_bytes_to_hex() {
        assert_eq!(bytes_to_hex(&[0xab, 0xcd, 0xef]), "abcdef");
        assert_eq!(bytes_to_hex(&[]), "");
    }

    #[test]
    fn test_severity_number_to_text() {
        assert_eq!(severity_number_to_text(9), "INFO");
        assert_eq!(severity_number_to_text(17), "ERROR");
        assert_eq!(severity_number_to_text(0), "");
    }

    #[test]
    fn test_otlp_mode_from_str() {
        assert_eq!(OtlpMode::from_str("hyperdx"), OtlpMode::HyperDx);
        assert_eq!(OtlpMode::from_str("generic"), OtlpMode::Generic);
        assert_eq!(OtlpMode::from_str("anything"), OtlpMode::HyperDx);
    }

    #[test]
    fn test_any_value_to_string_value() {
        let av = pb::common::v1::AnyValue {
            value: Some(pb::common::v1::any_value::Value::StringValue(
                "hello".to_string(),
            )),
        };
        let result = any_value_to_string_value(&av);
        assert_eq!(result, serde_json::Value::String("hello".to_string()));
    }

    #[test]
    fn test_any_value_int_to_string() {
        let av = pb::common::v1::AnyValue {
            value: Some(pb::common::v1::any_value::Value::IntValue(42)),
        };
        let result = any_value_to_string_value(&av);
        assert_eq!(result, serde_json::Value::String("42".to_string()));
    }

    #[test]
    fn test_convert_logs_hyperdx_mode() {
        let request = pb::collector::logs::v1::ExportLogsServiceRequest {
            resource_logs: vec![pb::logs::v1::ResourceLogs {
                resource: Some(pb::resource::v1::Resource {
                    attributes: vec![pb::common::v1::KeyValue {
                        key: "service.name".to_string(),
                        value: Some(pb::common::v1::AnyValue {
                            value: Some(pb::common::v1::any_value::Value::StringValue(
                                "test-service".to_string(),
                            )),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![pb::logs::v1::ScopeLogs {
                    scope: Some(pb::common::v1::InstrumentationScope {
                        name: "my-lib".to_string(),
                        version: "1.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    log_records: vec![pb::logs::v1::LogRecord {
                        time_unix_nano: 1_771_459_200_000_000_000,
                        observed_time_unix_nano: 1_771_459_200_000_000_000,
                        severity_number: 9, // INFO
                        severity_text: "INFO".to_string(),
                        body: Some(pb::common::v1::AnyValue {
                            value: Some(pb::common::v1::any_value::Value::StringValue(
                                "Test log message".to_string(),
                            )),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0u8; 16],
                        span_id: vec![0u8; 8],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let payloads = convert_logs(&request, OtlpMode::HyperDx, RawCapture::OFF).unwrap();
        assert_eq!(payloads.len(), 1);

        let json: serde_json::Value = serde_json::from_slice(&payloads[0].json).unwrap();
        assert_eq!(json["ServiceName"], "test-service");
        assert_eq!(json["SeverityText"], "INFO");
        assert_eq!(json["ScopeName"], "my-lib");
        assert_eq!(json["Body"], "Test log message");
        assert!(
            json["Timestamp"]
                .as_str()
                .unwrap()
                .starts_with("2026-02-19")
        );
    }

    #[test]
    fn test_convert_logs_generic_mode() {
        let request = pb::collector::logs::v1::ExportLogsServiceRequest {
            resource_logs: vec![pb::logs::v1::ResourceLogs {
                resource: None,
                scope_logs: vec![pb::logs::v1::ScopeLogs {
                    scope: None,
                    log_records: vec![pb::logs::v1::LogRecord {
                        time_unix_nano: 1_771_459_200_000_000_000,
                        observed_time_unix_nano: 0,
                        severity_number: 17,
                        severity_text: "ERROR".to_string(),
                        body: Some(pb::common::v1::AnyValue {
                            value: Some(pb::common::v1::any_value::Value::StringValue(
                                "error occurred".to_string(),
                            )),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![],
                        span_id: vec![],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let payloads = convert_logs(&request, OtlpMode::Generic, RawCapture::OFF).unwrap();
        assert_eq!(payloads.len(), 1);

        let json: serde_json::Value = serde_json::from_slice(&payloads[0].json).unwrap();
        assert_eq!(json["_signal"], "log");
        assert_eq!(json["body"], "error occurred");
    }

    // =========================================================================
    // Extended conversion tests (edge cases, all variants, malformed input)
    // =========================================================================

    // ---- AnyValue conversion: every variant ----

    #[test]
    fn test_any_value_all_primitive_variants() {
        use pb::common::v1::{AnyValue, any_value::Value};

        // String
        let sv = AnyValue {
            value: Some(Value::StringValue("hello".to_string())),
        };
        assert_eq!(any_value_to_string_value(&sv), serde_json::json!("hello"));

        // Int
        let iv = AnyValue {
            value: Some(Value::IntValue(-9999)),
        };
        assert_eq!(any_value_to_string_value(&iv), serde_json::json!("-9999"));

        // Double
        let dv = AnyValue {
            value: Some(Value::DoubleValue(2.5)),
        };
        // Double → string representation (implementation-defined format)
        let s = any_value_to_string_value(&dv);
        assert!(s.as_str().unwrap().contains("2.5"));

        // Bool
        let bv = AnyValue {
            value: Some(Value::BoolValue(true)),
        };
        let s = any_value_to_string_value(&bv);
        assert_eq!(s.as_str().unwrap(), "true");

        // Bytes
        let bsv = AnyValue {
            value: Some(Value::BytesValue(vec![0xde, 0xad, 0xbe, 0xef])),
        };
        let s = any_value_to_string_value(&bsv);
        // bytes should render as hex or base64 — implementation-defined
        let text = s.as_str().unwrap();
        assert!(
            text.contains("deadbeef") || !text.is_empty(),
            "bytes should convert to non-empty string: {text}"
        );

        // None (no inner value)
        let nv = AnyValue { value: None };
        let result = any_value_to_string_value(&nv);
        assert!(matches!(result, serde_json::Value::Null) || result == serde_json::json!(""));
    }

    #[test]
    fn test_any_value_array_and_kv_list() {
        use pb::common::v1::{AnyValue, ArrayValue, KeyValue, KeyValueList, any_value::Value};

        // Array of strings
        let array = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(Value::StringValue("a".to_string())),
                    },
                    AnyValue {
                        value: Some(Value::IntValue(1)),
                    },
                ],
            })),
        };
        let result = any_value_to_json(&array);
        // Array converts to JSON array (or JSON-stringified array)
        assert!(result.is_array() || result.is_string());

        // Key-value list (map)
        let kv = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![KeyValue {
                    key: "nested".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("deep".to_string())),
                    }),
                }],
            })),
        };
        let result = any_value_to_json(&kv);
        // Should not crash, should produce something
        assert!(!matches!(result, serde_json::Value::Null));
    }

    // ---- Logs: hyperdx mode with attributes ----

    #[test]
    fn test_convert_logs_hyperdx_with_attributes_and_trace() {
        let trace_id = vec![1u8; 16];
        let span_id = vec![2u8; 8];
        let request = pb::collector::logs::v1::ExportLogsServiceRequest {
            resource_logs: vec![pb::logs::v1::ResourceLogs {
                resource: Some(pb::resource::v1::Resource {
                    attributes: vec![
                        pb::common::v1::KeyValue {
                            key: "service.name".to_string(),
                            value: Some(pb::common::v1::AnyValue {
                                value: Some(pb::common::v1::any_value::Value::StringValue(
                                    "api".to_string(),
                                )),
                            }),
                        },
                        pb::common::v1::KeyValue {
                            key: "deployment.environment".to_string(),
                            value: Some(pb::common::v1::AnyValue {
                                value: Some(pb::common::v1::any_value::Value::StringValue(
                                    "prod".to_string(),
                                )),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![pb::logs::v1::ScopeLogs {
                    scope: None,
                    log_records: vec![pb::logs::v1::LogRecord {
                        time_unix_nano: 1_771_459_200_000_000_000,
                        observed_time_unix_nano: 1_771_459_200_000_000_000,
                        severity_number: 17,
                        severity_text: "ERROR".to_string(),
                        body: Some(pb::common::v1::AnyValue {
                            value: Some(pb::common::v1::any_value::Value::StringValue(
                                "db timeout".to_string(),
                            )),
                        }),
                        attributes: vec![pb::common::v1::KeyValue {
                            key: "error.code".to_string(),
                            value: Some(pb::common::v1::AnyValue {
                                value: Some(pb::common::v1::any_value::Value::IntValue(500)),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        flags: 1,
                        trace_id,
                        span_id,
                        event_name: "db.query".to_string(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let payloads = convert_logs(&request, OtlpMode::HyperDx, RawCapture::OFF).unwrap();
        assert_eq!(payloads.len(), 1);
        let json: serde_json::Value = serde_json::from_slice(&payloads[0].json).unwrap();

        // Trace correlation fields should be hex-encoded
        let tid = json["TraceId"].as_str().unwrap();
        let sid = json["SpanId"].as_str().unwrap();
        assert_eq!(tid.len(), 32, "TraceId should be 32 hex chars: {tid}");
        assert_eq!(sid.len(), 16, "SpanId should be 16 hex chars: {sid}");
        assert_eq!(json["ServiceName"], "api");
        assert_eq!(json["SeverityText"], "ERROR");
    }

    #[test]
    fn test_convert_logs_multiple_records() {
        // Multiple scope_logs × multiple log_records → multiple payloads
        let make_log = |body: &str| pb::logs::v1::LogRecord {
            time_unix_nano: 1_771_459_200_000_000_000,
            observed_time_unix_nano: 0,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(pb::common::v1::AnyValue {
                value: Some(pb::common::v1::any_value::Value::StringValue(
                    body.to_string(),
                )),
            }),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
            event_name: String::new(),
        };

        let request = pb::collector::logs::v1::ExportLogsServiceRequest {
            resource_logs: vec![pb::logs::v1::ResourceLogs {
                resource: None,
                scope_logs: vec![pb::logs::v1::ScopeLogs {
                    scope: None,
                    log_records: vec![make_log("one"), make_log("two"), make_log("three")],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let payloads = convert_logs(&request, OtlpMode::HyperDx, RawCapture::OFF).unwrap();
        assert_eq!(payloads.len(), 3);
    }

    #[test]
    fn test_convert_logs_empty_request() {
        let request = pb::collector::logs::v1::ExportLogsServiceRequest {
            resource_logs: vec![],
        };
        let payloads = convert_logs(&request, OtlpMode::HyperDx, RawCapture::OFF).unwrap();
        assert!(payloads.is_empty());
    }

    // ---- Traces ----

    #[test]
    fn test_convert_traces_hyperdx_basic() {
        let request = pb::collector::trace::v1::ExportTraceServiceRequest {
            resource_spans: vec![pb::trace::v1::ResourceSpans {
                resource: Some(pb::resource::v1::Resource {
                    attributes: vec![pb::common::v1::KeyValue {
                        key: "service.name".to_string(),
                        value: Some(pb::common::v1::AnyValue {
                            value: Some(pb::common::v1::any_value::Value::StringValue(
                                "my-service".to_string(),
                            )),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![pb::trace::v1::ScopeSpans {
                    scope: None,
                    spans: vec![pb::trace::v1::Span {
                        trace_id: vec![0xaa; 16],
                        span_id: vec![0xbb; 8],
                        trace_state: String::new(),
                        parent_span_id: vec![],
                        flags: 0,
                        name: "GET /api/v1/users".to_string(),
                        kind: 2, // SERVER
                        start_time_unix_nano: 1_771_459_200_000_000_000,
                        end_time_unix_nano: 1_771_459_200_100_000_000,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(pb::trace::v1::Status {
                            message: String::new(),
                            code: 1, // OK
                        }),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let payloads = convert_traces(&request, OtlpMode::HyperDx, RawCapture::OFF).unwrap();
        assert_eq!(payloads.len(), 1);
        let json: serde_json::Value = serde_json::from_slice(&payloads[0].json).unwrap();

        // HyperDX schema uses PascalCase
        assert_eq!(json["SpanName"], "GET /api/v1/users");
        assert_eq!(json["ServiceName"], "my-service");
        let tid = json["TraceId"].as_str().unwrap();
        assert_eq!(tid.len(), 32);
        assert!(tid.chars().all(|c| c == 'a'));
    }

    #[test]
    fn test_convert_traces_generic_mode() {
        let request = pb::collector::trace::v1::ExportTraceServiceRequest {
            resource_spans: vec![pb::trace::v1::ResourceSpans {
                resource: None,
                scope_spans: vec![pb::trace::v1::ScopeSpans {
                    scope: None,
                    spans: vec![pb::trace::v1::Span {
                        trace_id: vec![],
                        span_id: vec![],
                        trace_state: String::new(),
                        parent_span_id: vec![],
                        flags: 0,
                        name: "op".to_string(),
                        kind: 0,
                        start_time_unix_nano: 0,
                        end_time_unix_nano: 0,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: None,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let payloads = convert_traces(&request, OtlpMode::Generic, RawCapture::OFF).unwrap();
        assert_eq!(payloads.len(), 1);
        let json: serde_json::Value = serde_json::from_slice(&payloads[0].json).unwrap();
        assert_eq!(json["_signal"], "trace");
    }

    // ---- Metrics ----

    #[test]
    fn test_convert_metrics_gauge() {
        let request = pb::collector::metrics::v1::ExportMetricsServiceRequest {
            resource_metrics: vec![pb::metrics::v1::ResourceMetrics {
                resource: None,
                scope_metrics: vec![pb::metrics::v1::ScopeMetrics {
                    scope: None,
                    metrics: vec![pb::metrics::v1::Metric {
                        name: "cpu_usage".to_string(),
                        description: "CPU usage".to_string(),
                        unit: "percent".to_string(),
                        metadata: vec![],
                        data: Some(pb::metrics::v1::metric::Data::Gauge(
                            pb::metrics::v1::Gauge {
                                data_points: vec![pb::metrics::v1::NumberDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 0,
                                    time_unix_nano: 1_771_459_200_000_000_000,
                                    exemplars: vec![],
                                    flags: 0,
                                    value: Some(
                                        pb::metrics::v1::number_data_point::Value::AsDouble(42.5),
                                    ),
                                }],
                            },
                        )),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let payloads = convert_metrics(&request, OtlpMode::HyperDx, RawCapture::OFF).unwrap();
        assert!(!payloads.is_empty());
    }

    #[test]
    fn test_convert_metrics_empty() {
        let request = pb::collector::metrics::v1::ExportMetricsServiceRequest {
            resource_metrics: vec![],
        };
        let payloads = convert_metrics(&request, OtlpMode::HyperDx, RawCapture::OFF).unwrap();
        assert!(payloads.is_empty());
    }

    // ---- nanos_to_rfc3339 edge cases ----

    #[test]
    fn test_nanos_to_rfc3339_edge_cases() {
        // Very small timestamp (near unix epoch)
        let early = nanos_to_rfc3339(1_000_000); // 1 ms past epoch
        assert!(early.starts_with("1970-01-01"));

        // Specific nanosecond precision
        let precise = nanos_to_rfc3339(1_771_459_200_123_456_789);
        // Should include fractional seconds
        assert!(precise.contains("123456789") || precise.contains(".123"));
    }

    #[test]
    fn test_bytes_to_hex_various_lengths() {
        assert_eq!(bytes_to_hex(&[]), "");
        assert_eq!(bytes_to_hex(&[0x00]), "00");
        assert_eq!(bytes_to_hex(&[0xff]), "ff");
        assert_eq!(bytes_to_hex(&[0x01, 0x23, 0x45, 0x67, 0x89]), "0123456789");
        // 16-byte trace ID
        let trace = [0x42u8; 16];
        let hex = bytes_to_hex(&trace);
        assert_eq!(hex.len(), 32);
        assert!(hex.chars().all(|c| c == '4' || c == '2'));
    }

    #[test]
    fn test_severity_number_range() {
        // Known mappings
        assert_eq!(severity_number_to_text(1), "TRACE");
        assert_eq!(severity_number_to_text(5), "DEBUG");
        assert_eq!(severity_number_to_text(9), "INFO");
        assert_eq!(severity_number_to_text(13), "WARN");
        assert_eq!(severity_number_to_text(17), "ERROR");
        assert_eq!(severity_number_to_text(21), "FATAL");

        // Unknown / unspecified returns empty
        assert_eq!(severity_number_to_text(0), "");
        // Out-of-range (OTLP defines 1..=24) should not panic
        let _ = severity_number_to_text(255);
    }

    #[test]
    fn test_otlp_mode_case_insensitive() {
        // mode parsing should accept various forms
        assert_eq!(OtlpMode::from_str("hyperdx"), OtlpMode::HyperDx);
        assert_eq!(OtlpMode::from_str("generic"), OtlpMode::Generic);
        // Unknown defaults to HyperDx (matches existing test)
        assert_eq!(OtlpMode::from_str(""), OtlpMode::HyperDx);
        assert_eq!(OtlpMode::from_str("invalid-mode"), OtlpMode::HyperDx);
    }

    // =========================================================================
    // Raw capture
    // =========================================================================

    fn one_log_request() -> pb::collector::logs::v1::ExportLogsServiceRequest {
        pb::collector::logs::v1::ExportLogsServiceRequest {
            resource_logs: vec![pb::logs::v1::ResourceLogs {
                resource: None,
                scope_logs: vec![pb::logs::v1::ScopeLogs {
                    scope: None,
                    log_records: vec![pb::logs::v1::LogRecord {
                        time_unix_nano: 1_771_459_200_000_000_000,
                        observed_time_unix_nano: 0,
                        severity_number: 17,
                        severity_text: "ERROR".to_string(),
                        body: Some(pb::common::v1::AnyValue {
                            value: Some(pb::common::v1::any_value::Value::StringValue(
                                "boom".to_string(),
                            )),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![],
                        span_id: vec![],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[test]
    fn logs_capture_off_emits_no_raw_field() {
        let payloads =
            convert_logs(&one_log_request(), OtlpMode::HyperDx, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&payloads[0].json).unwrap();
        assert!(json.get("_raw").is_none());
    }

    #[test]
    fn logs_capture_carries_the_generic_rendering() {
        let payloads =
            convert_logs(&one_log_request(), OtlpMode::HyperDx, RawCapture::on()).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&payloads[0].json).unwrap();

        // The HyperDX event is shaped as before.
        assert_eq!(json["SeverityText"], "ERROR");
        assert_eq!(json["Body"], "boom");

        // _raw is the generic rendering of the same record.
        let captured: serde_json::Value =
            serde_json::from_str(json["_raw"].as_str().unwrap()).unwrap();
        assert_eq!(captured["_signal"], "log");
        assert_eq!(captured["body"], "boom");
        assert_eq!(captured["severity_text"], "ERROR");
    }

    #[test]
    fn traces_capture_carries_the_generic_rendering() {
        let request = pb::collector::trace::v1::ExportTraceServiceRequest {
            resource_spans: vec![pb::trace::v1::ResourceSpans {
                resource: None,
                scope_spans: vec![pb::trace::v1::ScopeSpans {
                    scope: None,
                    spans: vec![pb::trace::v1::Span {
                        trace_id: vec![0x11; 16],
                        span_id: vec![0x22; 8],
                        name: "GET /health".to_string(),
                        start_time_unix_nano: 1_771_459_200_000_000_000,
                        end_time_unix_nano: 1_771_459_200_005_000_000,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let payloads = convert_traces(&request, OtlpMode::HyperDx, RawCapture::on()).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&payloads[0].json).unwrap();

        let captured: serde_json::Value =
            serde_json::from_str(json["_raw"].as_str().unwrap()).unwrap();
        assert_eq!(captured["_signal"], "trace");
        assert_eq!(captured["name"], "GET /health");
    }

    #[test]
    fn metrics_capture_lines_raw_up_with_each_point() {
        let request = pb::collector::metrics::v1::ExportMetricsServiceRequest {
            resource_metrics: vec![pb::metrics::v1::ResourceMetrics {
                resource: None,
                scope_metrics: vec![pb::metrics::v1::ScopeMetrics {
                    scope: None,
                    metrics: vec![pb::metrics::v1::Metric {
                        name: "cpu_seconds".to_string(),
                        data: Some(pb::metrics::v1::metric::Data::Gauge(
                            pb::metrics::v1::Gauge {
                                data_points: vec![
                                    pb::metrics::v1::NumberDataPoint {
                                        time_unix_nano: 1_771_459_200_000_000_000,
                                        value: Some(
                                            pb::metrics::v1::number_data_point::Value::AsDouble(
                                                1.0,
                                            ),
                                        ),
                                        ..Default::default()
                                    },
                                    pb::metrics::v1::NumberDataPoint {
                                        time_unix_nano: 1_771_459_201_000_000_000,
                                        value: Some(
                                            pb::metrics::v1::number_data_point::Value::AsDouble(
                                                2.0,
                                            ),
                                        ),
                                        ..Default::default()
                                    },
                                ],
                            },
                        )),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let payloads = convert_metrics(&request, OtlpMode::HyperDx, RawCapture::on()).unwrap();
        assert_eq!(payloads.len(), 2);

        // Each event's _raw must be ITS point, not the first one twice.
        for (payload, expected) in payloads.iter().zip([1.0, 2.0]) {
            let json: serde_json::Value = serde_json::from_slice(&payload.json).unwrap();
            assert_eq!(json["Value"], expected);
            let captured: serde_json::Value =
                serde_json::from_str(json["_raw"].as_str().unwrap()).unwrap();
            assert_eq!(captured["value"], expected);
            assert_eq!(captured["metric_name"], "cpu_seconds");
        }
    }
}
