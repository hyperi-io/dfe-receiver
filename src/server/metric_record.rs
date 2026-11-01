// Project:   dfe-receiver
// File:      src/server/metric_record.rs
// Purpose:   Metric record fields the OTLP and remote-write listeners share
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Metric record fields the OTLP and Prometheus remote-write listeners write
//! the same way, so a metric looks alike whichever listener took it.

/// One exemplar, as either metric listener reads it.
pub(crate) struct Exemplar {
    pub time_unix_nano: u64,
    pub value: f64,
    /// Lowercase hex, empty where the exemplar names no trace.
    pub trace_id: String,
    /// Lowercase hex, empty where the exemplar names no span.
    pub span_id: String,
    pub attributes: serde_json::Map<String, serde_json::Value>,
}

/// The fields `exemplars` add to a metric record: one `exemplars` list in the
/// generic shape, the `Exemplars.*` arrays of HyperDX's metric tables in its
/// own. None for no exemplars.
pub(crate) fn exemplar_fields(
    exemplars: Vec<Exemplar>,
    hyperdx: bool,
) -> Vec<(String, serde_json::Value)> {
    if exemplars.is_empty() {
        return Vec::new();
    }
    if !hyperdx {
        let list: Vec<serde_json::Value> = exemplars
            .into_iter()
            .map(|exemplar| {
                serde_json::json!({
                    "timestamp": nanos_to_rfc3339(exemplar.time_unix_nano),
                    "value": exemplar.value,
                    "trace_id": exemplar.trace_id,
                    "span_id": exemplar.span_id,
                    "attributes": exemplar.attributes,
                })
            })
            .collect();
        return vec![("exemplars".to_string(), list.into())];
    }
    let mut attributes = Vec::with_capacity(exemplars.len());
    let mut times = Vec::with_capacity(exemplars.len());
    let mut values = Vec::with_capacity(exemplars.len());
    let mut span_ids = Vec::with_capacity(exemplars.len());
    let mut trace_ids = Vec::with_capacity(exemplars.len());
    for exemplar in exemplars {
        attributes.push(serde_json::Value::Object(exemplar.attributes));
        times.push(serde_json::Value::String(nanos_to_ch_datetime(
            exemplar.time_unix_nano,
        )));
        values.push(serde_json::json!(exemplar.value));
        span_ids.push(serde_json::Value::String(exemplar.span_id));
        trace_ids.push(serde_json::Value::String(exemplar.trace_id));
    }
    vec![
        (
            "Exemplars.FilteredAttributes".to_string(),
            attributes.into(),
        ),
        ("Exemplars.TimeUnix".to_string(), times.into()),
        ("Exemplars.Value".to_string(), values.into()),
        ("Exemplars.SpanId".to_string(), span_ids.into()),
        ("Exemplars.TraceId".to_string(), trace_ids.into()),
    ]
}

/// Add `exemplars` to `record`, a JSON object, as [`exemplar_fields`] shapes them.
pub(crate) fn attach_exemplars(
    record: &mut serde_json::Value,
    exemplars: Vec<Exemplar>,
    hyperdx: bool,
) {
    if let Some(object) = record.as_object_mut() {
        object.extend(exemplar_fields(exemplars, hyperdx));
    }
}

/// Nanoseconds since the epoch as RFC 3339 with nanosecond precision, empty
/// for zero, which OTLP uses for "not set".
pub(crate) fn nanos_to_rfc3339(nanos: u64) -> String {
    if nanos == 0 {
        return String::new();
    }
    let secs = (nanos / 1_000_000_000) as i64;
    let nsecs = (nanos % 1_000_000_000) as u32;
    match chrono::DateTime::from_timestamp(secs, nsecs) {
        Some(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        None => String::new(),
    }
}

/// Nanoseconds since the epoch as ClickHouse `DateTime64(9)` text, empty for
/// zero.
pub(crate) fn nanos_to_ch_datetime(nanos: u64) -> String {
    if nanos == 0 {
        return String::new();
    }
    let secs = (nanos / 1_000_000_000) as i64;
    let nsecs = (nanos % 1_000_000_000) as u32;
    match chrono::DateTime::from_timestamp(secs, nsecs) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
        None => String::new(),
    }
}
