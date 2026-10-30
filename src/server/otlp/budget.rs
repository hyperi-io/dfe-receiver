// Project:   dfe-receiver
// File:      src/server/otlp/budget.rs
// Purpose:   Bound what an OTLP export may decode into, before it is decoded
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The decode budget of an OTLP export, as [`crate::server::budget`] walks it.
//!
//! A resource and an instrumentation scope are copied into every record
//! beneath them. [`budget::BudgetCodec`] runs the walk on the gRPC listener,
//! [`admit`] on OTLP/HTTP.

use super::pb;
use crate::error::Result;
use crate::server::budget::{self, Budgeted, Shape};

static EMPTY: Shape = Shape::of(&[]);
static KEY_VALUE: Shape = Shape::of(&[(2, &ANY_VALUE)]);
static ANY_VALUE: Shape = Shape::of(&[(5, &ARRAY_VALUE), (6, &KEY_VALUE_LIST)]);
static ARRAY_VALUE: Shape = Shape::of(&[(1, &ANY_VALUE)]);
static KEY_VALUE_LIST: Shape = Shape::of(&[(1, &KEY_VALUE)]);
static RESOURCE: Shape = Shape::shared(&[(1, &KEY_VALUE)]);
static SCOPE: Shape = Shape::shared(&[(3, &KEY_VALUE)]);

static LOG_RECORD: Shape = Shape::record(&[(5, &ANY_VALUE), (6, &KEY_VALUE)]);
static SCOPE_LOGS: Shape = Shape::of(&[(1, &SCOPE), (2, &LOG_RECORD)]);
static RESOURCE_LOGS: Shape = Shape::of(&[(1, &RESOURCE), (2, &SCOPE_LOGS)]);
static LOGS: Shape = Shape::of(&[(1, &RESOURCE_LOGS)]);

static SPAN_EVENT: Shape = Shape::of(&[(3, &KEY_VALUE)]);
static SPAN_LINK: Shape = Shape::of(&[(4, &KEY_VALUE)]);
static SPAN: Shape = Shape::record(&[(9, &KEY_VALUE), (11, &SPAN_EVENT), (13, &SPAN_LINK)]);
static SCOPE_SPANS: Shape = Shape::of(&[(1, &SCOPE), (2, &SPAN)]);
static RESOURCE_SPANS: Shape = Shape::of(&[(1, &RESOURCE), (2, &SCOPE_SPANS)]);
static TRACES: Shape = Shape::of(&[(1, &RESOURCE_SPANS)]);

static EXEMPLAR: Shape = Shape::of(&[(7, &KEY_VALUE)]);
static NUMBER_POINT: Shape = Shape::record(&[(5, &EXEMPLAR), (7, &KEY_VALUE)]);
static HISTOGRAM_POINT: Shape = Shape::record(&[(8, &EXEMPLAR), (9, &KEY_VALUE)]);
static EXPONENTIAL_POINT: Shape =
    Shape::record(&[(1, &KEY_VALUE), (8, &EMPTY), (9, &EMPTY), (11, &EXEMPLAR)]);
static SUMMARY_POINT: Shape = Shape::record(&[(6, &EMPTY), (7, &KEY_VALUE)]);
static NUMBER_POINTS: Shape = Shape::of(&[(1, &NUMBER_POINT)]);
static HISTOGRAM_POINTS: Shape = Shape::of(&[(1, &HISTOGRAM_POINT)]);
static EXPONENTIAL_POINTS: Shape = Shape::of(&[(1, &EXPONENTIAL_POINT)]);
static SUMMARY_POINTS: Shape = Shape::of(&[(1, &SUMMARY_POINT)]);
// Its name, description and unit are copied into every data point.
static METRIC: Shape = Shape::copying(&[
    (5, &NUMBER_POINTS),
    (7, &NUMBER_POINTS),
    (9, &HISTOGRAM_POINTS),
    (10, &EXPONENTIAL_POINTS),
    (11, &SUMMARY_POINTS),
    (12, &KEY_VALUE),
]);
static SCOPE_METRICS: Shape = Shape::of(&[(1, &SCOPE), (2, &METRIC)]);
static RESOURCE_METRICS: Shape = Shape::of(&[(1, &RESOURCE), (2, &SCOPE_METRICS)]);
static METRICS: Shape = Shape::of(&[(1, &RESOURCE_METRICS)]);

impl Budgeted for pb::collector::logs::v1::ExportLogsServiceRequest {
    const WHAT: &'static str = "OTLP export";
    fn shape() -> &'static Shape {
        &LOGS
    }
}

impl Budgeted for pb::collector::trace::v1::ExportTraceServiceRequest {
    const WHAT: &'static str = "OTLP export";
    fn shape() -> &'static Shape {
        &TRACES
    }
}

impl Budgeted for pb::collector::metrics::v1::ExportMetricsServiceRequest {
    const WHAT: &'static str = "OTLP export";
    fn shape() -> &'static Shape {
        &METRICS
    }
}

// The generated client decodes the responses through the same codec.
impl Budgeted for pb::collector::logs::v1::ExportLogsServiceResponse {
    const WHAT: &'static str = "OTLP export";
    fn shape() -> &'static Shape {
        &EMPTY
    }
}

impl Budgeted for pb::collector::trace::v1::ExportTraceServiceResponse {
    const WHAT: &'static str = "OTLP export";
    fn shape() -> &'static Shape {
        &EMPTY
    }
}

impl Budgeted for pb::collector::metrics::v1::ExportMetricsServiceResponse {
    const WHAT: &'static str = "OTLP export";
    fn shape() -> &'static Shape {
        &EMPTY
    }
}

/// Refuse an export over its decode budget.
///
/// # Errors
///
/// [`crate::error::Error::Validation`] naming the budget the export is over.
pub fn admit<T: Budgeted>(body: &[u8]) -> Result<()> {
    budget::admit(body, T::shape(), T::WHAT)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use prost::Message;
    use prost::encoding::WireType;

    use super::*;
    use crate::server::budget::{MAX_COPIED_BYTES, MAX_MESSAGES, MAX_RECORDS};
    use pb::collector::logs::v1::ExportLogsServiceRequest;
    use pb::collector::metrics::v1::ExportMetricsServiceRequest;
    use pb::collector::trace::v1::ExportTraceServiceRequest;

    fn field(tag: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        prost::encoding::encode_key(tag, WireType::LengthDelimited, &mut out);
        prost::encoding::encode_varint(payload.len() as u64, &mut out);
        out.extend_from_slice(payload);
        out
    }

    /// `count` copies of the empty message at field `tag`.
    fn empties(tag: u32, count: usize) -> Vec<u8> {
        field(tag, &[]).repeat(count)
    }

    /// A logs export whose one scope holds `records`.
    fn logs(records: &[u8]) -> Vec<u8> {
        field(1, &field(2, records))
    }

    type Admit = fn(&[u8]) -> Result<()>;

    /// Each OTLP layout over its budget, and prost decoding what the budget admits.
    #[test]
    fn an_export_over_budget_is_refused_before_it_decodes() {
        let logs_admit: Admit = admit::<ExportLogsServiceRequest>;
        let attributes = field(2, &empties(6, MAX_MESSAGES));
        let nested = field(2, &field(5, &field(5, &empties(1, MAX_MESSAGES))));
        let spans = field(1, &field(2, &empties(2, MAX_RECORDS + 1)));
        let points = field(
            1,
            &field(2, &field(2, &field(5, &empties(1, MAX_RECORDS + 1)))),
        );
        // One 1 MiB resource attribute over 100 records is 100 MiB of copies.
        let resource = field(1, &field(1, &vec![b'x'; 1 << 20]));
        let shared = field(1, &[resource, field(2, &empties(2, 100))].concat());
        const { assert!(100 << 20 > MAX_COPIED_BYTES) };
        let cases: [(&str, Vec<u8>, Admit, Option<&str>); 8] = [
            (
                "records at the cap",
                logs(&empties(2, MAX_RECORDS)),
                logs_admit,
                None,
            ),
            (
                "records past the cap",
                logs(&empties(2, MAX_RECORDS + 1)),
                logs_admit,
                Some("records"),
            ),
            (
                "one record, attributes past the cap",
                logs(&attributes),
                logs_admit,
                Some("messages"),
            ),
            (
                "one body, nested values past the cap",
                logs(&nested),
                logs_admit,
                Some("messages"),
            ),
            (
                "a resource copied past the cap",
                shared,
                logs_admit,
                Some("shared bytes"),
            ),
            (
                "spans past the cap",
                spans,
                admit::<ExportTraceServiceRequest>,
                Some("records"),
            ),
            (
                "data points past the cap",
                points,
                admit::<ExportMetricsServiceRequest>,
                Some("records"),
            ),
            ("empty", Vec::new(), logs_admit, None),
        ];
        for (name, body, admit, refused) in cases {
            match (admit(&body), refused) {
                (Ok(()), None) => {}
                (Err(e), Some(text)) => assert!(e.to_string().contains(text), "{name}: {e}"),
                (got, want) => panic!("{name}: got {got:?}, want refusal {want:?}"),
            }
        }

        let admitted =
            ExportLogsServiceRequest::decode(&logs(&empties(2, MAX_RECORDS))[..]).unwrap();
        assert_eq!(
            admitted.resource_logs[0].scope_logs[0].log_records.len(),
            MAX_RECORDS
        );
    }
}
