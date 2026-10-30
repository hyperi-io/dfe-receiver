// Project:   dfe-receiver
// File:      src/server/budget.rs
// Purpose:   Bound what a protobuf request may decode into, before it is decoded
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The decode budget of one protobuf request, counted on the wire.
//!
//! A protobuf message can cost two bytes on the wire and hundreds once
//! decoded, and a message the converter copies into every record beneath it,
//! such as an OTLP resource or a remote-write series' labels, multiplies its
//! bytes by the record count. So a body within the size limit can still
//! decode into gigabytes. The walk here reads only tags and lengths, along the
//! messages a [`Shape`] names, and a request over budget is refused before
//! prost allocates anything. [`BudgetCodec`] runs it on a gRPC listener.

use std::marker::PhantomData;

use bytes::Buf;
use prost::Message;
use prost::encoding::{WireType, decode_key, decode_varint};
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder};
use tonic_prost::ProstEncoder;

use crate::error::{Error, Result};

/// Most records -- log records, spans, metric data points, samples -- one
/// request may carry, the same as one batched `/ingest` request.
pub const MAX_RECORDS: usize = crate::server::http::MAX_BATCH_EVENTS;

/// Most messages of any kind one request may carry: records, their
/// attributes and every nested value, which is room for ten a record.
pub const MAX_MESSAGES: usize = 10 * MAX_RECORDS;

/// Most bytes of shared messages the converter may copy into the records
/// beneath them, summed over every record.
pub const MAX_COPIED_BYTES: usize = 64 * 1024 * 1024;

/// Deepest nesting walked, the depth past which prost refuses a message.
const MAX_DEPTH: u32 = 100;

/// The fields of one message type that hold messages the walk descends into.
pub struct Shape {
    /// Whether one of these is a record the converter emits.
    record: bool,
    /// Whether the converter copies this message into every record beneath
    /// the message that holds it.
    shared: bool,
    /// Whether the converter copies this message's own string and bytes
    /// fields into every record beneath it.
    copies_fields: bool,
    children: &'static [(u32, &'static Shape)],
}

impl Shape {
    /// A message holding `children`.
    pub const fn of(children: &'static [(u32, &'static Shape)]) -> Self {
        Self {
            record: false,
            shared: false,
            copies_fields: false,
            children,
        }
    }

    /// A record holding `children`.
    pub const fn record(children: &'static [(u32, &'static Shape)]) -> Self {
        Self {
            record: true,
            shared: false,
            copies_fields: false,
            children,
        }
    }

    /// A message copied into every record beside it, holding `children`.
    pub const fn shared(children: &'static [(u32, &'static Shape)]) -> Self {
        Self {
            record: false,
            shared: true,
            copies_fields: false,
            children,
        }
    }

    /// A message whose own string and bytes fields are copied into every
    /// record beneath it, holding `children`.
    pub const fn copying(children: &'static [(u32, &'static Shape)]) -> Self {
        Self {
            record: false,
            shared: false,
            copies_fields: true,
            children,
        }
    }

    fn child(&self, tag: u32) -> Option<&'static Shape> {
        self.children
            .iter()
            .find_map(|&(field, shape)| (field == tag).then_some(shape))
    }
}

/// What a request spent of its budget.
#[derive(Debug, Default)]
struct Spent {
    records: usize,
    messages: usize,
    copied: usize,
}

/// Refuse `body`, a `what` laid out as `shape`, when it carries more than
/// [`MAX_RECORDS`] records or [`MAX_MESSAGES`] messages, would copy more than
/// [`MAX_COPIED_BYTES`] into its records, or holds a group, which neither
/// OTLP nor remote write uses and prost would skip unread.
///
/// Any other body that is not valid protobuf is let through for prost to
/// refuse with its own error.
///
/// # Errors
///
/// [`Error::Validation`] naming the budget the request is over.
pub fn admit(body: &[u8], shape: &Shape, what: &str) -> Result<()> {
    let mut spent = Spent::default();
    walk(body, shape, 0, 0, &mut spent)
        .map_err(|over| Error::Validation(format!("{what} {over}, send smaller batches")))
}

/// One field: its tag and the bytes of a length-delimited value.
enum Field<'a> {
    Delimited(u32, &'a [u8]),
    Scalar,
    Group,
}

/// The next field of `buf`, `None` at its end or where it stops parsing.
fn next_field<'a>(buf: &mut &'a [u8]) -> Option<Field<'a>> {
    if buf.is_empty() {
        return None;
    }
    let (tag, wire) = decode_key(buf).ok()?;
    let skip = match wire {
        WireType::Varint => {
            decode_varint(buf).ok()?;
            0
        }
        WireType::SixtyFourBit => 8,
        WireType::ThirtyTwoBit => 4,
        WireType::LengthDelimited => {
            let len = usize::try_from(decode_varint(buf).ok()?).ok()?;
            let value = buf.get(..len)?;
            *buf = &buf[len..];
            return Some(Field::Delimited(tag, value));
        }
        WireType::StartGroup | WireType::EndGroup => return Some(Field::Group),
    };
    *buf = buf.get(skip..)?;
    Some(Field::Scalar)
}

/// Walk one message of `shape`, whose records also carry `inherited` bytes of
/// shared messages from above.
fn walk(
    buf: &[u8],
    shape: &Shape,
    depth: u32,
    inherited: usize,
    spent: &mut Spent,
) -> std::result::Result<(), &'static str> {
    // A shared message may sit after the records on the wire, so it is sized first.
    let mut carried = inherited;
    let mut fields = buf;
    while let Some(field) = next_field(&mut fields) {
        if let Field::Delimited(tag, value) = field {
            let copied = match shape.child(tag) {
                Some(child) => child.shared,
                None => shape.copies_fields,
            };
            if copied {
                carried = carried.saturating_add(value.len());
            }
        }
    }

    let mut fields = buf;
    while let Some(field) = next_field(&mut fields) {
        let (tag, value) = match field {
            Field::Delimited(tag, value) => (tag, value),
            Field::Scalar => continue,
            Field::Group => return Err("carries a protobuf group"),
        };
        let Some(child) = shape.child(tag) else {
            continue;
        };
        spent.messages += 1;
        if child.record {
            spent.records += 1;
            spent.copied = spent.copied.saturating_add(carried);
        }
        if spent.records > MAX_RECORDS {
            return Err("carries more than the most records one request may");
        }
        if spent.messages > MAX_MESSAGES {
            return Err("carries more than the most messages one request may");
        }
        if spent.copied > MAX_COPIED_BYTES {
            return Err("copies more shared bytes into its records than one request may");
        }
        if depth < MAX_DEPTH {
            walk(value, child, depth + 1, carried, spent)?;
        }
    }
    Ok(())
}

/// A protobuf message whose decoded size the budget bounds.
pub trait Budgeted {
    /// What a refusal calls the message.
    const WHAT: &'static str;

    /// Where its repeated messages sit.
    fn shape() -> &'static Shape;
}

/// The prost codec, with [`admit`] run on every message before it decodes.
#[derive(Debug, Clone, Copy)]
pub struct BudgetCodec<T, U>(PhantomData<(T, U)>);

impl<T, U> Default for BudgetCodec<T, U> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T, U> Codec for BudgetCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Budgeted + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = ProstEncoder<T>;
    type Decoder = BudgetDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        ProstEncoder::default()
    }

    fn decoder(&mut self) -> Self::Decoder {
        BudgetDecoder(PhantomData)
    }
}

/// Decodes `U` once its budget admits it.
#[derive(Debug, Clone, Copy)]
pub struct BudgetDecoder<U>(PhantomData<U>);

impl<U: Message + Default + Budgeted> Decoder for BudgetDecoder<U> {
    type Item = U;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> std::result::Result<Option<U>, Status> {
        let message = buf.copy_to_bytes(buf.remaining());
        admit(&message, U::shape(), U::WHAT)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        U::decode(message)
            .map(Some)
            .map_err(|e| Status::internal(e.to_string()))
    }

    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::default()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The protobuf field `tag` holding `payload`.
    fn field(tag: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        prost::encoding::encode_key(tag, WireType::LengthDelimited, &mut out);
        prost::encoding::encode_varint(payload.len() as u64, &mut out);
        out.extend_from_slice(payload);
        out
    }

    static LEAF: Shape = Shape::of(&[]);
    static LABEL: Shape = Shape::shared(&[]);
    static ITEM: Shape = Shape::record(&[(1, &LEAF)]);
    static GROUP: Shape = Shape::of(&[(1, &LABEL), (2, &ITEM)]);
    static REQUEST: Shape = Shape::of(&[(1, &GROUP)]);
    static NAMED: Shape = Shape::copying(&[(2, &ITEM)]);
    static NAMED_REQUEST: Shape = Shape::of(&[(1, &NAMED)]);

    fn request(groups: &[Vec<u8>]) -> Vec<u8> {
        groups.iter().flat_map(|group| field(1, group)).collect()
    }

    /// Each body and its verdict: the three budgets at and past their edge,
    /// a shared message after its records, a group, a copying message's own
    /// field, and bodies prost refuses.
    #[test]
    fn a_request_over_any_budget_is_refused() {
        let items = |n: usize| field(2, &[]).repeat(n);
        let label = |bytes: usize| field(1, &vec![b'x'; bytes]);
        let copies = MAX_COPIED_BYTES / 1024;
        let named =
            |bytes: usize| field(1, &[field(3, &vec![b'x'; bytes]), items(copies)].concat());
        let cases: [(&str, Vec<u8>, &Shape, Option<&str>); 11] = [
            (
                "records at the cap",
                request(&[items(MAX_RECORDS)]),
                &REQUEST,
                None,
            ),
            (
                "records past the cap",
                request(&[items(MAX_RECORDS + 1)]),
                &REQUEST,
                Some("most records"),
            ),
            (
                "messages past the cap",
                request(&[field(2, &field(1, &[]).repeat(MAX_MESSAGES))]),
                &REQUEST,
                Some("most messages"),
            ),
            (
                "shared bytes at the cap",
                request(&[[label(1024), items(copies)].concat()]),
                &REQUEST,
                None,
            ),
            (
                "shared bytes past the cap",
                request(&[[label(1025), items(copies)].concat()]),
                &REQUEST,
                Some("shared bytes"),
            ),
            (
                "a shared message after its records",
                request(&[[items(copies), label(1025)].concat()]),
                &REQUEST,
                Some("shared bytes"),
            ),
            (
                "a copying message's own field at the cap",
                named(1024),
                &NAMED_REQUEST,
                None,
            ),
            (
                "a copying message's own field past the cap",
                named(1025),
                &NAMED_REQUEST,
                Some("shared bytes"),
            ),
            (
                "a group ahead of the records",
                [vec![0x13, 0x14], request(&[items(MAX_RECORDS + 1)])].concat(),
                &REQUEST,
                Some("group"),
            ),
            ("not protobuf", vec![0xff, 0xff, 0xff], &REQUEST, None),
            ("empty", Vec::new(), &REQUEST, None),
        ];
        for (name, body, shape, refused) in cases {
            match (admit(&body, shape, "request"), refused) {
                (Ok(()), None) => {}
                (Err(e), Some(text)) => assert!(e.to_string().contains(text), "{name}: {e}"),
                (got, want) => panic!("{name}: got {got:?}, want refusal {want:?}"),
            }
        }
    }
}
