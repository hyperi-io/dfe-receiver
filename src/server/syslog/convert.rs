// Project:   dfe-receiver
// File:      src/server/syslog/convert.rs
// Purpose:   Syslog message parsing and conversion to pipeline JSON
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Syslog message format conversion.
//!
//! An RFC 5424 line goes through `syslog_loose`. An RFC 3164 line is read here,
//! because devices bend that format in ways `syslog_loose` does not follow:
//! Cisco's `*`, `.` and sequence-number prefixes, fractional seconds, RFC 3339
//! stamps, and stamps with no zone, which take [`Converter`]'s default zone.
//! Fields borrow from the line, and the record is written once, straight into
//! the buffer it ships in.

use std::fmt;

use bytes::Bytes;
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, Offset, TimeZone, Timelike, Utc};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use syslog_loose::{ProcId, Protocol, StructuredElement, SyslogFacility, SyslogSeverity, Variant};

use crate::config::RawCapture;
use crate::error::{Error, Result};
use crate::server::raw_capture;

/// The `_source` this listener stamps on every record it builds.
pub const SOURCE: &str = "syslog";

/// The byte order mark, valid only at the start of an RFC 5424 MSG.
const BOM: char = '\u{feff}';

/// Record bytes beyond the line itself: keys, the PRI names and the stamp.
const RECORD_OVERHEAD: usize = 192;

/// How syslog lines become pipeline records.
#[derive(Debug, Clone, Copy)]
pub struct Converter {
    zone: FixedOffset,
    raw_capture: RawCapture,
}

impl Converter {
    /// A converter reading zone-less stamps in `zone`, capturing `_raw` per `raw_capture`.
    #[must_use]
    pub fn new(zone: FixedOffset, raw_capture: RawCapture) -> Self {
        Self { zone, raw_capture }
    }

    /// Convert one syslog line to pipeline-ready JSON bytes.
    ///
    /// The parse never fails: a line that does not parse lands whole in
    /// `message`. With `raw` capture on, `_raw` holds the wire line verbatim,
    /// which tells such a line apart from a clean parse.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when the record cannot be serialised.
    pub fn convert(&self, raw: &str) -> Result<Bytes> {
        let record = parse(raw, self.zone, Utc::now);
        let captured = raw_capture::borrow_str(raw, self.raw_capture);
        let capacity = raw.len() + captured.map_or(0, |(text, _)| text.len()) + RECORD_OVERHEAD;
        let mut out = Vec::with_capacity(capacity);
        serde_json::to_writer(
            &mut out,
            &Output {
                record: &record,
                raw: captured,
            },
        )
        .map_err(|e| Error::Validation(format!("syslog JSON serialisation failed: {e}")))?;
        Ok(Bytes::from(out))
    }
}

/// Convert a syslog line, reading zone-less stamps as UTC.
///
/// # Errors
///
/// [`Error::Validation`] when the record cannot be serialised.
pub fn syslog_to_json(raw: &str, raw_capture: RawCapture) -> Result<Bytes> {
    Converter::new(Utc.fix(), raw_capture).convert(raw)
}

/// A process id as the line gave it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcIdText<'a> {
    Text(&'a str),
    Number(i32),
}

/// The fields of one syslog line, borrowed from it.
#[derive(Debug)]
struct Record<'a> {
    facility: Option<SyslogFacility>,
    severity: Option<SyslogSeverity>,
    timestamp: Option<DateTime<FixedOffset>>,
    hostname: Option<&'a str>,
    appname: Option<&'a str>,
    procid: Option<ProcIdText<'a>>,
    msgid: Option<&'a str>,
    structured_data: Vec<StructuredElement<&'a str>>,
    message: &'a str,
}

impl<'a> Record<'a> {
    fn body(pri: Option<u8>, message: &'a str) -> Self {
        let (facility, severity) = pri.map_or((None, None), syslog_loose::decompose_pri);
        Self {
            facility,
            severity,
            timestamp: None,
            hostname: None,
            appname: None,
            procid: None,
            msgid: None,
            structured_data: Vec::new(),
            message,
        }
    }
}

/// Read a syslog line. `now` gives the year for a stamp that has none.
fn parse(raw: &str, zone: FixedOffset, now: fn() -> DateTime<Utc>) -> Record<'_> {
    let line = raw
        .trim_start_matches(BOM)
        .trim_start()
        .trim_end_matches(|c: char| c == '\0' || c.is_whitespace());
    let (pri, rest) = split_pri(line);
    if pri.is_some() && rest.starts_with("1 ") {
        let msg =
            syslog_loose::parse_message_with_year_tz(line, |_| 0, Some(zone), Variant::RFC5424);
        if matches!(msg.protocol, Protocol::RFC5424(_)) {
            return from_5424(line, msg);
        }
    }
    parse_3164(pri, rest, zone, now)
}

/// The PRI value and the text after it, when `line` opens with a valid `<PRI>`.
fn split_pri(line: &str) -> (Option<u8>, &str) {
    let b = line.as_bytes();
    if b.first() != Some(&b'<') {
        return (None, line);
    }
    let digits = b[1..]
        .iter()
        .take(4)
        .take_while(|d| d.is_ascii_digit())
        .count();
    if !(1..=3).contains(&digits) || b.get(1 + digits) != Some(&b'>') {
        return (None, line);
    }
    match line[1..=digits].parse::<u8>() {
        Ok(pri) if pri <= 191 => (Some(pri), &line[digits + 2..]),
        _ => (None, line),
    }
}

/// An RFC 5424 record, keeping the text of structured data that did not parse.
fn from_5424<'a>(line: &'a str, msg: syslog_loose::Message<&'a str>) -> Record<'a> {
    let mut message = msg.msg;
    if let Some(start) = nth_field_start(line, 6)
        && let Some(end) = offset_in(line, msg.msg)
        && let Some(text) = line.get(start..end)
        && text.starts_with('[')
        && count_sd_elements(text) > msg.structured_data.len()
    {
        message = &line[start..];
    }
    Record {
        facility: msg.facility,
        severity: msg.severity,
        timestamp: msg.timestamp,
        hostname: msg.hostname,
        appname: msg.appname,
        procid: msg.procid.map(|p| match p {
            ProcId::PID(n) => ProcIdText::Number(n),
            ProcId::Name(s) => ProcIdText::Text(s),
        }),
        msgid: msg.msgid,
        structured_data: msg.structured_data,
        message: message.strip_prefix(BOM).unwrap_or(message),
    }
}

/// Where the field after the `n`th space starts.
fn nth_field_start(line: &str, n: usize) -> Option<usize> {
    let mut start = 0;
    for _ in 0..n {
        start += memchr::memchr(b' ', &line.as_bytes()[start..])? + 1;
    }
    Some(start)
}

/// Where `part`, a slice of `line`, starts in it.
fn offset_in(line: &str, part: &str) -> Option<usize> {
    let at = part.as_ptr().addr().checked_sub(line.as_ptr().addr())?;
    (at + part.len() <= line.len()).then_some(at)
}

/// The `[...]` elements in a structured-data field, honouring quoted values.
fn count_sd_elements(text: &str) -> usize {
    let (mut count, mut in_element, mut in_quote, mut escaped) = (0, false, false, false);
    for b in text.bytes() {
        if in_quote {
            match (escaped, b) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => in_quote = false,
                (false, _) => {}
            }
            continue;
        }
        match b {
            b'[' if !in_element => {
                in_element = true;
                count += 1;
            }
            b']' if in_element => in_element = false,
            b'"' if in_element => in_quote = true,
            _ => {}
        }
    }
    count
}

/// An RFC 3164 record from the text after the PRI.
///
/// The header is `STAMP SP HOSTNAME SP TAG: MSG`. Cisco IOS may open it with a
/// sequence number (`123456: `) and a clock marker (`*` or `.`), and ends its
/// stamp with a colon in place of a host: `.407 EST: %LINK-3-UPDOWN: ...`.
fn parse_3164(
    pri: Option<u8>,
    rest: &str,
    zone: FixedOffset,
    now: fn() -> DateTime<Utc>,
) -> Record<'_> {
    let whole = Record::body(pri, rest.trim_start());
    let Some(stamp_at) = stamp_start(rest) else {
        return whole;
    };
    let Some(stamp) = read_stamp(&rest[stamp_at..], zone, now) else {
        return whole;
    };
    let mut record = Record {
        timestamp: Some(stamp.at),
        ..whole
    };
    let after = stamp.rest;
    if let Some(body) = after.strip_prefix(':') {
        record.message = body.trim_start();
    } else if let Some(body) = stamp.fractional.then(|| zone_colon(after)).flatten() {
        record.message = body;
    } else if after.starts_with(char::is_whitespace) || after.is_empty() {
        let (hostname, tag, message) = header_fields(after.trim_start());
        record.hostname = hostname;
        if let Some((name, pid)) = tag {
            record.appname = Some(name);
            record.procid = pid.map(ProcIdText::Text);
        }
        record.message = message;
    } else {
        record.message = after;
    }
    record.message = record.message.strip_prefix(BOM).unwrap_or(record.message);
    record
}

/// Where the stamp starts, past any Cisco sequence number and clock marker.
fn stamp_start(rest: &str) -> Option<usize> {
    let b = rest.as_bytes();
    let digits = b.iter().take_while(|d| d.is_ascii_digit()).count();
    let mut at = if digits > 0 && b[digits..].starts_with(b": ") {
        digits + 2
    } else {
        0
    };
    if matches!(b.get(at), Some(b'*' | b'.')) {
        at += 1;
    }
    let s = &b[at..];
    let opens = s.get(..3).and_then(month_number).is_some() && s.get(3) == Some(&b' ')
        || s.len() > 4 && s[..4].iter().all(u8::is_ascii_digit) && s[4] == b'-';
    opens.then_some(at)
}

/// A stamp read off the front of a header.
struct Stamp<'a> {
    at: DateTime<FixedOffset>,
    /// The stamp carried fractional seconds.
    fractional: bool,
    /// The header after the stamp.
    rest: &'a str,
}

/// An RFC 3339 stamp, or `MMM DD [YYYY] HH:MM:SS[.frac]` read in `zone`.
fn read_stamp(s: &str, zone: FixedOffset, now: fn() -> DateTime<Utc>) -> Option<Stamp<'_>> {
    let b = s.as_bytes();
    if b.first().is_some_and(u8::is_ascii_digit) {
        let end = b
            .iter()
            .position(u8::is_ascii_whitespace)
            .unwrap_or(b.len());
        let at = DateTime::parse_from_rfc3339(&s[..end]).ok()?;
        return Some(Stamp {
            at,
            fractional: false,
            rest: &s[end..],
        });
    }
    let month = month_number(b.get(..3)?)?;
    let at = skip_spaces(b, 3)?;
    let (day, at) = number(b, at, 2)?;
    let at = skip_spaces(b, at)?;
    let (first, end) = number(b, at, 4)?;
    let (year, hour, at) = if end - at == 4 {
        let at = skip_spaces(b, end)?;
        let (hour, at) = number(b, at, 2)?;
        (Some(first), hour, at)
    } else {
        (None, first, end)
    };
    let (minute, at) = number(b, expect(b, at, b':')?, 2)?;
    let (second, mut at) = number(b, expect(b, at, b':')?, 2)?;
    let mut nanos = 0;
    let fractional = b.get(at) == Some(&b'.') && b.get(at + 1).is_some_and(u8::is_ascii_digit);
    if fractional {
        let digits = b[at + 1..]
            .iter()
            .take_while(|d| d.is_ascii_digit())
            .count();
        for (i, d) in b[at + 1..at + 1 + digits].iter().take(9).enumerate() {
            nanos += u32::from(d - b'0') * 10u32.pow(8 - u32::try_from(i).ok()?);
        }
        at += 1 + digits;
    }
    let year = match year {
        Some(year) => i32::try_from(year).ok()?,
        None => year_for(month, now().with_timezone(&zone)),
    };
    let naive =
        NaiveDate::from_ymd_opt(year, month, day)?.and_hms_nano_opt(hour, minute, second, nanos)?;
    Some(Stamp {
        at: zone.from_local_datetime(&naive).single()?,
        fractional,
        rest: &s[at..],
    })
}

/// The year of a stamp in `month` with none of its own, received at `now`: a
/// December stamp arriving in January is last year's, and the reverse next year's.
fn year_for(month: u32, now: DateTime<FixedOffset>) -> i32 {
    match (month, now.month()) {
        (12, 1) => now.year() - 1,
        (1, 12) => now.year() + 1,
        _ => now.year(),
    }
}

/// `Jan` to `Dec` in any case, as 1 to 12.
fn month_number(name: &[u8]) -> Option<u32> {
    const MONTHS: [&[u8; 3]; 12] = [
        b"jan", b"feb", b"mar", b"apr", b"may", b"jun", b"jul", b"aug", b"sep", b"oct", b"nov",
        b"dec",
    ];
    let name: [u8; 3] = name.try_into().ok()?;
    let lower = name.map(|c| c.to_ascii_lowercase());
    let index = MONTHS.iter().position(|m| **m == lower)?;
    u32::try_from(index + 1).ok()
}

/// The index past one or more spaces at `at`.
fn skip_spaces(b: &[u8], at: usize) -> Option<usize> {
    let n = b.get(at..)?.iter().take_while(|&&c| c == b' ').count();
    (n > 0).then_some(at + n)
}

/// The index past `want` at `at`.
fn expect(b: &[u8], at: usize, want: u8) -> Option<usize> {
    (b.get(at) == Some(&want)).then_some(at + 1)
}

/// A number of 1 to `max` digits at `at`, and the index past it.
fn number(b: &[u8], at: usize, max: usize) -> Option<(u32, usize)> {
    let digits = b
        .get(at..)?
        .iter()
        .take_while(|d| d.is_ascii_digit())
        .count();
    if !(1..=max).contains(&digits) {
        return None;
    }
    let value = b[at..at + digits]
        .iter()
        .fold(0, |n, d| n * 10 + u32::from(d - b'0'));
    Some((value, at + digits))
}

/// The body after a Cisco zone abbreviation and colon: ` EST: body`.
fn zone_colon(after: &str) -> Option<&str> {
    let zone = after.strip_prefix(' ')?;
    let len = zone.bytes().take_while(u8::is_ascii_alphanumeric).count();
    let body = zone.get(len..)?.strip_prefix(':').filter(|_| len > 0)?;
    (body.is_empty() || body.starts_with(char::is_whitespace)).then(|| body.trim_start())
}

/// A tag: its name and any pid.
type Tag<'a> = (&'a str, Option<&'a str>);

/// The hostname, tag and message of the header after the stamp.
///
/// A token closing in a colon ends the header: a tag when it reads
/// `name[pid]:`, else the host. Otherwise the first token is the host, and the
/// second is the tag only when it follows the tag grammar.
fn header_fields(fields: &str) -> (Option<&str>, Option<Tag<'_>>, &str) {
    if let Some(body) = fields.strip_prefix(':') {
        return (None, None, body.trim_start());
    }
    let (first, after) = token(fields);
    if first.is_empty() {
        return (None, None, "");
    }
    if let Some(host) = first.strip_suffix(':') {
        if host.contains('[') {
            return match parse_tag(first) {
                Some(tag) => (None, Some(tag), after.trim_start()),
                None => (None, None, fields),
            };
        }
        return (Some(host), None, after.trim_start());
    }
    let hostname = (first != "-").then_some(first);
    let rest = after.trim_start();
    let (second, after) = token(rest);
    match parse_tag(second) {
        Some(tag) => (hostname, Some(tag), after.trim_start()),
        None => (hostname, None, rest),
    }
}

/// The token up to the first whitespace, and the text after it.
fn token(s: &str) -> (&str, &str) {
    s.split_at(s.find(char::is_whitespace).unwrap_or(s.len()))
}

/// RFC 3164 tag grammar: `name:` or `name[pid]:`, the name drawn from ASCII
/// alphanumerics and `-_./`.
fn parse_tag(token: &str) -> Option<Tag<'_>> {
    let tag = token.strip_suffix(':')?;
    let (name, pid) = match tag.strip_suffix(']').and_then(|t| t.split_once('[')) {
        Some((name, pid)) if !pid.is_empty() && !pid.contains(['[', ']']) => (name, Some(pid)),
        Some(_) => return None,
        None => (tag, None),
    };
    let valid = !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/'));
    valid.then_some((name, pid))
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

/// A record and its `_raw` capture, serialised as the pipeline payload.
struct Output<'r, 'a> {
    record: &'r Record<'a>,
    raw: Option<(&'a str, bool)>,
}

impl Serialize for Output<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let r = self.record;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("message", r.message)?;
        if let Some(facility) = r.facility {
            map.serialize_entry("facility", facility.as_str())?;
        }
        if let Some(severity) = r.severity {
            map.serialize_entry("severity", severity.as_str())?;
        }
        if let Some(ts) = r.timestamp {
            map.serialize_entry("timestamp", &Rfc3339(ts))?;
        }
        if let Some(hostname) = r.hostname {
            map.serialize_entry("hostname", hostname)?;
        }
        if let Some(appname) = r.appname {
            map.serialize_entry("appname", appname)?;
        }
        match r.procid {
            Some(ProcIdText::Text(pid)) => map.serialize_entry("procid", pid)?,
            Some(ProcIdText::Number(pid)) => map.serialize_entry("procid", &Display(pid))?,
            None => {}
        }
        if let Some(msgid) = r.msgid {
            map.serialize_entry("msgid", msgid)?;
        }
        if !r.structured_data.is_empty() {
            map.serialize_entry("structured_data", &Elements(&r.structured_data))?;
        }
        map.serialize_entry("_source", SOURCE)?;
        if let Some((text, truncated)) = self.raw {
            if truncated {
                map.serialize_entry(raw_capture::RAW_TRUNCATED_FIELD, &true)?;
            }
            map.serialize_entry(raw_capture::RAW_FIELD, text)?;
        }
        map.end()
    }
}

/// A value written as the JSON string of its `Display`.
struct Display<T>(T);

impl<T: fmt::Display> Serialize for Display<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

/// A stamp written as `to_rfc3339` writes it, with no buffer of its own.
struct Rfc3339(DateTime<FixedOffset>);

impl fmt::Display for Rfc3339 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ts = self.0;
        write!(
            f,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            ts.year(),
            ts.month(),
            ts.day(),
            ts.hour(),
            ts.minute(),
            ts.second()
        )?;
        let nanos = ts.nanosecond() % 1_000_000_000;
        if nanos != 0 {
            if nanos.is_multiple_of(1_000_000) {
                write!(f, ".{:03}", nanos / 1_000_000)?;
            } else if nanos.is_multiple_of(1_000) {
                write!(f, ".{:06}", nanos / 1_000)?;
            } else {
                write!(f, ".{nanos:09}")?;
            }
        }
        let offset = ts.offset().local_minus_utc();
        let sign = if offset < 0 { '-' } else { '+' };
        let minutes = offset.unsigned_abs() / 60;
        write!(f, "{sign}{:02}:{:02}", minutes / 60, minutes % 60)
    }
}

impl Serialize for Rfc3339 {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// Structured data as `[{"id": ..., "params": {...}}]`.
struct Elements<'r, 'a>(&'r [StructuredElement<&'a str>]);

impl Serialize for Elements<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for element in self.0 {
            seq.serialize_element(&Element(element))?;
        }
        seq.end()
    }
}

struct Element<'r, 'a>(&'r StructuredElement<&'a str>);

impl Serialize for Element<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("id", self.0.id)?;
        map.serialize_entry("params", &Params(&self.0.params))?;
        map.end()
    }
}

/// An element's params as an object. A repeated name keeps its last value.
struct Params<'r, 'a>(&'r [(&'a str, &'a str)]);

impl Serialize for Params<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (i, (name, value)) in self.0.iter().enumerate() {
            if !self.0[i + 1..].iter().any(|(later, _)| later == name) {
                map.serialize_entry(name, value)?;
            }
        }
        map.end()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// 2026-11-03T00:00:00Z, so a stamp with no year reads as 2026.
    fn fixed_now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 3, 0, 0, 0).unwrap()
    }

    fn to_json(raw: &str, zone: FixedOffset, capture: RawCapture) -> serde_json::Value {
        let record = parse(raw, zone, fixed_now);
        let out = serde_json::to_vec(&Output {
            record: &record,
            raw: raw_capture::borrow_str(raw, capture),
        })
        .unwrap();
        serde_json::from_slice(&out).unwrap()
    }

    fn json(raw: &str) -> serde_json::Value {
        to_json(raw, Utc.fix(), RawCapture::OFF)
    }

    fn field<'v>(json: &'v serde_json::Value, key: &str) -> Option<&'v str> {
        json.get(key).and_then(|v| v.as_str())
    }

    #[test]
    fn test_rfc5424_with_structured_data() {
        let raw = r#"<165>1 2026-03-03T10:30:00.123+11:00 web01 nginx 1234 ID47 [exampleSDID@32473 iut="3" eventSource="Application"] This is a test message"#;
        let json = json(raw);

        assert_eq!(json["message"], "This is a test message");
        assert_eq!(json["hostname"], "web01");
        assert_eq!(json["appname"], "nginx");
        assert_eq!(json["procid"], "1234");
        assert_eq!(json["msgid"], "ID47");
        assert_eq!(json["_source"], "syslog");
        assert_eq!(json["facility"], "local4");
        assert_eq!(json["severity"], "notice");
        assert_eq!(json["timestamp"], "2026-03-03T10:30:00.123+11:00");

        let sd = json["structured_data"].as_array().unwrap();
        assert_eq!(sd.len(), 1);
        assert_eq!(sd[0]["id"], "exampleSDID@32473");
        assert_eq!(sd[0]["params"]["iut"], "3");
        assert_eq!(sd[0]["params"]["eventSource"], "Application");
    }

    #[test]
    fn a_repeated_sd_param_keeps_its_last_value() {
        let json = json(r#"<14>1 2026-01-15T12:00:00Z h a - - [x@1 k="1" j="2" k="3"] m"#);
        let params = &json["structured_data"][0]["params"];
        assert_eq!(params, &serde_json::json!({"j": "2", "k": "3"}));
    }

    #[test]
    fn test_rfc3164_bsd_format() {
        let json =
            json("<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8");

        assert_eq!(json["hostname"], "mymachine");
        assert_eq!(json["appname"], "su");
        assert_eq!(
            json["message"],
            "'su root' failed for lonvick on /dev/pts/8"
        );
        assert_eq!(json["timestamp"], "2026-10-11T22:14:15+00:00");
        assert_eq!(json["_source"], "syslog");
        assert!(json.get("structured_data").is_none());
        assert!(json.get("msgid").is_none());
    }

    /// One device line and the header split the receiver must give it.
    struct HeaderCase {
        name: &'static str,
        raw: &'static str,
        hostname: Option<&'static str>,
        appname: Option<&'static str>,
        procid: Option<&'static str>,
        message: &'static str,
    }

    const fn case(
        name: &'static str,
        raw: &'static str,
        hostname: Option<&'static str>,
        appname: Option<&'static str>,
        procid: Option<&'static str>,
        message: &'static str,
    ) -> HeaderCase {
        HeaderCase {
            name,
            raw,
            hostname,
            appname,
            procid,
            message,
        }
    }

    const HEADER_CASES: &[HeaderCase] = &[
        // A token is the RFC 3164 tag only when it reads `name:` or `name[pid]:`.
        case(
            "panos_tagless",
            "<10>Jan  5 05:58:47 PAN1 1,2015/01/05 05:58:46,009401011564,THREAT,url,1,2015/01/05 05:58:46,10.0.0.1",
            Some("PAN1"),
            None,
            None,
            "1,2015/01/05 05:58:46,009401011564,THREAT,url,1,2015/01/05 05:58:46,10.0.0.1",
        ),
        case(
            "panos_tagless_fqdn_host",
            "<11>Jan  5 05:38:59 PAN1.exampleCustomer.com 1,2015/01/05 05:38:58,0006C110285,THREAT,vulnerability,1",
            Some("PAN1.exampleCustomer.com"),
            None,
            None,
            "1,2015/01/05 05:38:58,0006C110285,THREAT,vulnerability,1",
        ),
        case(
            "cisco_ios_msec_timezone",
            "<189>Mar  1 18:46:11.407 EST: %LINEPROTO-5-UPDOWN: Line protocol on Interface Vlan1, changed state to up",
            None,
            None,
            None,
            "%LINEPROTO-5-UPDOWN: Line protocol on Interface Vlan1, changed state to up",
        ),
        case(
            "cisco_ios_msec_no_timezone",
            "<189>Jun  1 12:50:26.172: %LINEPROTO-5-UPDOWN: Line protocol on Interface FastEthernet0/2, changed state to down",
            None,
            None,
            None,
            "%LINEPROTO-5-UPDOWN: Line protocol on Interface FastEthernet0/2, changed state to down",
        ),
        case(
            "cisco_ios_no_msec",
            "<189>Mar  1 18:46:11: %SYS-5-CONFIG_I: Configured from console by vty0",
            None,
            None,
            None,
            "%SYS-5-CONFIG_I: Configured from console by vty0",
        ),
        // Cisco clock markers and sequence numbers ahead of the stamp.
        case(
            "cisco_star",
            "<189>*Mar  1 18:46:11.407: %SYS-5-CONFIG_I: Configured from console by vty0",
            None,
            None,
            None,
            "%SYS-5-CONFIG_I: Configured from console by vty0",
        ),
        case(
            "cisco_dot",
            "<189>.Mar  1 18:46:11.407 UTC: %SYS-5-CONFIG_I: Configured from console by vty0",
            None,
            None,
            None,
            "%SYS-5-CONFIG_I: Configured from console by vty0",
        ),
        case(
            "cisco_sequence_number",
            "<189>123456: Mar  1 18:46:11.407 EST: %LINK-3-UPDOWN: Interface Gi0/1, changed state to up",
            None,
            None,
            None,
            "%LINK-3-UPDOWN: Interface Gi0/1, changed state to up",
        ),
        case(
            "cisco_sequence_number_and_star",
            "<189>45: *Mar  1 18:46:11: %SYS-5-CONFIG_I: Configured from console by vty0",
            None,
            None,
            None,
            "%SYS-5-CONFIG_I: Configured from console by vty0",
        ),
        case(
            "sequence_number_without_a_stamp",
            "<189>123456: %SYS-5-CONFIG_I: Configured",
            None,
            None,
            None,
            "123456: %SYS-5-CONFIG_I: Configured",
        ),
        // Fractional seconds ahead of a host.
        case(
            "fraction_then_host_and_tag",
            "<14>Oct 11 22:14:15.123 host app: hello",
            Some("host"),
            Some("app"),
            None,
            "hello",
        ),
        case(
            "fraction_then_host_and_tag_with_pid",
            "<14>Oct 11 22:14:15.123456 web01 sshd[77]: Accepted",
            Some("web01"),
            Some("sshd"),
            Some("77"),
            "Accepted",
        ),
        case(
            "rfc3339_stamp",
            "<13>2026-10-31T10:00:00.5+11:00 web01 sshd[12]: Accepted publickey",
            Some("web01"),
            Some("sshd"),
            Some("12"),
            "Accepted publickey",
        ),
        case(
            "year_in_stamp",
            "<14>Oct 11 2025 22:14:15 host app: x",
            Some("host"),
            Some("app"),
            None,
            "x",
        ),
        case(
            "cisco_asa_device",
            "<166>Oct 11 22:14:15 asa01 %ASA-6-302021: Teardown ICMP connection for faddr 10.0.0.1/0 gaddr 10.0.0.2/0 laddr 10.0.0.2/0",
            Some("asa01"),
            None,
            None,
            "%ASA-6-302021: Teardown ICMP connection for faddr 10.0.0.1/0 gaddr 10.0.0.2/0 laddr 10.0.0.2/0",
        ),
        case(
            "cisco_asa_behind_a_tag",
            "<134>Oct 28 10:00:00 relay relay: %ASA-6-302021: Teardown ICMP connection for faddr 10.0.0.1/0",
            Some("relay"),
            Some("relay"),
            None,
            "%ASA-6-302021: Teardown ICMP connection for faddr 10.0.0.1/0",
        ),
        case(
            "fortigate_key_value",
            "<189>Aug 05 06:28:35 192.168.250.1 date=2016-08-05 time=06:28:35 devname=gotham-fortigate logid=0100026003 type=event",
            Some("192.168.250.1"),
            None,
            None,
            "date=2016-08-05 time=06:28:35 devname=gotham-fortigate logid=0100026003 type=event",
        ),
        case(
            "tag_with_pid",
            "<38>Oct 11 22:14:15 web01 sshd[123]: Accepted publickey for ops from 10.0.0.5 port 50022 ssh2",
            Some("web01"),
            Some("sshd"),
            Some("123"),
            "Accepted publickey for ops from 10.0.0.5 port 50022 ssh2",
        ),
        case(
            "tag_with_slash_and_pid",
            "<22>Oct 11 22:14:15 mail01 postfix/smtpd[4242]: connect from unknown[10.0.0.9]",
            Some("mail01"),
            Some("postfix/smtpd"),
            Some("4242"),
            "connect from unknown[10.0.0.9]",
        ),
        case(
            "tag_with_dot_dash_underscore",
            "<78>Oct 11 22:14:15 host1 run-parts.cron_daily: starting",
            Some("host1"),
            Some("run-parts.cron_daily"),
            None,
            "starting",
        ),
        case(
            "tag_with_empty_body",
            "<13>Oct 11 22:14:15 host1 app:",
            Some("host1"),
            Some("app"),
            None,
            "",
        ),
        case(
            "host_closing_in_a_colon",
            "<13>Oct 11 22:14:15 web01: hello",
            Some("web01"),
            None,
            None,
            "hello",
        ),
        case(
            "tag_with_pid_and_no_host",
            "<13>Oct 11 22:14:15 sshd[12]: hello",
            None,
            Some("sshd"),
            Some("12"),
            "hello",
        ),
        case(
            "token_without_colon",
            "<131>Jun  8 11:54:08 master apache_error [Tue Jun 08 11:54:08.929301 2021] [php7:emerg] rest",
            Some("master"),
            None,
            None,
            "apache_error [Tue Jun 08 11:54:08.929301 2021] [php7:emerg] rest",
        ),
        case(
            "pid_without_colon",
            "<13>Oct 11 22:14:15 host1 app[12] no colon after the pid",
            Some("host1"),
            None,
            None,
            "app[12] no colon after the pid",
        ),
        case(
            "colon_inside_token",
            "<13>Oct 11 22:14:15 host1 https://example.test/x fetched",
            Some("host1"),
            None,
            None,
            "https://example.test/x fetched",
        ),
        case(
            "pid_without_name",
            "<13>Oct 11 22:14:15 host1 [12]: orphan pid",
            Some("host1"),
            None,
            None,
            "[12]: orphan pid",
        ),
        case(
            "non_ascii_token",
            "<13>Oct 11 22:14:15 host1 caf\u{e9}: body",
            Some("host1"),
            None,
            None,
            "caf\u{e9}: body",
        ),
        // RFC 5424 names its fields by position, so the tag grammar does not apply.
        case(
            "rfc5424",
            "<14>1 2026-01-15T12:00:00Z myhost my%app 5678 - - Application started",
            Some("myhost"),
            Some("my%app"),
            Some("5678"),
            "Application started",
        ),
        case(
            "invalid_sd_keeps_its_text",
            "<14>1 2026-01-15T12:00:00Z h a - - [x@1 k=v] hello",
            Some("h"),
            Some("a"),
            None,
            "[x@1 k=v] hello",
        ),
        case(
            "invalid_sd_after_a_valid_one",
            r#"<14>1 2026-01-15T12:00:00Z h a - - [ok@1 a="b"][x@1 k=v] hello"#,
            Some("h"),
            Some("a"),
            None,
            r#"[ok@1 a="b"][x@1 k=v] hello"#,
        ),
        case(
            "nil_sd_then_brackets_in_the_body",
            "<14>1 2026-01-15T12:00:00Z h a - - - [not sd] hello",
            Some("h"),
            Some("a"),
            None,
            "[not sd] hello",
        ),
        // A BOM is stripped, and so is a trailing NUL.
        case(
            "bom_in_5424_msg",
            "<14>1 2026-01-15T12:00:00Z h a - - - \u{feff}hello",
            Some("h"),
            Some("a"),
            None,
            "hello",
        ),
        case(
            "bom_ahead_of_the_pri",
            "\u{feff}<14>1 2026-01-15T12:00:00Z h a - - - hello",
            Some("h"),
            Some("a"),
            None,
            "hello",
        ),
        case(
            "bom_in_3164_body",
            "<14>Oct 11 22:14:15 host app: \u{feff}hello",
            Some("host"),
            Some("app"),
            None,
            "hello",
        ),
        case(
            "trailing_nul",
            "<14>Oct 11 22:14:15 host app: hello\0",
            Some("host"),
            Some("app"),
            None,
            "hello",
        ),
        case(
            "trailing_nul_and_newline",
            "<14>1 2026-01-15T12:00:00Z h a - - - hello\0\n",
            Some("h"),
            Some("a"),
            None,
            "hello",
        ),
        // A PRI with nothing parseable after it stays out of the message.
        case(
            "pri_then_text",
            "<14>hello world",
            None,
            None,
            None,
            "hello world",
        ),
        case(
            "no_header",
            "Just a plain log message",
            None,
            None,
            None,
            "Just a plain log message",
        ),
        case(
            "bad_pri",
            "<999 not really syslog at all",
            None,
            None,
            None,
            "<999 not really syslog at all",
        ),
        case(
            "pri_out_of_range",
            "<192>hello",
            None,
            None,
            None,
            "<192>hello",
        ),
        case("empty", "", None, None, None, ""),
    ];

    #[test]
    fn header_split_follows_the_tag_grammar() {
        let mut failures = Vec::new();
        for c in HEADER_CASES {
            let json = json(c.raw);
            let got = (
                field(&json, "hostname"),
                field(&json, "appname"),
                field(&json, "procid"),
                field(&json, "message").unwrap_or("<missing>"),
            );
            let want = (c.hostname, c.appname, c.procid, c.message);
            if got != want {
                failures.push(format!("{}: got {got:?}, want {want:?}", c.name));
            }
        }
        assert!(failures.is_empty(), "\n{}", failures.join("\n"));
    }

    /// The stamp each line carries, read in `zone` where it has none of its own.
    #[test]
    fn stamps_keep_their_fraction_and_take_the_default_zone() {
        let utc = Utc.fix();
        let east = FixedOffset::east_opt(10 * 3600).unwrap();
        let cases: &[(&str, FixedOffset, Option<&str>)] = &[
            (
                "<189>*Mar  1 18:46:11.407: %SYS-5",
                utc,
                Some("2026-03-01T18:46:11.407+00:00"),
            ),
            (
                "<14>Oct 11 22:14:15.123 host app: hi",
                utc,
                Some("2026-10-11T22:14:15.123+00:00"),
            ),
            (
                "<14>Oct 11 22:14:15.123456 h a: hi",
                utc,
                Some("2026-10-11T22:14:15.123456+00:00"),
            ),
            (
                "<14>Oct 11 22:14:15.1234567891 h a: hi",
                utc,
                Some("2026-10-11T22:14:15.123456789+00:00"),
            ),
            (
                "<14>Oct 11 22:14:15 host app: hi",
                east,
                Some("2026-10-11T22:14:15+10:00"),
            ),
            (
                "<14>Oct 11 2025 22:14:15 host app: hi",
                east,
                Some("2025-10-11T22:14:15+10:00"),
            ),
            (
                "<14>oct 11 22:14:15 host app: hi",
                utc,
                Some("2026-10-11T22:14:15+00:00"),
            ),
            // A stamp with its own offset keeps it, whatever the default.
            (
                "<13>2026-10-31T10:00:00.5+11:00 h a: hi",
                east,
                Some("2026-10-31T10:00:00.500+11:00"),
            ),
            (
                "<14>1 2026-01-15T12:00:00-05:00 h a - - - hi",
                east,
                Some("2026-01-15T12:00:00-05:00"),
            ),
            // A stamp that is not a date is no stamp.
            ("<14>Feb 30 10:00:00 host app: hi", utc, None),
            ("<14>Oct 11 25:14:15 host app: hi", utc, None),
            ("<14>Foo 11 22:14:15 host app: hi", utc, None),
            ("<14>Oct 11 22:14 host app: hi", utc, None),
        ];
        let mut failures = Vec::new();
        for (raw, zone, want) in cases {
            let json = to_json(raw, *zone, RawCapture::OFF);
            let got = field(&json, "timestamp");
            if got != *want {
                failures.push(format!("{raw}: got {got:?}, want {want:?}"));
            }
        }
        assert!(failures.is_empty(), "\n{}", failures.join("\n"));
    }

    #[test]
    fn a_stamp_with_no_year_takes_the_year_it_most_likely_has() {
        let at = |y, m| {
            Utc.with_ymd_and_hms(y, m, 1, 0, 0, 0)
                .unwrap()
                .fixed_offset()
        };
        for (month, now, want) in [
            (12, at(2027, 1), 2026),
            (1, at(2026, 12), 2027),
            (6, at(2026, 11), 2026),
            (11, at(2026, 11), 2026),
            (1, at(2026, 1), 2026),
        ] {
            assert_eq!(year_for(month, now), want, "month {month} at {now}");
        }
    }

    #[test]
    fn rfc3339_matches_chrono() {
        let zones = [0, 11 * 3600, -(5 * 3600 + 30 * 60)];
        let nanos = [0, 500_000_000, 123_000_000, 123_456_000, 123_456_789, 1];
        for offset in zones {
            for n in nanos {
                let zone = FixedOffset::east_opt(offset).unwrap();
                let ts = zone
                    .with_ymd_and_hms(2026, 3, 1, 8, 5, 9)
                    .unwrap()
                    .with_nanosecond(n)
                    .unwrap();
                assert_eq!(Rfc3339(ts).to_string(), ts.to_rfc3339(), "{ts:?}");
            }
        }
    }

    #[test]
    fn test_minimal_rfc3164() {
        let json = json("<13>Mar  3 10:00:00 myhost myapp: Hello world");
        assert_eq!(json["message"], "Hello world");
        assert_eq!(json["hostname"], "myhost");
        assert_eq!(json["facility"], "user");
        assert_eq!(json["severity"], "notice");
    }

    #[test]
    fn a_pri_with_no_header_keeps_its_facility() {
        let json = json("<0>test");
        assert_eq!(json["message"], "test");
        assert_eq!(json["facility"], "kern");
        assert_eq!(json["severity"], "emerg");
        assert_eq!(json["_source"], "syslog");
    }

    #[test]
    fn test_no_priority() {
        let json = json("Just a plain log message");
        assert_eq!(json["message"], "Just a plain log message");
        assert!(json.get("facility").is_none());
        assert!(json.get("severity").is_none());
    }

    proptest::proptest! {
        #[test]
        fn any_line_converts_and_keeps_its_body(tail in "\\PC{0,64}") {
            let raw = format!("<13>Oct 11 22:14:15 host1 {tail}");
            let json = json(&raw);
            let message = json["message"].as_str().unwrap();
            proptest::prop_assert!(raw.contains(message));
        }

        #[test]
        fn any_pri_led_line_converts(raw in "(<[0-9]{1,3}>)?[0-9*. :A-Za-z\\[\\]\"=-]{0,80}") {
            let json = json(&raw);
            let message = json["message"].as_str().unwrap();
            proptest::prop_assert!(raw.contains(message));
        }
    }

    // -----------------------------------------------------------------------
    // Raw capture
    // -----------------------------------------------------------------------

    #[test]
    fn capture_off_emits_no_raw_field() {
        let json = json("<34>Oct 11 22:14:15 mymachine su: failed");
        assert!(json.get("_raw").is_none());
    }

    #[test]
    fn capture_adds_raw_without_disturbing_the_parsed_fields() {
        let raw =
            r#"<165>1 2026-03-03T10:30:00.123+11:00 web01 nginx 1234 ID47 [ex@1 a="b"] hello"#;
        let json = to_json(raw, Utc.fix(), RawCapture::on());

        assert_eq!(json["message"], "hello");
        assert_eq!(json["hostname"], "web01");
        assert_eq!(json["appname"], "nginx");
        assert_eq!(json["procid"], "1234");
        assert_eq!(json["msgid"], "ID47");
        assert_eq!(json["_source"], "syslog");
        assert_eq!(json["_raw"], raw);
    }

    #[test]
    fn raw_keeps_the_pri_and_the_bytes_the_parse_strips() {
        let raw = "\u{feff}<165>1 2026-03-03T10:30:00Z web01 nginx - - - hello\0";
        let json = to_json(raw, Utc.fix(), RawCapture::on());
        assert_eq!(json["facility"], "local4");
        assert_eq!(json["message"], "hello");
        assert_eq!(json["_raw"], raw);
    }

    #[test]
    fn raw_distinguishes_a_malformed_line_from_a_clean_parse() {
        let raw = "<999 not really syslog at all";
        let json = to_json(raw, Utc.fix(), RawCapture::on());
        assert!(json.get("facility").is_none());
        assert_eq!(json["_raw"], raw);
    }

    #[test]
    fn oversized_capture_truncates_or_is_omitted() {
        let raw = "<13>Mar  3 10:00:00 myhost myapp: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        for (policy, want_raw, want_flag) in [
            (
                crate::config::OversizePolicy::Truncate,
                Some(&raw[..16]),
                Some(true),
            ),
            (crate::config::OversizePolicy::Omit, None, None),
        ] {
            let cfg = RawCapture {
                enabled: true,
                max_bytes: 16,
                on_oversize: policy,
            };
            let json = to_json(raw, Utc.fix(), cfg);
            assert_eq!(field(&json, "_raw"), want_raw, "{policy:?}");
            assert_eq!(
                json.get("_raw_truncated")
                    .and_then(serde_json::Value::as_bool),
                want_flag
            );
            assert_eq!(json["hostname"], "myhost");
        }
    }

    #[test]
    fn the_converter_writes_what_the_parse_reads() {
        let raw = "<34>Oct 11 22:14:15 mymachine su: failed";
        let bytes = Converter::new(Utc.fix(), RawCapture::on())
            .convert(raw)
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["hostname"], "mymachine");
        assert_eq!(json["_raw"], raw);
        let bytes = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["appname"], "su");
    }
}
