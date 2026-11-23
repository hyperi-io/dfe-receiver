// Project:   dfe-receiver
// File:      src/server/time_text.rs
// Purpose:   Instants written as record text, and RFC 3339 stamps read back
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Instants as the text records carry, and RFC 3339 stamps read from senders.
//!
//! A year in 0..=9999 is four digits, and any other is signed and at least
//! four digits (`+10000`, `-0001`).

use std::fmt;

use jiff::civil::DateTime;
use jiff::tz::{Offset, TimeZone};
use jiff::{Timestamp, ZonedDateTime};

/// How many fraction digits a stamp is written with.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Fraction {
    /// Three, six or nine digits, the fewest that hold the nanoseconds, and
    /// none when they are zero.
    Shortest,
    /// Always three.
    Millis,
    /// Always nine.
    Nanos,
}

/// An instant whose seconds and nanoseconds are given apart. `None` when
/// `nanos` is a second or more, or past the instants `Timestamp` holds.
pub(crate) fn instant(secs: i64, nanos: u32) -> Option<Timestamp> {
    let nanos = i32::try_from(nanos).ok().filter(|&n| n < 1_000_000_000)?;
    Timestamp::new(secs, nanos).ok()
}

/// RFC 3339 in UTC ending `+00:00`, the fraction [`Fraction::Shortest`].
pub(crate) fn rfc3339_utc(at: Timestamp) -> String {
    Rfc3339::new(
        Offset::UTC.to_datetime(at),
        Some(Offset::UTC),
        Fraction::Shortest,
    )
    .to_string()
}

/// RFC 3339 in UTC ending `Z`.
pub(crate) fn rfc3339_z(at: Timestamp, fraction: Fraction) -> String {
    Rfc3339::new(Offset::UTC.to_datetime(at), None, fraction).to_string()
}

/// A leap second: `nanos` of one to two seconds past second 59 of a minute,
/// written as second 60 with nine fraction digits and `Z`.
pub(crate) fn leap_second_z(secs: i64, nanos: u32) -> Option<String> {
    if !(1_000_000_000..2_000_000_000).contains(&nanos) || secs.rem_euclid(60) != 59 {
        return None;
    }
    let civil = Offset::UTC.to_datetime(Timestamp::from_second(secs).ok()?);
    let mut out = String::with_capacity(30);
    // Writing to a String never fails.
    let _ = write_civil(&mut out, &civil, 'T');
    out.truncate(out.len() - 2);
    out.push_str("60");
    let _ = write_fraction(
        &mut out,
        i32::try_from(nanos - 1_000_000_000).ok()?,
        Fraction::Nanos,
    );
    out.push('Z');
    Some(out)
}

/// `YYYY-MM-DD HH:MM:SS.fffffffff` in UTC, as `ClickHouse` reads a
/// `DateTime64(9)`.
pub(crate) fn clickhouse_utc(at: Timestamp) -> String {
    let civil = Offset::UTC.to_datetime(at);
    let mut out = String::with_capacity(29);
    // Writing to a String never fails.
    let _ = write_civil(&mut out, &civil, ' ')
        .and_then(|()| write_fraction(&mut out, civil.subsec_nanosecond(), Fraction::Nanos));
    out
}

/// An RFC 3339 stamp: the civil time, then `Z` for no offset or `±HH:MM`.
pub(crate) struct Rfc3339 {
    civil: DateTime,
    offset: Option<Offset>,
    fraction: Fraction,
}

impl Rfc3339 {
    pub(crate) fn new(civil: DateTime, offset: Option<Offset>, fraction: Fraction) -> Self {
        Self {
            civil,
            offset,
            fraction,
        }
    }

    /// A zoned instant in its own offset, the fraction [`Fraction::Shortest`].
    pub(crate) fn zoned(at: &ZonedDateTime) -> Self {
        Self::new(at.datetime(), Some(at.offset()), Fraction::Shortest)
    }
}

impl fmt::Display for Rfc3339 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_civil(f, &self.civil, 'T')?;
        write_fraction(f, self.civil.subsec_nanosecond(), self.fraction)?;
        let Some(offset) = self.offset else {
            return f.write_str("Z");
        };
        let secs = offset.seconds();
        let sign = if secs < 0 { '-' } else { '+' };
        let minutes = secs.unsigned_abs() / 60;
        write!(f, "{sign}{:02}:{:02}", minutes / 60, minutes % 60)
    }
}

/// `YYYY-MM-DD{sep}HH:MM:SS`.
fn write_civil(f: &mut impl fmt::Write, civil: &DateTime, sep: char) -> fmt::Result {
    let year = civil.year();
    if (0..=9999).contains(&year) {
        write!(f, "{year:04}")?;
    } else {
        write!(f, "{year:+05}")?;
    }
    write!(
        f,
        "-{:02}-{:02}{sep}{:02}:{:02}:{:02}",
        civil.month(),
        civil.day(),
        civil.hour(),
        civil.minute(),
        civil.second()
    )
}

fn write_fraction(f: &mut impl fmt::Write, nanos: i32, fraction: Fraction) -> fmt::Result {
    match fraction {
        Fraction::Millis => write!(f, ".{:03}", nanos / 1_000_000),
        Fraction::Nanos => write!(f, ".{nanos:09}"),
        Fraction::Shortest if nanos == 0 => Ok(()),
        Fraction::Shortest if nanos % 1_000_000 == 0 => write!(f, ".{:03}", nanos / 1_000_000),
        Fraction::Shortest if nanos % 1_000 == 0 => write!(f, ".{:06}", nanos / 1_000),
        Fraction::Shortest => write!(f, ".{nanos:09}"),
    }
}

/// An RFC 3339 stamp in the offset it names: `YYYY-MM-DD`, `T`, `t` or a
/// space, `HH:MM:SS`, any number of fraction digits with those past nine
/// dropped, then `Z`, `z` or `+HH:MM`, `-HH:MM` or U+2212 `HH:MM` under 24
/// hours. Second 60 reads as 59.
pub(crate) fn parse_rfc3339(s: &str) -> Option<ZonedDateTime> {
    let b = s.as_bytes();
    if b.len() < 19
        || b[4] != b'-'
        || b[7] != b'-'
        || !matches!(b[10], b'T' | b't' | b' ')
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let field = |at: usize, n: usize| {
        b[at..at + n].iter().try_fold(0_i32, |acc, &c| {
            c.is_ascii_digit().then(|| acc * 10 + i32::from(c - b'0'))
        })
    };
    let second = field(17, 2).filter(|&second| second <= 60)?.min(59);
    let mut rest = &s[19..];
    let mut nanos = 0;
    if let Some(fraction) = rest.strip_prefix('.') {
        let len = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if len == 0 {
            return None;
        }
        nanos = fraction.as_bytes()[..len.min(9)]
            .iter()
            .zip((0..9).rev())
            .fold(0, |acc, (&c, place)| {
                acc + i32::from(c - b'0') * 10_i32.pow(place)
            });
        rest = &fraction[len..];
    }
    let offset_secs = if matches!(rest, "Z" | "z") {
        0
    } else {
        let (sign, hhmm) = match rest.strip_prefix('+') {
            Some(hhmm) => (1, hhmm),
            None => (-1, rest.strip_prefix(['-', '\u{2212}'])?),
        };
        let &[h1, h2, b':', m1 @ b'0'..=b'5', m2] = hhmm.as_bytes() else {
            return None;
        };
        let [h1, h2, m1, m2] =
            [h1, h2, m1, m2].map(|c| c.is_ascii_digit().then(|| i32::from(c - b'0')));
        let secs = (h1? * 10 + h2?) * 3600 + (m1? * 10 + m2?) * 60;
        if secs >= 86_400 {
            return None;
        }
        sign * secs
    };
    let civil = DateTime::new(
        i16::try_from(field(0, 4)?).ok()?,
        i8::try_from(field(5, 2)?).ok()?,
        i8::try_from(field(8, 2)?).ok()?,
        i8::try_from(field(11, 2)?).ok()?,
        i8::try_from(field(14, 2)?).ok()?,
        i8::try_from(second).ok()?,
        nanos,
    )
    .ok()?;
    let zone = TimeZone::fixed(Offset::from_seconds(offset_secs).ok()?);
    civil.to_zoned_datetime(zone).ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// 2026-04-07T07:03:00Z.
    const SECS: i64 = 1_775_545_380;

    #[test]
    fn writers_keep_the_text_chrono_wrote() {
        let at = |nanos| instant(SECS, nanos).unwrap();
        let before_year_zero: Timestamp = "-000001-01-01T00:00:00Z".parse().unwrap();
        for (got, want) in [
            (rfc3339_utc(at(0)), "2026-04-07T07:03:00+00:00"),
            (
                rfc3339_utc(at(120_000_000)),
                "2026-04-07T07:03:00.120+00:00",
            ),
            (
                rfc3339_utc(at(123_456_000)),
                "2026-04-07T07:03:00.123456+00:00",
            ),
            (rfc3339_utc(at(1)), "2026-04-07T07:03:00.000000001+00:00"),
            (
                rfc3339_z(at(0), Fraction::Millis),
                "2026-04-07T07:03:00.000Z",
            ),
            (
                rfc3339_z(at(123_000_000), Fraction::Nanos),
                "2026-04-07T07:03:00.123000000Z",
            ),
            (
                clickhouse_utc(at(123_000_000)),
                "2026-04-07 07:03:00.123000000",
            ),
            (
                rfc3339_z(before_year_zero, Fraction::Nanos),
                "-0001-01-01T00:00:00.000000000Z",
            ),
            (
                leap_second_z(SECS - 1, 1_500_000_000).unwrap(),
                "2026-04-07T07:02:60.500000000Z",
            ),
        ] {
            assert_eq!(got, want);
        }
        assert_eq!(leap_second_z(SECS, 1_500_000_000), None, "not second 59");
        assert_eq!(instant(SECS, 1_000_000_000), None);
    }

    #[test]
    fn rfc3339_stamps_read_as_chrono_read_them() {
        for (text, want) in [
            (
                "2026-04-07T17:03:00.5+10:00",
                Some("2026-04-07T17:03:00.500+10:00"),
            ),
            (
                "2026-04-07t07:03:00.1234567891z",
                Some("2026-04-07T07:03:00.123456789+00:00"),
            ),
            (
                "2026-04-07 01:33:00\u{2212}05:30",
                Some("2026-04-07T01:33:00-05:30"),
            ),
            ("2026-04-07T07:02:60Z", Some("2026-04-07T07:02:59+00:00")),
            ("2026-04-07T07:03:00+1000", None),
            ("2026-04-07T07:03:00+10", None),
            ("2026-04-07T07:03:00+24:00", None),
            ("2026-04-07T07:03:00", None),
            ("2026-02-29T00:00:00Z", None),
            ("2026-04-07T07:03:61Z", None),
            ("2026-04-07T07:03:00.Z", None),
        ] {
            let got = parse_rfc3339(text).map(|at| Rfc3339::zoned(&at).to_string());
            assert_eq!(got.as_deref(), want, "{text:?}");
        }
    }
}
