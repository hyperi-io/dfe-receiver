// Project:   dfe-receiver
// File:      src/server/fluent/scan.rs
// Purpose:   Find where the next MessagePack value ends, across reads, without decoding it
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A resumable MessagePack scanner.
//!
//! A Forward message arrives over many reads. Decoding the buffer on every
//! read to learn whether it is complete costs the whole buffer each time, so a
//! 30 MB message cost minutes of CPU. The scanner keeps its place between
//! reads, reads only markers and lengths, and counts the values it passes, so
//! a message over the decode budget is refused before anything is decoded.

use rmp::Marker;

use crate::server::budget::MAX_MESSAGES;

/// Deepest nesting scanned, the depth rmpv refuses to decode past.
const MAX_DEPTH: usize = 1024;

/// Where the scan of the buffer stands.
#[derive(Debug, PartialEq, Eq)]
pub enum Scan {
    /// The first value of the buffer ends at this offset.
    Complete(usize),
    /// The value continues past the bytes received so far.
    Incomplete,
    /// The value holds more than [`MAX_MESSAGES`] values or nests deeper than
    /// rmpv decodes.
    OverBudget,
    /// The bytes are not MessagePack.
    Malformed,
}

/// The scan of one value, kept between reads of the buffer it is in.
#[derive(Debug, Default)]
pub struct Scanner {
    /// Offset of the next element to read.
    at: usize,
    /// Elements still to come in each open array or map, innermost last.
    open: Vec<u64>,
    /// Values passed so far.
    values: usize,
}

/// One element's header: its bytes, its payload's bytes and its children.
struct Head {
    len: usize,
    payload: usize,
    children: u64,
}

/// The header of the element `buf` opens with, `None` while it is cut short.
fn head(buf: &[u8]) -> Option<std::result::Result<Head, ()>> {
    let first = *buf.first()?;
    let number = |width: usize| -> Option<u64> {
        let bytes = buf.get(1..=width)?;
        Some(bytes.iter().fold(0, |n, &b| (n << 8) | u64::from(b)))
    };
    let fixed = |len: usize, payload: usize| Head {
        len,
        payload,
        children: 0,
    };
    let sized = |width: usize, extra: usize| -> Option<Head> {
        let payload = usize::try_from(number(width)?).ok()?;
        Some(fixed(1 + width + extra, payload))
    };
    let container = |width: usize, per: u64| -> Option<Head> {
        Some(Head {
            len: 1 + width,
            payload: 0,
            children: number(width)?.checked_mul(per)?,
        })
    };
    let head = match Marker::from_u8(first) {
        Marker::FixPos(_) | Marker::FixNeg(_) | Marker::Null | Marker::True | Marker::False => {
            fixed(1, 0)
        }
        Marker::U8 | Marker::I8 => fixed(2, 0),
        Marker::U16 | Marker::I16 => fixed(3, 0),
        Marker::U32 | Marker::I32 | Marker::F32 => fixed(5, 0),
        Marker::U64 | Marker::I64 | Marker::F64 => fixed(9, 0),
        Marker::FixStr(len) => fixed(1, usize::from(len)),
        Marker::Str8 | Marker::Bin8 => sized(1, 0)?,
        Marker::Str16 | Marker::Bin16 => sized(2, 0)?,
        Marker::Str32 | Marker::Bin32 => sized(4, 0)?,
        Marker::FixExt1 => fixed(2, 1),
        Marker::FixExt2 => fixed(2, 2),
        Marker::FixExt4 => fixed(2, 4),
        Marker::FixExt8 => fixed(2, 8),
        Marker::FixExt16 => fixed(2, 16),
        Marker::Ext8 => sized(1, 1)?,
        Marker::Ext16 => sized(2, 1)?,
        Marker::Ext32 => sized(4, 1)?,
        Marker::FixArray(n) => Head {
            len: 1,
            payload: 0,
            children: u64::from(n),
        },
        Marker::FixMap(n) => Head {
            len: 1,
            payload: 0,
            children: 2 * u64::from(n),
        },
        Marker::Array16 => container(2, 1)?,
        Marker::Array32 => container(4, 1)?,
        Marker::Map16 => container(2, 2)?,
        Marker::Map32 => container(4, 2)?,
        Marker::Reserved => return Some(Err(())),
    };
    Some(Ok(head))
}

impl Scanner {
    /// Carry on scanning `buf`, which holds every byte the earlier calls saw
    /// and any that arrived since. After [`Scan::Complete`] the next call
    /// scans the value after it, the budget counting both. A buffer whose
    /// front was removed needs a new scanner.
    pub fn scan(&mut self, buf: &[u8]) -> Scan {
        loop {
            let Some(rest) = buf.get(self.at..) else {
                return Scan::Incomplete;
            };
            let head = match head(rest) {
                None => return Scan::Incomplete,
                Some(Err(())) => return Scan::Malformed,
                Some(Ok(head)) => head,
            };
            let Some(end) = self
                .at
                .checked_add(head.len)
                .and_then(|at| at.checked_add(head.payload))
            else {
                return Scan::Malformed;
            };
            if end > buf.len() {
                return Scan::Incomplete;
            }
            self.at = end;
            self.values += 1;
            if self.values > MAX_MESSAGES {
                return Scan::OverBudget;
            }
            if head.children > 0 {
                if self.open.len() == MAX_DEPTH {
                    return Scan::OverBudget;
                }
                self.open.push(head.children);
            } else {
                self.close_one();
            }
            if self.open.is_empty() {
                return Scan::Complete(self.at);
            }
        }
    }

    /// The values `buf` holds end to end, under one budget.
    ///
    /// # Errors
    ///
    /// The [`Scan`] that stopped it: a cut value, the budget, or bytes that are
    /// not MessagePack.
    pub fn count(buf: &[u8]) -> std::result::Result<usize, Scan> {
        let mut scanner = Self::default();
        let mut values = 0;
        while scanner.at < buf.len() {
            match scanner.scan(buf) {
                Scan::Complete(_) => values += 1,
                stopped => return Err(stopped),
            }
        }
        Ok(values)
    }

    /// One element ended: count it against its container, and close every
    /// container it was the last of.
    fn close_one(&mut self) {
        while let Some(left) = self.open.last_mut() {
            *left -= 1;
            if *left > 0 {
                return;
            }
            self.open.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn whole(bytes: &[u8]) -> Scan {
        Scanner::default().scan(bytes)
    }

    /// A value is complete at its last byte and not before, whichever way the
    /// bytes arrive, and a value past the budget or not MessagePack is said so.
    #[test]
    fn the_scan_finds_where_each_value_ends() {
        let forward = [0x92, 0xa1, b't', 0x91, 0x92, 0x00, 0x80];
        let packed = [0x92, 0xa1, b't', 0xc4, 0x03, 1, 2, 3];
        let cases: [(&str, &[u8], Scan); 9] = [
            ("a scalar", &[0x05, 0xff], Scan::Complete(1)),
            ("a forward message", &forward, Scan::Complete(forward.len())),
            ("a bin payload", &packed, Scan::Complete(packed.len())),
            ("a cut map", &forward[..6], Scan::Incomplete),
            ("a cut bin payload", &packed[..7], Scan::Incomplete),
            ("an empty array then more", &[0x90, 0x01], Scan::Complete(1)),
            ("a cut length", &[0xdd, 0x00], Scan::Incomplete),
            ("the reserved marker", &[0x91, 0xc1], Scan::Malformed),
            (
                "nested past the depth",
                &[0x91; MAX_DEPTH + 1],
                Scan::OverBudget,
            ),
        ];
        for (name, bytes, want) in cases {
            assert_eq!(whole(bytes), want, "{name}");
        }

        let mut many = vec![0xdd];
        many.extend_from_slice(
            &u32::try_from(MAX_MESSAGES)
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        );
        many.extend(std::iter::repeat_n(0xc0, MAX_MESSAGES));
        assert_eq!(whole(&many), Scan::OverBudget);

        // A run of values counts under one budget.
        assert_eq!(Scanner::count(&[forward, forward].concat()), Ok(2));
        assert_eq!(Scanner::count(&forward[..5]), Err(Scan::Incomplete));
        let nils = vec![0xc0; MAX_MESSAGES + 1];
        assert_eq!(Scanner::count(&nils), Err(Scan::OverBudget));

        // Fed a byte at a time, the scan resumes where it stopped.
        let mut scanner = Scanner::default();
        for end in 1..forward.len() {
            assert_eq!(scanner.scan(&forward[..end]), Scan::Incomplete, "{end}");
        }
        assert_eq!(scanner.scan(&forward), Scan::Complete(forward.len()));
    }
}
