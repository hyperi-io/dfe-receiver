// Project:   dfe-receiver
// File:      src/server/content_encoding.rs
// Purpose:   Undo a request's Content-Encoding within a size bound
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `Content-Encoding` on an HTTP request body: `identity` and gzip (pure Rust,
//! `flate2`). Any other encoding is refused, so the sender is told rather than
//! the compressed bytes being read as the payload.

use std::borrow::Cow;
use std::io::Read;

use axum::http::{HeaderMap, StatusCode, header::CONTENT_ENCODING};

use crate::error::Error;
use crate::metrics::Metrics;

/// Why a request body could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EncodingError {
    /// An encoding the receiver does not decode (415).
    #[error("Content-Encoding '{0}' is not supported; use gzip or identity")]
    Unsupported(String),
    /// A body that is not valid gzip (400).
    #[error("gzip body is not valid: {0}")]
    Invalid(String),
    /// A body over the limit once decompressed (413).
    #[error("body is over the {0} byte limit once decompressed")]
    TooLarge(usize),
}

impl EncodingError {
    /// The HTTP status the refusal is answered with.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Unsupported(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::Invalid(_) => StatusCode::BAD_REQUEST,
            Self::TooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
        }
    }

    /// Count the refusal on `transport`: an unsupported encoding in
    /// `receiver_unsupported_encoding_total`, the rest as parse failures.
    pub fn count(&self, metrics: &Metrics, transport: &str) {
        match self {
            Self::Unsupported(_) => metrics.inc_unsupported_encoding(transport),
            Self::Invalid(_) | Self::TooLarge(_) => metrics.inc_parse_failure(transport),
        }
    }
}

impl From<EncodingError> for Error {
    fn from(e: EncodingError) -> Self {
        match e {
            EncodingError::Unsupported(_) => Error::UnsupportedMedia(e.to_string()),
            _ => Error::Validation(e.to_string()),
        }
    }
}

/// Undo the request's `Content-Encoding`, keeping the decoded body within `limit` bytes.
///
/// The decoder stops one byte past `limit`, so a small compressed body cannot
/// expand into memory beyond it.
///
/// # Errors
///
/// [`EncodingError`] for an encoding other than `identity` or gzip, a body that
/// is not valid gzip, or one over `limit` once decompressed.
pub fn decode<'a>(
    headers: &HeaderMap,
    body: &'a [u8],
    limit: usize,
) -> Result<Cow<'a, [u8]>, EncodingError> {
    let encoding = headers
        .get(CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map_or("identity", str::trim);
    if encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
        return Ok(Cow::Borrowed(body));
    }
    if !encoding.eq_ignore_ascii_case("gzip") {
        return Err(EncodingError::Unsupported(encoding.to_string()));
    }
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(body)
        .take(limit as u64 + 1)
        .read_to_end(&mut decoded)
        .map_err(|e| EncodingError::Invalid(e.to_string()))?;
    if decoded.len() > limit {
        return Err(EncodingError::TooLarge(limit));
    }
    Ok(Cow::Owned(decoded))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io::Write;

    use super::*;

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    /// Each request: its `Content-Encoding`, body and limit, against the
    /// decoded length or the refusal and status expected.
    #[test]
    fn content_encoding_cases() {
        let body = b"0123456789".repeat(100);
        let packed = gzip(&body);
        let bomb = gzip(&vec![0_u8; 1 << 20]);
        let ok = Ok(body.len());
        let too_large = |n| Err(EncodingError::TooLarge(n));
        let unsupported = |e: &str| Err(EncodingError::Unsupported(e.into()));
        let cases = [
            (None, &body, 1 << 20, ok.clone()),
            (Some("identity"), &body, 1 << 20, ok.clone()),
            (Some("gzip"), &packed, 1 << 20, ok.clone()),
            (Some("GZIP"), &packed, 1 << 20, ok.clone()),
            (Some("gzip"), &packed, body.len(), ok),
            (
                Some("gzip"),
                &packed,
                body.len() - 1,
                too_large(body.len() - 1),
            ),
            (Some("gzip"), &bomb, 1024, too_large(1024)),
            (Some("zstd"), &packed, 1 << 20, unsupported("zstd")),
            (Some("gzip, br"), &packed, 1 << 20, unsupported("gzip, br")),
        ];
        for (encoding, payload, limit, want) in cases {
            let mut headers = HeaderMap::new();
            if let Some(encoding) = encoding {
                headers.insert(CONTENT_ENCODING, encoding.parse().unwrap());
            }
            let got = decode(&headers, payload, limit).map(|b| b.len());
            assert_eq!(got, want, "{encoding:?} limit {limit}");
        }
        let not_gzip = decode(&gzip_header(), b"not gzip at all", 1 << 20);
        assert!(matches!(not_gzip, Err(EncodingError::Invalid(_))));
    }

    fn gzip_header() -> HeaderMap {
        HeaderMap::from_iter([(CONTENT_ENCODING, "gzip".parse().unwrap())])
    }

    /// 415 for the encoding, 400 for a bad body, 413 for a decoded body over the limit.
    #[test]
    fn each_refusal_has_its_status() {
        for (e, status) in [
            (EncodingError::Unsupported("zstd".into()), 415),
            (EncodingError::Invalid("x".into()), 400),
            (EncodingError::TooLarge(1), 413),
        ] {
            assert_eq!(e.status().as_u16(), status, "{e}");
        }
    }
}
