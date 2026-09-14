// Project:   dfe-receiver
// File:      src/server/webhook/auth.rs
// Purpose:   Per-caller webhook authentication (HMAC signature, static header)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Per-caller webhook authentication.
//!
//! A caller proves itself one of two ways:
//!
//! - `hmac`: the request carries a signature header holding
//!   HMAC-SHA256(secret, `"{timestamp}.{body}"`) and a timestamp header
//!   holding the unix seconds that were signed. The timestamp must be within
//!   the caller's tolerance of the receiver's clock, so a captured request
//!   stops verifying once the window closes.
//! - `header`: the request carries a static header whose value is the shared
//!   secret, compared as a SHA-256 hash through [`BearerTokenProvider`] --
//!   the same hashed-set comparison bearer tokens use. There is no replay
//!   defence in this mode; it exists for products that can only attach fixed
//!   headers to a webhook.
//!
//! Both modes load the secret from a `provider:path[:key]` reference and
//! refresh it on an interval, so a rotated secret is picked up without a
//! restart.

use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, HeaderName};
use parking_lot::RwLock;
use ring::hmac;
use tracing::{debug, error, info};

use crate::config::{BearerConfig, WebhookAuthConfig, WebhookAuthMode};
use crate::error::{Error, Result};
use crate::metrics::AuthFailureReason;
use crate::server::auth::{BearerTokenProvider, read_secret_source};

/// Why a request did not authenticate.
///
/// The label is what the client sees in the 401 body and what the
/// `auth_failures_total` reason carries, so it names the check that failed
/// and nothing about the secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthFailure {
    /// No signature header on an `hmac` caller.
    MissingSignature,
    /// The signature header was not a hex HMAC-SHA256 tag, or did not verify.
    InvalidSignature,
    /// No timestamp header on an `hmac` caller.
    MissingTimestamp,
    /// The timestamp header was not unix seconds.
    InvalidTimestamp,
    /// The timestamp is outside the caller's tolerance window.
    StaleSignature,
    /// No secret header on a `header` caller.
    MissingHeader,
    /// The secret header carried a value the caller's secret set does not
    /// contain.
    InvalidHeader,
}

impl AuthFailure {
    /// The label for the 401 body and the metric.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::MissingSignature => "missing_signature",
            Self::InvalidSignature => "invalid_signature",
            Self::MissingTimestamp => "missing_timestamp",
            Self::InvalidTimestamp => "invalid_timestamp",
            Self::StaleSignature => "stale_signature",
            Self::MissingHeader => "missing_auth_header",
            Self::InvalidHeader => "invalid_header_value",
        }
    }

    /// The metric reason this failure counts under.
    #[must_use]
    pub const fn metric_reason(self) -> AuthFailureReason {
        match self {
            Self::MissingSignature | Self::MissingTimestamp | Self::MissingHeader => {
                AuthFailureReason::MissingHeader
            }
            Self::InvalidSignature | Self::InvalidTimestamp => AuthFailureReason::InvalidSignature,
            Self::StaleSignature => AuthFailureReason::StaleSignature,
            Self::InvalidHeader => AuthFailureReason::InvalidHeader,
        }
    }
}

/// One caller's authenticator.
pub enum CallerAuth {
    /// Signed requests with a replay window.
    Hmac(Arc<HmacVerifier>),
    /// A static shared-secret header.
    Header(HeaderVerifier),
}

impl CallerAuth {
    /// Build the authenticator for one caller: load its secret now and keep
    /// refreshing it on the configured interval.
    ///
    /// # Errors
    ///
    /// Returns an error when the secret cannot be read or is empty. An
    /// authenticator with no secret would refuse every request, so the
    /// intake refuses to start instead.
    pub async fn from_config(caller: &str, config: &WebhookAuthConfig) -> Result<Self> {
        let refresh = Duration::from_secs(config.refresh_interval_secs);
        match config.mode {
            WebhookAuthMode::Hmac => {
                let verifier = Arc::new(HmacVerifier::load(caller, config).await?);
                if config.refresh_interval_secs > 0 {
                    HmacVerifier::start_refresh_task(
                        &verifier,
                        caller.to_string(),
                        config.secret_source.clone(),
                        refresh,
                    );
                }
                Ok(Self::Hmac(verifier))
            }
            WebhookAuthMode::Header => {
                let bearer = BearerConfig {
                    tokens: Vec::new(),
                    secret_source: Some(config.secret_source.clone()),
                    refresh_interval_secs: config.refresh_interval_secs,
                };
                let provider = Arc::new(BearerTokenProvider::from_config(&bearer).await?);
                if provider.token_count() == 0 {
                    return Err(Error::Config(format!(
                        "webhook caller '{caller}': secret source '{}' holds no secret",
                        config.secret_source
                    )));
                }
                if config.refresh_interval_secs > 0 {
                    provider
                        .clone()
                        .start_refresh_task(config.secret_source.clone(), refresh);
                }
                Ok(Self::Header(HeaderVerifier {
                    header: header_name(caller, "header", &config.header)?,
                    provider,
                }))
            }
        }
    }

    /// Check one request.
    ///
    /// # Errors
    ///
    /// Returns the check that failed.
    pub fn verify(
        &self,
        headers: &HeaderMap,
        body: &[u8],
        now: SystemTime,
    ) -> std::result::Result<(), AuthFailure> {
        match self {
            Self::Hmac(verifier) => verifier.verify(headers, body, now),
            Self::Header(verifier) => verifier.verify(headers),
        }
    }
}

/// Parse a configured header name once, so a bad one fails at startup.
fn header_name(caller: &str, field: &str, value: &str) -> Result<HeaderName> {
    HeaderName::from_bytes(value.as_bytes()).map_err(|e| {
        Error::Config(format!(
            "webhook caller '{caller}': auth.{field} '{value}' is not a header name: {e}"
        ))
    })
}

/// HMAC-SHA256 over `"{timestamp}.{body}"`, keyed by the caller's secret.
pub struct HmacVerifier {
    key: RwLock<hmac::Key>,
    signature_header: HeaderName,
    timestamp_header: HeaderName,
    tolerance: Duration,
}

impl HmacVerifier {
    /// Build a verifier around an already-known secret.
    ///
    /// # Errors
    ///
    /// Returns an error when the secret is empty or a header name is invalid.
    pub fn new(caller: &str, config: &WebhookAuthConfig, secret: &[u8]) -> Result<Self> {
        if secret.is_empty() {
            return Err(Error::Config(format!(
                "webhook caller '{caller}': the HMAC secret is empty"
            )));
        }
        Ok(Self {
            key: RwLock::new(hmac::Key::new(hmac::HMAC_SHA256, secret)),
            signature_header: header_name(caller, "header", &config.header)?,
            timestamp_header: header_name(caller, "timestamp_header", &config.timestamp_header)?,
            tolerance: Duration::from_secs(config.tolerance_secs),
        })
    }

    async fn load(caller: &str, config: &WebhookAuthConfig) -> Result<Self> {
        let secret = read_secret_source(&config.secret_source).await?;
        Self::new(caller, config, secret.trim().as_bytes())
    }

    /// Replace the key with a freshly read secret.
    fn rotate(&self, secret: &[u8]) {
        *self.key.write() = hmac::Key::new(hmac::HMAC_SHA256, secret);
    }

    /// Re-read the secret on an interval for as long as the verifier lives.
    ///
    /// The task holds a `Weak`, so dropping the verifier ends the task on its
    /// next tick rather than the task keeping the verifier alive.
    fn start_refresh_task(this: &Arc<Self>, caller: String, source: String, interval: Duration) {
        let weak: Weak<Self> = Arc::downgrade(this);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(verifier) = weak.upgrade() else {
                    debug!(caller, "webhook HMAC secret refresh task stopping");
                    break;
                };
                match read_secret_source(&source).await {
                    Ok(secret) if secret.trim().is_empty() => {
                        error!(
                            caller,
                            source,
                            "refreshed webhook HMAC secret is empty, keeping the current key"
                        );
                    }
                    Ok(secret) => {
                        verifier.rotate(secret.trim().as_bytes());
                        info!(caller, "webhook HMAC secret refreshed");
                    }
                    Err(e) => {
                        error!(caller, source, error = %e, "failed to refresh webhook HMAC secret");
                    }
                }
            }
        });
    }

    /// Verify the signature and the replay window for one request.
    ///
    /// The window is checked first: a stale timestamp fails without an HMAC
    /// computation, and a replayed request reports as stale rather than
    /// invalid.
    ///
    /// # Errors
    ///
    /// Returns the check that failed.
    pub fn verify(
        &self,
        headers: &HeaderMap,
        body: &[u8],
        now: SystemTime,
    ) -> std::result::Result<(), AuthFailure> {
        let timestamp = headers
            .get(&self.timestamp_header)
            .ok_or(AuthFailure::MissingTimestamp)?
            .to_str()
            .map_err(|_| AuthFailure::InvalidTimestamp)?
            .trim();
        let signed_at: u64 = timestamp
            .parse()
            .map_err(|_| AuthFailure::InvalidTimestamp)?;
        let now_secs = now
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now_secs.abs_diff(signed_at) > self.tolerance.as_secs() {
            return Err(AuthFailure::StaleSignature);
        }

        let signature = headers
            .get(&self.signature_header)
            .ok_or(AuthFailure::MissingSignature)?
            .to_str()
            .map_err(|_| AuthFailure::InvalidSignature)?
            .trim();
        let hex = signature
            .strip_prefix("sha256=")
            .or_else(|| signature.strip_prefix("SHA256="))
            .unwrap_or(signature);
        let expected = decode_hex(hex).ok_or(AuthFailure::InvalidSignature)?;
        if expected.len() != hmac::HMAC_SHA256.digest_algorithm().output_len() {
            return Err(AuthFailure::InvalidSignature);
        }

        let mut message = Vec::with_capacity(timestamp.len() + 1 + body.len());
        message.extend_from_slice(timestamp.as_bytes());
        message.push(b'.');
        message.extend_from_slice(body);
        hmac::verify(&self.key.read(), &message, &expected)
            .map_err(|_| AuthFailure::InvalidSignature)
    }
}

/// A static shared-secret header, compared as a hash.
pub struct HeaderVerifier {
    header: HeaderName,
    provider: Arc<BearerTokenProvider>,
}

impl HeaderVerifier {
    /// Build a verifier around a provider already holding the secret.
    ///
    /// # Errors
    ///
    /// Returns an error when the header name is invalid.
    pub fn new(caller: &str, header: &str, provider: Arc<BearerTokenProvider>) -> Result<Self> {
        Ok(Self {
            header: header_name(caller, "header", header)?,
            provider,
        })
    }

    /// Check the secret header on one request.
    ///
    /// On an `authorization` header a `Bearer ` prefix is accepted and
    /// stripped, so a product that can only send a bearer token fits this
    /// mode without a second header format.
    ///
    /// # Errors
    ///
    /// Returns the check that failed.
    pub fn verify(&self, headers: &HeaderMap) -> std::result::Result<(), AuthFailure> {
        let value = headers
            .get(&self.header)
            .ok_or(AuthFailure::MissingHeader)?
            .to_str()
            .map_err(|_| AuthFailure::InvalidHeader)?
            .trim();
        let secret = if self.header == axum::http::header::AUTHORIZATION {
            strip_bearer(value)
        } else {
            value
        };
        if secret.is_empty() || !self.provider.is_valid(secret) {
            return Err(AuthFailure::InvalidHeader);
        }
        Ok(())
    }
}

/// Drop a leading `Bearer ` scheme, case-insensitively.
fn strip_bearer(value: &str) -> &str {
    match value.get(..7) {
        Some(scheme) if scheme.eq_ignore_ascii_case("bearer ") => value[7..].trim(),
        _ => value,
    }
}

/// Decode a hex string; `None` when it is not hex or has odd length.
fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    hex.as_bytes()
        .chunks(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            Some((high * 16 + low) as u8)
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"correct-horse-battery-staple";

    fn hmac_config() -> WebhookAuthConfig {
        WebhookAuthConfig {
            mode: WebhookAuthMode::Hmac,
            secret_source: "file:/unused".to_string(),
            tolerance_secs: 300,
            ..WebhookAuthConfig::default()
        }
    }

    fn verifier() -> HmacVerifier {
        HmacVerifier::new("test", &hmac_config(), SECRET).unwrap()
    }

    fn encode_hex(bytes: &[u8]) -> String {
        use std::fmt::Write;
        bytes.iter().fold(String::new(), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
    }

    /// Sign the way a product would: HMAC-SHA256 over `"{ts}.{body}"`.
    fn sign(secret: &[u8], timestamp: u64, body: &[u8]) -> String {
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
        let mut message = timestamp.to_string().into_bytes();
        message.push(b'.');
        message.extend_from_slice(body);
        encode_hex(hmac::sign(&key, &message).as_ref())
    }

    fn signed_headers(timestamp: u64, signature: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-timestamp", timestamp.to_string().parse().unwrap());
        headers.insert("x-signature", signature.parse().unwrap());
        headers
    }

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn a_good_signature_inside_the_window_verifies() {
        let body = br#"{"event":"alert"}"#;
        let ts = 1_800_000_000;
        let headers = signed_headers(ts, &sign(SECRET, ts, body));
        assert_eq!(verifier().verify(&headers, body, at(ts + 10)), Ok(()));
    }

    #[test]
    fn a_sha256_prefixed_signature_verifies() {
        let body = br#"{"event":"alert"}"#;
        let ts = 1_800_000_000;
        let headers = signed_headers(ts, &format!("sha256={}", sign(SECRET, ts, body)));
        assert_eq!(verifier().verify(&headers, body, at(ts)), Ok(()));
    }

    #[test]
    fn a_wrong_secret_is_invalid() {
        let body = br#"{"event":"alert"}"#;
        let ts = 1_800_000_000;
        let headers = signed_headers(ts, &sign(b"other-secret", ts, body));
        assert_eq!(
            verifier().verify(&headers, body, at(ts)),
            Err(AuthFailure::InvalidSignature)
        );
    }

    #[test]
    fn a_tampered_body_is_invalid() {
        let ts = 1_800_000_000;
        let headers = signed_headers(ts, &sign(SECRET, ts, br#"{"amount":1}"#));
        assert_eq!(
            verifier().verify(&headers, br#"{"amount":1000}"#, at(ts)),
            Err(AuthFailure::InvalidSignature)
        );
    }

    #[test]
    fn a_tampered_timestamp_is_invalid() {
        // The timestamp is inside the signed string, so moving it forward to
        // re-open the window breaks the signature.
        let body = br#"{"event":"alert"}"#;
        let ts = 1_800_000_000;
        let headers = signed_headers(ts + 60, &sign(SECRET, ts, body));
        assert_eq!(
            verifier().verify(&headers, body, at(ts + 60)),
            Err(AuthFailure::InvalidSignature)
        );
    }

    #[test]
    fn a_signature_outside_the_window_is_stale() {
        let body = br#"{"event":"alert"}"#;
        let ts = 1_800_000_000;
        let headers = signed_headers(ts, &sign(SECRET, ts, body));
        let v = verifier();
        assert_eq!(
            v.verify(&headers, body, at(ts + 301)),
            Err(AuthFailure::StaleSignature)
        );
        assert_eq!(
            v.verify(&headers, body, at(ts - 301)),
            Err(AuthFailure::StaleSignature),
            "a timestamp from the future is as stale as one from the past"
        );
        assert_eq!(v.verify(&headers, body, at(ts + 300)), Ok(()));
    }

    #[test]
    fn a_missing_signature_header_is_reported_as_such() {
        let ts = 1_800_000_000;
        let mut headers = HeaderMap::new();
        headers.insert("x-timestamp", ts.to_string().parse().unwrap());
        assert_eq!(
            verifier().verify(&headers, b"{}", at(ts)),
            Err(AuthFailure::MissingSignature)
        );
    }

    #[test]
    fn a_missing_timestamp_header_is_reported_as_such() {
        let mut headers = HeaderMap::new();
        headers.insert("x-signature", "00".parse().unwrap());
        assert_eq!(
            verifier().verify(&headers, b"{}", at(1_800_000_000)),
            Err(AuthFailure::MissingTimestamp)
        );
    }

    #[test]
    fn a_non_numeric_timestamp_is_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert("x-timestamp", "yesterday".parse().unwrap());
        headers.insert("x-signature", "00".parse().unwrap());
        assert_eq!(
            verifier().verify(&headers, b"{}", at(1_800_000_000)),
            Err(AuthFailure::InvalidTimestamp)
        );
    }

    #[test]
    fn a_malformed_signature_is_invalid_not_a_panic() {
        let ts = 1_800_000_000;
        for bad in ["", "zz", "abc", "sha256=", "sha256=0g"] {
            let headers = signed_headers(ts, bad);
            assert_eq!(
                verifier().verify(&headers, b"{}", at(ts)),
                Err(AuthFailure::InvalidSignature),
                "signature {bad:?}"
            );
        }
    }

    #[test]
    fn a_rotated_secret_verifies_and_the_old_one_stops() {
        let body = br#"{"event":"alert"}"#;
        let ts = 1_800_000_000;
        let v = verifier();
        v.rotate(b"new-secret");
        let old = signed_headers(ts, &sign(SECRET, ts, body));
        let new = signed_headers(ts, &sign(b"new-secret", ts, body));
        assert_eq!(
            v.verify(&old, body, at(ts)),
            Err(AuthFailure::InvalidSignature)
        );
        assert_eq!(v.verify(&new, body, at(ts)), Ok(()));
    }

    #[test]
    fn an_empty_hmac_secret_is_refused() {
        assert!(HmacVerifier::new("test", &hmac_config(), b"").is_err());
    }

    #[test]
    fn a_bad_header_name_is_refused_at_construction() {
        let config = WebhookAuthConfig {
            header: "not a header".to_string(),
            ..hmac_config()
        };
        assert!(HmacVerifier::new("test", &config, SECRET).is_err());
    }

    fn header_verifier(header: &str) -> HeaderVerifier {
        let provider = Arc::new(BearerTokenProvider::new(&["shared-secret".to_string()]));
        HeaderVerifier::new("test", header, provider).unwrap()
    }

    #[test]
    fn header_mode_accepts_the_shared_secret() {
        let mut headers = HeaderMap::new();
        headers.insert("x-webhook-secret", "shared-secret".parse().unwrap());
        assert_eq!(header_verifier("x-webhook-secret").verify(&headers), Ok(()));
    }

    #[test]
    fn header_mode_rejects_a_wrong_or_missing_value() {
        let v = header_verifier("x-webhook-secret");
        let mut headers = HeaderMap::new();
        assert_eq!(v.verify(&headers), Err(AuthFailure::MissingHeader));
        headers.insert("x-webhook-secret", "shared-secre".parse().unwrap());
        assert_eq!(v.verify(&headers), Err(AuthFailure::InvalidHeader));
        headers.insert("x-webhook-secret", "".parse().unwrap());
        assert_eq!(v.verify(&headers), Err(AuthFailure::InvalidHeader));
    }

    #[test]
    fn header_mode_strips_a_bearer_scheme_on_authorization_only() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer shared-secret".parse().unwrap());
        assert_eq!(header_verifier("authorization").verify(&headers), Ok(()));
        headers.insert("authorization", "bearer shared-secret".parse().unwrap());
        assert_eq!(header_verifier("authorization").verify(&headers), Ok(()));

        // On any other header the scheme is part of the value.
        let mut headers = HeaderMap::new();
        headers.insert("x-webhook-secret", "Bearer shared-secret".parse().unwrap());
        assert_eq!(
            header_verifier("x-webhook-secret").verify(&headers),
            Err(AuthFailure::InvalidHeader)
        );
    }

    #[test]
    fn failure_labels_name_the_check_not_the_secret() {
        for failure in [
            AuthFailure::MissingSignature,
            AuthFailure::InvalidSignature,
            AuthFailure::MissingTimestamp,
            AuthFailure::InvalidTimestamp,
            AuthFailure::StaleSignature,
            AuthFailure::MissingHeader,
            AuthFailure::InvalidHeader,
        ] {
            let label = failure.label();
            assert!(!label.is_empty());
            assert!(label.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'));
        }
        assert_eq!(AuthFailure::StaleSignature.label(), "stale_signature");
    }

    #[test]
    fn hex_decoding_rejects_odd_length_and_non_hex() {
        assert_eq!(decode_hex("00ff"), Some(vec![0, 255]));
        assert_eq!(decode_hex("00F"), None);
        assert_eq!(decode_hex("0g"), None);
        assert_eq!(decode_hex(""), Some(Vec::new()));
    }
}
