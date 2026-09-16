// Project:   dfe-receiver
// File:      src/secrets.rs
// Purpose:   Credential-spec reads for the auth, webhook and TLS paths
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Reading a `provider:path[:key]` credential spec.
//!
//! Every spec the receiver accepts -- bearer tokens, webhook caller secrets,
//! TLS certificate material -- resolves through
//! [`scalo::secrets::resolve`], which owns the spec grammar, the OpenBao
//! connection and the cache policy. The receiver adds one check in front of it:
//! scalo returns a spec whose prefix it does not recognise as a literal value,
//! so a token or a PEM body pasted into a spec field would otherwise be used as
//! the secret itself rather than refused.
//!
//! `aws:` is in the accepted vocabulary on purpose even though the receiver
//! does not enable scalo's `secrets-aws` feature: letting the spec reach scalo
//! gets the operator `aws: spec requires the secrets-aws feature to be enabled`
//! when the receiver starts, rather than an unrecognised prefix silently
//! becoming the token.

use crate::error::{Error, Result};

/// The provider prefixes [`scalo::secrets::resolve`] recognises.
///
/// Anything else is a literal to scalo, which is why [`check_spec`] refuses it.
pub const PROVIDERS: [&str; 6] = ["file", "vault", "bao", "openbao", "env", "aws"];

/// Check that a spec names a provider, returning why it does not.
///
/// The value is deliberately left out of the no-provider message. A spec with
/// no `provider:` prefix is most often the secret itself pasted into the field,
/// and this message reaches the log.
///
/// # Errors
///
/// Returns the reason the spec is not a `provider:path[:key]` reference.
pub fn check_spec(spec: &str) -> std::result::Result<(), String> {
    match spec.split_once(':') {
        Some((provider, _)) if PROVIDERS.contains(&provider) => Ok(()),
        Some((provider, _)) => Err(format!(
            "'{provider}' is not a secret provider -- use one of {}",
            PROVIDERS.join(", ")
        )),
        None => Err(format!(
            "not a 'provider:path[:key]' reference -- use one of {}. The value is \
             not repeated here because a spec naming no provider is usually the \
             secret itself",
            PROVIDERS.join(", ")
        )),
    }
}

/// Resolve a `provider:path[:key]` spec to its text.
///
/// # Errors
///
/// Returns an error when the spec names no provider, or when scalo cannot serve
/// it -- an unreachable OpenBao, an unreadable or empty file, a `vault:` spec
/// with no key, an `aws:` spec the build cannot serve.
pub async fn read(spec: &str) -> Result<String> {
    check_spec(spec).map_err(|reason| Error::Config(format!("secret source: {reason}")))?;
    scalo::secrets::resolve(spec)
        .await
        .map_err(|e| Error::Config(format!("secret source '{spec}' did not resolve: {e}")))
}

/// Resolve a `provider:path[:key]` spec to its bytes.
///
/// Certificate and key material is PEM, which is UTF-8, so the resolved text is
/// the byte payload.
///
/// # Errors
///
/// As [`read`].
pub async fn read_bytes(spec: &str) -> Result<Vec<u8>> {
    Ok(read(spec).await?.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_provider_scalo_serves_is_accepted() {
        for provider in PROVIDERS {
            assert!(
                check_spec(&format!("{provider}:some/path:key")).is_ok(),
                "{provider}: must be accepted"
            );
        }
    }

    #[test]
    fn a_prefix_scalo_does_not_serve_is_refused() {
        let reason = check_spec("gcp:projects/p/secrets/s").unwrap_err();
        assert!(reason.contains("gcp"), "the refusal must name it: {reason}");
    }

    /// scalo returns a spec it does not recognise as a literal, so without this
    /// check a pasted token would be used as the secret.
    #[test]
    fn a_value_with_no_provider_is_refused_without_being_echoed() {
        let reason = check_spec("eyJhbGciOiJIUzI1NiJ9.pasted-token").unwrap_err();
        assert!(
            !reason.contains("pasted-token"),
            "the refusal must not echo the value: {reason}"
        );
        assert!(
            reason.contains("provider:path"),
            "the refusal must say what the field takes: {reason}"
        );
    }

    /// The receiver does not build scalo's `secrets-aws`, so the spec has to
    /// reach scalo and come back naming the feature -- not pass through as a
    /// literal and become the token.
    #[tokio::test]
    async fn an_aws_spec_is_refused_by_name() {
        let err = read("aws:prod/auth/tokens:bearer")
            .await
            .expect_err("the receiver cannot serve an aws: spec")
            .to_string();
        assert!(
            err.contains("secrets-aws"),
            "the refusal must name the feature: {err}"
        );
        assert!(
            !err.contains("provider not configured"),
            "the spec must be refused by name, not as an unconfigured provider: {err}"
        );
    }

    /// The resolver this replaced read a keyless `vault:` spec as key `value`,
    /// which is a guess at which field of the secret was wanted.
    #[tokio::test]
    async fn a_keyless_vault_spec_is_refused() {
        for spec in ["vault:secret/data/auth", "vault:secret/data/auth:"] {
            let err = read(spec)
                .await
                .expect_err("a vault spec with no key names no field")
                .to_string();
            assert!(
                err.contains("invalid credential spec"),
                "{spec} must be refused as malformed: {err}"
            );
        }
    }

    #[tokio::test]
    async fn a_file_spec_reads_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "mounted-token").unwrap();

        let text = read(&format!("file:{}", path.display())).await.unwrap();
        assert_eq!(text, "mounted-token");
    }

    #[tokio::test]
    async fn read_bytes_hands_back_the_pem_body() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cert.pem");
        std::fs::write(&path, "-----BEGIN CERTIFICATE-----\n").unwrap();

        let bytes = read_bytes(&format!("file:{}", path.display()))
            .await
            .unwrap();
        assert_eq!(bytes, b"-----BEGIN CERTIFICATE-----\n");
    }
}
