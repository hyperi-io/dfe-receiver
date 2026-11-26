// Project:   dfe-receiver
// File:      src/server/tls.rs
// Purpose:   Every listener's TLS: scalo ServerTls load and handshake
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! TLS for every receiver listener.
//!
//! A listener's `tls:` block loads into a scalo [`ServerTls`]. It reads the
//! certificate, key and client CA from files or from [`crate::secrets`]
//! specs (an OpenBao field with `vault:`, a mounted Kubernetes Secret with
//! `file:`), re-reads them every `tls.refresh_interval_secs`, and runs the
//! handshake. A rotated certificate is served from the next handshake on every
//! listener built from it, with no restart.

use std::path::PathBuf;

use scalo::tls::{ClientAuth, ServerTls, ServerTlsConfig, ServerTlsOptions};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::server::TlsStream;
use tracing::debug;

use crate::config::{ConnectionsConfig, TlsConfig};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::secrets;

/// The mode `s` names, `None` for a value that names none.
///
/// Case-insensitive, and `require` and `request` read as `required` and
/// `optional`.
#[must_use]
pub fn parse_client_auth(s: &str) -> Option<ClientAuth> {
    match s.to_ascii_lowercase().as_str() {
        "none" => Some(ClientAuth::None),
        "required" | "require" => Some(ClientAuth::Required),
        "optional" | "request" => Some(ClientAuth::Optional),
        _ => None,
    }
}

/// Whether a listener's TLS handshake turns away a client with no certificate.
///
/// Fixed when the listener starts: a reload changes neither `tls.client_auth`
/// nor `tls.enabled` on a running listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handshake {
    /// Every client presents a certificate that chains to the configured CA.
    RequiresClientCertificate,
    /// A client may connect with no certificate, or with no TLS at all.
    AdmitsAnyClient,
}

impl Handshake {
    /// The handshake a listener built from `tls` runs.
    #[must_use]
    pub fn of(tls: &TlsConfig) -> Self {
        if tls.enabled
            && parse_client_auth(&tls.client_auth) == Some(ClientAuth::Required)
            && (!tls.ca_files.is_empty() || tls.ca_secret.is_some())
        {
            Self::RequiresClientCertificate
        } else {
            Self::AdmitsAnyClient
        }
    }
}

/// `tls.refresh_interval_secs`, else the interval scalo's `ServerTls` defaults to.
fn reload_interval_secs(tls: &TlsConfig) -> u64 {
    tls.refresh_interval_secs
        .unwrap_or_else(|| ServerTlsConfig::default().reload_interval_secs)
}

/// What a gRPC listener's handshake offers: ALPN `h2`, which gRPC clients
/// require and tonic offered itself when it ran the handshake.
#[must_use]
pub fn grpc_options() -> ServerTlsOptions {
    ServerTlsOptions {
        alpn_protocols: vec![b"h2".to_vec()],
        ..ServerTlsOptions::default()
    }
}

/// Load a listener's `tls:` block, `None` when `tls.enabled` is off.
///
/// The handshake timeout is `connections.tls_handshake_timeout_ms`. Call inside
/// the tokio runtime: the reload task starts here and stops once the last clone
/// of the returned `ServerTls` drops.
///
/// # Errors
///
/// A secret spec naming no provider, a certificate, key or CA set both as a
/// file and as a secret or set neither, a client CA missing for `optional` or
/// `required`, an unreadable or unparsable source, and a key that does not
/// match its certificate.
pub async fn load_server_tls(
    tls: &TlsConfig,
    connections: &ConnectionsConfig,
    options: ServerTlsOptions,
) -> Result<Option<ServerTls>> {
    if !tls.enabled {
        return Ok(None);
    }
    for spec in [&tls.cert_secret, &tls.key_secret, &tls.ca_secret]
        .into_iter()
        .flatten()
    {
        secrets::check_spec(spec)
            .map_err(|reason| Error::Config(format!("secret source: {reason}")))?;
    }
    let client_auth = parse_client_auth(&tls.client_auth).ok_or_else(|| {
        Error::Config(format!(
            "tls.client_auth is '{}', which is not one of none, optional, required",
            tls.client_auth
        ))
    })?;
    let config = ServerTlsConfig {
        cert_file: tls.cert_file.as_ref().map(PathBuf::from),
        cert_secret: tls.cert_secret.clone(),
        key_file: tls.key_file.as_ref().map(PathBuf::from),
        key_secret: tls.key_secret.clone(),
        ca_files: tls.ca_files.iter().map(PathBuf::from).collect(),
        ca_secret: tls.ca_secret.clone(),
        client_auth,
        reload_interval_secs: reload_interval_secs(tls),
        handshake_timeout_ms: connections.tls_handshake_timeout_ms,
        ..ServerTlsConfig::default()
    };
    // Boxed for the reason `secrets::read` boxes: the AWS provider's future, awaited
    // inline, overflows rustc's layout query depth in every listener's async block.
    Box::pin(ServerTls::load_with(&config, options))
        .await
        .map(Some)
        .map_err(|e| Error::Tls(e.to_string()))
}

/// Run the TLS handshake on `io` from `peer`, counting a failure on `metrics`.
///
/// `None` for a handshake that failed or timed out. [`ServerTls::accept`] has
/// logged it and counted it on scalo's own metrics, and the connection is
/// dropped.
pub(crate) async fn handshake<IO>(
    tls: &ServerTls,
    io: IO,
    peer: std::net::SocketAddr,
    metrics: &Metrics,
) -> Option<TlsStream<IO>>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    match tls.accept(io, Some(peer)).await {
        Ok(stream) => {
            debug!(peer = %peer, "TLS connection established");
            Some(stream)
        }
        Err(_) => {
            metrics.inc_tls_handshake_failure();
            None
        }
    }
}

/// Information extracted from a validated client certificate.
#[derive(Debug, Clone)]
pub struct ClientCertInfo {
    /// Subject common name.
    pub common_name: Option<String>,
    /// Full subject DN.
    pub subject: String,
    /// Certificate serial number (hex).
    pub serial: String,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn enabled() -> TlsConfig {
        TlsConfig {
            enabled: true,
            cert_file: Some("/etc/ssl/receiver.crt".into()),
            key_file: Some("/etc/ssl/receiver.key".into()),
            ..TlsConfig::default()
        }
    }

    #[test]
    fn client_auth_names_parse_case_insensitively() {
        for (name, mode) in [
            ("none", Some(ClientAuth::None)),
            ("optional", Some(ClientAuth::Optional)),
            ("request", Some(ClientAuth::Optional)),
            ("required", Some(ClientAuth::Required)),
            ("REQUIRED", Some(ClientAuth::Required)),
            ("Require", Some(ClientAuth::Required)),
            ("requried", None),
            ("unknown", None),
            ("", None),
        ] {
            assert_eq!(parse_client_auth(name), mode, "{name:?}");
        }
    }

    #[test]
    fn an_unset_refresh_interval_takes_scalos_default() {
        let set = TlsConfig {
            refresh_interval_secs: Some(7),
            ..TlsConfig::default()
        };
        assert_eq!(reload_interval_secs(&set), 7);
        assert_eq!(
            reload_interval_secs(&TlsConfig::default()),
            ServerTlsConfig::default().reload_interval_secs
        );
    }

    #[tokio::test]
    async fn a_disabled_block_loads_nothing() {
        let loaded = load_server_tls(
            &TlsConfig::default(),
            &ConnectionsConfig::default(),
            ServerTlsOptions::default(),
        )
        .await
        .unwrap();
        assert!(loaded.is_none());
    }

    /// Each refusal names the key the operator has to change.
    #[tokio::test]
    async fn a_block_that_cannot_be_served_is_refused_naming_the_key() {
        let cases = [
            (
                "cert as file and secret",
                TlsConfig {
                    cert_secret: Some("vault:secret/tls:cert".into()),
                    ..enabled()
                },
                "cert_secret",
            ),
            (
                "key as file and secret",
                TlsConfig {
                    key_secret: Some("file:/etc/ssl/receiver.key".into()),
                    ..enabled()
                },
                "key_secret",
            ),
            (
                "no certificate",
                TlsConfig {
                    cert_file: None,
                    ..enabled()
                },
                "cert_file",
            ),
            (
                "no key",
                TlsConfig {
                    key_file: None,
                    ..enabled()
                },
                "key_file",
            ),
            (
                "client certificates with no CA",
                TlsConfig {
                    client_auth: "required".into(),
                    ..enabled()
                },
                "ca_files",
            ),
            (
                "a secret naming no provider",
                TlsConfig {
                    cert_file: None,
                    cert_secret: Some("eyJhbGciOiJIUzI1NiJ9.pasted".into()),
                    ..enabled()
                },
                "provider",
            ),
            (
                "an unknown client_auth",
                TlsConfig {
                    client_auth: "requried".into(),
                    ..enabled()
                },
                "client_auth",
            ),
        ];
        for (case, tls, key) in cases {
            let err = load_server_tls(
                &tls,
                &ConnectionsConfig::default(),
                ServerTlsOptions::default(),
            )
            .await
            .err()
            .unwrap_or_else(|| panic!("{case} must be refused"))
            .to_string();
            assert!(err.contains(key), "{case}: {err}");
        }
    }
}
