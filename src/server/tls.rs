// Project:   dfe-receiver
// File:      src/server/tls.rs
// Purpose:   TLS configuration and utilities
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

//! TLS configuration for HTTP and gRPC servers.
//!
//! Supports TLS termination and mTLS client certificate validation.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use rustls_pemfile::{certs, private_key};
use tokio_rustls::TlsAcceptor;

use crate::config::TlsConfig;
use crate::error::{Error, Result};

/// Load certificates from a PEM file.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)
        .map_err(|e| Error::Tls(format!("failed to open cert file {}: {e}", path.display())))?;
    let mut reader = BufReader::new(file);

    certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Tls(format!("failed to parse certs from {}: {e}", path.display())))
}

/// Load a private key from a PEM file.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path)
        .map_err(|e| Error::Tls(format!("failed to open key file {}: {e}", path.display())))?;
    let mut reader = BufReader::new(file);

    private_key(&mut reader)
        .map_err(|e| Error::Tls(format!("failed to parse key from {}: {e}", path.display())))?
        .ok_or_else(|| Error::Tls(format!("no private key found in {}", path.display())))
}

/// Load CA certificates into a root store.
fn load_ca_certs(path: &Path) -> Result<RootCertStore> {
    let ca_certs = load_certs(path)?;
    let mut root_store = RootCertStore::empty();

    for cert in ca_certs {
        root_store
            .add(cert)
            .map_err(|e| Error::Tls(format!("failed to add CA cert: {e}")))?;
    }

    if root_store.is_empty() {
        return Err(Error::Tls(format!(
            "no valid CA certificates found in {}",
            path.display()
        )));
    }

    Ok(root_store)
}

/// Client authentication mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientAuth {
    /// No client certificate required.
    None,
    /// Client certificate optional (validated if provided).
    Optional,
    /// Client certificate required.
    Required,
}

impl ClientAuth {
    /// Parse from string.
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "required" | "require" => Self::Required,
            "optional" | "request" => Self::Optional,
            _ => Self::None,
        }
    }
}

/// Build a TLS acceptor from configuration.
pub fn build_tls_acceptor(config: &TlsConfig) -> Result<Option<TlsAcceptor>> {
    if !config.enabled {
        return Ok(None);
    }

    let cert_path = config
        .cert_file
        .as_ref()
        .ok_or_else(|| Error::Tls("TLS enabled but cert_file not specified".into()))?;

    let key_path = config
        .key_file
        .as_ref()
        .ok_or_else(|| Error::Tls("TLS enabled but key_file not specified".into()))?;

    let client_auth = ClientAuth::from_str(&config.client_auth);

    // Check for ca_file early if client auth is required
    if client_auth != ClientAuth::None && config.ca_file.is_none() {
        return Err(Error::Tls("client_auth requires ca_file".into()));
    }

    let certs = load_certs(Path::new(cert_path))?;
    let key = load_private_key(Path::new(key_path))?;

    let tls_config = match client_auth {
        ClientAuth::None => ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| Error::Tls(format!("failed to build TLS config: {e}")))?,

        ClientAuth::Optional | ClientAuth::Required => {
            let ca_path = config
                .ca_file
                .as_ref()
                .ok_or_else(|| Error::Tls("client_auth requires ca_file".into()))?;

            let root_store = load_ca_certs(Path::new(ca_path))?;

            let verifier = if client_auth == ClientAuth::Required {
                WebPkiClientVerifier::builder(Arc::new(root_store))
                    .build()
                    .map_err(|e| Error::Tls(format!("failed to build client verifier: {e}")))?
            } else {
                WebPkiClientVerifier::builder(Arc::new(root_store))
                    .allow_unauthenticated()
                    .build()
                    .map_err(|e| Error::Tls(format!("failed to build client verifier: {e}")))?
            };

            ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|e| Error::Tls(format!("failed to build TLS config: {e}")))?
        }
    };

    Ok(Some(TlsAcceptor::from(Arc::new(tls_config))))
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
mod tests {
    use super::*;

    #[test]
    fn test_client_auth_parsing() {
        assert_eq!(ClientAuth::from_str("none"), ClientAuth::None);
        assert_eq!(ClientAuth::from_str("optional"), ClientAuth::Optional);
        assert_eq!(ClientAuth::from_str("required"), ClientAuth::Required);
        assert_eq!(ClientAuth::from_str("REQUIRED"), ClientAuth::Required);
        assert_eq!(ClientAuth::from_str("unknown"), ClientAuth::None);
    }

    #[test]
    fn test_tls_disabled() {
        let config = TlsConfig {
            enabled: false,
            cert_file: None,
            key_file: None,
            ca_file: None,
            client_auth: "none".to_string(),
        };

        let result = build_tls_acceptor(&config);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_tls_enabled_missing_cert() {
        let config = TlsConfig {
            enabled: true,
            cert_file: None,
            key_file: Some("/path/to/key".into()),
            ca_file: None,
            client_auth: "none".to_string(),
        };

        let result = build_tls_acceptor(&config);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("cert_file"));
    }

    #[test]
    fn test_tls_enabled_missing_key() {
        let config = TlsConfig {
            enabled: true,
            cert_file: Some("/path/to/cert".into()),
            key_file: None,
            ca_file: None,
            client_auth: "none".to_string(),
        };

        let result = build_tls_acceptor(&config);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("key_file"));
    }

    #[test]
    fn test_mtls_missing_ca() {
        let config = TlsConfig {
            enabled: true,
            cert_file: Some("/path/to/cert".into()),
            key_file: Some("/path/to/key".into()),
            ca_file: None,
            client_auth: "required".to_string(),
        };

        let result = build_tls_acceptor(&config);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("ca_file"));
    }
}
