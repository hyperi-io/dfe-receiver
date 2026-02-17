// Project:   dfe-receiver
// File:      src/server/tls.rs
// Purpose:   TLS configuration and utilities
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! TLS configuration for HTTP and gRPC servers.
//!
//! Supports TLS termination and mTLS client certificate validation.
//! Certificates can be loaded from files or secret managers (Vault, AWS, files).

use std::fs::File;
use std::io::{BufReader, Cursor};
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use rustls_pemfile::{certs, private_key};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info};

use crate::config::TlsConfig;
use crate::error::{Error, Result};

/// Load certificates from a PEM file.
fn load_certs_from_file(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)
        .map_err(|e| Error::Tls(format!("failed to open cert file {}: {e}", path.display())))?;
    let mut reader = BufReader::new(file);

    certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            Error::Tls(format!(
                "failed to parse certs from {}: {e}",
                path.display()
            ))
        })
}

/// Load certificates from PEM bytes.
fn load_certs_from_bytes(pem_data: &[u8], source: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader = Cursor::new(pem_data);

    certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Tls(format!("failed to parse certs from {source}: {e}")))
}

/// Load a private key from a PEM file.
fn load_private_key_from_file(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path)
        .map_err(|e| Error::Tls(format!("failed to open key file {}: {e}", path.display())))?;
    let mut reader = BufReader::new(file);

    private_key(&mut reader)
        .map_err(|e| Error::Tls(format!("failed to parse key from {}: {e}", path.display())))?
        .ok_or_else(|| Error::Tls(format!("no private key found in {}", path.display())))
}

/// Load a private key from PEM bytes.
fn load_private_key_from_bytes(pem_data: &[u8], source: &str) -> Result<PrivateKeyDer<'static>> {
    let mut reader = Cursor::new(pem_data);

    private_key(&mut reader)
        .map_err(|e| Error::Tls(format!("failed to parse key from {source}: {e}")))?
        .ok_or_else(|| Error::Tls(format!("no private key found in {source}")))
}

/// Load CA certificates into a root store from a file.
fn load_ca_certs_from_file(path: &Path) -> Result<RootCertStore> {
    let ca_certs = load_certs_from_file(path)?;
    build_root_store(ca_certs, &path.display().to_string())
}

/// Load CA certificates into a root store from bytes.
fn load_ca_certs_from_bytes(pem_data: &[u8], source: &str) -> Result<RootCertStore> {
    let ca_certs = load_certs_from_bytes(pem_data, source)?;
    build_root_store(ca_certs, source)
}

/// Build a root certificate store from certificates.
fn build_root_store(ca_certs: Vec<CertificateDer<'static>>, source: &str) -> Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    for cert in ca_certs {
        root_store
            .add(cert)
            .map_err(|e| Error::Tls(format!("failed to add CA cert: {e}")))?;
    }

    if root_store.is_empty() {
        return Err(Error::Tls(format!(
            "no valid CA certificates found in {source}"
        )));
    }

    Ok(root_store)
}

/// Load a secret from the secret manager.
///
/// Format: "provider:path:key" (e.g., "vault:secret/tls:cert")
async fn load_from_secret(source: &str) -> Result<Vec<u8>> {
    let parts: Vec<&str> = source.splitn(3, ':').collect();
    if parts.len() < 2 {
        return Err(Error::Config(format!(
            "Invalid secret source format: {source}. Expected 'provider:path' or 'provider:path:key'"
        )));
    }

    let provider_name = parts[0];
    let path = parts[1];
    let key = parts.get(2).copied();

    use hyperi_rustlib::secrets::{SecretSource, SecretsConfig, SecretsManager};

    let secret_source = match provider_name {
        "file" => SecretSource::File {
            path: path.to_string(),
        },
        "vault" | "openbao" => SecretSource::OpenBao {
            path: path.to_string(),
            key: key.unwrap_or("value").to_string(),
        },
        "aws" => SecretSource::Aws {
            secret_id: path.to_string(),
            key: key.map(String::from),
        },
        _ => {
            return Err(Error::Config(format!(
                "Unknown secret provider: {provider_name}. Supported: file, vault, openbao, aws"
            )));
        }
    };

    let config = SecretsConfig {
        sources: [("tls_secret".into(), secret_source)].into_iter().collect(),
        ..Default::default()
    };
    let secrets = SecretsManager::new(config)?;

    let secret_value = if provider_name == "file" {
        secrets.get_file(path).await?
    } else {
        secrets.get("tls_secret").await?
    };

    Ok(secret_value.as_bytes().to_vec())
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

/// Build a TLS acceptor from configuration (sync version for backwards compatibility).
///
/// Use `build_tls_acceptor_async` if loading from secrets.
pub fn build_tls_acceptor(config: &TlsConfig) -> Result<Option<TlsAcceptor>> {
    if !config.enabled {
        return Ok(None);
    }

    // If secrets are configured, fail - caller should use async version
    if config.cert_secret.is_some() || config.key_secret.is_some() || config.ca_secret.is_some() {
        return Err(Error::Tls(
            "TLS secrets configured - use build_tls_acceptor_async instead".into(),
        ));
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
        return Err(Error::Tls(
            "client_auth requires ca_file or ca_secret".into(),
        ));
    }

    let certs = load_certs_from_file(Path::new(cert_path))?;
    let key = load_private_key_from_file(Path::new(key_path))?;

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

            let root_store = load_ca_certs_from_file(Path::new(ca_path))?;

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

/// Build a TLS acceptor from configuration with secret manager support.
///
/// Supports loading certificates from:
/// - Local files (cert_file, key_file, ca_file)
/// - Secret managers (cert_secret, key_secret, ca_secret)
///
/// Secret format: "provider:path:key" (e.g., "vault:secret/tls:cert")
pub async fn build_tls_acceptor_async(config: &TlsConfig) -> Result<Option<TlsAcceptor>> {
    if !config.enabled {
        return Ok(None);
    }

    let client_auth = ClientAuth::from_str(&config.client_auth);

    // Load certificate (secret takes precedence over file)
    let certs = if let Some(ref secret) = config.cert_secret {
        info!(source = %secret, "Loading TLS certificate from secret");
        let pem_data = load_from_secret(secret).await?;
        load_certs_from_bytes(&pem_data, secret)?
    } else if let Some(ref path) = config.cert_file {
        debug!(path = %path, "Loading TLS certificate from file");
        load_certs_from_file(Path::new(path))?
    } else {
        return Err(Error::Tls(
            "TLS enabled but neither cert_file nor cert_secret specified".into(),
        ));
    };

    // Load private key (secret takes precedence over file)
    let key = if let Some(ref secret) = config.key_secret {
        info!(source = %secret, "Loading TLS private key from secret");
        let pem_data = load_from_secret(secret).await?;
        load_private_key_from_bytes(&pem_data, secret)?
    } else if let Some(ref path) = config.key_file {
        debug!(path = %path, "Loading TLS private key from file");
        load_private_key_from_file(Path::new(path))?
    } else {
        return Err(Error::Tls(
            "TLS enabled but neither key_file nor key_secret specified".into(),
        ));
    };

    // Build TLS config based on client auth mode
    let tls_config = match client_auth {
        ClientAuth::None => ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| Error::Tls(format!("failed to build TLS config: {e}")))?,

        ClientAuth::Optional | ClientAuth::Required => {
            // Load CA certificate (secret takes precedence over file)
            let root_store = if let Some(ref secret) = config.ca_secret {
                info!(source = %secret, "Loading CA certificate from secret");
                let pem_data = load_from_secret(secret).await?;
                load_ca_certs_from_bytes(&pem_data, secret)?
            } else if let Some(ref path) = config.ca_file {
                debug!(path = %path, "Loading CA certificate from file");
                load_ca_certs_from_file(Path::new(path))?
            } else {
                return Err(Error::Tls(
                    "client_auth requires ca_file or ca_secret".into(),
                ));
            };

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

    info!("TLS acceptor built successfully");
    Ok(Some(TlsAcceptor::from(Arc::new(tls_config))))
}

/// Check if TLS config uses secrets (requires async loading).
pub fn uses_secrets(config: &TlsConfig) -> bool {
    config.cert_secret.is_some() || config.key_secret.is_some() || config.ca_secret.is_some()
}

/// Build a tonic `ServerTlsConfig` for the gRPC server.
///
/// Loads certificates from files or secret managers and returns
/// a `ServerTlsConfig` suitable for `tonic::transport::Server::builder().tls_config()`.
pub async fn build_grpc_tls_config(
    config: &TlsConfig,
) -> Result<tonic::transport::ServerTlsConfig> {
    // Load certificate (secret takes precedence over file)
    let cert_pem = if let Some(ref secret) = config.cert_secret {
        info!(source = %secret, "Loading gRPC TLS certificate from secret");
        load_from_secret(secret).await?
    } else if let Some(ref path) = config.cert_file {
        debug!(path = %path, "Loading gRPC TLS certificate from file");
        std::fs::read(path)
            .map_err(|e| Error::Tls(format!("failed to read cert file {path}: {e}")))?
    } else {
        return Err(Error::Tls(
            "gRPC TLS enabled but neither cert_file nor cert_secret specified".into(),
        ));
    };

    // Load private key (secret takes precedence over file)
    let key_pem = if let Some(ref secret) = config.key_secret {
        info!(source = %secret, "Loading gRPC TLS private key from secret");
        load_from_secret(secret).await?
    } else if let Some(ref path) = config.key_file {
        debug!(path = %path, "Loading gRPC TLS private key from file");
        std::fs::read(path)
            .map_err(|e| Error::Tls(format!("failed to read key file {path}: {e}")))?
    } else {
        return Err(Error::Tls(
            "gRPC TLS enabled but neither key_file nor key_secret specified".into(),
        ));
    };

    let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
    let mut tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);

    // Load CA for client certificate verification (mTLS)
    let client_auth = ClientAuth::from_str(&config.client_auth);
    if client_auth != ClientAuth::None {
        let ca_pem = if let Some(ref secret) = config.ca_secret {
            info!(source = %secret, "Loading gRPC CA certificate from secret");
            load_from_secret(secret).await?
        } else if let Some(ref path) = config.ca_file {
            debug!(path = %path, "Loading gRPC CA certificate from file");
            std::fs::read(path)
                .map_err(|e| Error::Tls(format!("failed to read CA file {path}: {e}")))?
        } else {
            return Err(Error::Tls(
                "gRPC client_auth requires ca_file or ca_secret".into(),
            ));
        };

        let ca_cert = tonic::transport::Certificate::from_pem(ca_pem);
        tls_config = tls_config.client_ca_root(ca_cert);
    }

    info!("gRPC TLS config built successfully");
    Ok(tls_config)
}

/// TLS certificate provider with background hot-reload.
///
/// Follows the same pattern as `BearerTokenProvider` in `auth.rs`.
/// Wraps a `TlsAcceptor` behind a `RwLock` and periodically reloads
/// certificates from secret managers.
pub struct TlsCertProvider {
    acceptor: Arc<parking_lot::RwLock<TlsAcceptor>>,
    config: TlsConfig,
    shutdown: tokio_util::sync::CancellationToken,
}

impl TlsCertProvider {
    /// Create a new TLS cert provider and perform initial certificate load.
    pub async fn new(config: TlsConfig) -> Result<Self> {
        let acceptor = build_tls_acceptor_async(&config)
            .await?
            .ok_or_else(|| Error::Tls("TLS provider created but TLS is disabled".into()))?;

        Ok(Self {
            acceptor: Arc::new(parking_lot::RwLock::new(acceptor)),
            config,
            shutdown: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Get the shared acceptor handle for the HTTP server accept loop.
    ///
    /// The server clones the `TlsAcceptor` from behind the `RwLock` per connection.
    /// This is cheap because `TlsAcceptor` wraps `Arc<ServerConfig>`.
    pub fn acceptor_handle(&self) -> Arc<parking_lot::RwLock<TlsAcceptor>> {
        self.acceptor.clone()
    }

    /// Start background certificate refresh task.
    ///
    /// Periodically reloads certificates from secret managers and swaps
    /// the TLS acceptor behind the `RwLock`. Existing connections are unaffected;
    /// new connections use the updated certificates.
    pub fn start_refresh_task(&self) {
        let acceptor = self.acceptor.clone();
        let config = self.config.clone();
        let shutdown = self.shutdown.clone();
        let interval = std::time::Duration::from_secs(config.refresh_interval_secs);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // Skip immediate tick

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("TLS cert refresh task stopping");
                        break;
                    }
                    _ = ticker.tick() => {
                        match build_tls_acceptor_async(&config).await {
                            Ok(Some(new_acceptor)) => {
                                *acceptor.write() = new_acceptor;
                                info!("TLS certificates refreshed successfully");
                            }
                            Ok(None) => {
                                tracing::error!("TLS refresh returned None (TLS disabled?)");
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "Failed to refresh TLS certificates");
                            }
                        }
                    }
                }
            }
        });
    }

    /// Shutdown the refresh task.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

impl Drop for TlsCertProvider {
    fn drop(&mut self) {
        self.shutdown();
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

    fn base_config() -> TlsConfig {
        TlsConfig {
            enabled: false,
            cert_file: None,
            key_file: None,
            ca_file: None,
            client_auth: "none".to_string(),
            cert_secret: None,
            key_secret: None,
            ca_secret: None,
            refresh_interval_secs: 3600,
        }
    }

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
        let config = base_config();

        let result = build_tls_acceptor(&config);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_tls_enabled_missing_cert() {
        let mut config = base_config();
        config.enabled = true;
        config.key_file = Some("/path/to/key".into());

        let result = build_tls_acceptor(&config);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("cert_file"));
    }

    #[test]
    fn test_tls_enabled_missing_key() {
        let mut config = base_config();
        config.enabled = true;
        config.cert_file = Some("/path/to/cert".into());

        let result = build_tls_acceptor(&config);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("key_file"));
    }

    #[test]
    fn test_mtls_missing_ca() {
        let mut config = base_config();
        config.enabled = true;
        config.cert_file = Some("/path/to/cert".into());
        config.key_file = Some("/path/to/key".into());
        config.client_auth = "required".to_string();

        let result = build_tls_acceptor(&config);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("ca_file"));
    }

    #[test]
    fn test_uses_secrets() {
        let mut config = base_config();
        assert!(!uses_secrets(&config));

        config.cert_secret = Some("vault:secret/tls:cert".into());
        assert!(uses_secrets(&config));
    }

    #[test]
    fn test_sync_rejects_secrets() {
        let mut config = base_config();
        config.enabled = true;
        config.cert_secret = Some("vault:secret/tls:cert".into());
        config.key_file = Some("/path/to/key".into());

        let result = build_tls_acceptor(&config);
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("async"));
    }
}
