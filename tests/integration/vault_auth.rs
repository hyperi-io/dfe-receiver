// Project:   dfe-receiver
// File:      tests/integration/vault_auth.rs
// Purpose:   Integration tests for secret-manager-backed bearer tokens
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for secret loading from external secret sources.
//!
//! The `file:` provider is exercised directly. The `vault:` provider runs
//! against an OpenBao container in dev mode (skipped locally without Docker,
//! required in CI), for both the bearer-token path and the webhook caller
//! secrets that share `read_secret_source` with it.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::HeaderMap;
use dfe_receiver::config::{BearerConfig, WebhookAuthConfig, WebhookAuthMode};
use dfe_receiver::server::auth::BearerTokenProvider;
use dfe_receiver::server::webhook::auth::{AuthFailure, CallerAuth};
use tempfile::NamedTempFile;

use crate::common::start_vault_container;
use crate::{skip_if_no_docker, test_name};

/// Write tokens to a temp file and return the path (held open by the caller).
fn write_token_file(tokens: &[&str]) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create temp file");
    for t in tokens {
        writeln!(file, "{t}").expect("write token");
    }
    file.flush().expect("flush");
    file
}

// ---------------------------------------------------------------------------
// File-based secret provider (always available)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bearer_tokens_loaded_from_file() {
    let file = write_token_file(&["secret-a", "secret-b", "secret-c"]);
    let source = format!("file:{}", file.path().display());

    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some(source),
        refresh_interval_secs: 0,
    };
    let provider = BearerTokenProvider::from_config(&config).await.unwrap();

    assert_eq!(provider.token_count(), 3);
    assert!(provider.is_valid("secret-a"));
    assert!(provider.is_valid("secret-b"));
    assert!(provider.is_valid("secret-c"));
    assert!(!provider.is_valid("secret-d"));
}

#[tokio::test]
async fn test_bearer_tokens_newline_and_comma_separated() {
    // Source format supports both newline and comma separators
    let mut file = NamedTempFile::new().unwrap();
    writeln!(file, "token-1,token-2").unwrap();
    writeln!(file, "token-3").unwrap();
    file.flush().unwrap();

    let source = format!("file:{}", file.path().display());
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some(source),
        refresh_interval_secs: 0,
    };
    let provider = BearerTokenProvider::from_config(&config).await.unwrap();

    assert_eq!(provider.token_count(), 3);
    for t in &["token-1", "token-2", "token-3"] {
        assert!(provider.is_valid(t), "missing token: {t}");
    }
}

#[tokio::test]
async fn test_bearer_tokens_empty_file_no_tokens() {
    let file = NamedTempFile::new().unwrap();
    let source = format!("file:{}", file.path().display());
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some(source),
        refresh_interval_secs: 0,
    };
    let provider = BearerTokenProvider::from_config(&config).await.unwrap();

    // Empty file → 0 tokens loaded; provider is still usable
    assert_eq!(provider.token_count(), 0);
    assert!(!provider.is_valid("anything"));
}

#[tokio::test]
async fn test_bearer_tokens_file_ignores_blank_lines() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(file).unwrap();
    writeln!(file, "token-1").unwrap();
    writeln!(file).unwrap();
    writeln!(file, "   ").unwrap(); // whitespace-only line
    writeln!(file, "token-2").unwrap();
    file.flush().unwrap();

    let source = format!("file:{}", file.path().display());
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some(source),
        refresh_interval_secs: 0,
    };
    let provider = BearerTokenProvider::from_config(&config).await.unwrap();

    assert_eq!(
        provider.token_count(),
        2,
        "blank and whitespace lines should be filtered"
    );
}

#[tokio::test]
async fn test_bearer_tokens_missing_file_falls_back_to_static() {
    let config = BearerConfig {
        tokens: vec!["static-fallback".to_string()],
        secret_source: Some("file:/nonexistent/path/tokens".to_string()),
        refresh_interval_secs: 0,
    };
    // Missing secret file: from_config logs a warning and keeps static tokens
    let provider = BearerTokenProvider::from_config(&config).await.unwrap();

    assert_eq!(provider.token_count(), 1);
    assert!(provider.is_valid("static-fallback"));
}

#[tokio::test]
async fn test_bearer_tokens_invalid_source_format_rejected() {
    let config = BearerConfig {
        tokens: vec!["static".to_string()],
        secret_source: Some("invalid_no_colon".to_string()),
        refresh_interval_secs: 0,
    };
    // Malformed source: graceful fall-through to static tokens
    let provider = BearerTokenProvider::from_config(&config).await.unwrap();
    assert!(provider.is_valid("static"));
}

#[tokio::test]
async fn test_bearer_tokens_unknown_provider_rejected() {
    let config = BearerConfig {
        tokens: vec!["fallback".to_string()],
        secret_source: Some("nonexistent_provider:some_path".to_string()),
        refresh_interval_secs: 0,
    };
    let provider = BearerTokenProvider::from_config(&config).await.unwrap();
    // Unknown provider triggers warning and keeps static tokens
    assert!(provider.is_valid("fallback"));
}

// ---------------------------------------------------------------------------
// OpenBao integration (container in dev mode)
// ---------------------------------------------------------------------------

/// A running OpenBao dev container with one KV v2 secret written into it.
struct OpenBao {
    _container: testcontainers::ContainerAsync<testcontainers::GenericImage>,
    url: String,
    root_token: String,
    /// The KV v2 path the secret was written under, `secret/data/...` form.
    path: String,
}

impl OpenBao {
    /// Start the container and write `fields` at a fresh path under `secret/`.
    /// `None` means Docker is not available or the write failed; callers skip.
    async fn with_secret(test: &str, fields: serde_json::Value) -> Option<Self> {
        let (container, url, root_token) = match start_vault_container(test).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Skipping: {e}");
                return None;
            }
        };
        tokio::time::sleep(Duration::from_secs(1)).await;

        let path = format!("secret/data/test/{}", uuid::Uuid::new_v4());
        let bao = Self {
            _container: container,
            url,
            root_token,
            path,
        };
        if bao.write(fields).await {
            Some(bao)
        } else {
            None
        }
    }

    /// Write (or overwrite) the secret's fields.
    async fn write(&self, fields: serde_json::Value) -> bool {
        let client = reqwest::Client::new();
        let Ok(Ok(resp)) = tokio::time::timeout(
            Duration::from_secs(10),
            client
                .post(format!("{}/v1/{}", self.url, self.path))
                .header("X-Vault-Token", &self.root_token)
                .json(&serde_json::json!({ "data": fields }))
                .send(),
        )
        .await
        else {
            eprintln!("Skipping: could not write secret to the OpenBao container");
            return false;
        };
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            eprintln!("Skipping: OpenBao write returned {status}: {text}");
            return false;
        }
        true
    }

    /// The `vault:` reference for one field of the secret.
    fn source(&self, key: &str) -> String {
        format!("vault:{}:{key}", self.path)
    }

    /// Run `f` with the connection in the environment, the way a deployment
    /// hands it to the receiver.
    async fn with_env<F, T>(&self, f: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        temp_env::async_with_vars(
            [
                ("VAULT_ADDR", Some(self.url.as_str())),
                ("VAULT_TOKEN", Some(self.root_token.as_str())),
            ],
            f,
        )
        .await
    }
}

/// Bearer tokens load from a `vault:` reference.
#[tokio::test]
async fn test_bearer_tokens_loaded_from_vault_container() {
    skip_if_no_docker!();
    let Some(bao) = OpenBao::with_secret(
        test_name!(),
        serde_json::json!({ "tokens": "vault-token-1\nvault-token-2" }),
    )
    .await
    else {
        return;
    };

    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some(bao.source("tokens")),
        refresh_interval_secs: 0,
    };
    let provider = bao
        .with_env(async { BearerTokenProvider::from_config(&config).await.unwrap() })
        .await;

    assert_eq!(provider.token_count(), 2);
    assert!(provider.is_valid("vault-token-1"));
    assert!(provider.is_valid("vault-token-2"));
    assert!(!provider.is_valid("not-in-vault"));
}

/// A `header`-mode webhook caller's secret loads from a `vault:` reference and
/// a rotated value is picked up on the refresh interval.
#[tokio::test]
async fn test_webhook_header_secret_from_openbao_and_rotation() {
    skip_if_no_docker!();
    let Some(bao) = OpenBao::with_secret(
        test_name!(),
        serde_json::json!({ "webhook": "first-shared-secret" }),
    )
    .await
    else {
        return;
    };

    let config = WebhookAuthConfig {
        mode: WebhookAuthMode::Header,
        secret_source: bao.source("webhook"),
        refresh_interval_secs: 1,
        header: "x-webhook-secret".to_string(),
        ..WebhookAuthConfig::default()
    };

    bao.with_env(async {
        let auth = CallerAuth::from_config("runzero", &config)
            .await
            .expect("the caller secret resolves from OpenBao");

        let mut headers = HeaderMap::new();
        headers.insert("x-webhook-secret", "first-shared-secret".parse().unwrap());
        assert_eq!(auth.verify(&headers, b"{}", SystemTime::now()), Ok(()));

        let mut wrong = HeaderMap::new();
        wrong.insert("x-webhook-secret", "rotated-shared-secret".parse().unwrap());
        assert_eq!(
            auth.verify(&wrong, b"{}", SystemTime::now()),
            Err(AuthFailure::InvalidHeader)
        );

        // Rotate in OpenBao; the refresh task must pick it up within a few
        // ticks and drop the old value.
        assert!(
            bao.write(serde_json::json!({ "webhook": "rotated-shared-secret" }))
                .await
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if auth.verify(&wrong, b"{}", SystemTime::now()).is_ok() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the rotated secret was not picked up within 15s"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert_eq!(
            auth.verify(&headers, b"{}", SystemTime::now()),
            Err(AuthFailure::InvalidHeader),
            "the old secret must stop verifying after rotation"
        );
    })
    .await;
}

/// An `hmac`-mode webhook caller's signing secret loads from a `vault:`
/// reference and signs the way a product would.
#[tokio::test]
async fn test_webhook_hmac_secret_from_openbao() {
    skip_if_no_docker!();
    let Some(bao) = OpenBao::with_secret(
        test_name!(),
        serde_json::json!({ "signing": "openbao-held-signing-secret" }),
    )
    .await
    else {
        return;
    };

    let config = WebhookAuthConfig {
        mode: WebhookAuthMode::Hmac,
        secret_source: bao.source("signing"),
        refresh_interval_secs: 0,
        ..WebhookAuthConfig::default()
    };
    let auth = bao
        .with_env(async { CallerAuth::from_config("pager", &config).await.unwrap() })
        .await;

    let body = br#"{"event":"alert"}"#;
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"openbao-held-signing-secret");
    let mut message = ts.to_string().into_bytes();
    message.push(b'.');
    message.extend_from_slice(body);
    let signature =
        ring::hmac::sign(&key, &message)
            .as_ref()
            .iter()
            .fold(String::new(), |mut out, b| {
                use std::fmt::Write;
                let _ = write!(out, "{b:02x}");
                out
            });

    let mut headers = HeaderMap::new();
    headers.insert("x-timestamp", ts.to_string().parse().unwrap());
    headers.insert("x-signature", signature.parse().unwrap());
    assert_eq!(auth.verify(&headers, body, SystemTime::now()), Ok(()));
    assert_eq!(
        auth.verify(&headers, br#"{"event":"other"}"#, SystemTime::now()),
        Err(AuthFailure::InvalidSignature)
    );
}

/// A `vault:` reference with no OpenBao connection in the environment names
/// what to set rather than a bare "provider not configured".
#[tokio::test]
async fn test_vault_source_without_a_connection_names_the_variables() {
    let config = WebhookAuthConfig {
        mode: WebhookAuthMode::Header,
        secret_source: "vault:secret/data/nowhere:webhook".to_string(),
        refresh_interval_secs: 0,
        ..WebhookAuthConfig::default()
    };
    let err = temp_env::async_with_vars(
        [
            ("VAULT_ADDR", None::<&str>),
            ("VAULT_TOKEN", None),
            ("OPENBAO_ADDR", None),
            ("BAO_ADDR", None),
        ],
        async { CallerAuth::from_config("runzero", &config).await.err() },
    )
    .await
    .expect("no connection must be an error")
    .to_string();
    assert!(err.contains("VAULT_ADDR"), "got: {err}");
    assert!(!err.contains("provider not configured"), "got: {err}");
}

// ---------------------------------------------------------------------------
// Secret-source failure must not read as a successful start
// ---------------------------------------------------------------------------

/// A `vault:` secret source that cannot load (no OpenBao connection in the
/// environment) with no static fallback must fail, not succeed with zero
/// tokens. A warn line plus `Ok` with a provider holding nothing means the
/// receiver starts clean and then rejects every request.
#[tokio::test]
async fn test_vault_source_with_no_static_tokens_is_an_error() {
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some("vault:secret/data/nowhere:tokens".to_string()),
        refresh_interval_secs: 0,
    };

    let result = temp_env::async_with_vars(
        [("VAULT_ADDR", None::<&str>), ("VAULT_TOKEN", None)],
        async { BearerTokenProvider::from_config(&config).await },
    )
    .await;

    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("no usable tokens") || msg.contains("static bearer tokens"),
                "error should name the empty-token-set cause, got: {msg}"
            );
        }
        Ok(provider) => panic!(
            "from_config reported success with {} tokens after the only \
             configured secret source failed to load",
            provider.token_count()
        ),
    }
}

/// Same rule for an unreachable file source with no static fallback.
///
/// The `file:` provider is always compiled in, so this covers the rule without
/// depending on any cargo feature.
#[tokio::test]
async fn test_missing_file_source_with_no_static_tokens_is_an_error() {
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some("file:/nonexistent/path/tokens".to_string()),
        refresh_interval_secs: 0,
    };

    assert!(
        BearerTokenProvider::from_config(&config).await.is_err(),
        "an unloadable secret source with no static fallback must not report \
         success -- a zero-token provider rejects every request"
    );
}

// ---------------------------------------------------------------------------
// Token rotation (file-watching style)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bearer_tokens_reload_after_file_update() {
    // Initial tokens written to a known file path
    let file = NamedTempFile::new().unwrap();
    let file_path = file.path().to_path_buf();
    std::fs::write(&file_path, "initial-token\n").unwrap();

    let source = format!("file:{}", file_path.display());
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some(source.clone()),
        refresh_interval_secs: 0,
    };
    let provider1 = BearerTokenProvider::from_config(&config).await.unwrap();
    assert!(provider1.is_valid("initial-token"));
    assert!(!provider1.is_valid("rotated-token"));
    drop(provider1);

    // Simulate K8s secret rotation: rewrite the file atomically
    std::fs::write(&file_path, "rotated-token\n").unwrap();

    // Re-load via a fresh provider (equivalent to what the background
    // refresh task does internally — calls load_from_secret which
    // re-reads the file via the SecretsManager)
    let provider2 = BearerTokenProvider::from_config(&config).await.unwrap();
    assert!(
        !provider2.is_valid("initial-token"),
        "old token should be revoked after rotation"
    );
    assert!(
        provider2.is_valid("rotated-token"),
        "new token should be accepted after rotation"
    );

    // Keep the file alive through the test
    drop(file);
}
