// Project:   dfe-receiver
// File:      tests/integration/vault_auth.rs
// Purpose:   Integration tests for secret-manager-backed bearer tokens
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for bearer-token loading from external secret sources.
//!
//! These tests exercise the `BearerTokenProvider::load_from_secret` path
//! end-to-end via the `file:` provider (the only provider always compiled
//! in; `vault:` and `aws:` require the `secrets-vault` / `secrets-aws`
//! features in scalo).
//!
//! The tests also include a skipped-by-default live Vault test gated on
//! `VAULT_ADDR` that runs when a Vault/OpenBao instance is reachable.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::io::Write;
use std::time::Duration;

use dfe_receiver::config::BearerConfig;
use dfe_receiver::server::auth::BearerTokenProvider;
use tempfile::NamedTempFile;

use crate::common::start_vault_container;
use crate::skip_if_no_docker;

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
// Live Vault / OpenBao integration (skipped when VAULT_ADDR not set)
// ---------------------------------------------------------------------------

/// End-to-end Vault test using a fresh testcontainer Vault in dev mode.
///
/// Spins up a hashicorp/vault container, writes a secret, loads it via
/// BearerTokenProvider, and verifies the token authenticates successfully.
/// The container is stopped automatically when the test completes.
///
/// Ignored because the `vault:` / `openbao:` secret source that
/// `BearerTokenProvider::load_from_secret` accepts and documents
/// (`src/server/auth.rs`) cannot resolve, for two independent reasons:
///
///   1. `[dependencies] scalo` enables `secrets` but not `secrets-vault`, so
///      scalo compiles the `SecretSource::OpenBao` arm to a hard
///      `ProviderNotConfigured` error.
///   2. With `secrets-vault` enabled it still fails. `load_from_secret` builds
///      `SecretsConfig { sources, cache, ..Default::default() }`, leaving
///      `openbao: None`; `SecretsManager::new` only constructs the provider
///      when that field is `Some`, so the lookup returns
///      `provider not configured: openbao` regardless of the feature.
///
/// scalo 2.10.7 does NOT fix this, despite fixing the same shape elsewhere: its
/// `secrets_config_for_lookup` is reached only from
/// `scalo::secrets::resolve::resolve()`, and `load_from_secret` does not go
/// through that -- it builds the config itself. Reason 2 above is unchanged.
///
/// To un-ignore: enable `secrets-vault` AND populate `SecretsConfig.openbao`
/// from the app's secrets config in `load_from_secret`. Otherwise drop
/// `vault` / `openbao` / `aws` from that match arm and its doc comment, and
/// delete this test -- an advertised provider that always errors is worse than
/// no provider.
#[tokio::test]
#[ignore = "the vault:/openbao: bearer-token provider cannot work as wired; see doc comment"]
async fn test_bearer_tokens_loaded_from_vault_container() {
    skip_if_no_docker!();

    let (_container, vault_url, root_token) = match start_vault_container().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Wait for Vault to be fully ready
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Write a secret via Vault HTTP API (KV v2 engine is mounted at `secret/` in dev mode)
    let secret_path = format!("test/bearer-{}", uuid::Uuid::new_v4());
    let write_url = format!("{vault_url}/v1/secret/data/{secret_path}");
    let body = serde_json::json!({
        "data": { "tokens": "vault-token-1\nvault-token-2" }
    });

    let client = reqwest::Client::new();
    let Ok(Ok(write_resp)) = tokio::time::timeout(
        Duration::from_secs(10),
        client
            .post(&write_url)
            .header("X-Vault-Token", &root_token)
            .json(&body)
            .send(),
    )
    .await
    else {
        eprintln!("Skipping: could not write secret to Vault container");
        return;
    };

    if !write_resp.status().is_success() {
        let status = write_resp.status();
        let text = write_resp.text().await.unwrap_or_default();
        eprintln!("Skipping: Vault write returned {status}: {text}");
        return;
    }

    // Load tokens from Vault via BearerTokenProvider (exercises full code path)
    let source = format!("vault:secret/data/{secret_path}:tokens");
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some(source),
        refresh_interval_secs: 0,
    };

    let vault_url_str = vault_url.clone();
    let root_token_str = root_token.clone();
    let provider = temp_env::async_with_vars(
        [
            ("VAULT_ADDR", Some(vault_url_str.as_str())),
            ("VAULT_TOKEN", Some(root_token_str.as_str())),
        ],
        async move { BearerTokenProvider::from_config(&config).await.unwrap() },
    )
    .await;

    assert_eq!(
        provider.token_count(),
        2,
        "vault: source produced {} tokens instead of 2 -- if this is 0, the \
         scalo secrets-vault feature is not compiled in and the vault:/openbao: \
         provider advertised by BearerTokenProvider::load_from_secret cannot \
         work at all",
        provider.token_count()
    );
    assert!(provider.is_valid("vault-token-1"));
    assert!(provider.is_valid("vault-token-2"));
    assert!(!provider.is_valid("not-in-vault"));
}

// ---------------------------------------------------------------------------
// Secret-source failure must not read as a successful start
// ---------------------------------------------------------------------------

/// A `vault:` secret source with no static fallback must fail, not succeed
/// with zero tokens.
///
/// `load_from_secret` accepts `vault:` / `openbao:` sources and advertises
/// them in its docs, but `SecretSource::OpenBao` always resolves to
/// `ProviderNotConfigured` here. A warn line plus `Ok` with a provider
/// holding nothing means the receiver starts clean and then rejects every
/// request; "using static tokens" is untrue when `tokens` is empty.
#[tokio::test]
async fn test_vault_source_with_no_static_tokens_is_an_error() {
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some("vault:secret/data/nowhere:tokens".to_string()),
        refresh_interval_secs: 0,
    };

    let result = BearerTokenProvider::from_config(&config).await;

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
