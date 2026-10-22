// Project:   dfe-receiver
// File:      tests/integration/vault_auth.rs
// Purpose:   Integration tests for secret-manager-backed bearer tokens
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for secret loading from external secret sources.
//!
//! Every `provider:path[:key]` spec resolves through `scalo::secrets::resolve`,
//! reached via `dfe_receiver::secrets`. The `file:` provider is exercised
//! directly, for bearer tokens and for TLS certificate material. The `vault:`
//! provider runs against an OpenBao container in dev mode (skipped locally
//! without Docker, required in CI), for both the bearer-token path and the
//! webhook caller secrets that share the resolver with it. The `aws:` provider
//! runs against a loopback stand-in for Secrets Manager.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use dfe_receiver::config::{BearerConfig, TlsConfig, WebhookAuthConfig, WebhookAuthMode};
use dfe_receiver::server::auth::BearerTokenProvider;
use dfe_receiver::server::tls::build_tls_acceptor_async;
use dfe_receiver::server::webhook::auth::{AuthFailure, CallerAuth};
use dfe_receiver::tenancy::Principal;
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

/// Whether `provider` takes `token`.
fn accepts(provider: &BearerTokenProvider, token: &str) -> bool {
    provider.principal(token).is_some()
}

// ---------------------------------------------------------------------------
// File-based secret provider (always available)
// ---------------------------------------------------------------------------

/// A secret entry `<id>:<token>` authenticates the token alone, as that credential.
#[tokio::test]
async fn test_named_bearer_tokens_loaded_from_file_carry_their_credential() {
    let file = write_token_file(&["acme-ingest:token-a", "token-b"]);
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some(format!("file:{}", file.path().display())),
        refresh_interval_secs: 0,
    };
    let provider = BearerTokenProvider::from_config(&config).await.unwrap();

    assert_eq!(
        provider.principal("token-a"),
        Some(Principal::Credential("acme-ingest".into()))
    );
    assert_eq!(
        provider.principal("token-b"),
        Some(Principal::UnnamedCredential)
    );
    assert!(
        !accepts(&provider, "acme-ingest:token-a"),
        "the id is not part of the token"
    );
}

/// A webhook header secret is one whole value, so a colon in it names no credential.
#[tokio::test]
async fn test_a_webhook_header_secret_with_a_colon_is_matched_whole() {
    let file = write_token_file(&["vendor:shared-secret"]);
    let config = WebhookAuthConfig {
        mode: WebhookAuthMode::Header,
        secret_source: format!("file:{}", file.path().display()),
        refresh_interval_secs: 0,
        header: "x-webhook-secret".to_string(),
        ..WebhookAuthConfig::default()
    };
    let CallerAuth::Header(verifier) = CallerAuth::from_config("vendor", &config).await.unwrap()
    else {
        panic!("header mode builds a header verifier");
    };
    let with = |secret: &str| {
        let mut headers = HeaderMap::new();
        headers.insert("x-webhook-secret", secret.parse().unwrap());
        headers
    };

    assert!(verifier.verify(&with("vendor:shared-secret")).is_ok());
    assert!(matches!(
        verifier.verify(&with("shared-secret")),
        Err(AuthFailure::InvalidHeader)
    ));
}

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
    assert!(accepts(&provider, "secret-a"));
    assert!(accepts(&provider, "secret-b"));
    assert!(accepts(&provider, "secret-c"));
    assert!(!accepts(&provider, "secret-d"));
}

/// A secret holds one entry per line, and a comma is part of the token on it,
/// so half of a token never authenticates alone.
#[tokio::test]
async fn test_a_comma_in_a_bearer_secret_is_part_of_the_token() {
    let file = write_token_file(&["abc,def", "acme-ingest:x,y", "token-3"]);
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some(format!("file:{}", file.path().display())),
        refresh_interval_secs: 0,
    };
    let provider = BearerTokenProvider::from_config(&config).await.unwrap();

    assert_eq!(provider.token_count(), 3);
    assert!(accepts(&provider, "abc,def"));
    assert!(accepts(&provider, "token-3"));
    assert_eq!(
        provider.principal("x,y"),
        Some(Principal::Credential("acme-ingest".into()))
    );
    for half in ["abc", "def", "x", "y"] {
        assert!(!accepts(&provider, half), "{half} authenticated alone");
    }
}

/// A webhook header secret with a comma in it is matched whole, never by half.
#[tokio::test]
async fn test_a_comma_in_a_webhook_header_secret_is_part_of_it() {
    let file = write_token_file(&["abc,def"]);
    let config = WebhookAuthConfig {
        mode: WebhookAuthMode::Header,
        secret_source: format!("file:{}", file.path().display()),
        refresh_interval_secs: 0,
        header: "x-webhook-secret".to_string(),
        ..WebhookAuthConfig::default()
    };
    let CallerAuth::Header(verifier) = CallerAuth::from_config("vendor", &config).await.unwrap()
    else {
        panic!("header mode builds a header verifier");
    };
    let with = |secret: &str| {
        let mut headers = HeaderMap::new();
        headers.insert("x-webhook-secret", secret.parse().unwrap());
        headers
    };

    assert!(verifier.verify(&with("abc,def")).is_ok());
    for half in ["abc", "def"] {
        assert!(
            matches!(
                verifier.verify(&with(half)),
                Err(AuthFailure::InvalidHeader)
            ),
            "{half} authenticated alone"
        );
    }
}

/// A zero-byte token file is a mount or rotation failure, not an empty token
/// set, so the resolver refuses it and startup fails with the path named. The
/// old behaviour -- zero tokens and a clean start -- is a listener that rejects
/// every request while reading as healthy.
#[tokio::test]
async fn test_bearer_tokens_empty_file_is_an_error() {
    let file = NamedTempFile::new().unwrap();
    let source = format!("file:{}", file.path().display());
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some(source),
        refresh_interval_secs: 0,
    };

    let err = BearerTokenProvider::from_config(&config)
        .await
        .err()
        .expect("an empty token file must not start a zero-token provider")
        .to_string();
    assert!(
        err.contains(&file.path().display().to_string()),
        "the refusal must name the file: {err}"
    );
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
        tokens: vec!["static-fallback".into()],
        secret_source: Some("file:/nonexistent/path/tokens".to_string()),
        refresh_interval_secs: 0,
    };
    // Missing secret file: from_config logs a warning and keeps static tokens
    let provider = BearerTokenProvider::from_config(&config).await.unwrap();

    assert_eq!(provider.token_count(), 1);
    assert!(accepts(&provider, "static-fallback"));
}

/// A value with no `provider:` prefix must be refused, not used.
///
/// scalo's resolver returns a spec it does not recognise as a literal, so
/// without the receiver's prefix check the field's own contents would become
/// the accepted token.
#[tokio::test]
async fn test_bearer_tokens_source_with_no_provider_is_not_used_as_a_token() {
    let config = BearerConfig {
        tokens: vec!["static".into()],
        secret_source: Some("invalid_no_colon".to_string()),
        refresh_interval_secs: 0,
    };
    let provider = BearerTokenProvider::from_config(&config).await.unwrap();

    assert!(
        accepts(&provider, "static"),
        "the static token must survive"
    );
    assert!(
        !accepts(&provider, "invalid_no_colon"),
        "the spec itself must not become an accepted token"
    );
    assert_eq!(provider.token_count(), 1);
}

#[tokio::test]
async fn test_bearer_tokens_unknown_provider_rejected() {
    let config = BearerConfig {
        tokens: vec!["fallback".into()],
        secret_source: Some("nonexistent_provider:some_path".to_string()),
        refresh_interval_secs: 0,
    };
    let provider = BearerTokenProvider::from_config(&config).await.unwrap();

    assert!(accepts(&provider, "fallback"));
    assert!(
        !accepts(&provider, "nonexistent_provider:some_path"),
        "an unknown provider must not fall through as a literal token"
    );
}

// ---------------------------------------------------------------------------
// AWS Secrets Manager (loopback stand-in, no network)
// ---------------------------------------------------------------------------

/// The secret the stand-in holds, named the way the engine names a listener's
/// bearer tokens.
const AWS_TOKENS_SECRET: &str = "dfe/ingest/bearer-tokens/http";

/// A loopback stand-in for Secrets Manager's `GetSecretValue`, recording the
/// `SecretId` of every call it answers.
struct FakeSecretsManager {
    url: String,
    asked: Arc<Mutex<Vec<String>>>,
}

/// Answer `GetSecretValue` for [`AWS_TOKENS_SECRET`] and refuse any other id the
/// way Secrets Manager refuses a secret it does not hold.
async fn get_secret_value(
    State(asked): State<Arc<Mutex<Vec<String>>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    const JSON: (axum::http::HeaderName, &str) =
        (header::CONTENT_TYPE, "application/x-amz-json-1.1");

    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let request: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let secret_id = request["SecretId"].as_str().unwrap_or_default().to_string();
    if target != "secretsmanager.GetSecretValue" {
        let refusal = serde_json::json!({ "__type": "UnknownOperationException" });
        return (StatusCode::BAD_REQUEST, [JSON], refusal.to_string()).into_response();
    }
    asked.lock().unwrap().push(secret_id.clone());

    if secret_id == AWS_TOKENS_SECRET {
        let secret = serde_json::json!({
            "ARN": format!("arn:aws:secretsmanager:ap-southeast-2:000000000000:secret:{secret_id}-AbCdEf"),
            "Name": secret_id,
            "VersionId": "v1",
            "SecretString": "acme-ingest:token-a\ntoken-b\n",
        });
        return (StatusCode::OK, [JSON], secret.to_string()).into_response();
    }
    let refusal = serde_json::json!({
        "__type": "ResourceNotFoundException",
        "Message": "Secrets Manager can't find the specified secret.",
    });
    (StatusCode::BAD_REQUEST, [JSON], refusal.to_string()).into_response()
}

impl FakeSecretsManager {
    async fn start() -> Self {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/", post(get_secret_value))
            .with_state(Arc::clone(&asked));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        Self { url, asked }
    }

    /// Run `f` with the AWS SDK pointed at the stand-in and given static
    /// credentials, so nothing in the SDK's credential chain leaves the host.
    async fn with_env<F, T>(&self, f: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        temp_env::async_with_vars(
            [
                ("AWS_ENDPOINT_URL", Some(self.url.as_str())),
                ("AWS_DEFAULT_REGION", Some("ap-southeast-2")),
                ("AWS_ACCESS_KEY_ID", Some("AKIDEXAMPLE")),
                ("AWS_SECRET_ACCESS_KEY", Some("not-a-real-secret-key")),
                ("AWS_SESSION_TOKEN", None),
                ("AWS_EC2_METADATA_DISABLED", Some("true")),
            ],
            f,
        )
        .await
    }
}

/// An `aws:` source loads bearer tokens through scalo's AWS provider.
///
/// The spec has the shape the engine compiles for a listener: the secret's name
/// and no key, so the whole secret is the token list.
#[tokio::test]
async fn test_bearer_tokens_loaded_from_aws_secrets_manager() {
    let aws = FakeSecretsManager::start().await;
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some(format!("aws:{AWS_TOKENS_SECRET}")),
        refresh_interval_secs: 0,
    };

    let provider = aws
        .with_env(async { BearerTokenProvider::from_config(&config).await.unwrap() })
        .await;

    assert_eq!(provider.token_count(), 2);
    assert_eq!(
        provider.principal("token-a"),
        Some(Principal::Credential("acme-ingest".into()))
    );
    assert!(accepts(&provider, "token-b"));
    assert!(!accepts(&provider, "not-in-the-secret"));
    assert_eq!(
        *aws.asked.lock().unwrap(),
        [AWS_TOKENS_SECRET],
        "the lookup must reach Secrets Manager under the spec's secret name"
    );
}

/// A secret Secrets Manager does not hold refuses startup as a failed AWS
/// lookup, not as a missing build feature.
#[tokio::test]
async fn test_aws_source_that_cannot_load_is_refused_at_startup() {
    let aws = FakeSecretsManager::start().await;
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some("aws:dfe/ingest/bearer-tokens/missing".to_string()),
        refresh_interval_secs: 0,
    };

    let err = aws
        .with_env(async { BearerTokenProvider::from_config(&config).await.err() })
        .await
        .expect("a secret Secrets Manager does not hold cannot start a provider")
        .to_string();

    assert!(
        err.contains("aws resolution failed"),
        "the refusal must come from the AWS lookup: {err}"
    );
    assert!(
        !err.contains("secrets-aws"),
        "the build serves aws: specs, so the refusal must not blame the feature: {err}"
    );
}

/// A `vault:` spec names the field of the secret to read. The resolver this
/// replaced defaulted a missing key to `value`, which is a guess at which field
/// the operator meant.
#[tokio::test]
async fn test_keyless_vault_source_is_refused() {
    let config = BearerConfig {
        tokens: vec![],
        secret_source: Some("vault:secret/data/auth".to_string()),
        refresh_interval_secs: 0,
    };

    let err = BearerTokenProvider::from_config(&config)
        .await
        .err()
        .expect("a vault spec with no key names no field")
        .to_string();
    assert!(
        err.contains("invalid credential spec"),
        "the refusal must come from the resolver's spec check: {err}"
    );
}

// ---------------------------------------------------------------------------
// TLS certificate material through the same resolver
// ---------------------------------------------------------------------------

/// TLS certificate material loads from `file:` specs.
///
/// The TLS path used to build its own `SecretsManager` with the disk cache left
/// on and every read registered under the fixed name `tls_secret`, so the cert
/// and the key could be answered from one another's cache entry, and it never
/// set an OpenBao connection, so a `vault:` spec could not load at all. It now
/// goes through the same resolver as every other secret.
#[tokio::test]
async fn test_tls_material_loads_from_file_specs() {
    // Both ring and aws-lc-rs reach the test binary, so rustls refuses to pick
    // a provider for itself. Same install the vector integration test does.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = crate::common::self_signed_cert(dir.path());

    let config = TlsConfig {
        enabled: true,
        cert_secret: Some(format!("file:{}", cert.display())),
        key_secret: Some(format!("file:{}", key.display())),
        client_auth: "none".to_string(),
        ..TlsConfig::default()
    };

    let acceptor = build_tls_acceptor_async(&config)
        .await
        .expect("cert and key resolve through the file provider");
    assert!(
        acceptor.is_some(),
        "an enabled TLS config must produce an acceptor"
    );
}

/// A `file:` spec naming a path that is not there must name the path, not fail
/// somewhere inside the PEM parser.
#[tokio::test]
async fn test_tls_material_from_a_missing_file_names_the_path() {
    let config = TlsConfig {
        enabled: true,
        cert_secret: Some("file:/nonexistent/dfe-receiver-test/server.crt".to_string()),
        key_file: Some("/nonexistent/dfe-receiver-test/server.key".to_string()),
        client_auth: "none".to_string(),
        ..TlsConfig::default()
    };

    let err = build_tls_acceptor_async(&config)
        .await
        .err()
        .expect("a missing cert file cannot load")
        .to_string();
    assert!(
        err.contains("/nonexistent/dfe-receiver-test/server.crt"),
        "the error must name the path: {err}"
    );
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
    assert!(accepts(&provider, "vault-token-1"));
    assert!(accepts(&provider, "vault-token-2"));
    assert!(!accepts(&provider, "not-in-vault"));
}

/// TLS certificate material loads from `vault:` references, the certificate and
/// the key held as two fields of one secret.
///
/// This is what the deleted TLS loader could not do. It built its
/// `SecretsManager` from a default `SecretsConfig`, which carries no OpenBao
/// connection, so a `vault:` TLS reference was refused before an address was
/// read. It also registered every read under the single name `tls_secret` with
/// the disk cache left on, so the key could be answered from the certificate's
/// cache entry.
#[tokio::test]
async fn test_tls_material_loads_from_vault_container() {
    skip_if_no_docker!();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = crate::common::self_signed_cert(dir.path());
    let cert_pem = std::fs::read_to_string(&cert).unwrap();
    let key_pem = std::fs::read_to_string(&key).unwrap();

    let Some(bao) = OpenBao::with_secret(
        test_name!(),
        serde_json::json!({ "cert": cert_pem, "key": key_pem }),
    )
    .await
    else {
        return;
    };

    let config = TlsConfig {
        enabled: true,
        cert_secret: Some(bao.source("cert")),
        key_secret: Some(bao.source("key")),
        client_auth: "none".to_string(),
        ..TlsConfig::default()
    };

    let acceptor = bao
        .with_env(async { build_tls_acceptor_async(&config).await })
        .await
        .expect("the certificate and the key both resolve from OpenBao");
    assert!(
        acceptor.is_some(),
        "an enabled TLS config must produce an acceptor"
    );
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
    assert!(accepts(&provider1, "initial-token"));
    assert!(!accepts(&provider1, "rotated-token"));
    drop(provider1);

    // Simulate K8s secret rotation: rewrite the file atomically
    std::fs::write(&file_path, "rotated-token\n").unwrap();

    // Re-load via a fresh provider (equivalent to what the background
    // refresh task does internally -- calls load_tokens, which re-reads the
    // file through the resolver)
    let provider2 = BearerTokenProvider::from_config(&config).await.unwrap();
    assert!(
        !accepts(&provider2, "initial-token"),
        "old token should be revoked after rotation"
    );
    assert!(
        accepts(&provider2, "rotated-token"),
        "new token should be accepted after rotation"
    );

    // Keep the file alive through the test
    drop(file);
}
