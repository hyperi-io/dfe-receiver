// Project:   dfe-receiver
// File:      src/server/webhook/mod.rs
// Purpose:   Generic authenticated webhook intake (POST /webhook/{caller})
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Generic authenticated webhook intake.
//!
//! One route per declared caller, `POST /webhook/{caller}`, each with its own
//! secret, topic and body shape (see [`WebhookConfig`]). A record that passes
//! the caller's authentication and optional CEL filter is stamped with
//! `_source` (the caller name) and `_timestamp_receiver`, then delivered to
//! the caller's topic through [`PipelineState::process_to_topic`].
//!
//! With `webhook.bind_address` unset the routes are merged into the main
//! ingest listener by `server::http::run_server`, so they sit under its TLS,
//! IP filter, rate limit and concurrency cap while keeping their own body
//! limit and skipping its server-wide auth middleware. Set, the intake runs as
//! its own [`ProtocolHandler`] with the same accept loops the ingest listener
//! uses, so it inherits the slowloris hardening and the IP filter rather than
//! bypassing them.

pub mod auth;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use rustc_hash::FxHashMap;
use scalo::logger::security;
use sonic_rs::get_from_slice;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tracing::{debug, info};

use crate::config::{Config, WebhookBody, WebhookCallerConfig, WebhookConfig};
use crate::error::{Error, Result, unavailable_response};
use crate::metrics::Metrics;
use crate::pipeline::{BatchOutcome, PipelineState};
use crate::server::http::split_json_array;
use crate::server::ip_filter::IpFilter;
use crate::server::tls::{TlsCertProvider, build_tls_acceptor, uses_secrets};
use crate::server::traits::{BoundAddr, ProtocolHandler};
use crate::validation::depth::{self, MAX_BATCH_DEPTH, MAX_PARSE_DEPTH};

use self::auth::CallerAuth;

/// The transport label every webhook metric carries.
const TRANSPORT: &str = "webhook";

/// Webhook protocol handler on its own listener.
///
/// Built only when `webhook.bind_address` is set; the shared-listener case
/// never constructs one.
pub struct WebhookHandler {
    config: Config,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    bound: BoundAddr,
}

impl WebhookHandler {
    /// Create the handler. `config` is the whole receiver config: the listener
    /// takes `webhook.*` for itself and `server.ip_filter`,
    /// `server.rate_limit` and `server.max_concurrent_requests` as its
    /// admission limits.
    pub fn new(config: Config, pipeline: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        Self {
            config,
            pipeline,
            metrics,
            bound: BoundAddr::default(),
        }
    }

    /// The address the listener bound, once [`ProtocolHandler::start`] binds it.
    #[must_use]
    pub fn bound_addr(&self) -> BoundAddr {
        self.bound.clone()
    }
}

#[async_trait::async_trait]
impl ProtocolHandler for WebhookHandler {
    fn name(&self) -> &'static str {
        "webhook"
    }

    fn bind_address(&self) -> &str {
        self.config.webhook.bind_address.as_deref().unwrap_or("")
    }

    fn listeners(&self) -> Vec<BoundAddr> {
        vec![self.bound.clone()]
    }

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        run_webhook_server(
            &self.config,
            self.pipeline.clone(),
            self.metrics.clone(),
            shutdown,
            &self.bound,
        )
        .await
    }
}

/// One declared caller, compiled for the request path.
struct Caller {
    name: Arc<str>,
    topic: String,
    auth: CallerAuth,
    body: WebhookBody,
    filter: Option<cel::Program>,
}

impl Caller {
    async fn build(config: &WebhookCallerConfig) -> Result<Self> {
        let filter = match config.filter.trim() {
            "" => None,
            expr => Some(scalo::expression::compile(expr).map_err(|e| {
                Error::Config(format!(
                    "webhook caller '{}': filter rejected: {e}",
                    config.name
                ))
            })?),
        };
        Ok(Self {
            name: Arc::from(config.name.as_str()),
            topic: config.topic.clone(),
            auth: CallerAuth::from_config(&config.name, &config.auth).await?,
            body: config.body,
            filter,
        })
    }

    /// Check, filter and stamp every record before any is delivered, so one
    /// bad element refuses the whole request with nothing on the topic and
    /// the sender's retry duplicates nothing.
    ///
    /// # Errors
    ///
    /// Returns `record_not_an_object` for an element that is not a JSON
    /// object, or the stamp failure, and prepares none of the records.
    fn prepare(&self, records: Vec<Bytes>, now_ms: u128) -> Result<Prepared> {
        let mut prepared = Vec::with_capacity(records.len());
        let mut dropped = 0usize;
        for record in records {
            if !is_json_object(&record) {
                return Err(Error::Validation("record_not_an_object".into()));
            }
            if !self.keeps(&record) {
                dropped += 1;
                continue;
            }
            prepared.push(stamp(record, &self.name, now_ms)?);
        }
        Ok(Prepared {
            records: prepared,
            dropped,
        })
    }

    /// The filter's verdict on one record: a boolean as itself, a number by
    /// non-zero, anything else and every evaluation error as a drop.
    fn keeps(&self, record: &[u8]) -> bool {
        let Some(program) = &self.filter else {
            return true;
        };
        let Ok(serde_json::Value::Object(map)) =
            serde_json::from_slice::<serde_json::Value>(record)
        else {
            return false;
        };
        let Ok(context) = scalo::expression::build_context(&map) else {
            return false;
        };
        match program.execute(&context) {
            Ok(cel::Value::Bool(b)) => b,
            Ok(cel::Value::Int(n)) => n != 0,
            Ok(cel::Value::UInt(n)) => n != 0,
            Ok(cel::Value::Float(f)) => f != 0.0,
            _ => false,
        }
    }
}

/// The records of one request ready for delivery, and how many the filter
/// dropped.
#[derive(Debug)]
struct Prepared {
    records: Vec<Bytes>,
    dropped: usize,
}

/// Shared state for the webhook routes.
#[derive(Clone)]
struct WebhookState {
    callers: Arc<FxHashMap<String, Caller>>,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
}

/// Build the webhook routes with their own body limit and timeout.
///
/// Loads every caller's secret, so a caller whose secret cannot be read
/// refuses the whole intake at startup rather than answering 401 forever.
///
/// # Errors
///
/// Returns an error when a caller's secret cannot be loaded, a header name is
/// invalid, or a filter does not compile.
pub async fn build_router(
    config: &WebhookConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
) -> Result<Router> {
    let mut callers = FxHashMap::default();
    for caller in &config.callers {
        callers.insert(caller.name.clone(), Caller::build(caller).await?);
        info!(
            caller = %caller.name,
            topic = %caller.topic,
            auth = ?caller.auth.mode,
            body = ?caller.body,
            filtered = !caller.filter.trim().is_empty(),
            "webhook caller registered"
        );
    }

    let state = WebhookState {
        callers: Arc::new(callers),
        pipeline,
        metrics: metrics.clone(),
    };

    // Layer order, outermost first: timeout, then the 413 counter, then the
    // body limit it counts, then the handler.
    Ok(Router::new()
        .route("/webhook/{caller}", post(webhook_handler))
        .layer(RequestBodyLimitLayer::new(config.max_body_size))
        .layer(axum::middleware::from_fn_with_state(
            metrics,
            count_oversize,
        ))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_millis(config.request_timeout_ms),
        ))
        .with_state(state))
}

/// Count a 413 from the body limit, which otherwise leaves no metric behind.
async fn count_oversize(
    State(metrics): State<Arc<Metrics>>,
    request: Request,
    next: Next,
) -> Response {
    let response = next.run(request).await;
    if response.status() == StatusCode::PAYLOAD_TOO_LARGE {
        metrics.inc_requests_total(TRANSPORT);
        metrics.inc_requests_error(TRANSPORT);
        metrics.inc_body_size_rejected();
    }
    response
}

/// Run the webhook intake on its own listener.
async fn run_webhook_server(
    config: &Config,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    bound: &BoundAddr,
) -> Result<()> {
    let webhook = &config.webhook;
    let Some(bind_address) = webhook.bind_address.as_deref() else {
        return Err(Error::Config(
            "webhook handler started without webhook.bind_address".into(),
        ));
    };

    let app = build_router(webhook, pipeline, metrics.clone()).await?;
    let app = crate::server::http::apply_server_limits(app, &config.server)?;

    let addr: SocketAddr = bind_address
        .parse()
        .map_err(|e| Error::Config(format!("invalid webhook bind address: {e}")))?;
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Server(format!("webhook failed to bind: {e}")))?;

    let ip_filter = IpFilter::from_config(&config.server.ip_filter);

    let tls_provider = if webhook.tls.enabled && uses_secrets(&webhook.tls) {
        let provider = TlsCertProvider::new(webhook.tls.clone()).await?;
        provider.start_refresh_task();
        Some(provider)
    } else {
        None
    };
    let tls_acceptor = if tls_provider.is_some() {
        None
    } else {
        build_tls_acceptor(&webhook.tls)?
    };

    // Published once TLS is ready, so a failed TLS setup never reads as serving.
    let _serving = bound.publish(&listener.local_addr());

    if let Some(ref provider) = tls_provider {
        info!(addr = %addr, tls = true, hot_reload = true, "webhook server listening");
        crate::server::http::run_tls_server(
            listener,
            app,
            provider.acceptor_handle(),
            ip_filter,
            shutdown,
            metrics,
        )
        .await
    } else if let Some(acceptor) = tls_acceptor {
        info!(addr = %addr, tls = true, hot_reload = false, "webhook server listening");
        crate::server::http::run_tls_server(
            listener,
            app,
            Arc::new(parking_lot::RwLock::new(acceptor)),
            ip_filter,
            shutdown,
            metrics,
        )
        .await
    } else {
        info!(addr = %addr, tls = false, "webhook server listening");
        crate::server::http::run_plain_server(listener, app, ip_filter, shutdown).await
    }
}

/// A request refused before it reached the pipeline.
struct Refused {
    status: StatusCode,
    reason: &'static str,
}

impl IntoResponse for Refused {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.reason })),
        )
            .into_response()
    }
}

/// `POST /webhook/{caller}`.
///
/// Order: caller lookup, authentication, readiness, nesting depth, body split,
/// then every record is checked, filtered and stamped before any is delivered.
/// Authentication runs before the readiness check so an unauthenticated
/// client learns nothing about the pipeline's state.
async fn webhook_handler(
    State(state): State<WebhookState>,
    Path(caller_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.metrics.inc_requests_total(TRANSPORT);
    state
        .metrics
        .add_bytes_received(TRANSPORT, body.len() as u64);

    let Some(caller) = state.callers.get(&caller_name) else {
        debug!(transport = TRANSPORT, caller = %caller_name, "webhook request for an unknown caller");
        state.metrics.inc_requests_error(TRANSPORT);
        return Refused {
            status: StatusCode::NOT_FOUND,
            reason: "unknown_caller",
        }
        .into_response();
    };

    if let Err(failure) = caller.auth.verify(&headers, &body, SystemTime::now()) {
        // Per-request detail stays at debug: the counter and the security
        // event carry the signal, and a credential spray must not write one
        // warn line per attempt.
        debug!(
            transport = TRANSPORT,
            caller = %caller.name,
            reason = failure.label(),
            "webhook request refused"
        );
        state.metrics.inc_requests_error(TRANSPORT);
        state.metrics.inc_auth_failure(failure.metric_reason());
        security::auth_failure(&caller.name, failure.label(), None);
        return Error::Auth(failure.label().to_string()).into_response();
    }

    if !state.pipeline.is_ready() {
        debug!(transport = TRANSPORT, caller = %caller.name, "webhook request rejected -- pipeline not ready");
        state.metrics.inc_requests_error(TRANSPORT);
        state.metrics.record_backpressure();
        return unavailable_response("server is overloaded");
    }

    // The split and the stamp both parse lazily, so depth is settled before either.
    let max_depth = match caller.body {
        WebhookBody::Single => MAX_PARSE_DEPTH,
        WebhookBody::Array => MAX_BATCH_DEPTH,
    };
    if let Err(e) = depth::admit(&body, max_depth, Some(&state.metrics)) {
        state.metrics.inc_requests_error(TRANSPORT);
        return e.into_response();
    }

    let records = match caller.body {
        WebhookBody::Single => vec![body],
        WebhookBody::Array => match split_json_array(&body) {
            Some(Ok(records)) => records,
            Some(Err(e)) => {
                state.metrics.inc_requests_error(TRANSPORT);
                return e.into_response();
            }
            None => {
                state.metrics.inc_requests_error(TRANSPORT);
                return Refused {
                    status: StatusCode::BAD_REQUEST,
                    reason: "body_not_an_array",
                }
                .into_response();
            }
        },
    };

    let Prepared { records, dropped } = match caller.prepare(records, now_millis()) {
        Ok(prepared) => prepared,
        Err(e) => {
            state.metrics.inc_requests_error(TRANSPORT);
            return e.into_response();
        }
    };

    let start = std::time::Instant::now();
    let mut outcome = BatchOutcome::default();
    for record in records {
        let result = state.pipeline.process_to_topic(record, &caller.topic).await;
        if outcome.record(result).is_break() {
            break;
        }
    }
    let elapsed = start.elapsed();
    state
        .metrics
        .record_request_duration(TRANSPORT, elapsed.as_secs_f64());

    // A record not taken answers 503 even when others landed: the retry
    // duplicates those, where a 202 would lose the rest.
    if let Some(e) = outcome.unavailable {
        debug!(
            transport = TRANSPORT,
            caller = %caller.name,
            accepted = outcome.accepted,
            error = %e,
            "webhook request not fully taken"
        );
        state.metrics.inc_requests_error(TRANSPORT);
        state.metrics.record_backpressure();
        return unavailable_response("server is overloaded");
    }
    if let Some(e) = outcome.first_rejection {
        debug!(
            transport = TRANSPORT,
            caller = %caller.name,
            rejected = outcome.rejected,
            error = %e,
            "webhook request carried records refused for good"
        );
        state.metrics.inc_requests_error(TRANSPORT);
        return e.into_response();
    }
    debug!(
        transport = TRANSPORT,
        caller = %caller.name,
        accepted = outcome.accepted,
        dropped,
        duration_us = elapsed.as_micros(),
        "webhook request accepted"
    );
    state.metrics.inc_requests_success(TRANSPORT);
    StatusCode::ACCEPTED.into_response()
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Whether the bytes are a JSON object, judged by their outer braces.
fn is_json_object(record: &[u8]) -> bool {
    let first = record.iter().find(|b| !b.is_ascii_whitespace());
    let last = record.iter().rev().find(|b| !b.is_ascii_whitespace());
    matches!((first, last), (Some(b'{'), Some(b'}')))
}

/// Write `_source` (the caller) and `_timestamp_receiver` into the record.
///
/// The caller's identity wins: a record that already carries either key is
/// re-serialised with the receiver's values in place of the sender's, since
/// `_source` picks the destination table downstream and a duplicate key makes
/// the loader's JSON column reject the record. Records without them take the
/// byte-append path the pipeline uses.
fn stamp(record: Bytes, caller: &str, now_ms: u128) -> Result<Bytes> {
    let reserved = get_from_slice(&record, ["_source"].as_slice()).is_ok()
        || get_from_slice(&record, ["_timestamp_receiver"].as_slice()).is_ok();
    if !reserved {
        let stamped = crate::routing::stamp_source(record, caller);
        return Ok(PipelineState::enrich_payload_at(stamped, now_ms));
    }
    let mut value: serde_json::Value = serde_json::from_slice(&record)?;
    let Some(object) = value.as_object_mut() else {
        return Err(Error::Validation("record is not a JSON object".into()));
    };
    object.insert("_source".into(), serde_json::Value::String(caller.into()));
    object.insert(
        "_timestamp_receiver".into(),
        serde_json::Value::Number(serde_json::Number::from(now_ms as u64)),
    );
    Ok(Bytes::from(serde_json::to_vec(&value)?))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn default_config_shares_the_ingest_listener() {
        let config = WebhookConfig::default();
        assert!(!config.enabled);
        assert!(config.bind_address.is_none());
        assert_eq!(config.max_body_size, 1024 * 1024);
    }

    #[test]
    fn a_plain_record_is_stamped_by_byte_append() {
        let stamped = stamp(Bytes::from(r#"{"a":1}"#), "runzero", 1700).unwrap();
        let text = String::from_utf8(stamped.to_vec()).unwrap();
        assert_eq!(
            text,
            r#"{"a":1,"_source":"runzero","_timestamp_receiver":1700}"#
        );
    }

    #[test]
    fn an_empty_object_is_stamped_without_a_leading_comma() {
        let stamped = stamp(Bytes::from("{}"), "runzero", 1700).unwrap();
        let text = String::from_utf8(stamped.to_vec()).unwrap();
        assert_eq!(text, r#"{"_source":"runzero","_timestamp_receiver":1700}"#);
    }

    #[test]
    fn the_caller_wins_over_a_sender_supplied_source() {
        let stamped = stamp(Bytes::from(r#"{"_source":"other","a":1}"#), "runzero", 1700).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&stamped).unwrap();
        assert_eq!(value["_source"], "runzero");
        assert_eq!(value["_timestamp_receiver"], 1700);
        assert_eq!(value["a"], 1);
        // Exactly one of each key: a duplicate is what the slow path prevents.
        let text = String::from_utf8(stamped.to_vec()).unwrap();
        assert_eq!(text.matches("\"_source\"").count(), 1);
        assert_eq!(text.matches("\"_timestamp_receiver\"").count(), 1);
    }

    #[test]
    fn a_sender_supplied_receiver_timestamp_is_replaced() {
        let stamped = stamp(
            Bytes::from(r#"{"_timestamp_receiver":1,"a":1}"#),
            "runzero",
            1700,
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&stamped).unwrap();
        assert_eq!(value["_timestamp_receiver"], 1700);
        assert_eq!(value["_source"], "runzero");
    }

    #[test]
    fn object_detection_ignores_surrounding_whitespace() {
        assert!(is_json_object(b" \n{\"a\":1}\n"));
        assert!(!is_json_object(b"[1,2]"));
        assert!(!is_json_object(b"\"text\""));
        assert!(!is_json_object(b""));
    }

    fn array_caller(filter: Option<&str>) -> Caller {
        let verifier =
            auth::HmacVerifier::new("bulk", &crate::config::WebhookAuthConfig::default(), b"s")
                .unwrap();
        Caller {
            name: Arc::from("bulk"),
            topic: "bulk_land".to_string(),
            auth: CallerAuth::Hmac(Arc::new(verifier)),
            body: WebhookBody::Array,
            filter: filter.map(|expr| scalo::expression::compile(expr).unwrap()),
        }
    }

    #[test]
    fn a_bad_element_refuses_the_whole_request_before_any_delivery() {
        let records = vec![Bytes::from(r#"{"a":1}"#), Bytes::from("5")];
        let err = array_caller(None).prepare(records, 1700).unwrap_err();
        assert!(
            matches!(err, Error::Validation(ref reason) if reason == "record_not_an_object"),
            "{err}"
        );
    }

    #[test]
    fn every_kept_element_is_stamped_and_the_drops_are_counted() {
        let records = vec![
            Bytes::from(r#"{"n":1}"#),
            Bytes::from(r#"{"n":2}"#),
            Bytes::from(r#"{"n":3}"#),
        ];
        let prepared = array_caller(Some("n != 2")).prepare(records, 1700).unwrap();
        assert_eq!(prepared.dropped, 1);
        let texts: Vec<String> = prepared
            .records
            .iter()
            .map(|r| String::from_utf8(r.to_vec()).unwrap())
            .collect();
        assert_eq!(
            texts,
            [
                r#"{"n":1,"_source":"bulk","_timestamp_receiver":1700}"#,
                r#"{"n":3,"_source":"bulk","_timestamp_receiver":1700}"#,
            ]
        );
    }
}
