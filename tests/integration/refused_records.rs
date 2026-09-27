// Project:   dfe-receiver
// File:      tests/integration/refused_records.rs
// Purpose:   What each listener tells its sender when a record is not taken
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! What each listener tells its sender when the pipeline cannot take a record.
//!
//! Two failures stand in for the ones production sees:
//!
//! - [`refusing_bus`]: a bus that has stopped taking records. The broker is
//!   unroutable, librdkafka queues one record and refuses the rest, and the
//!   receiver's own queue is filled to one record short of its bound, so a
//!   request's first record is taken and every one after it is refused.
//! - [`pressured`]: a pipeline under memory pressure that the test lifts, for
//!   the listeners that hold a record and offer it again.
//!
//! Either way the sender must be told to retry, and a record the pipeline did
//! not take must never be answered as accepted.
//!
//! The refusing bus runs with acknowledgements off, the path that can take part
//! of a request; a held answer retries the whole request instead.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dfe_receiver::config::{
    BUS_DESTINATION, Config, DEFAULT_QUEUE_RECORDS, SharedConfig, WebhookAuthConfig,
    WebhookAuthMode, WebhookBody, WebhookCallerConfig,
};
use dfe_receiver::metrics::Metrics;
use dfe_receiver::pipeline::PipelineState;
use dfe_receiver::server::fluent::FluentHandler;
use dfe_receiver::server::gelf::GelfHandler;
use dfe_receiver::server::grpc::{GrpcVectorHandler, pb as grpc_pb};
use dfe_receiver::server::lumberjack::LumberjackHandler;
use dfe_receiver::server::prometheus_rw::{PrometheusRwHandler, proto as rw_proto};
use dfe_receiver::server::splunk_hec::SplunkHecHandler;
use dfe_receiver::server::syslog::SyslogHandler;
use dfe_receiver::server::traits::ProtocolHandler;
use dfe_receiver::server::webhook::WebhookHandler;
use prost::Message;
use scalo::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};
use scalo::transport::grpc::GrpcTransport;
use scalo::transport::{AcknowledgementsConfig, TransportReceiver};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

/// TEST-NET-1 (RFC 5737): never routable, so no broker here takes a record.
const UNROUTABLE_BROKER: &str = "192.0.2.1:9092";

/// Bytes the pressure guard allows; reserving all of them puts it under
/// pressure.
const GUARD_LIMIT: u64 = 1_000_000;

/// How long a test waits for something it expects to happen.
const DEADLINE: Duration = Duration::from_secs(10);

/// How long a held record is watched to prove it was not let through.
const HELD_FOR: Duration = Duration::from_millis(400);

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

/// A config routing every record to a bus that takes nothing.
fn bus_config() -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    config.destinations.default = BUS_DESTINATION.into();
    config.kafka.brokers = vec![UNROUTABLE_BROKER.to_string()];
    // librdkafka takes one record, then refuses the next with QueueFull.
    config
        .kafka
        .librdkafka_overrides
        .insert("queue.buffering.max.messages".to_string(), "1".to_string());
    config.routing.dlq.enabled = false;
    answer_at_enqueue(&mut config);
    config
}

/// Turn every listener's acknowledgements off, so a record is answered once
/// queued and the buffer these tests fill exists.
fn answer_at_enqueue(config: &mut Config) {
    let off = AcknowledgementsConfig::new(false);
    config.server.acknowledgements = off;
    config.grpc.acknowledgements = off;
    #[cfg(feature = "otlp")]
    {
        config.otlp.acknowledgements = off;
    }
    config.lumberjack.acknowledgements = off;
    config.splunk_hec.acknowledgements = off;
    config.prometheus_rw.acknowledgements = off;
    config.fluent.acknowledgements = off;
    config.webhook.acknowledgements = off;
}

/// Build the pipeline for `config` and fill its queue to one record short of
/// its bound, so the next record is taken and every one after it is refused.
async fn refusing_bus(config: &Config) -> Arc<PipelineState> {
    let pipeline = pipeline_for(config).await;
    let gauge = Metrics::default();
    let room_for_one = (DEFAULT_QUEUE_RECORDS - 1) as u64;
    for _ in 0..=DEFAULT_QUEUE_RECORDS {
        pipeline.update_metrics(&gauge).await;
        if gauge.get_batch_queue_size() >= room_for_one {
            break;
        }
        pipeline
            .process(Bytes::from_static(br#"{"filler":true}"#))
            .await
            .expect("the queue still has room");
    }
    pipeline.update_metrics(&gauge).await;
    assert_eq!(
        gauge.get_batch_queue_size(),
        room_for_one,
        "the queue must have room for exactly one more record"
    );
    pipeline
}

/// A brokerless config that refuses, for good, any record without `must_have`.
fn requiring_config() -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    config.destinations.default = "loader".into();
    config.loader.transport = "memory".to_string();
    config.routing.dlq.enabled = false;
    config.validation.required_fields = vec!["must_have".to_string()];
    config.validation.dlq_on_invalid = false;
    config
}

/// Build the pipeline for `config`.
async fn pipeline_for(config: &Config) -> Arc<PipelineState> {
    Arc::new(
        PipelineState::new(SharedConfig::new(config.clone()), CancellationToken::new())
            .await
            .expect("pipeline init"),
    )
}

/// A config delivering every record to the gRPC destination at `endpoint`.
fn delivering_config(endpoint: &str) -> Config {
    let mut config = Config::default();
    config.server.bind_address = "127.0.0.1:0".to_string();
    config.server.auth.mode = "none".to_string();
    config.destinations.default = "loader".into();
    config.loader.transport = "grpc".to_string();
    config.loader.grpc_endpoint = Some(endpoint.to_string());
    config.routing.dlq.enabled = false;
    config
}

/// Build the pipeline for `config` under memory pressure. Its guard counts
/// only its own reservations, so [`lift`] ends the pressure.
async fn pressured(config: &Config) -> Arc<PipelineState> {
    let guard = MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes: GUARD_LIMIT,
            pressure_threshold: 0.8,
            ..Default::default()
        },
        UsageSource::Reservations,
    );
    let pipeline = Arc::new(
        PipelineState::with_governor(
            SharedConfig::new(config.clone()),
            CancellationToken::new(),
            None,
            Some(Arc::new(guard)),
        )
        .await
        .expect("pipeline init"),
    );
    pipeline.memory_guard().add_bytes(GUARD_LIMIT);
    assert!(pipeline.should_apply_backpressure());
    pipeline
}

/// End the pressure [`pressured`] applied.
fn lift(pipeline: &PipelineState) {
    pipeline.memory_guard().release(GUARD_LIMIT);
    assert!(!pipeline.should_apply_backpressure());
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Start `handler` and return the addresses its listeners bound, in the order
/// `listeners()` names them.
async fn serve<H: ProtocolHandler + 'static>(handler: H) -> (Vec<SocketAddr>, CancellationToken) {
    let name = handler.name();
    let listeners = handler.listeners();
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let mut task = tokio::spawn(async move { handler.start(token).await });
    let mut addrs = Vec::with_capacity(listeners.len());
    for bound in &listeners {
        addrs.push(crate::common::bound_addr(name, bound, &mut task).await);
    }
    (addrs, shutdown)
}

/// Wait until `done` holds, or fail the test at the deadline.
async fn wait_until(what: &str, done: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Whether a record containing `marker` reaches `destination` in time.
async fn delivered(destination: &GrpcTransport, marker: &str) -> bool {
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while tokio::time::Instant::now() < deadline {
        if let Ok(batch) = destination.recv(100).await
            && batch
                .records
                .iter()
                .any(|r| String::from_utf8_lossy(&r.payload).contains(marker))
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// Read what the server sends until it closes the connection or goes quiet.
/// Returns the bytes and whether the server closed.
async fn read_until_closed(stream: &mut TcpStream) -> (Vec<u8>, bool) {
    let mut received = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) => return (received, true),
            Ok(Ok(n)) => received.extend_from_slice(&buf[..n]),
            Err(_) => return (received, false),
        }
    }
}

fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<&str> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
}

// ---------------------------------------------------------------------------
// HTTP listeners
// ---------------------------------------------------------------------------

/// Splunk HEC answers 503 "Server is busy" (code 9) when part of a batch was
/// not taken, as Splunk does. It used to answer 200 "Success" once any one
/// event landed, and the sender dropped the rest.
#[tokio::test]
async fn hec_answers_busy_when_part_of_a_batch_is_not_taken() {
    let mut config = bus_config();
    config.splunk_hec.enabled = true;
    config.splunk_hec.bind_address = "127.0.0.1:0".to_string();
    config.splunk_hec.auth.mode = "none".to_string();
    let pipeline = refusing_bus(&config).await;
    let metrics = Arc::new(Metrics::default());
    let handler = SplunkHecHandler::new(
        config.splunk_hec.clone(),
        config.raw_capture_for(&config.splunk_hec.raw_capture),
        pipeline,
        metrics.clone(),
    );
    let (addrs, shutdown) = serve(handler).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{}/services/collector/event", addrs[0]))
        .body(r#"{"event":"one"}{"event":"two"}{"event":"three"}"#)
        .send()
        .await
        .unwrap();

    let status = resp.status();
    let retry = retry_after(resp.headers()).map(str::to_string);
    let body: serde_json::Value = resp.json().await.unwrap();
    shutdown.cancel();

    assert_eq!(status, 503, "body: {body}");
    assert_eq!(body["code"], 9, "{body}");
    assert!(retry.is_some(), "a busy answer must carry Retry-After");
    assert_eq!(metrics.get_requests_success(), 0);
}

/// A raw HEC request the receiver could not place answers in HEC's own words
/// and nothing of the cause. It used to put the receiver's error text in the
/// body: "Internal server error: configuration error: Kafka sink not
/// configured".
#[tokio::test]
async fn hec_raw_refusal_carries_no_internal_detail() {
    // A DLQ directory under a plain file cannot be made, even as root, so the
    // DLQ fails to start and a dead letter falls back to a bus there is none of.
    let not_a_dir = tempfile::NamedTempFile::new().unwrap();
    let mut config = requiring_config();
    config.validation.dlq_on_invalid = true;
    config.routing.dlq.enabled = true;
    config.routing.dlq.mode = "file_only".to_string();
    config.routing.dlq.kafka_enabled = false;
    config.routing.dlq.file_path = not_a_dir.path().join("dlq").display().to_string();
    config.splunk_hec.enabled = true;
    config.splunk_hec.bind_address = "127.0.0.1:0".to_string();
    config.splunk_hec.auth.mode = "none".to_string();
    let pipeline = pipeline_for(&config).await;
    let metrics = Arc::new(Metrics::default());
    let handler = SplunkHecHandler::new(
        config.splunk_hec.clone(),
        config.raw_capture_for(&config.splunk_hec.raw_capture),
        pipeline,
        metrics.clone(),
    );
    let (addrs, shutdown) = serve(handler).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{}/services/collector/raw", addrs[0]))
        .body("a raw line without the required field")
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    shutdown.cancel();

    let text = body["text"].as_str().unwrap_or_default();
    assert!(
        ["Server is busy", "Internal server error"].contains(&text),
        "the answer must be HEC's own wording and nothing of the cause: {body}"
    );
    assert_eq!(metrics.get_requests_success(), 0);
}

/// Prometheus Remote Write answers 503 when part of a write was not taken:
/// senders MUST retry a 5xx. It used to answer 204 once any sample landed.
#[tokio::test]
async fn remote_write_answers_503_when_part_of_a_write_is_not_taken() {
    let mut config = bus_config();
    config.prometheus_rw.enabled = true;
    config.prometheus_rw.bind_address = "127.0.0.1:0".to_string();
    config.prometheus_rw.auth.mode = "none".to_string();
    let pipeline = refusing_bus(&config).await;
    let metrics = Arc::new(Metrics::default());
    let handler = PrometheusRwHandler::new(
        config.prometheus_rw.clone(),
        config.raw_capture_for(&config.prometheus_rw.raw_capture),
        pipeline,
        metrics.clone(),
    );
    let (addrs, shutdown) = serve(handler).await;

    let series = |name: &str| rw_proto::TimeSeries {
        labels: vec![rw_proto::Label {
            name: "__name__".to_string(),
            value: name.to_string(),
        }],
        samples: vec![rw_proto::Sample {
            value: 1.0,
            timestamp: 1_709_540_000_000,
        }],
        exemplars: vec![],
        histograms: vec![],
    };
    let request = rw_proto::WriteRequest {
        timeseries: vec![series("a"), series("b"), series("c")],
        metadata: vec![],
    };
    let body = snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .unwrap();

    let resp = reqwest::Client::new()
        .post(format!("http://{}/api/v1/write", addrs[0]))
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .body(body)
        .send()
        .await
        .unwrap();
    shutdown.cancel();

    assert_eq!(resp.status(), 503);
    assert!(retry_after(resp.headers()).is_some());
    assert_eq!(metrics.get_requests_success(), 0);
}

/// The webhook answers 503 when part of an array body was not taken. It used
/// to answer 202 once any record landed.
#[tokio::test]
async fn webhook_answers_503_when_part_of_a_body_is_not_taken() {
    const SECRET: &str = "a-static-shared-secret";
    let mut secret = tempfile::NamedTempFile::new().unwrap();
    std::io::Write::write_all(&mut secret, SECRET.as_bytes()).unwrap();

    let mut config = bus_config();
    config.webhook.enabled = true;
    config.webhook.bind_address = Some("127.0.0.1:0".to_string());
    config.webhook.callers = vec![WebhookCallerConfig {
        name: "bulk".to_string(),
        topic: "bulk_land".to_string(),
        auth: WebhookAuthConfig {
            mode: WebhookAuthMode::Header,
            secret_source: format!("file:{}", secret.path().display()),
            refresh_interval_secs: 0,
            header: "x-webhook-secret".to_string(),
            ..WebhookAuthConfig::default()
        },
        body: WebhookBody::Array,
        filter: String::new(),
    }];
    let pipeline = refusing_bus(&config).await;
    let metrics = Arc::new(Metrics::default());
    let handler = WebhookHandler::new(config, pipeline, metrics.clone());
    let (addrs, shutdown) = serve(handler).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{}/webhook/bulk", addrs[0]))
        .header("x-webhook-secret", SECRET)
        .body(r#"[{"n":1},{"n":2},{"n":3}]"#)
        .send()
        .await
        .unwrap();
    shutdown.cancel();

    assert_eq!(resp.status(), 503);
    assert!(retry_after(resp.headers()).is_some());
    assert_eq!(metrics.get_requests_success(), 0);
}

// ---------------------------------------------------------------------------
// OTLP
// ---------------------------------------------------------------------------

#[cfg(feature = "otlp")]
mod otlp {
    use dfe_receiver::server::otlp::{OtlpHandler, pb};

    use super::*;

    /// A logs export carrying three log records.
    fn three_logs() -> pb::collector::logs::v1::ExportLogsServiceRequest {
        use pb::common::v1::{AnyValue, any_value};
        use pb::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

        let record = |text: &str| LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(text.into())),
            }),
            ..LogRecord::default()
        };
        pb::collector::logs::v1::ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![record("one"), record("two"), record("three")],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    async fn otlp_on_a_refusing_bus() -> (Vec<SocketAddr>, CancellationToken, Arc<Metrics>) {
        let mut config = bus_config();
        config.otlp.enabled = true;
        config.otlp.grpc_bind_address = "127.0.0.1:0".to_string();
        config.otlp.http_bind_address = "127.0.0.1:0".to_string();
        config.otlp.auth.mode = "none".to_string();
        let pipeline = refusing_bus(&config).await;
        let metrics = Arc::new(Metrics::default());
        let handler = OtlpHandler::new(
            config.otlp.clone(),
            config.raw_capture_for(&config.otlp.raw_capture),
            pipeline,
            metrics.clone(),
        );
        let (addrs, shutdown) = serve(handler).await;
        (addrs, shutdown, metrics)
    }

    /// OTLP gRPC answers UNAVAILABLE, which the specification makes retryable,
    /// when part of an export was not taken. It used to answer OK once any
    /// record landed, and INTERNAL -- which clients must not retry -- when
    /// none did.
    #[tokio::test]
    async fn otlp_grpc_answers_unavailable_when_part_of_an_export_is_not_taken() {
        use pb::collector::logs::v1::logs_service_client::LogsServiceClient;

        let (addrs, shutdown, metrics) = otlp_on_a_refusing_bus().await;
        let mut client = LogsServiceClient::connect(format!("http://{}", addrs[0]))
            .await
            .unwrap();

        let answer = client.export(three_logs()).await;
        shutdown.cancel();

        let status = answer.expect_err("a partly taken export must not answer OK");
        assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
        assert_eq!(metrics.get_requests_success(), 0);
    }

    /// OTLP/HTTP answers 503 with Retry-After when part of an export was not
    /// taken.
    #[tokio::test]
    async fn otlp_http_answers_503_with_retry_after_when_part_of_an_export_is_not_taken() {
        let (addrs, shutdown, metrics) = otlp_on_a_refusing_bus().await;

        let resp = reqwest::Client::new()
            .post(format!("http://{}/v1/logs", addrs[1]))
            .header("content-type", "application/x-protobuf")
            .body(three_logs().encode_to_vec())
            .send()
            .await
            .unwrap();
        shutdown.cancel();

        assert_eq!(resp.status(), 503);
        assert!(
            retry_after(resp.headers()).is_some(),
            "the specification asks clients to honour Retry-After on a 503"
        );
        assert_eq!(metrics.get_requests_success(), 0);
    }
}

// ---------------------------------------------------------------------------
// gRPC push listener
// ---------------------------------------------------------------------------

/// A log event carrying `fields`, as the push protocol's peer sends it.
fn log_event(fields: &[(&str, &str)]) -> grpc_pb::event::EventWrapper {
    use grpc_pb::event::{EventWrapper, Log, Value, event_wrapper, value};

    let fields = fields
        .iter()
        .map(|(key, text)| {
            let value = Value {
                kind: Some(value::Kind::RawBytes(text.as_bytes().to_vec())),
            };
            ((*key).to_string(), value)
        })
        .collect();
    EventWrapper {
        event: Some(event_wrapper::Event::Log(Log {
            fields,
            ..Log::default()
        })),
    }
}

/// Push `events` to the listener at `addr` and return the gRPC status code.
async fn push(addr: SocketAddr, events: Vec<grpc_pb::event::EventWrapper>) -> tonic::Code {
    use grpc_pb::vector::{PushEventsRequest, PushEventsResponse};

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let codec = tonic_prost::ProstCodec::<PushEventsRequest, PushEventsResponse>::default();
    let path = http::uri::PathAndQuery::from_static("/vector.Vector/PushEvents");
    match client
        .unary(
            tonic::Request::new(PushEventsRequest { events }),
            path,
            codec,
        )
        .await
    {
        Ok(_) => tonic::Code::Ok,
        Err(status) => status.code(),
    }
}

async fn grpc_listener(
    mut config: Config,
    pipeline: Arc<PipelineState>,
) -> (SocketAddr, CancellationToken, Arc<Metrics>) {
    config.grpc.enabled = true;
    config.grpc.bind_address = "127.0.0.1:0".to_string();
    let metrics = Arc::new(Metrics::default());
    let handler = GrpcVectorHandler::new(config, pipeline, metrics.clone());
    let (addrs, shutdown) = serve(handler).await;
    (addrs[0], shutdown, metrics)
}

/// The gRPC push listener answers UNAVAILABLE, which the peer retries, when
/// part of a push was not taken. It used to answer INTERNAL for every failure.
#[tokio::test]
async fn grpc_push_answers_unavailable_when_part_of_a_push_is_not_taken() {
    let config = bus_config();
    let pipeline = refusing_bus(&config).await;
    let (addr, shutdown, metrics) = grpc_listener(config, pipeline).await;

    let code = push(
        addr,
        vec![
            log_event(&[("message", "one")]),
            log_event(&[("message", "two")]),
            log_event(&[("message", "three")]),
        ],
    )
    .await;
    shutdown.cancel();

    assert_eq!(code, tonic::Code::Unavailable);
    assert_eq!(metrics.get_requests_success(), 0);
}

/// The gRPC push listener answers INVALID_ARGUMENT, which the peer drops, for
/// a record refused for good. It used to answer INTERNAL, which the peer
/// retries, resending a record that can never land.
#[tokio::test]
async fn grpc_push_answers_invalid_argument_for_a_record_refused_for_good() {
    let config = requiring_config();
    let pipeline = pipeline_for(&config).await;
    let (addr, shutdown, metrics) = grpc_listener(config, pipeline).await;

    let code = push(addr, vec![log_event(&[("message", "no required field")])]).await;
    shutdown.cancel();

    assert_eq!(code, tonic::Code::InvalidArgument);
    assert_eq!(metrics.get_requests_success(), 0);
}

// ---------------------------------------------------------------------------
// Acknowledged TCP protocols
// ---------------------------------------------------------------------------

/// A Lumberjack v2 frame header: version `2` and the frame type.
fn lumberjack_frame(kind: u8) -> Vec<u8> {
    vec![b'2', kind]
}

/// Lumberjack acknowledges only the events it took, then closes, so Beats
/// resends the rest. It used to acknowledge the whole window.
#[tokio::test]
async fn lumberjack_acknowledges_only_the_events_it_took() {
    let mut config = bus_config();
    config.lumberjack.enabled = true;
    config.lumberjack.bind_address = "127.0.0.1:0".to_string();
    let pipeline = refusing_bus(&config).await;
    let metrics = Arc::new(Metrics::default());
    let handler = LumberjackHandler::new(config.lumberjack.clone(), pipeline, metrics.clone());
    let (addrs, shutdown) = serve(handler).await;

    let mut window = lumberjack_frame(b'W');
    window.extend_from_slice(&3u32.to_be_bytes());
    for sequence in 1..=3u32 {
        let payload = format!(r#"{{"message":"event {sequence}"}}"#);
        window.extend_from_slice(&lumberjack_frame(b'J'));
        window.extend_from_slice(&sequence.to_be_bytes());
        window.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        window.extend_from_slice(payload.as_bytes());
    }
    let mut stream = TcpStream::connect(addrs[0]).await.unwrap();
    stream.write_all(&window).await.unwrap();

    let (received, closed) = read_until_closed(&mut stream).await;
    shutdown.cancel();

    let acked: Vec<u32> = received
        .as_chunks::<6>()
        .0
        .iter()
        .filter(|ack| ack[..2] == *b"2A")
        .map(|ack| u32::from_be_bytes([ack[2], ack[3], ack[4], ack[5]]))
        .collect();
    assert_eq!(
        acked.iter().max(),
        Some(&1),
        "only the first event was taken, got ACKs {acked:?}"
    );
    assert!(
        closed,
        "the connection must close so Beats resends the rest"
    );
    assert_eq!(metrics.get_requests_success(), 1);
}

/// A Fluent Forward message in Message mode carrying `chunk`.
fn fluent_message(text: &str, chunk: Option<&str>) -> Vec<u8> {
    use rmpv::Value;

    let mut fields = vec![
        Value::String("app.log".into()),
        Value::Integer(1_700_000_000.into()),
        Value::Map(vec![(
            Value::String("message".into()),
            Value::String(text.into()),
        )]),
    ];
    if let Some(chunk) = chunk {
        fields.push(Value::Map(vec![(
            Value::String("chunk".into()),
            Value::String(chunk.into()),
        )]));
    }
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &Value::Array(fields)).unwrap();
    buf
}

fn fluent_config(config: &mut Config) {
    config.fluent.enabled = true;
    config.fluent.bind_address = "127.0.0.1:0".to_string();
    config.fluent.tls.enabled = false;
}

/// Fluent Forward withholds the ack for a chunk it could not take, and the
/// sender resends it. It used to ack every chunk.
#[tokio::test]
async fn fluent_withholds_the_ack_for_a_chunk_it_could_not_take() {
    let mut config = bus_config();
    fluent_config(&mut config);
    let pipeline = pressured(&config).await;
    let metrics = Arc::new(Metrics::default());
    let handler = FluentHandler::new(
        config.fluent.clone(),
        config.raw_capture_for(&config.fluent.raw_capture),
        pipeline,
        metrics.clone(),
    );
    let (addrs, shutdown) = serve(handler).await;

    let mut stream = TcpStream::connect(addrs[0]).await.unwrap();
    stream
        .write_all(&fluent_message("not taken", Some("Y2h1bmstMQ==")))
        .await
        .unwrap();

    let (received, closed) = read_until_closed(&mut stream).await;
    shutdown.cancel();

    assert!(
        received.is_empty(),
        "a chunk the pipeline did not take was acknowledged: {received:?}"
    );
    assert!(
        closed,
        "the connection must close so the sender resends now"
    );
    assert_eq!(metrics.get_requests_success(), 0);
}

// ---------------------------------------------------------------------------
// Unacknowledged TCP protocols: hold and retry
// ---------------------------------------------------------------------------

/// Write `bytes` to the TCP listener, hold pressure until the listener has read
/// them, check nothing was taken, lift the pressure, and report whether the
/// record carrying `marker` then reached the destination and was counted as
/// taken.
async fn held_then_delivered(
    pipeline: &PipelineState,
    metrics: &Metrics,
    destination: &GrpcTransport,
    listener: SocketAddr,
    bytes: &[u8],
    marker: &str,
) -> bool {
    let mut stream = TcpStream::connect(listener).await.unwrap();
    stream.write_all(bytes).await.unwrap();

    wait_until("the listener read the record", || {
        metrics.get_requests_total() == 1
    })
    .await;
    tokio::time::sleep(HELD_FOR).await;
    assert_eq!(
        metrics.get_requests_success(),
        0,
        "a record the pipeline could not take was counted as taken"
    );

    lift(pipeline);
    let arrived = delivered(destination, marker).await;
    // The destination can see the record before the listener counts it.
    if arrived {
        wait_until("the listener counted the record as taken", || {
            metrics.get_requests_success() == 1
        })
        .await;
    }
    arrived
}

/// Syslog over TCP holds a line the pipeline cannot take and delivers it once
/// the pipeline recovers. It used to drop the line.
#[tokio::test]
async fn syslog_tcp_holds_a_line_until_the_pipeline_takes_it() {
    let (endpoint, destination) = crate::common::grpc_destination().await;
    let mut config = delivering_config(&endpoint);
    config.syslog.enabled = true;
    config.syslog.udp_bind_address = "127.0.0.1:0".to_string();
    config.syslog.tcp_bind_address = "127.0.0.1:0".to_string();
    config.syslog.tls.enabled = false;
    let pipeline = pressured(&config).await;
    let metrics = Arc::new(Metrics::default());
    let handler = SyslogHandler::new(
        config.syslog.clone(),
        config.raw_capture_for(&config.syslog.raw_capture),
        pipeline.clone(),
        metrics.clone(),
    );
    let (addrs, shutdown) = serve(handler).await;

    let marker = "held-syslog-line";
    let line = format!("<14>Sep 25 10:00:00 host app: {marker}\n");
    let arrived = held_then_delivered(
        &pipeline,
        &metrics,
        &destination,
        addrs[1],
        line.as_bytes(),
        marker,
    )
    .await;
    shutdown.cancel();

    assert!(
        arrived,
        "the held syslog line never reached the destination"
    );
}

/// GELF over TCP holds a message the pipeline cannot take and delivers it once
/// the pipeline recovers. It used to drop the message.
#[tokio::test]
async fn gelf_tcp_holds_a_message_until_the_pipeline_takes_it() {
    let (endpoint, destination) = crate::common::grpc_destination().await;
    let mut config = delivering_config(&endpoint);
    config.gelf.enabled = true;
    config.gelf.bind_address = "127.0.0.1:0".to_string();
    config.gelf.tls.enabled = false;
    let pipeline = pressured(&config).await;
    let metrics = Arc::new(Metrics::default());
    let handler = GelfHandler::new(
        config.gelf.clone(),
        config.raw_capture_for(&config.gelf.raw_capture),
        pipeline.clone(),
        metrics.clone(),
    );
    let (addrs, shutdown) = serve(handler).await;

    let marker = "held-gelf-message";
    let message = format!(r#"{{"version":"1.1","host":"h","short_message":"{marker}"}}"#);
    let mut frame = message.into_bytes();
    frame.push(0);
    let arrived =
        held_then_delivered(&pipeline, &metrics, &destination, addrs[0], &frame, marker).await;
    shutdown.cancel();

    assert!(
        arrived,
        "the held GELF message never reached the destination"
    );
}

/// Fluent Forward without `chunk` has no ack to withhold, so it holds the
/// message and delivers it once the pipeline recovers. It used to drop it.
#[tokio::test]
async fn fluent_without_chunk_holds_a_message_until_the_pipeline_takes_it() {
    let (endpoint, destination) = crate::common::grpc_destination().await;
    let mut config = delivering_config(&endpoint);
    fluent_config(&mut config);
    let pipeline = pressured(&config).await;
    let metrics = Arc::new(Metrics::default());
    let handler = FluentHandler::new(
        config.fluent.clone(),
        config.raw_capture_for(&config.fluent.raw_capture),
        pipeline.clone(),
        metrics.clone(),
    );
    let (addrs, shutdown) = serve(handler).await;

    let marker = "held-fluent-message";
    let arrived = held_then_delivered(
        &pipeline,
        &metrics,
        &destination,
        addrs[0],
        &fluent_message(marker, None),
        marker,
    )
    .await;
    shutdown.cancel();

    assert!(
        arrived,
        "the held Fluent message never reached the destination"
    );
}
