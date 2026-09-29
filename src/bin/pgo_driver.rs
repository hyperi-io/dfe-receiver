// Project:   dfe-receiver
// File:      src/bin/pgo_driver.rs
// Purpose:   PGO workload driver -- production-shaped traffic plus a delivery gate
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! PGO workload driver for dfe-receiver.
//!
//! Sends production-shaped traffic to a running receiver so a PGO-instrumented
//! binary profiles the paths a deployment takes, then reads the receiver's
//! `/metrics` and fails unless every listener took records and the Kafka sink
//! delivered them. `scripts/pgo-workload.sh` runs it against a local broker.
//!
//! Builds only when the `pgo-driver` feature is enabled:
//! `cargo build --release --features pgo-driver --bin pgo-driver`
//!
//! Payloads are real captures, not invented shapes: the Elastic-shipped events
//! in `tests/fixtures/pgo/` and the flow datagrams in
//! `tests/fixtures/flow/external/`.
//!
//! Configuration via environment variables:
//! - `PGO_DRIVER_DURATION_SECS` (default 300) -- load duration
//! - `PGO_DRIVER_HTTP_URL` (default `http://127.0.0.1:8080/ingest`)
//! - `PGO_DRIVER_READY_URL` (default `http://127.0.0.1:8080/readyz`)
//! - `PGO_DRIVER_METRICS_URL` (default `http://127.0.0.1:9090/metrics`)
//! - `PGO_DRIVER_LUMBERJACK_ADDR` (default `127.0.0.1:5044`)
//! - `PGO_DRIVER_PROM_RW_URL` (default `http://127.0.0.1:9091/api/v1/write`)
//! - `PGO_DRIVER_HEC_URL` (default `http://127.0.0.1:8088/services/collector/event`)
//! - `PGO_DRIVER_OTLP_HTTP_URL` (default `http://127.0.0.1:4318/v1/logs`)
//! - `PGO_DRIVER_SYSLOG_UDP` (default `127.0.0.1:5514`)
//! - `PGO_DRIVER_SYSLOG_TCP` (default `127.0.0.1:5515`)
//! - `PGO_DRIVER_NETFLOW_ADDR` (default `127.0.0.1:2055`)
//! - `PGO_DRIVER_SFLOW_ADDR` (default `127.0.0.1:6343`)
//! - `PGO_DRIVER_HTTP_RPS` (default 200) -- HTTP ingest requests per second
//! - `PGO_DRIVER_OTHER_RPS` (default 50) -- requests or messages per second on
//!   each other listener; lumberjack sends this many windows per second
//! - `PGO_DRIVER_FLOW_PPS` (default 200) -- datagrams per second per flow type
//!
//! Exit codes:
//! - 0: workload ran for the full duration and the delivery gate passed
//! - 2: receiver unreachable
//! - 3: delivery gate failed

#![allow(clippy::expect_used)] // A workload driver: a setup failure should stop the run.

use std::env;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use prost::Message;
use reqwest::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};

/// Elastic-shipped events, one per line: see `tests/fixtures/pgo/README.md`.
const BEATS_EVENTS: &str = include_str!("../../tests/fixtures/pgo/beats-events.ndjson");

/// NetFlow datagrams as exporters emit them: v5, and v9 carrying its templates.
const NETFLOW_DATAGRAMS: [&[u8]; 3] = [
    include_bytes!("../../tests/fixtures/flow/external/telegraf-netflow-v5.bin"),
    include_bytes!("../../tests/fixtures/flow/external/telegraf-netflow-v9.bin"),
    include_bytes!("../../tests/fixtures/flow/external/telegraf-netflow-v9-options.bin"),
];

/// sFlow v5 datagrams carrying flow samples.
const SFLOW_DATAGRAMS: [&[u8]; 2] = [
    include_bytes!("../../tests/fixtures/flow/external/telegraf-sflow-v5.bin"),
    include_bytes!("../../tests/fixtures/flow/external/telegraf-sflow-issue-15918.bin"),
];

/// Events per lumberjack window; Beats batches in the low thousands.
const LUMBERJACK_WINDOW: usize = 200;

/// Events per NDJSON batch on the HTTP listener.
const NDJSON_BATCH: usize = 100;

/// Events per JSON-array batch on the HTTP listener.
const ARRAY_BATCH: usize = 50;

/// Concurrent senders per HTTP listener. A listener holds its answer until the
/// broker confirms delivery, so one sender cannot reach the configured rate.
const HTTP_WORKERS: u32 = 16;

/// Longest the gate waits for the Kafka sink to finish delivering.
const SETTLE_LIMIT: Duration = Duration::from_secs(30);

/// Listeners whose `receiver_requests_success_total` must be non-zero.
const GATED_TRANSPORTS: [&str; 6] = [
    "http",
    "lumberjack",
    "splunk_hec",
    "prometheus_rw",
    "otlp",
    "syslog",
];

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let cfg = Config::from_env();
    println!("pgo-driver starting: {cfg:#?}");

    if !wait_for_receiver(&cfg.ready_url).await {
        eprintln!("pgo-driver: receiver not ready at {}", cfg.ready_url);
        std::process::exit(2);
    }

    let events: Vec<Bytes> = BEATS_EVENTS
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| Bytes::copy_from_slice(line.as_bytes()))
        .collect();
    let stats = Arc::new(Stats::new());
    let deadline = Instant::now() + Duration::from_secs(cfg.duration_secs);
    let mut tasks = JoinSet::new();

    let json = [("content-type", "application/json")];
    let protobuf = [("content-type", "application/x-protobuf")];
    let posts = [
        Post::new(&stats.http, &cfg.http_url, &json, http_bodies(&events)),
        Post::new(&stats.hec, &cfg.hec_url, &json, hec_bodies(&events)),
        Post::new(
            &stats.prom_rw,
            &cfg.prom_rw_url,
            &[
                ("content-type", "application/x-protobuf"),
                ("content-encoding", "snappy"),
                ("x-prometheus-remote-write-version", "0.1.0"),
            ],
            prom_rw_bodies(),
        ),
        Post::new(&stats.otlp, &cfg.otlp_http_url, &protobuf, otlp_bodies()),
    ];
    for post in posts {
        let rps = if post.counter.name == "http" {
            cfg.http_rps
        } else {
            cfg.other_rps
        };
        let post = Arc::new(post);
        let workers = HTTP_WORKERS.min(rps.max(1));
        for worker in 0..workers {
            tasks.spawn(drive_post(
                post.clone(),
                rps.div_ceil(workers),
                worker as usize,
                deadline,
            ));
        }
    }
    tasks.spawn(drive_lumberjack(
        cfg.clone(),
        stats.clone(),
        events.clone(),
        deadline,
    ));
    tasks.spawn(drive_syslog_udp(
        cfg.clone(),
        stats.clone(),
        events.clone(),
        deadline,
    ));
    tasks.spawn(drive_syslog_tcp(
        cfg.clone(),
        stats.clone(),
        events.clone(),
        deadline,
    ));
    tasks.spawn(drive_datagrams(
        cfg.netflow_addr.clone(),
        &NETFLOW_DATAGRAMS,
        cfg.flow_pps,
        stats.clone(),
        Protocol::Netflow,
        deadline,
    ));
    tasks.spawn(drive_datagrams(
        cfg.sflow_addr.clone(),
        &SFLOW_DATAGRAMS,
        cfg.flow_pps,
        stats.clone(),
        Protocol::Sflow,
        deadline,
    ));

    let reporter_stats = stats.clone();
    tasks.spawn(async move {
        let mut tick = interval(Duration::from_secs(15));
        tick.tick().await;
        let end = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(end);
        loop {
            tokio::select! {
                _ = tick.tick() => reporter_stats.report(),
                () = &mut end => break,
            }
        }
    });

    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            eprintln!("pgo-driver task error: {e}");
        }
    }

    stats.report();
    println!("pgo-driver: load complete, checking delivery");
    if let Err(reason) = delivery_gate(&cfg, &stats).await {
        eprintln!("pgo-driver: delivery gate FAILED: {reason}");
        std::process::exit(3);
    }
    println!("pgo-driver: delivery gate passed");
}

async fn wait_for_receiver(url: &str) -> bool {
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("reqwest client");
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(resp) = client.get(url).send().await
            && resp.status().is_success()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

// ===========================================================================
// Config
// ===========================================================================

#[derive(Clone, Debug)]
struct Config {
    duration_secs: u64,
    http_url: String,
    ready_url: String,
    metrics_url: String,
    lumberjack_addr: String,
    prom_rw_url: String,
    hec_url: String,
    otlp_http_url: String,
    syslog_udp_addr: String,
    syslog_tcp_addr: String,
    netflow_addr: String,
    sflow_addr: String,
    http_rps: u32,
    other_rps: u32,
    flow_pps: u32,
}

impl Config {
    fn from_env() -> Self {
        Self {
            duration_secs: env_parsed("PGO_DRIVER_DURATION_SECS", 300),
            http_url: env_str("PGO_DRIVER_HTTP_URL", "http://127.0.0.1:8080/ingest"),
            ready_url: env_str("PGO_DRIVER_READY_URL", "http://127.0.0.1:8080/readyz"),
            metrics_url: env_str("PGO_DRIVER_METRICS_URL", "http://127.0.0.1:9090/metrics"),
            lumberjack_addr: env_str("PGO_DRIVER_LUMBERJACK_ADDR", "127.0.0.1:5044"),
            prom_rw_url: env_str(
                "PGO_DRIVER_PROM_RW_URL",
                "http://127.0.0.1:9091/api/v1/write",
            ),
            hec_url: env_str(
                "PGO_DRIVER_HEC_URL",
                "http://127.0.0.1:8088/services/collector/event",
            ),
            otlp_http_url: env_str("PGO_DRIVER_OTLP_HTTP_URL", "http://127.0.0.1:4318/v1/logs"),
            syslog_udp_addr: env_str("PGO_DRIVER_SYSLOG_UDP", "127.0.0.1:5514"),
            syslog_tcp_addr: env_str("PGO_DRIVER_SYSLOG_TCP", "127.0.0.1:5515"),
            netflow_addr: env_str("PGO_DRIVER_NETFLOW_ADDR", "127.0.0.1:2055"),
            sflow_addr: env_str("PGO_DRIVER_SFLOW_ADDR", "127.0.0.1:6343"),
            http_rps: env_parsed("PGO_DRIVER_HTTP_RPS", 200),
            other_rps: env_parsed("PGO_DRIVER_OTHER_RPS", 50),
            flow_pps: env_parsed("PGO_DRIVER_FLOW_PPS", 200),
        }
    }
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parsed<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ===========================================================================
// Stats
// ===========================================================================

/// Outcomes on one protocol.
struct Counter {
    name: &'static str,
    /// Requests answered 2xx, frames acknowledged, or datagrams sent.
    ok: AtomicU64,
    /// Requests answered with a non-2xx status.
    rejected: AtomicU64,
    /// Sends that failed before an answer.
    failed: AtomicU64,
}

impl Counter {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            ok: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        }
    }
}

#[derive(Clone, Copy)]
enum Protocol {
    Netflow,
    Sflow,
}

struct Stats {
    http: Arc<Counter>,
    lumberjack: Arc<Counter>,
    hec: Arc<Counter>,
    prom_rw: Arc<Counter>,
    otlp: Arc<Counter>,
    syslog_udp: Arc<Counter>,
    syslog_tcp: Arc<Counter>,
    netflow: Arc<Counter>,
    sflow: Arc<Counter>,
    start: Instant,
}

impl Stats {
    fn new() -> Self {
        Self {
            http: Arc::new(Counter::new("http")),
            lumberjack: Arc::new(Counter::new("lumberjack")),
            hec: Arc::new(Counter::new("hec")),
            prom_rw: Arc::new(Counter::new("prom_rw")),
            otlp: Arc::new(Counter::new("otlp")),
            syslog_udp: Arc::new(Counter::new("syslog_udp")),
            syslog_tcp: Arc::new(Counter::new("syslog_tcp")),
            netflow: Arc::new(Counter::new("netflow")),
            sflow: Arc::new(Counter::new("sflow")),
            start: Instant::now(),
        }
    }

    fn all(&self) -> [&Counter; 9] {
        [
            &self.http,
            &self.lumberjack,
            &self.hec,
            &self.prom_rw,
            &self.otlp,
            &self.syslog_udp,
            &self.syslog_tcp,
            &self.netflow,
            &self.sflow,
        ]
    }

    fn flow(&self, protocol: Protocol) -> &Counter {
        match protocol {
            Protocol::Netflow => &self.netflow,
            Protocol::Sflow => &self.sflow,
        }
    }

    fn report(&self) {
        let mut line = format!("pgo-driver [{:>6.1}s]", self.start.elapsed().as_secs_f64());
        for c in self.all() {
            line.push_str(&format!(
                " {}={}/{}/{}",
                c.name,
                c.ok.load(Ordering::Relaxed),
                c.rejected.load(Ordering::Relaxed),
                c.failed.load(Ordering::Relaxed),
            ));
        }
        println!("{line} (ok/rejected/failed)");
    }
}

// ===========================================================================
// Protocol drivers
// ===========================================================================

fn rate_limiter(per_sec: u32) -> tokio::time::Interval {
    let period = Duration::from_nanos(1_000_000_000 / u64::from(per_sec.max(1)));
    let mut iv = interval(period);
    iv.set_missed_tick_behavior(MissedTickBehavior::Delay);
    iv
}

/// One HTTP listener: where to post, how, and the bodies to cycle through.
struct Post {
    counter: Arc<Counter>,
    url: String,
    headers: Vec<(&'static str, &'static str)>,
    bodies: Vec<Bytes>,
    client: Client,
}

impl Post {
    fn new(
        counter: &Arc<Counter>,
        url: &str,
        headers: &[(&'static str, &'static str)],
        bodies: Vec<Bytes>,
    ) -> Self {
        Self {
            counter: counter.clone(),
            url: url.to_string(),
            headers: headers.to_vec(),
            bodies,
            client: Client::builder()
                .pool_max_idle_per_host(HTTP_WORKERS as usize)
                .timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
        }
    }
}

async fn drive_post(post: Arc<Post>, rps: u32, offset: usize, deadline: Instant) {
    let mut tick = rate_limiter(rps);
    let mut idx = offset;
    while Instant::now() < deadline {
        tick.tick().await;
        let body = post.bodies[idx % post.bodies.len()].clone();
        idx = idx.wrapping_add(1);
        let mut req = post.client.post(&post.url).body(body);
        for (name, value) in &post.headers {
            req = req.header(*name, *value);
        }
        let outcome = match req.send().await {
            Ok(resp) if resp.status().is_success() => &post.counter.ok,
            Ok(_) => &post.counter.rejected,
            Err(_) => &post.counter.failed,
        };
        outcome.fetch_add(1, Ordering::Relaxed);
    }
}

/// Beats' logstash output: a window frame, then the window's events as JSON
/// frames inside one zlib-compressed frame, then wait for the ACK of the last.
async fn drive_lumberjack(cfg: Config, stats: Arc<Stats>, events: Vec<Bytes>, deadline: Instant) {
    let counter = &stats.lumberjack;
    let window = lumberjack_window(&events);
    let last_sequence = u32::try_from(LUMBERJACK_WINDOW).expect("window fits u32");
    let mut tick = rate_limiter(cfg.other_rps);
    let mut conn: Option<TcpStream> = None;
    while Instant::now() < deadline {
        tick.tick().await;
        if conn.is_none() {
            conn = TcpStream::connect(&cfg.lumberjack_addr).await.ok();
        }
        let Some(stream) = conn.as_mut() else {
            counter.failed.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        let mut ack = [0u8; 6];
        let sent = async {
            stream.write_all(&window).await?;
            stream.read_exact(&mut ack).await?;
            Ok::<_, std::io::Error>(())
        };
        match tokio::time::timeout(Duration::from_secs(10), sent).await {
            Ok(Ok(())) if ack[..2] == *b"2A" && ack[2..] == last_sequence.to_be_bytes() => {
                counter.ok.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Ok(())) => {
                counter.rejected.fetch_add(1, Ordering::Relaxed);
                conn = None;
            }
            _ => {
                counter.failed.fetch_add(1, Ordering::Relaxed);
                conn = None;
            }
        }
    }
}

fn lumberjack_window(events: &[Bytes]) -> Vec<u8> {
    let mut frames = Vec::new();
    for (i, event) in events.iter().cycle().take(LUMBERJACK_WINDOW).enumerate() {
        let sequence = u32::try_from(i + 1).expect("sequence fits u32");
        let len = u32::try_from(event.len()).expect("event fits u32");
        frames.extend_from_slice(b"2J");
        frames.extend_from_slice(&sequence.to_be_bytes());
        frames.extend_from_slice(&len.to_be_bytes());
        frames.extend_from_slice(event);
    }
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(3));
    encoder.write_all(&frames).expect("zlib into a Vec");
    let compressed = encoder.finish().expect("zlib into a Vec");

    let window = u32::try_from(LUMBERJACK_WINDOW).expect("window fits u32");
    let compressed_len = u32::try_from(compressed.len()).expect("frame fits u32");
    let mut out = Vec::with_capacity(compressed.len() + 12);
    out.extend_from_slice(b"2W");
    out.extend_from_slice(&window.to_be_bytes());
    out.extend_from_slice(b"2C");
    out.extend_from_slice(&compressed_len.to_be_bytes());
    out.extend_from_slice(&compressed);
    out
}

async fn drive_syslog_udp(cfg: Config, stats: Arc<Stats>, events: Vec<Bytes>, deadline: Instant) {
    let counter = &stats.syslog_udp;
    let Ok(sock) = UdpSocket::bind("127.0.0.1:0").await else {
        counter.failed.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let lines = syslog_lines(&events);
    let mut tick = rate_limiter(cfg.other_rps);
    let mut idx = 0usize;
    while Instant::now() < deadline {
        tick.tick().await;
        let line = &lines[idx % lines.len()];
        idx = idx.wrapping_add(1);
        let outcome = match sock.send_to(line, &cfg.syslog_udp_addr).await {
            Ok(_) => &counter.ok,
            Err(_) => &counter.failed,
        };
        outcome.fetch_add(1, Ordering::Relaxed);
    }
}

/// One long-lived connection with RFC 6587 octet-counted framing, as a relay
/// forwards it; reconnects when the receiver drops it.
async fn drive_syslog_tcp(cfg: Config, stats: Arc<Stats>, events: Vec<Bytes>, deadline: Instant) {
    let counter = &stats.syslog_tcp;
    let frames: Vec<Vec<u8>> = syslog_lines(&events)
        .iter()
        .map(|line| {
            let mut frame = format!("{} ", line.len()).into_bytes();
            frame.extend_from_slice(line);
            frame
        })
        .collect();
    let mut tick = rate_limiter(cfg.other_rps);
    let mut idx = 0usize;
    let mut conn: Option<TcpStream> = None;
    while Instant::now() < deadline {
        tick.tick().await;
        if conn.is_none() {
            conn = TcpStream::connect(&cfg.syslog_tcp_addr).await.ok();
        }
        let Some(stream) = conn.as_mut() else {
            counter.failed.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        let frame = &frames[idx % frames.len()];
        idx = idx.wrapping_add(1);
        if stream.write_all(frame).await.is_ok() {
            counter.ok.fetch_add(1, Ordering::Relaxed);
        } else {
            counter.failed.fetch_add(1, Ordering::Relaxed);
            conn = None;
        }
    }
    if let Some(mut stream) = conn {
        let _ = stream.shutdown().await;
    }
}

async fn drive_datagrams(
    addr: String,
    datagrams: &'static [&'static [u8]],
    pps: u32,
    stats: Arc<Stats>,
    protocol: Protocol,
    deadline: Instant,
) {
    let counter = stats.flow(protocol);
    let Ok(sock) = UdpSocket::bind("127.0.0.1:0").await else {
        counter.failed.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let mut tick = rate_limiter(pps);
    let mut idx = 0usize;
    while Instant::now() < deadline {
        tick.tick().await;
        let datagram = datagrams[idx % datagrams.len()];
        idx = idx.wrapping_add(1);
        let outcome = match sock.send_to(datagram, &addr).await {
            Ok(_) => &counter.ok,
            Err(_) => &counter.failed,
        };
        outcome.fetch_add(1, Ordering::Relaxed);
    }
}

// ===========================================================================
// Payload corpora
// ===========================================================================

/// Single events, an NDJSON batch and a JSON-array batch: the three shapes the
/// HTTP listener splits.
fn http_bodies(events: &[Bytes]) -> Vec<Bytes> {
    let mut bodies: Vec<Bytes> = events.to_vec();

    let mut ndjson = Vec::new();
    for event in events.iter().cycle().take(NDJSON_BATCH) {
        ndjson.extend_from_slice(event);
        ndjson.push(b'\n');
    }
    bodies.push(Bytes::from(ndjson));

    let mut array = vec![b'['];
    for (i, event) in events.iter().cycle().take(ARRAY_BATCH).enumerate() {
        if i > 0 {
            array.push(b',');
        }
        array.extend_from_slice(event);
    }
    array.push(b']');
    bodies.push(Bytes::from(array));
    bodies
}

/// The vendor line each event carries, as syslog would have delivered it.
fn vendor_lines(events: &[Bytes]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| {
            let value: serde_json::Value = serde_json::from_slice(event).ok()?;
            value
                .get("message")
                .or_else(|| value.pointer("/event/original"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect()
}

/// Each vendor line as RFC 3164 and as RFC 5424.
fn syslog_lines(events: &[Bytes]) -> Vec<Bytes> {
    let mut lines = Vec::new();
    for msg in vendor_lines(events) {
        lines.push(Bytes::from(format!("<189>{msg}")));
        lines.push(Bytes::from(format!(
            "<189>1 2026-09-30T04:00:48.272Z edge-rtr-01 cisco-ios - SYS-5 - {msg}"
        )));
    }
    lines
}

/// Splunk HEC batches: concatenated event objects with the metadata a
/// forwarder sets.
fn hec_bodies(events: &[Bytes]) -> Vec<Bytes> {
    let lines = vendor_lines(events);
    (0..4)
        .map(|batch| {
            let mut body = String::new();
            for (i, line) in lines.iter().cycle().skip(batch).take(20).enumerate() {
                let event = serde_json::json!({
                    "time": 1_759_204_848.272 + i as f64,
                    "host": format!("edge-rtr-{:02}", i % 8),
                    "source": "udp:514",
                    "sourcetype": "cisco:ios",
                    "index": "network",
                    "fields": { "site": "syd1", "env": "prod" },
                    "event": line,
                });
                body.push_str(&event.to_string());
            }
            Bytes::from(body)
        })
        .collect()
}

/// Remote-write requests shaped like a Prometheus agent's: 50 series across
/// node-exporter metric families, one sample each.
fn prom_rw_bodies() -> Vec<Bytes> {
    use dfe_receiver::server::prometheus_rw::proto;
    const FAMILIES: [&str; 5] = [
        "node_cpu_seconds_total",
        "node_memory_MemAvailable_bytes",
        "node_network_receive_bytes_total",
        "node_filesystem_avail_bytes",
        "node_load1",
    ];
    let ts = chrono::Utc::now().timestamp_millis();
    (0..8)
        .map(|variant| {
            let timeseries = (0..50)
                .map(|i| {
                    let label = |name: &str, value: String| proto::Label {
                        name: name.into(),
                        value,
                    };
                    proto::TimeSeries {
                        labels: vec![
                            label("__name__", FAMILIES[i % FAMILIES.len()].into()),
                            label("instance", format!("node-{:02}:9100", (i + variant) % 16)),
                            label("job", "node".into()),
                            label("cpu", (i % 8).to_string()),
                        ],
                        samples: vec![proto::Sample {
                            value: 1_000.0 + (i * 17 + variant) as f64,
                            timestamp: ts,
                        }],
                        exemplars: vec![],
                        histograms: vec![],
                    }
                })
                .collect();
            let req = proto::WriteRequest {
                timeseries,
                metadata: vec![],
            };
            let compressed = snap::raw::Encoder::new()
                .compress_vec(&req.encode_to_vec())
                .expect("snappy into a Vec");
            Bytes::from(compressed)
        })
        .collect()
}

/// OTLP log exports shaped like a collector's batch: 50 records under one
/// resource, each with attributes.
fn otlp_bodies() -> Vec<Bytes> {
    use dfe_receiver::server::otlp::pb;
    use pb::common::v1::{AnyValue, KeyValue, any_value::Value};
    let kv = |key: &str, value: &str| KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
    };
    let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    (0..4u64)
        .map(|variant| {
            let log_records = (0..50u64)
                .map(|i| pb::logs::v1::LogRecord {
                    time_unix_nano: ts + i,
                    observed_time_unix_nano: ts + i,
                    severity_number: if i % 10 == 0 { 17 } else { 9 },
                    severity_text: if i % 10 == 0 { "ERROR" } else { "INFO" }.into(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue(format!(
                            "GET /api/v1/orders/{} 200 {}ms",
                            i * 31 + variant,
                            3 + i % 40
                        ))),
                    }),
                    attributes: vec![
                        kv("http.request.method", "GET"),
                        kv("http.route", "/api/v1/orders/{id}"),
                        kv("url.scheme", "https"),
                    ],
                    dropped_attributes_count: 0,
                    flags: 1,
                    trace_id: (i + variant).to_be_bytes().repeat(2),
                    span_id: (i * 7 + variant).to_be_bytes().to_vec(),
                    event_name: String::new(),
                })
                .collect();
            let request = pb::collector::logs::v1::ExportLogsServiceRequest {
                resource_logs: vec![pb::logs::v1::ResourceLogs {
                    resource: Some(pb::resource::v1::Resource {
                        attributes: vec![
                            kv("service.name", "orders-api"),
                            kv("service.version", "4.12.0"),
                            kv("host.name", &format!("orders-api-{variant}")),
                            kv("k8s.namespace.name", "shop"),
                        ],
                        dropped_attributes_count: 0,
                    }),
                    scope_logs: vec![pb::logs::v1::ScopeLogs {
                        scope: None,
                        log_records,
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }],
            };
            Bytes::from(request.encode_to_vec())
        })
        .collect()
}

// ===========================================================================
// Delivery gate
// ===========================================================================

/// Pass only when every listener took records and the Kafka sink delivered.
///
/// A profile collected against a listener that refused everything, or a sink
/// that never reached the broker, optimises the error path instead of ingest.
async fn delivery_gate(cfg: &Config, stats: &Stats) -> Result<(), String> {
    let idle: Vec<&str> = stats
        .all()
        .iter()
        .filter(|c| c.ok.load(Ordering::Relaxed) == 0)
        .map(|c| c.name)
        .collect();
    if !idle.is_empty() {
        return Err(format!("no successful sends on: {}", idle.join(", ")));
    }

    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");
    let mut metrics = String::new();
    let mut last_delivered = None;
    let settle_until = Instant::now() + SETTLE_LIMIT;
    while Instant::now() < settle_until {
        metrics = match client.get(&cfg.metrics_url).send().await {
            Ok(resp) => resp.text().await.map_err(|e| e.to_string())?,
            Err(e) => return Err(format!("scrape {}: {e}", cfg.metrics_url)),
        };
        // Counters are whole numbers, so the integer compare is exact.
        let delivered = metric_sum(&metrics, "receiver_kafka_delivered_total", None) as u64;
        if last_delivered == Some(delivered) {
            break;
        }
        last_delivered = Some(delivered);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let delivered = metric_sum(&metrics, "receiver_kafka_delivered_total", None);
    let delivery_failures = metric_sum(&metrics, "receiver_kafka_delivery_failures_total", None);
    println!(
        "pgo-driver gate: receiver_kafka_delivered_total={delivered} \
         receiver_kafka_delivery_failures_total={delivery_failures}"
    );
    let mut failures = Vec::new();
    if delivered <= 0.0 {
        failures.push("receiver_kafka_delivered_total is 0".to_string());
    }
    for transport in GATED_TRANSPORTS {
        let label = ("transport", transport);
        let success = metric_sum(&metrics, "receiver_requests_success_total", Some(label));
        let errors = metric_sum(&metrics, "receiver_requests_error_total", Some(label));
        println!(
            "pgo-driver gate: transport={transport} requests_success={success} requests_error={errors}"
        );
        if success <= 0.0 {
            failures.push(format!("no successful {transport} requests"));
        }
    }
    for transport in ["netflow", "sflow"] {
        let emitted = metric_sum(
            &metrics,
            "flow_records_emitted_total",
            Some(("transport", transport)),
        );
        println!("pgo-driver gate: transport={transport} flow_records_emitted={emitted}");
        if emitted <= 0.0 {
            failures.push(format!("no {transport} records emitted"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

/// Sum every sample of `name` in a Prometheus text exposition, optionally only
/// those carrying `label`.
fn metric_sum(text: &str, name: &str, label: Option<(&str, &str)>) -> f64 {
    let wanted = label.map(|(k, v)| format!("{k}=\"{v}\""));
    text.lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (series, value) = line.rsplit_once(' ')?;
            let (series_name, labels) = series.split_once('{').unwrap_or((series, ""));
            if series_name != name {
                return None;
            }
            if let Some(wanted) = &wanted
                && !labels.contains(wanted.as_str())
            {
                return None;
            }
            value.parse::<f64>().ok()
        })
        .fold(0.0, |total, value| total + value)
}
