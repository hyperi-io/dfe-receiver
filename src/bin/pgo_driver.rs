// Project:   dfe-receiver
// File:      src/bin/pgo-driver.rs
// Purpose:   PGO/profiling workload driver — exercises receiver hot paths
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! PGO workload driver for dfe-receiver.
//!
//! Drives realistic traffic against a running dfe-receiver instance so that
//! a PGO-instrumented binary accumulates representative profile data.
//! Intended to be invoked by `scripts/pgo-workload.sh` which handles the
//! receiver + testcontainers Kafka lifecycle.
//!
//! Builds only when the `pgo-driver` feature is enabled:
//! `cargo build --release --features pgo-driver --bin pgo-driver`
//!
//! Configuration via environment variables:
//! - `PGO_DRIVER_DURATION_SECS` (default 300) — total runtime
//! - `PGO_DRIVER_HTTP_URL` (default `http://127.0.0.1:8080/`)
//! - `PGO_DRIVER_PROM_RW_URL` (default `http://127.0.0.1:9091/api/v1/write`)
//! - `PGO_DRIVER_HEC_URL` (default `http://127.0.0.1:8088/services/collector/event`)
//! - `PGO_DRIVER_OTLP_HTTP_URL` (default `http://127.0.0.1:4318/v1/logs`)
//! - `PGO_DRIVER_SYSLOG_UDP` (default `127.0.0.1:514`)
//! - `PGO_DRIVER_SYSLOG_TCP` (default `127.0.0.1:514`)
//! - `PGO_DRIVER_NETFLOW_ADDR` (default `127.0.0.1:2055`)
//! - `PGO_DRIVER_SFLOW_ADDR` (default `127.0.0.1:6343`)
//! - `PGO_DRIVER_HTTP_RPS` (default 500)
//! - `PGO_DRIVER_OTHER_RPS` (default 100) — each non-HTTP protocol
//!
//! Traffic mix (per-second, defaults):
//! - HTTP JSON POST:         500 rps (60%)
//! - Prometheus RW:          100 rps (~12%)
//! - Splunk HEC event:       100 rps (~12%)
//! - OTLP HTTP logs:         100 rps (~12%)
//! - Syslog UDP (RFC 5424):   50 rps (~6%)
//! - Syslog TCP (framed):     50 rps (~6%)
//! - NetFlow v5 UDP:          ~100 rps (sub-ms send loop)
//! - sFlow v5 UDP:            ~100 rps (sub-ms send loop)
//!
//! Exit codes:
//! - 0: workload completed for full duration
//! - 1: fatal setup error
//! - 2: receiver unreachable (retries exhausted)

#![allow(clippy::expect_used)] // This is a workload driver, not library code.

use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use prost::Message;
use reqwest::Client;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UdpSocket};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let cfg = Config::from_env();
    println!("pgo-driver starting: {cfg:#?}");

    // Wait up to 30s for the receiver HTTP endpoint to come up
    if !wait_for_receiver(&cfg.http_url).await {
        eprintln!("pgo-driver: receiver unreachable at {}", cfg.http_url);
        std::process::exit(2);
    }

    let stats = Arc::new(Stats::new());
    let mut tasks = JoinSet::new();
    let deadline = Instant::now() + Duration::from_secs(cfg.duration_secs);

    // Launch protocol drivers
    tasks.spawn(drive_http(cfg.clone(), stats.clone(), deadline));
    tasks.spawn(drive_prom_rw(cfg.clone(), stats.clone(), deadline));
    tasks.spawn(drive_hec(cfg.clone(), stats.clone(), deadline));
    tasks.spawn(drive_otlp_http(cfg.clone(), stats.clone(), deadline));
    tasks.spawn(drive_syslog_udp(cfg.clone(), stats.clone(), deadline));
    tasks.spawn(drive_syslog_tcp(cfg.clone(), stats.clone(), deadline));
    tasks.spawn(drive_netflow_v5(cfg.clone(), stats.clone(), deadline));
    tasks.spawn(drive_sflow_v5(cfg.clone(), stats.clone(), deadline));

    // Progress reporter
    let reporter_stats = stats.clone();
    tasks.spawn(async move {
        let mut tick = interval(Duration::from_secs(15));
        tick.tick().await; // skip immediate
        while Instant::now() < deadline {
            tick.tick().await;
            reporter_stats.report();
        }
    });

    // Wait for all tasks
    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            eprintln!("pgo-driver task error: {e}");
        }
    }

    stats.report();
    println!("pgo-driver: complete");
}

async fn wait_for_receiver(url: &str) -> bool {
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("reqwest client");
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if client.post(url).body("{}").send().await.is_ok() {
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
    prom_rw_url: String,
    hec_url: String,
    otlp_http_url: String,
    syslog_udp_addr: String,
    syslog_tcp_addr: String,
    netflow_addr: String,
    sflow_addr: String,
    http_rps: u32,
    other_rps: u32,
}

impl Config {
    fn from_env() -> Self {
        Self {
            duration_secs: env_u64("PGO_DRIVER_DURATION_SECS", 300),
            http_url: env_str("PGO_DRIVER_HTTP_URL", "http://127.0.0.1:8080/"),
            prom_rw_url: env_str(
                "PGO_DRIVER_PROM_RW_URL",
                "http://127.0.0.1:9091/api/v1/write",
            ),
            hec_url: env_str(
                "PGO_DRIVER_HEC_URL",
                "http://127.0.0.1:8088/services/collector/event",
            ),
            otlp_http_url: env_str("PGO_DRIVER_OTLP_HTTP_URL", "http://127.0.0.1:4318/v1/logs"),
            syslog_udp_addr: env_str("PGO_DRIVER_SYSLOG_UDP", "127.0.0.1:514"),
            syslog_tcp_addr: env_str("PGO_DRIVER_SYSLOG_TCP", "127.0.0.1:514"),
            netflow_addr: env_str("PGO_DRIVER_NETFLOW_ADDR", "127.0.0.1:2055"),
            sflow_addr: env_str("PGO_DRIVER_SFLOW_ADDR", "127.0.0.1:6343"),
            http_rps: env_u32("PGO_DRIVER_HTTP_RPS", 500),
            other_rps: env_u32("PGO_DRIVER_OTHER_RPS", 100),
        }
    }
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}
fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_u32(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ===========================================================================
// Stats
// ===========================================================================

struct Stats {
    http: AtomicU64,
    prom_rw: AtomicU64,
    hec: AtomicU64,
    otlp_http: AtomicU64,
    syslog_udp: AtomicU64,
    syslog_tcp: AtomicU64,
    netflow: AtomicU64,
    sflow: AtomicU64,
    errors: AtomicU64,
    start: Instant,
}

impl Stats {
    fn new() -> Self {
        Self {
            http: AtomicU64::new(0),
            prom_rw: AtomicU64::new(0),
            hec: AtomicU64::new(0),
            otlp_http: AtomicU64::new(0),
            syslog_udp: AtomicU64::new(0),
            syslog_tcp: AtomicU64::new(0),
            netflow: AtomicU64::new(0),
            sflow: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            start: Instant::now(),
        }
    }

    fn report(&self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let http = self.http.load(Ordering::Relaxed);
        let prom = self.prom_rw.load(Ordering::Relaxed);
        let hec = self.hec.load(Ordering::Relaxed);
        let otlp = self.otlp_http.load(Ordering::Relaxed);
        let s_udp = self.syslog_udp.load(Ordering::Relaxed);
        let s_tcp = self.syslog_tcp.load(Ordering::Relaxed);
        let nf = self.netflow.load(Ordering::Relaxed);
        let sf = self.sflow.load(Ordering::Relaxed);
        let errs = self.errors.load(Ordering::Relaxed);
        let total = http + prom + hec + otlp + s_udp + s_tcp + nf + sf;
        println!(
            "pgo-driver [{elapsed:>6.1}s] total={total:>8} \
             http={http} prom={prom} hec={hec} otlp={otlp} \
             syslog_udp={s_udp} syslog_tcp={s_tcp} \
             netflow={nf} sflow={sf} errors={errs} \
             rate={:.0}/s",
            (total as f64) / elapsed.max(1.0)
        );
    }
}

// ===========================================================================
// Protocol drivers
// ===========================================================================

/// Per-RPS rate limiter using a tokio interval. Returns a next-tick future.
fn rate_limiter(rps: u32) -> tokio::time::Interval {
    let period = Duration::from_nanos(1_000_000_000 / u64::from(rps.max(1)));
    let mut iv = interval(period);
    iv.set_missed_tick_behavior(MissedTickBehavior::Delay);
    iv
}

async fn drive_http(cfg: Config, stats: Arc<Stats>, deadline: Instant) {
    let client = Client::builder()
        .pool_max_idle_per_host(16)
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");
    let mut tick = rate_limiter(cfg.http_rps);
    let bodies = http_payloads();
    let mut idx = 0usize;
    while Instant::now() < deadline {
        tick.tick().await;
        let body = &bodies[idx % bodies.len()];
        idx = idx.wrapping_add(1);
        match client
            .post(&cfg.http_url)
            .header("content-type", "application/json")
            .body(body.clone())
            .send()
            .await
        {
            Ok(_) => {
                stats.http.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

async fn drive_hec(cfg: Config, stats: Arc<Stats>, deadline: Instant) {
    let client = Client::builder()
        .pool_max_idle_per_host(8)
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");
    let mut tick = rate_limiter(cfg.other_rps);
    while Instant::now() < deadline {
        tick.tick().await;
        let body = serde_json::json!({
            "event": {
                "message": "auth success",
                "user": "svc-account-01",
                "ip": "10.2.3.4"
            },
            "source": "pgo-driver",
            "sourcetype": "pgo:auth",
        });
        if client
            .post(&cfg.hec_url)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .is_ok()
        {
            stats.hec.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn drive_prom_rw(cfg: Config, stats: Arc<Stats>, deadline: Instant) {
    use dfe_receiver::server::prometheus_rw::proto;
    let client = Client::builder()
        .pool_max_idle_per_host(8)
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");
    let mut tick = rate_limiter(cfg.other_rps);

    while Instant::now() < deadline {
        tick.tick().await;
        let ts = chrono::Utc::now().timestamp_millis();
        let req = proto::WriteRequest {
            timeseries: vec![
                proto::TimeSeries {
                    labels: vec![
                        proto::Label {
                            name: "__name__".into(),
                            value: "cpu_usage_percent".into(),
                        },
                        proto::Label {
                            name: "host".into(),
                            value: format!("h-{}", ts % 100),
                        },
                    ],
                    samples: vec![proto::Sample {
                        value: 42.5 + ((ts % 10) as f64),
                        timestamp: ts,
                    }],
                    exemplars: vec![],
                    histograms: vec![],
                },
                proto::TimeSeries {
                    labels: vec![proto::Label {
                        name: "__name__".into(),
                        value: "mem_used_bytes".into(),
                    }],
                    samples: vec![proto::Sample {
                        value: 1_073_741_824.0,
                        timestamp: ts,
                    }],
                    exemplars: vec![],
                    histograms: vec![],
                },
            ],
            metadata: vec![],
        };
        let encoded = req.encode_to_vec();
        let Ok(compressed) = snap::raw::Encoder::new().compress_vec(&encoded) else {
            stats.errors.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        if client
            .post(&cfg.prom_rw_url)
            .header("content-type", "application/x-protobuf")
            .header("content-encoding", "snappy")
            .header("x-prometheus-remote-write-version", "0.1.0")
            .body(compressed)
            .send()
            .await
            .is_ok()
        {
            stats.prom_rw.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn drive_otlp_http(cfg: Config, stats: Arc<Stats>, deadline: Instant) {
    // OTLP HTTP accepts protobuf bodies at /v1/logs. Construct a minimal
    // valid ExportLogsServiceRequest using the vendored OTLP protos.
    use dfe_receiver::server::otlp::pb;
    let client = Client::builder()
        .pool_max_idle_per_host(8)
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");
    let mut tick = rate_limiter(cfg.other_rps);

    while Instant::now() < deadline {
        tick.tick().await;
        let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let request = pb::collector::logs::v1::ExportLogsServiceRequest {
            resource_logs: vec![pb::logs::v1::ResourceLogs {
                resource: Some(pb::resource::v1::Resource {
                    attributes: vec![pb::common::v1::KeyValue {
                        key: "service.name".into(),
                        value: Some(pb::common::v1::AnyValue {
                            value: Some(pb::common::v1::any_value::Value::StringValue(
                                "pgo-driver".into(),
                            )),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![pb::logs::v1::ScopeLogs {
                    scope: None,
                    log_records: vec![pb::logs::v1::LogRecord {
                        time_unix_nano: ts,
                        observed_time_unix_nano: ts,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(pb::common::v1::AnyValue {
                            value: Some(pb::common::v1::any_value::Value::StringValue(
                                "pgo workload log record".into(),
                            )),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0u8; 16],
                        span_id: vec![0u8; 8],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let body = Bytes::from(request.encode_to_vec());
        if client
            .post(&cfg.otlp_http_url)
            .header("content-type", "application/x-protobuf")
            .body(body)
            .send()
            .await
            .is_ok()
        {
            stats.otlp_http.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn drive_syslog_udp(cfg: Config, stats: Arc<Stats>, deadline: Instant) {
    let sock = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("syslog_udp bind: {e}");
            stats.errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let mut tick = rate_limiter(cfg.other_rps / 2); // half rate each
    while Instant::now() < deadline {
        tick.tick().await;
        let msg = format!(
            "<134>1 2026-04-17T12:34:56Z host01 pgo-driver 1234 - - workload syslog UDP entry seq={}",
            stats.syslog_udp.load(Ordering::Relaxed)
        );
        if sock
            .send_to(msg.as_bytes(), &cfg.syslog_udp_addr)
            .await
            .is_ok()
        {
            stats.syslog_udp.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn drive_syslog_tcp(cfg: Config, stats: Arc<Stats>, deadline: Instant) {
    let mut tick = rate_limiter(cfg.other_rps / 2);
    while Instant::now() < deadline {
        tick.tick().await;
        let Ok(mut stream) = TcpStream::connect(&cfg.syslog_tcp_addr).await else {
            stats.errors.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        let msg = format!(
            "<134>1 2026-04-17T12:34:56Z host01 pgo-driver 1234 - - workload syslog TCP entry seq={}",
            stats.syslog_tcp.load(Ordering::Relaxed)
        );
        // RFC 6587 octet-counted framing: "<length> <message>"
        let frame = format!("{} {}", msg.len(), msg);
        if stream.write_all(frame.as_bytes()).await.is_ok() {
            stats.syslog_tcp.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.errors.fetch_add(1, Ordering::Relaxed);
        }
        let _ = stream.shutdown().await;
    }
}

async fn drive_netflow_v5(cfg: Config, stats: Arc<Stats>, deadline: Instant) {
    let sock = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("netflow_v5 bind: {e}");
            stats.errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let pkt = build_netflow_v5_packet();
    // Tight loop -- UDP send is sub-ms; sleep ~200us between sends to land
    // around 5000 pps theoretical but rate-limited by the OS scheduler.
    while Instant::now() < deadline {
        match sock.send_to(&pkt, &cfg.netflow_addr).await {
            Ok(_) => {
                stats.netflow.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
}

async fn drive_sflow_v5(cfg: Config, stats: Arc<Stats>, deadline: Instant) {
    let sock = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("sflow_v5 bind: {e}");
            stats.errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let pkt = build_sflow_v5_packet();
    while Instant::now() < deadline {
        match sock.send_to(&pkt, &cfg.sflow_addr).await {
            Ok(_) => {
                stats.sflow.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
}

/// Build a valid NetFlow v5 datagram (header + one record). Same layout as
/// the unit-test fixture in `src/server/netflow/tests/decode_v5.rs`.
fn build_netflow_v5_packet() -> Vec<u8> {
    let mut pkt = vec![0u8; 72];
    pkt[0..2].copy_from_slice(&5u16.to_be_bytes());
    pkt[2..4].copy_from_slice(&1u16.to_be_bytes());
    pkt[4..8].copy_from_slice(&1000u32.to_be_bytes());
    pkt[8..12].copy_from_slice(&1_700_000_000u32.to_be_bytes());
    pkt[12..16].copy_from_slice(&0u32.to_be_bytes());
    pkt[16..20].copy_from_slice(&42u32.to_be_bytes());
    pkt[24..28].copy_from_slice(&[10, 0, 0, 1]);
    pkt[28..32].copy_from_slice(&[10, 0, 0, 2]);
    pkt[36..38].copy_from_slice(&7u16.to_be_bytes());
    pkt[38..40].copy_from_slice(&9u16.to_be_bytes());
    pkt[40..44].copy_from_slice(&5u32.to_be_bytes());
    pkt[44..48].copy_from_slice(&1500u32.to_be_bytes());
    pkt[48..52].copy_from_slice(&500u32.to_be_bytes());
    pkt[52..56].copy_from_slice(&800u32.to_be_bytes());
    pkt[56..58].copy_from_slice(&12345u16.to_be_bytes());
    pkt[58..60].copy_from_slice(&80u16.to_be_bytes());
    pkt[61] = 0x18;
    pkt[62] = 6;
    pkt[64..66].copy_from_slice(&64500u16.to_be_bytes());
    pkt[66..68].copy_from_slice(&64501u16.to_be_bytes());
    pkt[68] = 24;
    pkt[69] = 24;
    pkt
}

/// Build a minimal sFlow v5 datagram with zero samples. The PGO target is to
/// exercise UDP recv + autosense dispatch, not the per-record decode path; a
/// zero-sample datagram is sufficient and keeps generator cost low.
fn build_sflow_v5_packet() -> Vec<u8> {
    let mut pkt = Vec::with_capacity(28);
    pkt.extend_from_slice(&5u32.to_be_bytes()); // version
    pkt.extend_from_slice(&1u32.to_be_bytes()); // agent_address_type = IPv4
    pkt.extend_from_slice(&[10, 0, 0, 1]); // agent ip
    pkt.extend_from_slice(&0u32.to_be_bytes()); // sub_agent_id
    pkt.extend_from_slice(&100u32.to_be_bytes()); // sequence
    pkt.extend_from_slice(&3600u32.to_be_bytes()); // uptime
    pkt.extend_from_slice(&0u32.to_be_bytes()); // num_samples = 0
    pkt
}

// ===========================================================================
// Payload corpora — realistic mix of sizes and shapes
// ===========================================================================

fn http_payloads() -> Vec<Bytes> {
    vec![
        // Small: simple event
        Bytes::from(
            br#"{"src":"pgo","msg":"user login","user":"alice"}"#.as_slice(),
        ),
        // Medium: nested structure
        Bytes::from(
            br#"{"timestamp":"2026-04-17T12:34:56Z","level":"info","service":"auth","request_id":"abc-123","user_id":42,"action":"login","metadata":{"ip":"10.0.0.1","user_agent":"Mozilla/5.0","session_id":"sess-xyz"}}"#.as_slice(),
        ),
        // Large: 4 KiB representative event with arrays
        Bytes::from(large_payload()),
    ]
}

fn large_payload() -> Vec<u8> {
    let mut buf =
        br#"{"timestamp":"2026-04-17T12:34:56Z","level":"info","service":"api","events":["#
            .to_vec();
    for i in 0..100 {
        if i > 0 {
            buf.push(b',');
        }
        buf.extend_from_slice(
            format!(
                r#"{{"id":{i},"type":"http.request","path":"/api/v1/users/{i}","status":200,"duration_ms":12}}"#
            )
            .as_bytes(),
        );
    }
    buf.extend_from_slice(br#"]}"#);
    buf
}
