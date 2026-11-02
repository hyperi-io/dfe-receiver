// Project:   dfe-receiver
// File:      benches/syslog_allocs.rs
// Purpose:   Heap allocations per syslog message, framing and conversion
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prints allocations per message. A separate binary from `benches/syslog.rs`
//! because the counting allocator would slow every timed run beside it.

// Counts far below 2^52 become per-message averages.
#![allow(clippy::cast_precision_loss)]

mod common;

use bytes::BytesMut;
use dfe_receiver::config::RawCapture;
use dfe_receiver::server::syslog::convert::syslog_to_json;
use dfe_receiver::server::syslog::framing::SyslogFrameDecoder;
use tokio_util::codec::Decoder;

use common::{CORPUS, lf_stream, octet_stream};

const ROUNDS: u64 = 1_000;

fn per_message(f: impl Fn()) -> (f64, f64) {
    let info = allocation_counter::measure(|| {
        for _ in 0..ROUNDS {
            f();
        }
    });
    (
        info.count_total as f64 / ROUNDS as f64,
        info.bytes_total as f64 / ROUNDS as f64,
    )
}

fn frames_per_stream(stream: &[u8]) -> (f64, f64) {
    let mut frames = 0u64;
    let mut buf = BytesMut::from(stream);
    let info = allocation_counter::measure(|| {
        let mut decoder = SyslogFrameDecoder::default();
        while let Ok(Some(frame)) = decoder.decode(&mut buf) {
            frames += u64::from(!frame.is_empty());
        }
    });
    let frames = frames.max(1) as f64;
    (
        info.count_total as f64 / frames,
        info.bytes_total as f64 / frames,
    )
}

fn main() {
    println!("{:<28} {:>12} {:>12}", "case", "allocs/msg", "bytes/msg");
    for (name, line) in CORPUS {
        let (allocs, bytes) = per_message(|| {
            let _ = std::hint::black_box(syslog_to_json(line, RawCapture::OFF));
        });
        println!(
            "{:<28} {allocs:>12.2} {bytes:>12.1}",
            format!("convert/{name}")
        );
    }
    let (allocs, bytes) = per_message(|| {
        for (_, line) in CORPUS {
            let _ = std::hint::black_box(syslog_to_json(line, RawCapture::OFF));
        }
    });
    let n = CORPUS.len() as f64;
    println!(
        "{:<28} {:>12.2} {:>12.1}",
        "convert/mix",
        allocs / n,
        bytes / n
    );
    for (name, stream) in [("lf", lf_stream(100)), ("octet", octet_stream(100))] {
        let (allocs, bytes) = frames_per_stream(&stream);
        println!(
            "{:<28} {allocs:>12.2} {bytes:>12.1}",
            format!("framing/{name}")
        );
    }
}
