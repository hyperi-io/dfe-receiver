// Project:   dfe-receiver
// File:      benches/syslog.rs
// Purpose:   Syslog framing and conversion throughput
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

mod common;

use std::hint::black_box;

use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use dfe_receiver::config::RawCapture;
use dfe_receiver::server::syslog::convert::syslog_to_json;
use dfe_receiver::server::syslog::framing::SyslogFrameDecoder;
use tokio_util::codec::Decoder;

use common::{CORPUS, lf_stream, octet_stream};

/// Stream repeats, so a framing run decodes a few thousand frames.
const STREAM_REPEATS: usize = 500;

fn convert(c: &mut Criterion) {
    let mut group = c.benchmark_group("syslog_convert");
    for (name, line) in CORPUS {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(name), line, |b, line| {
            b.iter(|| syslog_to_json(black_box(line), RawCapture::OFF));
        });
    }
    group.throughput(Throughput::Elements(CORPUS.len() as u64));
    group.bench_function("mix", |b| {
        b.iter(|| {
            for (_, line) in CORPUS {
                let _ = black_box(syslog_to_json(black_box(line), RawCapture::OFF));
            }
        });
    });
    group.finish();
}

fn framing(c: &mut Criterion) {
    let mut group = c.benchmark_group("syslog_framing");
    let frames = (CORPUS.len() * STREAM_REPEATS) as u64;
    for (name, stream) in [
        ("lf", lf_stream(STREAM_REPEATS)),
        ("octet", octet_stream(STREAM_REPEATS)),
    ] {
        group.throughput(Throughput::Elements(frames));
        group.bench_with_input(BenchmarkId::from_parameter(name), &stream, |b, stream| {
            b.iter(|| {
                let mut decoder = SyslogFrameDecoder::default();
                let mut buf = BytesMut::from(&stream[..]);
                let mut n = 0usize;
                while let Ok(Some(frame)) = decoder.decode(&mut buf) {
                    n += frame.len();
                }
                black_box(n)
            });
        });
    }
    group.finish();
}

criterion_group!(benches, convert, framing);
criterion_main!(benches);
