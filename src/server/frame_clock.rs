// Project:   dfe-receiver
// File:      src/server/frame_clock.rs
// Purpose:   Close a raw TCP connection whose frame stops arriving part way
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The incomplete-frame deadline of the raw TCP listeners: syslog TCP and TLS,
//! GELF, Fluent Forward and Lumberjack.
//!
//! Once a frame's first byte arrives the rest of it has
//! `server.connections.frame_timeout_ms` to follow, or the connection closes and
//! frees its place under the listener's cap. The clock runs only while the
//! listener waits on the socket with part of a frame in hand, so a connection
//! idle between whole frames is kept, and a hold on the pipeline is not counted.
//!
//! [`Clocked`] wraps the socket and starts the clock on a frame's first byte. A
//! listener that buffers bytes above it says where its frames end, through
//! [`FrameClock`] or through [`ClockedDecoder`] around its framing codec.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Instant, Sleep};
use tokio_util::codec::Decoder;

use crate::metrics::Metrics;

/// No word from the listener since the socket last looked.
const HELD_UNKNOWN: u8 = 0;
/// The listener holds no part of a frame.
const HELD_NOTHING: u8 = 1;
/// The listener holds the start of a frame not yet whole.
const HELD_PART: u8 = 2;

/// What the listener last said about its frames. It and its socket run on one
/// task, so relaxed ordering is enough.
#[derive(Default)]
struct Signals {
    ended: AtomicBool,
    held: AtomicU8,
}

/// A listener's word to its [`Clocked`] socket on where its frames end.
#[derive(Clone)]
pub(crate) struct FrameClock(Arc<Signals>);

impl FrameClock {
    /// A frame ended, so the next one has a deadline of its own.
    pub(crate) fn frame_ended(&self) {
        self.0.ended.store(true, Ordering::Relaxed);
    }

    /// Whether the listener's own buffer holds the start of a frame not yet whole.
    pub(crate) fn holding(&self, part: bool) {
        let held = if part { HELD_PART } else { HELD_NOTHING };
        self.0.held.store(held, Ordering::Relaxed);
    }
}

/// A raw TCP listener's socket, failing a read with [`io::ErrorKind::TimedOut`]
/// once a frame has taken longer than its deadline.
pub(crate) struct Clocked<S> {
    inner: S,
    signals: Arc<Signals>,
    limit: Duration,
    deadline: Pin<Box<Sleep>>,
    armed: bool,
    metrics: Arc<Metrics>,
    transport: &'static str,
}

impl<S> Clocked<S> {
    /// `inner`, whose frames each have `limit` to arrive whole, counting a
    /// close on `metrics` under `transport`.
    pub(crate) fn new(
        inner: S,
        limit: Duration,
        metrics: Arc<Metrics>,
        transport: &'static str,
    ) -> Self {
        Self {
            inner,
            signals: Arc::default(),
            limit,
            deadline: Box::pin(tokio::time::sleep(Duration::ZERO)),
            armed: false,
            metrics,
            transport,
        }
    }

    /// How a listener that buffers bytes itself says where its frames end.
    pub(crate) fn clock(&self) -> FrameClock {
        FrameClock(Arc::clone(&self.signals))
    }

    /// `codec`, telling this socket where each frame it decodes ends.
    pub(crate) fn decoder<D>(&self, codec: D) -> ClockedDecoder<D> {
        ClockedDecoder {
            inner: codec,
            clock: self.clock(),
        }
    }

    /// The wrapped socket, for a listener's writes.
    pub(crate) fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    fn arm(&mut self) {
        self.deadline.as_mut().reset(Instant::now() + self.limit);
        self.armed = true;
    }

    /// Apply what the listener said since the last read.
    fn take_signals(&mut self) {
        if self.signals.ended.swap(false, Ordering::Relaxed) {
            self.armed = false;
        }
        match self.signals.held.swap(HELD_UNKNOWN, Ordering::Relaxed) {
            HELD_NOTHING => self.armed = false,
            HELD_PART if !self.armed => self.arm(),
            _ => {}
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Clocked<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        this.take_signals();
        let before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if buf.filled().len() > before && !this.armed {
                    this.arm();
                }
                Poll::Ready(Ok(()))
            }
            Poll::Pending if this.armed && this.deadline.as_mut().poll(cx).is_ready() => {
                this.armed = false;
                this.metrics
                    .inc_connection_closed(this.transport, "frame_timeout");
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "a frame did not arrive whole within the frame timeout",
                )))
            }
            other => other,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Clocked<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A framing codec that tells its [`Clocked`] socket where each frame ends.
pub(crate) struct ClockedDecoder<D> {
    inner: D,
    clock: FrameClock,
}

impl<D: Decoder> Decoder for ClockedDecoder<D> {
    type Item = D::Item;
    type Error = D::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let frame = self.inner.decode(src)?;
        if frame.is_some() {
            self.clock.frame_ended();
        } else {
            self.clock.holding(!src.is_empty());
        }
        Ok(frame)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode_eof(src)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use tokio::io::AsyncWriteExt;
    use tokio_stream::StreamExt;
    use tokio_util::codec::FramedRead;

    use super::*;
    use crate::server::syslog::framing::SyslogFrameDecoder;

    const LIMIT: Duration = Duration::from_millis(500);

    /// What the client does next: send bytes, or wait.
    enum Step {
        Send(&'static [u8]),
        Wait(Duration),
    }

    /// The frames a listener read, and whether one timed out.
    #[derive(Debug, PartialEq)]
    struct Read {
        frames: Vec<String>,
        timed_out: bool,
    }

    /// Run `steps` against a syslog-framed listener that holds each frame for
    /// `hold` before it reads on, until a frame times out or the client closes.
    async fn run(steps: Vec<Step>, hold: Duration) -> Read {
        let (mut client, server) = tokio::io::duplex(1024);
        let socket = Clocked::new(server, LIMIT, Arc::new(Metrics::default()), "test");
        let decoder = socket.decoder(SyslogFrameDecoder::default());
        let mut framed = FramedRead::new(socket, decoder);
        let mut read = Read {
            frames: Vec::new(),
            timed_out: false,
        };
        let sender = tokio::spawn(async move {
            for step in steps {
                match step {
                    Step::Send(bytes) => client.write_all(bytes).await.unwrap(),
                    Step::Wait(wait) => tokio::time::sleep(wait).await,
                }
            }
        });
        let listen = async {
            while let Some(frame) = framed.next().await {
                let Ok(frame) = frame else {
                    read.timed_out = true;
                    return;
                };
                read.frames
                    .push(String::from_utf8_lossy(&frame).into_owned());
                tokio::time::sleep(hold).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(20), listen)
            .await
            .unwrap();
        sender.abort();
        read
    }

    /// A frame that stops part way, or trickles in slower than the limit, times
    /// out. Waiting between whole frames, after a stray newline the codec
    /// skips, or while the frame before the next one is held does not count.
    #[tokio::test]
    async fn only_a_frame_in_progress_runs_the_clock() {
        let quiet = LIMIT * 3;
        let trickle = (0..12)
            .flat_map(|_| [Step::Send(b"x"), Step::Wait(LIMIT / 4)])
            .collect();
        let read = |frames: &[&str], timed_out: bool| Read {
            frames: frames.iter().map(ToString::to_string).collect(),
            timed_out,
        };
        let cases: [(&str, Vec<Step>, Duration, Read); 5] = [
            (
                "a frame that stops part way",
                vec![Step::Send(b"a\npar"), Step::Wait(quiet)],
                Duration::ZERO,
                read(&["a"], true),
            ),
            (
                "a frame trickled in slower than the limit",
                trickle,
                Duration::ZERO,
                read(&[], true),
            ),
            (
                "idle between whole frames",
                vec![Step::Send(b"a\n"), Step::Wait(quiet), Step::Send(b"b\n")],
                Duration::ZERO,
                read(&["a", "b"], false),
            ),
            (
                "idle after a stray newline",
                vec![
                    Step::Send(b"a\n"),
                    Step::Wait(quiet),
                    Step::Send(b"\n"),
                    Step::Wait(quiet),
                    Step::Send(b"b\n"),
                ],
                Duration::ZERO,
                read(&["a", "b"], false),
            ),
            (
                "the next frame begun behind a held one",
                vec![
                    Step::Send(b"a\nb"),
                    Step::Wait(quiet + LIMIT / 5),
                    Step::Send(b"\n"),
                ],
                quiet,
                read(&["a", "b"], false),
            ),
        ];
        let mut failed = Vec::new();
        for (name, steps, hold, want) in cases {
            let got = run(steps, hold).await;
            if got != want {
                failed.push(format!("{name}: got {got:?}, want {want:?}"));
            }
        }
        assert!(failed.is_empty(), "{}", failed.join("\n"));
    }
}
