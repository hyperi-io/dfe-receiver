// Project:   dfe-receiver
// File:      src/server/traits.rs
// Purpose:   Protocol handler trait for pluggable ingestion protocols
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Protocol handler trait for pluggable ingestion protocols.
//!
//! All ingestion protocols (HTTP/JSON, gRPC/Vector, OTLP, Prometheus Remote
//! Write, etc.) implement [`ProtocolHandler`]. The server orchestration layer
//! starts all enabled handlers in parallel and monitors their health.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::SetOnce;
use tokio::task::{Id, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

/// The address one listener bound, set the moment its bind succeeds, and
/// whether that listener is still serving.
///
/// A handler binds inside [`ProtocolHandler::start`], which runs until
/// shutdown, so a caller that configured port 0 learns the port the OS
/// assigned by waiting on this. Readiness reads
/// [`is_serving`](Self::is_serving). Clones share one cell.
#[derive(Clone, Debug, Default)]
pub struct BoundAddr(Arc<ListenerState>);

#[derive(Debug, Default)]
struct ListenerState {
    addr: SetOnce<SocketAddr>,
    serving: AtomicBool,
}

impl BoundAddr {
    /// Record the bound socket's `local_addr()` and mark the listener serving
    /// until the returned guard drops. A handler instance binds each listener
    /// once, so a second address is ignored.
    pub(crate) fn publish(&self, local: &std::io::Result<SocketAddr>) -> Serving {
        // A socket that cannot name its address leaves the cell empty, and the listener still serves.
        if let Ok(addr) = local {
            let _ = self.0.addr.set(*addr);
        }
        self.0.serving.store(true, Ordering::Relaxed);
        Serving(Arc::clone(&self.0))
    }

    /// Wait for the bind and return the address it took.
    ///
    /// Never resolves when the bind fails, so bound the wait with a timeout.
    pub async fn wait(&self) -> SocketAddr {
        *self.0.addr.wait().await
    }

    /// Whether the listener has bound and has not stopped since.
    #[must_use]
    pub fn is_serving(&self) -> bool {
        self.0.serving.load(Ordering::Relaxed)
    }
}

/// Keeps a published listener serving. Dropping it, however the listener
/// ends, marks the listener stopped.
#[must_use = "the listener reads as stopped the moment this guard drops"]
pub(crate) struct Serving(Arc<ListenerState>);

impl Drop for Serving {
    fn drop(&mut self) {
        self.0.serving.store(false, Ordering::Relaxed);
    }
}

/// Trait for pluggable protocol handlers.
///
/// Each protocol implements this trait. The server collects all enabled
/// handlers and spawns them concurrently. Handlers run until the
/// cancellation token is triggered, then return.
#[async_trait::async_trait]
pub trait ProtocolHandler: Send + Sync {
    /// Human-readable handler name (e.g. "http", "grpc-vector", "otlp-grpc").
    fn name(&self) -> &'static str;

    /// Address this handler listens on (for logging/health).
    fn bind_address(&self) -> &str;

    /// One cell per listener [`start`](Self::start) binds, which readiness
    /// waits on. A cell nothing publishes to holds the pod not ready for good.
    fn listeners(&self) -> Vec<BoundAddr>;

    /// Start the handler. Blocks until shutdown is signalled.
    ///
    /// Returns an error when a listener cannot bind, or stops before shutdown.
    async fn start(&self, shutdown: CancellationToken) -> Result<()>;

    /// Check if the handler is healthy and accepting traffic.
    fn is_healthy(&self) -> bool {
        true
    }
}

/// The listener tasks of one handler, run until shutdown as one unit.
///
/// A listener that fails, or stops before shutdown, fails the whole handler.
#[derive(Default)]
pub(crate) struct Listeners {
    tasks: JoinSet<Result<()>>,
    names: Vec<(Id, String)>,
}

impl Listeners {
    /// Spawn one listener, named in the error that reports it.
    pub(crate) fn spawn<F>(&mut self, name: impl Into<String>, listener: F)
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let id = self.tasks.spawn(listener).id();
        self.names.push((id, name.into()));
    }

    /// Run until `shutdown`, or until a listener stops before it.
    ///
    /// A listener that returns an error, or returns at all before shutdown,
    /// fails the handler with an error naming it, and the other listeners are
    /// aborted. After shutdown every listener is awaited, and the first error
    /// any of them returned is the handler's.
    pub(crate) async fn run(mut self, shutdown: &CancellationToken) -> Result<()> {
        if self.tasks.is_empty() {
            shutdown.cancelled().await;
            return Ok(());
        }

        let mut first_error = None;
        while let Some(joined) = self.tasks.join_next_with_id().await {
            let (id, failure) = match joined {
                Ok((_, Ok(()))) if shutdown.is_cancelled() => continue,
                Ok((id, Ok(()))) => (id, "stopped before shutdown".to_string()),
                Ok((id, Err(e))) => (id, e.to_string()),
                Err(e) => (e.id(), e.to_string()),
            };
            let error = Error::Server(format!("{} listener: {failure}", self.name_of(id)));
            if !shutdown.is_cancelled() {
                self.tasks.shutdown().await;
                return Err(error);
            }
            first_error.get_or_insert(error);
        }
        first_error.map_or(Ok(()), Err)
    }

    fn name_of(&self, id: Id) -> &str {
        self.names
            .iter()
            .find(|(task, _)| *task == id)
            .map_or("unnamed", |(_, name)| name.as_str())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// How long a supervised handler gets to report a stopped listener.
    const DEADLINE: Duration = Duration::from_secs(5);

    #[test]
    fn a_cell_serves_from_publish_until_its_guard_drops() {
        let bound = BoundAddr::default();
        assert!(
            !bound.is_serving(),
            "an unpublished listener is not serving"
        );

        let serving = bound.publish(&Ok(SocketAddr::from(([127, 0, 0, 1], 9))));
        assert!(bound.clone().is_serving(), "clones share the cell");

        drop(serving);
        assert!(
            !bound.is_serving(),
            "a dropped guard marks the listener stopped"
        );
    }

    #[test]
    fn a_listener_that_cannot_name_its_address_still_serves() {
        let bound = BoundAddr::default();
        let _serving = bound.publish(&Err(std::io::Error::other("no name")));
        assert!(bound.is_serving());
    }

    #[tokio::test]
    async fn a_failed_listener_fails_the_handler_and_stops_the_others() {
        let shutdown = CancellationToken::new();
        let sibling = BoundAddr::default();
        let mut listeners = Listeners::default();
        let held = sibling.clone();
        listeners.spawn("sibling", async move {
            let _serving = held.publish(&Err(std::io::Error::other("unnamed")));
            std::future::pending::<Result<()>>().await
        });
        listeners.spawn("broken", async {
            Err(Error::Server("bind refused".into()))
        });

        let outcome = tokio::time::timeout(DEADLINE, listeners.run(&shutdown))
            .await
            .unwrap();

        let message = outcome.unwrap_err().to_string();
        assert!(message.contains("broken listener"), "{message}");
        assert!(message.contains("bind refused"), "{message}");
        assert!(!sibling.is_serving(), "the sibling must be stopped");
    }

    #[tokio::test]
    async fn a_listener_that_returns_before_shutdown_fails_the_handler() {
        let shutdown = CancellationToken::new();
        let mut listeners = Listeners::default();
        listeners.spawn("early", async { Ok(()) });
        listeners.spawn("steady", std::future::pending());

        let outcome = tokio::time::timeout(DEADLINE, listeners.run(&shutdown))
            .await
            .unwrap();

        let message = outcome.unwrap_err().to_string();
        assert!(
            message.contains("early listener: stopped before shutdown"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn a_panicking_listener_fails_the_handler() {
        let shutdown = CancellationToken::new();
        let mut listeners = Listeners::default();
        listeners.spawn("panicky", async { panic!("listener blew up") });

        let outcome = tokio::time::timeout(DEADLINE, listeners.run(&shutdown))
            .await
            .unwrap();

        let message = outcome.unwrap_err().to_string();
        assert!(message.contains("panicky listener"), "{message}");
    }

    #[tokio::test]
    async fn listeners_that_stop_on_shutdown_return_ok() {
        let shutdown = CancellationToken::new();
        let mut listeners = Listeners::default();
        for name in ["one", "two"] {
            let token = shutdown.clone();
            listeners.spawn(name, async move {
                token.cancelled().await;
                Ok(())
            });
        }

        let run = tokio::spawn({
            let shutdown = shutdown.clone();
            async move { listeners.run(&shutdown).await }
        });
        shutdown.cancel();

        let outcome = tokio::time::timeout(DEADLINE, run).await.unwrap().unwrap();
        assert!(outcome.is_ok(), "{outcome:?}");
    }

    #[tokio::test]
    async fn an_error_during_shutdown_is_still_returned() {
        let shutdown = CancellationToken::new();
        let mut listeners = Listeners::default();
        let token = shutdown.clone();
        listeners.spawn("draining", async move {
            token.cancelled().await;
            Err(Error::Server("drain failed".into()))
        });

        let run = tokio::spawn({
            let shutdown = shutdown.clone();
            async move { listeners.run(&shutdown).await }
        });
        shutdown.cancel();

        let outcome = tokio::time::timeout(DEADLINE, run).await.unwrap().unwrap();
        let message = outcome.unwrap_err().to_string();
        assert!(message.contains("draining listener: "), "{message}");
    }

    #[tokio::test]
    async fn no_listeners_wait_for_shutdown() {
        let shutdown = CancellationToken::new();
        let run = tokio::spawn({
            let shutdown = shutdown.clone();
            async move { Listeners::default().run(&shutdown).await }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!run.is_finished(), "an empty set must run until shutdown");

        shutdown.cancel();
        let outcome = tokio::time::timeout(DEADLINE, run).await.unwrap().unwrap();
        assert!(outcome.is_ok());
    }
}
