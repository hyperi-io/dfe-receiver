// Project:   dfe-receiver
// File:      src/server/flow/handler.rs
// Purpose:   FlowHandler -- ProtocolHandler impl for NetFlow + sFlow over UDP
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `FlowHandler` -- the orchestrator that brings up `UdpFlowListener`
//! instances across one or more bind ports and ties the flow subsystem into
//! the existing dfe-receiver server.
//!
//! Two modes:
//! - **Unified (default):** `flow.enabled: true`. One `FlowConfig` block, N
//!   ports each running a `UdpFlowListener` with both `NetflowDecoder` +
//!   `SflowDecoder` enabled. Per-packet protocol autosense routes the
//!   packet to the right decoder.
//! - **Split (opt-in):** `flow.split:` set, `flow.enabled` MUST be false.
//!   Separate listener pools for NetFlow ports and sFlow ports, each with a
//!   dedicated decoder. Ports MUST NOT overlap (validated at construction).
//!
//! On Linux a single `/proc/net/udp` poller is spawned for the union of all
//! bind ports, surfacing kernel-side UDP drops as
//! `dfe_flow_kernel_drops_total`.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::RawCapture;
use crate::error::{Error, Result};
use crate::pipeline::PipelineState;
use crate::server::flow::config::{FlowConfig, FlowListenerConfig};
use crate::server::flow::kernel_drops;
use crate::server::flow::listener::UdpFlowListener;
use crate::server::flow::metrics::FlowMetrics;
use crate::server::flow::rate_limit::PerSourceRateLimiter;
use crate::server::ip_filter::IpFilter;
use crate::server::netflow::decoder::NetflowDecoder;
use crate::server::sflow::decoder::SflowDecoder;
use crate::server::traits::ProtocolHandler;

/// `ProtocolHandler` for the flow subsystem (NetFlow v5/v9, IPFIX, sFlow v5).
pub struct FlowHandler {
    cfg: FlowConfig,
    /// Raw capture already resolved against the common `raw_capture` block.
    raw: RawCapture,
    metrics: FlowMetrics,
    pipeline: Arc<PipelineState>,
    /// Pre-rendered bind address summary, returned by `bind_address()`.
    /// Stored as `String` because the trait returns `&str`.
    bind_address_summary: String,
}

impl FlowHandler {
    /// Build a `FlowHandler`. Returns `Err` if config validation fails.
    pub fn new(
        cfg: FlowConfig,
        raw: RawCapture,
        metrics: FlowMetrics,
        pipeline: Arc<PipelineState>,
    ) -> Result<Self> {
        cfg.validate().map_err(Error::Config)?;
        let bind_address_summary = render_bind_summary(&cfg);
        Ok(Self {
            cfg,
            raw,
            metrics,
            pipeline,
            bind_address_summary,
        })
    }

    /// True if neither unified nor split mode is active -- handler is a no-op.
    fn is_disabled(&self) -> bool {
        !self.cfg.enabled && self.cfg.split.is_none()
    }

    /// Build the union of bind ports across both unified and split modes.
    /// Used to seed the kernel-drops poller.
    fn union_ports(&self) -> Vec<u16> {
        if let Some(split) = &self.cfg.split {
            let mut ports = Vec::with_capacity(split.netflow.ports.len() + split.sflow.ports.len());
            ports.extend(split.netflow.ports.iter().copied());
            ports.extend(split.sflow.ports.iter().copied());
            ports
        } else {
            self.cfg.ports.clone()
        }
    }

    /// Construct a synthetic `FlowListenerConfig` from the unified `FlowConfig`
    /// for a single port. The listener doesn't actually use the `ports`
    /// field on the config; each listener gets a single `SocketAddr`.
    fn unified_listener_cfg(&self) -> FlowListenerConfig {
        FlowListenerConfig {
            enabled: self.cfg.enabled,
            bind_address: self.cfg.bind_address,
            // The listener.cfg.ports field is unused at runtime -- it serves
            // as documentation. We pass through what was configured.
            ports: self.cfg.ports.clone(),
            output: self.cfg.output.clone(),
            channel_capacity: self.cfg.channel_capacity,
            recv_buffer_bytes: self.cfg.recv_buffer_bytes,
            ip_filter: self.cfg.ip_filter.clone(),
            rate_limit: self.cfg.rate_limit.clone(),
            template_cache: self.cfg.netflow.template_cache.clone(),
            topic: self.cfg.netflow.topic.clone(),
        }
    }

    /// Spawn the unified-mode listeners: one `UdpFlowListener` per port, each
    /// carrying both decoders (gated by `netflow.enabled` / `sflow.enabled`).
    /// Returns `(JoinHandles, RateLimiters)` -- the rate limiters are exposed so
    /// the caller can spawn periodic LRU eviction tasks against them.
    fn build_unified_listeners(
        &self,
        shutdown: &CancellationToken,
    ) -> (Vec<JoinHandle<()>>, Vec<Arc<PerSourceRateLimiter>>) {
        let ip_filter = Arc::new(match &self.cfg.ip_filter {
            Some(f) => IpFilter::from_config(f),
            None => IpFilter::disabled(),
        });
        let rate_limiter: Option<Arc<PerSourceRateLimiter>> = if self.cfg.rate_limit.enabled {
            Some(Arc::new(PerSourceRateLimiter::new(
                self.cfg.rate_limit.clone(),
            )))
        } else {
            None
        };
        let listener_cfg = self.unified_listener_cfg();

        let mut handles = Vec::with_capacity(self.cfg.ports.len());
        for port in &self.cfg.ports {
            let bind_addr = SocketAddr::new(self.cfg.bind_address, *port);
            let netflow = if self.cfg.netflow.enabled {
                Some(NetflowDecoder::new(
                    self.cfg.netflow.template_cache.max_per_exporter,
                    self.cfg.netflow.template_cache.max_exporters,
                    self.metrics.clone(),
                ))
            } else {
                None
            };
            let sflow = if self.cfg.sflow.enabled {
                Some(SflowDecoder::new())
            } else {
                None
            };

            let listener = UdpFlowListener::new(
                bind_addr,
                netflow,
                sflow,
                listener_cfg.clone(),
                self.raw,
                self.metrics.clone(),
                self.pipeline.clone(),
                ip_filter.clone(),
                rate_limiter.clone(),
            );
            let l_shutdown = shutdown.clone();
            handles.push(tokio::spawn(async move {
                if let Err(e) = listener.run(l_shutdown).await {
                    error!(error = %e, addr = %bind_addr, "flow listener (unified) exited with error");
                }
            }));
        }
        let rls = rate_limiter.into_iter().collect();
        (handles, rls)
    }

    /// Spawn split-mode listeners. NetFlow-only listeners on `split.netflow.ports`
    /// and sFlow-only listeners on `split.sflow.ports`. Each sub-config has its
    /// own ip_filter + rate_limit instance. Returns `(JoinHandles, RateLimiters)`
    /// -- the rate limiters are exposed so the caller can spawn periodic LRU
    /// eviction tasks against them.
    fn build_split_listeners(
        &self,
        shutdown: &CancellationToken,
    ) -> (Vec<JoinHandle<()>>, Vec<Arc<PerSourceRateLimiter>>) {
        let split = match self.cfg.split.as_ref() {
            Some(s) => s,
            None => return (Vec::new(), Vec::new()),
        };

        let mut handles = Vec::new();
        let mut rate_limiters: Vec<Arc<PerSourceRateLimiter>> = Vec::new();

        // NetFlow side
        if split.netflow.enabled {
            let ip_filter = Arc::new(match &split.netflow.ip_filter {
                Some(f) => IpFilter::from_config(f),
                None => IpFilter::disabled(),
            });
            let rate_limiter = if split.netflow.rate_limit.enabled {
                Some(Arc::new(PerSourceRateLimiter::new(
                    split.netflow.rate_limit.clone(),
                )))
            } else {
                None
            };
            if let Some(rl) = &rate_limiter {
                rate_limiters.push(rl.clone());
            }
            for port in &split.netflow.ports {
                let bind_addr = SocketAddr::new(split.netflow.bind_address, *port);
                let listener = UdpFlowListener::new(
                    bind_addr,
                    Some(NetflowDecoder::new(
                        split.netflow.template_cache.max_per_exporter,
                        split.netflow.template_cache.max_exporters,
                        self.metrics.clone(),
                    )),
                    None,
                    split.netflow.clone(),
                    self.raw,
                    self.metrics.clone(),
                    self.pipeline.clone(),
                    ip_filter.clone(),
                    rate_limiter.clone(),
                );
                let l_shutdown = shutdown.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) = listener.run(l_shutdown).await {
                        error!(error = %e, addr = %bind_addr, "flow listener (split/netflow) exited with error");
                    }
                }));
            }
        }

        // sFlow side
        if split.sflow.enabled {
            let ip_filter = Arc::new(match &split.sflow.ip_filter {
                Some(f) => IpFilter::from_config(f),
                None => IpFilter::disabled(),
            });
            let rate_limiter = if split.sflow.rate_limit.enabled {
                Some(Arc::new(PerSourceRateLimiter::new(
                    split.sflow.rate_limit.clone(),
                )))
            } else {
                None
            };
            if let Some(rl) = &rate_limiter {
                rate_limiters.push(rl.clone());
            }
            for port in &split.sflow.ports {
                let bind_addr = SocketAddr::new(split.sflow.bind_address, *port);
                let listener = UdpFlowListener::new(
                    bind_addr,
                    None,
                    Some(SflowDecoder::new()),
                    split.sflow.clone(),
                    self.raw,
                    self.metrics.clone(),
                    self.pipeline.clone(),
                    ip_filter.clone(),
                    rate_limiter.clone(),
                );
                let l_shutdown = shutdown.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) = listener.run(l_shutdown).await {
                        error!(error = %e, addr = %bind_addr, "flow listener (split/sflow) exited with error");
                    }
                }));
            }
        }

        (handles, rate_limiters)
    }
}

/// Render the `bind_address()` summary string from a `FlowConfig`.
fn render_bind_summary(cfg: &FlowConfig) -> String {
    if let Some(split) = &cfg.split {
        let mut parts: Vec<String> = Vec::new();
        for p in &split.netflow.ports {
            parts.push(format!("{}:{}/udp", split.netflow.bind_address, p));
        }
        for p in &split.sflow.ports {
            parts.push(format!("{}:{}/udp", split.sflow.bind_address, p));
        }
        parts.join(",")
    } else {
        cfg.ports
            .iter()
            .map(|p| format!("{}:{}/udp", cfg.bind_address, p))
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[async_trait::async_trait]
impl ProtocolHandler for FlowHandler {
    fn name(&self) -> &'static str {
        "flow"
    }

    fn bind_address(&self) -> &str {
        &self.bind_address_summary
    }

    async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        if self.is_disabled() {
            info!("flow handler disabled (no unified or split config); skipping");
            return Ok(());
        }

        // EXPERIMENTAL marker: WARN log + info-gauge.
        if self.cfg.experimental {
            warn!(
                handler = "flow",
                "EXPERIMENTAL: flow handler (NetFlow + sFlow) is experimental; \
                 behaviour may change between minor versions, and production use \
                 is at your own risk. Set flow.experimental: false to suppress \
                 this warning once the handler stabilises."
            );
            self.metrics
                .handler_experimental
                .set(&[("handler", "flow")], 1.0);
        }

        let (handles, rate_limiters) = if self.cfg.split.is_some() {
            self.build_split_listeners(&shutdown)
        } else {
            self.build_unified_listeners(&shutdown)
        };

        // Spawn the /proc/net/udp poller on Linux for the union of bind ports.
        let kd_metrics = Arc::new(self.metrics.clone());
        let kd_shutdown = shutdown.clone();
        let ports = self.union_ports();
        let kd_handle = tokio::spawn(async move {
            kernel_drops::poll_kernel_drops(ports, kd_metrics, kd_shutdown).await;
        });

        // Spawn periodic LRU eviction for each constructed rate limiter so the
        // per-source DashMap can't grow unbounded under spoofed-source-IP
        // attacks. One task per limiter (unified mode has at most one; split
        // mode has up to two -- netflow and sflow).
        let mut evict_handles: Vec<JoinHandle<()>> = Vec::with_capacity(rate_limiters.len());
        for rl in rate_limiters {
            let token = shutdown.clone();
            evict_handles.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_mins(1));
                loop {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => return,
                        _ = tick.tick() => rl.evict_lru(),
                    }
                }
            }));
        }

        // Await shutdown -- the listeners exit themselves on the same token.
        shutdown.cancelled().await;

        for h in handles {
            let _ = h.await;
        }
        for h in evict_handles {
            let _ = h.await;
        }
        let _ = kd_handle.await;

        info!("flow handler stopped");
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::{Config, SharedConfig};
    use crate::server::flow::metrics::mock::flow_metrics_for_test;

    /// Build a real `PipelineState` for tests. Uses the loader memory transport
    /// so no Kafka broker is required.
    async fn test_pipeline() -> Arc<PipelineState> {
        let mut config = Config::default();
        config.destinations.default = "loader".into();
        config.loader.transport = "memory".to_string();
        let state = PipelineState::new(SharedConfig::new(config), CancellationToken::new())
            .await
            .unwrap();
        Arc::new(state)
    }

    #[tokio::test]
    async fn new_rejects_invalid_config() {
        // enabled + split set => validate fails.
        let yaml = r"
enabled: true
split:
  netflow:
    ports: [2055]
    topic: x
  sflow:
    ports: [6343]
    topic: y
";
        let cfg: FlowConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let result = FlowHandler::new(
            cfg,
            RawCapture::OFF,
            flow_metrics_for_test(),
            test_pipeline().await,
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn bind_address_unified_renders_all_ports() {
        let cfg = FlowConfig {
            enabled: true,
            ..Default::default()
        };
        let handler = FlowHandler::new(
            cfg,
            RawCapture::OFF,
            flow_metrics_for_test(),
            test_pipeline().await,
        )
        .unwrap();
        let addr = handler.bind_address();
        assert!(addr.contains(":2055/udp"));
        assert!(addr.contains(":4739/udp"));
        assert!(addr.contains(":6343/udp"));
    }

    #[tokio::test]
    async fn bind_address_split_renders_both_sides() {
        let yaml = r"
enabled: false
split:
  netflow:
    ports: [2055, 4739]
    topic: netflow_land
  sflow:
    ports: [6343]
    topic: sflow_land
";
        let cfg: FlowConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let handler = FlowHandler::new(
            cfg,
            RawCapture::OFF,
            flow_metrics_for_test(),
            test_pipeline().await,
        )
        .unwrap();
        let addr = handler.bind_address();
        assert!(addr.contains(":2055/udp"));
        assert!(addr.contains(":4739/udp"));
        assert!(addr.contains(":6343/udp"));
    }

    #[tokio::test]
    async fn name_is_flow() {
        let cfg = FlowConfig::default();
        let handler = FlowHandler::new(
            cfg,
            RawCapture::OFF,
            flow_metrics_for_test(),
            test_pipeline().await,
        )
        .unwrap();
        assert_eq!(handler.name(), "flow");
    }

    #[tokio::test]
    async fn start_disabled_returns_immediately() {
        // Default config has enabled=false and split=None.
        let cfg = FlowConfig::default();
        let handler = FlowHandler::new(
            cfg,
            RawCapture::OFF,
            flow_metrics_for_test(),
            test_pipeline().await,
        )
        .unwrap();
        let shutdown = CancellationToken::new();
        // No need to cancel -- the disabled path returns Ok immediately.
        handler.start(shutdown).await.unwrap();
    }

    #[tokio::test]
    async fn is_disabled_when_neither_unified_nor_split() {
        let cfg = FlowConfig::default();
        let handler = FlowHandler::new(
            cfg,
            RawCapture::OFF,
            flow_metrics_for_test(),
            test_pipeline().await,
        )
        .unwrap();
        assert!(handler.is_disabled());
    }

    #[tokio::test]
    async fn is_disabled_false_when_unified_enabled() {
        let cfg = FlowConfig {
            enabled: true,
            ..Default::default()
        };
        let handler = FlowHandler::new(
            cfg,
            RawCapture::OFF,
            flow_metrics_for_test(),
            test_pipeline().await,
        )
        .unwrap();
        assert!(!handler.is_disabled());
    }

    #[tokio::test]
    async fn union_ports_unified() {
        let cfg = FlowConfig {
            enabled: true,
            ports: vec![2055, 4739, 6343],
            ..Default::default()
        };
        let handler = FlowHandler::new(
            cfg,
            RawCapture::OFF,
            flow_metrics_for_test(),
            test_pipeline().await,
        )
        .unwrap();
        let mut ports = handler.union_ports();
        ports.sort_unstable();
        assert_eq!(ports, vec![2055, 4739, 6343]);
    }

    #[tokio::test]
    async fn union_ports_split() {
        let yaml = r"
enabled: false
split:
  netflow:
    ports: [2055, 4739]
    topic: n
  sflow:
    ports: [6343, 7343]
    topic: s
";
        let cfg: FlowConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let handler = FlowHandler::new(
            cfg,
            RawCapture::OFF,
            flow_metrics_for_test(),
            test_pipeline().await,
        )
        .unwrap();
        let mut ports = handler.union_ports();
        ports.sort_unstable();
        assert_eq!(ports, vec![2055, 4739, 6343, 7343]);
    }
}
