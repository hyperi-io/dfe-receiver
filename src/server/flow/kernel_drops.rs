// Project:   dfe-receiver
// File:      src/server/flow/kernel_drops.rs
// Purpose:   Poll /proc/net/udp(6) for kernel-side UDP drop counters
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Linux `/proc/net/udp` poller. Reads the `drops` column for our bind ports
//! and emits deltas as `dfe_flow_kernel_drops_total`.
//!
//! Linux-only; non-Linux platforms get a no-op stub.

use std::sync::Arc;

use crate::server::flow::metrics::FlowMetrics;

#[cfg(target_os = "linux")]
pub async fn poll_kernel_drops(
    bind_ports: Vec<u16>,
    metrics: Arc<FlowMetrics>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    use std::collections::HashMap;
    use tokio::time::{Duration, interval};

    let mut tick = interval(Duration::from_secs(10));
    let mut last_drops: HashMap<u16, u64> = bind_ports.iter().copied().map(|p| (p, 0u64)).collect();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tick.tick() => {
                for (port, drops) in read_drop_counts(&bind_ports) {
                    let last = *last_drops.get(&port).unwrap_or(&0);
                    let delta = drops.saturating_sub(last);
                    if delta > 0 {
                        metrics.kernel_drops_total.add(delta);
                    }
                    last_drops.insert(port, drops);
                }
            }
        }
    }
}

/// Parse `/proc/net/udp` and `/proc/net/udp6` and return (port, drops) pairs
/// for every entry whose local port is in `bind_ports`.
#[cfg(target_os = "linux")]
fn read_drop_counts(bind_ports: &[u16]) -> Vec<(u16, u64)> {
    use std::collections::HashSet;

    let wanted: HashSet<u16> = bind_ports.iter().copied().collect();
    let mut results: Vec<(u16, u64)> = Vec::new();

    for path in ["/proc/net/udp", "/proc/net/udp6"] {
        let s = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(_) => continue,
        };
        // Header line:
        //   sl  local_address rem_address st tx_queue:rx_queue tr:tm_when retrnsmt
        //   uid timeout inode ref pointer drops
        for line in s.lines().skip(1) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 13 {
                continue;
            }
            let local_port_hex = match parts[1].split(':').nth(1) {
                Some(p) => p,
                None => continue,
            };
            let port = match u16::from_str_radix(local_port_hex, 16) {
                Ok(p) => p,
                Err(_) => continue,
            };
            if !wanted.contains(&port) {
                continue;
            }
            if let Ok(drops) = parts[12].parse::<u64>() {
                // Multiple rows may share a port (one per socket); accumulate.
                if let Some(existing) = results.iter_mut().find(|(p, _)| *p == port) {
                    existing.1 = existing.1.saturating_add(drops);
                } else {
                    results.push((port, drops));
                }
            }
        }
    }
    results
}

// Async signature must mirror the Linux version for call-site uniformity; the
// stub has nothing to await, so silence the lint here only.
#[cfg(not(target_os = "linux"))]
#[allow(clippy::unused_async)]
pub async fn poll_kernel_drops(
    _bind_ports: Vec<u16>,
    _metrics: Arc<FlowMetrics>,
    _shutdown: tokio_util::sync::CancellationToken,
) {
    // No-op on non-Linux.
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn reads_proc_net_udp_without_error() {
        // /proc/net/udp always exists on Linux; just check the parser
        // doesn't blow up. The result may be empty (port unlikely to be
        // bound in tests) but the call must not panic.
        let _ = read_drop_counts(&[12345, 54321]);
    }

    #[test]
    fn empty_port_list_returns_empty() {
        let result = read_drop_counts(&[]);
        assert_eq!(result, [] as [(u16, u64); 0]);
    }
}
