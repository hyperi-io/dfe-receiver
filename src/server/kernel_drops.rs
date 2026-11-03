// Project:   dfe-receiver
// File:      src/server/kernel_drops.rs
// Purpose:   Poll /proc/net/udp(6) for kernel-side UDP drop counters
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Linux `/proc/net/udp` poller. Reads the `drops` column for a listener's
//! bind ports, the datagrams the kernel threw away because the socket's
//! receive buffer was full, and hands each increase to the listener's counter.
//!
//! Linux only. Elsewhere the poller does nothing.

use std::time::Duration;

/// How often the counters are read.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Call `on_drops` with each increase in the kernel's drop count for
/// `bind_ports`, until `shutdown`.
#[cfg(target_os = "linux")]
pub async fn poll_kernel_drops(
    bind_ports: Vec<u16>,
    on_drops: impl Fn(u64) + Send,
    shutdown: tokio_util::sync::CancellationToken,
) {
    use std::collections::HashMap;

    let mut tick = tokio::time::interval(POLL_INTERVAL);
    let mut last_drops: HashMap<u16, u64> = bind_ports.iter().map(|&p| (p, 0)).collect();

    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {
                for (port, drops) in read_drop_counts(&bind_ports) {
                    let last = last_drops.insert(port, drops).unwrap_or(0);
                    let delta = drops.saturating_sub(last);
                    if delta > 0 {
                        on_drops(delta);
                    }
                }
            }
        }
    }
}

/// Parse `/proc/net/udp` and `/proc/net/udp6` and return (port, drops) pairs
/// for every entry whose local port is in `bind_ports`.
#[cfg(target_os = "linux")]
fn read_drop_counts(bind_ports: &[u16]) -> Vec<(u16, u64)> {
    let mut results: Vec<(u16, u64)> = Vec::new();
    for path in ["/proc/net/udp", "/proc/net/udp6"] {
        let Ok(table) = std::fs::read_to_string(path) else {
            continue;
        };
        add_drop_counts(&table, bind_ports, &mut results);
    }
    results
}

/// Add the drops of each row of a `/proc/net/udp` table bound to one of
/// `bind_ports` into `results`, summing the sockets that share a port.
///
/// Columns: `sl local_address rem_address st tx_queue:rx_queue tr:tm_when
/// retrnsmt uid timeout inode ref pointer drops`.
#[cfg(target_os = "linux")]
fn add_drop_counts(table: &str, bind_ports: &[u16], results: &mut Vec<(u16, u64)>) {
    for line in table.lines().skip(1) {
        let mut columns = line.split_whitespace();
        let Some(port) = columns
            .nth(1)
            .and_then(|local| local.split(':').nth(1))
            .and_then(|hex| u16::from_str_radix(hex, 16).ok())
            .filter(|port| bind_ports.contains(port))
        else {
            continue;
        };
        let Some(drops) = columns.nth(10).and_then(|d| d.parse::<u64>().ok()) else {
            continue;
        };
        match results.iter_mut().find(|(p, _)| *p == port) {
            Some(existing) => existing.1 = existing.1.saturating_add(drops),
            None => results.push((port, drops)),
        }
    }
}

// The stub keeps the Linux signature, async included, so callers need no cfg.
#[cfg(not(target_os = "linux"))]
#[allow(clippy::unused_async)]
pub async fn poll_kernel_drops(
    _bind_ports: Vec<u16>,
    _on_drops: impl Fn(u64) + Send,
    _shutdown: tokio_util::sync::CancellationToken,
) {
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    const TABLE: &str = "\
   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  191: 00000000:0202 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 4242 2 0000000000000000 17
  192: 0100007F:0202 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 4243 2 0000000000000000 3
  193: 00000000:080F 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 4244 2 0000000000000000 9
  194: 00000000:1234 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 4245 2 0000000000000000 x
  195: short row
";

    /// Port 514 (0x0202) is summed over its two sockets, 2063 is not asked
    /// for, and rows with no number or too few columns are skipped.
    #[test]
    fn drops_are_summed_per_asked_port() {
        let mut results = Vec::new();
        add_drop_counts(TABLE, &[514, 4660], &mut results);
        assert_eq!(results, vec![(514, 20)]);
    }

    /// A socket with the smallest buffer, flooded and never read, shows its
    /// drops in the live table.
    #[test]
    fn a_flooded_socket_shows_its_drops() {
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )
        .unwrap();
        socket.set_recv_buffer_size(1).unwrap();
        socket
            .bind(
                &"127.0.0.1:0"
                    .parse::<std::net::SocketAddr>()
                    .unwrap()
                    .into(),
            )
            .unwrap();
        let port = socket.local_addr().unwrap().as_socket().unwrap().port();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        for _ in 0..500 {
            sender.send_to(&[b'x'; 512], ("127.0.0.1", port)).unwrap();
        }
        let drops: u64 = read_drop_counts(&[port]).iter().map(|(_, d)| d).sum();
        assert!(
            drops > 0,
            "no drops on port {port} after flooding a tiny buffer"
        );
    }

    #[test]
    fn the_live_tables_read_without_error() {
        let _ = read_drop_counts(&[12345, 54321]);
        assert_eq!(read_drop_counts(&[]), [] as [(u16, u64); 0]);
    }
}
