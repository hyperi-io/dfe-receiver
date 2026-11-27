// Project:   dfe-receiver
// File:      src/server/ip_filter.rs
// Purpose:   IP allowlist/denylist on a CIDR trie, checked as connections are accepted
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! IP-based connection filtering, run by each accept loop before any protocol work.
//!
//! Supports allowlist (only listed CIDRs pass) and denylist (listed CIDRs
//! blocked) modes. Uses a prefix trie for O(prefix-length) lookup per connection.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use ipnet::IpNet;
use ipnet_trie::IpnetTrie;
use scalo::logger::log_debounced;
use tracing::{error, info};

use crate::config::IpFilterConfig;
use crate::metrics::Metrics;

/// Shortest gap between two rejection log lines from one filter.
const REJECTION_LOG_INTERVAL_MS: u64 = 5_000;

/// Compiled IP filter for hot-path lookups.
#[derive(Clone)]
pub struct IpFilter {
    inner: Arc<IpFilterInner>,
    /// When this filter last logged a rejection.
    rejection_logged: Arc<AtomicU64>,
}

enum IpFilterInner {
    Disabled,
    Allowlist(IpnetTrie<()>),
    Denylist(IpnetTrie<()>),
    /// A configuration that did not parse: nothing is admitted.
    DenyAll,
}

/// Parse `cidrs` into a trie, refusing the first entry that is not a CIDR.
///
/// An IPv4-mapped IPv6 CIDR (`::ffff:10.0.0.0/104`) is stored as the IPv4 CIDR
/// it maps, since a peer is looked up by its canonical address.
pub(crate) fn parse_cidrs(cidrs: &[String]) -> Result<IpnetTrie<()>, String> {
    let mut trie = IpnetTrie::new();
    for cidr in cidrs {
        let net = cidr
            .trim()
            .parse::<IpNet>()
            .map_err(|e| format!("'{cidr}' is not a CIDR ({e})"))?;
        trie.insert(canonical(net), ());
    }
    Ok(trie)
}

/// `net` as an IPv4 CIDR when it lies wholly inside `::ffff:0:0/96`.
fn canonical(net: IpNet) -> IpNet {
    let IpNet::V6(v6) = net else {
        return net;
    };
    match (
        v6.network().to_ipv4_mapped(),
        v6.prefix_len().checked_sub(96),
    ) {
        (Some(v4), Some(prefix)) => ipnet::Ipv4Net::new(v4, prefix).map_or(net, IpNet::V4),
        _ => net,
    }
}

impl IpFilter {
    /// A disabled filter that allows all IPs. Used by protocol handlers
    /// that don't have their own IP filter config.
    pub fn disabled() -> Self {
        Self::with(IpFilterInner::Disabled)
    }

    fn with(inner: IpFilterInner) -> Self {
        Self {
            inner: Arc::new(inner),
            rejection_logged: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Compile `config`, refusing an unknown mode, an entry in `cidrs` that is
    /// not a CIDR, and an allowlist with no CIDRs.
    pub fn parse(config: &IpFilterConfig) -> Result<Self, String> {
        let trie = parse_cidrs(&config.cidrs)?;
        let inner = match config.mode.to_ascii_lowercase().as_str() {
            "disabled" => IpFilterInner::Disabled,
            "allowlist" if config.cidrs.is_empty() => {
                return Err("mode is 'allowlist' but cidrs is empty, which admits no \
                            one -- list the CIDRs to admit, or set mode: disabled"
                    .into());
            }
            "allowlist" => IpFilterInner::Allowlist(trie),
            "denylist" => IpFilterInner::Denylist(trie),
            _ => {
                return Err(format!(
                    "mode is '{}', which is not one of disabled, allowlist, denylist",
                    config.mode
                ));
            }
        };
        Ok(Self::with(inner))
    }

    /// Build from config, admitting nothing when it does not [`parse`](Self::parse).
    ///
    /// `Config::validate` refuses such a config at load, so only a caller that
    /// skips validation reaches the refusal.
    pub fn from_config(config: &IpFilterConfig) -> Self {
        Self::parse(config).unwrap_or_else(|e| {
            error!(error = %e, "ip_filter does not parse; refusing every connection");
            Self::with(IpFilterInner::DenyAll)
        })
    }

    /// Whether a freshly accepted connection, or a datagram, from `peer` on
    /// `transport` may proceed.
    ///
    /// Every accept loop calls this before any protocol work -- before the TLS
    /// handshake on a TLS listener -- so a barred peer costs one trie lookup
    /// and the connection is dropped by the caller returning to the loop. A
    /// refusal counts on `metrics`; the log line naming the listener is
    /// written at most once per `REJECTION_LOG_INTERVAL_MS`.
    #[must_use]
    pub fn admits(&self, peer: SocketAddr, transport: &str, metrics: &Metrics) -> bool {
        if self.is_allowed(peer.ip()) {
            return true;
        }
        metrics.inc_ip_filter_rejected(transport);
        if log_debounced(&self.rejection_logged, REJECTION_LOG_INTERVAL_MS) {
            info!(peer = %peer, transport, "connection rejected by IP filter");
        }
        false
    }

    /// Check whether a given IP is allowed through.
    ///
    /// An IPv4 client on a dual-stack socket arrives as `::ffff:a.b.c.d`, so it
    /// is matched as the IPv4 address it is.
    #[inline]
    pub fn is_allowed(&self, ip: IpAddr) -> bool {
        match self.inner.as_ref() {
            IpFilterInner::Disabled => true,
            IpFilterInner::Allowlist(trie) => {
                let net = IpNet::from(ip.to_canonical());
                trie.longest_match(&net).is_some()
            }
            IpFilterInner::Denylist(trie) => {
                let net = IpNet::from(ip.to_canonical());
                trie.longest_match(&net).is_none()
            }
            IpFilterInner::DenyAll => false,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn filter_config(mode: &str, cidrs: &[&str]) -> IpFilterConfig {
        IpFilterConfig {
            mode: mode.to_string(),
            cidrs: cidrs.iter().map(ToString::to_string).collect(),
        }
    }

    fn make_filter(mode: &str, cidrs: &[&str]) -> IpFilter {
        IpFilter::from_config(&filter_config(mode, cidrs))
    }

    #[test]
    fn test_disabled_allows_all() {
        let filter = make_filter("disabled", &["10.0.0.0/8"]);
        assert!(filter.is_allowed("1.2.3.4".parse().unwrap()));
        assert!(filter.is_allowed("10.0.0.1".parse().unwrap()));
    }

    #[test]
    fn test_allowlist_permits_listed() {
        let filter = make_filter("allowlist", &["10.0.0.0/8", "172.16.0.0/12"]);
        assert!(filter.is_allowed("10.0.0.1".parse().unwrap()));
        assert!(filter.is_allowed("172.16.5.5".parse().unwrap()));
        assert!(!filter.is_allowed("192.168.1.1".parse().unwrap()));
        assert!(!filter.is_allowed("8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn test_denylist_blocks_listed() {
        let filter = make_filter("denylist", &["10.0.0.0/8"]);
        assert!(!filter.is_allowed("10.0.0.1".parse().unwrap()));
        assert!(filter.is_allowed("192.168.1.1".parse().unwrap()));
        assert!(filter.is_allowed("8.8.8.8".parse().unwrap()));
    }

    /// An IPv4 peer on a dual-stack socket, `::ffff:a.b.c.d`, meets the IPv4
    /// CIDRs, in either mode, and so does a plain IPv4 peer. A CIDR written in
    /// its IPv4-mapped IPv6 form matches both the same way.
    #[test]
    fn an_ipv4_mapped_peer_is_filtered_as_ipv4() {
        for cidr in ["10.0.0.0/8", "::ffff:10.0.0.0/104"] {
            let deny = make_filter("denylist", &[cidr]);
            let allow = make_filter("allowlist", &[cidr]);
            for (peer, listed) in [
                ("::ffff:10.0.0.1", true),
                ("10.0.0.1", true),
                ("::ffff:192.168.1.1", false),
                ("192.168.1.1", false),
            ] {
                let peer: IpAddr = peer.parse().unwrap();
                assert_eq!(deny.is_allowed(peer), !listed, "denylist {cidr} {peer}");
                assert_eq!(allow.is_allowed(peer), listed, "allowlist {cidr} {peer}");
            }
        }
    }

    #[test]
    fn test_ipv6_support() {
        let filter = make_filter("allowlist", &["::1/128", "fd00::/8"]);
        assert!(filter.is_allowed("::1".parse().unwrap()));
        assert!(filter.is_allowed("fd00::1".parse().unwrap()));
        assert!(!filter.is_allowed("2001:db8::1".parse().unwrap()));
    }

    /// Each of these reads as a filter and would admit what it names as barred.
    #[test]
    fn a_config_that_reads_as_filtering_and_would_not_is_refused() {
        for (config, says) in [
            (
                filter_config("allowlist", &["not-a-cidr", "10.0.0.0/8"]),
                "not-a-cidr",
            ),
            (filter_config("denylist", &["10.0.0.0/33"]), "10.0.0.0/33"),
            (filter_config("allowlist", &[]), "cidrs is empty"),
            (filter_config("foobar", &["10.0.0.0/8"]), "'foobar'"),
        ] {
            let Err(err) = IpFilter::parse(&config) else {
                panic!("{config:?} parsed");
            };
            assert!(err.contains(says), "{config:?}: {err}");
        }
    }

    /// A filter built from a config that does not parse admits no one.
    #[test]
    fn an_unparsed_filter_admits_no_one() {
        for filter in [
            make_filter("allowlist", &["not-a-cidr", "10.0.0.0/8"]),
            make_filter("allowlist", &[]),
            make_filter("foobar", &["10.0.0.0/8"]),
        ] {
            assert!(!filter.is_allowed("10.0.0.1".parse().unwrap()));
            assert!(!filter.is_allowed("1.2.3.4".parse().unwrap()));
        }
    }

    #[test]
    fn the_modes_parse_case_insensitively() {
        assert!(IpFilter::parse(&filter_config("AllowList", &["10.0.0.0/8"])).is_ok());
        assert!(IpFilter::parse(&filter_config("DISABLED", &[])).is_ok());
    }

    /// A denylist with no CIDRs bars nobody, which is what it reads as.
    #[test]
    fn an_empty_denylist_parses_and_admits_everyone() {
        let filter = IpFilter::parse(&filter_config("denylist", &[])).unwrap();
        assert!(filter.is_allowed("1.2.3.4".parse().unwrap()));
    }

    #[test]
    fn admits_ignores_the_peer_port() {
        // Accept loops hand over the whole peer address; only the IP is keyed.
        let metrics = Metrics::default();
        let filter = make_filter("allowlist", &["10.0.0.0/8"]);
        assert!(filter.admits("10.0.0.1:54321".parse().unwrap(), "http", &metrics));
        assert!(!filter.admits("8.8.8.8:443".parse().unwrap(), "http", &metrics));
    }

    /// Every refusal counts on the listener's label, and an admitted peer counts nothing.
    #[test]
    fn a_refused_peer_counts_on_its_listener() {
        use crate::metrics::testing::CounterTotals;

        let metrics = Metrics::default();
        let filter = make_filter("denylist", &["10.0.0.0/8"]);
        let totals = CounterTotals::default();
        metrics::with_local_recorder(&totals, || {
            assert!(filter.admits("192.0.2.1:1".parse().unwrap(), "syslog", &metrics));
            for port in 1..=3 {
                let peer = SocketAddr::from(([10, 0, 0, 1], port));
                assert!(!filter.admits(peer, "syslog", &metrics));
            }
        });

        assert_eq!(metrics.get_ip_filter_rejected_total(), 3);
        assert_eq!(totals.total("receiver_ip_filter_rejected_total"), 3);
    }
}
