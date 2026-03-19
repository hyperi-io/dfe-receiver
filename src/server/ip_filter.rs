// Project:   dfe-receiver
// File:      src/server/ip_filter.rs
// Purpose:   IP allowlist/denylist middleware using CIDR trie
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! IP-based request filtering middleware.
//!
//! Supports allowlist (only listed CIDRs pass) and denylist (listed CIDRs
//! blocked) modes. Uses a prefix trie for O(prefix-length) lookup per request.

use std::net::IpAddr;
use std::sync::Arc;

use ipnet::IpNet;
use ipnet_trie::IpnetTrie;
use tracing::warn;

use crate::config::IpFilterConfig;

/// Compiled IP filter for hot-path lookups.
#[derive(Clone)]
pub struct IpFilter {
    inner: Arc<IpFilterInner>,
}

enum IpFilterInner {
    Disabled,
    Allowlist(IpnetTrie<()>),
    Denylist(IpnetTrie<()>),
}

impl IpFilter {
    /// A disabled filter that allows all IPs. Used by protocol handlers
    /// that don't have their own IP filter config.
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(IpFilterInner::Disabled),
        }
    }

    /// Build from config. Parses CIDRs once at startup.
    pub fn from_config(config: &IpFilterConfig) -> Self {
        let mode = config.mode.to_lowercase();
        if mode == "disabled" || config.cidrs.is_empty() {
            return Self {
                inner: Arc::new(IpFilterInner::Disabled),
            };
        }

        let mut trie = IpnetTrie::new();
        for cidr in &config.cidrs {
            match cidr.parse::<IpNet>() {
                Ok(net) => {
                    trie.insert(net, ());
                }
                Err(e) => {
                    warn!(cidr = %cidr, error = %e, "invalid CIDR in ip_filter config, skipping");
                }
            }
        }

        let inner = match mode.as_str() {
            "allowlist" => IpFilterInner::Allowlist(trie),
            "denylist" => IpFilterInner::Denylist(trie),
            other => {
                warn!(mode = %other, "unknown ip_filter mode, disabling");
                IpFilterInner::Disabled
            }
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    /// Check whether a given IP is allowed through.
    #[inline]
    pub fn is_allowed(&self, ip: IpAddr) -> bool {
        match self.inner.as_ref() {
            IpFilterInner::Disabled => true,
            IpFilterInner::Allowlist(trie) => {
                let net = IpNet::from(ip);
                trie.longest_match(&net).is_some()
            }
            IpFilterInner::Denylist(trie) => {
                let net = IpNet::from(ip);
                trie.longest_match(&net).is_none()
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn make_filter(mode: &str, cidrs: &[&str]) -> IpFilter {
        IpFilter::from_config(&IpFilterConfig {
            mode: mode.to_string(),
            cidrs: cidrs.iter().map(ToString::to_string).collect(),
        })
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

    #[test]
    fn test_ipv6_support() {
        let filter = make_filter("allowlist", &["::1/128", "fd00::/8"]);
        assert!(filter.is_allowed("::1".parse().unwrap()));
        assert!(filter.is_allowed("fd00::1".parse().unwrap()));
        assert!(!filter.is_allowed("2001:db8::1".parse().unwrap()));
    }

    #[test]
    fn test_invalid_cidr_skipped() {
        let filter = make_filter("allowlist", &["not-a-cidr", "10.0.0.0/8"]);
        assert!(filter.is_allowed("10.0.0.1".parse().unwrap()));
        assert!(!filter.is_allowed("8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn test_empty_cidrs_disables() {
        let filter = make_filter("allowlist", &[]);
        assert!(filter.is_allowed("1.2.3.4".parse().unwrap()));
    }

    #[test]
    fn test_unknown_mode_disables() {
        let filter = make_filter("foobar", &["10.0.0.0/8"]);
        assert!(filter.is_allowed("1.2.3.4".parse().unwrap()));
    }
}
