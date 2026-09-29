// Project:   dfe-receiver
// File:      src/server/client_ip.rs
// Purpose:   Attribute each HTTP request to the client that sent it
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The address an HTTP request is attributed to.
//!
//! The per-IP rate limit keys on it and auth-failure events report it. It is
//! the TCP peer, unless the peer is one of `server.trusted_proxies`: only a
//! trusted proxy's `X-Forwarded-For` or `X-Real-IP` names the client, since
//! from anyone else those headers say whatever the client wrote.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::Response;
use ipnet::IpNet;
use ipnet_trie::IpnetTrie;
use tower_governor::GovernorError;
use tower_governor::key_extractor::KeyExtractor;

const X_FORWARDED_FOR: &str = "x-forwarded-for";
const X_REAL_IP: &str = "x-real-ip";

/// The client a request is attributed to, set by [`attribute_client`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientIp(pub IpAddr);

/// The proxies whose forwarding headers are believed.
#[derive(Clone, Default)]
pub struct TrustedProxies(Option<Arc<IpnetTrie<()>>>);

impl TrustedProxies {
    /// Parse `cidrs`, refusing the first entry that is not a CIDR.
    pub fn parse(cidrs: &[String]) -> Result<Self, String> {
        if cidrs.is_empty() {
            return Ok(Self(None));
        }
        let trie = crate::server::ip_filter::parse_cidrs(cidrs)?;
        Ok(Self(Some(Arc::new(trie))))
    }

    fn contains(&self, ip: IpAddr) -> bool {
        self.0
            .as_ref()
            .is_some_and(|trie| trie.longest_match(&IpNet::from(ip)).is_some())
    }

    /// The client behind a connection from `peer`.
    ///
    /// `peer` itself unless it is a trusted proxy. From a trusted proxy,
    /// `X-Forwarded-For` wins over `X-Real-IP`, and `peer` stands when neither
    /// names a usable address.
    #[must_use]
    pub fn client_ip(&self, peer: IpAddr, headers: &HeaderMap) -> IpAddr {
        if !self.contains(peer) {
            return peer;
        }
        if headers.contains_key(X_FORWARDED_FOR) {
            return self.nearest_untrusted_hop(headers).unwrap_or(peer);
        }
        headers
            .get(X_REAL_IP)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_hop)
            .unwrap_or(peer)
    }

    /// The right-most `X-Forwarded-For` hop that is not a trusted proxy.
    ///
    /// Each proxy appends the address it took the request from, so every
    /// entry left of the first untrusted one is the client's own writing. An
    /// entry that does not parse ends the walk at the last proxy read.
    fn nearest_untrusted_hop(&self, headers: &HeaderMap) -> Option<IpAddr> {
        let mut nearest = None;
        for value in headers.get_all(X_FORWARDED_FOR).iter().rev() {
            let Ok(value) = value.to_str() else {
                return nearest;
            };
            for hop in value.rsplit(',') {
                let Some(ip) = parse_hop(hop) else {
                    return nearest;
                };
                if !self.contains(ip) {
                    return Some(ip);
                }
                nearest = Some(ip);
            }
        }
        nearest
    }
}

/// One forwarding-header entry: a bare address, or an address with a port.
fn parse_hop(hop: &str) -> Option<IpAddr> {
    let hop = hop.trim();
    hop.parse::<IpAddr>()
        .ok()
        .or_else(|| hop.parse::<SocketAddr>().ok().map(|addr| addr.ip()))
}

/// Set [`ClientIp`] on `request` from the peer address the accept loop put there.
///
/// A request with no peer address gets no [`ClientIp`], and the rate limiter
/// refuses it rather than guess.
pub async fn attribute_client(
    State(proxies): State<TrustedProxies>,
    mut request: Request,
    next: Next,
) -> Response {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip());
    if let Some(peer) = peer {
        let client = proxies.client_ip(peer, request.headers());
        request.extensions_mut().insert(ClientIp(client));
    }
    next.run(request).await
}

/// Rate-limit key: the [`ClientIp`] [`attribute_client`] set on the request.
#[derive(Debug, Clone, Copy)]
pub struct ClientIpKey;

impl KeyExtractor for ClientIpKey {
    type Key = IpAddr;

    fn extract<T>(&self, req: &http::Request<T>) -> Result<Self::Key, GovernorError> {
        req.extensions()
            .get::<ClientIp>()
            .map(|ClientIp(ip)| *ip)
            .ok_or(GovernorError::UnableToExtractKey)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const PROXY: &str = "10.0.0.5";
    const CLIENT: &str = "198.51.100.7";
    const VICTIM: &str = "203.0.113.9";

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn proxies(cidrs: &[&str]) -> TrustedProxies {
        TrustedProxies::parse(&cidrs.iter().map(ToString::to_string).collect::<Vec<_>>()).unwrap()
    }

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(*name, value.parse().unwrap());
        }
        map
    }

    /// With no trusted proxies a client cannot name any address but its own.
    #[test]
    fn with_no_trusted_proxies_the_peer_is_the_client() {
        let forged = headers(&[(X_FORWARDED_FOR, VICTIM), (X_REAL_IP, VICTIM)]);
        assert_eq!(
            TrustedProxies::default().client_ip(ip(CLIENT), &forged),
            ip(CLIENT)
        );
    }

    #[test]
    fn a_peer_outside_the_trusted_set_is_the_client_whatever_it_forwards() {
        let forged = headers(&[(X_FORWARDED_FOR, VICTIM)]);
        assert_eq!(
            proxies(&["10.0.0.0/8"]).client_ip(ip(CLIENT), &forged),
            ip(CLIENT)
        );
    }

    #[test]
    fn a_trusted_proxy_names_the_client_it_appended() {
        let forwarded = headers(&[(X_FORWARDED_FOR, CLIENT)]);
        assert_eq!(
            proxies(&["10.0.0.0/8"]).client_ip(ip(PROXY), &forwarded),
            ip(CLIENT)
        );
    }

    /// The client's own entry sits left of what the proxy appended, so a
    /// forged victim address never becomes the key.
    #[test]
    fn a_forged_entry_left_of_the_proxys_is_ignored() {
        let forwarded = headers(&[(X_FORWARDED_FOR, &format!("{VICTIM}, {CLIENT}"))]);
        assert_eq!(
            proxies(&["10.0.0.0/8"]).client_ip(ip(PROXY), &forwarded),
            ip(CLIENT)
        );
    }

    /// A chain of trusted proxies is walked past, across header lines too.
    #[test]
    fn a_chain_of_trusted_proxies_is_walked_past() {
        let forwarded = headers(&[
            (X_FORWARDED_FOR, &format!("{VICTIM}, {CLIENT}")),
            (X_FORWARDED_FOR, "10.0.0.9"),
        ]);
        assert_eq!(
            proxies(&["10.0.0.0/8"]).client_ip(ip(PROXY), &forwarded),
            ip(CLIENT)
        );
    }

    #[test]
    fn an_unparseable_hop_stops_at_the_last_proxy_read() {
        let forwarded = headers(&[(X_FORWARDED_FOR, "garbage, 10.0.0.9")]);
        assert_eq!(
            proxies(&["10.0.0.0/8"]).client_ip(ip(PROXY), &forwarded),
            ip("10.0.0.9")
        );
        let unusable = headers(&[(X_FORWARDED_FOR, "garbage")]);
        assert_eq!(
            proxies(&["10.0.0.0/8"]).client_ip(ip(PROXY), &unusable),
            ip(PROXY)
        );
    }

    #[test]
    fn a_hop_with_a_port_is_read() {
        let forwarded = headers(&[(X_FORWARDED_FOR, "198.51.100.7:4711")]);
        assert_eq!(
            proxies(&["10.0.0.0/8"]).client_ip(ip(PROXY), &forwarded),
            ip(CLIENT)
        );
        let v6 = headers(&[(X_FORWARDED_FOR, "[2001:db8::1]:443")]);
        assert_eq!(
            proxies(&["10.0.0.0/8"]).client_ip(ip(PROXY), &v6),
            ip("2001:db8::1")
        );
    }

    /// `X-Real-IP` is read only from a trusted proxy that sent no `X-Forwarded-For`.
    #[test]
    fn x_real_ip_is_read_only_without_x_forwarded_for() {
        let trusted = proxies(&["10.0.0.0/8"]);
        let real_only = headers(&[(X_REAL_IP, CLIENT)]);
        assert_eq!(trusted.client_ip(ip(PROXY), &real_only), ip(CLIENT));

        let both = headers(&[(X_FORWARDED_FOR, CLIENT), (X_REAL_IP, VICTIM)]);
        assert_eq!(trusted.client_ip(ip(PROXY), &both), ip(CLIENT));
    }

    #[test]
    fn a_trusted_proxy_with_no_forwarding_header_is_the_client() {
        assert_eq!(
            proxies(&["10.0.0.0/8"]).client_ip(ip(PROXY), &HeaderMap::new()),
            ip(PROXY)
        );
    }

    #[test]
    fn an_entry_that_is_not_a_cidr_is_refused() {
        let Err(err) = TrustedProxies::parse(&["10.0.0.5".to_string()]) else {
            panic!("a bare address parsed as a CIDR");
        };
        assert!(err.contains("10.0.0.5"), "{err}");
    }

    #[test]
    fn the_key_is_the_attributed_client_and_absent_without_one() {
        let mut request = http::Request::new(());
        assert!(matches!(
            ClientIpKey.extract(&request),
            Err(GovernorError::UnableToExtractKey)
        ));
        request.extensions_mut().insert(ClientIp(ip(CLIENT)));
        assert_eq!(ClientIpKey.extract(&request).unwrap(), ip(CLIENT));
    }
}
