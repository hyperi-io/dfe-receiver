//! Per-source-IP token-bucket rate limiter for UDP listeners.
//!
//! Sharded via DashMap. Each entry: (tokens, last_refill_micros) packed into
//! one AtomicU64 for lock-free CAS-based refill. Bounded by LRU eviction
//! (cache_size). Disabled mode short-circuits to always-allow with zero
//! overhead.

use crate::server::flow::config::RateLimitConfig;
use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct PerSourceRateLimiter {
    cfg: RateLimitConfig,
    started_at: Instant,
    buckets: DashMap<IpAddr, AtomicBucket>,
}

struct AtomicBucket {
    /// Upper 32 bits: tokens (u32). Lower 32 bits: last_refill_micros (u32, mod 2^32 since `started_at`).
    state: AtomicU64,
}

impl AtomicBucket {
    fn new(tokens: u32, micros: u32) -> Self {
        Self {
            state: AtomicU64::new(pack(tokens, micros)),
        }
    }
}

#[inline]
fn pack(tokens: u32, micros: u32) -> u64 {
    (u64::from(tokens) << 32) | u64::from(micros)
}

#[inline]
fn unpack(s: u64) -> (u32, u32) {
    ((s >> 32) as u32, s as u32)
}

impl PerSourceRateLimiter {
    pub fn new(cfg: RateLimitConfig) -> Self {
        Self {
            cfg,
            started_at: Instant::now(),
            buckets: DashMap::with_capacity(64),
        }
    }

    pub fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    /// Returns true if the packet is allowed; false if rate-limited.
    pub fn try_acquire(&self, src: IpAddr) -> bool {
        if !self.cfg.enabled {
            return true;
        }
        let now_us = self.started_at.elapsed().as_micros() as u32;
        let entry = self
            .buckets
            .entry(src)
            .or_insert_with(|| AtomicBucket::new(self.cfg.burst, now_us));
        let bucket = entry.value();

        loop {
            let cur = bucket.state.load(Ordering::Acquire);
            let (mut tokens, last_us) = unpack(cur);
            let elapsed_us = now_us.wrapping_sub(last_us);
            // Refill: pps tokens per 1_000_000 us.
            let refill =
                (u64::from(elapsed_us) * u64::from(self.cfg.packets_per_second) / 1_000_000) as u32;
            tokens = tokens.saturating_add(refill).min(self.cfg.burst);
            if tokens == 0 {
                return false;
            }
            let new = pack(tokens - 1, now_us);
            if bucket
                .state
                .compare_exchange(cur, new, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
            // CAS lost the race; retry.
        }
    }

    /// LRU eviction -- called periodically (e.g. every minute) to bound memory.
    /// Removes buckets when cache exceeds `cfg.cache_size`. Simple strategy:
    /// DashMap doesn't expose insertion order, so we remove arbitrary entries
    /// until back under capacity. Operators tune cache_size = 4x expected
    /// steady-state distinct sources.
    pub fn evict_lru(&self) {
        if self.buckets.len() <= self.cfg.cache_size {
            return;
        }
        let target = self.cfg.cache_size;
        let to_remove = self.buckets.len() - target;
        let keys: Vec<_> = self
            .buckets
            .iter()
            .take(to_remove)
            .map(|e| *e.key())
            .collect();
        for k in keys {
            self.buckets.remove(&k);
        }
    }

    #[cfg(test)]
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn ip(o: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, o))
    }

    #[test]
    fn disabled_always_allows() {
        let cfg = RateLimitConfig {
            enabled: false,
            ..Default::default()
        };
        let rl = PerSourceRateLimiter::new(cfg);
        for _ in 0..1000 {
            assert!(rl.try_acquire(ip(1)));
        }
    }

    #[test]
    fn burst_then_throttle() {
        let cfg = RateLimitConfig {
            enabled: true,
            packets_per_second: 100,
            burst: 10,
            cache_size: 4096,
        };
        let rl = PerSourceRateLimiter::new(cfg);
        // Burst of 10 should pass immediately.
        for _ in 0..10 {
            assert!(rl.try_acquire(ip(1)));
        }
        // 11th packet inside microseconds: rejected.
        assert!(!rl.try_acquire(ip(1)));
    }

    #[test]
    fn distinct_sources_have_independent_buckets() {
        let cfg = RateLimitConfig {
            enabled: true,
            packets_per_second: 100,
            burst: 5,
            cache_size: 4096,
        };
        let rl = PerSourceRateLimiter::new(cfg);
        for _ in 0..5 {
            assert!(rl.try_acquire(ip(1)));
        }
        assert!(!rl.try_acquire(ip(1)));
        // Different source: own bucket, fresh tokens.
        for _ in 0..5 {
            assert!(rl.try_acquire(ip(2)));
        }
    }

    #[test]
    fn evict_when_over_capacity() {
        let cfg = RateLimitConfig {
            enabled: true,
            packets_per_second: 100,
            burst: 1,
            cache_size: 4,
        };
        let rl = PerSourceRateLimiter::new(cfg);
        for i in 0..10 {
            let _ = rl.try_acquire(ip(i));
        }
        assert!(rl.bucket_count() > 4);
        rl.evict_lru();
        assert!(rl.bucket_count() <= 4);
    }

    #[test]
    fn the_default_config_passes_traffic_once_enabled() {
        // `enabled` is reachable on its own: DFE_RECEIVER_FLOW_RATE_LIMIT_ENABLED
        // sets it against whatever the rest of the block already holds, and
        // `#[serde(default)]` on the FlowConfig field means a config file with no
        // `flow.rate_limit:` map at all fills the whole struct from
        // RateLimitConfig::default() -- the per-field serde defaults never run.
        // A zero burst and a zero rate is not a loose limiter, it is a closed
        // gate: try_acquire finds no tokens, refills none, and drops the packet.
        let rl = PerSourceRateLimiter::new(RateLimitConfig {
            enabled: true,
            ..Default::default()
        });
        assert!(
            rl.try_acquire(ip(1)),
            "the default rate limit drops the first packet from a source, so \
             turning the limiter on alone discards all flow traffic silently"
        );
    }

    #[test]
    fn the_default_cache_size_holds_a_source() {
        // A zero cache_size evicts every bucket on each sweep.
        let rl = PerSourceRateLimiter::new(RateLimitConfig {
            enabled: true,
            ..Default::default()
        });
        let _ = rl.try_acquire(ip(1));
        rl.evict_lru();
        assert_eq!(
            rl.bucket_count(),
            1,
            "the default cache_size evicts the bucket it just created"
        );
    }

    #[test]
    fn enabled_reports_flag() {
        let off = PerSourceRateLimiter::new(RateLimitConfig {
            enabled: false,
            ..Default::default()
        });
        let on = PerSourceRateLimiter::new(RateLimitConfig {
            enabled: true,
            packets_per_second: 100,
            burst: 10,
            cache_size: 100,
        });
        assert!(!off.enabled());
        assert!(on.enabled());
    }
}
