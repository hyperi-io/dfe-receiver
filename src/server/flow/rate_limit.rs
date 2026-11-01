//! Per-source-IP token-bucket rate limiter for UDP listeners.
//!
//! Sharded via DashMap. Each entry: (tokens, last_refill_micros) packed into
//! one AtomicU64 for lock-free CAS-based refill. A new source past
//! `cache_size` evicts a bucket first, an idle one where it can find one, so
//! spoofed sources cannot grow the map. Disabled mode short-circuits to
//! always-allow with zero overhead.

use crate::server::flow::config::RateLimitConfig;
use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Buckets a full cache looks at for an idle one before it evicts the first.
const EVICTION_SCAN: usize = 32;

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
        self.acquire_at(src, self.started_at.elapsed().as_micros() as u32)
    }

    /// `try_acquire` at a supplied clock reading, so tests run without real time.
    fn acquire_at(&self, src: IpAddr, now_us: u32) -> bool {
        if self.buckets.len() >= self.cfg.cache_size && !self.buckets.contains_key(&src) {
            self.make_room(now_us);
        }
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
            let refilled = tokens.saturating_add(refill);
            tokens = refilled.min(self.cfg.burst);
            if tokens == 0 {
                return false;
            }
            // Advance by the time the whole tokens cover, so the part-token time
            // stays in the bucket. A full bucket has nothing to bank, so it
            // restarts from now.
            let refilled_at = if refilled > self.cfg.burst {
                now_us
            } else if refill == 0 {
                last_us
            } else {
                let covered_us =
                    u64::from(refill) * 1_000_000 / u64::from(self.cfg.packets_per_second);
                last_us.wrapping_add(covered_us as u32)
            };
            let new = pack(tokens - 1, refilled_at);
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

    /// Evict one bucket for a new source: the first idle one among the
    /// first [`EVICTION_SCAN`] looked at, else the first of them. An idle
    /// bucket has refilled to the burst, so a new bucket for its source
    /// starts where it would have.
    fn make_room(&self, now_us: u32) {
        let mut victim = None;
        for entry in self.buckets.iter().take(EVICTION_SCAN) {
            if self.is_idle(entry.value(), now_us) {
                victim = Some(*entry.key());
                break;
            }
            victim.get_or_insert(*entry.key());
        }
        if let Some(src) = victim
            && self.buckets.remove(&src).is_some()
        {
            metrics::counter!("flow_rate_limit_evicted_total").increment(1);
        }
    }

    /// Whether `bucket` has refilled to the burst by `now_us`.
    fn is_idle(&self, bucket: &AtomicBucket, now_us: u32) -> bool {
        let (tokens, last_us) = unpack(bucket.state.load(Ordering::Acquire));
        let elapsed_us = now_us.wrapping_sub(last_us);
        let refill = u64::from(elapsed_us) * u64::from(self.cfg.packets_per_second) / 1_000_000;
        u64::from(tokens) + refill >= u64::from(self.cfg.burst)
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
        // Burst of 10 passes at one instant.
        for _ in 0..10 {
            assert!(rl.acquire_at(ip(1), 0));
        }
        // 11th packet at the same instant: rejected.
        assert!(!rl.acquire_at(ip(1), 0));
        // 100 pps refills one token every 10ms, and only one.
        assert!(rl.acquire_at(ip(1), 10_000));
        assert!(!rl.acquire_at(ip(1), 10_000));
    }

    /// A source sending above the limit gets the limit, not less. The packet
    /// that lets a token through must not discard the fraction of a token the
    /// elapsed time had already earned.
    #[test]
    fn a_steady_over_rate_source_passes_at_the_configured_rate() {
        // (label, limit pps, send pps, burst)
        let cases = [
            ("1.5x the limit", 100u32, 150u32, 10u32),
            ("1.1x the limit", 100, 110, 10),
            ("3x the limit", 100, 300, 10),
            ("a high limit", 5_000, 7_500, 50),
        ];
        let seconds = 5u64;
        for (label, limit, send, burst) in cases {
            let rl = PerSourceRateLimiter::new(RateLimitConfig {
                enabled: true,
                packets_per_second: limit,
                burst,
                cache_size: 4096,
            });
            let total = u64::from(send) * seconds;
            let passed = (0..total)
                .filter(|i| rl.acquire_at(ip(1), (i * 1_000_000 / u64::from(send)) as u32))
                .count() as u64;
            // The burst, plus the limit for every second the stream ran.
            let expected = u64::from(burst) + u64::from(limit) * seconds;
            assert!(
                passed.abs_diff(expected) <= seconds,
                "{label}: {passed} packets passed in {seconds}s, expected {expected} +/- {seconds}"
            );
        }
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
            assert!(rl.acquire_at(ip(1), 0));
        }
        assert!(!rl.acquire_at(ip(1), 0));
        // Different source: own bucket, fresh tokens.
        for _ in 0..5 {
            assert!(rl.acquire_at(ip(2), 0));
        }
    }

    /// A flood of new sources, spoofed ones included, never holds more
    /// buckets than `cache_size` between sweeps, and each bucket evicted is
    /// counted.
    #[test]
    fn a_flood_of_sources_holds_no_more_buckets_than_the_cache() {
        let recorder = crate::buffer::rejects::CountedKeys::default();
        let _local = metrics::set_default_local_recorder(&recorder);
        let rl = PerSourceRateLimiter::new(RateLimitConfig {
            enabled: true,
            packets_per_second: 100,
            burst: 1,
            cache_size: 4,
        });
        for i in 0..10 {
            assert!(rl.acquire_at(ip(i), 0), "a new source has a token");
        }
        assert_eq!(rl.bucket_count(), 4);
        assert_eq!(recorder.get("flow_rate_limit_evicted_total{}"), 6);
        assert!(
            !rl.acquire_at(ip(9), 0),
            "the newest source lost the bucket it just spent"
        );
    }

    /// A full cache evicts an idle bucket, refilled to the burst and so the
    /// same as a new one, before a source still being throttled.
    #[test]
    fn a_full_cache_evicts_an_idle_bucket_before_a_throttled_one() {
        let rl = PerSourceRateLimiter::new(RateLimitConfig {
            enabled: true,
            packets_per_second: 1,
            burst: 1,
            cache_size: 2,
        });
        assert!(rl.acquire_at(ip(1), 0));
        assert!(rl.acquire_at(ip(2), 3_000_000));
        // Source 1 has refilled by now, source 2 has not.
        assert!(rl.acquire_at(ip(3), 3_500_000));
        assert!(
            !rl.acquire_at(ip(2), 3_500_000),
            "the throttled source was evicted, so it got a fresh burst"
        );
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
    fn the_default_cache_size_holds_more_than_one_source() {
        // A zero or one cache_size evicts a bucket for every new source.
        let rl = PerSourceRateLimiter::new(RateLimitConfig {
            enabled: true,
            ..Default::default()
        });
        let _ = rl.try_acquire(ip(1));
        let _ = rl.try_acquire(ip(2));
        assert_eq!(
            rl.bucket_count(),
            2,
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
