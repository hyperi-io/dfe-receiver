// Project:   dfe-receiver
// File:      src/buffer/mod.rs
// Purpose:   Memory buffer and disk spillover
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Buffer management with memory pressure detection.
//!
//! Provides in-memory batching with disk spillover when under pressure.

pub mod adapter;
pub mod tiered;

pub use tiered::{InMemoryBuffer, InMemoryBufferStats};
// Re-export CircuitState from hyperi-rustlib for convenience
pub use hyperi_rustlib::tiered_sink::CircuitState;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::config::BufferConfig;

/// Memory pressure levels.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MemoryPressure {
    /// Memory usage is low.
    Low,
    /// Memory usage is moderate.
    Medium,
    /// Memory usage is high - apply backpressure.
    High,
}

/// Buffer manager tracking memory usage.
pub struct BufferManager {
    total_bytes: AtomicU64,
    memory_limit: u64,
    pressure_threshold: f64,
    under_pressure: AtomicBool,
}

impl BufferManager {
    /// Create a new buffer manager.
    pub fn new(config: &BufferConfig) -> Self {
        // Auto-detect memory limit if not set
        let memory_limit = if config.memory_limit == 0 {
            // Default to 67% of available memory
            let sys = sysinfo::System::new_all();
            let available = sys.available_memory();
            (available * 67) / 100
        } else {
            config.memory_limit as u64
        };

        Self {
            total_bytes: AtomicU64::new(0),
            memory_limit,
            pressure_threshold: config.pressure_threshold,
            under_pressure: AtomicBool::new(false),
        }
    }

    /// Add bytes to the tracked total.
    #[inline]
    pub fn add_bytes(&self, bytes: u64) {
        let new_total = self.total_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.update_pressure(new_total);
    }

    /// Remove bytes from the tracked total.
    #[inline]
    pub fn remove_bytes(&self, bytes: u64) {
        let new_total = self.total_bytes.fetch_sub(bytes, Ordering::Relaxed) - bytes;
        self.update_pressure(new_total);
    }

    /// Get current memory pressure level.
    #[inline]
    pub fn pressure(&self) -> MemoryPressure {
        let total = self.total_bytes.load(Ordering::Relaxed);
        let ratio = total as f64 / self.memory_limit as f64;

        if ratio >= self.pressure_threshold {
            MemoryPressure::High
        } else if ratio >= 0.5 {
            MemoryPressure::Medium
        } else {
            MemoryPressure::Low
        }
    }

    /// Check if under memory pressure.
    #[inline]
    pub fn is_under_pressure(&self) -> bool {
        self.under_pressure.load(Ordering::Relaxed)
    }

    /// Get current total bytes.
    #[inline]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Get configured memory limit.
    #[inline]
    pub fn memory_limit(&self) -> u64 {
        self.memory_limit
    }

    /// Update pressure state.
    #[inline]
    fn update_pressure(&self, total: u64) {
        let ratio = total as f64 / self.memory_limit as f64;
        let under_pressure = ratio >= self.pressure_threshold;
        self.under_pressure.store(under_pressure, Ordering::Relaxed);
    }
}

impl Default for BufferManager {
    fn default() -> Self {
        Self::new(&BufferConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pressure() {
        let config = BufferConfig {
            memory_limit: 1000,
            pressure_threshold: 0.8,
            ..Default::default()
        };
        let manager = BufferManager::new(&config);

        // Low usage
        manager.add_bytes(100);
        assert_eq!(manager.pressure(), MemoryPressure::Low);
        assert!(!manager.is_under_pressure());

        // Medium usage
        manager.add_bytes(400);
        assert_eq!(manager.pressure(), MemoryPressure::Medium);
        assert!(!manager.is_under_pressure());

        // High usage
        manager.add_bytes(400);
        assert_eq!(manager.pressure(), MemoryPressure::High);
        assert!(manager.is_under_pressure());
    }
}
