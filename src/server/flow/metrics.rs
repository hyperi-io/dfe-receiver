//! Flow-specific Prometheus metrics. Wraps the existing DfeMetrics framework.
//!
//! Concrete registration against `hyperi_rustlib::metrics::MetricsManager` is
//! wired in Task 21 (server orchestration). For now this module ships the
//! trait surface (allowing listener / envelope tests to use mocks) and the
//! `FlowMetrics` struct that holds the metric handles.

use std::sync::Arc;

/// Initialized once per FlowHandler. Provides label-scoped counter handles.
#[derive(Clone)]
pub struct FlowMetrics {
    pub recv_total: Arc<dyn FlowCounter>,
    pub recv_bytes_total: Arc<dyn FlowCounter>,
    pub decode_err_total: Arc<dyn FlowLabelledCounter>,
    pub drops_total: Arc<dyn FlowLabelledCounter>,
    pub invalid_packet_total: Arc<dyn FlowLabelledCounter>,
    pub rate_limited_total: Arc<dyn FlowLabelledCounter>,
    pub records_emitted_total: Arc<dyn FlowLabelledCounter>,
    pub records_per_packet: Arc<dyn FlowHistogram>,
    pub template_cache_size: Arc<dyn FlowLabelledGauge>,
    pub template_evicted_total: Arc<dyn FlowCounter>,
    pub kernel_drops_total: Arc<dyn FlowCounter>,
    pub send_duration_seconds: Arc<dyn FlowHistogram>,
    pub unknown_version_total: Arc<dyn FlowCounter>,
    /// Info-gauge: labels=handler, value=1 when experimental is true.
    pub handler_experimental: Arc<dyn FlowLabelledGauge>,
}

pub trait FlowCounter: Send + Sync {
    fn inc(&self);
    fn add(&self, n: u64);
}

pub trait FlowLabelledCounter: Send + Sync {
    fn inc(&self, labels: &[(&'static str, &str)]);
}

pub trait FlowHistogram: Send + Sync {
    fn observe(&self, value: f64);
}

pub trait FlowLabelledGauge: Send + Sync {
    fn set(&self, labels: &[(&'static str, &str)], value: f64);
}

#[cfg(test)]
pub mod mock {
    //! Mock implementations of the metric traits. Used by listener / envelope
    //! tests so they don't need a real MetricsManager wired up.
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    pub struct MockCounter(pub AtomicU64);
    impl FlowCounter for MockCounter {
        fn inc(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn add(&self, n: u64) {
            self.0.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub struct MockLabelledCounter(pub AtomicU64);
    impl FlowLabelledCounter for MockLabelledCounter {
        fn inc(&self, _labels: &[(&'static str, &str)]) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub struct MockHistogram(pub AtomicU64);
    impl FlowHistogram for MockHistogram {
        fn observe(&self, _value: f64) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub struct MockLabelledGauge;
    impl FlowLabelledGauge for MockLabelledGauge {
        fn set(&self, _labels: &[(&'static str, &str)], _value: f64) {}
    }

    pub fn flow_metrics_for_test() -> FlowMetrics {
        FlowMetrics {
            recv_total: Arc::new(MockCounter(AtomicU64::new(0))),
            recv_bytes_total: Arc::new(MockCounter(AtomicU64::new(0))),
            decode_err_total: Arc::new(MockLabelledCounter(AtomicU64::new(0))),
            drops_total: Arc::new(MockLabelledCounter(AtomicU64::new(0))),
            invalid_packet_total: Arc::new(MockLabelledCounter(AtomicU64::new(0))),
            rate_limited_total: Arc::new(MockLabelledCounter(AtomicU64::new(0))),
            records_emitted_total: Arc::new(MockLabelledCounter(AtomicU64::new(0))),
            records_per_packet: Arc::new(MockHistogram(AtomicU64::new(0))),
            template_cache_size: Arc::new(MockLabelledGauge),
            template_evicted_total: Arc::new(MockCounter(AtomicU64::new(0))),
            kernel_drops_total: Arc::new(MockCounter(AtomicU64::new(0))),
            send_duration_seconds: Arc::new(MockHistogram(AtomicU64::new(0))),
            unknown_version_total: Arc::new(MockCounter(AtomicU64::new(0))),
            handler_experimental: Arc::new(MockLabelledGauge),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn mock_counter_increments() {
            let c = MockCounter(AtomicU64::new(0));
            c.inc();
            c.inc();
            c.add(5);
            assert_eq!(c.0.load(Ordering::Relaxed), 7);
        }

        #[test]
        fn mock_labelled_counter_increments() {
            let c = MockLabelledCounter(AtomicU64::new(0));
            c.inc(&[("transport", "netflow"), ("reason", "parse_err")]);
            c.inc(&[("transport", "sflow"), ("reason", "parse_err")]);
            assert_eq!(c.0.load(Ordering::Relaxed), 2);
        }

        #[test]
        fn flow_metrics_for_test_constructs() {
            let m = flow_metrics_for_test();
            m.recv_total.inc();
            m.drops_total
                .inc(&[("transport", "netflow"), ("reason", "ip_filter")]);
            m.records_per_packet.observe(42.0);
            m.handler_experimental.set(&[("handler", "flow")], 1.0);
            // Compile-only check: handles are usable.
        }
    }
}
