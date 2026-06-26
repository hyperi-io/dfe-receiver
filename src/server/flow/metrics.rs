//! Flow-specific Prometheus metrics. Wraps the existing ServiceMetrics framework.
//!
//! The trait surface (`FlowCounter`, `FlowLabelledCounter`, `FlowHistogram`,
//! `FlowLabelledGauge`) lets listener / envelope tests use mocks. The
//! `register()` constructor wires the production adapters against the global
//! `metrics` crate recorder that scalo's `MetricsManager` installs.
//! `register()` accepts a `&MetricsManager` reference for API symmetry with
//! `ServiceMetrics::register`; the actual metric routing goes through the global
//! recorder, so the parameter is only used to ensure the recorder has been
//! installed before any metric handles are constructed.

use std::sync::Arc;

use scalo::metrics::MetricsManager;

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

// ---------------------------------------------------------------------------
// Production adapters: route every trait call through the `metrics` crate
// macros so the same `MetricsManager`-installed global recorder receives the
// values. Metric names are looked up (and lazily registered) by the recorder
// on the first emission -- there is no separate `register()` step against the
// recorder. Names that overlap with `ServiceMetrics` (e.g. `transport_*`, which
// the namespace prefixes to `dfe_transport_*`) are shared by design: both call
// sites end up incrementing the same series.
// ---------------------------------------------------------------------------

/// Unlabelled counter routed through `metrics::counter!`.
struct PromCounter {
    name: &'static str,
}

impl FlowCounter for PromCounter {
    fn inc(&self) {
        metrics::counter!(self.name).increment(1);
    }
    fn add(&self, n: u64) {
        metrics::counter!(self.name).increment(n);
    }
}

/// Counter with a fixed label set, emitted via `metrics::counter!`.
struct PromLabelledCounter {
    name: &'static str,
}

impl FlowLabelledCounter for PromLabelledCounter {
    fn inc(&self, labels: &[(&'static str, &str)]) {
        let labels: Vec<(&'static str, String)> =
            labels.iter().map(|(k, v)| (*k, (*v).to_string())).collect();
        metrics::counter!(self.name, &labels).increment(1);
    }
}

/// Histogram routed through `metrics::histogram!`.
struct PromHistogram {
    name: &'static str,
}

impl FlowHistogram for PromHistogram {
    fn observe(&self, value: f64) {
        metrics::histogram!(self.name).record(value);
    }
}

/// Gauge with a fixed label set, emitted via `metrics::gauge!`.
struct PromLabelledGauge {
    name: &'static str,
}

impl FlowLabelledGauge for PromLabelledGauge {
    fn set(&self, labels: &[(&'static str, &str)], value: f64) {
        let labels: Vec<(&'static str, String)> =
            labels.iter().map(|(k, v)| (*k, (*v).to_string())).collect();
        metrics::gauge!(self.name, &labels).set(value);
    }
}

impl FlowMetrics {
    /// Build a `FlowMetrics` backed by the global `metrics` crate recorder
    /// installed by scalo's `MetricsManager`. Idempotent on metric
    /// names -- the `metrics` crate dedupes by `(name, labels)` so registering
    /// the same series from multiple call sites (e.g. `ServiceMetrics` +
    /// `FlowMetrics`) is intentional.
    pub fn register(_mm: &MetricsManager) -> anyhow::Result<Self> {
        describe_flow_metrics();
        Ok(Self {
            recv_total: Arc::new(PromCounter {
                name: "transport_recv_total",
            }),
            recv_bytes_total: Arc::new(PromCounter {
                name: "transport_recv_bytes_total",
            }),
            decode_err_total: Arc::new(PromLabelledCounter {
                name: "transport_decode_err_total",
            }),
            drops_total: Arc::new(PromLabelledCounter {
                name: "transport_drops_total",
            }),
            invalid_packet_total: Arc::new(PromLabelledCounter {
                name: "flow_invalid_packet_total",
            }),
            rate_limited_total: Arc::new(PromLabelledCounter {
                name: "flow_rate_limited_total",
            }),
            records_emitted_total: Arc::new(PromLabelledCounter {
                name: "flow_records_emitted_total",
            }),
            records_per_packet: Arc::new(PromHistogram {
                name: "flow_records_per_packet",
            }),
            template_cache_size: Arc::new(PromLabelledGauge {
                name: "flow_template_cache_size",
            }),
            template_evicted_total: Arc::new(PromCounter {
                name: "flow_template_evicted_total",
            }),
            kernel_drops_total: Arc::new(PromCounter {
                name: "flow_kernel_drops_total",
            }),
            send_duration_seconds: Arc::new(PromHistogram {
                name: "transport_send_duration_seconds",
            }),
            unknown_version_total: Arc::new(PromCounter {
                name: "flow_unknown_version_total",
            }),
            handler_experimental: Arc::new(PromLabelledGauge {
                name: "handler_experimental",
            }),
        })
    }
}

/// Describe flow-specific metrics that don't already have descriptions from
/// `ServiceMetrics`. Shared `transport_*` series (namespace -> `dfe_transport_*`)
/// are described by scalo.
fn describe_flow_metrics() {
    metrics::describe_counter!(
        "flow_invalid_packet_total",
        "Flow packets rejected before decode (truncated, wrong magic, etc.)"
    );
    metrics::describe_counter!(
        "flow_rate_limited_total",
        "Flow packets dropped by the per-source UDP rate limiter"
    );
    metrics::describe_counter!(
        "flow_records_emitted_total",
        "Flow records emitted downstream after decode and envelope rendering"
    );
    metrics::describe_histogram!(
        "flow_records_per_packet",
        "Distribution of flow records produced per UDP datagram"
    );
    metrics::describe_gauge!(
        "flow_template_cache_size",
        "Current entries in the per-exporter NetFlow/IPFIX template cache"
    );
    metrics::describe_counter!(
        "flow_template_evicted_total",
        "Template cache evictions (LRU + per-exporter cap)"
    );
    metrics::describe_counter!(
        "flow_kernel_drops_total",
        "UDP datagrams dropped by the kernel before our recv loop (from /proc/net/udp)"
    );
    metrics::describe_counter!(
        "flow_unknown_version_total",
        "Flow packets whose first 16 bits matched no known protocol version"
    );
    metrics::describe_gauge!(
        "handler_experimental",
        "Set to 1 while a protocol handler is marked experimental"
    );
}

// Mock metrics adapters. Available to unit tests (`cfg(test)`) and to benches
// + integration tests that pull in the crate normally -- they need a working
// `FlowMetrics` value without spinning up a real `MetricsManager`.
pub mod mock {
    //! Mock implementations of the metric traits. Used by listener / envelope
    //! tests so they don't need a real MetricsManager wired up.
    use super::{
        Arc, FlowCounter, FlowHistogram, FlowLabelledCounter, FlowLabelledGauge, FlowMetrics,
    };
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
