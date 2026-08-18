//  Project:      dfe-receiver
//  File:         src/server/flow/config.rs
//  Purpose:      Flow listener configuration (unified + split modes)
//  Language:     Rust
//
//  License:      BUSL-1.1
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! Flow listener configuration. Unified (default) and split modes.
//!
//! Unified mode: `flow.enabled: true` with `ports` listening on autosense.
//! Split mode: `flow.split:` opt-in, with separate NetFlow / sFlow listeners.

use serde::{Deserialize, Serialize};
use std::net::IpAddr;

use crate::config::IpFilterConfig;

/// Shape of the emitted event: one per packet, or one per flow record.
///
/// Raw record retention is no longer a mode. It is
/// `flow.raw_capture.enabled`, which composes with either shape and writes
/// the common-header `_raw` field like every other transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OutputMode {
    Canonical,
    Exploded,
}

impl Default for OutputMode {
    fn default() -> Self {
        OutputMode::Canonical
    }
}

impl OutputMode {
    /// Parse a config string, `None` for an unrecognised value.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "canonical" => Some(OutputMode::Canonical),
            "exploded" => Some(OutputMode::Exploded),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            OutputMode::Canonical => "canonical",
            OutputMode::Exploded => "exploded",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct FlowOutputConfig {
    #[serde(default)]
    pub mode: OutputMode,
    #[serde(default = "default_max_records")]
    pub max_records_per_packet: usize,
}

impl Default for FlowOutputConfig {
    fn default() -> Self {
        Self {
            mode: OutputMode::default(),
            max_records_per_packet: default_max_records(),
        }
    }
}

fn default_max_records() -> usize {
    200
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, schemars::JsonSchema)]
pub struct RateLimitConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_pps")]
    pub packets_per_second: u32,
    #[serde(default = "default_burst")]
    pub burst: u32,
    #[serde(default = "default_rl_cache")]
    pub cache_size: usize,
}

fn default_pps() -> u32 {
    5000
}

fn default_burst() -> u32 {
    10000
}

fn default_rl_cache() -> usize {
    4096
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct TemplateCacheConfig {
    #[serde(default = "default_per_exporter")]
    pub max_per_exporter: usize,
    #[serde(default = "default_max_exporters")]
    pub max_exporters: usize,
}

impl Default for TemplateCacheConfig {
    fn default() -> Self {
        Self {
            max_per_exporter: default_per_exporter(),
            max_exporters: default_max_exporters(),
        }
    }
}

fn default_per_exporter() -> usize {
    1000
}

fn default_max_exporters() -> usize {
    10000
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct NetflowSubConfig {
    #[serde(default = "yes")]
    pub enabled: bool,
    #[serde(default)]
    pub template_cache: TemplateCacheConfig,
    #[serde(default = "default_netflow_topic")]
    pub topic: String,
}

impl Default for NetflowSubConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            template_cache: TemplateCacheConfig::default(),
            topic: default_netflow_topic(),
        }
    }
}

fn default_netflow_topic() -> String {
    "netflow_land".into()
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct SflowSubConfig {
    #[serde(default = "yes")]
    pub enabled: bool,
    #[serde(default = "default_sflow_topic")]
    pub topic: String,
}

impl Default for SflowSubConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            topic: default_sflow_topic(),
        }
    }
}

fn default_sflow_topic() -> String {
    "sflow_land".into()
}

fn yes() -> bool {
    true
}

/// Unified-mode listener config -- one block applies to all decoders on
/// configured ports.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct FlowConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_bind")]
    pub bind_address: IpAddr,

    #[serde(default = "default_ports")]
    pub ports: Vec<u16>,

    #[serde(default)]
    pub output: FlowOutputConfig,

    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,

    #[serde(default = "default_recv_buffer_bytes")]
    pub recv_buffer_bytes: usize,

    pub ip_filter: Option<IpFilterConfig>,

    #[serde(default)]
    pub rate_limit: RateLimitConfig,

    #[serde(default)]
    pub netflow: NetflowSubConfig,

    #[serde(default)]
    pub sflow: SflowSubConfig,

    /// If present, switches to split mode and `flow.enabled` MUST be false.
    pub split: Option<FlowSplitConfig>,

    /// EXPERIMENTAL marker. When true (default), the handler emits a startup
    /// WARN log and sets `dfe_handler_experimental{handler="flow"} 1`.
    /// Flipped to false in a follow-up PR after stability period.
    #[serde(default = "yes")]
    pub experimental: bool,

    /// Raw-record capture override (inherits the common `raw_capture` block).
    ///
    /// `_raw` carries the decoder's verbatim record rendering: a JSON array
    /// of every record in `canonical` mode, the single record in `exploded`.
    #[serde(default)]
    pub raw_capture: crate::config::RawCaptureConfig,
}

fn default_bind() -> IpAddr {
    IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
}

fn default_ports() -> Vec<u16> {
    vec![2055, 4739, 6343]
}

fn default_channel_capacity() -> usize {
    4096
}

fn default_recv_buffer_bytes() -> usize {
    8 * 1024 * 1024
}

impl Default for FlowConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: default_bind(),
            ports: default_ports(),
            output: FlowOutputConfig::default(),
            channel_capacity: default_channel_capacity(),
            recv_buffer_bytes: default_recv_buffer_bytes(),
            ip_filter: None,
            rate_limit: RateLimitConfig::default(),
            netflow: NetflowSubConfig::default(),
            sflow: SflowSubConfig::default(),
            split: None,
            experimental: true,
            raw_capture: crate::config::RawCaptureConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct FlowSplitConfig {
    pub netflow: FlowListenerConfig,
    pub sflow: FlowListenerConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct FlowListenerConfig {
    #[serde(default = "yes")]
    pub enabled: bool,
    #[serde(default = "default_bind")]
    pub bind_address: IpAddr,
    pub ports: Vec<u16>,
    #[serde(default)]
    pub output: FlowOutputConfig,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "default_recv_buffer_bytes")]
    pub recv_buffer_bytes: usize,
    pub ip_filter: Option<IpFilterConfig>,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub template_cache: TemplateCacheConfig,
    pub topic: String,
}

impl FlowConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled && self.split.is_some() {
            return Err("flow.enabled and flow.split are mutually exclusive".into());
        }
        if let Some(split) = &self.split
            && split
                .netflow
                .ports
                .iter()
                .any(|p| split.sflow.ports.contains(p))
        {
            return Err("split.netflow.ports and split.sflow.ports must not overlap".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_unified_defaults() {
        let yaml = r"
enabled: true
";
        let cfg: FlowConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.ports, vec![2055, 4739, 6343]);
        assert_eq!(cfg.output.mode, OutputMode::Canonical);
        assert_eq!(cfg.output.max_records_per_packet, 200);
        assert!(cfg.netflow.enabled);
        assert!(cfg.sflow.enabled);
        assert!(cfg.split.is_none());
        assert!(cfg.experimental, "experimental defaults to true");
    }

    #[test]
    fn parses_explicit_exploded_mode() {
        let yaml = r"
enabled: true
output:
  mode: exploded
  max_records_per_packet: 500
";
        let cfg: FlowConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.output.mode, OutputMode::Exploded);
        assert_eq!(cfg.output.max_records_per_packet, 500);
    }

    #[test]
    fn parses_split_mode() {
        let yaml = r"
enabled: false
split:
  netflow:
    ports: [2055, 4739]
    topic: netflow_land
  sflow:
    ports: [6343]
    topic: sflow_land
";
        let cfg: FlowConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(!cfg.enabled);
        assert!(cfg.split.is_some());
        let split = cfg.split.unwrap();
        assert_eq!(split.netflow.ports, vec![2055, 4739]);
        assert_eq!(split.sflow.ports, vec![6343]);
    }

    #[test]
    fn validate_rejects_both_enabled_and_split() {
        let yaml = r"
enabled: true
split:
  netflow:
    ports: [2055]
    topic: x
  sflow:
    ports: [6343]
    topic: y
";
        let cfg: FlowConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_overlapping_split_ports() {
        let yaml = r"
enabled: false
split:
  netflow:
    ports: [2055, 6343]
    topic: n
  sflow:
    ports: [6343]
    topic: s
";
        let cfg: FlowConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn experimental_can_be_disabled() {
        let yaml = r"
enabled: true
experimental: false
";
        let cfg: FlowConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(!cfg.experimental);
    }

    #[test]
    fn output_mode_label_strings() {
        assert_eq!(OutputMode::Canonical.label(), "canonical");
        assert_eq!(OutputMode::parse("exploded"), Some(OutputMode::Exploded));
        assert_eq!(OutputMode::parse("CANONICAL"), Some(OutputMode::Canonical));
        // Removed in favour of flow.raw_capture.enabled -- must not resolve.
        assert_eq!(OutputMode::parse("canonical_with_raw"), None);
        assert_eq!(OutputMode::Exploded.label(), "exploded");
    }

    #[test]
    fn removed_canonical_with_raw_fails_the_yaml_parse_loudly() {
        // The breaking half of the flow fold: a config file still carrying the
        // old mode must stop the receiver rather than silently downgrade to
        // canonical and drop the raw records the operator asked for.
        let yaml = "
enabled: true
output:
  mode: canonical_with_raw
";
        let err = serde_yaml_ng::from_str::<FlowConfig>(yaml)
            .expect_err("removed variant must not deserialise");
        assert!(
            err.to_string().contains("canonical_with_raw"),
            "error should name the offending value: {err}"
        );
    }

    #[test]
    fn raw_capture_defaults_to_unset_so_it_inherits() {
        let cfg = FlowConfig::default();
        assert!(cfg.raw_capture.enabled.is_none());
        assert!(cfg.raw_capture.max_bytes.is_none());
    }

    #[test]
    fn raw_capture_parses_from_the_flow_block() {
        let yaml = "
enabled: true
raw_capture:
  enabled: true
  max_bytes: 2048
  on_oversize: omit
";
        let cfg: FlowConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.raw_capture.enabled, Some(true));
        assert_eq!(cfg.raw_capture.max_bytes, Some(2048));
        assert_eq!(
            cfg.raw_capture.on_oversize,
            Some(crate::config::OversizePolicy::Omit)
        );
    }
}
