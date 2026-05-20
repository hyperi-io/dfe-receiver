// Project:   dfe-receiver
// File:      src/deployment.rs
// Purpose:   Deployment contract (SSoT for Docker, Helm, Compose artefacts)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract for dfe-receiver.
//!
//! Defines the single source of truth used by rustlib's deployment generators
//! to produce Dockerfile, Helm chart, and Docker Compose fragments.

use hyperi_rustlib::deployment::{
    DeploymentContract, HealthContract, ImageProfile, KedaContract, NativeDepsContract, OciLabels,
    PortContract, SecretEnvContract, SecretGroupContract,
};

/// Build the deployment contract for dfe-receiver.
///
/// Captures all deployment-facing configuration: ports, health paths,
/// secrets, KEDA scaling, and default config. Artefact generators
/// (`generate_dockerfile`, `generate_chart`, `generate_compose_fragment`)
/// use this contract as their single source of truth.
#[allow(clippy::too_many_lines)]
pub fn contract() -> DeploymentContract {
    DeploymentContract {
        app_name: "dfe-receiver".into(),
        binary_name: "dfe-receiver".into(),
        base_image: "ubuntu:24.04".into(),
        native_deps: NativeDepsContract::for_rustlib_features(
            &[
                "config",
                "config-reload",
                "logger",
                "metrics",
                "http-server",
                "transport-kafka",
                "transport-grpc",
                "dlq-kafka",
                "spool",
                "tiered-sink",
                "runtime",
                "secrets",
                "scaling",
                "cli",
                "deployment",
            ],
            "ubuntu:24.04",
        ),
        image_profile: ImageProfile::Production,
        description: "High-performance HTTP/gRPC receiver for PB/s scale data ingestion".into(),
        metrics_port: 9090,
        health: HealthContract {
            liveness_path: "/health/live".into(),
            readiness_path: "/health/ready".into(),
            metrics_path: "/metrics".into(),
        },
        env_prefix: "DFE_RECEIVER".into(),
        metric_prefix: "receiver".into(),
        config_mount_path: "/etc/dfe-receiver/config.yaml".into(),
        image_registry: "ghcr.io/hyperi-io".into(),
        extra_ports: vec![
            PortContract {
                name: "http".into(),
                port: 8080,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "grpc".into(),
                port: 6000,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "otlp-grpc".into(),
                port: 4317,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "otlp-http".into(),
                port: 4318,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "beats".into(),
                port: 5044,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "hec".into(),
                port: 8088,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "prometheus-rw".into(),
                port: 9091,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "syslog".into(),
                port: 514,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "syslog-tls".into(),
                port: 6514,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "fluent".into(),
                port: 24224,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "gelf".into(),
                port: 12201,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "netflow".into(),
                port: 2055,
                protocol: "UDP".into(),
            },
            PortContract {
                name: "netflow-ipfix".into(),
                port: 4739,
                protocol: "UDP".into(),
            },
            PortContract {
                name: "sflow".into(),
                port: 6343,
                protocol: "UDP".into(),
            },
        ],
        entrypoint_args: vec!["--config".into(), "/etc/dfe-receiver/config.yaml".into()],
        secrets: vec![
            SecretGroupContract {
                group_name: "kafka".into(),
                env_vars: vec![
                    SecretEnvContract {
                        env_var: "DFE_RECEIVER__KAFKA__SASL__USERNAME".into(),
                        key_name: "username".into(),
                        secret_key: "kafka-username".into(),
                    },
                    SecretEnvContract {
                        env_var: "DFE_RECEIVER__KAFKA__SASL__PASSWORD".into(),
                        key_name: "password".into(),
                        secret_key: "kafka-password".into(),
                    },
                ],
            },
            SecretGroupContract {
                group_name: "auth".into(),
                env_vars: vec![SecretEnvContract {
                    env_var: "DFE_RECEIVER__SERVER__AUTH__BEARER__TOKENS".into(),
                    key_name: "bearer-tokens".into(),
                    secret_key: "bearer-tokens".into(),
                }],
            },
        ],
        default_config: Some(serde_json::json!({
            "server": {
                "bind_address": "0.0.0.0:8080",
                "max_body_size": 10_485_760,
                "request_timeout_ms": 30_000,
                "auth": { "mode": "none" }
            },
            "grpc": {
                "enabled": false,
                "bind_address": "0.0.0.0:6000"
            },
            "otlp": {
                "enabled": false,
                "grpc_bind_address": "0.0.0.0:4317",
                "http_bind_address": "0.0.0.0:4318",
                "mode": "hyperdx"
            },
            "lumberjack": {
                "enabled": false,
                "bind_address": "0.0.0.0:5044"
            },
            "splunk_hec": {
                "enabled": false,
                "bind_address": "0.0.0.0:8088"
            },
            "prometheus_rw": {
                "enabled": false,
                "bind_address": "0.0.0.0:9091"
            },
            "syslog": {
                "enabled": false,
                "udp_bind_address": "0.0.0.0:514",
                "tcp_bind_address": "0.0.0.0:514",
                "tls_bind_address": "0.0.0.0:6514"
            },
            "fluent": {
                "enabled": false,
                "bind_address": "0.0.0.0:24224"
            },
            "gelf": {
                "enabled": false,
                "bind_address": "0.0.0.0:12201"
            },
            "flow": {
                "enabled": false,
                "experimental": true,
                "bind_address": "0.0.0.0",
                "ports": [2055, 4739, 6343],
                "output": {
                    "mode": "canonical",
                    "max_records_per_packet": 200
                },
                "channel_capacity": 4096,
                "recv_buffer_bytes": 8388608,
                "rate_limit": {
                    "enabled": false,
                    "packets_per_second": 5000,
                    "burst": 10000,
                    "cache_size": 4096
                },
                "netflow": {
                    "enabled": true,
                    "template_cache": {
                        "max_per_exporter": 1000,
                        "max_exporters": 10000
                    },
                    "topic": "netflow_land"
                },
                "sflow": {
                    "enabled": true,
                    "topic": "sflow_land"
                }
            },
            "kafka": {
                "brokers": ["kafka:9092"],
                "client_id": "dfe-receiver",
                "producer": {
                    "compression": "zstd",
                    "acks": "all"
                }
            },
            "routing": {
                "default_source": "default",
                "topic_suffix": "_land"
            },
            "metrics": {
                "enabled": true,
                "address": "0.0.0.0:9090"
            }
        })),
        depends_on: vec!["kafka".into()],
        schema_version: 2,
        oci_labels: OciLabels {
            title: "dfe-receiver".into(),
            description: "High-performance HTTP/gRPC receiver for PB/s scale data ingestion".into(),
            ..OciLabels::default()
        },
        keda: Some(KedaContract {
            min_replicas: 1,
            max_replicas: 10,
            polling_interval: 15,
            cooldown_period: 120,
            kafka_lag_threshold: 10_000,
            activation_lag_threshold: 0,
            cpu_enabled: true,
            cpu_threshold: 80,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contract_fields() {
        let c = contract();
        assert_eq!(c.app_name, "dfe-receiver");
        assert_eq!(c.binary(), "dfe-receiver");
        assert_eq!(c.metrics_port, 9090);
        assert_eq!(c.env_prefix, "DFE_RECEIVER");
        assert_eq!(c.metric_prefix, "receiver");
        assert_eq!(c.config_mount_path, "/etc/dfe-receiver/config.yaml");
        assert_eq!(c.config_filename(), "config.yaml");
        assert_eq!(c.config_dir(), "/etc/dfe-receiver");
    }

    #[test]
    fn test_contract_ports() {
        let c = contract();
        assert_eq!(c.extra_ports.len(), 14);
        let port_names: Vec<&str> = c.extra_ports.iter().map(|p| p.name.as_str()).collect();
        assert!(port_names.contains(&"http"));
        assert!(port_names.contains(&"grpc"));
        assert!(port_names.contains(&"otlp-grpc"));
        assert!(port_names.contains(&"otlp-http"));
        assert!(port_names.contains(&"beats"));
        assert!(port_names.contains(&"hec"));
        assert!(port_names.contains(&"prometheus-rw"));
        assert!(port_names.contains(&"syslog"));
        assert!(port_names.contains(&"syslog-tls"));
        assert!(port_names.contains(&"fluent"));
        assert!(port_names.contains(&"gelf"));
        assert!(port_names.contains(&"netflow"));
        assert!(port_names.contains(&"netflow-ipfix"));
        assert!(port_names.contains(&"sflow"));
    }

    #[test]
    fn test_contract_secrets() {
        let c = contract();
        assert_eq!(c.secrets.len(), 2);
        assert_eq!(c.secrets[0].group_name, "kafka");
        assert_eq!(c.secrets[1].group_name, "auth");
    }

    #[test]
    fn test_contract_serialises() {
        let c = contract();
        let json = c.to_json();
        assert!(json.contains("dfe-receiver"));
        let yaml = c.to_yaml();
        assert!(yaml.contains("dfe-receiver"));
    }
}
