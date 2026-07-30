// Project:   dfe-receiver
// File:      src/deployment.rs
// Purpose:   Deployment contract (SSoT for Docker, Helm, Compose artefacts)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract for dfe-receiver.
//!
//! Defines the single source of truth used by scalo's deployment generators
//! to produce Dockerfile, Helm chart, and Docker Compose fragments.

use scalo::deployment::{
    DeploymentContract, HealthContract, ImageProfile, KedaConfig, KedaContract, NativeDepsContract,
    OciLabels, PortContract, SecretEnvContract, SecretGroupContract, base_image_from_cascade,
};

/// Build the deployment contract for dfe-receiver.
///
/// Captures all deployment-facing configuration: ports, health paths,
/// secrets, KEDA scaling, and default config. Artefact generators
/// (`generate_dockerfile`, `generate_chart`, `generate_compose_fragment`)
/// use this contract as their single source of truth.
#[allow(clippy::too_many_lines)]
pub fn contract() -> DeploymentContract {
    // Resolve the base image via the scalo cascade helper so the org-wide
    // `deployment.base_image` override (config or env) wins before falling
    // back to scalo's DEFAULT_BASE_IMAGE (debian:trixie-slim). NEVER hardcode
    // a distro -- the old "ubuntu:24.04" pin predated the trixie cutover.
    let base_image = base_image_from_cascade();
    DeploymentContract {
        app_name: "dfe-receiver".into(),
        binary_name: "dfe-receiver".into(),
        native_deps: NativeDepsContract::for_scalo_features(
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
            &base_image,
        ),
        base_image,
        image_profile: ImageProfile::Production,
        description: "High-performance HTTP/gRPC receiver for PB/s scale data ingestion".into(),
        metrics_port: 9090,
        health: HealthContract {
            liveness_path: "/livez".into(),
            readiness_path: "/readyz".into(),
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
                "recv_buffer_bytes": 8_388_608,
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
        schema_version: 3,
        oci_labels: OciLabels {
            title: "dfe-receiver".into(),
            description: "High-performance HTTP/gRPC receiver for PB/s scale data ingestion".into(),
            // BUSL-1.1 drives the OCI `org.opencontainers.image.licenses`
            // label and the Dockerfile `# License` header. Copyright stays
            // the scalo default (the right HYPERI line).
            licenses: "BUSL-1.1".into(),
            ..OciLabels::default()
        },
        // KedaContract is #[non_exhaustive] (scalo): construct via
        // KedaConfig + From rather than a struct literal so future contract
        // fields stay non-breaking. The scaling_pressure_* trigger comes from
        // KedaConfig defaults (enabled=false, threshold=70) -- OFF: the
        // serverAddress is cluster-specific and must be set in values.yaml
        // before enabling, no runtime change here. The receiver pushes the
        // engine signals; an operator opts in per cluster.
        keda: Some(KedaContract::from_config(&KedaConfig {
            min_replicas: 1,
            max_replicas: 10,
            polling_interval: 15,
            cooldown_period: 120,
            kafka_lag_threshold: 10_000,
            activation_lag_threshold: 0,
            cpu_enabled: true,
            cpu_threshold: 80,
            ..Default::default()
        })),
        // Reflectable config (scalo-rs#6): the derived JSON Schema of the full
        // Config (all ingest protocols + destinations, secret fields marked
        // x-dfe-secret) plus a capability catalog of the ingest protocols the
        // receiver accepts and the destinations it writes to.
        config_schema: Some(scalo::deployment::config_schema_json::<crate::config::Config>()),
        capabilities: capabilities(),
    }
}

/// Capability catalog for dfe-receiver: the ingest protocols it accepts and the
/// output destinations it writes to. Grounded in `config::Config` -- the typed
/// per-protocol knobs live in the derived schema; this lists the protocols +
/// their maturity for the control plane's endpoint picker.
fn capabilities() -> Vec<scalo::deployment::Capability> {
    use scalo::deployment::Capability;
    let ingest = |name: &str, desc: &str| {
        Capability::new("ingest", name)
            .description(desc.to_string())
            .maturity("stable")
    };
    vec![
        Capability::source("receiver")
            .description("Multi-protocol ingest receiver: accepts data over many wire protocols and routes to Kafka / the loader.")
            .maturity("stable")
            .children(vec![
                ingest("http", "HTTP/JSON + NDJSON ingest (the ServerConfig endpoint)."),
                ingest("grpc", "gRPC ingest (scalo Vector-compatible push service)."),
                ingest("otlp", "OpenTelemetry OTLP logs/metrics/traces (feature-gated)."),
                ingest("lumberjack", "Elastic Beats / Lumberjack v2 frames."),
                ingest("splunk_hec", "Splunk HTTP Event Collector."),
                ingest("syslog", "Syslog RFC3164/RFC5424 over TCP/UDP/TLS."),
                ingest("prometheus_rw", "Prometheus Remote Write."),
                ingest("fluent", "Fluent Forward protocol."),
                ingest("gelf", "Graylog Extended Log Format."),
                ingest("flow", "NetFlow v5/v9 + IPFIX + sFlow v5."),
            ]),
        Capability::sink("destinations")
            .description("Output destinations the receiver routes accepted events to.")
            .maturity("stable")
            .children(vec![
                Capability::service("kafka").description("Kafka producer (the primary destination)."),
                Capability::service("loader").description("Direct gRPC connection to dfe-loader (broker-less low-latency path)."),
                Capability::service("file_sink").description("Debug file sink (writes processed messages to a file)."),
            ]),
    ]
}

/// Generate the Dockerfile from the deployment contract.
///
/// Thin wrapper over `scalo::deployment::generate_dockerfile`: the receiver
/// is a single-binary image with no consumer-side splice (unlike
/// dfe-transform-vector, which injects the upstream `vector` binary). Both
/// the `--emit-dockerfile` CLI path and the `checked_in_dockerfile_matches_emit_dockerfile`
/// drift guard call THIS function so there is one source of truth for the
/// checked-in `Dockerfile`. `None` = no contract-identity labels (those are
/// stamped by the CI-orchestrated invocation, not this one-off).
#[must_use]
pub fn emit_dockerfile() -> String {
    scalo::deployment::generate_dockerfile(&contract(), None)
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
    fn test_contract_carries_reflectable_config() {
        let c = contract();
        assert_eq!(c.schema_version, 3);
        assert!(c.config_schema.is_some());
        let recv = c
            .capabilities
            .iter()
            .find(|cap| cap.name == "receiver")
            .expect("receiver ingest capability");
        let protos: Vec<&str> = recv.children.iter().map(|s| s.name.as_str()).collect();
        assert!(protos.contains(&"syslog") && protos.contains(&"grpc") && protos.contains(&"flow"));
    }

    /// Committed reflectable artefacts under docs/ must not drift. Regenerate
    /// with `dfe-receiver config-schema --dir docs`.
    #[test]
    fn test_config_artifacts_do_not_drift() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("docs");
        scalo::deployment::assert_no_config_artifact_drift(&contract(), dir);
    }

    #[test]
    fn test_contract_base_image() {
        // The cascade helper resolves the org-wide `deployment.base_image`
        // override (config or env) when set, else falls back to scalo's
        // DEFAULT_BASE_IMAGE (debian:trixie-slim). In CI / local-dev with no
        // overrides the default applies. Assert it is non-empty and carries an
        // explicit tag -- never pin a distro here.
        let c = contract();
        assert!(!c.base_image.is_empty());
        assert!(
            c.base_image.contains(':'),
            "base_image must include an explicit tag: {}",
            c.base_image
        );
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

    #[test]
    fn emit_dockerfile_produces_valid_output() {
        let dockerfile = emit_dockerfile();
        // Base image is cascade-resolved (debian:trixie-slim by default), so
        // assert the FROM line matches the contract's resolved base_image
        // rather than pinning a distro.
        assert!(
            dockerfile.contains(&format!("FROM {}", contract().base_image)),
            "missing/incorrect base image FROM line in Dockerfile",
        );
        // BUSL-1.1 from the contract's oci_labels.licenses drives the header.
        assert!(
            dockerfile.contains("# License:   BUSL-1.1"),
            "Dockerfile missing BUSL-1.1 license header",
        );
        assert!(
            dockerfile.contains("COPY dfe-receiver /usr/local/bin/dfe-receiver"),
            "missing binary COPY in Dockerfile",
        );
    }

    #[test]
    fn checked_in_dockerfile_matches_emit_dockerfile() {
        // The checked-in Dockerfile is autogenerated from emit_dockerfile()
        // (the same function the `--emit-dockerfile` CLI path uses). If they
        // drift, CI publishes from a stale Dockerfile -- e.g. a base-image or
        // licence change in the contract that never got regenerated.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Dockerfile");
        let on_disk = std::fs::read_to_string(&path).expect("read Dockerfile");
        let emitted = emit_dockerfile();
        assert_eq!(
            on_disk.trim(),
            emitted.trim(),
            "Dockerfile on disk does not match emit_dockerfile() output -- \
             regenerate with: `dfe-receiver --emit-dockerfile Dockerfile`",
        );
    }
}
