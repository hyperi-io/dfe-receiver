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
    DeploymentContract, HealthContract, ImageProfile, KafkaLagTrigger, KedaConfig, KedaContract,
    NativeDepsContract, OciLabels, PortContract, SecretEnvContract, SecretGroupContract,
    base_image_from_cascade,
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
        // Every listener but HTTP binds only while its section's `enabled` switch is on.
        extra_ports: vec![
            PortContract::tcp("http", 8080).bound_from("server.bind_address"),
            PortContract::tcp("grpc", 6000)
                .when_equals("config.grpc.enabled", "true")
                .bound_from("grpc.bind_address"),
            PortContract::tcp("otlp-grpc", 4317)
                .when_equals("config.otlp.enabled", "true")
                .bound_from("otlp.grpc_bind_address"),
            PortContract::tcp("otlp-http", 4318)
                .when_equals("config.otlp.enabled", "true")
                .bound_from("otlp.http_bind_address"),
            PortContract::tcp("beats", 5044)
                .when_equals("config.lumberjack.enabled", "true")
                .bound_from("lumberjack.bind_address"),
            PortContract::tcp("hec", 8088)
                .when_equals("config.splunk_hec.enabled", "true")
                .bound_from("splunk_hec.bind_address"),
            PortContract::tcp("prometheus-rw", 9091)
                .when_equals("config.prometheus_rw.enabled", "true")
                .bound_from("prometheus_rw.bind_address"),
            PortContract::tcp("webhook", 8090)
                .when_equals("config.webhook.enabled", "true")
                .bound_from("webhook.bind_address"),
            PortContract::tcp("syslog", 514)
                .when_equals("config.syslog.enabled", "true")
                .bound_from("syslog.tcp_bind_address"),
            PortContract::udp("syslog-udp", 514)
                .when_equals("config.syslog.enabled", "true")
                .bound_from("syslog.udp_bind_address"),
            // Binds only when syslog.tls.enabled is also on; one gate path cannot say both.
            PortContract::tcp("syslog-tls", 6514)
                .when_equals("config.syslog.enabled", "true")
                .bound_from("syslog.tls_bind_address"),
            PortContract::tcp("fluent", 24224)
                .when_equals("config.fluent.enabled", "true")
                .bound_from("fluent.bind_address"),
            PortContract::tcp("gelf", 12201)
                .when_equals("config.gelf.enabled", "true")
                .bound_from("gelf.bind_address"),
            // Unified flow mode only: split mode binds its own operator-chosen ports.
            PortContract::udp("netflow", 2055)
                .when_equals("config.flow.enabled", "true")
                .bound_from("flow.bind_address"),
            PortContract::udp("netflow-ipfix", 4739)
                .when_equals("config.flow.enabled", "true")
                .bound_from("flow.bind_address"),
            PortContract::udp("sflow", 6343)
                .when_equals("config.flow.enabled", "true")
                .bound_from("flow.bind_address"),
        ],
        unbound_listen_paths: vec![],
        entrypoint_args: vec!["--config".into(), "/etc/dfe-receiver/config.yaml".into()],
        secrets: vec![
            SecretGroupContract {
                group_name: "kafka".into(),
                env_vars: vec![
                    // apply_flat_env reads these; flat_env joins prefix and key
                    // with ONE underscore, and the key is USER, not USERNAME.
                    SecretEnvContract {
                        env_var: "DFE_RECEIVER_KAFKA_SASL_USER".into(),
                        key_name: "username".into(),
                        secret_key: "kafka-username".into(),
                    },
                    SecretEnvContract {
                        env_var: "DFE_RECEIVER_KAFKA_SASL_PASSWORD".into(),
                        key_name: "password".into(),
                        secret_key: "kafka-password".into(),
                    },
                ],
            },
            SecretGroupContract {
                group_name: "auth".into(),
                // apply_flat_env reads this one; flat_env joins the prefix and
                // the key with ONE underscore. entrypoint_args pass --config,
                // which reads the receiver's own sections from the file and
                // apply_flat_env, never from the cascade's double-underscore form.
                env_vars: vec![SecretEnvContract {
                    env_var: "DFE_RECEIVER_BEARER_TOKENS".into(),
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
            "webhook": {
                "enabled": false,
                "bind_address": "0.0.0.0:8090"
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
                "client_id": "dfe-receiver"
            },
            "routing": {
                "default_source": "main",
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
        keda: Some(
            KedaContract::from_config(&KedaConfig {
                min_replicas: 1,
                max_replicas: 10,
                polling_interval: 15,
                cooldown_period: 120,
                cpu_enabled: true,
                cpu_threshold: 80,
                ..Default::default()
            })
            // The receiver consumes no topic, so consumer lag says nothing about its load.
            .with_kafka_trigger(KafkaLagTrigger::disabled()),
        ),
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
                ingest("webhook", "Generic authenticated webhook intake: POST /webhook/{caller}, per-caller HMAC or static-header auth, per-caller topic."),
            ]),
        Capability::sink("destinations")
            .description("The named destination set: a match rule sends accepted events to one destination or fans them out to several.")
            .maturity("stable")
            .children(vec![
                Capability::service("kafka").description("The bus, under the topic the event's source resolves to."),
                Capability::service("loader").description("dfe-loader, over gRPC or the bus per the loader block."),
                Capability::service("grpc").description("Any declared scalo Push listener -- a transform, the archiver -- by name."),
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
    use crate::config::Config;
    use scalo::config::flat_env::ApplyFlatEnv;

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

    /// A consumer of the schema masks a field only when it carries the secret marker.
    #[test]
    fn credential_fields_carry_the_secret_marker() {
        let schema = scalo::deployment::config_schema_json::<crate::config::Config>();
        for (def, field) in [
            ("BearerConfig", "tokens"),
            ("AcceptedHeader", "values"),
            ("AuthConfig", "header_values"),
        ] {
            let items = &schema["$defs"][def]["properties"][field]["items"];
            assert_eq!(
                items["x-dfe-secret"],
                serde_json::Value::Bool(true),
                "{def}.{field}"
            );
        }
        assert_eq!(
            schema["$defs"]["SaslConfig"]["properties"]["password"]["x-dfe-secret"],
            serde_json::Value::Bool(true),
            "SaslConfig.password"
        );
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
        assert_eq!(c.extra_ports.len(), 16);
        let port_names: Vec<&str> = c.extra_ports.iter().map(|p| p.name.as_str()).collect();
        assert!(port_names.contains(&"http"));
        assert!(port_names.contains(&"grpc"));
        assert!(port_names.contains(&"otlp-grpc"));
        assert!(port_names.contains(&"otlp-http"));
        assert!(port_names.contains(&"beats"));
        assert!(port_names.contains(&"hec"));
        assert!(port_names.contains(&"prometheus-rw"));
        assert!(port_names.contains(&"webhook"));
        assert!(port_names.contains(&"syslog"));
        assert!(port_names.contains(&"syslog-udp"));
        assert!(port_names.contains(&"syslog-tls"));
        assert!(port_names.contains(&"fluent"));
        assert!(port_names.contains(&"gelf"));
        assert!(port_names.contains(&"netflow"));
        assert!(port_names.contains(&"netflow-ipfix"));
        assert!(port_names.contains(&"sflow"));

        let udp: Vec<&str> = c
            .extra_ports
            .iter()
            .filter(|p| p.protocol == "UDP")
            .map(|p| p.name.as_str())
            .collect();
        assert_eq!(udp, ["syslog-udp", "netflow", "netflow-ipfix", "sflow"]);
    }

    /// `generate-artefacts` and `generate_chart` write nothing for a contract
    /// that fails these checks.
    #[test]
    fn test_contract_passes_the_generate_artefacts_checks() {
        let c = contract();
        c.validate()
            .expect("every generator must accept the contract");
        scalo::deployment::assert_listeners_declared(&c);
        let unresolved = c.unresolved_values_paths();
        assert!(
            unresolved.is_empty(),
            "the chart reads values default_config never sets: {unresolved:?}"
        );
    }

    /// Each port follows the switch of the section it serves: off in the
    /// published default, on once the binary itself reads that switch as on.
    #[test]
    fn test_every_gated_port_follows_its_own_section_switch() {
        let c = contract();
        let published = c.default_config.clone().expect("default_config present");
        for port in &c.extra_ports {
            let section = port
                .bound_from
                .as_deref()
                .and_then(|path| path.split('.').next())
                .expect("every port names the listener it serves");
            let Some(gate) = port.when.as_ref() else {
                assert_eq!(
                    section, "server",
                    "{} is published unconditionally",
                    port.name
                );
                continue;
            };
            assert_eq!(
                gate.path(),
                format!("config.{section}.enabled"),
                "{} is gated on another section's switch",
                port.name
            );
            assert_eq!(
                gate.holds_in(&published),
                Some(false),
                "{} is published by the default install",
                port.name
            );

            let mut on = published.clone();
            on[section]["enabled"] = serde_json::json!(true);
            assert_eq!(
                gate.holds_in(&on),
                Some(true),
                "{} stays unpublished with its switch on",
                port.name
            );
            let read: Config =
                serde_json::from_value(on).expect("published default deserialises into Config");
            let read = serde_json::to_value(&read).expect("config serialises");
            assert_eq!(
                read[section]["enabled"],
                serde_json::json!(true),
                "the binary does not read {section}.enabled, so {} gates on nothing",
                port.name
            );
        }
    }

    /// KEDA stays on with CPU as its only trigger: the receiver consumes no
    /// topic, so a consumer-lag trigger has nothing to read.
    #[test]
    fn test_keda_has_no_kafka_lag_trigger() {
        let c = contract();
        let keda = c.keda.as_ref().expect("keda contract");
        assert!(keda.enabled);
        assert!(keda.cpu_enabled);
        assert_eq!(keda.cpu_threshold, 80);
        assert!(!keda.kafka_trigger.enabled, "a raw-lag trigger is declared");
        assert!(keda.min_replicas >= 1, "CPU alone cannot scale from zero");
    }

    #[test]
    fn test_contract_secrets() {
        let c = contract();
        assert_eq!(c.secrets.len(), 2);
        assert_eq!(c.secrets[0].group_name, "kafka");
        assert_eq!(c.secrets[1].group_name, "auth");
    }

    /// Every env var the contract declares must reach the config.
    ///
    /// The chart is generated from these names, so one the app does not read
    /// mounts a Secret into the pod environment and is ignored, with nothing
    /// failing to say so.
    #[test]
    fn every_declared_secret_env_var_reaches_the_config() {
        // A declared name the binary never reads mounts a Secret that is
        // silently ignored; the per-field mapping is pinned by the config tests.
        for group in &contract().secrets {
            for env in &group.env_vars {
                assert!(
                    env.env_var
                        .starts_with(&format!("{}_", crate::config::ENV_PREFIX)),
                    "{} does not carry the app prefix",
                    env.env_var
                );

                let sentinel = format!("sentinel-{}", env.key_name);
                let mut config = Config::default();
                temp_env::with_var(&env.env_var, Some(sentinel.as_str()), || {
                    config.apply_flat_env(crate::config::ENV_PREFIX);
                });

                // Secrets serialise redacted outside `expose_during`.
                let applied = scalo::expose_during(|| serde_json::to_string(&config))
                    .expect("config serialises");
                assert!(
                    applied.contains(&sentinel),
                    "{} ({}) was set and no config field read it",
                    env.env_var,
                    group.group_name
                );
            }
        }
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

    /// Map a chart directory to relative path -> file body.
    fn chart_files(root: &std::path::Path) -> std::collections::BTreeMap<String, String> {
        let mut files = std::collections::BTreeMap::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read_dir") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    let rel = path.strip_prefix(root).expect("relative path");
                    let body = std::fs::read_to_string(&path).expect("read chart file");
                    files.insert(rel.display().to_string(), body);
                }
            }
        }
        files
    }

    #[test]
    fn checked_in_chart_matches_generate_chart() {
        // The chart is emitted from contract(), so a hand edit here is silently
        // reverted the next time anything regenerates it.
        const REGEN: &str = "regenerate with: `dfe-receiver --emit-helm chart`";

        let tmp = tempfile::tempdir().expect("tempdir");
        scalo::deployment::generate_chart(&contract(), tmp.path(), None).expect("generate_chart");
        let expected = chart_files(tmp.path());
        let committed =
            chart_files(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("chart"));

        let expected_names: Vec<&String> = expected.keys().collect();
        let committed_names: Vec<&String> = committed.keys().collect();
        assert_eq!(
            committed_names, expected_names,
            "chart/ file list differs from generate_chart() -- {REGEN}"
        );
        for (name, want) in &expected {
            assert_eq!(
                committed.get(name),
                Some(want),
                "chart/{name} differs from generate_chart() -- {REGEN}"
            );
        }
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
