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
//! to produce the Dockerfile and Docker Compose fragments, and that the
//! release emits for the thin chart it assembles on the scalo-service library
//! chart.

use scalo::deployment::{
    CONTRACT_SCHEMA_VERSION, DeploymentContract, HealthContract, ImageProfile, KafkaLagTrigger,
    KedaConfig, KedaContract, NativeDepsContract, OciLabels, PortCondition, PortContract,
    ResourceList, ResourcesContract, SecretEnvContract, SecretGroupContract, SecurityContract,
    WritablePath, base_image_from_cascade,
};

use crate::config::{DEFAULT_DLQ_FILE_PATH, DEFAULT_SPILLOVER_PATH};

/// Build the deployment contract for dfe-receiver.
///
/// Captures all deployment-facing configuration: ports, health paths,
/// secrets, writable paths, resources, KEDA scaling, and default config.
/// Artefact generators (`generate_dockerfile`, `generate_chart`,
/// `generate_compose_fragment`) and the released thin chart use this
/// contract as their single source of truth.
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
            startup_budget_seconds: 60,
            ..HealthContract::default()
        },
        env_prefix: "DFE_RECEIVER".into(),
        metric_prefix: "receiver".into(),
        config_mount_path: "/etc/dfe-receiver/config.yaml".into(),
        image_registry: "ghcr.io/hyperi-io".into(),
        // Every listener but HTTP binds only while its section's `enabled` switch
        // is on. The ingest listeners are public, and gRPC is the in-cluster hop.
        extra_ports: vec![
            PortContract::tcp("http", 8080)
                .bound_from("server.bind_address")
                .public(),
            // Cleartext gRPC, so a proxy in front of it must speak h2c.
            PortContract::tcp("grpc", 6000)
                .when_equals("config.grpc.enabled", "true")
                .bound_from("grpc.bind_address")
                .app_protocol("kubernetes.io/h2c"),
            PortContract::tcp("otlp-grpc", 4317)
                .when_equals("config.otlp.enabled", "true")
                .bound_from("otlp.grpc_bind_address")
                .public(),
            PortContract::tcp("otlp-http", 4318)
                .when_equals("config.otlp.enabled", "true")
                .bound_from("otlp.http_bind_address")
                .public(),
            PortContract::tcp("beats", 5044)
                .when_equals("config.lumberjack.enabled", "true")
                .bound_from("lumberjack.bind_address")
                .public(),
            PortContract::tcp("hec", 8088)
                .when_equals("config.splunk_hec.enabled", "true")
                .bound_from("splunk_hec.bind_address")
                .public(),
            PortContract::tcp("prometheus-rw", 9091)
                .when_equals("config.prometheus_rw.enabled", "true")
                .bound_from("prometheus_rw.bind_address")
                .public(),
            PortContract::tcp("webhook", 8090)
                .when_equals("config.webhook.enabled", "true")
                .bound_from("webhook.bind_address")
                .public(),
            PortContract::tcp("syslog", 514)
                .when_equals("config.syslog.enabled", "true")
                .bound_from("syslog.tcp_bind_address")
                .public(),
            PortContract::udp("syslog-udp", 514)
                .when_equals("config.syslog.enabled", "true")
                .bound_from("syslog.udp_bind_address")
                .public(),
            // Binds only when syslog.tls.enabled is also on; one gate path cannot say both.
            PortContract::tcp("syslog-tls", 6514)
                .when_equals("config.syslog.enabled", "true")
                .bound_from("syslog.tls_bind_address")
                .public(),
            PortContract::tcp("fluent", 24224)
                .when_equals("config.fluent.enabled", "true")
                .bound_from("fluent.bind_address")
                .public(),
            PortContract::tcp("gelf", 12201)
                .when_equals("config.gelf.enabled", "true")
                .bound_from("gelf.bind_address")
                .public(),
            // Unified flow mode only: split mode binds its own operator-chosen ports.
            PortContract::udp("netflow", 2055)
                .when_equals("config.flow.enabled", "true")
                .bound_from("flow.bind_address")
                .public(),
            PortContract::udp("netflow-ipfix", 4739)
                .when_equals("config.flow.enabled", "true")
                .bound_from("flow.bind_address")
                .public(),
            PortContract::udp("sflow", 6343)
                .when_equals("config.flow.enabled", "true")
                .bound_from("flow.bind_address")
                .public(),
        ],
        unbound_listen_paths: vec![],
        entrypoint_args: vec!["--config".into(), "/etc/dfe-receiver/config.yaml".into()],
        secrets: secrets(),
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
        schema_version: CONTRACT_SCHEMA_VERSION,
        oci_labels: OciLabels {
            title: "dfe-receiver".into(),
            description: "High-performance HTTP/gRPC receiver for PB/s scale data ingestion".into(),
            vendor: "HYPERI PTY LIMITED".into(),
            label_namespace: "io.hyperi".into(),
            // Drive the OCI licence label and the Dockerfile `# License:` and `# Copyright:` headers.
            licenses: "BUSL-1.1".into(),
            copyright: "(c) 2026 HYPERI PTY LIMITED".into(),
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
        // x-scalo-secret) plus a capability catalog of the ingest protocols the
        // receiver accepts and the destinations it writes to.
        config_schema: Some(scalo::deployment::config_schema_json::<crate::config::Config>()),
        capabilities: capabilities(),
        // The spool and the file DLQ: the directories the app writes under a read-only root.
        writable_paths: vec![
            WritablePath::new("spool", DEFAULT_SPILLOVER_PATH)
                .size_limit("10Gi")
                .when(PortCondition::Equals {
                    path: "config.buffer.spillover.enabled".into(),
                    value: "true".into(),
                }),
            // Ungated: three dlq settings decide the file backend, and a gate reads one path.
            WritablePath::new("dlq", DEFAULT_DLQ_FILE_PATH).size_limit("1Gi"),
        ],
        termination_grace_seconds: 45,
        resources: ResourcesContract {
            requests: ResourceList {
                cpu: "200m".into(),
                memory: "256Mi".into(),
            },
            limits: ResourceList {
                cpu: "1".into(),
                memory: "512Mi".into(),
            },
        },
        security: SecurityContract::default(),
        singleton: false,
    }
}

/// The Secrets the chart mounts as env vars.
///
/// `Config::apply_flat_env` reads these names: flat env joins the prefix and
/// the key with ONE underscore, and the user key is `USER`, not `USERNAME`.
/// `entrypoint_args` pass `--config`, which never reads the cascade's
/// double-underscore form. The `auth` group is optional because only bearer
/// auth reads it.
fn secrets() -> Vec<SecretGroupContract> {
    let env = |env_var: &str, key_name: &str, secret_key: &str| SecretEnvContract {
        env_var: env_var.into(),
        key_name: key_name.into(),
        secret_key: secret_key.into(),
    };
    vec![
        SecretGroupContract::new(
            "kafka",
            vec![
                env("DFE_RECEIVER_KAFKA_SASL_USER", "username", "kafka-username"),
                env(
                    "DFE_RECEIVER_KAFKA_SASL_PASSWORD",
                    "password",
                    "kafka-password",
                ),
                env(
                    "DFE_RECEIVER_KAFKA_SASL_MECHANISM",
                    "mechanism",
                    "kafka-sasl-mechanism",
                ),
            ],
        ),
        SecretGroupContract::new(
            "auth",
            vec![env(
                "DFE_RECEIVER_BEARER_TOKENS",
                "bearer-tokens",
                "bearer-tokens",
            )],
        )
        .optional(),
    ]
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
    use scalo::dlq::DlqMode;

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
        assert_eq!(c.schema_version, CONTRACT_SCHEMA_VERSION);
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
                items["x-scalo-secret"],
                serde_json::Value::Bool(true),
                "{def}.{field}"
            );
        }
        assert_eq!(
            schema["$defs"]["SaslConfig"]["properties"]["password"]["x-scalo-secret"],
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
        assert_ne!(c.base_image, "");
        assert!(
            c.base_image.contains(':'),
            "base_image must include an explicit tag: {}",
            c.base_image
        );
    }

    /// A deployment names these ports to wire its own listeners, so a rename
    /// breaks every deployment that refers to the old name.
    #[test]
    fn test_contract_ports() {
        let c = contract();
        let port_names: Vec<&str> = c.extra_ports.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            port_names,
            [
                "http",
                "grpc",
                "otlp-grpc",
                "otlp-http",
                "beats",
                "hec",
                "prometheus-rw",
                "webhook",
                "syslog",
                "syslog-udp",
                "syslog-tls",
                "fluent",
                "gelf",
                "netflow",
                "netflow-ipfix",
                "sflow",
            ]
        );

        let udp: Vec<&str> = c
            .extra_ports
            .iter()
            .filter(|p| p.protocol == "UDP")
            .map(|p| p.name.as_str())
            .collect();
        assert_eq!(udp, ["syslog-udp", "netflow", "netflow-ipfix", "sflow"]);
    }

    /// The public ports are the ingest surface a load balancer may expose, so
    /// the in-cluster gRPC hop stays off it.
    #[test]
    fn every_ingest_port_is_public_and_grpc_is_not() {
        let c = contract();
        let internal: Vec<&str> = c
            .extra_ports
            .iter()
            .filter(|p| !p.public)
            .map(|p| p.name.as_str())
            .collect();
        assert_eq!(internal, ["grpc"]);

        let grpc = c
            .extra_ports
            .iter()
            .find(|p| p.name == "grpc")
            .expect("the gRPC listener is a declared port");
        // The gRPC listener is cleartext, so a proxy in front of it must speak h2c.
        assert_eq!(grpc.app_protocol, "kubernetes.io/h2c");
        for port in c.extra_ports.iter().filter(|p| p.name != "grpc") {
            assert_eq!(port.app_protocol, "", "{} names an app protocol", port.name);
        }
    }

    /// The root filesystem is read-only, so the default spool directory must
    /// sit under a writable path mounted exactly while spillover is on.
    #[test]
    fn the_spool_has_somewhere_to_write_while_spillover_is_on() {
        let c = contract();
        assert!(c.security.read_only_root_filesystem);

        let spool = &Config::default().buffer.spillover.path;
        let spillover_on = PortCondition::Equals {
            path: "config.buffer.spillover.enabled".into(),
            value: "true".into(),
        };
        let covering: Vec<&WritablePath> = c
            .writable_paths
            .iter()
            .filter(|writable| spool.starts_with(&writable.path))
            .collect();
        assert_eq!(covering.len(), 1, "{:?}", c.writable_paths);
        assert_eq!(covering[0].when.as_ref(), Some(&spillover_on));
        assert!(!covering[0].persistent);
        assert_eq!(covering[0].size_limit, "10Gi");

        // The gate reads the same switch the binary reads.
        let mut on = c.default_config.clone().expect("default_config present");
        on["buffer"] = serde_json::json!({ "spillover": { "enabled": true } });
        assert_eq!(spillover_on.holds_in(&on), Some(true));
        let read: Config = serde_json::from_value(on).expect("deserialises into Config");
        assert!(read.buffer.spillover.enabled);
    }

    /// The root filesystem is read-only, so every DLQ mode that writes the file
    /// backend needs a writable path over the DLQ directory, mounted while that
    /// mode is set.
    #[test]
    fn every_file_dlq_mode_has_somewhere_to_write() {
        let c = contract();
        assert!(c.security.read_only_root_filesystem);
        let published = c.default_config.clone().expect("default_config present");

        // The modes scalo builds a file backend for. An unknown mode runs as cascade.
        let file_modes = [DlqMode::Cascade, DlqMode::FanOut, DlqMode::FileOnly];
        // `None` is the published default, which sets no mode at all.
        let modes = [
            None,
            Some("cascade"),
            Some("fan_out"),
            Some("file_only"),
            Some("kafka_only"),
            Some(""),
            Some("not-a-mode"),
        ];
        let mut writing = Vec::new();
        for mode in modes {
            let mut values = published.clone();
            if let Some(mode) = mode {
                values["routing"]["dlq"] = serde_json::json!({ "mode": mode });
            }
            let read: Config =
                serde_json::from_value(values.clone()).expect("deserialises into Config");
            let dlq = read.routing.dlq.to_scalo_config();
            if !(dlq.enabled && dlq.file.enabled && file_modes.contains(&dlq.mode)) {
                continue;
            }
            writing.push(mode);

            let mounted: Vec<&WritablePath> = c
                .writable_paths
                .iter()
                .filter(|writable| dlq.file.path.starts_with(&writable.path))
                .filter(|writable| {
                    writable
                        .when
                        .as_ref()
                        .is_none_or(|gate| gate.holds_in(&values) == Some(true))
                })
                .collect();
            assert_eq!(
                mounted.len(),
                1,
                "dlq.mode {mode:?}: {:?}",
                c.writable_paths
            );
            assert!(!mounted[0].persistent);
        }
        assert_eq!(
            writing,
            [
                None,
                Some("cascade"),
                Some("fan_out"),
                Some("file_only"),
                Some(""),
                Some("not-a-mode")
            ]
        );
    }

    /// The startup probe, the grace period and the pod's resources.
    #[test]
    fn test_contract_pod_settings() {
        let c = contract();
        assert_eq!(c.health.startup_budget_seconds, 60);
        assert_eq!(c.termination_grace_seconds, 45);
        assert_eq!(c.resources.requests.cpu, "200m");
        assert_eq!(c.resources.requests.memory, "256Mi");
        assert_eq!(c.resources.limits.cpu, "1");
        assert_eq!(c.resources.limits.memory, "512Mi");
        assert!(!c.singleton);
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

    /// The pod cannot produce without its broker credentials, so the Kafka
    /// group is required. Bearer tokens matter only under bearer auth.
    #[test]
    fn test_contract_secrets() {
        let c = contract();
        let groups: Vec<(&str, bool)> = c
            .secrets
            .iter()
            .map(|g| (g.group_name.as_str(), g.optional))
            .collect();
        assert_eq!(groups, [("kafka", false), ("auth", true)]);

        let kafka: Vec<(&str, &str, &str)> = c.secrets[0]
            .env_vars
            .iter()
            .map(|e| {
                (
                    e.env_var.as_str(),
                    e.key_name.as_str(),
                    e.secret_key.as_str(),
                )
            })
            .collect();
        assert_eq!(
            kafka,
            [
                ("DFE_RECEIVER_KAFKA_SASL_USER", "username", "kafka-username"),
                (
                    "DFE_RECEIVER_KAFKA_SASL_PASSWORD",
                    "password",
                    "kafka-password"
                ),
                (
                    "DFE_RECEIVER_KAFKA_SASL_MECHANISM",
                    "mechanism",
                    "kafka-sasl-mechanism"
                ),
            ]
        );
    }

    /// The config field each Secret env var the contract declares must fill.
    ///
    /// The flat names do not spell their fields (`..._SASL_USER` fills
    /// `kafka.sasl.username`), so the field is named here rather than derived.
    const SECRET_FIELDS: &[(&str, &str)] = &[
        ("DFE_RECEIVER_KAFKA_SASL_USER", "/kafka/sasl/username"),
        ("DFE_RECEIVER_KAFKA_SASL_PASSWORD", "/kafka/sasl/password"),
        ("DFE_RECEIVER_KAFKA_SASL_MECHANISM", "/kafka/sasl/mechanism"),
        ("DFE_RECEIVER_BEARER_TOKENS", "/server/auth/bearer/tokens/0"),
    ];

    /// Every Secret env var the contract declares lands on the one config
    /// field it fills.
    ///
    /// The chart mounts a Secret under every declared name, so a name the
    /// config never reads leaves the credential silently unused, and a name
    /// read into another field connects with the wrong value.
    #[test]
    fn every_declared_secret_env_var_reaches_the_config() {
        let contract = contract();
        let declared: std::collections::BTreeSet<&str> = contract
            .secrets
            .iter()
            .flat_map(|group| group.env_vars.iter().map(|env| env.env_var.as_str()))
            .collect();
        let named: std::collections::BTreeSet<&str> =
            SECRET_FIELDS.iter().map(|(env_var, _)| *env_var).collect();
        assert_eq!(
            declared, named,
            "every env var the contract declares needs its field in SECRET_FIELDS"
        );

        for group in &contract.secrets {
            for env in &group.env_vars {
                let field = SECRET_FIELDS
                    .iter()
                    .find(|(env_var, _)| *env_var == env.env_var)
                    .map(|(_, field)| *field)
                    .expect("checked against SECRET_FIELDS above");
                let sentinel = format!("sentinel-{}", env.key_name);
                let mut config = Config::default();
                temp_env::with_var(&env.env_var, Some(sentinel.as_str()), || {
                    config.apply_flat_env(&contract.env_prefix);
                });

                // A credential field redacts on every other serialise path.
                let applied = scalo::expose_during(|| serde_json::to_value(&config))
                    .expect("config serialises");
                let reached = applied.pointer(field).and_then(serde_json::Value::as_str)
                    == Some(sentinel.as_str());
                // The message carries the env var and group names only, never a value.
                assert!(
                    reached,
                    "{} ({}) was set and the config field it fills did not read it",
                    env.env_var, group.group_name
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
