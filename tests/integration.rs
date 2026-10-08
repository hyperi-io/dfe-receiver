// Project:   dfe-receiver
// File:      tests/integration.rs
// Purpose:   Single-binary integration test -- all protocol + security tests as submodules
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests consolidated into a single binary for compile-time efficiency.
//!
//! Each `tests/*.rs` file compiles as a separate binary (separate link cycle).
//! One entry point with submodules = 1 link cycle = ~3x faster test compilation.

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

#[path = "integration/config_dotenv.rs"]
mod config_dotenv;
// A leak check has to fail the test when the container is still there, and the
// poll loop it sits after cannot express that as an assert.
#[allow(clippy::panic)]
#[path = "integration/container_hygiene.rs"]
mod container_hygiene;
#[path = "integration/flow_corpus.rs"]
mod flow_corpus;
#[path = "integration/flow_netflow_e2e.rs"]
mod flow_netflow_e2e;
#[path = "integration/flow_sflow_e2e.rs"]
mod flow_sflow_e2e;
#[path = "integration/fluent.rs"]
mod fluent;
#[path = "integration/gelf.rs"]
mod gelf;
#[path = "integration/grpc_outage.rs"]
mod grpc_outage;
#[path = "integration/grpc_sink.rs"]
mod grpc_sink;
#[path = "integration/http_ingest.rs"]
mod http_ingest;
#[path = "integration/http_security.rs"]
mod http_security;
#[path = "integration/json_depth.rs"]
mod json_depth;
#[path = "integration/json_only.rs"]
mod json_only;
#[path = "integration/kafka_sink.rs"]
mod kafka_sink;
#[path = "integration/listener_admission.rs"]
mod listener_admission;
#[path = "integration/listener_failures.rs"]
mod listener_failures;
#[path = "integration/lumberjack.rs"]
mod lumberjack;
#[path = "integration/named_destinations.rs"]
mod named_destinations;
#[path = "integration/otlp.rs"]
mod otlp;
#[path = "integration/prometheus_rw.rs"]
mod prometheus_rw;
#[path = "integration/protocol_kafka_roundtrip.rs"]
mod protocol_kafka_roundtrip;
#[path = "integration/raw_capture.rs"]
mod raw_capture;
#[path = "integration/record_counts.rs"]
mod record_counts;
#[path = "integration/refused_records.rs"]
mod refused_records;
#[path = "integration/source_routing.rs"]
mod source_routing;
#[path = "integration/splunk_hec.rs"]
mod splunk_hec;
#[path = "integration/syslog.rs"]
mod syslog;
#[path = "integration/vault_auth.rs"]
mod vault_auth;
#[path = "integration/vector.rs"]
mod vector;
#[path = "integration/webhook.rs"]
mod webhook;
