// Project:   dfe-receiver
// File:      tests/integration.rs
// Purpose:   Single-binary integration test — all protocol + security tests as submodules
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
#[path = "integration/grpc_sink.rs"]
mod grpc_sink;
#[path = "integration/http_security.rs"]
mod http_security;
#[path = "integration/kafka_sink.rs"]
mod kafka_sink;
#[path = "integration/lumberjack.rs"]
mod lumberjack;
#[path = "integration/minio_spool.rs"]
mod minio_spool;
#[path = "integration/otlp.rs"]
mod otlp;
#[path = "integration/prometheus_rw.rs"]
mod prometheus_rw;
#[path = "integration/protocol_kafka_roundtrip.rs"]
mod protocol_kafka_roundtrip;
#[path = "integration/splunk_hec.rs"]
mod splunk_hec;
#[path = "integration/syslog.rs"]
mod syslog;
#[path = "integration/vault_auth.rs"]
mod vault_auth;
#[path = "integration/vector.rs"]
mod vector;
