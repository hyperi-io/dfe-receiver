// Project:   dfe-receiver
// File:      tests/integration.rs
// Purpose:   Single-binary integration test — all protocol + security tests as submodules
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests consolidated into a single binary for compile-time efficiency.
//!
//! Each `tests/*.rs` file compiles as a separate binary (separate link cycle).
//! One entry point with submodules = 1 link cycle = ~3x faster test compilation.

#[path = "common/mod.rs"]
mod common;

#[path = "integration/fluent.rs"]
mod fluent;
#[path = "integration/gelf.rs"]
mod gelf;
#[path = "integration/http_security.rs"]
mod http_security;
#[path = "integration/lumberjack.rs"]
mod lumberjack;
#[path = "integration/otlp.rs"]
mod otlp;
#[path = "integration/prometheus_rw.rs"]
mod prometheus_rw;
#[path = "integration/splunk_hec.rs"]
mod splunk_hec;
#[path = "integration/syslog.rs"]
mod syslog;
#[path = "integration/vector.rs"]
mod vector;
