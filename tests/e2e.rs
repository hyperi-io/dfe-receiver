// Project:   dfe-receiver
// File:      tests/e2e.rs
// Purpose:   End-to-end tests requiring real infrastructure (Kafka, remote clusters)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! E2E tests that require real infrastructure.
//! Run with: `cargo nextest run --test e2e` or `cargo nextest run -- --ignored`

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

#[path = "e2e/kafka.rs"]
mod kafka;

#[path = "e2e/contract_artefacts.rs"]
mod contract_artefacts;
