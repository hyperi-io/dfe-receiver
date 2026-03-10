// Project:   dfe-receiver
// File:      src/lib.rs
// Purpose:   Library root with public exports
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-receiver: High-performance HTTP/gRPC receiver for data ingestion.
//!
//! This library provides the core components for receiving JSON data over
//! HTTP(S) and gRPC, validating it, routing to Kafka topics or dfe-loader,
//! with optimised batching and disk spillover.
//!
//! ## Architecture
//!
//! ```text
//! HTTP/gRPC Request (bytes::Bytes)
//!     │
//!     ├─ Auth middleware (header + mTLS)
//!     │
//!     ├─ JSON validation (sonic_rs - no full parse)
//!     │
//!     ├─ Router (zero-copy field extraction)
//!     │
//!     ├─ Destination dispatcher
//!     │   ├─ Kafka batcher (per-topic)
//!     │   └─ dfe-loader transport
//!     │
//!     └─ Tiered sink (memory + disk spillover)
//! ```

#![forbid(unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]
// Suppress pedantic lints that require extensive refactoring — previously warnings, not errors.
// These are suppressed for CI compatibility; fix incrementally over time.
#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::format_push_string)]
#![allow(clippy::ignored_unit_patterns)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::default_trait_access)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::single_match_else)]
#![allow(clippy::should_implement_trait)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::if_not_else)]
#![allow(clippy::assigning_clones)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::unnecessary_map_or)]
#![allow(clippy::semicolon_if_nothing_returned)]
// Allow unwrap/expect in test code — they're idiomatic for failing fast on errors
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]
#![cfg_attr(test, allow(clippy::unreadable_literal))]

pub mod buffer;
pub mod config;
pub mod deployment;
pub mod error;
pub mod metrics;
pub mod pipeline;
pub mod routing;
pub mod server;
pub mod sink;
pub mod validation;

pub use error::{Error, Result};
