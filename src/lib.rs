// Project:   dfe-receiver
// File:      src/lib.rs
// Purpose:   Library root with public exports
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-receiver: High-performance HTTP/gRPC receiver for data ingestion.
//!
//! This library provides the core components for receiving events over
//! HTTP(S), gRPC and the stream and datagram protocols agents speak,
//! validating them as JSON, and routing them to Kafka topics or gRPC
//! destinations such as dfe-loader.
//!
//! ## Architecture
//!
//! ```text
//! Listener (bytes::Bytes per record)
//!     |
//!     +- Auth (header, bearer, mTLS)
//!     +- JSON validation (sonic_rs, no full parse)
//!     +- Router (rules set _source and _org_id, then the destinations)
//!     +- Destinations, each behind its buffer (in memory, or a disk spool)
//!         +- Kafka (librdkafka producer)
//!         +- gRPC destination (scalo Push)
//! ```

#![forbid(unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]
// Pedantic lints that would need a large refactor.
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
// Allow unwrap/expect in test code -- they're idiomatic for failing fast on errors
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
pub mod secrets;
pub mod server;
pub mod sink;
pub mod tenancy;
pub mod validation;

pub use error::{Error, Result};
