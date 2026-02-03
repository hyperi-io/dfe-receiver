// Project:   dfe-receiver
// File:      src/lib.rs
// Purpose:   Library root with public exports
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

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

pub mod buffer;
pub mod config;
pub mod error;
pub mod metrics;
pub mod pipeline;
pub mod routing;
pub mod server;
pub mod sink;
pub mod validation;

pub use error::{Error, Result};
