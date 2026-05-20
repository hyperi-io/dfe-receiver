//  Project:      dfe-receiver
//  File:         src/server/sflow/mod.rs
//  Purpose:      sFlow v5 decoder module root
//  Language:     Rust
//
//  License:      FSL-1.1-ALv2
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! sFlow v5 decoder.
//!
//! Hand-rolled on nom 8.0; there is no maintained Rust sFlow crate. The
//! `parser` submodule parses datagrams into typed `Sample` enums; the
//! `decoder` submodule wraps that parser into the project's generic
//! `FlowDecoder` trait so the listener can dispatch sFlow alongside NetFlow
//! through the same surface.

pub mod decoder;
pub mod parser;

#[cfg(test)]
mod tests;
