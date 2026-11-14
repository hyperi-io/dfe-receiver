//  Project:      dfe-receiver
//  File:         src/server/netflow/mod.rs
//  Purpose:      NetFlow / IPFIX decoder module root
//  Language:     Rust
//
//  License:      BUSL-1.1
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! NetFlow / IPFIX decoder.
//!
//! NetFlow v5 is hand-rolled here (fixed 24-byte header + N x 48-byte records;
//! `netgauze-flow-pkt` does not implement v5). NetFlow v9 and IPFIX are
//! delegated to `netgauze_flow_pkt::codec::FlowInfoCodec` with per-exporter
//! template state.

pub mod decoder;

#[cfg(test)]
mod tests;
