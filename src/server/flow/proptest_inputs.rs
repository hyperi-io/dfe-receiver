//  Project:      dfe-receiver
//  File:         src/server/flow/proptest_inputs.rs
//  Purpose:      Shared proptest strategies for fuzzing flow decoders
//  Language:     Rust
//
//  License:      BUSL-1.1
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! Fuzz strategies for flow decoders. Asserts decoders never panic on
//! arbitrary input. Shared between the netflow and sflow test modules.

#![cfg(test)]

use proptest::prelude::*;

prop_compose! {
    pub fn random_bytes(max: usize)(bytes in prop::collection::vec(any::<u8>(), 0..max)) -> Vec<u8> {
        bytes
    }
}

prop_compose! {
    /// Random datagram prefixed with one of the four NetFlow version words we
    /// dispatch on (5, 7, 9, 10). Forces the dispatch + decoder code paths to
    /// run on garbage payloads rather than rejecting at the version header.
    pub fn random_netflow_versioned()(
        version in prop_oneof![Just(5u16), Just(7u16), Just(9u16), Just(10u16)],
        body in random_bytes(2000),
    ) -> Vec<u8> {
        let mut v = Vec::with_capacity(body.len() + 2);
        v.extend_from_slice(&version.to_be_bytes());
        v.extend_from_slice(&body);
        v
    }
}

prop_compose! {
    /// Random datagram prefixed with an sFlow-shaped 4-byte version=5 word
    /// followed by random body. Exercises the sFlow decoder header path on
    /// garbage payloads.
    pub fn random_sflow_versioned()(
        sub in 0u16..256,
        body in random_bytes(2000),
    ) -> Vec<u8> {
        let mut v = Vec::with_capacity(body.len() + 4);
        v.extend_from_slice(&0u16.to_be_bytes());
        v.extend_from_slice(&sub.to_be_bytes());
        v.extend_from_slice(&body);
        v
    }
}
