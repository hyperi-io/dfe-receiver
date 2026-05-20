//  Project:      dfe-receiver
//  File:         src/server/sflow/tests/proptest.rs
//  Purpose:      Proptest fuzz harness for SflowDecoder (never panic)
//  Language:     Rust
//
//  License:      FSL-1.1-ALv2
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! Property tests asserting `SflowDecoder::decode` never panics on
//! arbitrary input.

use crate::server::flow::decoder::FlowDecoder;
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::flow::proptest_inputs::random_sflow_versioned;
use crate::server::sflow::decoder::SflowDecoder;
use proptest::proptest;
use std::net::IpAddr;

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 256,
        .. proptest::test_runner::Config::default()
    })]

    #[test]
    fn sflow_decoder_never_panics(input in random_sflow_versioned()) {
        let mut d = SflowDecoder::new();
        let exporter: IpAddr = "127.0.0.1".parse().expect("ipv4 literal");
        let _ = d.decode(&input, exporter, ProtocolKind::SflowV5);
    }
}
