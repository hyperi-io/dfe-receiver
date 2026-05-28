//  Project:      dfe-receiver
//  File:         src/server/netflow/tests/proptest.rs
//  Purpose:      Proptest fuzz harness for NetflowDecoder (never panic)
//  Language:     Rust
//
//  License:      BUSL-1.1
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! Property tests asserting `NetflowDecoder::decode` never panics on
//! arbitrary input across the three supported `ProtocolKind` discriminators.

use crate::server::flow::decoder::FlowDecoder;
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::flow::proptest_inputs::random_netflow_versioned;
use crate::server::netflow::decoder::NetflowDecoder;
use proptest::proptest;
use std::net::IpAddr;

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 256,
        .. proptest::test_runner::Config::default()
    })]

    #[test]
    fn netflow_decoder_v5_never_panics(input in random_netflow_versioned()) {
        let mut d = NetflowDecoder::new(100, 100, crate::server::flow::metrics::mock::flow_metrics_for_test());
        let exporter: IpAddr = "127.0.0.1".parse().expect("ipv4 literal");
        let _ = d.decode(&input, exporter, ProtocolKind::NetflowV5);
    }

    #[test]
    fn netflow_decoder_v9_never_panics(input in random_netflow_versioned()) {
        let mut d = NetflowDecoder::new(100, 100, crate::server::flow::metrics::mock::flow_metrics_for_test());
        let exporter: IpAddr = "127.0.0.1".parse().expect("ipv4 literal");
        let _ = d.decode(&input, exporter, ProtocolKind::NetflowV9);
    }

    #[test]
    fn netflow_decoder_ipfix_never_panics(input in random_netflow_versioned()) {
        let mut d = NetflowDecoder::new(100, 100, crate::server::flow::metrics::mock::flow_metrics_for_test());
        let exporter: IpAddr = "127.0.0.1".parse().expect("ipv4 literal");
        let _ = d.decode(&input, exporter, ProtocolKind::Ipfix);
    }
}
