//  Project:      dfe-receiver
//  File:         src/server/netflow/tests/template_miss.rs
//  Purpose:      Test template_miss error handling in NetflowDecoder
//  Language:     Rust
//
//  License:      FSL-1.1-ALv2
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! template_miss test for NetflowDecoder.
//!
//! Current behaviour (netgauze 0.12 limitation):
//!   A data record referencing a never-declared template ID produces some
//!   form of `NetflowError::Parse(...)`. The decoder does NOT distinguish
//!   "template not yet received" from a hard parse error. `is_template_miss`
//!   always returns false for these errors.
//!
//! Future behaviour (deferred follow-up):
//!   Once netgauze exposes the discriminator (or we add a custom error
//!   classifier), `is_template_miss` should return true for the
//!   never-declared-template case. This test will then need to be flipped to
//!   assert that behaviour.

use crate::server::flow::decoder::FlowDecoder;
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::netflow::decoder::{NetflowDecoder, NetflowError};
use std::net::IpAddr;

/// Build a NetFlow v9 datagram with a data flowset (id=256) but NO matching
/// template -- the decoder cannot interpret the record without the template.
fn build_v9_data_record_without_template() -> Vec<u8> {
    // v9 header is 20 bytes.
    let mut pkt = vec![0u8; 32];
    pkt[0..2].copy_from_slice(&9u16.to_be_bytes()); // version 9
    pkt[2..4].copy_from_slice(&1u16.to_be_bytes()); // count = 1 flowset
    pkt[4..8].copy_from_slice(&0u32.to_be_bytes()); // sys_uptime
    pkt[8..12].copy_from_slice(&1_700_000_000u32.to_be_bytes()); // unix_secs
    pkt[12..16].copy_from_slice(&0u32.to_be_bytes()); // seq
    pkt[16..20].copy_from_slice(&0u32.to_be_bytes()); // source_id
    // Data flowset header
    pkt[20..22].copy_from_slice(&256u16.to_be_bytes()); // flowset id = 256 (data, not template)
    pkt[22..24].copy_from_slice(&12u16.to_be_bytes()); // length = 12
    // 8 bytes of opaque data (decoder has no template to interpret it)
    // bytes 24-31 already zero from the initial allocation
    pkt
}

#[test]
fn data_record_without_template_returns_error() {
    let pkt = build_v9_data_record_without_template();
    let mut decoder = NetflowDecoder::new(
        1000,
        10000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let result = decoder.decode(&pkt, exporter, ProtocolKind::NetflowV9);
    assert!(
        result.is_err(),
        "expected decode error for missing-template data record"
    );

    // Current limitation: error type is Parse, not TemplateMiss.
    // When netgauze adds the discriminator (or we add a custom classifier),
    // flip the assertion below.
    let err = result.unwrap_err();
    assert!(
        matches!(err, NetflowError::Parse(_) | NetflowError::Io(_)),
        "expected Parse error (template_miss not yet differentiable); got {err:?}"
    );
}

#[test]
fn is_template_miss_returns_false_for_current_errors() {
    // Documents current behaviour: every error path returns is_template_miss=false.
    // When netgauze gains the discriminator, flip the expected value for the
    // genuine missing-template case.
    let pkt = build_v9_data_record_without_template();
    let mut decoder = NetflowDecoder::new(
        1000,
        10000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "127.0.0.1".parse().unwrap();
    let err = decoder
        .decode(&pkt, exporter, ProtocolKind::NetflowV9)
        .unwrap_err();
    assert!(
        !NetflowDecoder::is_template_miss(&err),
        "current netgauze 0.12 limitation -- this is expected behaviour, see decoder.rs TODO"
    );
}
