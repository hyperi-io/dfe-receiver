//  Project:      dfe-receiver
//  File:         src/server/netflow/tests/template_cap.rs
//  Purpose:      Template caching: the per-exporter cap and observation-domain scope
//  Language:     Rust
//
//  License:      BUSL-1.1
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

use crate::server::flow::decoder::FlowDecoder;
use crate::server::flow::dispatch::ProtocolKind;
use crate::server::netflow::decoder::NetflowDecoder;
use bytes::BytesMut;
use chrono::{TimeZone, Utc};
use netgauze_flow_pkt::codec::FlowInfoCodec;
use netgauze_flow_pkt::ipfix::{DataRecord, IpfixPacket, Set as IpfixSet, TemplateRecord};
use netgauze_flow_pkt::{DataSetId, FieldSpecifier, FlowInfo, ie};
use std::net::{IpAddr, Ipv4Addr};
use tokio_util::codec::Encoder;

/// An IPFIX packet carrying one template per id in `ids`.
fn templates(ids: std::ops::Range<u16>) -> Vec<u8> {
    let records: Vec<TemplateRecord> = ids
        .map(|id| {
            TemplateRecord::new(
                id,
                Box::new([FieldSpecifier::new(ie::IE::sourceIPv4Address, 4).unwrap()]),
            )
        })
        .collect();
    let packet = IpfixPacket::new(
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        1,
        7,
        Box::new([IpfixSet::Template(records.into_boxed_slice())]),
    );
    let mut buf = BytesMut::new();
    FlowInfoCodec::new()
        .encode(FlowInfo::IPFIX(packet), &mut buf)
        .expect("encode templates");
    buf.to_vec()
}

/// An IPFIX data packet of one record under template `id`.
fn data(id: u16) -> Vec<u8> {
    let mut codec = FlowInfoCodec::new();
    let mut warm = BytesMut::from(&templates(id..id + 1)[..]);
    let _ = tokio_util::codec::Decoder::decode(&mut codec, &mut warm).expect("template");
    let packet = IpfixPacket::new(
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 1).unwrap(),
        2,
        7,
        Box::new([IpfixSet::Data {
            id: DataSetId::new(id).unwrap(),
            records: Box::new([DataRecord::new(
                Box::new([]),
                Box::new([ie::Field::sourceIPv4Address(Ipv4Addr::new(192, 0, 2, 1))]),
            )]),
        }]),
    );
    let mut buf = BytesMut::new();
    codec
        .encode(FlowInfo::IPFIX(packet), &mut buf)
        .expect("encode data");
    buf.to_vec()
}

/// An exporter sending more templates than the cap keeps only the newest,
/// across packets and within one, and the newest still decodes data.
#[test]
fn an_exporter_holds_no_more_templates_than_the_cap() {
    const CAP: usize = 4;
    let mut decoder = NetflowDecoder::new(
        CAP,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "198.51.100.9".parse().unwrap();

    decoder
        .decode(&templates(300..302), exporter, ProtocolKind::Ipfix)
        .expect("two templates");
    assert_eq!(decoder.templates(exporter), 2);
    decoder
        .decode(&templates(310..320), exporter, ProtocolKind::Ipfix)
        .expect("ten templates in one packet");
    assert_eq!(
        decoder.templates(exporter),
        CAP,
        "the exporter holds more templates than the cap"
    );

    let newest = decoder
        .decode(&data(319), exporter, ProtocolKind::Ipfix)
        .expect("the newest template decodes data");
    assert_eq!(newest.records.len(), 1);
    let other: IpAddr = "198.51.100.10".parse().unwrap();
    decoder
        .decode(&templates(300..302), other, ProtocolKind::Ipfix)
        .expect("another exporter");
    assert_eq!(decoder.templates(other), 2, "the cap is per exporter");
}

/// An IPFIX packet from observation domain `domain` carrying template `id`
/// over the one field `field`.
fn template_in(domain: u32, id: u16, field: ie::IE) -> Vec<u8> {
    let packet = IpfixPacket::new(
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        1,
        domain,
        Box::new([IpfixSet::Template(Box::new([TemplateRecord::new(
            id,
            Box::new([FieldSpecifier::new(field, 4).unwrap()]),
        )]))]),
    );
    let mut buf = BytesMut::new();
    FlowInfoCodec::new()
        .encode(FlowInfo::IPFIX(packet), &mut buf)
        .expect("encode template");
    buf.to_vec()
}

/// Template ids are scoped to their observation domain: two domains of one
/// exporter reusing an id with different layouts each decode by their own.
#[test]
fn a_template_id_is_scoped_to_its_observation_domain() {
    let mut decoder = NetflowDecoder::new(
        16,
        10_000,
        crate::server::flow::metrics::mock::flow_metrics_for_test(),
    );
    let exporter: IpAddr = "198.51.100.9".parse().unwrap();

    decoder
        .decode(
            &template_in(7, 400, ie::IE::sourceIPv4Address),
            exporter,
            ProtocolKind::Ipfix,
        )
        .expect("domain 7 template");
    decoder
        .decode(
            &template_in(8, 400, ie::IE::destinationIPv4Address),
            exporter,
            ProtocolKind::Ipfix,
        )
        .expect("domain 8 template");

    let packet = decoder
        .decode(&data(400), exporter, ProtocolKind::Ipfix)
        .expect("domain 7 data");
    let fields = &packet.records[0].fields;
    assert_eq!(fields.src_ip, Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))));
    assert_eq!(fields.dst_ip, None, "decoded under domain 8's template");
}
