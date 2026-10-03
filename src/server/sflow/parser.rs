//  Project:      dfe-receiver
//  File:         src/server/sflow/parser.rs
//  Purpose:      sFlow v5 datagram parser (nom 8.0 based)
//  Language:     Rust
//
//  License:      BUSL-1.1
//  Copyright:    (c) 2026 HYPERI PTY LIMITED

//! sFlow v5 datagram parser. sFlow.org v5 spec.
//!
//! Scope v1: datagram header, flow-samples (format 1), counter-samples
//! (format 2). Vendor-specific opaque blocks captured as raw bytes
//! (preserved in raw output, skipped in canonical).
//!
//! Wire format: big-endian XDR-style 32-bit fields. Variable-length opaque
//! data is padded to 4-byte boundaries; we do NOT add padding handling here
//! because the only variable-length fields we surface (sampled header bytes)
//! are length-prefixed and the caller never overruns into padding.

use nom::{
    IResult, Parser,
    bytes::complete::take,
    multi::count,
    number::complete::{be_u32, be_u64},
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Parsed sFlow v5 datagram.
#[derive(Debug, Clone)]
pub struct SflowDatagram {
    pub version: u32,
    pub agent_address: IpAddr,
    pub sub_agent_id: u32,
    pub sequence_number: u32,
    pub uptime: u32,
    pub samples: Vec<Sample>,
}

/// One sample inside a datagram. Format 1 = flow_sample, 2 = counter_sample.
/// Other formats (3 = expanded_flow_sample, 4 = expanded_counter_sample,
/// vendor-specific) are captured as `Opaque` so callers can still surface them
/// in raw output without losing data.
#[derive(Debug, Clone)]
pub enum Sample {
    Flow(FlowSample),
    Counter(CounterSample),
    Opaque { format: u32, data: Vec<u8> },
}

/// sFlow flow sample (format 1).
#[derive(Debug, Clone)]
pub struct FlowSample {
    pub sequence_number: u32,
    pub source_id: u32,
    pub sampling_rate: u32,
    pub sample_pool: u32,
    pub drops: u32,
    pub input_iface: u32,
    pub output_iface: u32,
    pub records: Vec<FlowRecord>,
}

/// Flow record inside a flow sample. Format 1 = sampled_header (raw L2/L3/L4
/// bytes). Other formats are captured opaquely so we never drop data.
#[derive(Debug, Clone)]
pub enum FlowRecord {
    SampledHeader {
        protocol: u32,
        frame_length: u32,
        stripped: u32,
        header: Vec<u8>,
    },
    Opaque {
        format: u32,
        data: Vec<u8>,
    },
}

/// sFlow counter sample (format 2).
#[derive(Debug, Clone)]
pub struct CounterSample {
    pub sequence_number: u32,
    pub source_id: u32,
    pub records: Vec<CounterRecord>,
}

/// Counter record. Format 1 = generic_interface (full IF-MIB counters).
/// Other formats (ethernet, token ring, vlan, processor, host CPU, etc.)
/// are captured opaquely.
#[derive(Debug, Clone)]
pub enum CounterRecord {
    Generic {
        if_index: u32,
        if_type: u32,
        if_speed: u64,
        if_direction: u32,
        if_status: u32,
        if_in_octets: u64,
        if_in_ucast_pkts: u32,
        if_in_multicast_pkts: u32,
        if_in_broadcast_pkts: u32,
        if_in_discards: u32,
        if_in_errors: u32,
        if_in_unknown_protos: u32,
        if_out_octets: u64,
        if_out_ucast_pkts: u32,
        if_out_multicast_pkts: u32,
        if_out_broadcast_pkts: u32,
        if_out_discards: u32,
        if_out_errors: u32,
        if_promiscuous_mode: u32,
    },
    Opaque {
        format: u32,
        data: Vec<u8>,
    },
}

/// Parse a full sFlow v5 datagram. Returns `Err` if version != 5 or the
/// agent_address_type is not 1 (IPv4) or 2 (IPv6).
pub fn parse_datagram(input: &[u8]) -> IResult<&[u8], SflowDatagram> {
    let (input, version) = be_u32(input)?;
    if version != 5 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }
    let (input, agent_address) = parse_agent_address(input)?;
    let (input, sub_agent_id) = be_u32(input)?;
    let (input, sequence_number) = be_u32(input)?;
    let (input, uptime) = be_u32(input)?;
    let (input, num_samples) = be_u32(input)?;
    let (input, samples) = count(parse_sample, num_samples as usize).parse(input)?;
    Ok((
        input,
        SflowDatagram {
            version,
            agent_address,
            sub_agent_id,
            sequence_number,
            uptime,
            samples,
        },
    ))
}

/// sFlow v5 agent_address_type: 1 = IPv4 (4 bytes), 2 = IPv6 (16 bytes).
fn parse_agent_address(input: &[u8]) -> IResult<&[u8], IpAddr> {
    let (input, addr_type) = be_u32(input)?;
    match addr_type {
        1 => {
            let (input, bytes) = take(4usize)(input)?;
            Ok((
                input,
                IpAddr::V4(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])),
            ))
        }
        2 => {
            let (input, bytes) = take(16usize)(input)?;
            let mut octets = [0u8; 16];
            octets.copy_from_slice(bytes);
            Ok((input, IpAddr::V6(Ipv6Addr::from(octets))))
        }
        _ => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        ))),
    }
}

/// Parse one sample (any format). Uses the sample_length field to skip
/// unknown formats without bleeding into the next sample.
fn parse_sample(input: &[u8]) -> IResult<&[u8], Sample> {
    let (input, format) = be_u32(input)?;
    let (input, sample_length) = be_u32(input)?;
    let (input, body) = take(sample_length as usize)(input)?;
    match format {
        1 => {
            let (_, fs) = parse_flow_sample(body)?;
            Ok((input, Sample::Flow(fs)))
        }
        2 => {
            let (_, cs) = parse_counter_sample(body)?;
            Ok((input, Sample::Counter(cs)))
        }
        _ => Ok((
            input,
            Sample::Opaque {
                format,
                data: body.to_vec(),
            },
        )),
    }
}

fn parse_flow_sample(input: &[u8]) -> IResult<&[u8], FlowSample> {
    let (input, sequence_number) = be_u32(input)?;
    let (input, source_id) = be_u32(input)?;
    let (input, sampling_rate) = be_u32(input)?;
    let (input, sample_pool) = be_u32(input)?;
    let (input, drops) = be_u32(input)?;
    let (input, input_iface) = be_u32(input)?;
    let (input, output_iface) = be_u32(input)?;
    let (input, num_records) = be_u32(input)?;
    let (input, records) = count(parse_flow_record, num_records as usize).parse(input)?;
    Ok((
        input,
        FlowSample {
            sequence_number,
            source_id,
            sampling_rate,
            sample_pool,
            drops,
            input_iface,
            output_iface,
            records,
        },
    ))
}

fn parse_flow_record(input: &[u8]) -> IResult<&[u8], FlowRecord> {
    let (input, format) = be_u32(input)?;
    let (input, data_len) = be_u32(input)?;
    let (input, body) = take(data_len as usize)(input)?;
    match format {
        1 => {
            // sampled_header
            let (body, protocol) = be_u32(body)?;
            let (body, frame_length) = be_u32(body)?;
            let (body, stripped) = be_u32(body)?;
            let (body, header_length) = be_u32(body)?;
            let (_, header_bytes) = take(header_length as usize)(body)?;
            Ok((
                input,
                FlowRecord::SampledHeader {
                    protocol,
                    frame_length,
                    stripped,
                    header: header_bytes.to_vec(),
                },
            ))
        }
        _ => Ok((
            input,
            FlowRecord::Opaque {
                format,
                data: body.to_vec(),
            },
        )),
    }
}

fn parse_counter_sample(input: &[u8]) -> IResult<&[u8], CounterSample> {
    let (input, sequence_number) = be_u32(input)?;
    let (input, source_id) = be_u32(input)?;
    let (input, num_records) = be_u32(input)?;
    let (input, records) = count(parse_counter_record, num_records as usize).parse(input)?;
    Ok((
        input,
        CounterSample {
            sequence_number,
            source_id,
            records,
        },
    ))
}

fn parse_counter_record(input: &[u8]) -> IResult<&[u8], CounterRecord> {
    let (input, format) = be_u32(input)?;
    let (input, data_len) = be_u32(input)?;
    let (input, body) = take(data_len as usize)(input)?;
    match format {
        1 => {
            // generic_interface counters
            let (body, if_index) = be_u32(body)?;
            let (body, if_type) = be_u32(body)?;
            let (body, if_speed) = be_u64(body)?;
            let (body, if_direction) = be_u32(body)?;
            let (body, if_status) = be_u32(body)?;
            let (body, if_in_octets) = be_u64(body)?;
            let (body, if_in_ucast_pkts) = be_u32(body)?;
            let (body, if_in_multicast_pkts) = be_u32(body)?;
            let (body, if_in_broadcast_pkts) = be_u32(body)?;
            let (body, if_in_discards) = be_u32(body)?;
            let (body, if_in_errors) = be_u32(body)?;
            let (body, if_in_unknown_protos) = be_u32(body)?;
            let (body, if_out_octets) = be_u64(body)?;
            let (body, if_out_ucast_pkts) = be_u32(body)?;
            let (body, if_out_multicast_pkts) = be_u32(body)?;
            let (body, if_out_broadcast_pkts) = be_u32(body)?;
            let (body, if_out_discards) = be_u32(body)?;
            let (body, if_out_errors) = be_u32(body)?;
            let (_, if_promiscuous_mode) = be_u32(body)?;
            Ok((
                input,
                CounterRecord::Generic {
                    if_index,
                    if_type,
                    if_speed,
                    if_direction,
                    if_status,
                    if_in_octets,
                    if_in_ucast_pkts,
                    if_in_multicast_pkts,
                    if_in_broadcast_pkts,
                    if_in_discards,
                    if_in_errors,
                    if_in_unknown_protos,
                    if_out_octets,
                    if_out_ucast_pkts,
                    if_out_multicast_pkts,
                    if_out_broadcast_pkts,
                    if_out_discards,
                    if_out_errors,
                    if_promiscuous_mode,
                },
            ))
        }
        _ => Ok((
            input,
            CounterRecord::Opaque {
                format,
                data: body.to_vec(),
            },
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal v5 header (no samples) for a parser primitive check.
    fn build_header_only() -> Vec<u8> {
        let mut pkt = Vec::new();
        pkt.extend_from_slice(&5u32.to_be_bytes()); // version
        pkt.extend_from_slice(&1u32.to_be_bytes()); // agent_address_type = IPv4
        pkt.extend_from_slice(&[10, 0, 0, 1]); // agent ip
        pkt.extend_from_slice(&7u32.to_be_bytes()); // sub_agent_id
        pkt.extend_from_slice(&100u32.to_be_bytes()); // sequence_number
        pkt.extend_from_slice(&1_000_000u32.to_be_bytes()); // uptime ms
        pkt.extend_from_slice(&0u32.to_be_bytes()); // num_samples = 0
        pkt
    }

    #[test]
    fn parses_minimal_datagram() {
        let pkt = build_header_only();
        let (rest, dg) = parse_datagram(&pkt).expect("parse ok");
        assert_eq!(rest, [] as [u8; 0]);
        assert_eq!(dg.version, 5);
        assert_eq!(dg.agent_address, "10.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(dg.sub_agent_id, 7);
        assert_eq!(dg.sequence_number, 100);
        assert_eq!(dg.uptime, 1_000_000);
        assert_eq!(dg.samples.len(), 0);
    }

    #[test]
    fn parses_datagram_with_one_flow_sample() {
        // Build a minimal sampled-header (ethernet + IPv4 + TCP, 54 bytes).
        // Eth: 14 bytes (dst MAC 6, src MAC 6, ethertype 0x0800)
        // IPv4: 20 bytes (IHL=5, proto=6=TCP, src/dst)
        // TCP: 20 bytes
        let mut hdr = Vec::new();
        // Ethernet
        hdr.extend_from_slice(&[0xAA; 6]); // dst MAC
        hdr.extend_from_slice(&[0xBB; 6]); // src MAC
        hdr.extend_from_slice(&[0x08, 0x00]); // ethertype IPv4
        // IPv4
        hdr.push(0x45); // version=4, IHL=5
        hdr.push(0x00); // DSCP/ECN
        hdr.extend_from_slice(&40u16.to_be_bytes()); // total length
        hdr.extend_from_slice(&[0, 0, 0, 0]); // id, flags, frag
        hdr.push(64); // TTL
        hdr.push(6); // protocol = TCP
        hdr.extend_from_slice(&[0, 0]); // checksum
        hdr.extend_from_slice(&[10, 0, 0, 1]); // src ip
        hdr.extend_from_slice(&[10, 0, 0, 2]); // dst ip
        // TCP
        hdr.extend_from_slice(&12345u16.to_be_bytes()); // src port
        hdr.extend_from_slice(&80u16.to_be_bytes()); // dst port
        hdr.extend_from_slice(&[0; 8]); // seq, ack
        hdr.push(0x50); // data offset 5 << 4
        hdr.push(0x18); // flags: PSH+ACK
        hdr.extend_from_slice(&[0; 6]); // window, checksum, urgent
        assert_eq!(hdr.len(), 54);

        // flow_record body: protocol(4) + frame_length(4) + stripped(4) + header_length(4) + header
        let mut flow_record_body = Vec::new();
        flow_record_body.extend_from_slice(&1u32.to_be_bytes()); // header protocol = ethernet
        flow_record_body.extend_from_slice(&54u32.to_be_bytes()); // frame length
        flow_record_body.extend_from_slice(&0u32.to_be_bytes()); // stripped
        flow_record_body.extend_from_slice(&(hdr.len() as u32).to_be_bytes()); // header_length
        flow_record_body.extend_from_slice(&hdr);

        // flow_record header: format(4) + data_length(4) + body
        let mut flow_record = Vec::new();
        flow_record.extend_from_slice(&1u32.to_be_bytes()); // format = sampled_header
        flow_record.extend_from_slice(&(flow_record_body.len() as u32).to_be_bytes());
        flow_record.extend_from_slice(&flow_record_body);

        // flow_sample body
        let mut flow_sample_body = Vec::new();
        flow_sample_body.extend_from_slice(&1u32.to_be_bytes()); // seq
        flow_sample_body.extend_from_slice(&0x_0100_0001u32.to_be_bytes()); // source_id type=1, idx=1
        flow_sample_body.extend_from_slice(&1000u32.to_be_bytes()); // sampling_rate
        flow_sample_body.extend_from_slice(&5000u32.to_be_bytes()); // sample_pool
        flow_sample_body.extend_from_slice(&0u32.to_be_bytes()); // drops
        flow_sample_body.extend_from_slice(&7u32.to_be_bytes()); // input
        flow_sample_body.extend_from_slice(&9u32.to_be_bytes()); // output
        flow_sample_body.extend_from_slice(&1u32.to_be_bytes()); // num_records
        flow_sample_body.extend_from_slice(&flow_record);

        // sample wrapper: format(4) + length(4) + body
        let mut sample = Vec::new();
        sample.extend_from_slice(&1u32.to_be_bytes()); // format = flow_sample
        sample.extend_from_slice(&(flow_sample_body.len() as u32).to_be_bytes());
        sample.extend_from_slice(&flow_sample_body);

        // Datagram header + 1 sample
        let mut pkt = Vec::new();
        pkt.extend_from_slice(&5u32.to_be_bytes()); // version
        pkt.extend_from_slice(&1u32.to_be_bytes()); // agent_address_type = IPv4
        pkt.extend_from_slice(&[10, 0, 0, 1]); // agent ip
        pkt.extend_from_slice(&7u32.to_be_bytes()); // sub_agent_id
        pkt.extend_from_slice(&100u32.to_be_bytes()); // sequence
        pkt.extend_from_slice(&1_000_000u32.to_be_bytes()); // uptime
        pkt.extend_from_slice(&1u32.to_be_bytes()); // num_samples
        pkt.extend_from_slice(&sample);

        let (rest, dg) = parse_datagram(&pkt).expect("parse ok");
        assert_eq!(rest, [] as [u8; 0]);
        assert_eq!(dg.samples.len(), 1);
        match &dg.samples[0] {
            Sample::Flow(fs) => {
                assert_eq!(fs.sampling_rate, 1000);
                assert_eq!(fs.input_iface, 7);
                assert_eq!(fs.output_iface, 9);
                assert_eq!(fs.records.len(), 1);
                match &fs.records[0] {
                    FlowRecord::SampledHeader {
                        protocol, header, ..
                    } => {
                        assert_eq!(*protocol, 1);
                        assert_eq!(header.len(), 54);
                    }
                    other @ FlowRecord::Opaque { .. } => {
                        panic!("expected SampledHeader, got {other:?}")
                    }
                }
            }
            other => panic!("expected flow sample, got {other:?}"),
        }
    }

    #[test]
    fn parses_datagram_with_one_counter_sample() {
        // generic_interface counters: 88 bytes total
        let mut counter_body = Vec::new();
        counter_body.extend_from_slice(&5u32.to_be_bytes()); // if_index
        counter_body.extend_from_slice(&6u32.to_be_bytes()); // if_type (ethernetCsmacd)
        counter_body.extend_from_slice(&1_000_000_000u64.to_be_bytes()); // 1Gbps
        counter_body.extend_from_slice(&1u32.to_be_bytes()); // direction = full-duplex
        counter_body.extend_from_slice(&3u32.to_be_bytes()); // status (admin+oper up)
        counter_body.extend_from_slice(&999_888u64.to_be_bytes()); // if_in_octets
        counter_body.extend_from_slice(&100u32.to_be_bytes()); // ucast
        counter_body.extend_from_slice(&10u32.to_be_bytes()); // multicast
        counter_body.extend_from_slice(&5u32.to_be_bytes()); // broadcast
        counter_body.extend_from_slice(&0u32.to_be_bytes()); // discards
        counter_body.extend_from_slice(&1u32.to_be_bytes()); // errors
        counter_body.extend_from_slice(&0u32.to_be_bytes()); // unknown_protos
        counter_body.extend_from_slice(&777_666u64.to_be_bytes()); // if_out_octets
        counter_body.extend_from_slice(&90u32.to_be_bytes()); // out_ucast
        counter_body.extend_from_slice(&5u32.to_be_bytes()); // out_multicast
        counter_body.extend_from_slice(&2u32.to_be_bytes()); // out_broadcast
        counter_body.extend_from_slice(&0u32.to_be_bytes()); // out_discards
        counter_body.extend_from_slice(&0u32.to_be_bytes()); // out_errors
        counter_body.extend_from_slice(&0u32.to_be_bytes()); // promiscuous

        let mut counter_record = Vec::new();
        counter_record.extend_from_slice(&1u32.to_be_bytes()); // format = generic
        counter_record.extend_from_slice(&(counter_body.len() as u32).to_be_bytes());
        counter_record.extend_from_slice(&counter_body);

        let mut counter_sample_body = Vec::new();
        counter_sample_body.extend_from_slice(&1u32.to_be_bytes()); // seq
        counter_sample_body.extend_from_slice(&0u32.to_be_bytes()); // source_id
        counter_sample_body.extend_from_slice(&1u32.to_be_bytes()); // num_records
        counter_sample_body.extend_from_slice(&counter_record);

        let mut sample = Vec::new();
        sample.extend_from_slice(&2u32.to_be_bytes()); // format = counter_sample
        sample.extend_from_slice(&(counter_sample_body.len() as u32).to_be_bytes());
        sample.extend_from_slice(&counter_sample_body);

        let mut pkt = Vec::new();
        pkt.extend_from_slice(&5u32.to_be_bytes());
        pkt.extend_from_slice(&1u32.to_be_bytes()); // IPv4
        pkt.extend_from_slice(&[10, 0, 0, 1]);
        pkt.extend_from_slice(&7u32.to_be_bytes());
        pkt.extend_from_slice(&100u32.to_be_bytes());
        pkt.extend_from_slice(&1_000_000u32.to_be_bytes());
        pkt.extend_from_slice(&1u32.to_be_bytes()); // num_samples
        pkt.extend_from_slice(&sample);

        let (rest, dg) = parse_datagram(&pkt).expect("parse ok");
        assert_eq!(rest, [] as [u8; 0]);
        assert_eq!(dg.samples.len(), 1);
        match &dg.samples[0] {
            Sample::Counter(cs) => {
                assert_eq!(cs.records.len(), 1);
                match &cs.records[0] {
                    CounterRecord::Generic {
                        if_index,
                        if_speed,
                        if_in_octets,
                        if_out_octets,
                        if_in_ucast_pkts,
                        ..
                    } => {
                        assert_eq!(*if_index, 5);
                        assert_eq!(*if_speed, 1_000_000_000);
                        assert_eq!(*if_in_octets, 999_888);
                        assert_eq!(*if_out_octets, 777_666);
                        assert_eq!(*if_in_ucast_pkts, 100);
                    }
                    other @ CounterRecord::Opaque { .. } => {
                        panic!("expected Generic, got {other:?}")
                    }
                }
            }
            other => panic!("expected counter sample, got {other:?}"),
        }
    }

    #[test]
    fn rejects_invalid_version() {
        let mut pkt = build_header_only();
        // Stomp version to 6.
        pkt[0..4].copy_from_slice(&6u32.to_be_bytes());
        let err = parse_datagram(&pkt).expect_err("version 6 must error");
        // nom errors carry the remaining input, not a structured kind we want
        // to switch on -- enough to confirm we got an Err.
        assert!(matches!(err, nom::Err::Error(_)));
    }

    #[test]
    fn rejects_invalid_agent_address_type() {
        let mut pkt = build_header_only();
        // Stomp agent_address_type to 99.
        pkt[4..8].copy_from_slice(&99u32.to_be_bytes());
        let err = parse_datagram(&pkt).expect_err("address_type 99 must error");
        assert!(matches!(err, nom::Err::Error(_)));
    }

    #[test]
    fn parses_ipv6_agent_address() {
        let mut pkt = Vec::new();
        pkt.extend_from_slice(&5u32.to_be_bytes()); // version
        pkt.extend_from_slice(&2u32.to_be_bytes()); // address_type = IPv6
        let v6: Ipv6Addr = "2001:db8::1".parse().unwrap();
        pkt.extend_from_slice(&v6.octets());
        pkt.extend_from_slice(&7u32.to_be_bytes()); // sub_agent
        pkt.extend_from_slice(&100u32.to_be_bytes()); // seq
        pkt.extend_from_slice(&1_000u32.to_be_bytes()); // uptime
        pkt.extend_from_slice(&0u32.to_be_bytes()); // num_samples
        let (_, dg) = parse_datagram(&pkt).expect("ipv6 datagram parses");
        assert_eq!(dg.agent_address, IpAddr::V6(v6));
    }

    #[test]
    fn skips_unknown_sample_format_to_opaque() {
        // Build a sample with format 99 (unknown) carrying 12 bytes of body.
        let body = vec![0xAAu8; 12];
        let mut sample = Vec::new();
        sample.extend_from_slice(&99u32.to_be_bytes());
        sample.extend_from_slice(&(body.len() as u32).to_be_bytes());
        sample.extend_from_slice(&body);

        let mut pkt = Vec::new();
        pkt.extend_from_slice(&5u32.to_be_bytes());
        pkt.extend_from_slice(&1u32.to_be_bytes());
        pkt.extend_from_slice(&[10, 0, 0, 1]);
        pkt.extend_from_slice(&7u32.to_be_bytes());
        pkt.extend_from_slice(&100u32.to_be_bytes());
        pkt.extend_from_slice(&1_000u32.to_be_bytes());
        pkt.extend_from_slice(&1u32.to_be_bytes()); // num_samples
        pkt.extend_from_slice(&sample);

        let (rest, dg) = parse_datagram(&pkt).expect("parses with opaque sample");
        assert_eq!(rest, [] as [u8; 0]);
        assert_eq!(dg.samples.len(), 1);
        match &dg.samples[0] {
            Sample::Opaque { format, data } => {
                assert_eq!(*format, 99);
                assert_eq!(data.len(), 12);
            }
            other => panic!("expected Opaque, got {other:?}"),
        }
    }
}
