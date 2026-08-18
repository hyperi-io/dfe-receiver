//! `FlowDecoder` trait. Each protocol family (NetFlow, sFlow) implements this.
//! The listener is generic over decoder via this trait surface; no `dyn`.

use crate::server::flow::dispatch::ProtocolKind;
use std::io;
use std::net::IpAddr;

/// A successfully decoded UDP datagram.
#[derive(Debug, Clone)]
pub struct DecodedPacket<R> {
    pub exporter_ip: IpAddr,
    pub observation_domain: u32,
    pub packet_seq: u32,
    pub kind: ProtocolKind,
    pub records: Vec<R>,
}

pub trait FlowDecoder: Send + 'static {
    type Record: Send;
    type DecodeError: std::error::Error + Send + Sync + 'static;

    /// Metric label: "netflow" or "sflow".
    const PROTOCOL: &'static str;

    /// Decode a UDP datagram. The `kind` passed in matches what
    /// `dispatch_protocol_kind` returned; the decoder may use it for fast-path
    /// switching.
    fn decode(
        &mut self,
        data: &[u8],
        source: IpAddr,
        kind: ProtocolKind,
    ) -> Result<DecodedPacket<Self::Record>, Self::DecodeError>;

    /// Render canonical schema into the provided buffer.
    fn render_canonical(record: &Self::Record, buf: &mut Vec<u8>) -> io::Result<()>;

    /// Render verbatim parser output (used by `flow.raw_capture`).
    fn render_raw(record: &Self::Record, buf: &mut Vec<u8>) -> io::Result<()>;

    /// Return the canonical `record_kind` string for this record.
    /// Default "flow"; SflowDecoder may return "counter" for interface samples;
    /// NetflowDecoder may return "security_event" / "nat_translation".
    fn record_kind(record: &Self::Record) -> &'static str {
        let _ = record;
        "flow"
    }

    /// Distinguish "template not yet received" from a real parse error.
    /// Default false; NetflowDecoder overrides.
    fn is_template_miss(_err: &Self::DecodeError) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    // Compile-time check that the trait surface is usable in a generic context.
    fn _is_decoder_send<D: FlowDecoder>() {}

    #[test]
    fn decoded_packet_constructs() {
        let p: DecodedPacket<u32> = DecodedPacket {
            exporter_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            observation_domain: 0,
            packet_seq: 0,
            kind: ProtocolKind::NetflowV5,
            records: vec![1, 2, 3],
        };
        assert_eq!(p.records.len(), 3);
        assert_eq!(p.kind, ProtocolKind::NetflowV5);
    }
}
