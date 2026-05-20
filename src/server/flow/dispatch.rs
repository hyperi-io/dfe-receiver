//! Wire-format autosense for NetFlow / sFlow datagrams.
//!
//! Reads bytes 0-1 of the UDP payload to determine the protocol family.
//! Pure function, ~1-2 ns. See design spec section "Wire-format dispatch".

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolKind {
    NetflowV5,
    NetflowV7,
    NetflowV9,
    Ipfix,
    SflowV5,
    Unknown,
    TooShort,
}

impl ProtocolKind {
    pub fn is_netflow_family(self) -> bool {
        matches!(
            self,
            ProtocolKind::NetflowV5
                | ProtocolKind::NetflowV7
                | ProtocolKind::NetflowV9
                | ProtocolKind::Ipfix
        )
    }

    pub fn is_sflow(self) -> bool {
        matches!(self, ProtocolKind::SflowV5)
    }

    /// Drop-reason label for `dfe_flow_invalid_packet_total`.
    pub fn drop_reason(self) -> &'static str {
        match self {
            ProtocolKind::TooShort => "too_short",
            ProtocolKind::Unknown => "unknown_version",
            _ => "ok",
        }
    }

    pub fn version_tag(self) -> &'static str {
        match self {
            ProtocolKind::NetflowV5 => "netflow_v5",
            ProtocolKind::NetflowV7 => "netflow_v7",
            ProtocolKind::NetflowV9 => "netflow_v9",
            ProtocolKind::Ipfix => "ipfix",
            ProtocolKind::SflowV5 => "sflow_v5",
            ProtocolKind::Unknown | ProtocolKind::TooShort => "unknown",
        }
    }
}

#[inline]
pub fn dispatch_protocol_kind(packet: &[u8]) -> ProtocolKind {
    if packet.len() < 4 {
        return ProtocolKind::TooShort;
    }
    match u16::from_be_bytes([packet[0], packet[1]]) {
        0x0005 => ProtocolKind::NetflowV5,
        0x0007 => ProtocolKind::NetflowV7,
        0x0009 => ProtocolKind::NetflowV9,
        0x000A => ProtocolKind::Ipfix,
        0x0000 => ProtocolKind::SflowV5,
        _ => ProtocolKind::Unknown,
    }
}

/// Cheap declared-length sanity. Returns true if header-declared sizes are
/// consistent with `packet.len()`. False -> drop with `length_overflow`.
pub fn length_sanity_check(kind: ProtocolKind, packet: &[u8]) -> bool {
    match kind {
        ProtocolKind::NetflowV5 => netflow_v5_length_ok(packet),
        ProtocolKind::NetflowV9 | ProtocolKind::Ipfix => netflow_v9_ipfix_length_ok(packet),
        ProtocolKind::SflowV5 => sflow_v5_length_ok(packet),
        _ => true,
    }
}

#[inline]
fn netflow_v5_length_ok(packet: &[u8]) -> bool {
    // v5 header is 24 bytes; flow record fixed 48 bytes.
    if packet.len() < 24 {
        return false;
    }
    let count = u16::from_be_bytes([packet[2], packet[3]]) as usize;
    24usize.saturating_add(count.saturating_mul(48)) == packet.len()
}

#[inline]
fn netflow_v9_ipfix_length_ok(packet: &[u8]) -> bool {
    // v9/IPFIX have variable-length flowsets; conservative check: header present.
    packet.len() >= 20
}

#[inline]
fn sflow_v5_length_ok(packet: &[u8]) -> bool {
    // sFlow v5 minimum datagram size: 28 bytes (header + agent_address + sub_agent_id +
    //   sequence_number + uptime + num_samples).
    packet.len() >= 28
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatches_netflow_v5() {
        let pkt = [0x00, 0x05, 0x00, 0x01, 0xAA, 0xBB];
        assert_eq!(dispatch_protocol_kind(&pkt), ProtocolKind::NetflowV5);
    }

    #[test]
    fn dispatches_netflow_v7() {
        let pkt = [0x00, 0x07, 0x00, 0x01, 0xAA, 0xBB];
        assert_eq!(dispatch_protocol_kind(&pkt), ProtocolKind::NetflowV7);
    }

    #[test]
    fn dispatches_netflow_v9() {
        let pkt = [0x00, 0x09, 0x00, 0x01, 0xAA, 0xBB];
        assert_eq!(dispatch_protocol_kind(&pkt), ProtocolKind::NetflowV9);
    }

    #[test]
    fn dispatches_ipfix() {
        let pkt = [0x00, 0x0A, 0x00, 0x10, 0xAA, 0xBB];
        assert_eq!(dispatch_protocol_kind(&pkt), ProtocolKind::Ipfix);
    }

    #[test]
    fn dispatches_sflow_v5() {
        let pkt = [0x00, 0x00, 0x00, 0x05, 0xAA, 0xBB];
        assert_eq!(dispatch_protocol_kind(&pkt), ProtocolKind::SflowV5);
    }

    #[test]
    fn rejects_unknown_version() {
        let pkt = [0x00, 0x42, 0xAA, 0xBB];
        assert_eq!(dispatch_protocol_kind(&pkt), ProtocolKind::Unknown);
    }

    #[test]
    fn rejects_too_short() {
        let pkt = [0x00, 0x05];
        assert_eq!(dispatch_protocol_kind(&pkt), ProtocolKind::TooShort);
    }

    #[test]
    fn netflow_v5_length_check_passes_on_valid() {
        // header (24) + 1 flow (48) = 72
        let mut pkt = vec![0u8; 72];
        pkt[0] = 0x00;
        pkt[1] = 0x05;
        pkt[2] = 0x00;
        pkt[3] = 0x01; // count = 1
        assert!(length_sanity_check(ProtocolKind::NetflowV5, &pkt));
    }

    #[test]
    fn netflow_v5_length_check_fails_on_overflow() {
        // count claims 99 flows in a small packet
        let mut pkt = vec![0u8; 80];
        pkt[0] = 0x00;
        pkt[1] = 0x05;
        pkt[2] = 0x00;
        pkt[3] = 0x63; // count = 99
        assert!(!length_sanity_check(ProtocolKind::NetflowV5, &pkt));
    }

    #[test]
    fn protocol_kind_helpers() {
        assert!(ProtocolKind::NetflowV5.is_netflow_family());
        assert!(ProtocolKind::Ipfix.is_netflow_family());
        assert!(!ProtocolKind::SflowV5.is_netflow_family());
        assert!(ProtocolKind::SflowV5.is_sflow());
        assert!(!ProtocolKind::NetflowV5.is_sflow());
        assert_eq!(ProtocolKind::TooShort.drop_reason(), "too_short");
        assert_eq!(ProtocolKind::Unknown.drop_reason(), "unknown_version");
        assert_eq!(ProtocolKind::NetflowV5.version_tag(), "netflow_v5");
    }
}
