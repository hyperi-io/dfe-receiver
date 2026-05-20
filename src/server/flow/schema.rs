//! Canonical flow-record schema.
//!
//! Field names and types are stable across protocol versions; downstream
//! consumers see one shape regardless of wire format (NetFlow v5/v7/v9,
//! IPFIX, sFlow v5).
//!
//! `record_kind` discriminator:
//! - "flow": standard flow record (most NetFlow + sFlow data)
//! - "counter": sFlow counter sample (interface statistics)
//! - "security_event": NSEL firewall event (Cisco ASA firewallEvent / NF_F_FW_EVENT)
//! - "nat_translation": NAT44 translation event (CGNAT, natEvent IE)

use serde::Serialize;
use std::net::IpAddr;

#[derive(Debug, Clone, Serialize)]
pub struct CanonicalRecord {
    /// "flow" | "security_event" (NSEL) | "nat_translation" (NAT44/CGNAT)
    pub record_kind: &'static str,

    // Core flow fields (populated for all record_kind values where present)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src_ip: Option<IpAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dst_ip: Option<IpAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src_port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dst_port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_version: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub packets: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub t_flow_start: Option<String>, // RFC 3339
    #[serde(skip_serializing_if = "Option::is_none")]
    pub t_flow_end: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_flags: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_iface: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_iface: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src_as: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dst_as: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_hop: Option<IpAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampling_rate: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vlan_id: Option<u16>,

    // NSEL (record_kind="security_event") -- firewallEvent / NF_F_FW_EVENT triggered
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_type: Option<u8>, // 1=created, 2=deleted, 3=denied, 4=teardown, 5=updated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_subtype: Option<u16>, // NF_F_FW_EXT_EVENT (33002)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>, // NF_F_USERNAME (40000)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acl_in_id: Option<String>, // NF_F_INGRESS_ACL_ID (33000)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acl_out_id: Option<String>, // NF_F_EGRESS_ACL_ID (33001)

    // NAT44 (record_kind="nat_translation") -- natEvent IE present
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nat_event_type: Option<u8>, // natEvent (230)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pre_nat_src_ip: Option<IpAddr>, // postNATSourceIPv4Address (225)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pre_nat_dst_ip: Option<IpAddr>, // postNATDestinationIPv4Address (226)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pre_nat_src_port: Option<u16>, // postNAPTSourceTransportPort (227)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pre_nat_dst_port: Option<u16>, // postNAPTDestinationTransportPort (228)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nat_pool_name: Option<String>, // natPoolName (283)
}

#[derive(Debug, Clone, Serialize)]
pub struct CanonicalCounterRecord {
    pub record_kind: &'static str, // "counter"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_speed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_in_octets: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_in_packets: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_in_errors: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_in_discards: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_out_octets: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_out_packets: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_out_errors: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_out_discards: Option<u64>,
}

/// Wire-protocol-agnostic record envelope (used in envelope rendering).
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum AnyRecord {
    Flow(CanonicalRecord),
    Counter(CanonicalCounterRecord),
}

impl CanonicalRecord {
    pub fn empty() -> Self {
        Self {
            record_kind: "flow",
            src_ip: None,
            dst_ip: None,
            src_port: None,
            dst_port: None,
            protocol: None,
            ip_version: None,
            bytes: None,
            packets: None,
            t_flow_start: None,
            t_flow_end: None,
            duration_ms: None,
            tcp_flags: None,
            input_iface: None,
            output_iface: None,
            src_as: None,
            dst_as: None,
            next_hop: None,
            sampling_rate: None,
            vlan_id: None,
            // NSEL
            event_type: None,
            event_subtype: None,
            username: None,
            acl_in_id: None,
            acl_out_id: None,
            // NAT44
            nat_event_type: None,
            pre_nat_src_ip: None,
            pre_nat_dst_ip: None,
            pre_nat_src_port: None,
            pre_nat_dst_port: None,
            nat_pool_name: None,
        }
    }
}

impl CanonicalCounterRecord {
    pub fn empty() -> Self {
        Self {
            record_kind: "counter",
            if_index: None,
            if_speed: None,
            if_in_octets: None,
            if_in_packets: None,
            if_in_errors: None,
            if_in_discards: None,
            if_out_octets: None,
            if_out_packets: None,
            if_out_errors: None,
            if_out_discards: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_record_serialises_to_json() {
        let mut r = CanonicalRecord::empty();
        r.src_ip = Some("10.0.0.1".parse().unwrap());
        r.dst_ip = Some("10.0.0.2".parse().unwrap());
        r.bytes = Some(1500);
        r.packets = Some(1);
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains(r#""record_kind":"flow""#));
        assert!(json.contains(r#""src_ip":"10.0.0.1""#));
        assert!(json.contains(r#""bytes":1500"#));
        assert!(!json.contains("\"tcp_flags\"")); // None fields skipped
        assert!(!json.contains("\"event_type\""));
        assert!(!json.contains("\"nat_event_type\""));
    }

    #[test]
    fn security_event_record_serialises_to_json() {
        let mut r = CanonicalRecord::empty();
        r.record_kind = "security_event";
        r.src_ip = Some("10.0.0.1".parse().unwrap());
        r.event_type = Some(3); // denied
        r.username = Some("alice".into());
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains(r#""record_kind":"security_event""#));
        assert!(json.contains(r#""event_type":3"#));
        assert!(json.contains(r#""username":"alice""#));
    }

    #[test]
    fn nat_translation_record_serialises_to_json() {
        let mut r = CanonicalRecord::empty();
        r.record_kind = "nat_translation";
        r.src_ip = Some("203.0.113.5".parse().unwrap()); // public
        r.pre_nat_src_ip = Some("10.0.0.42".parse().unwrap()); // private subscriber
        r.nat_event_type = Some(1); // create
        r.pre_nat_src_port = Some(54321);
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains(r#""record_kind":"nat_translation""#));
        assert!(json.contains(r#""pre_nat_src_ip":"10.0.0.42""#));
        assert!(json.contains(r#""nat_event_type":1"#));
    }

    #[test]
    fn counter_record_serialises_to_json() {
        let mut c = CanonicalCounterRecord::empty();
        c.if_index = Some(5);
        c.if_in_octets = Some(1_000_000);
        let json = serde_json::to_string(&c).unwrap();
        assert!(json.contains(r#""record_kind":"counter""#));
        assert!(json.contains(r#""if_in_octets":1000000"#));
        assert!(json.contains(r#""if_index":5"#));
    }

    #[test]
    fn any_record_serialises_untagged() {
        let r = AnyRecord::Flow(CanonicalRecord::empty());
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains(r#""record_kind":"flow""#));

        let c = AnyRecord::Counter(CanonicalCounterRecord::empty());
        let json = serde_json::to_string(&c).unwrap();
        assert!(json.contains(r#""record_kind":"counter""#));
    }
}
