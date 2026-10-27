// Project:   dfe-receiver
// File:      src/server/syslog/convert.rs
// Purpose:   Syslog message parsing and conversion to pipeline JSON
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Syslog message format conversion.
//!
//! Parses syslog messages (RFC 5424 + RFC 3164 auto-detect) using `syslog_loose`
//! and converts them to pipeline-ready JSON payloads.

use bytes::Bytes;
use chrono::Timelike;
use serde_json::Map;

use crate::config::RawCapture;
use crate::error::{Error, Result};
use crate::server::raw_capture;

/// The `_source` this listener stamps on every record it builds.
pub const SOURCE: &str = "syslog";

/// Convert a raw syslog message string to pipeline-ready JSON bytes.
///
/// Uses `syslog_loose::parse_message` which auto-detects RFC 5424 vs RFC 3164.
/// The `_source` field is set to `"syslog"` for routing.
///
/// With `raw` capture enabled the parsed fields are joined by `_raw` holding
/// the wire line verbatim -- the PRI, the header and the original spacing,
/// none of which survives the parse. It matters most on the lines that parse
/// badly: `syslog_loose` never fails, so a malformed line silently lands its
/// whole text in `message` and is indistinguishable from a clean parse.
pub fn syslog_to_json(raw: &str, raw_capture: RawCapture) -> Result<Bytes> {
    let mut msg = syslog_loose::parse_message(raw, syslog_loose::Variant::Either);
    if matches!(msg.protocol, syslog_loose::Protocol::RFC3164) {
        settle_3164_header(raw, &mut msg);
    }

    let mut obj = Map::new();

    // Message body (always present)
    obj.insert(
        "message".to_string(),
        serde_json::Value::String(msg.msg.to_string()),
    );

    // Facility
    if let Some(facility) = msg.facility {
        obj.insert(
            "facility".to_string(),
            serde_json::Value::String(facility.as_str().to_string()),
        );
    }

    // Severity
    if let Some(severity) = msg.severity {
        obj.insert(
            "severity".to_string(),
            serde_json::Value::String(severity.as_str().to_string()),
        );
    }

    // Timestamp (RFC 3339 string)
    if let Some(ts) = msg.timestamp {
        obj.insert(
            "timestamp".to_string(),
            serde_json::Value::String(ts.to_rfc3339()),
        );
    }

    // Hostname
    if let Some(hostname) = msg.hostname {
        obj.insert(
            "hostname".to_string(),
            serde_json::Value::String(hostname.to_string()),
        );
    }

    // Application name
    if let Some(appname) = msg.appname {
        obj.insert(
            "appname".to_string(),
            serde_json::Value::String(appname.to_string()),
        );
    }

    // Process ID
    if let Some(ref procid) = msg.procid {
        obj.insert(
            "procid".to_string(),
            serde_json::Value::String(procid.to_string()),
        );
    }

    // Message ID (RFC 5424)
    if let Some(msgid) = msg.msgid {
        obj.insert(
            "msgid".to_string(),
            serde_json::Value::String(msgid.to_string()),
        );
    }

    // Structured data (RFC 5424)
    if !msg.structured_data.is_empty() {
        let sd: Vec<serde_json::Value> = msg
            .structured_data
            .iter()
            .map(|elem| {
                let mut sd_obj = Map::new();
                sd_obj.insert(
                    "id".to_string(),
                    serde_json::Value::String(elem.id.to_string()),
                );
                let params: Map<String, serde_json::Value> = elem
                    .params
                    .iter()
                    .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
                    .collect();
                sd_obj.insert("params".to_string(), serde_json::Value::Object(params));
                serde_json::Value::Object(sd_obj)
            })
            .collect();
        obj.insert("structured_data".to_string(), serde_json::Value::Array(sd));
    }

    // Source tag for routing
    obj.insert(
        "_source".to_string(),
        serde_json::Value::String(SOURCE.to_string()),
    );

    raw_capture::attach_str_to_map(&mut obj, raw, raw_capture);

    serde_json::to_vec(&obj)
        .map(Bytes::from)
        .map_err(|e| Error::Validation(format!("syslog JSON serialisation failed: {e}")))
}

/// Correct the two places `syslog_loose` splits an RFC 3164 header wrongly.
///
/// It takes the token after the host as the tag whatever it holds, so a
/// tagless PAN-OS line gets `1,2015/01/05` as its app name. And it stops a
/// Cisco stamp at the seconds, leaving `.407 EST: ` at the front of the body.
fn settle_3164_header<'a>(raw: &'a str, msg: &mut syslog_loose::Message<&'a str>) {
    if let Some(app) = msg.appname {
        if let Some(body) = untagged_body(raw, app) {
            msg.appname = None;
            msg.procid = None;
            // Anything parsed as structured data sits inside `body` already.
            msg.structured_data.clear();
            msg.msg = body;
        }
    } else if msg.hostname.is_none()
        && msg.timestamp.is_some()
        && let Some((nanos, body)) = cisco_stamp_tail(msg.msg)
    {
        msg.timestamp = msg
            .timestamp
            .map(|ts| ts.with_nanosecond(nanos).unwrap_or(ts));
        msg.msg = body;
    }
}

/// The body from the tag token on, when that token is not a tag.
///
/// `app` is a slice of `raw` that starts at the token.
fn untagged_body<'a>(raw: &'a str, app: &str) -> Option<&'a str> {
    let line = raw.trim_end();
    let start = app.as_ptr().addr().checked_sub(line.as_ptr().addr())?;
    let body = line.get(start..)?;
    let token = &body[..body.find(char::is_whitespace).unwrap_or(body.len())];
    (!is_tag(token)).then_some(body)
}

/// RFC 3164 tag grammar: `name:` or `name[pid]:`, the name drawn from ASCII
/// alphanumerics and `-_./`.
fn is_tag(token: &str) -> bool {
    let Some(tag) = token.strip_suffix(':') else {
        return false;
    };
    let name = match tag.strip_suffix(']').and_then(|t| t.split_once('[')) {
        Some((name, pid)) if !pid.is_empty() && !pid.contains(['[', ']']) => name,
        Some(_) => return false,
        None => tag,
    };
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/'))
}

/// The nanoseconds and body after a Cisco stamp's `.407 EST: ` or `.407: `.
///
/// The zone abbreviation is dropped: `EST` and the like do not name one offset.
fn cisco_stamp_tail(body: &str) -> Option<(u32, &str)> {
    let fraction = body.strip_prefix('.')?;
    let digits = fraction.bytes().take_while(u8::is_ascii_digit).count();
    if !(1..=9).contains(&digits) {
        return None;
    }
    let scale = 10u32.pow(9 - u32::try_from(digits).ok()?);
    let nanos = fraction[..digits].parse::<u32>().ok()? * scale;
    let mut rest = &fraction[digits..];
    if let Some(zone) = rest.strip_prefix(' ') {
        let len = zone.bytes().take_while(u8::is_ascii_alphanumeric).count();
        if len == 0 {
            return None;
        }
        rest = &zone[len..];
    }
    let body = rest.strip_prefix(':')?;
    (body.is_empty() || body.starts_with(char::is_whitespace)).then(|| (nanos, body.trim_start()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_rfc5424_with_structured_data() {
        let raw = r#"<165>1 2026-03-03T10:30:00.123+11:00 web01 nginx 1234 ID47 [exampleSDID@32473 iut="3" eventSource="Application"] This is a test message"#;
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(json["message"], "This is a test message");
        assert_eq!(json["hostname"], "web01");
        assert_eq!(json["appname"], "nginx");
        assert_eq!(json["procid"], "1234");
        assert_eq!(json["msgid"], "ID47");
        assert_eq!(json["_source"], "syslog");

        // Facility 20 (local4) and severity 5 (notice)
        assert!(json["facility"].is_string());
        assert!(json["severity"].is_string());

        // Structured data
        let sd = json["structured_data"].as_array().unwrap();
        assert_eq!(sd.len(), 1);
        assert_eq!(sd[0]["id"], "exampleSDID@32473");
        assert_eq!(sd[0]["params"]["iut"], "3");
        assert_eq!(sd[0]["params"]["eventSource"], "Application");
    }

    #[test]
    fn test_rfc3164_bsd_format() {
        let raw = "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(json["hostname"], "mymachine");
        assert_eq!(json["appname"], "su");
        assert_eq!(json["_source"], "syslog");
        // RFC 3164 has no structured data
        assert!(json.get("structured_data").is_none());
        // RFC 3164 has no msgid
        assert!(json.get("msgid").is_none());
    }

    #[test]
    fn test_minimal_rfc3164() {
        // Minimal RFC 3164 with timestamp, hostname, and app
        let raw = "<13>Mar  3 10:00:00 myhost myapp: Hello world";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert!(json["message"].as_str().unwrap().contains("Hello world"));
        assert_eq!(json["hostname"], "myhost");
        assert_eq!(json["_source"], "syslog");
        // Facility 1 (user), severity 5 (notice) — PRI 13
        assert_eq!(json["facility"], "user");
        assert_eq!(json["severity"], "notice");
    }

    #[test]
    fn test_no_priority() {
        // Message without priority tag
        let raw = "Just a plain log message";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert!(
            json["message"]
                .as_str()
                .unwrap()
                .contains("Just a plain log message")
        );
        assert_eq!(json["_source"], "syslog");
        // No facility/severity without PRI
        assert!(json.get("facility").is_none());
        assert!(json.get("severity").is_none());
    }

    #[test]
    fn test_rfc5424_no_structured_data() {
        let raw = "<14>1 2026-01-15T12:00:00Z myhost myapp 5678 - - Application started";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(json["hostname"], "myhost");
        assert_eq!(json["appname"], "myapp");
        assert_eq!(json["procid"], "5678");
        assert!(json.get("structured_data").is_none());
    }

    /// One device line and the header split the receiver must give it.
    struct HeaderCase {
        name: &'static str,
        raw: &'static str,
        hostname: Option<&'static str>,
        appname: Option<&'static str>,
        procid: Option<&'static str>,
        message: &'static str,
    }

    const fn case(
        name: &'static str,
        raw: &'static str,
        hostname: Option<&'static str>,
        appname: Option<&'static str>,
        procid: Option<&'static str>,
        message: &'static str,
    ) -> HeaderCase {
        HeaderCase {
            name,
            raw,
            hostname,
            appname,
            procid,
            message,
        }
    }

    const HEADER_CASES: &[HeaderCase] = &[
        // A token is the RFC 3164 tag only when it reads `name:` or `name[pid]:`.
        case(
            "panos_tagless",
            "<10>Jan  5 05:58:47 PAN1 1,2015/01/05 05:58:46,009401011564,THREAT,url,1,2015/01/05 05:58:46,10.0.0.1",
            Some("PAN1"),
            None,
            None,
            "1,2015/01/05 05:58:46,009401011564,THREAT,url,1,2015/01/05 05:58:46,10.0.0.1",
        ),
        case(
            "panos_tagless_fqdn_host",
            "<11>Jan  5 05:38:59 PAN1.exampleCustomer.com 1,2015/01/05 05:38:58,0006C110285,THREAT,vulnerability,1",
            Some("PAN1.exampleCustomer.com"),
            None,
            None,
            "1,2015/01/05 05:38:58,0006C110285,THREAT,vulnerability,1",
        ),
        case(
            "cisco_ios_msec_timezone",
            "<189>Mar  1 18:46:11.407 EST: %LINEPROTO-5-UPDOWN: Line protocol on Interface Vlan1, changed state to up",
            None,
            None,
            None,
            "%LINEPROTO-5-UPDOWN: Line protocol on Interface Vlan1, changed state to up",
        ),
        case(
            "cisco_ios_msec_no_timezone",
            "<189>Jun  1 12:50:26.172: %LINEPROTO-5-UPDOWN: Line protocol on Interface FastEthernet0/2, changed state to down",
            None,
            None,
            None,
            "%LINEPROTO-5-UPDOWN: Line protocol on Interface FastEthernet0/2, changed state to down",
        ),
        case(
            "cisco_asa_device",
            "<166>Oct 11 22:14:15 asa01 %ASA-6-302021: Teardown ICMP connection for faddr 10.0.0.1/0 gaddr 10.0.0.2/0 laddr 10.0.0.2/0",
            Some("asa01"),
            None,
            None,
            "%ASA-6-302021: Teardown ICMP connection for faddr 10.0.0.1/0 gaddr 10.0.0.2/0 laddr 10.0.0.2/0",
        ),
        case(
            "cisco_asa_behind_a_tag",
            "<134>Oct 28 10:00:00 relay relay: %ASA-6-302021: Teardown ICMP connection for faddr 10.0.0.1/0",
            Some("relay"),
            Some("relay"),
            None,
            "%ASA-6-302021: Teardown ICMP connection for faddr 10.0.0.1/0",
        ),
        case(
            "fortigate_key_value",
            "<189>Aug 05 06:28:35 192.168.250.1 date=2016-08-05 time=06:28:35 devname=gotham-fortigate logid=0100026003 type=event",
            Some("192.168.250.1"),
            None,
            None,
            "date=2016-08-05 time=06:28:35 devname=gotham-fortigate logid=0100026003 type=event",
        ),
        case(
            "tag_with_pid",
            "<38>Oct 11 22:14:15 web01 sshd[123]: Accepted publickey for ops from 10.0.0.5 port 50022 ssh2",
            Some("web01"),
            Some("sshd"),
            Some("123"),
            "Accepted publickey for ops from 10.0.0.5 port 50022 ssh2",
        ),
        case(
            "tag_with_slash_and_pid",
            "<22>Oct 11 22:14:15 mail01 postfix/smtpd[4242]: connect from unknown[10.0.0.9]",
            Some("mail01"),
            Some("postfix/smtpd"),
            Some("4242"),
            "connect from unknown[10.0.0.9]",
        ),
        case(
            "tag_with_dot_dash_underscore",
            "<78>Oct 11 22:14:15 host1 run-parts.cron_daily: starting",
            Some("host1"),
            Some("run-parts.cron_daily"),
            None,
            "starting",
        ),
        case(
            "tag_with_empty_body",
            "<13>Oct 11 22:14:15 host1 app:",
            Some("host1"),
            Some("app"),
            None,
            "",
        ),
        case(
            "token_without_colon",
            "<131>Jun  8 11:54:08 master apache_error [Tue Jun 08 11:54:08.929301 2021] [php7:emerg] rest",
            Some("master"),
            None,
            None,
            "apache_error [Tue Jun 08 11:54:08.929301 2021] [php7:emerg] rest",
        ),
        case(
            "pid_without_colon",
            "<13>Oct 11 22:14:15 host1 app[12] no colon after the pid",
            Some("host1"),
            None,
            None,
            "app[12] no colon after the pid",
        ),
        case(
            "colon_inside_token",
            "<13>Oct 11 22:14:15 host1 https://example.test/x fetched",
            Some("host1"),
            None,
            None,
            "https://example.test/x fetched",
        ),
        case(
            "pid_without_name",
            "<13>Oct 11 22:14:15 host1 [12]: orphan pid",
            Some("host1"),
            None,
            None,
            "[12]: orphan pid",
        ),
        case(
            "non_ascii_token",
            "<13>Oct 11 22:14:15 host1 caf\u{e9}: body",
            Some("host1"),
            None,
            None,
            "caf\u{e9}: body",
        ),
        // RFC 5424 names its fields by position, so the tag grammar does not apply.
        case(
            "rfc5424",
            "<14>1 2026-01-15T12:00:00Z myhost my%app 5678 - - Application started",
            Some("myhost"),
            Some("my%app"),
            Some("5678"),
            "Application started",
        ),
        case(
            "no_header",
            "Just a plain log message",
            None,
            None,
            None,
            "Just a plain log message",
        ),
        case(
            "bad_pri",
            "<999 not really syslog at all",
            None,
            None,
            None,
            "<999 not really syslog at all",
        ),
        case("empty", "", None, None, None, ""),
    ];

    #[test]
    fn header_split_follows_the_tag_grammar() {
        let mut failures = Vec::new();
        for c in HEADER_CASES {
            let json: serde_json::Value =
                serde_json::from_slice(&syslog_to_json(c.raw, RawCapture::OFF).unwrap()).unwrap();
            let got = (
                json.get("hostname").and_then(|v| v.as_str()),
                json.get("appname").and_then(|v| v.as_str()),
                json.get("procid").and_then(|v| v.as_str()),
                json["message"].as_str().unwrap_or("<missing>"),
            );
            let want = (c.hostname, c.appname, c.procid, c.message);
            if got != want {
                failures.push(format!("{}: got {got:?}, want {want:?}", c.name));
            }
        }
        assert!(failures.is_empty(), "\n{}", failures.join("\n"));
    }

    #[test]
    fn cisco_stamp_fraction_lands_in_the_timestamp() {
        let raw = "<189>Mar  1 18:46:11.407 EST: %LINEPROTO-5-UPDOWN: Line protocol on Interface Vlan1, changed state to up";
        let json: serde_json::Value =
            serde_json::from_slice(&syslog_to_json(raw, RawCapture::OFF).unwrap()).unwrap();
        let ts = json["timestamp"].as_str().unwrap();
        assert!(ts.contains(":11.407"), "timestamp {ts}");
    }

    proptest::proptest! {
        #[test]
        fn any_line_converts_and_keeps_its_body(tail in "\\PC{0,64}") {
            let raw = format!("<13>Oct 11 22:14:15 host1 {tail}");
            let json: serde_json::Value =
                serde_json::from_slice(&syslog_to_json(&raw, RawCapture::OFF).unwrap()).unwrap();
            let message = json["message"].as_str().unwrap();
            proptest::prop_assert!(raw.contains(message));
        }
    }

    #[test]
    fn test_source_field_always_set() {
        let raw = "<0>test";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["_source"], "syslog");
    }

    #[test]
    fn test_timestamp_is_rfc3339() {
        let raw = "<14>1 2026-03-03T10:30:00+11:00 host app - - - test";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

        let ts = json["timestamp"].as_str().unwrap();
        // Should parse as valid chrono DateTime
        assert!(chrono::DateTime::parse_from_rfc3339(ts).is_ok());
    }

    // -----------------------------------------------------------------------
    // Raw capture
    // -----------------------------------------------------------------------

    #[test]
    fn capture_off_emits_no_raw_field() {
        let raw = "<34>Oct 11 22:14:15 mymachine su: failed";
        let result = syslog_to_json(raw, RawCapture::OFF).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!(json.get("_raw").is_none());
    }

    #[test]
    fn capture_adds_raw_without_disturbing_the_parsed_fields() {
        let raw =
            r#"<165>1 2026-03-03T10:30:00.123+11:00 web01 nginx 1234 ID47 [ex@1 a="b"] hello"#;
        let json: serde_json::Value =
            serde_json::from_slice(&syslog_to_json(raw, RawCapture::on()).unwrap()).unwrap();

        // Every parsed field is still exactly what it was without capture.
        assert_eq!(json["message"], "hello");
        assert_eq!(json["hostname"], "web01");
        assert_eq!(json["appname"], "nginx");
        assert_eq!(json["procid"], "1234");
        assert_eq!(json["msgid"], "ID47");
        assert_eq!(json["_source"], "syslog");
        // _raw is an addition alongside them, byte-for-byte off the wire.
        assert_eq!(json["_raw"], raw);
    }

    #[test]
    fn raw_keeps_the_pri_that_the_parse_discards() {
        // The PRI number itself appears nowhere in the parsed envelope --
        // only the decoded facility/severity names do. Recovering "<165>"
        // is the point of capturing.
        let raw = "<165>1 2026-03-03T10:30:00Z web01 nginx - - - hello";
        let json: serde_json::Value =
            serde_json::from_slice(&syslog_to_json(raw, RawCapture::on()).unwrap()).unwrap();

        assert_eq!(json["facility"], "local4");
        assert_eq!(json["severity"], "notice");
        assert!(json["_raw"].as_str().unwrap().starts_with("<165>"));
    }

    #[test]
    fn raw_distinguishes_a_malformed_line_from_a_clean_parse() {
        // syslog_loose never errors: this lands whole in `message`, which is
        // indistinguishable from a well-formed message body. _raw is what
        // tells the two apart downstream.
        let raw = "<999 not really syslog at all";
        let json: serde_json::Value =
            serde_json::from_slice(&syslog_to_json(raw, RawCapture::on()).unwrap()).unwrap();

        assert!(json.get("facility").is_none());
        assert_eq!(json["_raw"], raw);
    }

    #[test]
    fn oversized_line_truncates_and_flags() {
        let raw = "<13>Mar  3 10:00:00 myhost myapp: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let cfg = RawCapture {
            enabled: true,
            max_bytes: 16,
            on_oversize: crate::config::OversizePolicy::Truncate,
        };
        let json: serde_json::Value =
            serde_json::from_slice(&syslog_to_json(raw, cfg).unwrap()).unwrap();

        assert_eq!(json["_raw"], &raw[..16]);
        assert_eq!(json["_raw_truncated"], true);
        // The parsed fields are unaffected by the cap on _raw.
        assert_eq!(json["hostname"], "myhost");
    }

    #[test]
    fn oversized_line_can_be_dropped_instead() {
        let raw = "<13>Mar  3 10:00:00 myhost myapp: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let cfg = RawCapture {
            enabled: true,
            max_bytes: 16,
            on_oversize: crate::config::OversizePolicy::Omit,
        };
        let json: serde_json::Value =
            serde_json::from_slice(&syslog_to_json(raw, cfg).unwrap()).unwrap();

        assert!(json.get("_raw").is_none());
        assert!(json.get("_raw_truncated").is_none());
        assert_eq!(json["hostname"], "myhost");
    }
}
