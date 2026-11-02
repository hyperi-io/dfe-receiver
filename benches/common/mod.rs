// Project:   dfe-receiver
// File:      benches/common/mod.rs
// Purpose:   Syslog line corpus shared by the syslog benches
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

/// Device lines in the proportions a mixed estate sends: RFC 3164 from network
/// gear and Unix hosts, RFC 5424 from modern agents, and CEF over syslog.
pub const CORPUS: &[(&str, &str)] = &[
    (
        "3164_bsd",
        "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8",
    ),
    (
        "3164_pid",
        "<38>Oct 11 22:14:15 web01 sshd[123]: Accepted publickey for ops from 10.0.0.5 port 50022 ssh2",
    ),
    (
        "3164_cisco_ios",
        "<189>Mar  1 18:46:11.407 EST: %LINEPROTO-5-UPDOWN: Line protocol on Interface Vlan1, changed state to up",
    ),
    (
        "3164_panos",
        "<10>Jan  5 05:58:47 PAN1 1,2015/01/05 05:58:46,009401011564,THREAT,url,1,2015/01/05 05:58:46,10.0.0.1",
    ),
    (
        "3164_fortigate",
        "<189>Aug 05 06:28:35 192.168.250.1 date=2016-08-05 time=06:28:35 devname=gotham-fortigate logid=0100026003 type=event subtype=system level=notice",
    ),
    (
        "5424_sd",
        r#"<165>1 2026-03-03T10:30:00.123+11:00 web01 nginx 1234 ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] GET /index.html 200"#,
    ),
    (
        "5424_plain",
        "<14>1 2026-01-15T12:00:00Z myhost myapp 5678 - - Application started on port 8080",
    ),
    (
        "cef",
        "<134>Oct 11 22:14:15 fw01 CEF:0|Security|threatmanager|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232 dpt=443 act=blocked",
    ),
];

/// The corpus as one LF-framed TCP stream, repeated `times`.
pub fn lf_stream(times: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for _ in 0..times {
        for (_, line) in CORPUS {
            out.extend_from_slice(line.as_bytes());
            out.push(b'\n');
        }
    }
    out
}

/// The corpus as one octet-counted TCP stream, repeated `times`.
pub fn octet_stream(times: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for _ in 0..times {
        for (_, line) in CORPUS {
            out.extend_from_slice(format!("{} {line}", line.len()).as_bytes());
        }
    }
    out
}
