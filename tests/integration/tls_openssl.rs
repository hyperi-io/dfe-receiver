// Project:   dfe-receiver
// File:      tests/integration/tls_openssl.rs
// Purpose:   Every TLS listener of the receiver binary, checked by openssl s_client in Docker
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `openssl s_client`, in one container, against every TLS listener of the
//! built receiver. The receiver starts through its binary and a config file, so
//! the rustls provider install in `main` is on the path.
//!
//! One receiver runs every listener with `client_auth: none` and takes the
//! server-side cases: a handshake that verifies, a client trusting the wrong CA,
//! and a plaintext client on a TLS port. A second runs them all with
//! `client_auth: required` and takes the client-certificate cases. A syslog line
//! and an HTTP ingest request go through TLS to the destination, so a listener
//! that completes a handshake and then drops the data fails here too.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dfe_receiver::config::{Config, WebhookAuthConfig, WebhookCallerConfig};

use crate::common::peers::{
    Cases, Exec, OPENSSL_IMAGE, Peer, PeerSpec, Pki, Receiver, Sink, free_addr,
};
use crate::{skip_if_no_docker, test_name};

/// One TLS listener of the receiver.
struct Listener {
    name: &'static str,
    addr: SocketAddr,
}

/// A receiver config with TLS on every listener that has it.
///
/// With `client_auth` other than `none`, each listener verifies client certificates against the CA.
fn all_tls_config(pki: &Pki, client_auth: &str, secret: &str) -> (Config, Vec<Listener>) {
    let mut config = Config::default();
    let mut listeners: Vec<Listener> = Vec::new();
    let mut add = |name: &'static str| -> String {
        let addr = free_addr();
        listeners.push(Listener { name, addr });
        addr.to_string()
    };
    let tls = || pki.server_tls(client_auth);
    let unauthenticated = client_auth != "required";

    config.server.bind_address = add("http");
    config.server.auth.mode = "none".to_string();
    config.server.tls = tls();

    config.grpc.enabled = true;
    config.grpc.bind_address = add("grpc");
    config.grpc.tls = tls();

    config.otlp.enabled = true;
    config.otlp.grpc_bind_address = add("otlp-grpc");
    config.otlp.http_bind_address = add("otlp-http");
    config.otlp.auth.mode = "none".to_string();
    config.otlp.tls = tls();

    config.lumberjack.enabled = true;
    config.lumberjack.bind_address = add("lumberjack");
    config.lumberjack.accept_unauthenticated = unauthenticated;
    config.lumberjack.tls = tls();

    config.syslog.enabled = true;
    config.syslog.udp_bind_address = free_addr().to_string();
    config.syslog.tcp_bind_address = free_addr().to_string();
    config.syslog.tls_bind_address = add("syslog-tls");
    config.syslog.accept_unauthenticated = true;
    config.syslog.tls = tls();

    config.splunk_hec.enabled = true;
    config.splunk_hec.bind_address = add("splunk-hec");
    config.splunk_hec.auth.mode = "none".to_string();
    config.splunk_hec.tls = tls();

    config.prometheus_rw.enabled = true;
    config.prometheus_rw.bind_address = add("prometheus-rw");
    config.prometheus_rw.auth.mode = "none".to_string();
    config.prometheus_rw.tls = tls();

    // A webhook caller delivers to a Kafka topic, so the config needs a broker to be valid.
    config.kafka.brokers = vec!["127.0.0.1:1".to_string()];
    config.webhook.enabled = true;
    config.webhook.bind_address = Some(add("webhook"));
    config.webhook.callers = vec![WebhookCallerConfig {
        name: "probe".to_string(),
        topic: "probe".to_string(),
        auth: WebhookAuthConfig {
            mode: dfe_receiver::config::WebhookAuthMode::Header,
            secret_source: format!("file:{secret}"),
            header: "x-probe-secret".to_string(),
            ..WebhookAuthConfig::default()
        },
        ..WebhookCallerConfig::default()
    }];
    config.webhook.tls = tls();

    config.fluent.enabled = true;
    config.fluent.bind_address = add("fluent");
    config.fluent.accept_unauthenticated = unauthenticated;
    config.fluent.tls = tls();

    config.gelf.enabled = true;
    config.gelf.bind_address = add("gelf");
    config.gelf.accept_unauthenticated = unauthenticated;
    config.gelf.tls = tls();

    (config, listeners)
}

/// `openssl s_client` to `addr`, verifying with `ca` and presenting `client` if named.
fn s_client(addr: SocketAddr, ca: &str, client: Option<&str>) -> String {
    let identity = client.map_or_else(String::new, |name| {
        format!("-cert /dfe-test/{name}.crt -key /dfe-test/{name}.key")
    });
    format!(
        "(sleep 1; echo) | timeout 10 openssl s_client -connect {addr} -CAfile /dfe-test/{ca}.crt \
         -verify_ip 127.0.0.1 -verify_return_error {identity}"
    )
}

async fn sh(peer: &Peer, script: &str) -> Exec {
    peer.exec(&["sh", "-c", script]).await
}

/// Run every script in the peer at once and return each one's outcome by key.
async fn sh_all(peer: &Arc<Peer>, jobs: Vec<(String, String)>) -> HashMap<String, Exec> {
    let mut tasks = tokio::task::JoinSet::new();
    for (key, script) in jobs {
        let peer = Arc::clone(peer);
        tasks.spawn(async move { (key, sh(&peer, &script).await) });
    }
    let mut done = HashMap::new();
    while let Some(joined) = tasks.join_next().await {
        let (key, exec) = joined.unwrap();
        done.insert(key, exec);
    }
    done
}

/// A peer that sits idle while commands run in it.
async fn openssl_peer(pki: &Pki, test: &str) -> Arc<Peer> {
    let peer = Peer::start(PeerSpec {
        test,
        service: "openssl",
        image: OPENSSL_IMAGE,
        entrypoint: Some("sleep"),
        args: vec!["3600".to_string()],
        files: Some(pki.dir()),
        user: None,
        env: vec![],
        extra_files: vec![],
    })
    .await
    .unwrap_or_else(|e| panic!("the openssl peer did not start: {e}"));
    Arc::new(peer)
}

#[tokio::test]
async fn every_tls_listener_serves_openssl() {
    skip_if_no_docker!();
    let test = test_name!();
    let pki = Pki::generate().await;
    let peer = openssl_peer(&pki, test).await;
    let sink = Sink::start().await;
    let secret = pki.file("webhook-secret");
    std::fs::write(&secret, "probe-secret").unwrap();

    let (mut receiver, listeners) = Receiver::start(&sink, || {
        let (config, listeners) = all_tls_config(&pki, "none", &secret);
        let ready = listeners.iter().map(|l| l.addr).collect();
        (config, ready, listeners)
    })
    .await
    .unwrap_or_else(|e| panic!("{e}"));

    let mut jobs: Vec<(String, String)> = Vec::new();
    for listener in &listeners {
        let name = listener.name;
        jobs.push((format!("{name}/ok"), s_client(listener.addr, "ca", None)));
        jobs.push((
            format!("{name}/wrong"),
            s_client(listener.addr, "rogue-ca", None),
        ));
        jobs.push((
            format!("{name}/plain"),
            format!(
                "printf 'GET / HTTP/1.0\\r\\n\\r\\n' | timeout 10 nc -w 2 {} {}",
                listener.addr.ip(),
                listener.addr.port()
            ),
        ));
    }
    let done = sh_all(&peer, jobs).await;

    let mut cases = Cases::default();
    for listener in &listeners {
        let name = listener.name;
        let ok = &done[&format!("{name}/ok")];
        cases.check(
            &format!("{name}: handshake verifies against the CA"),
            ok.output.contains("Verify return code: 0 (ok)"),
            || ok.output.clone(),
        );
        let wrong = &done[&format!("{name}/wrong")];
        cases.check(
            &format!("{name}: a client trusting another CA refuses the certificate"),
            wrong.code != Some(0) && wrong.output.contains("verify error"),
            || wrong.output.clone(),
        );
        let plain = &done[&format!("{name}/plain")];
        cases.check(
            &format!("{name}: a plaintext client gets no application answer"),
            !plain.output.contains("HTTP/1."),
            || plain.output.clone(),
        );
    }

    let syslog = listeners.iter().find(|l| l.name == "syslog-tls").unwrap();
    let line = "<14>1 2026-10-28T00:00:00Z probe app 1 - - tlsopenssl-syslog-marker";
    let sent = sh(
        &peer,
        &format!(
            "(printf '%s\\n' '{line}'; sleep 1) | timeout 5 openssl s_client -quiet -connect {} \
             -CAfile /dfe-test/ca.crt -verify_ip 127.0.0.1 -verify_return_error",
            syslog.addr
        ),
    )
    .await;
    cases.check(
        "syslog-tls: a line sent over TLS reaches the destination",
        sink.wait_for("tlsopenssl-syslog-marker", Duration::from_secs(15))
            .await,
        || format!("{}\n{}", sent.output, sink.dump()),
    );

    let http = listeners.iter().find(|l| l.name == "http").unwrap();
    let body = r#"{"event":"tlsopenssl-http-marker"}"#;
    let posted = sh(
        &peer,
        &format!(
            "printf 'POST /ingest HTTP/1.1\\r\\nHost: localhost\\r\\nContent-Type: application/json\\r\\n\
             Content-Length: {}\\r\\nConnection: close\\r\\n\\r\\n{body}' | timeout 5 openssl s_client -quiet \
             -connect {} -CAfile /dfe-test/ca.crt -verify_ip 127.0.0.1 -verify_return_error",
            body.len(),
            http.addr
        ),
    )
    .await;
    cases.check(
        "http: a JSON POST over TLS is accepted and reaches the destination",
        posted.output.contains("HTTP/1.1 202")
            && sink
                .wait_for("tlsopenssl-http-marker", Duration::from_secs(15))
                .await,
        || format!("{}\n{}", posted.output, sink.dump()),
    );

    cases.check("the receiver is still running", receiver.alive(), || {
        receiver.log_text()
    });
    cases.finish(|| format!("receiver log:\n{}", receiver.log_text()));
}

#[tokio::test]
async fn client_certificates_are_required_on_every_mtls_listener() {
    skip_if_no_docker!();
    let test = test_name!();
    let pki = Pki::generate().await;
    let peer = openssl_peer(&pki, test).await;
    let sink = Sink::start().await;
    let secret = pki.file("webhook-secret");
    std::fs::write(&secret, "probe-secret").unwrap();

    let (mut receiver, listeners) = Receiver::start(&sink, || {
        let (config, listeners) = all_tls_config(&pki, "required", &secret);
        let ready = listeners.iter().map(|l| l.addr).collect();
        (config, ready, listeners)
    })
    .await
    .unwrap_or_else(|e| panic!("{e}"));

    let mut jobs: Vec<(String, String)> = Vec::new();
    for listener in &listeners {
        let name = listener.name;
        jobs.push((
            format!("{name}/cert"),
            s_client(listener.addr, "ca", Some("client")),
        ));
        jobs.push((format!("{name}/none"), s_client(listener.addr, "ca", None)));
        jobs.push((
            format!("{name}/rogue"),
            s_client(listener.addr, "ca", Some("rogue-client")),
        ));
    }
    let done = sh_all(&peer, jobs).await;

    let mut cases = Cases::default();
    for listener in &listeners {
        let name = listener.name;
        let with_cert = &done[&format!("{name}/cert")];
        cases.check(
            &format!("{name}: a certificate from the trusted CA is accepted"),
            with_cert.output.contains("Verify return code: 0 (ok)")
                && !with_cert.output.contains("alert"),
            || with_cert.output.clone(),
        );
        let none = &done[&format!("{name}/none")];
        cases.check(
            &format!("{name}: no client certificate is turned away"),
            none.output.contains("alert"),
            || none.output.clone(),
        );
        let rogue = &done[&format!("{name}/rogue")];
        cases.check(
            &format!("{name}: a certificate from another CA is turned away"),
            rogue.output.contains("alert"),
            || rogue.output.clone(),
        );
    }
    cases.check("the receiver is still running", receiver.alive(), || {
        receiver.log_text()
    });
    cases.finish(|| format!("receiver log:\n{}", receiver.log_text()));
}
