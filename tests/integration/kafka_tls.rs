// Project:   dfe-receiver
// File:      tests/integration/kafka_tls.rs
// Purpose:   The receiver binary delivers to Kafka over SASL_SSL with a private CA
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A chart delivers a private CA as a file named by `SSL_CERT_FILE`, with no
//! CA in the receiver's own config. The binary runs as its own process, so
//! the variable reaches the Kafka client as it does in a pod.

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use rdkafka::ClientConfig;
use scalo::transport::kafka::test_support::{
    SCRAM_USER, TlsBroker, require_docker, scram_password,
};

use crate::common::peers::free_port;
use crate::common::{SUITE, kafka_consume_next};
use crate::test_name;

/// The receiver binary, killed when dropped.
struct Receiver(Child);

impl Drop for Receiver {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A client of `broker`'s SASL_SSL listener that names the CA itself.
fn sasl_ssl_client(broker: &TlsBroker) -> ClientConfig {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", &broker.sasl_ssl)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", "SCRAM-SHA-512")
        .set("sasl.username", SCRAM_USER)
        .set("sasl.password", scram_password())
        .set("ssl.ca.location", broker.pki.file("ca.crt"));
    client
}

/// A record POSTed to the receiver lands over SASL_SSL, its CA taken from
/// `SSL_CERT_FILE` alone.
#[tokio::test]
async fn a_record_lands_over_sasl_ssl_with_the_ca_from_ssl_cert_file() {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::consumer::{Consumer, StreamConsumer};

    if !require_docker() {
        return;
    }
    let broker = TlsBroker::start(&SUITE, test_name!())
        .await
        .expect("TLS broker");
    let source = format!("tls{}", uuid::Uuid::new_v4().simple());
    let topic = format!("{source}_land");

    let admin: AdminClient<DefaultClientContext> =
        sasl_ssl_client(&broker).create().expect("admin client");
    for created in admin
        .create_topics(
            &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create topics")
    {
        created.expect("topic created");
    }
    let consumer: StreamConsumer = sasl_ssl_client(&broker)
        .set("group.id", format!("test-{}", uuid::Uuid::new_v4()))
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic.as_str()]).expect("subscribe");

    let dir = tempfile::tempdir().expect("config dir");
    let ingest = free_port();
    let config_path = dir.path().join("receiver.yaml");
    let mut config = std::fs::File::create(&config_path).expect("config file");
    write!(
        config,
        r#"server:
  bind_address: "127.0.0.1:{ingest}"
routing:
  default_source: "{source}"
kafka:
  brokers:
    - "{brokers}"
  tls:
    enabled: true
  sasl:
    enabled: true
    mechanism: "SCRAM-SHA-512"
    username: "{user}"
    password: "{password}"
"#,
        brokers = broker.sasl_ssl,
        user = SCRAM_USER,
        password = scram_password(),
    )
    .expect("write config");
    config.flush().expect("flush config");

    let log_path = dir.path().join("receiver.log");
    let log = std::fs::File::create(&log_path).expect("log file");
    let _receiver = Receiver(
        Command::new(env!("CARGO_BIN_EXE_dfe-receiver"))
            .arg("--config")
            .arg(&config_path)
            .arg("--metrics-addr")
            .arg(format!("127.0.0.1:{}", free_port()))
            .arg("run")
            .env("SSL_CERT_FILE", broker.pki.file("ca.crt"))
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("receiver binary starts"),
    );

    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{ingest}/ingest");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut answer = None;
    while tokio::time::Instant::now() < deadline {
        match client
            .post(&url)
            .body(r#"{"over":"sasl_ssl"}"#)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                answer = Some(response.status());
                break;
            }
            Ok(response) => answer = Some(response.status()),
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let landed = kafka_consume_next(&consumer, Duration::from_secs(30)).await;
    let receiver_log = std::fs::read_to_string(&log_path).unwrap_or_default();
    assert!(
        answer.is_some_and(|status| status.is_success()),
        "the receiver answered {answer:?}\n{receiver_log}"
    );
    let landed = landed.unwrap_or_else(|| panic!("the record never landed\n{receiver_log}"));
    let record: serde_json::Value = serde_json::from_slice(&landed).expect("JSON record");
    assert_eq!(record["over"], "sasl_ssl", "{record}");
}
