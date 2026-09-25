// Project:   dfe-receiver
// File:      tests/integration/json_only.rs
// Purpose:   A body that is not JSON is refused at the ingest listener
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! JSON is the only payload format: a body that is not JSON, MessagePack
//! included, is refused with a 400 and counted, whatever `dlq_on_invalid` says.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::large_futures)]

use super::json_depth::{ingest_config, start_ingest};

/// POST `body` and hand back the status and the response text.
async fn post(url: &str, content_type: &str, body: Vec<u8>) -> (u16, String) {
    let response = reqwest::Client::new()
        .post(url)
        .header("content-type", content_type)
        .body(body)
        .send()
        .await
        .expect("POST failed");
    let status = response.status().as_u16();
    (status, response.text().await.unwrap())
}

#[tokio::test]
async fn a_body_that_is_not_json_is_refused_and_counted() {
    let config = ingest_config();
    assert!(
        config.validation.dlq_on_invalid,
        "the default dead-letters a record missing a required field"
    );
    let started = start_ingest(config).await;

    // MessagePack for {"foo": 1}, and a body that is plainly not JSON.
    let msgpack = vec![0x81, 0xA3, b'f', b'o', b'o', 0x01];
    for (content_type, body) in [
        ("application/msgpack", msgpack),
        ("application/json", b"not json".to_vec()),
    ] {
        let (status, text) = post(&started.url, content_type, body).await;
        assert_eq!(status, 400, "{content_type}: {text}");
        assert!(text.contains("invalid JSON"), "{content_type}: {text}");
    }
    assert_eq!(started.metrics.get_validation_failures_total(), 2);

    let (status, text) = post(&started.url, "application/json", br#"{"ok":true}"#.to_vec()).await;
    assert_eq!(status, 202, "{text}");

    started.shutdown.cancel();
}
