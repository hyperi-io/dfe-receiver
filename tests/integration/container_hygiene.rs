// Project:   dfe-receiver
// File:      tests/integration/container_hygiene.rs
// Purpose:   Pin the test-container naming and cleanup convention
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The naming and cleanup convention for containers this suite starts.
//!
//! Several runs share a developer machine, so a container has to say what it is,
//! which suite started it, and whose run owns it -- otherwise `docker ps` shows a
//! wall of random hex and nobody can tell what is safe to remove. These tests
//! pin the scheme so it cannot drift back to testcontainers' defaults.

// A failed docker inspect IS the finding here, so the panic message is the
// report. Matches the other container-backed modules in this binary.
#![allow(clippy::expect_used)]
// The test constant sits with the test it names rather than at module scope.
#![allow(clippy::items_after_statements)]

use crate::common::{OPENBAO_TAG, TEST_SUITE_LABEL, claim_container_name, container_name};
use crate::{skip_if_no_docker, test_name};

/// A per-test instance carries the test name between the suite and the service,
/// so two tests owning their own container do not collide.
#[test]
fn per_test_container_name_includes_the_test() {
    let name = container_name(Some("kafka_sink_roundtrip"), "kafka");
    assert_eq!(
        name,
        "dfe-receiver-test-integration-kafka-sink-roundtrip-kafka"
    );
}

/// The binary-scoped form, for a container started once for a whole test binary.
#[test]
fn binary_scoped_container_name_omits_the_test() {
    let name = container_name(None, "openbao");
    assert_eq!(name, "dfe-receiver-test-integration-openbao");
}

/// Two tests asking for the same service must get DIFFERENT names.
///
/// nextest runs each test in its own process, so the 13 tests calling
/// `kafka_backend` start 13 brokers -- they do not share one. On a single name the
/// first create wins and the rest fail the start, take their `else` branch and
/// skip. Green, testing nothing.
#[test]
fn two_tests_wanting_one_service_do_not_collide() {
    assert_ne!(
        container_name(Some("first_test"), "kafka"),
        container_name(Some("second_test"), "kafka"),
    );
}

/// `test_name!()` must report the test it expands in, not the helper it is passed
/// to and not the module.
///
/// This is what makes the 16 call sites distinct without 16 hand-written literals
/// that drift as tests get renamed. If the macro ever resolved to something
/// constant, every container would collide again -- so the property is asserted
/// rather than assumed.
#[test]
fn test_name_reports_the_calling_test() {
    assert_eq!(test_name!(), "test_name_reports_the_calling_test");
}

/// The same, from an async test: an `async fn` body compiles to a generated
/// future, so the raw path carries `::{{closure}}` and has to be trimmed. Every
/// real caller of `test_name!` here is an async test, so this is the case that
/// actually matters.
#[tokio::test]
async fn test_name_reports_the_calling_test_when_async() {
    assert_eq!(
        test_name!(),
        "test_name_reports_the_calling_test_when_async"
    );
}

/// Docker only accepts `[a-zA-Z0-9][a-zA-Z0-9_.-]*`. A Rust test path carries
/// colons, which would be rejected at create time as what looks like a Docker
/// fault, so they are normalised here instead.
#[test]
fn container_name_is_a_legal_docker_name() {
    let name = container_name(Some("vault_auth::test_approle"), "OpenBao");
    assert_eq!(
        name,
        "dfe-receiver-test-integration-vault-auth--test-approle-openbao"
    );

    let legal = |s: &str| {
        let mut chars = s.chars();
        chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
            && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    };
    assert!(legal(&name), "{name} is not a legal Docker container name");
    assert!(legal(&container_name(None, "kafka")));
}

/// Every name is prefixed so one `docker ps` filter finds everything this repo's
/// suite started, and nothing belonging to another repo.
#[test]
fn names_share_one_greppable_prefix() {
    for name in [
        container_name(None, "kafka"),
        container_name(None, "openbao"),
        container_name(Some("some_test"), "kafka"),
    ] {
        assert!(
            name.starts_with("dfe-receiver-test-integration-"),
            "{name} does not carry the suite prefix"
        );
    }
}

/// The label is what makes a bulk sweep possible when a run was killed and the
/// names are not known: `docker rm -f $(docker ps -aq --filter label=...)`.
#[test]
fn suite_label_identifies_this_repo() {
    assert_eq!(TEST_SUITE_LABEL.0, "io.hyperi.test.suite");
    assert_eq!(TEST_SUITE_LABEL.1, "dfe-receiver-integration");
}

/// A REAL container carries the name and labels, and is gone afterwards.
///
/// The tests above only exercise `container_name`, which proves the helper
/// returns the right string -- not that anything calls it. A convention that is
/// never wired to a container is decoration, so this asks Docker what actually
/// got created, and then that it was removed.
///
/// Uses OpenBao rather than Kafka: it comes up faster, and
/// `start_vault_container` always starts one, so there is no live-service path
/// that could turn this into a silent skip.
#[tokio::test]
async fn a_started_container_carries_the_name_and_label_then_goes_away() {
    skip_if_no_docker!();

    const TEST: &str = "container-hygiene-inspects-a-real-one";
    let expected = container_name(Some(TEST), "openbao");

    let inspect = |field: &str| {
        std::process::Command::new("docker")
            .args(["inspect", &expected, "--format", field])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    };

    {
        let (_container, _url, _token) = crate::common::start_vault_container(TEST)
            .await
            .expect("OpenBao container must start once Docker is up");

        let name =
            inspect("{{.Name}}").expect("docker must know the container by its expected name");
        assert_eq!(
            name.trim_start_matches('/'),
            expected,
            "the container is not named per the convention"
        );

        let suite = inspect(&format!(
            "{{{{index .Config.Labels \"{}\"}}}}",
            TEST_SUITE_LABEL.0
        ))
        .expect("docker inspect must report labels");
        assert_eq!(suite, TEST_SUITE_LABEL.1, "suite label missing or wrong");

        let owner = inspect(r#"{{index .Config.Labels "io.hyperi.test.owner-pid"}}"#)
            .expect("docker inspect must report labels");
        assert_eq!(
            owner,
            std::process::id().to_string(),
            "owner-pid label must name THIS test process, or it cannot answer whose run left it"
        );
    }

    // Dropped above. Removal is asynchronous, so give it a moment before
    // asserting -- a bare check here would be racy and fail for the wrong reason.
    for _ in 0..50 {
        if inspect("{{.Name}}").is_none() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{expected} still exists after the holder was dropped -- the suite leaks containers");
}

/// Removes the named containers when the test ends, however it ends.
struct Remove(Vec<String>);

impl Drop for Remove {
    fn drop(&mut self) {
        for name in &self.0 {
            let _ = std::process::Command::new("docker")
                .args(["rm", "--force", "--volumes", name])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
    }
}

/// Create, without starting, a container holding `name`, labelled with
/// `owner` as its owner pid when one is given.
fn hold_name(name: &str, owner: Option<u32>) {
    let mut args = vec![
        "create".to_string(),
        "--name".to_string(),
        name.to_string(),
        "--label".to_string(),
        format!("{}={}", TEST_SUITE_LABEL.0, TEST_SUITE_LABEL.1),
    ];
    if let Some(pid) = owner {
        args.push("--label".to_string());
        args.push(format!("io.hyperi.test.owner-pid={pid}"));
    }
    args.push(format!("openbao/openbao:{OPENBAO_TAG}"));
    let status = std::process::Command::new("docker")
        .args(&args)
        .stdout(std::process::Stdio::null())
        .status()
        .expect("docker create must run");
    assert!(status.success(), "docker create {name} failed");
}

fn exists(name: &str) -> bool {
    std::process::Command::new("docker")
        .args(["inspect", "--type", "container", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// A container a concurrent run on this host still owns is left alone, and this
/// run takes a name of its own instead of killing the other run's container.
#[test]
fn a_name_a_live_run_holds_is_left_and_a_distinct_one_taken() {
    skip_if_no_docker!();
    let name = container_name(Some(test_name!()), "probe");
    let own = format!("{name}-{}", std::process::id());
    let _cleanup = Remove(vec![name.clone(), own.clone()]);

    let mut other_run = std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("spawn a stand-in for the other run");
    hold_name(&name, Some(other_run.id()));

    let claimed = claim_container_name(&name);
    let still_held = exists(&name);
    let _ = other_run.kill();
    let _ = other_run.wait();

    assert!(still_held, "the live run's container was removed");
    assert_eq!(
        claimed, own,
        "this run must not reuse a name a live run holds"
    );
}

/// A container whose owner process has exited is a leak, and is removed so the
/// name it holds is free again.
#[test]
fn a_name_an_exited_run_left_is_reclaimed() {
    skip_if_no_docker!();
    let name = container_name(Some(test_name!()), "probe");
    let _cleanup = Remove(vec![name.clone()]);

    let mut exited = std::process::Command::new("true")
        .spawn()
        .expect("spawn a process that exits");
    let pid = exited.id();
    exited.wait().expect("the process exits");
    hold_name(&name, Some(pid));

    assert_eq!(claim_container_name(&name), name);
    assert!(!exists(&name), "the leaked container was not removed");
}

/// A container with no owner label cannot be attributed, so it is left alone.
#[test]
fn a_name_held_with_no_owner_label_is_left() {
    skip_if_no_docker!();
    let name = container_name(Some(test_name!()), "probe");
    let own = format!("{name}-{}", std::process::id());
    let _cleanup = Remove(vec![name.clone(), own.clone()]);

    hold_name(&name, None);

    assert_eq!(claim_container_name(&name), own);
    assert!(exists(&name), "an unattributed container was removed");
}
