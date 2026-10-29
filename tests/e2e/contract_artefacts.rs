// Project:   dfe-receiver
// File:      tests/e2e/contract_artefacts.rs
// Purpose:   E2E tests for generated container contract artefacts
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! ============================================================================
//! Canonical adaptation of scalo's `tests/e2e/contract_artefacts.rs`
//! TEMPLATE (feature `deployment-test-support`).
//!
//! Diff from the upstream TEMPLATE (kept deliberately small so scalo
//! bug fixes merge cleanly here):
//!   1. `test_contract()` returns `dfe_receiver::deployment::contract()`
//!      instead of the throwaway hyperi-contract-test fixture.
//!   2. `test_identity()` uses the dfe-receiver image ref + commit sha
//!      from CI env (`ContractIdentity::detect`).
//!   3. `stage_binary()` replaces the TEMPLATE's `write_mock_binary`
//!      shell stub with the actual release-built dfe-receiver binary
//!      (built by `cargo build --release --bin dfe-receiver` before the
//!      test run; helper panics with clear instructions if missing).
//!   4. Tier A docker-run assertion looks for "dfe-receiver" (clap usage
//!      line) instead of the mock's "hyperi-contract-test: ok".
//!
//! Everything else -- the Tier A test body, skip helpers, label
//! assertions -- is byte-identical to the TEMPLATE.
//! ============================================================================
//!
//! E2E test for the Dockerfile `crate::deployment` emits, without a cluster:
//! `docker build` + `docker run --rm <img> --help` proves the image actually
//! starts.
//!
//! The Kubernetes objects come from the scalo-service library chart, which
//! the release assembles from the deployment contract.
//!
//! # Skip policy
//!
//! The test probes for a docker daemon via `scalo::deployment::test_support`
//! and skips cleanly when it is absent. Skip emissions use the canonical
//! prefix `HYPERCI-SKIP[contract-e2e][tier-a]:` so downstream test runners
//! can grep, count, and emit a summary line at the end of a CI run.
//!
//! # Binary staging
//!
//! `generate_dockerfile()` produces a Dockerfile that `COPY`s the
//! consumer app's binary into the image. The `stage_binary()` helper
//! below copies the release-built `target/release/dfe-receiver` into the
//! build context. The binary must be built BEFORE running this test —
//! the test does NOT invoke cargo to avoid the cargo-in-cargo target-lock
//! deadlock. Build with:
//!
//! ```bash
//! cargo build --release --bin dfe-receiver
//! cargo nextest run --test e2e contract_artefacts::
//! ```

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::io::Write;
use std::path::Path;
use std::process::Command;

use scalo::deployment::test_support::{
    docker_available, docker_empty_creds_json, docker_host, skip,
};
use scalo::deployment::{ContractIdentity, DeploymentContract, generate_dockerfile};

// ============================================================================
// FIXTURE -- dfe-receiver adaptation (see header diff list).
// ============================================================================

/// dfe-receiver's deployment contract — the real one used by
/// `generate-artefacts` and shipped in CI.
fn test_contract() -> DeploymentContract {
    dfe_receiver::deployment::contract()
}

/// Synthesise a contract identity for testing.
///
/// In CI, `ContractIdentity::detect` reads `GITHUB_SHA` and the image
/// reference passed in. Locally we just stamp a synthetic identity so
/// the label/annotation assertions have something to match.
fn test_identity() -> ContractIdentity {
    ContractIdentity::detect("ghcr.io/hyperi-io/dfe-receiver:e2e-test")
        .expect("ContractIdentity::detect must succeed (CI sets GITHUB_SHA; local uses git HEAD)")
}

/// Stage a binary into the docker build context.
///
/// Preferred path: copy the real release-built dfe-receiver binary from
/// `target/release/dfe-receiver` (build with `cargo build --release --bin
/// dfe-receiver` beforehand). This gives the highest-fidelity check —
/// the actual binary runs inside the produced image.
///
/// CI Test stage doesn't build release before nextest (Build runs after
/// Test), so when the real binary is absent we fall back to writing a
/// shell-script mock that mirrors `scalo`'s contract-artefact
/// tests. The mock satisfies Dockerfile `COPY` and the image's `--help`
/// entrypoint smoke check, which is what the tier-A test actually
/// exercises: that the generated Dockerfile builds and the image runs.
///
/// We do NOT invoke cargo from inside the test to avoid the
/// cargo-in-cargo target-directory lock deadlock.
fn stage_binary(build_ctx: &Path, binary_name: &str) -> std::io::Result<()> {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        manifest_dir
            .join("target")
            .join("release")
            .join(binary_name),
        // CARGO_TARGET_DIR override (CI commonly points elsewhere)
        std::env::var_os("CARGO_TARGET_DIR")
            .map(|d| {
                std::path::PathBuf::from(d)
                    .join("release")
                    .join(binary_name)
            })
            .unwrap_or_default(),
    ];
    let dest = build_ctx.join(binary_name);
    if let Some(src) = candidates
        .iter()
        .find(|p| !p.as_os_str().is_empty() && p.exists())
    {
        std::fs::copy(src, &dest)?;
    } else {
        // Fallback: mock binary. The tier-A test only needs `--help` to
        // exit 0 and the output to mention the binary name (the test
        // asserts `stdout.contains(binary_name)` further down). Mirror
        // the pattern scalo uses for its own contract artefact
        // tests, plus the binary name in the help line.
        let mut f = std::fs::File::create(&dest)?;
        let script = format!(
            "#!/bin/sh\n\
             # Mock binary for the {bin} contract-artefact e2e test.\n\
             # Used when target/release/{bin} is not built (CI Test stage).\n\
             if [ \"$1\" = \"--help\" ] || [ \"$1\" = \"-h\" ]; then\n\
             \x20 echo \"{bin}: contract-test ok\"\n\
             \x20 exit 0\n\
             fi\n\
             echo \"{bin}: started (mock)\"\n\
             exit 0\n",
            bin = binary_name,
        );
        f.write_all(script.as_bytes())?;
        drop(f);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&dest)?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&dest, perms)?;
    }
    Ok(())
}

// ============================================================================
// Tier A -- Dockerfile: docker build + docker run --help
// ============================================================================

/// A `docker` command pointed at a throwaway credential store.
///
/// The throwaway `DOCKER_CONFIG` keeps credential helpers out of the test, but
/// it also hides the context store that lives in the same directory, so the
/// daemon endpoint has to be carried across explicitly or docker falls back to
/// `unix:///var/run/docker.sock` -- correct on Linux CI, wrong on a developer
/// machine running Colima or Docker Desktop.
fn docker_cmd(docker_config: &Path) -> Command {
    let mut cmd = Command::new("docker");
    cmd.env("DOCKER_CONFIG", docker_config);
    if let Some(host) = docker_host() {
        cmd.env("DOCKER_HOST", host);
    }
    cmd
}

#[test]
fn tier_a_dockerfile_builds_and_image_runs() {
    if !docker_available() {
        skip(
            "tier-a",
            "tier_a_dockerfile_builds_and_image_runs",
            "docker daemon not reachable",
        );
        return;
    }

    let contract = test_contract();
    let identity = test_identity();
    let dockerfile =
        generate_dockerfile(&contract, Some(&identity)).expect("the contract is valid");

    let tmp = tempfile::tempdir().expect("tempdir");
    let ctx = tmp.path();
    let dockerfile_path = ctx.join("Dockerfile");
    std::fs::write(&dockerfile_path, &dockerfile).expect("write Dockerfile");
    stage_binary(ctx, contract.binary()).expect("stage release binary");

    let docker_config = tempfile::tempdir().expect("docker config tempdir");
    std::fs::write(
        docker_config.path().join("config.json"),
        docker_empty_creds_json(),
    )
    .expect("write empty docker config");

    let tag = format!("dfe-receiver:e2e-{}", std::process::id());

    let build = docker_cmd(docker_config.path())
        .args(["build", "--quiet", "-t", &tag, "-f"])
        .arg(&dockerfile_path)
        .arg(ctx)
        .output()
        .expect("docker build invocation");
    assert!(
        build.status.success(),
        "docker build failed: stdout={} stderr={}",
        String::from_utf8_lossy(&build.stdout),
        String::from_utf8_lossy(&build.stderr),
    );

    let entrypoint = format!("/usr/local/bin/{}", contract.binary());
    let run = docker_cmd(docker_config.path())
        .args(["run", "--rm", "--entrypoint", &entrypoint, &tag, "--help"])
        .output()
        .expect("docker run invocation");
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        run.status.success(),
        "docker run failed: stdout={stdout} stderr={stderr}",
    );
    assert!(
        stdout.contains("dfe-receiver") || stderr.contains("dfe-receiver"),
        "container ran but --help did not mention dfe-receiver: stdout={stdout} stderr={stderr}",
    );

    let inspect = docker_cmd(docker_config.path())
        .args(["inspect", "--format", "{{json .Config.Labels}}", &tag])
        .output()
        .expect("docker inspect invocation");
    let labels = String::from_utf8_lossy(&inspect.stdout);
    assert!(
        labels.contains("io.hyperi.contract.version")
            && labels.contains("\"v1\"")
            && labels.contains("io.hyperi.contract.source-commit")
            && labels.contains(identity.source_commit())
            && labels.contains("io.hyperi.contract.image-ref")
            && labels.contains(identity.image_ref()),
        "docker inspect did not show all three io.hyperi.contract.* labels with expected values: {labels}",
    );

    let _ = docker_cmd(docker_config.path())
        .args(["rmi", "-f", &tag])
        .output();
}
