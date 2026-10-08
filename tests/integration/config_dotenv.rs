// Project:   dfe-receiver
// File:      tests/integration/config_dotenv.rs
// Purpose:   The binary reads only its working directory's .env
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Which `.env` the binary reads, proved through `config-check` on the binary
//! itself rather than a library call.

use std::path::Path;

/// The flat env names the two `.env` files set. Each lands in the config dump
/// `config-check` prints, and neither is masked there.
const PARENT_VAR: &str = "DFE_RECEIVER_KAFKA_CLIENT_ID";
const PROJECT_VAR: &str = "DFE_RECEIVER_DEFAULT_SOURCE";

/// `config-check` run by the binary from `dir` with no `--config`, returning
/// what it printed.
fn config_check_in(dir: &Path) -> String {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-receiver"))
        .arg("config-check")
        .current_dir(dir)
        .env_remove(PARENT_VAR)
        .env_remove(PROJECT_VAR)
        // The default destination is the bus, and validation refuses it with no broker.
        .env("DFE_RECEIVER_KAFKA_BROKERS", "localhost:9092")
        // Validation reads the app environment, so the outer shell's must not decide it.
        .env_remove("APP_ENV")
        .env_remove("ENVIRONMENT")
        .env_remove("ENV")
        .output()
        .expect("the binary runs");
    let printed = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(out.status.success(), "config-check failed:\n{printed}");
    printed
}

/// The binary reads the `.env` in its working directory and no other.
///
/// A `.env` in a parent directory belongs to whatever project sits above, so a
/// search up the tree loads another project's settings and credentials.
#[test]
fn a_dotenv_in_a_parent_directory_is_not_loaded() {
    let root = tempfile::TempDir::new().expect("tempdir");
    let project = root.path().join("project");
    std::fs::create_dir(&project).expect("project dir");
    std::fs::write(
        root.path().join(".env"),
        format!("{PARENT_VAR}=from_parent_dotenv\n"),
    )
    .expect("parent .env");

    let printed = config_check_in(&project);
    assert!(
        !printed.contains("from_parent_dotenv"),
        "a .env in the parent directory reached the config"
    );

    // The project's own .env still loads.
    std::fs::write(
        project.join(".env"),
        format!("{PROJECT_VAR}=from_project_dotenv\n"),
    )
    .expect("project .env");
    let printed = config_check_in(&project);
    assert!(
        printed.contains("from_project_dotenv"),
        "the project's own .env did not reach the config"
    );
    assert!(
        !printed.contains("from_parent_dotenv"),
        "a .env in the parent directory reached the config"
    );
}
