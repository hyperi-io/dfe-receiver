// Project:   dfe-receiver
// File:      tests/integration/container_hygiene.rs
// Purpose:   Pin the test_name macro the suite names its owned containers by
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `test_name!()` names a container a test holds alone. Shared containers, their
//! names, labels and cleanup are scalo's `test_support::shared_service`, tested
//! there.

use crate::test_name;

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
