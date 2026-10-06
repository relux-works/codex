//! Goal-side unit tests for the pending-work snapshot API.
//!
//! These tests use only `codex-extension-api` types through the thread
//! extension data, proving ext/goal can read pending work with no new
//! core-to-goal dependency. Core publishes the provider; goal only reads it.

use codex_extension_api::ExtensionData;
use codex_extension_api::PendingWorkProvider;
use codex_extension_api::PendingWorkReadError;
use codex_extension_api::PendingWorkReceipt;
use codex_extension_api::PendingWorkSnapshot;
use codex_extension_api::read_pending_work;
use pretty_assertions::assert_eq;

#[test]
fn goal_reads_pending_work_through_extension_api() {
    let store = ExtensionData::new("thread");
    let snapshot = PendingWorkSnapshot::new(
        vec![PendingWorkReceipt::new("armed-1")],
        vec![PendingWorkReceipt::new("queued-1")],
        vec![PendingWorkReceipt::new("leased-1")],
        7,
    );
    let expected = snapshot.clone();
    store.insert(PendingWorkProvider::new(move || Ok(snapshot.clone())));

    let observed = read_pending_work(&store).expect("read");
    assert_eq!(observed, expected);
    assert_eq!(observed.revision(), 7);
    assert!(!observed.is_empty());
    assert_eq!(observed.len(), 3);
    assert_eq!(observed.armed()[0].receipt_id(), "armed-1");
    assert_eq!(observed.queued()[0].receipt_id(), "queued-1");
    assert_eq!(observed.leased()[0].receipt_id(), "leased-1");
}

#[test]
fn goal_read_failure_is_explicit_error_not_empty() {
    // Missing provider is an error, never an empty snapshot.
    let store = ExtensionData::new("thread");
    let result = read_pending_work(&store);
    assert_eq!(result, Err(PendingWorkReadError::ProviderMissing));
    assert_ne!(
        result,
        Ok(PendingWorkSnapshot::empty(0)),
        "missing provider must not masquerade as empty"
    );

    // A failing provider stays failed.
    let store = ExtensionData::new("thread");
    store.insert(PendingWorkProvider::new(|| {
        Err(PendingWorkReadError::SessionUnavailable)
    }));
    let result = read_pending_work(&store);
    assert_eq!(result, Err(PendingWorkReadError::SessionUnavailable));
    assert_ne!(
        result,
        Ok(PendingWorkSnapshot::empty(0)),
        "failed read must not masquerade as empty"
    );

    // An empty snapshot is still Ok when there is genuinely no work.
    let store = ExtensionData::new("thread");
    store.insert(PendingWorkProvider::new(|| {
        Ok(PendingWorkSnapshot::empty(3))
    }));
    let observed = read_pending_work(&store).expect("empty is ok");
    assert!(observed.is_empty());
    assert_eq!(observed.revision(), 3);
}
