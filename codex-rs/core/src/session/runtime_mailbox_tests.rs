use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;

use super::RuntimeMailbox;
use crate::unified_exec::completion_receipt::CompletionReceiptStore;
use crate::unified_exec::completion_receipt::ReceiptId;
use crate::unified_exec::completion_receipt::ReceiptOwner;

fn receipt_owner(call_id: &str) -> ReceiptOwner {
    ReceiptOwner::new(
        ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0001),
        /*runtime_generation*/ 7,
        call_id,
    )
    .expect("test owner should be valid")
}

fn reserve_receipt(store: &CompletionReceiptStore, call_id: &str) -> (ReceiptId, ReceiptOwner) {
    let owner = receipt_owner(call_id);
    let receipt_id = store
        .reserve(owner.clone())
        .expect("reserve should succeed");
    (receipt_id, owner)
}

#[test]
fn runtime_entry_survives_lease_until_acknowledged() {
    let store = CompletionReceiptStore::default();
    let (receipt_id, owner) = reserve_receipt(&store, "call-lease");
    let mut mailbox = RuntimeMailbox::new();

    assert!(mailbox.enqueue(receipt_id, owner));
    assert!(mailbox.has_pending());
    assert!(mailbox.has_trigger());

    let leases = mailbox.lease_available();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].receipt_id(), receipt_id);
    // Leased entries stay for acknowledgement: trigger priority remains while
    // pending work is already handed out.
    assert!(!mailbox.has_pending());
    assert!(mailbox.has_trigger());

    // A second drain does not re-offer while leased.
    assert!(mailbox.lease_available().is_empty());

    assert!(mailbox.acknowledge(&leases[0]));
    assert!(!mailbox.has_pending());
    assert!(!mailbox.has_trigger());
    assert!(mailbox.lease_available().is_empty());
}

#[test]
fn runtime_double_acknowledgement_is_refused() {
    let store = CompletionReceiptStore::default();
    let (receipt_id, owner) = reserve_receipt(&store, "call-double-ack");
    let mut mailbox = RuntimeMailbox::new();
    assert!(mailbox.enqueue(receipt_id, owner));

    let leases = mailbox.lease_available();
    assert_eq!(leases.len(), 1);
    assert!(mailbox.acknowledge(&leases[0]));
    assert!(!mailbox.acknowledge(&leases[0]));
    assert!(mailbox.lease_available().is_empty());
}

#[test]
fn runtime_failed_lease_returns_unleased_exactly_once() {
    let store = CompletionReceiptStore::default();
    let (receipt_id, owner) = reserve_receipt(&store, "call-fail");
    let mut mailbox = RuntimeMailbox::new();
    assert!(mailbox.enqueue(receipt_id, owner));

    let first = mailbox.lease_available();
    assert_eq!(first.len(), 1);
    assert!(mailbox.fail(&first[0]));
    // A stale retry of the same token is refused without duplicating.
    assert!(!mailbox.fail(&first[0]));
    assert!(mailbox.has_pending());
    assert!(mailbox.has_trigger());

    let second = mailbox.lease_available();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].receipt_id(), receipt_id);
    assert_ne!(second[0], first[0]);
    // The old token can no longer acknowledge after the retry leased anew.
    assert!(!mailbox.acknowledge(&first[0]));
    assert!(mailbox.acknowledge(&second[0]));
    assert!(mailbox.lease_available().is_empty());
}

#[test]
fn runtime_suspended_entries_are_excluded_from_trigger_and_lease() {
    let store = CompletionReceiptStore::default();
    let (receipt_id, owner) = reserve_receipt(&store, "call-suspend");
    let mut mailbox = RuntimeMailbox::new();
    assert!(mailbox.enqueue(receipt_id, owner));

    assert!(mailbox.suspend(receipt_id));
    assert!(!mailbox.has_pending());
    assert!(!mailbox.has_trigger());
    assert!(mailbox.lease_available().is_empty());
}

#[test]
fn runtime_suspended_while_leased_stops_suppressing() {
    let store = CompletionReceiptStore::default();
    let (receipt_id, owner) = reserve_receipt(&store, "call-suspend-leased");
    let mut mailbox = RuntimeMailbox::new();
    assert!(mailbox.enqueue(receipt_id, owner));

    let leases = mailbox.lease_available();
    assert_eq!(leases.len(), 1);
    assert!(mailbox.has_trigger());

    assert!(mailbox.suspend(receipt_id));
    assert!(!mailbox.has_pending());
    assert!(!mailbox.has_trigger());
    assert!(mailbox.lease_available().is_empty());
    // Acknowledgement still removes a suspended lease for cleanup.
    assert!(mailbox.acknowledge(&leases[0]));
}

#[test]
fn runtime_cancel_removes_leased_and_unleased_entries() {
    let store = CompletionReceiptStore::default();
    let (unleased_id, unleased_owner) = reserve_receipt(&store, "call-cancel-unleased");
    let (leased_id, leased_owner) = reserve_receipt(&store, "call-cancel-leased");
    let mut mailbox = RuntimeMailbox::new();
    assert!(mailbox.enqueue(unleased_id, unleased_owner));
    assert!(mailbox.enqueue(leased_id, leased_owner));

    let leases = mailbox.lease_available();
    assert_eq!(leases.len(), 2);
    let leased_token = leases
        .iter()
        .find(|lease| lease.receipt_id() == leased_id)
        .expect("leased entry should have a token");
    let unleased_token = leases
        .iter()
        .find(|lease| lease.receipt_id() == unleased_id)
        .expect("unleased entry leases too");

    // Cancel one while leased and fail the other back to unleased first so
    // both lease states are covered.
    assert!(mailbox.fail(unleased_token));
    assert!(mailbox.cancel(unleased_id));
    assert!(mailbox.cancel(leased_id));

    assert!(!mailbox.has_pending());
    assert!(!mailbox.has_trigger());
    assert!(mailbox.lease_available().is_empty());
    // Lease tokens for cancelled entries can no longer acknowledge or fail.
    assert!(!mailbox.acknowledge(leased_token));
    assert!(!mailbox.fail(leased_token));
    assert!(!mailbox.acknowledge(unleased_token));
}

#[test]
fn runtime_duplicate_enqueue_does_not_duplicate_delivery() {
    let store = CompletionReceiptStore::default();
    let (receipt_id, owner) = reserve_receipt(&store, "call-duplicate");
    let mut mailbox = RuntimeMailbox::new();

    assert!(mailbox.enqueue(receipt_id, owner.clone()));
    assert!(!mailbox.enqueue(receipt_id, owner));

    let leases = mailbox.lease_available();
    assert_eq!(leases.len(), 1);
    assert!(mailbox.lease_available().is_empty());
}
