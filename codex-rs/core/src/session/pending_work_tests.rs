use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use codex_extension_api::PendingWorkReadError;
use codex_extension_api::PendingWorkSnapshot;
use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;

use super::build_snapshot;
use crate::context::ExecCompletion;
use crate::context::ExecOutputRetention;
use crate::session::input_queue::InputQueue;
use crate::session::runtime_mailbox::RuntimeMailbox;
use crate::unified_exec::completion_receipt::CancellationReason;
use crate::unified_exec::completion_receipt::CompletionReceiptStore;
use crate::unified_exec::completion_receipt::InitialResponseDecision;
use crate::unified_exec::completion_receipt::ReceiptOwner;
use crate::unified_exec::completion_receipt::SamplingSource;
use crate::unified_exec::completion_receipt::TerminalCompletion;

fn test_owner(call_id: &str) -> ReceiptOwner {
    ReceiptOwner::new(ThreadId::new(), 1, call_id).expect("owner")
}

fn test_terminal() -> TerminalCompletion {
    TerminalCompletion {
        exit_code: Some(0),
        timed_out: false,
    }
}

fn test_completion() -> ExecCompletion {
    ExecCompletion {
        process_id: 1,
        exit_code: Some(0),
        timed_out: false,
        failure: None,
        retention: ExecOutputRetention::Absent,
    }
}

fn arm_receipt(
    store: &CompletionReceiptStore,
    call_id: &str,
) -> (
    ReceiptOwner,
    crate::unified_exec::completion_receipt::ReceiptId,
) {
    let owner = test_owner(call_id);
    let receipt_id = store.reserve(owner.clone()).expect("reserve");
    let outcome = store
        .resolve_initial_response(receipt_id, &owner, InitialResponseDecision::Arm)
        .expect("arm");
    assert_eq!(
        outcome,
        crate::unified_exec::completion_receipt::InitialResponseOutcome::Armed
    );
    (owner, receipt_id)
}

fn queue_receipt(
    store: &CompletionReceiptStore,
    owner: &ReceiptOwner,
    receipt_id: crate::unified_exec::completion_receipt::ReceiptId,
) {
    let outcome = store
        .publish_exit(receipt_id, owner, test_terminal())
        .expect("publish");
    assert_eq!(
        outcome,
        crate::unified_exec::completion_receipt::ExitPublicationOutcome::Queued
    );
}

fn snapshot_from(
    store: &CompletionReceiptStore,
    mailbox: &RuntimeMailbox,
    revision: u64,
) -> PendingWorkSnapshot {
    let lists = store.try_list_pending().expect("list");
    let entries = mailbox.snapshot_entries();
    build_snapshot(lists, &entries, revision)
}

#[test]
fn snapshot_reports_armed_queued_and_leased_only() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let mut mailbox = RuntimeMailbox::with_revision(Arc::clone(&revision));

    // Armed: reserved + armed, no exit yet.
    let (_, armed_id) = arm_receipt(&store, "armed");
    // Queued via store: armed + published, no mailbox entry yet (pre-enqueue).
    let (queued_owner, queued_id) = arm_receipt(&store, "queued");
    queue_receipt(&store, &queued_owner, queued_id);
    // Queued via mailbox: store queued + mailbox enqueued (unleased).
    let (mbox_queued_owner, mbox_queued_id) = arm_receipt(&store, "mbox-queued");
    queue_receipt(&store, &mbox_queued_owner, mbox_queued_id);
    assert!(mailbox.enqueue(mbox_queued_id, mbox_queued_owner, test_completion()));
    // Leased via mailbox: enqueued + leased.
    let (mbox_leased_owner, mbox_leased_id) = arm_receipt(&store, "mbox-leased");
    queue_receipt(&store, &mbox_leased_owner, mbox_leased_id);
    assert!(mailbox.enqueue(mbox_leased_id, mbox_leased_owner, test_completion()));
    let leases = mailbox.lease_available_up_to(1);
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].receipt_id(), mbox_queued_id);
    // Lease the second queued entry too so both mailbox entries are leased.
    let leases = mailbox.lease_available_up_to(1);
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].receipt_id(), mbox_leased_id);

    let snapshot = snapshot_from(&store, &mailbox, store.revision());
    let armed: HashSet<String> = snapshot
        .armed()
        .iter()
        .map(|receipt| receipt.receipt_id().to_string())
        .collect();
    let queued: HashSet<String> = snapshot
        .queued()
        .iter()
        .map(|receipt| receipt.receipt_id().to_string())
        .collect();
    let leased: HashSet<String> = snapshot
        .leased()
        .iter()
        .map(|receipt| receipt.receipt_id().to_string())
        .collect();

    assert_eq!(armed, HashSet::from([armed_id.model_handle()]));
    // Store-queued without mailbox still counts as queued (never neither).
    assert!(queued.contains(&queued_id.model_handle()));
    // Mailbox entries are leased, so they appear as leased, not queued.
    assert_eq!(queued.len(), 1);
    assert_eq!(
        leased,
        HashSet::from([mbox_queued_id.model_handle(), mbox_leased_id.model_handle()])
    );
    assert!(!snapshot.is_empty());
    // Reserved (not armed) is excluded: create one and verify it never appears.
    let reserved_owner = test_owner("reserved-only");
    let reserved_id = store.reserve(reserved_owner).expect("reserve");
    let snapshot = snapshot_from(&store, &mailbox, store.revision());
    let all: HashSet<String> = snapshot
        .armed()
        .iter()
        .chain(snapshot.queued())
        .chain(snapshot.leased())
        .map(|receipt| receipt.receipt_id().to_string())
        .collect();
    assert!(!all.contains(&reserved_id.model_handle()));
}

#[test]
fn snapshot_excludes_suspended_and_cancelled() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let mut mailbox = RuntimeMailbox::with_revision(Arc::clone(&revision));

    // Suspended via explicit suspend: store queued + mailbox suspended.
    let (susp_owner, susp_id) = arm_receipt(&store, "susp");
    queue_receipt(&store, &susp_owner, susp_id);
    assert!(mailbox.enqueue(susp_id, susp_owner, test_completion()));
    assert!(mailbox.suspend(susp_id));

    // Acknowledged work is covered by production_acknowledgement_removes_pending_work,
    // which drives the real sampling acceptance path instead of hand-acking stores.

    // Cancelled via both stores.
    let (cancel_owner, cancel_id) = arm_receipt(&store, "cancel");
    queue_receipt(&store, &cancel_owner, cancel_id);
    assert!(mailbox.enqueue(cancel_id, cancel_owner.clone(), test_completion()));
    assert!(mailbox.cancel(cancel_id));
    store
        .cancel(cancel_id, &cancel_owner, CancellationReason::OwnerStopped)
        .expect("store cancel");

    let snapshot = snapshot_from(&store, &mailbox, store.revision());
    assert!(snapshot.is_empty());
    assert_eq!(snapshot.len(), 0);
    let all: HashSet<String> = snapshot
        .armed()
        .iter()
        .chain(snapshot.queued())
        .chain(snapshot.leased())
        .map(|receipt| receipt.receipt_id().to_string())
        .collect();
    assert!(!all.contains(&susp_id.model_handle()));
    assert!(!all.contains(&cancel_id.model_handle()));
}

#[tokio::test]
async fn production_acknowledgement_removes_pending_work() {
    use crate::context::ContextualUserFragment;
    use crate::context::ExecCompletionFragment;
    use crate::session::exec_completion_ack;
    use crate::session::pending_work::try_read_snapshot;
    use crate::session::tests::make_session_and_context;
    use crate::unified_exec::completion_receipt::ReceiptStatus;

    let (session, turn_context) = make_session_and_context().await;
    let session = Arc::new(session);
    {
        let weak = Arc::downgrade(&session);
        session.services.thread_extension_data.insert(
            codex_extension_api::PendingWorkProvider::new(move || {
                let Some(session) = weak.upgrade() else {
                    return Err(codex_extension_api::PendingWorkReadError::SessionUnavailable);
                };
                try_read_snapshot(session.as_ref())
            }),
        );
    }

    // Reserve, arm, and publish through the session's real receipt store.
    let owner =
        ReceiptOwner::new(session.thread_id, /*runtime_generation*/ 1, "prod-ack").expect("owner");
    let store = session.services.unified_exec_manager.receipt_store();
    let receipt_id = store.reserve(owner.clone()).expect("reserve");
    let outcome = store
        .resolve_initial_response(receipt_id, &owner, InitialResponseDecision::Arm)
        .expect("arm");
    assert_eq!(
        outcome,
        crate::unified_exec::completion_receipt::InitialResponseOutcome::Armed
    );
    queue_receipt(store, &owner, receipt_id);
    assert!(
        session
            .input_queue
            .enqueue_runtime_notification(receipt_id, owner.clone(), test_completion())
            .await
    );

    // Lease and record exactly as the wake and record paths do.
    let leases = session
        .input_queue
        .lease_runtime_notifications_up_to(8)
        .await;
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].receipt_id(), receipt_id);
    exec_completion_ack::note_recorded(&turn_context, &leases);

    // Before acceptance the receipt is pending through the installed provider.
    let before = codex_extension_api::read_pending_work(&session.services.thread_extension_data)
        .expect("read before");
    assert!(!before.is_empty());

    // Drive the real production acceptance entry point: server acceptance of a
    // prompt containing the trusted fragment. No manual store acknowledgement.
    let fragment: codex_protocol::models::ResponseItem = ContextualUserFragment::into(
        ExecCompletionFragment::new(receipt_id.model_handle(), leases[0].completion()),
    );
    exec_completion_ack::acknowledge_submitted(
        &session,
        &turn_context,
        std::slice::from_ref(&fragment),
    )
    .await;

    // After acceptance the receipt is in neither set, via the provider.
    let after = codex_extension_api::read_pending_work(&session.services.thread_extension_data)
        .expect("read after");
    assert!(after.is_empty());
    assert_eq!(after.len(), 0);
    let handle = receipt_id.model_handle();
    assert!(
        !after
            .armed()
            .iter()
            .chain(after.queued())
            .chain(after.leased())
            .any(|receipt| receipt.receipt_id() == handle)
    );
    // The B receipt itself retired to sampled through the pushed path.
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::Sampled {
            source: SamplingSource::PushedCompletion
        })
    );
    // A fail after acknowledgement stays a no-op in the mailbox.
    assert!(!session.input_queue.fail_runtime_lease(&leases[0]).await);
}

#[test]
fn revision_increases_on_arm() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let owner = test_owner("arm-rev");
    let receipt_id = store.reserve(owner.clone()).expect("reserve");
    let before = store.revision();
    store
        .resolve_initial_response(receipt_id, &owner, InitialResponseDecision::Arm)
        .expect("arm");
    assert!(store.revision() > before);
}

#[test]
fn revision_increases_on_queue_via_publish() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "queue-publish");
    let before = store.revision();
    queue_receipt(&store, &owner, receipt_id);
    assert!(store.revision() > before);
}

#[test]
fn revision_increases_on_queue_via_mailbox_enqueue() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let mut mailbox = RuntimeMailbox::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "queue-enqueue");
    queue_receipt(&store, &owner, receipt_id);
    let before = mailbox.revision();
    assert!(mailbox.enqueue(receipt_id, owner, test_completion()));
    assert!(mailbox.revision() > before);
}

#[test]
fn revision_increases_on_lease() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let mut mailbox = RuntimeMailbox::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "lease");
    queue_receipt(&store, &owner, receipt_id);
    assert!(mailbox.enqueue(receipt_id, owner, test_completion()));
    let before = mailbox.revision();
    let leases = mailbox.lease_available_up_to(1);
    assert_eq!(leases.len(), 1);
    assert!(mailbox.revision() > before);
}

#[test]
fn revision_increases_on_store_lease() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "store-lease");
    queue_receipt(&store, &owner, receipt_id);
    let before = store.revision();
    store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
        .expect("lease");
    assert!(store.revision() > before);
}

#[test]
fn revision_increases_on_fail_back() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let mut mailbox = RuntimeMailbox::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "fail-back");
    queue_receipt(&store, &owner, receipt_id);
    assert!(mailbox.enqueue(receipt_id, owner, test_completion()));
    let leases = mailbox.lease_available_up_to(1);
    assert_eq!(leases.len(), 1);
    let before = mailbox.revision();
    assert!(mailbox.fail(&leases[0]));
    assert!(mailbox.revision() > before);
    assert!(!mailbox.is_suspended(receipt_id));
}

#[test]
fn revision_increases_on_store_fail_back() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "store-fail");
    queue_receipt(&store, &owner, receipt_id);
    let lease = store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
        .expect("lease");
    let before = store.revision();
    store.fail_sampling(&lease).expect("fail");
    assert!(store.revision() > before);
}

#[test]
fn revision_increases_on_acknowledge() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let mut mailbox = RuntimeMailbox::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "ack-rev");
    queue_receipt(&store, &owner, receipt_id);
    assert!(mailbox.enqueue(receipt_id, owner, test_completion()));
    let leases = mailbox.lease_available_up_to(1);
    assert_eq!(leases.len(), 1);
    let before = mailbox.revision();
    assert!(mailbox.acknowledge(&leases[0]));
    assert!(mailbox.revision() > before);
}

#[test]
fn revision_increases_on_store_acknowledge() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "store-ack");
    queue_receipt(&store, &owner, receipt_id);
    let lease = store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
        .expect("lease");
    let before = store.revision();
    store.acknowledge_sampled(&lease).expect("ack");
    assert!(store.revision() > before);
}

#[test]
fn revision_increases_on_cancel() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "cancel-rev");
    let before = store.revision();
    store
        .cancel(receipt_id, &owner, CancellationReason::OwnerStopped)
        .expect("cancel");
    assert!(store.revision() > before);
}

#[test]
fn revision_increases_on_mailbox_cancel() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let mut mailbox = RuntimeMailbox::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "mbox-cancel");
    queue_receipt(&store, &owner, receipt_id);
    assert!(mailbox.enqueue(receipt_id, owner, test_completion()));
    let before = mailbox.revision();
    assert!(mailbox.cancel(receipt_id));
    assert!(mailbox.revision() > before);
}

#[test]
fn revision_increases_on_suspend() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let mut mailbox = RuntimeMailbox::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "suspend-rev");
    queue_receipt(&store, &owner, receipt_id);
    assert!(mailbox.enqueue(receipt_id, owner, test_completion()));
    let before = mailbox.revision();
    assert!(mailbox.suspend(receipt_id));
    assert!(mailbox.revision() > before);
}

#[test]
fn revision_increases_on_suspend_via_exhausted_fail() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let mut mailbox = RuntimeMailbox::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "exhaust");
    queue_receipt(&store, &owner, receipt_id);
    assert!(mailbox.enqueue(receipt_id, owner, test_completion()));
    for _ in 0..crate::session::runtime_mailbox::MAX_RUNTIME_SAMPLING_ATTEMPTS {
        let leases = mailbox.lease_available_up_to(1);
        assert_eq!(leases.len(), 1);
        let before = mailbox.revision();
        assert!(mailbox.fail(&leases[0]));
        assert!(mailbox.revision() > before);
    }
    assert!(mailbox.is_suspended(receipt_id));
}

#[test]
fn revision_increases_on_release() {
    let revision = Arc::new(AtomicU64::new(0));
    let store = CompletionReceiptStore::with_revision(Arc::clone(&revision));
    let (owner, receipt_id) = arm_receipt(&store, "release-rev");
    let before = store.revision();
    store
        .cancel(receipt_id, &owner, CancellationReason::Released)
        .expect("release");
    assert!(store.revision() > before);
}

#[test]
fn read_failure_returns_explicit_error_not_empty_snapshot() {
    // Provider missing is an explicit error, never an empty snapshot.
    let store = codex_extension_api::ExtensionData::new("thread");
    let result = codex_extension_api::read_pending_work(&store);
    assert_eq!(result, Err(PendingWorkReadError::ProviderMissing));
    assert_ne!(
        result,
        Ok(PendingWorkSnapshot::empty(0)),
        "a failed read must not masquerade as empty"
    );

    // A provider that fails stays failed; mapping it to empty is rejected.
    let failing = codex_extension_api::PendingWorkProvider::new(|| {
        Err(PendingWorkReadError::SessionUnavailable)
    });
    let result = failing.read();
    assert_eq!(result, Err(PendingWorkReadError::SessionUnavailable));
    assert_ne!(
        result,
        Ok(PendingWorkSnapshot::empty(0)),
        "a failed read must not masquerade as empty"
    );
}

#[tokio::test]
async fn mailbox_contended_read_fails_explicitly_not_empty() {
    let queue = InputQueue::new();
    let _held = queue.test_hold_runtime_lock().await;
    let result = queue.try_snapshot_runtime_mailbox();
    assert!(
        result.is_err(),
        "contended mailbox must fail, not return empty"
    );
}

#[test]
fn store_contended_read_fails_explicitly_not_empty() {
    let store = CompletionReceiptStore::default();
    store.test_with_held_lock(|| {
        let result = store.try_list_pending();
        assert_eq!(
            result,
            Err(crate::unified_exec::completion_receipt::ReceiptError::LockContended)
        );
    });
}

#[tokio::test]
async fn armed_to_queued_is_atomic_for_concurrent_readers() {
    use tokio::sync::Barrier;

    let revision = Arc::new(AtomicU64::new(0));
    let store = Arc::new(CompletionReceiptStore::with_revision(Arc::clone(&revision)));
    let mailbox = Arc::new(tokio::sync::Mutex::new(RuntimeMailbox::with_revision(
        Arc::clone(&revision),
    )));

    // One receipt starts Armed. The writer publishes it to Queued while
    // readers snapshot concurrently; no reader may observe neither set.
    let owner = test_owner("atomic");
    let receipt_id = store.reserve(owner.clone()).expect("reserve");
    store
        .resolve_initial_response(receipt_id, &owner, InitialResponseDecision::Arm)
        .expect("arm");
    let handle = receipt_id.model_handle();

    let readers = 4;
    let iterations = 200;
    let barrier = Arc::new(Barrier::new(readers + 1));
    let mut joins = Vec::new();
    for _ in 0..readers {
        let store = Arc::clone(&store);
        let mailbox = Arc::clone(&mailbox);
        let barrier = Arc::clone(&barrier);
        let handle = handle.clone();
        joins.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..iterations {
                let lists = store.try_list_pending().expect("list");
                let entries = mailbox.lock().await.snapshot_entries();
                let snapshot = build_snapshot(lists, &entries, 0);
                let armed = snapshot
                    .armed()
                    .iter()
                    .any(|receipt| receipt.receipt_id() == handle);
                let queued = snapshot
                    .queued()
                    .iter()
                    .chain(snapshot.leased())
                    .any(|receipt| receipt.receipt_id() == handle);
                assert!(
                    armed || queued,
                    "concurrent reader saw receipt in neither set"
                );
                assert!(
                    !(armed && queued),
                    "concurrent reader saw receipt in both sets"
                );
            }
        }));
    }

    let writer_store = Arc::clone(&store);
    let writer_barrier = Arc::clone(&barrier);
    let writer = tokio::spawn(async move {
        writer_barrier.wait().await;
        // Flip Armed -> Queued once; readers race this single transition.
        // The store holds one lock across the move, so readers see exactly
        // one side.
        let _ = writer_store.publish_exit(receipt_id, &owner, test_terminal());
    });

    writer.await.expect("writer");
    for join in joins {
        join.await.expect("reader");
    }

    // Post-transition the receipt is Queued (store) even before any mailbox
    // enqueue, so it stays pending without a gap.
    let lists = store.try_list_pending().expect("list");
    let entries = mailbox.lock().await.snapshot_entries();
    let snapshot = build_snapshot(lists, &entries, 0);
    assert!(
        snapshot
            .queued()
            .iter()
            .any(|receipt| receipt.receipt_id() == handle)
    );
}
