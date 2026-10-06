//! Thread-level pending-work snapshot for the goal waiting policy.
//!
//! Combines Armed receipts from the completion receipt store (B) with
//! unsampled non-suspended Queued/Leased runtime-mailbox entries (C1).
//! Suspended, acknowledged, cancelled, and inline-settled receipts are
//! excluded, as are live processes without receipts. This module never
//! consults process liveness.
//!
//! Armed-to-Queued stays atomically pending: the receipt store lists Armed,
//! Queued, and Leased under one lock, and the snapshot unions store Queued and
//! Leased with mailbox entries, so a receipt moving from Armed to Queued never
//! appears in neither set. Mailbox suspended entries suppress the matching
//! store entry so exhausted retries stop counting as pending.

use std::collections::HashMap;
use std::collections::HashSet;

use codex_extension_api::PendingWorkReadError;
use codex_extension_api::PendingWorkReceipt;
use codex_extension_api::PendingWorkSnapshot;

use super::runtime_mailbox::MailboxEntrySnapshot;
use super::session::Session;
use crate::unified_exec::completion_receipt::PendingReceiptLists;
use crate::unified_exec::completion_receipt::ReceiptId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingState {
    Armed,
    Queued,
    Leased,
}

/// Reads the pending-work snapshot with non-blocking locks.
///
/// Fails with an explicit error when either store is contended or poisoned.
/// Never returns an empty snapshot on failure.
pub(crate) fn try_read_snapshot(
    session: &Session,
) -> Result<PendingWorkSnapshot, PendingWorkReadError> {
    let store_lists = session
        .services
        .unified_exec_manager
        .receipt_store()
        .try_list_pending()
        .map_err(|err| PendingWorkReadError::ReceiptStoreUnavailable {
            reason: err.to_string(),
        })?;
    let mailbox_entries = session
        .input_queue
        .try_snapshot_runtime_mailbox()
        .map_err(|reason| PendingWorkReadError::MailboxUnavailable { reason })?;
    let revision = session.input_queue.pending_work_revision();
    Ok(build_snapshot(store_lists, &mailbox_entries, revision))
}

/// Builds a snapshot from store and mailbox views.
///
/// Exported for unit tests that drive the stores directly without a `Session`.
pub(crate) fn build_snapshot(
    store_lists: PendingReceiptLists,
    mailbox_entries: &[MailboxEntrySnapshot],
    revision: u64,
) -> PendingWorkSnapshot {
    let suspended: HashSet<ReceiptId> = mailbox_entries
        .iter()
        .filter(|entry| entry.suspended)
        .map(|entry| entry.receipt_id)
        .collect();
    let mut states: HashMap<ReceiptId, PendingState> = HashMap::new();
    for receipt_id in store_lists.armed {
        states.insert(receipt_id, PendingState::Armed);
    }
    for receipt_id in store_lists.queued {
        if suspended.contains(&receipt_id) {
            continue;
        }
        states.insert(receipt_id, PendingState::Queued);
    }
    for receipt_id in store_lists.leased {
        if suspended.contains(&receipt_id) {
            continue;
        }
        states.insert(receipt_id, PendingState::Leased);
    }
    for entry in mailbox_entries {
        if entry.suspended {
            states.remove(&entry.receipt_id);
            continue;
        }
        if states
            .get(&entry.receipt_id)
            .is_some_and(|state| *state == PendingState::Armed)
        {
            continue;
        }
        let state = if entry.leased {
            PendingState::Leased
        } else {
            PendingState::Queued
        };
        states
            .entry(entry.receipt_id)
            .and_modify(|existing| {
                if *existing == PendingState::Queued && state == PendingState::Leased {
                    *existing = PendingState::Leased;
                }
            })
            .or_insert(state);
    }

    let mut armed = Vec::new();
    let mut queued = Vec::new();
    let mut leased = Vec::new();
    for (receipt_id, state) in states {
        if suspended.contains(&receipt_id) {
            continue;
        }
        let receipt = PendingWorkReceipt::new(receipt_id.model_handle());
        match state {
            PendingState::Armed => armed.push(receipt),
            PendingState::Queued => queued.push(receipt),
            PendingState::Leased => leased.push(receipt),
        }
    }
    armed.sort_by(|left, right| left.receipt_id().cmp(right.receipt_id()));
    queued.sort_by(|left, right| left.receipt_id().cmp(right.receipt_id()));
    leased.sort_by(|left, right| left.receipt_id().cmp(right.receipt_id()));
    PendingWorkSnapshot::new(armed, queued, leased, revision)
}

#[cfg(test)]
#[path = "pending_work_tests.rs"]
mod tests;
