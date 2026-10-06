//! Internal runtime-notification mailbox entries for exec completions.
//!
//! This module owns the mailbox side of completion receipts. Each entry carries
//! a receipt reference from the unified-exec receipt state machine (stage B)
//! and stays logically leased from drain until it is acknowledged as sampled
//! or is cancelled. A failed sampling attempt returns the entry to the
//! unleased state so a later drain retries it without creating a duplicate.
//!
//! Runtime entries are never [`InterAgentCommunication`][iac]: they have no
//! author, no recipient lineage, and no trigger-turn fork boundary. Trigger
//! queries include unsampled, non-suspended entries so they keep priority over
//! goal continuation; suspended entries (retry exhausted) are excluded so they
//! never block idle starts or suppress idle contributors.
//!
//! Idle wake leases entries via [`RuntimeMailbox::lease_available_up_to`] and
//! hands the leases to the turn starter as the internal `TurnInput` variant,
//! which the record path renders through the exec-completion context fragment.
//! The sampling path acknowledges a lease only after a submitted prompt is
//! observed to contain its fragment; a failed or aborted submission fails the
//! lease back to unleased so a later wake retries it. Each failed attempt
//! counts against [`MAX_RUNTIME_SAMPLING_ATTEMPTS`]; the attempt that exhausts
//! the budget suspends the entry instead of re-offering it, so a persistently
//! failing completion can neither spin wake turns nor wedge the mailbox.
//!
//! [iac]: codex_protocol::protocol::InterAgentCommunication

use std::collections::VecDeque;

use uuid::Uuid;

use crate::context::ExecCompletion;
use crate::unified_exec::completion_receipt::ReceiptId;
use crate::unified_exec::completion_receipt::ReceiptOwner;

/// Turn trigger recorded when an idle wake starts for runtime completions.
///
/// The wake preserves the thread's execution settings, invents no initiating
/// agent, and resets no human quota.
pub(crate) const EXEC_COMPLETION_TURN_TRIGGER: &str = "exec_completion";

/// Maximum failed sampling attempts before a runtime entry is suspended.
///
/// One attempt is one wake turn that carried the entry without observing its
/// fragment in a submitted prompt (failed or aborted submission, or omission
/// from the prompt). In-turn transport retries are bounded separately and do
/// not count here. The failing attempt that reaches this budget suspends the
/// entry: it stays retained and visible but starts no further wake turns.
pub(crate) const MAX_RUNTIME_SAMPLING_ATTEMPTS: u32 = 3;

/// Opaque sampling lease for one runtime mailbox entry.
///
/// The token is minted by [`RuntimeMailbox::lease_available`] and must be
/// presented to acknowledge or fail the lease. Callers cannot mint leases.
/// The completion snapshot was captured at mailbox admission and is what the
/// record path renders; story D acknowledges the lease after sampling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuntimeLease {
    receipt_id: ReceiptId,
    token: Uuid,
    completion: ExecCompletion,
}

impl RuntimeLease {
    pub(crate) fn receipt_id(&self) -> ReceiptId {
        self.receipt_id
    }

    pub(crate) fn completion(&self) -> &ExecCompletion {
        &self.completion
    }
}

/// One pending runtime notification carrying a receipt reference.
#[derive(Debug)]
struct PendingRuntimeNotification {
    receipt_id: ReceiptId,
    // Carries the B receipt owner for retained-output reads (story E).
    #[allow(dead_code)]
    owner: ReceiptOwner,
    completion: ExecCompletion,
    suspended: bool,
    lease: Option<Uuid>,
    /// Failed sampling attempts so far; reaching
    /// [`MAX_RUNTIME_SAMPLING_ATTEMPTS`] suspends the entry.
    attempts: u32,
}

/// Mailbox-side lease tracking for runtime notifications.
///
/// Entries are removed only by acknowledgement or cancellation. Draining
/// leases unleased, non-suspended entries without removing them.
#[derive(Debug, Default)]
pub(crate) struct RuntimeMailbox {
    entries: VecDeque<PendingRuntimeNotification>,
}

impl RuntimeMailbox {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Enqueues a runtime notification for a completion receipt.
    ///
    /// Returns `false` without duplicating when the receipt is already
    /// present, whether leased or not. The completion snapshot is captured at
    /// admission and rendered if a later wake leases this entry.
    pub(crate) fn enqueue(
        &mut self,
        receipt_id: ReceiptId,
        owner: ReceiptOwner,
        completion: ExecCompletion,
    ) -> bool {
        if self
            .entries
            .iter()
            .any(|entry| entry.receipt_id == receipt_id)
        {
            return false;
        }
        self.entries.push_back(PendingRuntimeNotification {
            receipt_id,
            owner,
            completion,
            suspended: false,
            lease: None,
            attempts: 0,
        });
        true
    }

    /// Reports whether an unleased, non-suspended entry awaits a wake turn.
    ///
    /// Leased entries are already handed to a turn and suspended entries never
    /// start one, so neither counts as pending work here.
    pub(crate) fn has_pending(&self) -> bool {
        self.entries
            .iter()
            .any(|entry| !entry.suspended && entry.lease.is_none())
    }

    /// Reports whether an unsampled, non-suspended entry exists.
    ///
    /// Leased but unacknowledged entries still count so they keep priority
    /// over goal continuation. Suspended entries never count.
    pub(crate) fn has_trigger(&self) -> bool {
        self.entries.iter().any(|entry| !entry.suspended)
    }

    /// Leases every unleased, non-suspended entry in FIFO order.
    ///
    /// Test-only: production wake caps one sampling request. Leased and
    /// suspended entries are left untouched and never re-offered while their
    /// state holds.
    #[cfg(test)]
    pub(crate) fn lease_available(&mut self) -> Vec<RuntimeLease> {
        self.lease_available_up_to(usize::MAX)
    }

    /// Leases up to `limit` unleased, non-suspended entries in FIFO order.
    ///
    /// Entries beyond the limit stay unleased and retained for a later wake,
    /// so one sampling request never carries more fragments than the cap.
    pub(crate) fn lease_available_up_to(&mut self, limit: usize) -> Vec<RuntimeLease> {
        let mut leases = Vec::new();
        for entry in self
            .entries
            .iter_mut()
            .filter(|entry| !entry.suspended && entry.lease.is_none())
            .take(limit)
        {
            let token = Uuid::new_v4();
            entry.lease = Some(token);
            leases.push(RuntimeLease {
                receipt_id: entry.receipt_id,
                token,
                completion: entry.completion.clone(),
            });
        }
        leases
    }

    /// Acknowledges a lease after its fragment was observed in a submitted prompt.
    ///
    /// Removes the entry. Unknown receipts, stale tokens, and leases for
    /// cancelled entries are refused.
    pub(crate) fn acknowledge(&mut self, lease: &RuntimeLease) -> bool {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.receipt_id == lease.receipt_id)
        else {
            return false;
        };
        if self.entries[index].lease != Some(lease.token) {
            return false;
        }
        self.entries.remove(index);
        true
    }

    /// Returns a leased entry to the unleased state after a failed attempt.
    ///
    /// The next drain retries it with a fresh token. Stale tokens and unknown
    /// receipts are refused without duplicating the entry and without counting
    /// an attempt. The attempt that reaches [`MAX_RUNTIME_SAMPLING_ATTEMPTS`]
    /// suspends the entry instead: it stays retained but is excluded from
    /// pending, trigger, and lease queries.
    pub(crate) fn fail(&mut self, lease: &RuntimeLease) -> bool {
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.receipt_id == lease.receipt_id)
        else {
            return false;
        };
        if entry.lease != Some(lease.token) {
            return false;
        }
        entry.lease = None;
        entry.attempts = entry.attempts.saturating_add(1);
        let exhausted = entry.attempts >= MAX_RUNTIME_SAMPLING_ATTEMPTS;
        let receipt_id = entry.receipt_id;
        if exhausted {
            self.suspend(receipt_id);
        }
        true
    }

    /// Reports whether the entry is suspended (attempts exhausted).
    ///
    /// Unknown receipts report `false`. Callers use this after [`Self::fail`]
    /// to warn visibly when an entry stops retrying.
    pub(crate) fn is_suspended(&self, receipt_id: ReceiptId) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.receipt_id == receipt_id && entry.suspended)
    }

    /// Removes the entry whether leased or not.
    ///
    /// Cancelled entries are never re-offered, and their lease tokens can no
    /// longer acknowledge. Production callers arrive with receipt
    /// cancellation (story E); covered by tests until then.
    #[allow(dead_code)]
    pub(crate) fn cancel(&mut self, receipt_id: ReceiptId) -> bool {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.receipt_id == receipt_id)
        else {
            return false;
        };
        self.entries.remove(index);
        true
    }

    /// Marks an entry suspended after retries are exhausted.
    ///
    /// Suspended entries are excluded from pending, trigger, and lease
    /// queries until they are cancelled or acknowledged.
    pub(crate) fn suspend(&mut self, receipt_id: ReceiptId) -> bool {
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.receipt_id == receipt_id)
        else {
            return false;
        };
        entry.suspended = true;
        true
    }
}

#[cfg(test)]
#[path = "runtime_mailbox_tests.rs"]
mod tests;
