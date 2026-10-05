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
//! Sampling acknowledgement wiring (story D) and the context fragment plus
//! internal `TurnInput` variant (leaf 2) are out of scope. Until then, idle
//! wake leases entries via [`RuntimeMailbox::lease_available`] and hands the
//! leases to the turn starter; active-turn delivery arrives with the variant.
//!
//! [iac]: codex_protocol::protocol::InterAgentCommunication

use std::collections::VecDeque;

use uuid::Uuid;

use crate::unified_exec::completion_receipt::ReceiptId;
use crate::unified_exec::completion_receipt::ReceiptOwner;

/// Turn trigger recorded when an idle wake starts for runtime completions.
///
/// The wake preserves the thread's execution settings, invents no initiating
/// agent, and resets no human quota.
pub(crate) const EXEC_COMPLETION_TURN_TRIGGER: &str = "exec_completion";

/// Opaque sampling lease for one runtime mailbox entry.
///
/// The token is minted by [`RuntimeMailbox::lease_available`] and must be
/// presented to acknowledge or fail the lease. Callers cannot mint leases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RuntimeLease {
    receipt_id: ReceiptId,
    token: Uuid,
}

impl RuntimeLease {
    // Leaf 2 maps leases to the internal TurnInput variant; story D uses the
    // receipt id to acknowledge sampling. Covered by tests until then.
    #[allow(dead_code)]
    pub(crate) fn receipt_id(&self) -> ReceiptId {
        self.receipt_id
    }
}

/// One pending runtime notification carrying a receipt reference.
#[derive(Debug)]
struct PendingRuntimeNotification {
    receipt_id: ReceiptId,
    // Carries the B receipt owner for sampling-acknowledgement wiring (story D).
    #[allow(dead_code)]
    owner: ReceiptOwner,
    suspended: bool,
    lease: Option<Uuid>,
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
    /// present, whether leased or not.
    pub(crate) fn enqueue(&mut self, receipt_id: ReceiptId, owner: ReceiptOwner) -> bool {
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
            suspended: false,
            lease: None,
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
    /// Leased and suspended entries are left untouched and never re-offered
    /// while their state holds.
    pub(crate) fn lease_available(&mut self) -> Vec<RuntimeLease> {
        let mut leases = Vec::new();
        for entry in self
            .entries
            .iter_mut()
            .filter(|entry| !entry.suspended && entry.lease.is_none())
        {
            let token = Uuid::new_v4();
            entry.lease = Some(token);
            leases.push(RuntimeLease {
                receipt_id: entry.receipt_id,
                token,
            });
        }
        leases
    }

    /// Acknowledges a lease after its contents were sampled.
    ///
    /// Removes the entry. Unknown receipts, stale tokens, and leases for
    /// cancelled entries are refused. Production callers arrive with sampling
    /// acknowledgement (story D); covered by tests until then.
    #[allow(dead_code)]
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
    /// receipts are refused without duplicating the entry. Production callers
    /// arrive with sampling acknowledgement (story D); covered by tests until then.
    #[allow(dead_code)]
    pub(crate) fn fail(&mut self, lease: &RuntimeLease) -> bool {
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.receipt_id == lease.receipt_id)
        else {
            return false;
        };
        entry.lease = None;
        true
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
    /// queries until they are cancelled or acknowledged. Production callers
    /// arrive with bounded sampling retries (story D); covered by tests until then.
    #[allow(dead_code)]
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
