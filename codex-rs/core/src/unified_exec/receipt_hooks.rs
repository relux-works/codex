//! Thin receipt hooks wiring the completion receipt store into unified exec.
//!
//! The process manager owns one [`CompletionReceiptStore`] per runtime
//! generation. Opted-in launches reserve a receipt before spawning; the exit
//! watcher publishes the finalized exit at the point it classifies the
//! terminal event; release, terminate, interrupt, and shutdown paths cancel
//! with matching reasons before the process is killed. Default launches never
//! touch this state.
//
// Release/read/lease/acknowledge entry points serve later stories (mailbox,
// exec_notification tool) and are covered by tests until then.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::ExitWatcherReceiptHook;
use super::UnifiedExecContext;
use super::UnifiedExecProcessManager;
use super::completion_receipt::CancellationReason;
use super::completion_receipt::CompletionReceiptStore;
use super::completion_receipt::InitialResponseDecision;
use super::completion_receipt::MAX_COMPLETION_RECEIPTS;
use super::completion_receipt::ReceiptError;
use super::completion_receipt::ReceiptId;
use super::completion_receipt::ReceiptOwner;
use super::completion_receipt::ReceiptStatus;
use super::completion_receipt::SamplingLease;
use super::completion_receipt::SamplingSource;
use super::completion_receipt::TerminalCompletion;
use super::receipt_output::RetainedOutputSnapshot;
use super::receipt_output::RetentionLookup;
use super::receipt_output::RetentionState;

pub(crate) fn next_receipt_generation() -> u64 {
    static NEXT_RECEIPT_GENERATION: AtomicU64 = AtomicU64::new(1);
    NEXT_RECEIPT_GENERATION.fetch_add(1, Ordering::Relaxed)
}

struct ReceiptBinding {
    owner: ReceiptOwner,
    process_id: Option<i32>,
}

/// Receipt bindings and retention for one runtime generation.
///
/// All mutations that change combined capacity (reserve, inline settle,
/// cancel, acknowledge transfer) run under the single manager lock that owns
/// this state, so the active-plus-sampled count stays exact.
#[derive(Default)]
pub(crate) struct ReceiptHooksState {
    bindings: HashMap<ReceiptId, ReceiptBinding>,
    by_process: HashMap<i32, ReceiptId>,
    pub(crate) retention: RetentionState,
    sample_seq: u64,
}

impl ReceiptHooksState {
    fn bind(&mut self, receipt_id: ReceiptId, owner: ReceiptOwner, process_id: i32) {
        if let Some(previous) = self.by_process.insert(process_id, receipt_id)
            && previous != receipt_id
            && let Some(binding) = self.bindings.get_mut(&previous)
        {
            binding.process_id = None;
        }
        if let Some(existing) = self.bindings.get(&receipt_id)
            && let Some(old_process_id) = existing.process_id
            && old_process_id != process_id
        {
            self.by_process.remove(&old_process_id);
        }
        self.bindings.insert(
            receipt_id,
            ReceiptBinding {
                owner,
                process_id: Some(process_id),
            },
        );
    }

    fn unbind_process(&mut self, process_id: i32) {
        if let Some(receipt_id) = self.by_process.remove(&process_id)
            && let Some(binding) = self.bindings.get_mut(&receipt_id)
        {
            binding.process_id = None;
        }
    }

    fn remove_binding(&mut self, receipt_id: ReceiptId) {
        if let Some(binding) = self.bindings.remove(&receipt_id)
            && let Some(process_id) = binding.process_id
        {
            self.by_process.remove(&process_id);
        }
    }

    fn binding_for_process(&self, process_id: i32) -> Option<(ReceiptId, ReceiptOwner)> {
        let receipt_id = *self.by_process.get(&process_id)?;
        let binding = self.bindings.get(&receipt_id)?;
        Some((receipt_id, binding.owner.clone()))
    }

    fn binding_owner_mismatch(&self, receipt_id: ReceiptId, owner: &ReceiptOwner) -> bool {
        self.bindings
            .get(&receipt_id)
            .is_some_and(|binding| binding.owner != *owner)
    }

    fn next_sample_seq(&mut self) -> u64 {
        self.sample_seq = self.sample_seq.saturating_add(1);
        self.sample_seq
    }

    fn clear_all(&mut self) {
        self.bindings.clear();
        self.by_process.clear();
        self.retention.clear();
    }
}

impl UnifiedExecProcessManager {
    /// Builds the receipt owner for an opted-in launch from live context.
    pub(crate) fn receipt_owner_for(
        &self,
        context: &UnifiedExecContext,
    ) -> Result<ReceiptOwner, ReceiptError> {
        ReceiptOwner::new(
            context.session.thread_id,
            self.receipt_generation,
            context.call_id.clone(),
        )
    }

    pub(crate) fn receipt_store(&self) -> &CompletionReceiptStore {
        &self.receipt_store
    }

    /// Reserves a receipt and binds it to the launching process atomically.
    ///
    /// Combined capacity counts active receipts plus sampled receipts with
    /// retained output. When full with at least one sampled entry, the
    /// least-recently-sampled output is retired first; with 64 unsampled
    /// receipts the reservation is refused before any process starts.
    pub(crate) async fn reserve_completion_receipt(
        &self,
        owner: ReceiptOwner,
        process_id: i32,
    ) -> Result<ReceiptId, ReceiptError> {
        let mut hooks = self.receipt_hooks.lock().await;
        let used = self.receipt_store.active_len()?;
        if used >= MAX_COMPLETION_RECEIPTS
            && hooks.retention.retire_least_recently_sampled().is_none()
        {
            return Err(ReceiptError::CapacityExceeded {
                capacity: MAX_COMPLETION_RECEIPTS,
            });
        }
        let receipt_id = self.receipt_store.reserve(owner.clone())?;
        hooks.bind(receipt_id, owner, process_id);
        Ok(receipt_id)
    }

    /// Forgets the process side of a binding without cancelling the receipt.
    pub(crate) async fn unbind_process_from_receipt(&self, process_id: i32) {
        self.receipt_hooks.lock().await.unbind_process(process_id);
    }

    pub(crate) fn watcher_receipt_hook(
        &self,
        receipt_id: ReceiptId,
        owner: ReceiptOwner,
    ) -> ExitWatcherReceiptHook {
        ExitWatcherReceiptHook {
            receipt_id,
            owner,
            store: Arc::clone(&self.receipt_store),
            hooks: Arc::clone(&self.receipt_hooks),
        }
    }

    /// Settles an exit observed before arming as an inline result.
    ///
    /// Best-effort: a concurrent watcher publication or release wins the race
    /// instead, and every outcome stays exactly-once. Inline delivery frees
    /// the slot and retains no output.
    pub(crate) async fn settle_opted_in_inline(
        &self,
        receipt_id: ReceiptId,
        owner: &ReceiptOwner,
        completion: TerminalCompletion,
    ) {
        let mut hooks = self.receipt_hooks.lock().await;
        let _ = self
            .receipt_store
            .publish_exit(receipt_id, owner, completion);
        let _ = self.receipt_store.resolve_initial_response(
            receipt_id,
            owner,
            InitialResponseDecision::InlineResult,
        );
        hooks.retention.drop(receipt_id);
        hooks.remove_binding(receipt_id);
    }

    /// Cancels the receipt bound to a process, best-effort.
    pub(crate) async fn cancel_receipt_for_process(
        &self,
        process_id: i32,
        reason: CancellationReason,
    ) {
        let mut hooks = self.receipt_hooks.lock().await;
        let Some((receipt_id, owner)) = hooks.binding_for_process(process_id) else {
            return;
        };
        let _ = self.receipt_store.cancel(receipt_id, &owner, reason);
        hooks.retention.drop(receipt_id);
        hooks.remove_binding(receipt_id);
    }

    /// Cancels every receipt of this generation, freeing all slots.
    pub(crate) async fn cancel_all_completion_receipts(&self, reason: CancellationReason) {
        let mut hooks = self.receipt_hooks.lock().await;
        let bound: Vec<(ReceiptId, ReceiptOwner)> = hooks
            .bindings
            .iter()
            .map(|(receipt_id, binding)| (*receipt_id, binding.owner.clone()))
            .collect();
        for (receipt_id, owner) in bound {
            let _ = self.receipt_store.cancel(receipt_id, &owner, reason);
        }
        hooks.clear_all();
    }

    /// Releases a receipt without killing its process.
    ///
    /// Unknown and foreign receipts are refused without mutating anything.
    /// Releasing an already-terminal receipt still drops retained output.
    pub(crate) async fn release_completion_receipt(
        &self,
        receipt_id: ReceiptId,
        owner: &ReceiptOwner,
    ) -> Result<(), ReceiptError> {
        let mut hooks = self.receipt_hooks.lock().await;
        if hooks.binding_owner_mismatch(receipt_id, owner) {
            return Err(ReceiptError::ForeignOwner);
        }
        let had_output = match hooks.retention.lookup(receipt_id, owner) {
            RetentionLookup::ForeignOwner => return Err(ReceiptError::ForeignOwner),
            RetentionLookup::Present { .. } | RetentionLookup::Retired => {
                hooks.retention.drop(receipt_id);
                hooks.remove_binding(receipt_id);
                true
            }
            RetentionLookup::Absent => {
                hooks.remove_binding(receipt_id);
                false
            }
        };
        match self
            .receipt_store
            .cancel(receipt_id, owner, CancellationReason::Released)
        {
            Ok(()) | Err(ReceiptError::AlreadyTerminal) => Ok(()),
            Err(ReceiptError::UnknownReceipt) if had_output => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Reads retained output by receipt id for the matching owner.
    pub(crate) async fn read_retained_output(
        &self,
        receipt_id: ReceiptId,
        owner: &ReceiptOwner,
    ) -> Result<RetainedOutputSnapshot, ReceiptError> {
        let hooks = self.receipt_hooks.lock().await;
        match hooks.retention.lookup(receipt_id, owner) {
            RetentionLookup::Present {
                bytes,
                omitted_bytes,
            } => Ok(RetainedOutputSnapshot {
                bytes,
                truncated: omitted_bytes > 0,
                omitted_bytes,
            }),
            RetentionLookup::Retired => Err(ReceiptError::Retired),
            RetentionLookup::ForeignOwner => Err(ReceiptError::ForeignOwner),
            RetentionLookup::Absent => match self.receipt_store.status(receipt_id, owner) {
                Ok(ReceiptStatus::Sampled { .. }) => Err(ReceiptError::AlreadyConsumed),
                Ok(ReceiptStatus::Cancelled { reason }) => Err(ReceiptError::Cancelled { reason }),
                Ok(actual) => Err(ReceiptError::InvalidTransition { actual }),
                Err(err) => Err(err),
            },
        }
    }

    /// Leases the single queued claim for the pushed-completion path.
    pub(crate) fn lease_pushed_completion(
        &self,
        receipt_id: ReceiptId,
        owner: &ReceiptOwner,
    ) -> Result<SamplingLease, ReceiptError> {
        self.receipt_store
            .lease_for_sampling(receipt_id, owner, SamplingSource::PushedCompletion)
    }

    /// Acknowledges a pushed lease, retaining output under the sampled slot.
    pub(crate) async fn acknowledge_pushed_completion(
        &self,
        lease: &SamplingLease,
    ) -> Result<TerminalCompletion, ReceiptError> {
        let mut hooks = self.receipt_hooks.lock().await;
        let completion = self.receipt_store.acknowledge_sampled(lease)?;
        let sampled_seq = hooks.next_sample_seq();
        hooks
            .retention
            .move_to_sampled(lease.receipt_id(), sampled_seq);
        Ok(completion)
    }

    /// Claims terminal stdin output through the single shared claim.
    ///
    /// Best-effort: when the pushed path already consumed the claim (or the
    /// exit is not published yet) the tool response still returns normally and
    /// no second completion is produced.
    pub(crate) async fn claim_terminal_stdin_output(&self, process_id: i32) {
        let mut hooks = self.receipt_hooks.lock().await;
        let Some((receipt_id, owner)) = hooks.binding_for_process(process_id) else {
            return;
        };
        let Ok(lease) = self.receipt_store.lease_for_sampling(
            receipt_id,
            &owner,
            SamplingSource::TerminalStdinOutput,
        ) else {
            return;
        };
        let Ok(_) = self.receipt_store.acknowledge_sampled(&lease) else {
            return;
        };
        let sampled_seq = hooks.next_sample_seq();
        hooks.retention.move_to_sampled(receipt_id, sampled_seq);
    }

    pub(crate) async fn receipt_for_process(
        &self,
        process_id: i32,
    ) -> Option<(ReceiptId, ReceiptOwner)> {
        self.receipt_hooks
            .lock()
            .await
            .binding_for_process(process_id)
    }

    pub(crate) fn receipt_status(
        &self,
        receipt_id: ReceiptId,
        owner: &ReceiptOwner,
    ) -> Result<ReceiptStatus, ReceiptError> {
        self.receipt_store.status(receipt_id, owner)
    }

    /// Combined slots held: active receipts plus sampled retained outputs.
    pub(crate) async fn receipt_capacity_used(&self) -> Result<usize, ReceiptError> {
        let hooks = self.receipt_hooks.lock().await;
        Ok(self.receipt_store.active_len()? + hooks.retention.sampled_count())
    }
}

#[cfg(test)]
#[path = "receipt_hooks_tests.rs"]
mod tests;
