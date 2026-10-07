//! In-memory receipt state for opted-in background exec completions.
//!
//! This module owns only the receipt lifecycle. Exec launch, watcher, mailbox,
//! and sampling-request integration are separate stages.
//
// The exported API is intentionally unused until those later integration stages.
#![allow(dead_code)]

use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use codex_protocol::ThreadId;
use thiserror::Error;
use uuid::Uuid;

pub(crate) const MAX_COMPLETION_RECEIPTS: usize = 64;
const MAX_TERMINAL_RECEIPTS: usize = 64;
const MAX_RECEIPT_ID_ATTEMPTS: usize = 4;
const MAX_CALL_ID_BYTES: usize = 256;

/// Identifies the thread runtime and tool call that owns a receipt.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ReceiptOwner {
    thread_id: ThreadId,
    runtime_generation: u64,
    call_id: Box<str>,
}

impl ReceiptOwner {
    pub(crate) fn new(
        thread_id: ThreadId,
        runtime_generation: u64,
        call_id: impl Into<String>,
    ) -> Result<Self, ReceiptError> {
        let call_id = call_id.into();
        if call_id.is_empty() || call_id.len() > MAX_CALL_ID_BYTES {
            return Err(ReceiptError::InvalidOwner);
        }

        Ok(Self {
            thread_id,
            runtime_generation,
            call_id: call_id.into_boxed_str(),
        })
    }
}

impl Debug for ReceiptOwner {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReceiptOwner(<opaque>)")
    }
}

/// Opaque identifier for one reserved completion slot.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ReceiptId(Uuid);

impl Debug for ReceiptId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReceiptId(<opaque>)")
    }
}

impl ReceiptId {
    /// Model-visible handle for this receipt: the hyphenated lowercase UUID.
    ///
    /// [`Debug`] stays opaque so logs never leak handles; use this only for
    /// model-visible context. The format is stable: a later story parses it
    /// back for retained-output reads.
    pub(crate) fn model_handle(&self) -> String {
        self.0.hyphenated().to_string()
    }
}

/// The finalized process outcome retained by the receipt state machine.
///
/// `exit_code` is `None` when the process failed without producing an exit
/// code (for example a failure message), mirroring the failed
/// `ExecCommandEnd` event. Otherwise it carries the observed exit code and
/// `timed_out` preserves the process timeout flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TerminalCompletion {
    pub(crate) exit_code: Option<i32>,
    pub(crate) timed_out: bool,
}

/// Internal opt-in for exit notification on an exec launch.
///
/// This is never exposed to the model and never changes the tool schema;
/// exposing `notify_on_exit` is a later story. Default launches behave
/// exactly as before and never reserve a receipt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ExecCompletionMode {
    #[default]
    Default,
    NotifyOnExit,
}

/// The initial tool response either returns a terminal result or arms a wake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InitialResponseDecision {
    InlineResult,
    Arm,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InitialResponseOutcome {
    InlineResult(TerminalCompletion),
    Armed,
    Queued,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExitPublicationOutcome {
    RetainedUntilDecision,
    Queued,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SamplingSource {
    PushedCompletion,
    TerminalStdinOutput,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CancellationReason {
    Released,
    OwnerStopped,
    Shutdown,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReceiptStatus {
    Reserved,
    Armed,
    Queued,
    LeasedToSampling { source: SamplingSource },
    InlineResult,
    Sampled { source: SamplingSource },
    Cancelled { reason: CancellationReason },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum ReceiptError {
    #[error("receipt capacity is full (limit {capacity})")]
    CapacityExceeded { capacity: usize },
    #[error("receipt owner must have a non-empty call id no longer than 256 bytes")]
    InvalidOwner,
    #[error("receipt id is unknown or has expired from the bounded terminal history")]
    UnknownReceipt,
    #[error("receipt belongs to a different thread, runtime generation, or call")]
    ForeignOwner,
    #[error("receipt is already leased to a sampling path")]
    AlreadyLeased,
    #[error("receipt claim was already consumed")]
    AlreadyConsumed,
    #[error("receipt was cancelled: {reason:?}")]
    Cancelled { reason: CancellationReason },
    #[error("receipt output was retired to free capacity")]
    Retired,
    #[error("receipt is not valid for this operation in state {actual:?}")]
    InvalidTransition { actual: ReceiptStatus },
    #[error("sampling lease is stale or no longer owns this receipt")]
    StaleLease,
    #[error("receipt is already terminal")]
    AlreadyTerminal,
    #[error("could not allocate a unique opaque receipt id")]
    IdGenerationFailed,
    #[error("completion receipt lock was poisoned by a panic")]
    LockPoisoned,
    #[error("completion receipt lock is contended")]
    LockContended,
}

/// Pending receipt ids by state, read under one lock.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PendingReceiptLists {
    pub(crate) armed: Vec<ReceiptId>,
    pub(crate) queued: Vec<ReceiptId>,
    pub(crate) leased: Vec<ReceiptId>,
}

/// A sampling claim. Its fields are private so a caller cannot mint a lease.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SamplingLease {
    receipt_id: ReceiptId,
    owner: ReceiptOwner,
    token: Uuid,
    source: SamplingSource,
}

impl Debug for SamplingLease {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SamplingLease(<opaque>)")
    }
}

impl SamplingLease {
    pub(crate) fn receipt_id(&self) -> ReceiptId {
        self.receipt_id
    }
}

#[derive(Default)]
pub(crate) struct CompletionReceiptStore {
    state: Mutex<StoreState>,
    revision: Arc<AtomicU64>,
}

#[derive(Default)]
struct StoreState {
    active: HashMap<ReceiptId, ReceiptRecord>,
    terminal: VecDeque<TerminalReceipt>,
}

struct ReceiptRecord {
    owner: ReceiptOwner,
    phase: ReceiptPhase,
}

enum ReceiptPhase {
    Reserved {
        completion: Option<TerminalCompletion>,
    },
    Armed,
    Queued(TerminalCompletion),
    LeasedToSampling {
        completion: TerminalCompletion,
        token: Uuid,
        source: SamplingSource,
    },
    InlineResult,
    Sampled {
        source: SamplingSource,
    },
    Cancelled(CancellationReason),
}

impl ReceiptPhase {
    fn status(&self) -> ReceiptStatus {
        match self {
            Self::Reserved { .. } => ReceiptStatus::Reserved,
            Self::Armed => ReceiptStatus::Armed,
            Self::Queued(_) => ReceiptStatus::Queued,
            Self::LeasedToSampling { source, .. } => {
                ReceiptStatus::LeasedToSampling { source: *source }
            }
            Self::InlineResult => ReceiptStatus::InlineResult,
            Self::Sampled { source } => ReceiptStatus::Sampled { source: *source },
            Self::Cancelled(reason) => ReceiptStatus::Cancelled { reason: *reason },
        }
    }

    fn error(&self) -> ReceiptError {
        match self {
            Self::Reserved { .. }
            | Self::Armed
            | Self::Queued(_)
            | Self::LeasedToSampling { .. } => ReceiptError::InvalidTransition {
                actual: self.status(),
            },
            Self::InlineResult => ReceiptError::InvalidTransition {
                actual: ReceiptStatus::InlineResult,
            },
            Self::Sampled { .. } => ReceiptError::AlreadyConsumed,
            Self::Cancelled(reason) => ReceiptError::Cancelled { reason: *reason },
        }
    }
}

struct TerminalReceipt {
    id: ReceiptId,
    owner: ReceiptOwner,
    phase: ReceiptPhase,
}

impl StoreState {
    fn terminal(&self, receipt_id: ReceiptId) -> Option<&TerminalReceipt> {
        self.terminal
            .iter()
            .rev()
            .find(|receipt| receipt.id == receipt_id)
    }

    fn terminal_error(&self, receipt_id: ReceiptId, owner: &ReceiptOwner) -> ReceiptError {
        match self.terminal(receipt_id) {
            Some(receipt) if receipt.owner != *owner => ReceiptError::ForeignOwner,
            Some(receipt) => receipt.phase.error(),
            None => ReceiptError::UnknownReceipt,
        }
    }

    fn retire(&mut self, receipt_id: ReceiptId, phase: ReceiptPhase) -> Result<(), ReceiptError> {
        let record = self
            .active
            .remove(&receipt_id)
            .ok_or(ReceiptError::UnknownReceipt)?;
        if self.terminal.len() == MAX_TERMINAL_RECEIPTS {
            self.terminal.pop_front();
        }
        self.terminal.push_back(TerminalReceipt {
            id: receipt_id,
            owner: record.owner,
            phase,
        });
        Ok(())
    }
}

impl CompletionReceiptStore {
    fn lock_state(&self) -> Result<MutexGuard<'_, StoreState>, ReceiptError> {
        self.state.lock().map_err(|_| ReceiptError::LockPoisoned)
    }

    /// Creates a store sharing the given pending-work revision.
    pub(crate) fn with_revision(revision: Arc<AtomicU64>) -> Self {
        Self {
            state: Mutex::new(StoreState::default()),
            revision,
        }
    }

    /// Returns the current pending-work revision.
    #[cfg(test)]
    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::SeqCst)
    }

    fn bump_revision(&self) {
        self.revision.fetch_add(1, Ordering::SeqCst);
    }

    /// Lists pending receipt ids by state under one lock.
    ///
    /// Uses a non-blocking lock so snapshot readers fail with an explicit
    /// error instead of stalling a turn. Armed, Queued, and LeasedToSampling
    /// are read atomically: a receipt appears in exactly one list.
    pub(crate) fn try_list_pending(&self) -> Result<PendingReceiptLists, ReceiptError> {
        let state = self.state.try_lock().map_err(|err| match err {
            std::sync::TryLockError::Poisoned(_) => ReceiptError::LockPoisoned,
            std::sync::TryLockError::WouldBlock => ReceiptError::LockContended,
        })?;
        let mut lists = PendingReceiptLists::default();
        for (receipt_id, record) in &state.active {
            match &record.phase {
                ReceiptPhase::Armed => lists.armed.push(*receipt_id),
                ReceiptPhase::Queued(_) => lists.queued.push(*receipt_id),
                ReceiptPhase::LeasedToSampling { .. } => lists.leased.push(*receipt_id),
                ReceiptPhase::Reserved { .. }
                | ReceiptPhase::InlineResult
                | ReceiptPhase::Sampled { .. }
                | ReceiptPhase::Cancelled(_) => {}
            }
        }
        Ok(lists)
    }

    /// Runs `f` while holding the receipt-store lock, for admission publication.
    ///
    /// LOCK ORDER (goal admission publication, AC7): `active_turn` (tokio, held
    /// by the `start_task` caller) -> `Session.state` (tokio) -> receipt-store
    /// `state` (std, here) -> runtime mailbox (tokio `try_lock`, innermost).
    /// The store and mailbox are leaves: no receipt-store, mailbox, or
    /// receipt-hooks method acquires `Session.state`, `active_turn`, or any
    /// other lock while held; watchers, process hooks, and test holders call
    /// the store without holding session locks. Inner locks use `try_lock` so
    /// contention fails safe (the caller rejects) instead of blocking the
    /// executor or deadlocking. Never `.await` while `f` runs.
    pub(crate) fn try_with_locked_state<R>(
        &self,
        f: impl FnOnce() -> R,
    ) -> Result<R, ReceiptError> {
        let _guard = self.state.try_lock().map_err(|err| match err {
            std::sync::TryLockError::Poisoned(_) => ReceiptError::LockPoisoned,
            std::sync::TryLockError::WouldBlock => ReceiptError::LockContended,
        })?;
        Ok(f())
    }

    /// Reserves capacity before the caller launches an opted-in process.
    pub(crate) fn reserve(&self, owner: ReceiptOwner) -> Result<ReceiptId, ReceiptError> {
        let mut state = self.lock_state()?;
        if state.active.len() >= MAX_COMPLETION_RECEIPTS {
            return Err(ReceiptError::CapacityExceeded {
                capacity: MAX_COMPLETION_RECEIPTS,
            });
        }

        let mut receipt_id = None;
        for _ in 0..MAX_RECEIPT_ID_ATTEMPTS {
            let candidate = ReceiptId(Uuid::new_v4());
            if !state.active.contains_key(&candidate) && state.terminal(candidate).is_none() {
                receipt_id = Some(candidate);
                break;
            }
        }
        let receipt_id = receipt_id.ok_or(ReceiptError::IdGenerationFailed)?;
        state.active.insert(
            receipt_id,
            ReceiptRecord {
                owner,
                phase: ReceiptPhase::Reserved { completion: None },
            },
        );
        self.bump_revision();
        Ok(receipt_id)
    }

    /// Resolves whether the initial response is terminal or acknowledges a wake.
    pub(crate) fn resolve_initial_response(
        &self,
        receipt_id: ReceiptId,
        owner: &ReceiptOwner,
        decision: InitialResponseDecision,
    ) -> Result<InitialResponseOutcome, ReceiptError> {
        enum Action {
            Inline(TerminalCompletion),
            Armed,
            Queued(TerminalCompletion),
        }

        let mut state = self.lock_state()?;
        let action = match state.active.get(&receipt_id) {
            Some(record) if record.owner != *owner => return Err(ReceiptError::ForeignOwner),
            Some(record) => match (&record.phase, decision) {
                (
                    ReceiptPhase::Reserved {
                        completion: Some(completion),
                    },
                    InitialResponseDecision::InlineResult,
                ) => Action::Inline(*completion),
                (
                    ReceiptPhase::Reserved { completion: None },
                    InitialResponseDecision::InlineResult,
                ) => {
                    return Err(ReceiptError::InvalidTransition {
                        actual: ReceiptStatus::Reserved,
                    });
                }
                (
                    ReceiptPhase::Reserved {
                        completion: Some(completion),
                    },
                    InitialResponseDecision::Arm,
                ) => Action::Queued(*completion),
                (ReceiptPhase::Reserved { completion: None }, InitialResponseDecision::Arm) => {
                    Action::Armed
                }
                (phase, _) => {
                    return Err(ReceiptError::InvalidTransition {
                        actual: phase.status(),
                    });
                }
            },
            None => return Err(state.terminal_error(receipt_id, owner)),
        };

        let outcome = match action {
            Action::Inline(completion) => {
                state.retire(receipt_id, ReceiptPhase::InlineResult)?;
                InitialResponseOutcome::InlineResult(completion)
            }
            Action::Armed => {
                if let Some(record) = state.active.get_mut(&receipt_id) {
                    record.phase = ReceiptPhase::Armed;
                    InitialResponseOutcome::Armed
                } else {
                    return Err(ReceiptError::UnknownReceipt);
                }
            }
            Action::Queued(completion) => {
                if let Some(record) = state.active.get_mut(&receipt_id) {
                    record.phase = ReceiptPhase::Queued(completion);
                    InitialResponseOutcome::Queued
                } else {
                    return Err(ReceiptError::UnknownReceipt);
                }
            }
        };
        self.bump_revision();
        Ok(outcome)
    }

    /// Publishes a fully finalized exit, retaining it if the response is undecided.
    pub(crate) fn publish_exit(
        &self,
        receipt_id: ReceiptId,
        owner: &ReceiptOwner,
        completion: TerminalCompletion,
    ) -> Result<ExitPublicationOutcome, ReceiptError> {
        let mut state = self.lock_state()?;
        let record = match state.active.get_mut(&receipt_id) {
            Some(record) if record.owner != *owner => return Err(ReceiptError::ForeignOwner),
            Some(record) => record,
            None => return Err(state.terminal_error(receipt_id, owner)),
        };

        let outcome = match &mut record.phase {
            ReceiptPhase::Reserved { completion: stored } if stored.is_none() => {
                *stored = Some(completion);
                ExitPublicationOutcome::RetainedUntilDecision
            }
            ReceiptPhase::Armed => {
                record.phase = ReceiptPhase::Queued(completion);
                ExitPublicationOutcome::Queued
            }
            phase => {
                return Err(ReceiptError::InvalidTransition {
                    actual: phase.status(),
                });
            }
        };
        self.bump_revision();
        Ok(outcome)
    }

    /// Leases the single queued claim to either the pushed or terminal-stdin path.
    pub(crate) fn lease_for_sampling(
        &self,
        receipt_id: ReceiptId,
        owner: &ReceiptOwner,
        source: SamplingSource,
    ) -> Result<SamplingLease, ReceiptError> {
        let mut state = self.lock_state()?;
        let record = match state.active.get_mut(&receipt_id) {
            Some(record) if record.owner != *owner => return Err(ReceiptError::ForeignOwner),
            Some(record) => record,
            None => return Err(state.terminal_error(receipt_id, owner)),
        };

        let lease = match &record.phase {
            ReceiptPhase::Queued(completion) => {
                let completion = *completion;
                let token = Uuid::new_v4();
                record.phase = ReceiptPhase::LeasedToSampling {
                    completion,
                    token,
                    source,
                };
                SamplingLease {
                    receipt_id,
                    owner: owner.clone(),
                    token,
                    source,
                }
            }
            ReceiptPhase::LeasedToSampling { .. } => return Err(ReceiptError::AlreadyLeased),
            phase => {
                return Err(ReceiptError::InvalidTransition {
                    actual: phase.status(),
                });
            }
        };
        self.bump_revision();
        Ok(lease)
    }

    /// A failed sampling attempt requeues the same receipt without another claim.
    pub(crate) fn fail_sampling(&self, lease: &SamplingLease) -> Result<(), ReceiptError> {
        let mut state = self.lock_state()?;
        let record = match state.active.get_mut(&lease.receipt_id) {
            Some(record) if record.owner != lease.owner => return Err(ReceiptError::ForeignOwner),
            Some(record) => record,
            None => return Err(state.terminal_error(lease.receipt_id, &lease.owner)),
        };

        match &record.phase {
            ReceiptPhase::LeasedToSampling {
                completion,
                token,
                source,
            } if *token == lease.token && *source == lease.source => {
                let completion = *completion;
                record.phase = ReceiptPhase::Queued(completion);
            }
            _ => return Err(ReceiptError::StaleLease),
        }
        self.bump_revision();
        Ok(())
    }

    /// Acknowledges the claim only after its contents were included in sampling.
    pub(crate) fn acknowledge_sampled(
        &self,
        lease: &SamplingLease,
    ) -> Result<TerminalCompletion, ReceiptError> {
        let mut state = self.lock_state()?;
        let completion = match state.active.get(&lease.receipt_id) {
            Some(record) if record.owner != lease.owner => return Err(ReceiptError::ForeignOwner),
            Some(record) => match &record.phase {
                ReceiptPhase::LeasedToSampling {
                    completion,
                    token,
                    source,
                } if *token == lease.token && *source == lease.source => *completion,
                _ => return Err(ReceiptError::StaleLease),
            },
            None => return Err(state.terminal_error(lease.receipt_id, &lease.owner)),
        };

        state.retire(
            lease.receipt_id,
            ReceiptPhase::Sampled {
                source: lease.source,
            },
        )?;
        self.bump_revision();
        Ok(completion)
    }

    /// Cancels any reserved, armed, queued, or leased receipt and frees its slot.
    pub(crate) fn cancel(
        &self,
        receipt_id: ReceiptId,
        owner: &ReceiptOwner,
        reason: CancellationReason,
    ) -> Result<(), ReceiptError> {
        let mut state = self.lock_state()?;
        match state.active.get(&receipt_id) {
            Some(record) if record.owner != *owner => return Err(ReceiptError::ForeignOwner),
            Some(_) => {}
            None => {
                return match state.terminal_error(receipt_id, owner) {
                    ReceiptError::UnknownReceipt => Err(ReceiptError::UnknownReceipt),
                    ReceiptError::ForeignOwner => Err(ReceiptError::ForeignOwner),
                    _ => Err(ReceiptError::AlreadyTerminal),
                };
            }
        }

        state.retire(receipt_id, ReceiptPhase::Cancelled(reason))?;
        self.bump_revision();
        Ok(())
    }

    /// Returns the number of receipts currently holding an active slot.
    ///
    /// Terminal outcomes (inline, sampled, cancelled) move to the bounded
    /// terminal history and no longer count here. The unified exec hooks add
    /// sampled receipts with retained output on top to size combined capacity.
    pub(crate) fn active_len(&self) -> Result<usize, ReceiptError> {
        Ok(self.lock_state()?.active.len())
    }

    /// Holds the store lock while running `f`, for contended-read tests.
    #[cfg(test)]
    pub(crate) fn test_with_held_lock<R>(&self, f: impl FnOnce() -> R) -> R {
        let _guard = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f()
    }

    /// Returns the current state for the matching owner, including recent outcomes.
    pub(crate) fn status(
        &self,
        receipt_id: ReceiptId,
        owner: &ReceiptOwner,
    ) -> Result<ReceiptStatus, ReceiptError> {
        let state = self.lock_state()?;
        match state.active.get(&receipt_id) {
            Some(record) if record.owner != *owner => Err(ReceiptError::ForeignOwner),
            Some(record) => Ok(record.phase.status()),
            None => match state.terminal(receipt_id) {
                Some(record) if record.owner != *owner => Err(ReceiptError::ForeignOwner),
                Some(record) => Ok(record.phase.status()),
                None => Err(ReceiptError::UnknownReceipt),
            },
        }
    }
}

#[cfg(test)]
#[path = "completion_receipt_tests.rs"]
mod tests;
