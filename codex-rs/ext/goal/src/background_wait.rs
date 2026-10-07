//! Goal-owned background waiting policy, inactive by default.
//!
//! Gates ONLY automatic goal continuation on known pending exec-completion
//! work. User input, follow-up input, and queued or trigger mail never consult
//! this module; trigger-mail priority stays authoritative and no global idle
//! suppression exists here.
//!
//! The goal continuation path calls
//! [`BackgroundWaitState::evaluate_continuation`] with the E1 snapshot. When
//! known pending work exists and no check-in is due, evaluation returns
//! [`BackgroundWaitEvaluation::Wait`] and the caller drops the goal semaphore
//! before returning without starting a turn. Never hold the semaphore during
//! a delay; this module is synchronous and holds its own lock only briefly.
//!
//! Fallback check-ins fire at absolute offsets of 30, 60, and 120 minutes
//! from the check-in epoch, at most three per human input. Each ticket
//! bypasses only this work gate, exactly once. Status, Plan, shutdown,
//! capacity, input, and newer-turn checks still apply in Core. After the third
//! check-in the caller emits [`CHECK_INS_STOPPED_WARNING`] exactly once; the
//! goal stays active and event-wakeable, never paused, blocked, or complete.
//!
//! The check-in epoch is anchored at the last human input, where the allowance
//! renews. Admitted check-in turns and other automatic turns never move it;
//! only human input, goal mutation, clear, stop, and release reset the epoch
//! and the used count together. Turn start and steering invalidate tickets
//! without renewing the epoch.
//!
//! Every `Wait` evaluation with a deadline registers that deadline as the
//! pending scheduled re-entry. The runtime spawns one abortable timer per
//! registration; when it fires, the timer claims the registration through
//! [`BackgroundWaitState::claim_due_deadline`] and re-enters the normal goal
//! continuation path. Every invalidation cancels the registration (and aborts
//! the timer through the hook installed by the runtime); a newer deadline
//! replaces it. A completion (observed as empty pending work on reassessment)
//! pre-empts timers without counting as human input. Releasing a mis-armed
//! subscription lets a later reassessment proceed without killing the process.
//!
//! A failed snapshot read is never treated as empty: evaluation returns
//! [`BackgroundWaitEvaluation::WaitOnReadFailure`] and no automatic
//! continuation proceeds past the gate. Callers use fake `now` values in
//! tests, never real sleeps.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use codex_extension_api::GoalAdmissionDecision;
use codex_extension_api::GoalBackgroundWaitAdmission;
use codex_extension_api::PendingWorkReadError;
use codex_extension_api::PendingWorkSnapshot;

/// Absolute check-in deadlines from the current wait start.
pub const CHECK_IN_DELAYS: [Duration; 3] = [
    Duration::from_secs(30 * 60),
    Duration::from_secs(60 * 60),
    Duration::from_secs(120 * 60),
];

/// Maximum check-ins per human input epoch.
pub const MAX_CHECK_INS_PER_HUMAN_INPUT: u8 = 3;

/// Warning emitted exactly once after the third check-in is exhausted.
pub const CHECK_INS_STOPPED_WARNING: &str =
    "Automatic check-ins stopped; waiting for subscribed work or your next message.";

/// Whether the goal is eligible for background waiting.
///
/// Only an active goal engages the gate. Inactive and budget-limited goals
/// proceed without recording, as do unopted or server-only processes, which
/// the E1 snapshot already reports as empty.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GoalWaitStatus {
    /// The goal can wait on subscribed work.
    Active,
    /// Inactive or budget-limited: the gate does not engage.
    InactiveOrBudgetLimited,
}

/// Evaluation of one goal continuation attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackgroundWaitEvaluation {
    /// Gate not engaged: disabled or ineligible status. No recording.
    ProceedWithoutGate,
    /// No pending work: proceed, recording the revision for the admission
    /// recheck that catches receipt transitions before turn start.
    ProceedNormal { expected_revision: u64 },
    /// Pending work exists but a check-in is due: proceed once with a
    /// single-use ticket that bypasses only this work gate.
    ProceedWithTicket {
        expected_revision: u64,
        ticket: CheckInTicket,
    },
    /// Known pending work and no check-in due: wait for events. The caller
    /// drops the goal semaphore and returns without starting. `next_check_in`
    /// is the absolute deadline when allowance remains, or `None` once the
    /// cap is exhausted. `emit_warning` is true exactly once, on the first
    /// blocked attempt after the third check-in.
    Wait {
        next_check_in: Option<Duration>,
        emit_warning: bool,
    },
    /// Snapshot read failed: safe behavior is to wait, never to treat the
    /// failure as empty. No automatic continuation proceeds past the gate.
    WaitOnReadFailure { error: PendingWorkReadError },
}

/// Single-use check-in ticket bypassing only the work gate, exactly once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckInTicket {
    ticket_id: u64,
    generation: u64,
    issued_at: Duration,
}

impl CheckInTicket {
    /// Unique ticket identity within this state.
    pub fn ticket_id(&self) -> u64 {
        self.ticket_id
    }

    /// State generation at issuance; invalidated generations reject.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Fake-time instant of issuance.
    pub fn issued_at(&self) -> Duration {
        self.issued_at
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AdmissionAttempt {
    goal_id: String,
    expected_revision: u64,
    expected_generation: u64,
    ticket: Option<CheckInTicket>,
}

/// Registered scheduled re-entry for one fallback check-in deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ArmedDeadline {
    deadline: Duration,
    generation: u64,
}

/// Synchronous hook fired on every invalidation so the runtime can abort its
/// pending check-in timer. Opaque in `Debug`, like the admission closure.
#[derive(Clone)]
struct InvalidationHook(Arc<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for InvalidationHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("InvalidationHook(<opaque>)")
    }
}

#[derive(Debug)]
struct WaitState {
    enabled: bool,
    active_goal_id: Option<String>,
    generation: u64,
    check_ins_used: u8,
    wait_started_at: Option<Duration>,
    warning_emitted: bool,
    outstanding_ticket: Option<CheckInTicket>,
    last_consumed_ticket_id: Option<u64>,
    next_ticket_id: u64,
    admission_attempt: Option<AdmissionAttempt>,
    pending_deadline: Option<ArmedDeadline>,
    invalidation_hook: Option<InvalidationHook>,
}

impl WaitState {
    fn new() -> Self {
        Self {
            enabled: false,
            active_goal_id: None,
            generation: 0,
            check_ins_used: 0,
            wait_started_at: None,
            warning_emitted: false,
            outstanding_ticket: None,
            last_consumed_ticket_id: None,
            next_ticket_id: 1,
            admission_attempt: None,
            pending_deadline: None,
            invalidation_hook: None,
        }
    }

    fn invalidate_tickets(&mut self) {
        self.generation += 1;
        self.outstanding_ticket = None;
        self.admission_attempt = None;
        self.pending_deadline = None;
        self.fire_invalidation_hook();
    }

    fn fire_invalidation_hook(&self) {
        if let Some(hook) = &self.invalidation_hook {
            hook.0();
        }
    }
}

/// Goal-owned background waiting policy.
///
/// Synchronous and lock-brief: no method awaits, so evaluation can never hold
/// the goal semaphore across a delay. Disabled by default until activation.
#[derive(Debug)]
pub struct BackgroundWaitState {
    inner: Mutex<WaitState>,
}

impl BackgroundWaitState {
    /// Creates disabled policy state.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(WaitState::new()),
        }
    }

    /// Enables the policy for subsequent evaluations.
    pub fn enable(&self) {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .enabled = true;
    }

    /// Disables the policy and resets transient wait state for a fresh start.
    pub fn disable(&self) {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        state.enabled = false;
        state.active_goal_id = None;
        state.check_ins_used = 0;
        state.wait_started_at = None;
        state.warning_emitted = false;
        state.outstanding_ticket = None;
        state.last_consumed_ticket_id = None;
        state.admission_attempt = None;
        state.pending_deadline = None;
        state.fire_invalidation_hook();
    }

    /// Reports whether the policy engages.
    pub fn is_enabled(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .enabled
    }

    /// Builds the Core admission checker capturing this state.
    pub fn admission_checker(self: &std::sync::Arc<Self>) -> GoalBackgroundWaitAdmission {
        let state = std::sync::Arc::clone(self);
        GoalBackgroundWaitAdmission::new(move |outcome| state.check_admission(outcome))
    }

    /// Evaluates one goal continuation attempt at fake-time `now`.
    ///
    /// Records the admission attempt for [`BackgroundWaitState::check_admission`]
    /// on every `Proceed` variant. On `Wait` variants the caller drops the
    /// goal semaphore and returns without starting.
    ///
    /// A `Wait` with a deadline registers it as the pending scheduled
    /// re-entry, replacing any older registration; the runtime spawns the
    /// matching abortable timer. Every other outcome consumes or clears the
    /// registration, except a read failure, which keeps a previously armed
    /// deadline so its already-spawned timer still re-enters.
    pub fn evaluate_continuation(
        &self,
        goal_id: &str,
        status: GoalWaitStatus,
        snapshot: Result<PendingWorkSnapshot, PendingWorkReadError>,
        now: Duration,
    ) -> BackgroundWaitEvaluation {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        self.evaluate_locked(&mut state, goal_id, status, snapshot, now)
    }

    fn evaluate_locked(
        &self,
        state: &mut WaitState,
        goal_id: &str,
        status: GoalWaitStatus,
        snapshot: Result<PendingWorkSnapshot, PendingWorkReadError>,
        now: Duration,
    ) -> BackgroundWaitEvaluation {
        if !state.enabled || status == GoalWaitStatus::InactiveOrBudgetLimited {
            if status == GoalWaitStatus::InactiveOrBudgetLimited {
                state.wait_started_at = None;
            }
            state.pending_deadline = None;
            return BackgroundWaitEvaluation::ProceedWithoutGate;
        }
        if state.active_goal_id.as_deref() != Some(goal_id) {
            state.active_goal_id = Some(goal_id.to_string());
            state.check_ins_used = 0;
            state.wait_started_at = None;
            state.warning_emitted = false;
            state.outstanding_ticket = None;
            state.last_consumed_ticket_id = None;
            state.admission_attempt = None;
        }
        let snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return BackgroundWaitEvaluation::WaitOnReadFailure { error };
            }
        };
        if snapshot.is_empty() {
            state.wait_started_at = None;
            state.pending_deadline = None;
            state.admission_attempt = Some(AdmissionAttempt {
                goal_id: goal_id.to_string(),
                expected_revision: snapshot.revision(),
                expected_generation: state.generation,
                ticket: None,
            });
            return BackgroundWaitEvaluation::ProceedNormal {
                expected_revision: snapshot.revision(),
            };
        }
        if state.check_ins_used >= MAX_CHECK_INS_PER_HUMAN_INPUT {
            let emit_warning = !state.warning_emitted;
            state.warning_emitted = true;
            state.pending_deadline = None;
            return BackgroundWaitEvaluation::Wait {
                next_check_in: None,
                emit_warning,
            };
        }
        let wait_started_at = *state.wait_started_at.get_or_insert(now);
        let delay = CHECK_IN_DELAYS
            .get(usize::from(state.check_ins_used))
            .copied()
            .unwrap_or(Duration::MAX);
        let deadline = wait_started_at.checked_add(delay).unwrap_or(Duration::MAX);
        if now < deadline {
            state.pending_deadline = Some(ArmedDeadline {
                deadline,
                generation: state.generation,
            });
            return BackgroundWaitEvaluation::Wait {
                next_check_in: Some(deadline),
                emit_warning: false,
            };
        }
        state.pending_deadline = None;
        let ticket = CheckInTicket {
            ticket_id: state.next_ticket_id,
            generation: state.generation,
            issued_at: now,
        };
        state.outstanding_ticket = Some(ticket.clone());
        state.admission_attempt = Some(AdmissionAttempt {
            goal_id: goal_id.to_string(),
            expected_revision: snapshot.revision(),
            expected_generation: state.generation,
            ticket: Some(ticket.clone()),
        });
        BackgroundWaitEvaluation::ProceedWithTicket {
            expected_revision: snapshot.revision(),
            ticket,
        }
    }

    /// Claims the registered check-in deadline for one fired timer.
    ///
    /// Production timers call this after sleeping: on `true` the registration
    /// is consumed and the caller re-enters goal continuation; on `false` the
    /// timer is stale (superseded, invalidated, disabled, or early) and the
    /// caller returns without doing anything. Consuming the claim never starts
    /// a turn by itself; the re-entry evaluates and admits normally.
    pub fn claim_due_deadline(&self, deadline: Duration, generation: u64, now: Duration) -> bool {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.enabled {
            return false;
        }
        let Some(armed) = state.pending_deadline else {
            return false;
        };
        if armed.deadline != deadline
            || armed.generation != generation
            || armed.generation != state.generation
        {
            return false;
        }
        if now < armed.deadline {
            return false;
        }
        state.pending_deadline = None;
        true
    }

    /// Installs the hook fired on every invalidation.
    ///
    /// The runtime installs its timer abort here so steering, turn start, goal
    /// mutation, clear, stop, release, human input, and resume all cancel the
    /// pending scheduled re-entry without further call-site wiring.
    pub fn set_invalidation_hook(&self, hook: impl Fn() + Send + Sync + 'static) {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .invalidation_hook = Some(InvalidationHook(Arc::new(hook)));
    }

    /// Rechecks pending state and revision for one recorded attempt.
    ///
    /// Consumes the recorded attempt exactly once. Returns `Wait` when no
    /// attempt was recorded, when the generation or revision changed, when a
    /// ticket is missing, stale, or reused, and on every read failure.
    pub fn check_admission(
        &self,
        outcome: Result<PendingWorkSnapshot, PendingWorkReadError>,
    ) -> GoalAdmissionDecision {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(attempt) = state.admission_attempt.take() else {
            return GoalAdmissionDecision::Wait;
        };
        if attempt.expected_generation != state.generation {
            return GoalAdmissionDecision::Wait;
        }
        let snapshot = match outcome {
            Ok(snapshot) => snapshot,
            Err(_) => return GoalAdmissionDecision::Wait,
        };
        match attempt.ticket {
            Some(ticket) => {
                let outstanding_matches = state
                    .outstanding_ticket
                    .as_ref()
                    .is_some_and(|outstanding| outstanding.ticket_id == ticket.ticket_id)
                    && ticket.generation == state.generation
                    && state.last_consumed_ticket_id != Some(ticket.ticket_id);
                if !outstanding_matches {
                    return GoalAdmissionDecision::Wait;
                }
                if snapshot.revision() != attempt.expected_revision {
                    state.outstanding_ticket = None;
                    return GoalAdmissionDecision::Wait;
                }
                if state.check_ins_used >= MAX_CHECK_INS_PER_HUMAN_INPUT {
                    return GoalAdmissionDecision::Wait;
                }
                state.last_consumed_ticket_id = Some(ticket.ticket_id);
                state.outstanding_ticket = None;
                state.check_ins_used += 1;
                GoalAdmissionDecision::Allow
            }
            None => {
                if snapshot.revision() != attempt.expected_revision {
                    return GoalAdmissionDecision::Wait;
                }
                if !snapshot.is_empty() {
                    return GoalAdmissionDecision::Wait;
                }
                GoalAdmissionDecision::Allow
            }
        }
    }

    /// Renews the check-in allowance on human input.
    pub fn note_human_input(&self) {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        state.invalidate_tickets();
        state.check_ins_used = 0;
        state.wait_started_at = None;
        state.warning_emitted = false;
    }

    /// Invalidates tickets on turn start without renewing the allowance.
    ///
    /// The check-in epoch is anchored at the last human input: admitted
    /// check-in turns and other automatic turns never move it.
    pub fn note_turn_start(&self) {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .invalidate_tickets();
    }

    /// Invalidates tickets on steering.
    pub fn note_steering(&self) {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .invalidate_tickets();
    }

    /// Resets the check-in epoch and allowance on goal mutation.
    pub fn note_goal_mutation(&self) {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        state.invalidate_tickets();
        state.check_ins_used = 0;
        state.wait_started_at = None;
        state.warning_emitted = false;
    }

    /// Clears wait state on goal clear for a fresh next goal.
    pub fn note_clear(&self) {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        state.invalidate_tickets();
        state.active_goal_id = None;
        state.check_ins_used = 0;
        state.wait_started_at = None;
        state.warning_emitted = false;
        state.last_consumed_ticket_id = None;
    }

    /// Clears wait state on thread stop.
    pub fn note_stop(&self) {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        state.invalidate_tickets();
        state.active_goal_id = None;
        state.check_ins_used = 0;
        state.wait_started_at = None;
        state.warning_emitted = false;
        state.last_consumed_ticket_id = None;
    }

    /// Resets the check-in epoch and allowance on subscription release.
    pub fn note_release(&self) {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        state.invalidate_tickets();
        state.check_ins_used = 0;
        state.wait_started_at = None;
        state.warning_emitted = false;
    }

    /// Discards stale waits on resume; live-generation-only state restarts.
    pub fn note_resume(&self) {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        state.invalidate_tickets();
        state.active_goal_id = None;
        state.check_ins_used = 0;
        state.wait_started_at = None;
        state.warning_emitted = false;
        state.last_consumed_ticket_id = None;
    }

    /// Check-ins consumed in the current human-input epoch.
    pub fn check_ins_used(&self) -> u8 {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .check_ins_used
    }

    /// Reports whether the stopped-check-ins warning was emitted.
    pub fn warning_emitted(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .warning_emitted
    }

    /// Start of the current wait episode, if waiting.
    pub fn wait_started_at(&self) -> Option<Duration> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .wait_started_at
    }

    /// Registered scheduled re-entry as `(deadline, generation)`, if armed.
    pub fn armed_deadline(&self) -> Option<(Duration, u64)> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pending_deadline
            .map(|armed| (armed.deadline, armed.generation))
    }

    /// Currently outstanding ticket, if one was issued and not yet consumed.
    pub fn outstanding_ticket(&self) -> Option<CheckInTicket> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .outstanding_ticket
            .clone()
    }

    /// Current invalidation generation.
    pub fn generation(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .generation
    }
}

impl Default for BackgroundWaitState {
    fn default() -> Self {
        Self::new()
    }
}
