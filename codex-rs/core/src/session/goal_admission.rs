//! Goal background-wait admission recheck for automatic continuations.
//!
//! Invoked from `start_if_idle` after the trigger-mail recheck and before
//! settings preparation, with a fast-path revision comparison before the turn
//! is started and the authoritative comparison at the linearization point
//! inside `start_task`. Gates ONLY automatic goal continuation: every other
//! turn kind (user, recovery) and every other trigger (including
//! exec-completion and realtime) bypasses without consulting the policy, so
//! user input, follow-up input, and queued or trigger mail are still admitted
//! immediately while a goal waits.
//!
//! When goal published no admission checker the gate is inactive and the
//! continuation proceeds. Otherwise the current E1 snapshot is read and passed
//! to the goal-owned checker, which rechecks pending state and revision
//! against the continuation-time recording. A receipt transition between the
//! continuation check and turn start changes the revision and is rejected; a
//! failed read is never treated as empty.
//!
//! The early check consumes the recorded admission attempt (and any ticket)
//! exactly once. The admitted revision is carried through the late fast-path
//! recheck before `start_task` into `start_task` itself, whose
//! linearization-point comparison (serialized with every receipt-store and
//! mailbox transition under one shared lock set, with no await between
//! comparison and publication) is the only authoritative one. Any rejection
//! leaves the consumed ticket consumed (single-use per AC4); the next idle
//! evaluation re-arms from the new revision. Gated goal starts carry no
//! persistent settings delta, so every rejection path leaves thread settings
//! and notifications untouched.

use super::session::Session;
use super::turn_input::TurnStartKind;
use codex_protocol::turn_input::NotSubmittedReason;

/// Outcome of the early goal admission check.
pub(crate) struct GoalAdmissionOutcome {
    /// `Some` reason to reject the start, or `None` to allow it through to
    /// the remaining admission checks.
    pub(crate) reason: Option<NotSubmittedReason>,
    /// Revision admitted by the early check, for the late recheck before
    /// `start_task`. `None` when the gate did not engage (non-goal trigger,
    /// inactive policy) and no late comparison is needed.
    pub(crate) admitted_revision: Option<u64>,
}

/// Rechecks the goal background-wait gate for one idle-start candidate.
///
/// Returns the rejection reason (if any) and the admitted revision for the
/// late recheck.
pub(crate) fn check_goal_admission(
    session: &Session,
    kind: TurnStartKind,
    turn_trigger: Option<&str>,
) -> GoalAdmissionOutcome {
    if kind != TurnStartKind::Automatic || turn_trigger != Some("goal") {
        return GoalAdmissionOutcome {
            reason: None,
            admitted_revision: None,
        };
    }
    let Some(checker) = session
        .services
        .thread_extension_data
        .get::<codex_extension_api::GoalBackgroundWaitAdmission>()
    else {
        return GoalAdmissionOutcome {
            reason: None,
            admitted_revision: None,
        };
    };
    let outcome = super::pending_work::try_read_snapshot(session);
    let current_revision = outcome
        .as_ref()
        .ok()
        .map(codex_extension_api::PendingWorkSnapshot::revision);
    match checker.check(outcome) {
        codex_extension_api::GoalAdmissionDecision::Allow => GoalAdmissionOutcome {
            reason: None,
            admitted_revision: current_revision,
        },
        codex_extension_api::GoalAdmissionDecision::Wait => GoalAdmissionOutcome {
            reason: Some(NotSubmittedReason::GoalBackgroundWait),
            admitted_revision: None,
        },
    }
}

/// Recompares the pending-work revision immediately before starting.
///
/// Returns `Some` reason to abandon the automatic start when a receipt
/// transition landed after the early admission check, or when the re-read
/// fails (a failed read is never treated as empty). Returns `None` to proceed.
/// Synchronous: safe to call under held locks with no added suspension point.
pub(crate) fn recheck_goal_admission_before_start(
    session: &Session,
    admitted_revision: Option<u64>,
) -> Option<NotSubmittedReason> {
    let expected = admitted_revision?;
    let snapshot = match super::pending_work::try_read_snapshot(session) {
        Ok(snapshot) => snapshot,
        Err(_) => return Some(NotSubmittedReason::GoalBackgroundWait),
    };
    if snapshot.revision() != expected {
        return Some(NotSubmittedReason::GoalBackgroundWait);
    }
    None
}

impl Session {
    /// Publishes the started turn, enforcing the goal revision at the point of
    /// publication.
    ///
    /// LINEARIZATION POINT for automatic goal continuation (AC7): when
    /// `admitted_revision` is `Some`, the current work revision is compared
    /// and `last_started_turn_id` is written while holding ONE shared
    /// serialization: `active_turn` (tokio, held by the `start_task` caller),
    /// `Session.state` (tokio, held here), the receipt-store lock (std), and
    /// the runtime-mailbox lock (tokio `try_lock`, innermost), with NO await
    /// between the comparison and the write. Every revision-bumping transition
    /// (receipt-store reserve/arm/publish/lease/fail/acknowledge/cancel and
    /// mailbox enqueue/lease/acknowledge/fail/suspend/cancel) needs the store
    /// or mailbox lock, so an Arm is totally ordered with publication: ordered
    /// before, it is observed and rejects the automatic start (returns `false`
    /// without publishing; the caller clears its reservation); ordered after,
    /// it is by definition work arriving after the turn started, which is
    /// legitimate (the turn is running and the receipt is delivered later).
    /// Inner locks use `try_lock`: contention or poisoning rejects without
    /// publishing (a failed read is never treated as empty). Earlier
    /// comparisons in `turn_input.rs` are fast-path rejections only; this
    /// check is authoritative. When `admitted_revision` is `None` the gate did
    /// not engage and the turn publishes unconditionally without inner locks.
    pub(crate) async fn publish_goal_turn_if_revision_matches(
        &self,
        turn_id: &str,
        admitted_revision: Option<u64>,
    ) -> bool {
        let mut state = self.state.lock().await;
        let Some(expected) = admitted_revision else {
            state.last_started_turn_id = Some(turn_id.to_string());
            return true;
        };
        // Fetch the test gate before the inner locks so the extension-data
        // lock never nests inside the store lock.
        let gate = self
            .services
            .thread_extension_data
            .get::<crate::codex_thread::TestGoalPublishGate>();
        if self.input_queue.pending_work_revision() != expected {
            return false;
        }
        if let Some(gate) = gate.as_ref() {
            gate.signal_compared();
            let _ = gate.wait_attempting(std::time::Duration::from_secs(5));
            let _ = gate.wait_done(std::time::Duration::from_millis(200));
            gate.record_publish_seq();
        }
        state.last_started_turn_id = Some(turn_id.to_string());
        true
    }
}
