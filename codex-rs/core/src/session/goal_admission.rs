//! Goal background-wait admission recheck for automatic continuations.
//!
//! Invoked from `start_if_idle` after the trigger-mail recheck and before
//! settings preparation, with a second revision comparison immediately before
//! the turn is started. Gates ONLY automatic goal continuation: every other
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
//! exactly once. The admitted revision is carried to the late recheck before
//! `start_task`, which rejects when a receipt transition landed during the
//! awaited preparation window. A late rejection leaves the consumed ticket
//! consumed (single-use per AC4); the next idle evaluation re-arms from the
//! new revision.

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
    if kind != TurnStartKind::Automatic {
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
