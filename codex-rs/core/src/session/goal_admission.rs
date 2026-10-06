//! Goal background-wait admission recheck for automatic continuations.
//!
//! Invoked from `start_if_idle` after the trigger-mail recheck and before
//! settings preparation. Gates ONLY automatic goal continuation: every other
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

use super::session::Session;
use super::turn_input::TurnStartKind;
use codex_protocol::turn_input::NotSubmittedReason;

/// Rechecks the goal background-wait gate for one idle-start candidate.
///
/// Returns `Some` reason to reject the start, or `None` to allow it through
/// to the remaining admission checks.
pub(crate) fn check_goal_admission(
    session: &Session,
    kind: TurnStartKind,
    turn_trigger: Option<&str>,
) -> Option<NotSubmittedReason> {
    if kind != TurnStartKind::Automatic {
        return None;
    }
    let checker = session
        .services
        .thread_extension_data
        .get::<codex_extension_api::GoalBackgroundWaitAdmission>()?;
    let outcome = super::pending_work::try_read_snapshot(session);
    match checker.check(outcome) {
        codex_extension_api::GoalAdmissionDecision::Allow => None,
        codex_extension_api::GoalAdmissionDecision::Wait => {
            Some(NotSubmittedReason::GoalBackgroundWait)
        }
    }
}
