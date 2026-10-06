//! Goal background-wait policy tests with fake time.
//!
//! Drives [`BackgroundWaitState`] directly through its production entry
//! points: [`BackgroundWaitState::evaluate_continuation`] (goal continuation
//! path) and [`BackgroundWaitState::check_admission`] (Core admission
//! recheck via the published checker). No real sleeps; all deadlines use
//! explicit fake `Duration` values.

use std::sync::Arc;
use std::time::Duration;

use codex_extension_api::GoalAdmissionDecision;
use codex_extension_api::PendingWorkReadError;
use codex_extension_api::PendingWorkReceipt;
use codex_extension_api::PendingWorkSnapshot;
use codex_goal_extension::BackgroundWaitEvaluation;
use codex_goal_extension::BackgroundWaitState;
use codex_goal_extension::CHECK_INS_STOPPED_WARNING;
use codex_goal_extension::GoalWaitStatus;
use pretty_assertions::assert_eq;

fn enabled_state() -> Arc<BackgroundWaitState> {
    let state = Arc::new(BackgroundWaitState::new());
    state.enable();
    state
}

fn pending_snapshot(revision: u64) -> PendingWorkSnapshot {
    PendingWorkSnapshot::new(
        vec![PendingWorkReceipt::new("armed-1")],
        Vec::new(),
        Vec::new(),
        revision,
    )
}

fn minutes(value: u64) -> Duration {
    Duration::from_secs(value * 60)
}

#[test]
fn disabled_policy_never_gates() {
    let state = BackgroundWaitState::new();
    assert!(!state.is_enabled());
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(3)),
        minutes(0),
    );
    assert_eq!(evaluation, BackgroundWaitEvaluation::ProceedWithoutGate);
    assert_eq!(state.wait_started_at(), None);
}

#[test]
fn inactive_or_budget_limited_status_never_gates() {
    let state = enabled_state();
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::InactiveOrBudgetLimited,
        Ok(pending_snapshot(3)),
        minutes(0),
    );
    assert_eq!(evaluation, BackgroundWaitEvaluation::ProceedWithoutGate);
    assert_eq!(state.wait_started_at(), None);
}

#[test]
fn empty_snapshot_proceeds_and_admission_allows_same_revision() {
    let state = enabled_state();
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(PendingWorkSnapshot::empty(7)),
        minutes(0),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::ProceedNormal {
            expected_revision: 7
        }
    );
    let decision = state.check_admission(Ok(PendingWorkSnapshot::empty(7)));
    assert_eq!(decision, GoalAdmissionDecision::Allow);
}

#[test]
fn read_failure_never_treated_as_empty() {
    let state = enabled_state();
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Err(PendingWorkReadError::SessionUnavailable),
        minutes(0),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::WaitOnReadFailure {
            error: PendingWorkReadError::SessionUnavailable
        }
    );
    assert_ne!(
        evaluation,
        BackgroundWaitEvaluation::ProceedNormal {
            expected_revision: 0
        }
    );

    let state = enabled_state();
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(PendingWorkSnapshot::empty(4)),
        minutes(0),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::ProceedNormal {
            expected_revision: 4
        }
    );
    let decision = state.check_admission(Err(PendingWorkReadError::MailboxUnavailable {
        reason: "contended".to_string(),
    }));
    assert_eq!(decision, GoalAdmissionDecision::Wait);
}

#[test]
fn check_ins_fire_at_30_60_120_minutes_fake_time() {
    let state = enabled_state();
    let start = minutes(1000);

    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(1)),
        start,
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::Wait {
            next_check_in: Some(start + minutes(30)),
            emit_warning: false,
        }
    );

    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(1)),
        start + minutes(29),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::Wait {
            next_check_in: Some(start + minutes(30)),
            emit_warning: false,
        }
    );

    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(1)),
        start + minutes(30),
    );
    let ticket_id = match evaluation {
        BackgroundWaitEvaluation::ProceedWithTicket {
            expected_revision: 1,
            ticket,
        } => ticket.ticket_id(),
        other => panic!("expected first ticket at 30m, got {other:?}"),
    };
    assert_eq!(
        state.check_admission(Ok(pending_snapshot(1))),
        GoalAdmissionDecision::Allow
    );
    assert_eq!(state.check_ins_used(), 1);

    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(1)),
        start + minutes(30) + Duration::from_secs(1),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::Wait {
            next_check_in: Some(start + minutes(60)),
            emit_warning: false,
        }
    );

    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(1)),
        start + minutes(60),
    );
    let second_id = match evaluation {
        BackgroundWaitEvaluation::ProceedWithTicket { ticket, .. } => ticket.ticket_id(),
        other => panic!("expected second ticket at 60m, got {other:?}"),
    };
    assert_ne!(ticket_id, second_id);
    assert_eq!(
        state.check_admission(Ok(pending_snapshot(1))),
        GoalAdmissionDecision::Allow
    );
    assert_eq!(state.check_ins_used(), 2);

    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(1)),
        start + minutes(119),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::Wait {
            next_check_in: Some(start + minutes(120)),
            emit_warning: false,
        }
    );

    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(1)),
        start + minutes(120),
    );
    assert!(matches!(
        evaluation,
        BackgroundWaitEvaluation::ProceedWithTicket { .. }
    ));
    assert_eq!(
        state.check_admission(Ok(pending_snapshot(1))),
        GoalAdmissionDecision::Allow
    );
    assert_eq!(state.check_ins_used(), 3);
}

#[test]
fn at_most_three_check_ins_per_human_input() {
    let state = enabled_state();
    let start = minutes(0);
    let _ = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(1)),
        start,
    );
    for (index, at) in [30, 60, 120].into_iter().enumerate() {
        let evaluation = state.evaluate_continuation(
            "goal-1",
            GoalWaitStatus::Active,
            Ok(pending_snapshot(1)),
            start + minutes(at),
        );
        assert!(
            matches!(
                evaluation,
                BackgroundWaitEvaluation::ProceedWithTicket { .. }
            ),
            "check-in {index} should issue"
        );
        assert_eq!(
            state.check_admission(Ok(pending_snapshot(1))),
            GoalAdmissionDecision::Allow
        );
    }
    assert_eq!(state.check_ins_used(), 3);

    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(1)),
        start + minutes(240),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::Wait {
            next_check_in: None,
            emit_warning: true,
        }
    );

    state.note_human_input();
    assert_eq!(state.check_ins_used(), 0);
    assert!(!state.warning_emitted());
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(1)),
        start + minutes(240),
    );
    assert!(matches!(
        evaluation,
        BackgroundWaitEvaluation::Wait {
            next_check_in: Some(_),
            emit_warning: false,
        }
    ));
}

#[test]
fn ticket_bypasses_work_gate_exactly_once() {
    let state = enabled_state();
    let start = minutes(0);
    let _ = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(9)),
        start,
    );
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(9)),
        start + minutes(30),
    );
    let ticket_id = match evaluation {
        BackgroundWaitEvaluation::ProceedWithTicket { ticket, .. } => ticket.ticket_id(),
        other => panic!("expected ticket, got {other:?}"),
    };
    assert_eq!(
        state.check_admission(Ok(pending_snapshot(9))),
        GoalAdmissionDecision::Allow
    );

    let _ = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(9)),
        start + minutes(31),
    );
    assert_ne!(
        state.outstanding_ticket().map(|ticket| ticket.ticket_id()),
        Some(ticket_id)
    );
    let decision = state.check_admission(Ok(pending_snapshot(9)));
    assert_eq!(decision, GoalAdmissionDecision::Wait);
    assert_eq!(state.check_ins_used(), 1);
}

#[test]
fn ticket_bypass_still_enforces_revision_and_generation() {
    let state = enabled_state();
    let start = minutes(0);
    let _ = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(5)),
        start,
    );
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(5)),
        start + minutes(30),
    );
    assert!(matches!(
        evaluation,
        BackgroundWaitEvaluation::ProceedWithTicket { .. }
    ));
    let decision = state.check_admission(Ok(pending_snapshot(6)));
    assert_eq!(decision, GoalAdmissionDecision::Wait);
    assert_eq!(state.check_ins_used(), 0);
    assert_eq!(state.outstanding_ticket(), None);

    let state = enabled_state();
    let _ = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(5)),
        start,
    );
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(5)),
        start + minutes(30),
    );
    assert!(matches!(
        evaluation,
        BackgroundWaitEvaluation::ProceedWithTicket { .. }
    ));
    state.note_turn_start();
    let decision = state.check_admission(Ok(pending_snapshot(5)));
    assert_eq!(decision, GoalAdmissionDecision::Wait);
    assert_eq!(state.check_ins_used(), 0);
}

#[test]
fn warning_emitted_once_after_third_check_in_with_exact_text() {
    assert_eq!(
        CHECK_INS_STOPPED_WARNING,
        "Automatic check-ins stopped; waiting for subscribed work or your next message."
    );
    let state = enabled_state();
    let start = minutes(0);
    let _ = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(2)),
        start,
    );
    for at in [30, 60, 120] {
        let _ = state.evaluate_continuation(
            "goal-1",
            GoalWaitStatus::Active,
            Ok(pending_snapshot(2)),
            start + minutes(at),
        );
        assert_eq!(
            state.check_admission(Ok(pending_snapshot(2))),
            GoalAdmissionDecision::Allow
        );
    }
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(2)),
        start + minutes(180),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::Wait {
            next_check_in: None,
            emit_warning: true,
        }
    );
    assert!(state.warning_emitted());
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(2)),
        start + minutes(240),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::Wait {
            next_check_in: None,
            emit_warning: false,
        }
    );
}

#[test]
fn completion_after_cap_wakes_without_counting_as_human_input() {
    let state = enabled_state();
    let start = minutes(0);
    let _ = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(2)),
        start,
    );
    for at in [30, 60, 120] {
        let _ = state.evaluate_continuation(
            "goal-1",
            GoalWaitStatus::Active,
            Ok(pending_snapshot(2)),
            start + minutes(at),
        );
        assert_eq!(
            state.check_admission(Ok(pending_snapshot(2))),
            GoalAdmissionDecision::Allow
        );
    }
    let _ = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(2)),
        start + minutes(180),
    );
    assert!(state.warning_emitted());

    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(PendingWorkSnapshot::empty(8)),
        start + minutes(181),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::ProceedNormal {
            expected_revision: 8
        }
    );
    assert_eq!(state.check_ins_used(), 3);
    assert!(state.warning_emitted());

    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(9)),
        start + minutes(182),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::Wait {
            next_check_in: None,
            emit_warning: false,
        }
    );
}

#[test]
fn revision_change_between_check_and_start_rejects() {
    let state = enabled_state();
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(PendingWorkSnapshot::empty(5)),
        minutes(0),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::ProceedNormal {
            expected_revision: 5
        }
    );
    let decision = state.check_admission(Ok(pending_snapshot(6)));
    assert_eq!(decision, GoalAdmissionDecision::Wait);

    let state = enabled_state();
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(PendingWorkSnapshot::empty(5)),
        minutes(0),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::ProceedNormal {
            expected_revision: 5
        }
    );
    let decision = state.check_admission(Ok(PendingWorkSnapshot::empty(6)));
    assert_eq!(decision, GoalAdmissionDecision::Wait);
}

#[test]
fn admission_without_recorded_attempt_waits() {
    let state = enabled_state();
    let decision = state.check_admission(Ok(PendingWorkSnapshot::empty(1)));
    assert_eq!(decision, GoalAdmissionDecision::Wait);
}

#[test]
fn release_invalidates_ticket_and_reassessment_can_proceed() {
    let state = enabled_state();
    let start = minutes(0);
    let _ = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(5)),
        start,
    );
    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(5)),
        start + minutes(30),
    );
    assert!(matches!(
        evaluation,
        BackgroundWaitEvaluation::ProceedWithTicket { .. }
    ));
    assert!(state.outstanding_ticket().is_some());
    state.note_release();
    assert_eq!(state.outstanding_ticket(), None);
    let decision = state.check_admission(Ok(pending_snapshot(5)));
    assert_eq!(decision, GoalAdmissionDecision::Wait);

    let evaluation = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(PendingWorkSnapshot::empty(7)),
        start + minutes(31),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::ProceedNormal {
            expected_revision: 7
        }
    );
    assert_eq!(
        state.check_admission(Ok(PendingWorkSnapshot::empty(7))),
        GoalAdmissionDecision::Allow
    );
}

#[test]
fn turn_start_and_steering_invalidate_without_renewing() {
    for invalidate in [
        BackgroundWaitState::note_turn_start,
        BackgroundWaitState::note_steering,
        BackgroundWaitState::note_goal_mutation,
        BackgroundWaitState::note_release,
    ] {
        let state = enabled_state();
        let start = minutes(0);
        let _ = state.evaluate_continuation(
            "goal-1",
            GoalWaitStatus::Active,
            Ok(pending_snapshot(5)),
            start,
        );
        let _ = state.evaluate_continuation(
            "goal-1",
            GoalWaitStatus::Active,
            Ok(pending_snapshot(5)),
            start + minutes(30),
        );
        assert!(state.outstanding_ticket().is_some());
        let generation = state.generation();
        invalidate(&state);
        assert_eq!(state.outstanding_ticket(), None);
        assert_eq!(state.generation(), generation + 1);
        assert_eq!(
            state.check_admission(Ok(pending_snapshot(5))),
            GoalAdmissionDecision::Wait
        );
        assert_eq!(state.check_ins_used(), 0);
    }
}

#[test]
fn clear_stop_and_resume_reset_transient_wait_state() {
    for reset in [
        BackgroundWaitState::note_clear,
        BackgroundWaitState::note_stop,
        BackgroundWaitState::note_resume,
    ] {
        let state = enabled_state();
        let start = minutes(0);
        let _ = state.evaluate_continuation(
            "goal-1",
            GoalWaitStatus::Active,
            Ok(pending_snapshot(5)),
            start,
        );
        let _ = state.evaluate_continuation(
            "goal-1",
            GoalWaitStatus::Active,
            Ok(pending_snapshot(5)),
            start + minutes(30),
        );
        let _ = state.check_admission(Ok(pending_snapshot(5)));
        assert_eq!(state.check_ins_used(), 1);
        reset(&state);
        assert_eq!(state.check_ins_used(), 0);
        assert!(!state.warning_emitted());
        assert_eq!(state.wait_started_at(), None);
        assert_eq!(state.outstanding_ticket(), None);
    }
}

#[test]
fn new_goal_identity_starts_fresh_epoch() {
    let state = enabled_state();
    let start = minutes(0);
    let _ = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(5)),
        start,
    );
    let _ = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(5)),
        start + minutes(30),
    );
    let _ = state.check_admission(Ok(pending_snapshot(5)));
    assert_eq!(state.check_ins_used(), 1);
    let evaluation = state.evaluate_continuation(
        "goal-2",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(5)),
        start + minutes(31),
    );
    assert_eq!(state.check_ins_used(), 0);
    assert!(matches!(
        evaluation,
        BackgroundWaitEvaluation::Wait {
            next_check_in: Some(_),
            ..
        }
    ));
}

#[test]
fn evaluations_are_synchronous_and_reentrant_without_holding_state() {
    let state = enabled_state();
    for revision in [1, 2, 3] {
        let evaluation = state.evaluate_continuation(
            "goal-1",
            GoalWaitStatus::Active,
            Ok(PendingWorkSnapshot::empty(revision)),
            minutes(0),
        );
        assert_eq!(
            evaluation,
            BackgroundWaitEvaluation::ProceedNormal {
                expected_revision: revision
            }
        );
        assert_eq!(
            state.check_admission(Ok(PendingWorkSnapshot::empty(revision))),
            GoalAdmissionDecision::Allow
        );
    }
    let first = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(9)),
        minutes(0),
    );
    let second = state.evaluate_continuation(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(pending_snapshot(9)),
        minutes(1),
    );
    assert!(matches!(first, BackgroundWaitEvaluation::Wait { .. }));
    assert!(matches!(second, BackgroundWaitEvaluation::Wait { .. }));
}
