//! Goal-owned durable-sleep lease tests.
//!
//! Drives the production lease helpers (`try_register_goal_wait_sleep`,
//! `remove_goal_wait_sleep`) and the check-in reuse
//! (`evaluate_continuation_with_native_pending`) without model requests.
//! Registration never clobbers foreign sleep, removal only removes the owned
//! id, and empty exec work still waits when native work is pending.

use std::time::Duration;

use codex_extension_api::ExtensionData;
use codex_extension_api::PendingWorkSnapshot;
use codex_extension_items::sleep::SleepItem;
use codex_extension_items::sleep::goal_wait_sleep_id;
use codex_goal_extension::BackgroundWaitEvaluation;
use codex_goal_extension::BackgroundWaitState;
use codex_goal_extension::GoalWaitStatus;
use codex_goal_extension::has_goal_wait_sleep;
use codex_goal_extension::remove_goal_wait_sleep;
use codex_goal_extension::try_register_goal_wait_sleep;
use pretty_assertions::assert_eq;

fn minutes(value: u64) -> Duration {
    Duration::from_secs(value * 60)
}

#[test]
fn register_never_clobbers_foreign_sleep() {
    let store = ExtensionData::new("thread-1");
    let foreign = SleepItem {
        id: "clock-wait-1".to_string(),
        duration_ms: 60_000,
    };
    store.insert(foreign.clone());

    assert!(!try_register_goal_wait_sleep(&store, /*generation*/ 3));
    assert_eq!(store.get::<SleepItem>().as_deref(), Some(&foreign));
    assert!(!has_goal_wait_sleep(&store));
}

#[test]
fn register_refreshes_stale_own_generation() {
    let store = ExtensionData::new("thread-1");

    assert!(try_register_goal_wait_sleep(&store, /*generation*/ 1));
    assert_eq!(
        store.get::<SleepItem>().as_deref(),
        Some(&SleepItem {
            id: goal_wait_sleep_id(1),
            duration_ms: 0,
        })
    );
    assert!(has_goal_wait_sleep(&store));

    assert!(try_register_goal_wait_sleep(&store, /*generation*/ 2));
    assert_eq!(
        store.get::<SleepItem>().as_deref(),
        Some(&SleepItem {
            id: goal_wait_sleep_id(2),
            duration_ms: 0,
        })
    );
}

#[test]
fn conditional_remove_only_removes_owned_id() {
    let store = ExtensionData::new("thread-1");
    assert!(!remove_goal_wait_sleep(&store));

    let foreign = SleepItem {
        id: "clock-wait-1".to_string(),
        duration_ms: 60_000,
    };
    store.insert(foreign.clone());
    assert!(!remove_goal_wait_sleep(&store));
    assert_eq!(store.get::<SleepItem>().as_deref(), Some(&foreign));

    store.insert(SleepItem {
        id: goal_wait_sleep_id(7),
        duration_ms: 0,
    });
    assert!(has_goal_wait_sleep(&store));
    assert!(remove_goal_wait_sleep(&store));
    assert_eq!(store.get::<SleepItem>(), None);
    assert!(!has_goal_wait_sleep(&store));
}

#[test]
fn empty_exec_with_native_pending_waits_and_uses_checkins() {
    let state = BackgroundWaitState::new();
    state.enable();
    let start = minutes(1000);

    let evaluation = state.evaluate_continuation_with_native_pending(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(PendingWorkSnapshot::empty(1)),
        /*native_pending*/ true,
        start,
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::Wait {
            next_check_in: Some(start + minutes(30)),
            emit_warning: false,
        }
    );

    let evaluation = state.evaluate_continuation_with_native_pending(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(PendingWorkSnapshot::empty(1)),
        /*native_pending*/ true,
        start + minutes(30),
    );
    assert!(matches!(
        evaluation,
        BackgroundWaitEvaluation::ProceedWithTicket { .. }
    ));
    assert_eq!(
        state.check_admission(Ok(PendingWorkSnapshot::empty(1))),
        codex_extension_api::GoalAdmissionDecision::Allow
    );
    assert_eq!(state.check_ins_used(), 1);
}

#[test]
fn empty_exec_without_native_pending_proceeds_normally() {
    let state = BackgroundWaitState::new();
    state.enable();

    let evaluation = state.evaluate_continuation_with_native_pending(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(PendingWorkSnapshot::empty(7)),
        /*native_pending*/ false,
        minutes(0),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::ProceedNormal {
            expected_revision: 7
        }
    );
}

#[test]
fn native_pending_exhausts_checkins_then_waits_without_deadline() {
    let state = BackgroundWaitState::new();
    state.enable();
    let start = minutes(0);
    let _ = state.evaluate_continuation_with_native_pending(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(PendingWorkSnapshot::empty(1)),
        /*native_pending*/ true,
        start,
    );
    for at in [30, 60, 120] {
        let evaluation = state.evaluate_continuation_with_native_pending(
            "goal-1",
            GoalWaitStatus::Active,
            Ok(PendingWorkSnapshot::empty(1)),
            /*native_pending*/ true,
            start + minutes(at),
        );
        assert!(matches!(
            evaluation,
            BackgroundWaitEvaluation::ProceedWithTicket { .. }
        ));
        assert_eq!(
            state.check_admission(Ok(PendingWorkSnapshot::empty(1))),
            codex_extension_api::GoalAdmissionDecision::Allow
        );
    }
    assert_eq!(state.check_ins_used(), 3);

    let evaluation = state.evaluate_continuation_with_native_pending(
        "goal-1",
        GoalWaitStatus::Active,
        Ok(PendingWorkSnapshot::empty(1)),
        /*native_pending*/ true,
        start + minutes(240),
    );
    assert_eq!(
        evaluation,
        BackgroundWaitEvaluation::Wait {
            next_check_in: None,
            emit_warning: true,
        }
    );
}
