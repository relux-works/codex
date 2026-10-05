#![allow(dead_code)]
#![allow(clippy::expect_used)]

#[path = "../src/activity.rs"]
mod activity;

use activity::GoalActivityPublisher;
use codex_extension_api::ExtensionData;
use codex_extension_api::GoalActivity;
use codex_extension_api::GoalActivityState;
use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;

#[test]
fn publisher_refuses_stale_revision_and_recovers_unknown_state() {
    let store = ExtensionData::new("thread");
    let publisher = GoalActivityPublisher::new(/*enabled*/ true);
    let goal = codex_state::ThreadGoal {
        thread_id: ThreadId::new(),
        goal_id: "live-goal".to_string(),
        objective: "work".to_string(),
        status: codex_state::ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    publisher
        .publish(&store, publisher.revision(), Ok(Some(goal.clone())))
        .expect("publish");
    let observed = publisher.revision();
    let expected = GoalActivity {
        goal_id: goal.goal_id.clone(),
        revision: observed,
        state: GoalActivityState::Active,
    };
    assert_eq!(store.get::<GoalActivity>().as_deref(), Some(&expected));
    // Duplicate delivery of the same committed snapshot preserves its revision.
    publisher
        .publish(&store, observed, Ok(Some(goal.clone())))
        .expect("duplicate");
    assert_eq!(publisher.revision(), observed);
    publisher.clear(&store);
    assert!(
        publisher
            .publish(&store, observed, Ok(Some(goal.clone())))
            .is_err()
    );
    assert_eq!(store.get::<GoalActivity>(), None);
    publisher
        .publish(&store, publisher.revision(), Ok(Some(goal.clone())))
        .expect("new read");
    let before_failure = publisher.revision();
    assert!(
        publisher
            .publish(&store, before_failure, Err("store read failed".to_string()))
            .is_err()
    );
    assert_eq!(store.get::<GoalActivity>(), None);
    assert!(
        publisher
            .publish(&store, before_failure, Ok(Some(goal.clone())))
            .is_err()
    );
    publisher
        .publish(&store, publisher.revision(), Ok(Some(goal.clone())))
        .expect("recover");
    let before_disable = publisher.revision();
    publisher.set_enabled(/*enabled*/ false, &store);
    publisher.set_enabled(/*enabled*/ true, &store);
    assert!(
        publisher
            .publish(&store, before_disable, Ok(Some(goal.clone())))
            .is_err()
    );
    assert_eq!(store.get::<GoalActivity>(), None);
    publisher
        .publish(&store, publisher.revision(), Ok(Some(goal.clone())))
        .expect("reenable from new read");
    publisher.stop(&store);
    assert!(
        publisher
            .publish(&store, publisher.revision(), Ok(Some(goal)))
            .is_err()
    );
    assert_eq!(store.get::<GoalActivity>(), None);
}
