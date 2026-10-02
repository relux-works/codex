use super::*;
use codex_extension_api::GoalActivity;
use codex_extension_api::GoalActivityState;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn tool_finish_reconciles_committed_state_independently_of_tool_name() -> anyhow::Result<()> {
    let db = test_runtime().await?;
    let id = test_thread_id()?;
    seed_thread_metadata(&db, id).await?;
    let h = GoalExtensionHarness::new(db.clone(), id).await?;
    h.start_turn("turn-1", &TokenUsage::default()).await;
    h.notify_tool_finish("turn-1", "forged-success", "create_goal")
        .await;
    assert_eq!(h.thread_store.get::<GoalActivity>(), None);
    db.thread_goals()
        .replace_thread_goal(
            id,
            "work",
            codex_state::ThreadGoalStatus::BudgetLimited,
            /*token_budget*/ None,
        )
        .await?;
    h.notify_tool_finish("turn-1", "other-tool", "shell").await;
    assert_eq!(
        h.thread_store.get::<GoalActivity>().map(|a| a.state),
        Some(GoalActivityState::BudgetLimited)
    );
    db.thread_goals().delete_thread_goal(id).await?;
    h.notify_tool_finish("turn-1", "late-success", "create_goal")
        .await;
    assert_eq!(h.thread_store.get::<GoalActivity>(), None);
    Ok(())
}

#[tokio::test]
async fn disable_and_stop_revoke_activity_and_pending_options() -> anyhow::Result<()> {
    let db = test_runtime().await?;
    let id = test_thread_id()?;
    seed_thread_metadata(&db, id).await?;
    let h = GoalExtensionHarness::new(db.clone(), id).await?;
    db.thread_goals()
        .replace_thread_goal(
            id,
            "work",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    h.resume_thread().await;
    for c in h.registry.config_contributors() {
        c.on_config_changed(&h.session_store, &h.thread_store, &true, &false);
        c.on_config_changed(&h.session_store, &h.thread_store, &false, &true);
    }
    assert_eq!(h.thread_store.get::<GoalActivity>(), None);
    h.start_turn("reenabled", &TokenUsage::default()).await;
    assert_eq!(
        h.thread_store.get::<GoalActivity>().map(|a| a.state),
        Some(GoalActivityState::Active)
    );
    h.thread_store
        .insert(codex_core::TurnStartOptions::default());
    for c in h.registry.config_contributors() {
        c.on_config_changed(&h.session_store, &h.thread_store, &true, &false);
    }
    assert_eq!(h.thread_store.get::<GoalActivity>(), None);
    assert!(
        h.thread_store
            .get::<codex_core::TurnStartOptions>()
            .is_none()
    );
    db.thread_goals().delete_thread_goal(id).await?;
    for c in h.registry.config_contributors() {
        c.on_config_changed(&h.session_store, &h.thread_store, &false, &true);
    }
    h.start_turn("turn-1", &TokenUsage::default()).await;
    assert_eq!(h.thread_store.get::<GoalActivity>(), None);
    db.thread_goals()
        .replace_thread_goal(
            id,
            "new",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    h.resume_thread().await;
    h.thread_store
        .insert(codex_core::TurnStartOptions::default());
    h.stop_thread().await;
    h.notify_tool_finish("turn-1", "late", "create_goal").await;
    assert_eq!(h.thread_store.get::<GoalActivity>(), None);
    assert!(
        h.thread_store
            .get::<codex_core::TurnStartOptions>()
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn read_failure_revokes_activity_and_next_turn_recovers() -> anyhow::Result<()> {
    let db = test_runtime().await?;
    let id = test_thread_id()?;
    seed_thread_metadata(&db, id).await?;
    let h = GoalExtensionHarness::new(db.clone(), id).await?;
    db.thread_goals()
        .replace_thread_goal(
            id,
            "work",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    h.resume_thread().await;
    let previous = h.thread_store.get::<GoalActivity>().expect("active");
    let pool = db
        .sqlite()
        .open_read_write_pool(&db.sqlite().goals_db_path())
        .await?;
    // A malformed committed row exercises the actual GoalStore read failure,
    // without a production test-only switch or replacing the publisher.
    sqlx::query("UPDATE thread_goals SET updated_at_ms = 9223372036854775807 WHERE thread_id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await?;
    assert!(db.thread_goals().get_thread_goal(id).await.is_err());
    h.start_turn("turn-1", &TokenUsage::default()).await;
    assert_eq!(h.thread_store.get::<GoalActivity>(), None);
    sqlx::query("UPDATE thread_goals SET updated_at_ms = 0 WHERE thread_id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await?;
    h.start_turn("turn-2", &TokenUsage::default()).await;
    let recovered = h
        .thread_store
        .get::<GoalActivity>()
        .expect("reconciled again");
    assert_eq!(
        (&recovered.goal_id, recovered.state),
        (&previous.goal_id, previous.state)
    );
    assert!(recovered.revision > previous.revision);
    Ok(())
}

#[tokio::test]
async fn turn_stop_accounting_read_failure_revokes_activity_and_next_turn_recovers()
-> anyhow::Result<()> {
    let db = test_runtime().await?;
    let id = test_thread_id()?;
    seed_thread_metadata(&db, id).await?;
    let h = GoalExtensionHarness::new(db.clone(), id).await?;
    db.thread_goals()
        .replace_thread_goal(
            id,
            "work",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    h.start_turn("turn-1", &TokenUsage::default()).await;
    h.record_token_usage(
        "turn-1",
        &token_usage(
            /*input_tokens*/ 20, /*cached_input_tokens*/ 5, /*output_tokens*/ 8,
            /*reasoning_output_tokens*/ 2, /*total_tokens*/ 30,
        ),
    )
    .await;
    let previous = h.thread_store.get::<GoalActivity>().expect("active");
    let pool = db
        .sqlite()
        .open_read_write_pool(&db.sqlite().goals_db_path())
        .await?;
    // A malformed committed row exercises the actual GoalStore read failure
    // through the turn-stop progress-accounting path.
    sqlx::query("UPDATE thread_goals SET updated_at_ms = 9223372036854775807 WHERE thread_id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await?;
    assert!(db.thread_goals().get_thread_goal(id).await.is_err());
    h.stop_turn("turn-1").await;
    assert_eq!(h.thread_store.get::<GoalActivity>(), None);
    sqlx::query("UPDATE thread_goals SET updated_at_ms = 0 WHERE thread_id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await?;
    h.start_turn("turn-2", &TokenUsage::default()).await;
    let recovered = h
        .thread_store
        .get::<GoalActivity>()
        .expect("reconciled again");
    assert_eq!(
        (&recovered.goal_id, recovered.state),
        (&previous.goal_id, previous.state)
    );
    assert!(recovered.revision > previous.revision);
    Ok(())
}

#[tokio::test]
async fn turn_abort_accounting_read_failure_revokes_activity_and_next_turn_recovers()
-> anyhow::Result<()> {
    let db = test_runtime().await?;
    let id = test_thread_id()?;
    seed_thread_metadata(&db, id).await?;
    let h = GoalExtensionHarness::new(db.clone(), id).await?;
    db.thread_goals()
        .replace_thread_goal(
            id,
            "work",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    h.start_turn("turn-1", &TokenUsage::default()).await;
    h.record_token_usage(
        "turn-1",
        &token_usage(
            /*input_tokens*/ 20, /*cached_input_tokens*/ 5, /*output_tokens*/ 8,
            /*reasoning_output_tokens*/ 2, /*total_tokens*/ 30,
        ),
    )
    .await;
    let previous = h.thread_store.get::<GoalActivity>().expect("active");
    let pool = db
        .sqlite()
        .open_read_write_pool(&db.sqlite().goals_db_path())
        .await?;
    // A malformed committed row exercises the actual GoalStore read failure
    // through the turn-abort progress-accounting path.
    sqlx::query("UPDATE thread_goals SET updated_at_ms = 9223372036854775807 WHERE thread_id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await?;
    assert!(db.thread_goals().get_thread_goal(id).await.is_err());
    h.abort_turn("turn-1").await;
    assert_eq!(h.thread_store.get::<GoalActivity>(), None);
    sqlx::query("UPDATE thread_goals SET updated_at_ms = 0 WHERE thread_id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await?;
    h.start_turn("turn-2", &TokenUsage::default()).await;
    let recovered = h
        .thread_store
        .get::<GoalActivity>()
        .expect("reconciled again");
    assert_eq!(
        (&recovered.goal_id, recovered.state),
        (&previous.goal_id, previous.state)
    );
    assert!(recovered.revision > previous.revision);
    Ok(())
}

#[tokio::test]
async fn automatic_stop_revokes_activity_for_error_and_usage_limit() -> anyhow::Result<()> {
    let db = test_runtime().await?;
    let id = test_thread_id()?;
    seed_thread_metadata(&db, id).await?;
    let h = GoalExtensionHarness::new(db.clone(), id).await?;
    for error in [
        CodexErr::Fatal("test error".to_string()),
        CodexErr::UsageNotIncluded,
    ] {
        db.thread_goals()
            .replace_thread_goal(
                id,
                "work",
                codex_state::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await?;
        h.start_turn("turn-1", &TokenUsage::default()).await;
        assert!(h.thread_store.get::<GoalActivity>().is_some());
        h.notify_turn_error("turn-1", error).await;
        assert_eq!(h.thread_store.get::<GoalActivity>(), None);
    }
    Ok(())
}
