//! Goal publisher integration through the real tools and sampling router.
use anyhow::Result;
use codex_analytics::AnalyticsEventsClient;
use codex_core::config::Config;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::GoalActivity;
use codex_extension_api::ThreadStartInput;
use codex_extension_api::ToolFinishInput;
use codex_extension_api::ToolLifecycleContributor;
use codex_extension_api::ToolLifecycleFuture;
use codex_features::Feature;
use codex_features::SleepToolMode;
use codex_goal_extension::GoalExtensionConfig;
use codex_goal_extension::GoalObjectiveUpdate;
use codex_goal_extension::GoalRuntimeHandle;
use codex_goal_extension::GoalService;
use codex_goal_extension::GoalSetRequest;
use codex_goal_extension::GoalTokenBudgetUpdate;
use codex_goal_extension::install_with_backend;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadGoalStatus;
use core_test_support::responses::{self};
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::sync::Arc;
use std::sync::Weak;
use tempfile::TempDir;
use tokio::sync::Notify;

#[derive(Default)]
struct FinishLatch {
    entered: Notify,
    release: Notify,
}
impl ToolLifecycleContributor for FinishLatch {
    fn on_tool_finish<'a>(&'a self, input: ToolFinishInput<'a>) -> ToolLifecycleFuture<'a> {
        Box::pin(async move {
            if input.tool_name.name == "create_goal" {
                self.entered.notify_one();
                self.release.notified().await;
            }
        })
    }
}

async fn tool_steps(
    server: &wiremock::MockServer,
    steps: &[(&str, &str)],
) -> responses::ResponseMock {
    let scripts = steps
        .iter()
        .enumerate()
        .map(|(index, (name, arguments))| {
            let id = format!("response-{index}");
            let mut events = vec![responses::ev_response_created(&id)];
            if !name.is_empty() {
                events.push(responses::ev_function_call(name, name, arguments));
            }
            events.push(responses::ev_completed_with_tokens(
                &id, /*total_tokens*/ 10,
            ));
            responses::sse(events)
        })
        .collect();
    responses::mount_sse_sequence(server, scripts).await
}

fn install(
    registry: &mut ExtensionRegistryBuilder<Config>,
    db: &Arc<codex_state::StateRuntime>,
    service: &Arc<GoalService>,
    manager: Weak<codex_core::ThreadManager>,
) {
    install_with_backend(
        registry,
        db.clone(),
        AnalyticsEventsClient::disabled(),
        /*metrics_client*/ None,
        manager,
        service.clone(),
        |c: &Config| GoalExtensionConfig {
            enabled: c.features.enabled(Feature::Goals),
            max_goal_token_budget: c.max_goal_token_budget,
        },
    );
}

/// Drives the production Goals enablement hook against the live thread store.
/// `refresh_runtime_config` keeps `Feature::Goals` session-static (only
/// MCP-related flags are copied), so flipping Goals there never notifies
/// config contributors. Instead, build the same production
/// `GoalExtension<Config>` the fixture installed and invoke its
/// `ConfigContributor::on_config_changed` with previous/new configs, exactly
/// as the session would if enablement changed.
fn notify_goals_enabled(
    test: &TestCodex,
    db: &Arc<codex_state::StateRuntime>,
    service: &Arc<GoalService>,
    previous_enabled: bool,
    new_enabled: bool,
) -> Result<()> {
    let mut previous = test.config.clone();
    if previous_enabled {
        previous.features.enable(Feature::Goals)?;
    } else {
        previous.features.disable(Feature::Goals)?;
    }
    let mut new = test.config.clone();
    if new_enabled {
        new.features.enable(Feature::Goals)?;
    } else {
        new.features.disable(Feature::Goals)?;
    }
    let mut registry = ExtensionRegistryBuilder::<Config>::new();
    install(
        &mut registry,
        db,
        service,
        Arc::downgrade(&test.thread_manager),
    );
    let session_store = ExtensionData::new(test.session_configured.session_id.to_string());
    for contributor in registry.build().config_contributors() {
        contributor.on_config_changed(
            &session_store,
            test.codex.thread_extension_data(),
            &previous,
            &new,
        );
    }
    Ok(())
}

async fn fixture(
    server: &wiremock::MockServer,
    latch: Option<Arc<FinishLatch>>,
) -> Result<(TestCodex, Arc<codex_state::StateRuntime>, Arc<GoalService>)> {
    let home = Arc::new(TempDir::new()?);
    let mut config = core_test_support::load_default_config_for_test(&home).await;
    config.features.disable(Feature::CurrentTimeReminder)?;
    config.sleep_tool_mode = SleepToolMode::ModelDriven;
    let db = codex_core::init_state_db(&config)
        .await
        .expect("state database");
    let service = Arc::new(GoalService::new());
    let mut registry = ExtensionRegistryBuilder::<Config>::new();
    if let Some(latch) = latch {
        registry.tool_lifecycle_contributor(latch);
    }
    install(&mut registry, &db, &service, Weak::new());
    let test = test_codex()
        .with_home(home)
        .with_extensions(Arc::new(registry.build()))
        .with_model_info_override("gpt-5.5", |m| {
            m.experimental_supported_tools.retain(|t| t != "clock")
        })
        .with_config(|c| {
            c.features
                .disable(Feature::CurrentTimeReminder)
                .expect("feature");
            c.sleep_tool_mode = SleepToolMode::ModelDriven;
        })
        .build_with_auto_env(server)
        .await?;
    // TestCodex creates its manager after accepting the extension registry.
    // Bind the installed goal runtime to that live manager via the production
    // thread-start hook before driving any tool or turn.
    test.codex
        .thread_extension_data()
        .remove::<GoalRuntimeHandle>();
    let mut bound = ExtensionRegistryBuilder::<Config>::new();
    install(
        &mut bound,
        &db,
        &service,
        Arc::downgrade(&test.thread_manager),
    );
    for c in bound.build().thread_lifecycle_contributors() {
        c.on_thread_start(ThreadStartInput {
            config: &test.config,
            session_source: &SessionSource::Cli,
            persistent_thread_state_available: true,
            environments: &[],
            mcp_resource_client: None,
            extension_metrics: None,
            session_store: &ExtensionData::new(test.session_configured.session_id.to_string()),
            thread_store: test.codex.thread_extension_data(),
        })
        .await;
    }
    Ok((test, db, service))
}

#[test_case::test_case(true, "complete"; "complete")]
#[test_case::test_case(true, "paused"; "paused")]
#[test_case::test_case(true, "blocked"; "blocked")]
#[test_case::test_case(false, "complete"; "refused")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_goal_changes_the_next_sampling_tools(
    success: bool,
    stop_status: &str,
) -> Result<()> {
    let server = responses::start_mock_server().await;
    let create = json!({"objective": if success {"work"} else {""}}).to_string();
    let stop = json!({"status": stop_status}).to_string();
    let mock = tool_steps(
        &server,
        &[("create_goal", &create), ("update_goal", &stop), ("", "")],
    )
    .await;
    let (test, _, _) = fixture(&server, /*latch*/ None).await?;
    test.submit_text_turn("create a goal, then complete it")
        .await?;
    let requests = mock.requests();
    assert_eq!(
        requests
            .iter()
            .map(|r| r.tool_by_name("clock", "sleep").is_some())
            .collect::<Vec<_>>(),
        [false, success, false]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disable_mid_turn_removes_sleep_from_next_request() -> Result<()> {
    let server = responses::start_mock_server().await;
    let mock = tool_steps(
        &server,
        &[("create_goal", r#"{"objective":"work"}"#), ("", "")],
    )
    .await;
    let latch = Arc::new(FinishLatch::default());
    let (test, db, service) = fixture(&server, Some(latch.clone())).await?;
    let (turn, disable) = tokio::join!(test.submit_text_turn("create"), async {
        latch.entered.notified().await;
        assert!(
            test.codex
                .thread_extension_data()
                .get::<GoalActivity>()
                .is_some()
        );
        notify_goals_enabled(
            &test, &db, &service, /*previous_enabled*/ true, /*new_enabled*/ false,
        )?;
        assert_eq!(
            test.codex.thread_extension_data().get::<GoalActivity>(),
            None
        );
        latch.release.notify_one();
        Ok::<_, anyhow::Error>(())
    });
    turn?;
    disable?;
    assert_eq!(
        mock.requests()
            .iter()
            .map(|r| r.tool_by_name("clock", "sleep").is_some())
            .collect::<Vec<_>>(),
        [false, false]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clear_revokes_before_late_create_finish_and_stale_set_effects() -> Result<()> {
    let server = responses::start_mock_server().await;
    let mock = tool_steps(
        &server,
        &[("create_goal", r#"{"objective":"work"}"#), ("", "")],
    )
    .await;
    let latch = Arc::new(FinishLatch::default());
    let (test, db, service) = fixture(&server, Some(latch.clone())).await?;
    let (turn, clear) = tokio::join!(test.submit_text_turn("create"), async {
        latch.entered.notified().await;
        let activity = test
            .codex
            .thread_extension_data()
            .get::<GoalActivity>()
            .expect("create publishes before its finish callback");
        let outcome = service
            .set_thread_goal(
                &db,
                GoalSetRequest {
                    thread_id: test.session_configured.session_id.into(),
                    objective: GoalObjectiveUpdate::Keep,
                    status: Some(ThreadGoalStatus::Active),
                    token_budget: GoalTokenBudgetUpdate::Keep,
                    max_goal_token_budget: None,
                },
            )
            .await?;
        notify_goals_enabled(
            &test, &db, &service, /*previous_enabled*/ true, /*new_enabled*/ false,
        )?;
        // The committed goal is still Active, so absence here proves the
        // disable hook actually ran; a no-op disable would leave Some.
        assert_eq!(
            test.codex.thread_extension_data().get::<GoalActivity>(),
            None
        );
        // Attack unconditional clear independently of disable's revocation.
        test.codex
            .thread_extension_data()
            .insert(activity.as_ref().clone());
        assert!(
            service
                .clear_thread_goal(&db, test.session_configured.session_id.into())
                .await?
        );
        assert_eq!(
            test.codex.thread_extension_data().get::<GoalActivity>(),
            None
        );
        outcome.apply_runtime_effects(&service).await;
        notify_goals_enabled(
            &test, &db, &service, /*previous_enabled*/ false, /*new_enabled*/ true,
        )?;
        outcome.apply_runtime_effects(&service).await;
        assert_eq!(
            test.codex.thread_extension_data().get::<GoalActivity>(),
            None
        );
        latch.release.notify_one();
        Ok::<_, anyhow::Error>(())
    });
    turn?;
    clear?;
    assert_eq!(
        mock.requests()
            .iter()
            .map(|r| r.tool_by_name("clock", "sleep").is_some())
            .collect::<Vec<_>>(),
        [false, false]
    );
    Ok(())
}

#[test_case::test_case(ThreadGoalStatus::Active, true; "active")]
#[test_case::test_case(ThreadGoalStatus::BudgetLimited, true; "budget_limited")]
#[test_case::test_case(ThreadGoalStatus::Paused, false; "paused")]
#[test_case::test_case(ThreadGoalStatus::Blocked, false; "blocked")]
#[test_case::test_case(ThreadGoalStatus::Complete, false; "complete")]
#[test_case::test_case(ThreadGoalStatus::UsageLimited, false; "usage_limited")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_set_and_resume_reconcile_first_request(
    status: ThreadGoalStatus,
    sleep: bool,
) -> Result<()> {
    let server = responses::start_mock_server().await;
    let mock = tool_steps(
        &server,
        &[("update_goal", r#"{"status":"complete"}"#), ("", "")],
    )
    .await;
    let (test, db, service) = fixture(&server, /*latch*/ None).await?;
    let outcome = service
        .set_thread_goal(
            &db,
            GoalSetRequest {
                thread_id: test.session_configured.session_id.into(),
                objective: GoalObjectiveUpdate::Set("work"),
                status: Some(status),
                token_budget: GoalTokenBudgetUpdate::Keep,
                max_goal_token_budget: None,
            },
        )
        .await?;
    core_test_support::submit_thread_settings(
        &test.codex,
        codex_protocol::protocol::ThreadSettingsOverrides {
            collaboration_mode: Some(codex_protocol::config_types::CollaborationMode {
                mode: codex_protocol::config_types::ModeKind::Plan,
                settings: codex_protocol::config_types::Settings {
                    model: "gpt-5.5".to_string(),
                    reasoning_effort: None,
                    developer_instructions: None,
                },
            }),
            ..Default::default()
        },
    )
    .await?;
    test.codex.thread_extension_data().remove::<GoalActivity>();
    outcome.apply_runtime_effects(&service).await;
    assert_eq!(
        test.codex
            .thread_extension_data()
            .get::<GoalActivity>()
            .is_some(),
        sleep
    );
    test.codex.thread_extension_data().remove::<GoalActivity>();
    service
        .restore_thread_runtime_after_resume(test.session_configured.session_id.into())
        .await?;
    assert_eq!(
        test.codex
            .thread_extension_data()
            .get::<GoalActivity>()
            .is_some(),
        sleep
    );
    test.submit_text_turn("inspect goal tools").await?;
    assert_eq!(
        mock.requests()[0].tool_by_name("clock", "sleep").is_some(),
        sleep
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accounting_budget_keeps_sleep_without_automatic_continuation() -> Result<()> {
    let server = responses::start_mock_server().await;
    let mock = tool_steps(
        &server,
        &[
            ("create_goal", r#"{"objective":"work","token_budget":1}"#),
            ("get_goal", "{}"),
            ("", ""),
        ],
    )
    .await;
    let (test, db, _) = fixture(&server, /*latch*/ None).await?;
    test.submit_text_turn("work until the budget limit").await?;
    assert_eq!(
        db.thread_goals()
            .get_thread_goal(test.session_configured.session_id.into())
            .await?
            .expect("goal")
            .status,
        codex_state::ThreadGoalStatus::BudgetLimited
    );
    test.codex
        .emit_thread_idle_lifecycle_if_idle(codex_extension_api::ThreadIdleCause::Completed)
        .await;
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(250),
            core_test_support::wait_for_event(&test.codex, |e| matches!(
                e,
                codex_protocol::protocol::EventMsg::TurnStarted(_)
            ))
        )
        .await
        .is_err(),
        "budget-limited goal must not admit a new turn"
    );
    assert_eq!(
        mock.requests()
            .iter()
            .map(|r| r.tool_by_name("clock", "sleep").is_some())
            .collect::<Vec<_>>(),
        [false, true, true]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_start_before_missing_baseline_and_plan_then_removes_cleared_goal() -> Result<()> {
    use codex_protocol::config_types::CollaborationMode;
    use codex_protocol::config_types::ModeKind;
    use codex_protocol::config_types::Settings;
    let server = responses::start_mock_server().await;
    let mock = tool_steps(&server, &[("", ""), ("", "")]).await;
    let (test, db, service) = fixture(&server, /*latch*/ None).await?;
    let id = test.session_configured.session_id.into();
    db.thread_goals()
        .replace_thread_goal(
            id,
            "work",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    let mode = CollaborationMode {
        mode: ModeKind::Plan,
        settings: Settings {
            model: "gpt-5.5".to_string(),
            reasoning_effort: None,
            developer_instructions: None,
        },
    };
    let mut registry = ExtensionRegistryBuilder::<Config>::new();
    install(
        &mut registry,
        &db,
        &service,
        Arc::downgrade(&test.thread_manager),
    );
    for c in registry.build().turn_lifecycle_contributors() {
        c.on_turn_start(codex_extension_api::TurnStartInput {
            turn_id: "without-baseline",
            collaboration_mode: &mode,
            token_usage_at_turn_start: None,
            session_store: &ExtensionData::new("session"),
            thread_store: test.codex.thread_extension_data(),
            turn_store: &ExtensionData::new("without-baseline"),
        })
        .await;
    }
    assert!(
        test.codex
            .thread_extension_data()
            .get::<GoalActivity>()
            .is_some()
    );
    core_test_support::submit_thread_settings(
        &test.codex,
        codex_protocol::protocol::ThreadSettingsOverrides {
            collaboration_mode: Some(mode),
            ..Default::default()
        },
    )
    .await?;
    test.submit_text_turn("plan").await?;
    db.thread_goals().delete_thread_goal(id).await?;
    test.submit_text_turn("goal was cleared").await?;
    assert_eq!(
        mock.requests()
            .iter()
            .map(|r| r.tool_by_name("clock", "sleep").is_some())
            .collect::<Vec<_>>(),
        [true, false]
    );
    Ok(())
}

/// Drives the production turn-start hook against the live thread store without
/// a model turn, following the direct-contributor pattern of
/// `turn_start_before_missing_baseline_and_plan_then_removes_cleared_goal`.
async fn start_live_turn(
    test: &TestCodex,
    db: &Arc<codex_state::StateRuntime>,
    service: &Arc<GoalService>,
    turn_id: &str,
    mode: codex_protocol::config_types::ModeKind,
) -> Result<()> {
    use codex_protocol::config_types::CollaborationMode;
    use codex_protocol::config_types::Settings;
    let mut registry = ExtensionRegistryBuilder::<Config>::new();
    install(
        &mut registry,
        db,
        service,
        Arc::downgrade(&test.thread_manager),
    );
    let collaboration_mode = CollaborationMode {
        mode,
        settings: Settings {
            model: "gpt-5.5".to_string(),
            reasoning_effort: None,
            developer_instructions: None,
        },
    };
    let usage = codex_protocol::protocol::TokenUsage::default();
    for c in registry.build().turn_lifecycle_contributors() {
        c.on_turn_start(codex_extension_api::TurnStartInput {
            turn_id,
            collaboration_mode: &collaboration_mode,
            token_usage_at_turn_start: Some(&usage),
            session_store: &ExtensionData::new(test.session_configured.session_id.to_string()),
            thread_store: test.codex.thread_extension_data(),
            turn_store: &ExtensionData::new(turn_id),
        })
        .await;
    }
    Ok(())
}

/// Records token usage against the live thread store so the turn carries an
/// accountable progress snapshot.
async fn record_live_usage(
    test: &TestCodex,
    db: &Arc<codex_state::StateRuntime>,
    service: &Arc<GoalService>,
    turn_id: &str,
) -> Result<()> {
    use codex_protocol::protocol::TokenUsage;
    use codex_protocol::protocol::TokenUsageInfo;
    let mut registry = ExtensionRegistryBuilder::<Config>::new();
    install(
        &mut registry,
        db,
        service,
        Arc::downgrade(&test.thread_manager),
    );
    let total = TokenUsage {
        input_tokens: 20,
        cached_input_tokens: 5,
        output_tokens: 8,
        reasoning_output_tokens: 2,
        total_tokens: 30,
        ..TokenUsage::default()
    };
    let info = TokenUsageInfo {
        total_token_usage: total,
        last_token_usage: TokenUsage::default(),
        model_context_window: None,
    };
    for c in registry.build().token_usage_contributors() {
        c.on_token_usage(
            &ExtensionData::new(test.session_configured.session_id.to_string()),
            test.codex.thread_extension_data(),
            &ExtensionData::new(turn_id),
            &info,
        )
        .await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_set_get_failure_revokes_activity_and_next_turn_recovers() -> Result<()> {
    use codex_protocol::config_types::ModeKind;
    let server = responses::start_mock_server().await;
    let (test, db, service) = fixture(&server, /*latch*/ None).await?;
    let id = test.session_configured.session_id.into();
    db.thread_goals()
        .replace_thread_goal(
            id,
            "work",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    // Plan mode reconciles the marker but disables turn accounting, so
    // external-set preparation deterministically short-circuits and the
    // injected failure lands in the set's goal read.
    start_live_turn(&test, &db, &service, "turn-1", ModeKind::Plan).await?;
    let previous = test
        .codex
        .thread_extension_data()
        .get::<GoalActivity>()
        .expect("active marker");
    let pool = db
        .sqlite()
        .open_read_write_pool(&db.sqlite().goals_db_path())
        .await?;
    // A malformed committed row exercises the actual GoalStore read failure
    // through the external-set path.
    sqlx::query("UPDATE thread_goals SET updated_at_ms = 9223372036854775807 WHERE thread_id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await?;
    assert!(db.thread_goals().get_thread_goal(id).await.is_err());
    // Status-only branch (no objective).
    let error = service
        .set_thread_goal(
            &db,
            GoalSetRequest {
                thread_id: id,
                objective: GoalObjectiveUpdate::Keep,
                status: Some(ThreadGoalStatus::Active),
                token_budget: GoalTokenBudgetUpdate::Keep,
                max_goal_token_budget: None,
            },
        )
        .await
        .expect_err("set must report the read failure");
    assert!(
        error.to_string().contains("failed to read thread goal"),
        "unexpected error: {error}"
    );
    assert_eq!(
        test.codex.thread_extension_data().get::<GoalActivity>(),
        None
    );
    sqlx::query("UPDATE thread_goals SET updated_at_ms = 0 WHERE thread_id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await?;
    start_live_turn(&test, &db, &service, "turn-2", ModeKind::Plan).await?;
    let recovered = test
        .codex
        .thread_extension_data()
        .get::<GoalActivity>()
        .expect("reconciled again");
    assert_eq!(recovered.goal_id, previous.goal_id);
    assert_eq!(recovered.state, previous.state);
    assert!(recovered.revision > previous.revision);
    // Objective branch.
    sqlx::query("UPDATE thread_goals SET updated_at_ms = 9223372036854775807 WHERE thread_id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await?;
    service
        .set_thread_goal(
            &db,
            GoalSetRequest {
                thread_id: id,
                objective: GoalObjectiveUpdate::Set("work-2"),
                status: Some(ThreadGoalStatus::Active),
                token_budget: GoalTokenBudgetUpdate::Keep,
                max_goal_token_budget: None,
            },
        )
        .await
        .expect_err("set must report the read failure");
    assert_eq!(
        test.codex.thread_extension_data().get::<GoalActivity>(),
        None
    );
    sqlx::query("UPDATE thread_goals SET updated_at_ms = 0 WHERE thread_id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await?;
    start_live_turn(&test, &db, &service, "turn-3", ModeKind::Plan).await?;
    assert!(
        test.codex
            .thread_extension_data()
            .get::<GoalActivity>()
            .is_some()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_set_prepare_failure_revokes_activity_and_next_turn_recovers() -> Result<()> {
    use codex_protocol::config_types::ModeKind;
    let server = responses::start_mock_server().await;
    let (test, db, service) = fixture(&server, /*latch*/ None).await?;
    let id = test.session_configured.session_id.into();
    db.thread_goals()
        .replace_thread_goal(
            id,
            "work",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    start_live_turn(&test, &db, &service, "turn-1", ModeKind::Default).await?;
    record_live_usage(&test, &db, &service, "turn-1").await?;
    let previous = test
        .codex
        .thread_extension_data()
        .get::<GoalActivity>()
        .expect("active marker");
    let pool = db
        .sqlite()
        .open_read_write_pool(&db.sqlite().goals_db_path())
        .await?;
    // Fail goal-table writes while leaving reads intact, so external-set
    // preparation fails in progress accounting before the set's own read.
    sqlx::query("CREATE TRIGGER fail_goal_updates BEFORE UPDATE ON thread_goals BEGIN SELECT RAISE(ABORT, 'injected goal write failure'); END")
        .execute(&pool)
        .await?;
    let error = service
        .set_thread_goal(
            &db,
            GoalSetRequest {
                thread_id: id,
                objective: GoalObjectiveUpdate::Keep,
                status: Some(ThreadGoalStatus::Active),
                token_budget: GoalTokenBudgetUpdate::Keep,
                max_goal_token_budget: None,
            },
        )
        .await
        .expect_err("set must report the storage failure");
    assert!(
        error.to_string().contains("failed to update thread goal"),
        "unexpected error: {error}"
    );
    assert_eq!(
        test.codex.thread_extension_data().get::<GoalActivity>(),
        None
    );
    sqlx::query("DROP TRIGGER fail_goal_updates")
        .execute(&pool)
        .await?;
    start_live_turn(&test, &db, &service, "turn-2", ModeKind::Default).await?;
    let recovered = test
        .codex
        .thread_extension_data()
        .get::<GoalActivity>()
        .expect("reconciled again");
    assert_eq!(recovered.goal_id, previous.goal_id);
    assert_eq!(recovered.state, previous.state);
    assert!(recovered.revision > previous.revision);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_flush_read_failure_revokes_activity_and_next_turn_recovers() -> Result<()> {
    use codex_protocol::config_types::ModeKind;
    let server = responses::start_mock_server().await;
    let (test, db, service) = fixture(&server, /*latch*/ None).await?;
    let id = test.session_configured.session_id.into();
    db.thread_goals()
        .replace_thread_goal(
            id,
            "work",
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    start_live_turn(&test, &db, &service, "turn-1", ModeKind::Default).await?;
    record_live_usage(&test, &db, &service, "turn-1").await?;
    let previous = test
        .codex
        .thread_extension_data()
        .get::<GoalActivity>()
        .expect("active marker");
    let pool = db
        .sqlite()
        .open_read_write_pool(&db.sqlite().goals_db_path())
        .await?;
    // A malformed committed row exercises the actual GoalStore read failure
    // through the fork-flush preparation path.
    sqlx::query("UPDATE thread_goals SET updated_at_ms = 9223372036854775807 WHERE thread_id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await?;
    assert!(db.thread_goals().get_thread_goal(id).await.is_err());
    service
        .flush_thread_goal_progress_for_fork(id)
        .await
        .expect_err("flush must report the read failure");
    assert_eq!(
        test.codex.thread_extension_data().get::<GoalActivity>(),
        None
    );
    sqlx::query("UPDATE thread_goals SET updated_at_ms = 0 WHERE thread_id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await?;
    start_live_turn(&test, &db, &service, "turn-2", ModeKind::Default).await?;
    let recovered = test
        .codex
        .thread_extension_data()
        .get::<GoalActivity>()
        .expect("reconciled again");
    assert_eq!(recovered.goal_id, previous.goal_id);
    assert_eq!(recovered.state, previous.state);
    assert!(recovered.revision > previous.revision);
    Ok(())
}
