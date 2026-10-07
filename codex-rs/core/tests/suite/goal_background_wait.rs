//! Goal background-wait scheduled check-ins through the production runtime.
//!
//! Builds a real session with the goal extension and the background-wait
//! policy enabled, holds one stalled Armed receipt, and advances paused Tokio
//! time alone. The production `CheckInTimer` -> `GoalRuntimeHandle`
//! (`runtime.rs` Wait arm, `continue_if_idle` -> Core `start_turn_if_idle`)
//! re-entry starts real turns marked automatic at 30/60/120 minutes, then the
//! production warning fires once. No manual per-check-in
//! evaluate/claim/admit calls; no real sleeps.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use std::time::Duration;

use anyhow::Result;
use codex_analytics::AnalyticsEventsClient;
use codex_core::TurnInputRequest;
use codex_core::config::Config;
use codex_extension_api::AsyncNotificationSupport;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionEventSink;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::ExtensionWarning;
use codex_extension_api::ThreadIdleCause;
use codex_extension_api::ThreadStartInput;
use codex_features::Feature;
use codex_goal_extension::CHECK_INS_STOPPED_WARNING;
use codex_goal_extension::CheckInClock;
use codex_goal_extension::GoalExtensionConfig;
use codex_goal_extension::GoalObjectiveUpdate;
use codex_goal_extension::GoalRuntimeHandle;
use codex_goal_extension::GoalService;
use codex_goal_extension::GoalSetRequest;
use codex_goal_extension::GoalTokenBudgetUpdate;
use codex_goal_extension::install_with_backend_and_clock;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

struct PausedClock {
    base: Duration,
    start: tokio::time::Instant,
}

impl PausedClock {
    fn new(base: Duration) -> Self {
        Self {
            base,
            start: tokio::time::Instant::now(),
        }
    }
}

impl CheckInClock for PausedClock {
    fn now(&self) -> Duration {
        self.base + self.start.elapsed()
    }

    fn sleep_until(&self, deadline: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let delay = deadline.saturating_sub(self.now());
        Box::pin(tokio::time::sleep(delay))
    }
}

#[derive(Default)]
struct RecordingWarningSink {
    warnings: Mutex<Vec<String>>,
}

impl RecordingWarningSink {
    fn warnings(&self) -> Vec<String> {
        self.warnings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl ExtensionEventSink for RecordingWarningSink {
    fn emit(&self, _event: Event) {}

    fn emit_warning(&self, warning: ExtensionWarning) {
        self.warnings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(warning.message);
    }
}

fn install(
    registry: &mut ExtensionRegistryBuilder<Config>,
    db: &Arc<codex_state::StateRuntime>,
    service: &Arc<GoalService>,
    manager: Weak<codex_core::ThreadManager>,
    clock: Arc<dyn CheckInClock>,
) {
    install_with_backend_and_clock(
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
        clock,
    );
}

async fn fixture(
    server: &wiremock::MockServer,
    clock: Arc<dyn CheckInClock>,
    sink: Arc<RecordingWarningSink>,
    support: Option<AsyncNotificationSupport>,
) -> Result<(TestCodex, Arc<codex_state::StateRuntime>, Arc<GoalService>)> {
    let home = Arc::new(TempDir::new()?);
    let mut config = core_test_support::load_default_config_for_test(&home).await;
    config.features.disable(Feature::CurrentTimeReminder)?;
    let db = codex_core::init_state_db(&config)
        .await
        .expect("state database");
    let service = Arc::new(GoalService::new());
    let mut registry = ExtensionRegistryBuilder::<Config>::with_event_sink(sink.clone());
    install(
        &mut registry,
        &db,
        &service,
        Weak::new(),
        Arc::clone(&clock),
    );
    let test = test_codex()
        .with_home(home)
        .with_extensions(Arc::new(registry.build()))
        .with_config(|c| {
            c.features
                .disable(Feature::CurrentTimeReminder)
                .expect("feature");
        })
        .build_with_auto_env(server)
        .await?;
    // TestCodex creates its manager after accepting the extension registry.
    // Bind the installed goal runtime to that live manager via the production
    // thread-start hook before driving any turn. The reinstalled runtime keeps
    // the same paused clock and recording sink.
    test.codex
        .thread_extension_data()
        .remove::<GoalRuntimeHandle>();
    if let Some(support) = support {
        test.codex.thread_extension_data().insert(support);
    }
    let mut bound = ExtensionRegistryBuilder::<Config>::with_event_sink(sink);
    install(
        &mut bound,
        &db,
        &service,
        Arc::downgrade(&test.thread_manager),
        clock,
    );
    for contributor in bound.build().thread_lifecycle_contributors() {
        contributor
            .on_thread_start(ThreadStartInput {
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

/// Scheduled check-ins fire through the production timer/runtime/Core path as
/// paused time advances alone. Production entry points:
/// `CodexThread::emit_thread_idle_lifecycle_if_idle` ->
/// `GoalExtension::on_thread_idle` -> `GoalRuntimeHandle::continue_if_idle`
/// (Wait arm, `spawn_check_in_timer`) -> `CheckInTimer` fire ->
/// `claim_due_deadline` -> `continue_if_idle` -> Core `start_turn_if_idle`
/// (linearization-point admission) -> automatic turn accounting -> production
/// warning. Each ticket bypasses only the work gate exactly once; the goal
/// stays active and event-wakeable throughout.
#[tokio::test(flavor = "current_thread")]
async fn scheduled_checkins_fire_through_production_runtime_under_paused_time() -> Result<()> {
    let server = responses::start_mock_server().await;
    let mock = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse_completed("checkin-1"),
            responses::sse_completed("checkin-2"),
            responses::sse_completed("checkin-3"),
        ],
    )
    .await;
    let clock: Arc<dyn CheckInClock> = Arc::new(PausedClock::new(Duration::from_secs(600_000)));
    let sink = Arc::new(RecordingWarningSink::default());
    let (test, db, service) = fixture(
        &server,
        Arc::clone(&clock),
        Arc::clone(&sink),
        Some(AsyncNotificationSupport::Available),
    )
    .await?;
    let thread_id = test.session_configured.thread_id;

    // Enable the policy and arm the stalled receipt BEFORE creating the goal:
    // `apply_runtime_effects` runs an initial `continue_if_idle`, which must
    // see the Armed work and wait (arming the first timer) instead of starting
    // an immediate turn that would leave the thread non-idle. Thread start on
    // a capable host activates the policy; the explicit enable below keeps the
    // pre-activation setup assumption intact.
    let runtime = test
        .codex
        .thread_extension_data()
        .get::<GoalRuntimeHandle>()
        .expect("goal runtime should exist");
    assert!(
        runtime.background_wait_state().is_enabled(),
        "capable host should activate the policy at thread start"
    );
    runtime.background_wait_state().enable();
    assert!(
        test.codex.test_arm_exec_receipt_for_background_wait().await,
        "stalled Armed receipt should arm",
    );

    let outcome = service
        .set_thread_goal(
            &db,
            GoalSetRequest {
                thread_id,
                objective: GoalObjectiveUpdate::Set("scheduled check-in work"),
                status: None,
                token_budget: GoalTokenBudgetUpdate::Keep,
                max_goal_token_budget: None,
            },
        )
        .await
        .expect("goal creation should succeed");
    outcome.apply_runtime_effects(&service).await;

    tokio::time::pause();
    test.codex
        .emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed)
        .await;
    assert_eq!(mock.requests().len(), 0);
    assert_eq!(runtime.background_wait_state().check_ins_used(), 0);
    assert!(runtime.test_marked_goal_continuations().is_empty());

    tokio::time::advance(Duration::from_secs(30 * 60)).await;
    tokio::time::resume();
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert_eq!(runtime.background_wait_state().check_ins_used(), 1);
    assert_eq!(runtime.test_marked_goal_continuations().len(), 1);

    tokio::time::pause();
    test.codex
        .emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed)
        .await;
    tokio::time::advance(Duration::from_secs(30 * 60)).await;
    tokio::time::resume();
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert_eq!(runtime.background_wait_state().check_ins_used(), 2);
    assert_eq!(runtime.test_marked_goal_continuations().len(), 2);

    tokio::time::pause();
    test.codex
        .emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed)
        .await;
    tokio::time::advance(Duration::from_secs(60 * 60)).await;
    tokio::time::resume();
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert_eq!(runtime.background_wait_state().check_ins_used(), 3);
    assert_eq!(runtime.test_marked_goal_continuations().len(), 3);

    for _ in 0..1000 {
        if sink.warnings().len() == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(sink.warnings(), vec![CHECK_INS_STOPPED_WARNING.to_string()]);
    assert!(runtime.background_wait_state().warning_emitted());

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(60 * 60)).await;
    tokio::time::resume();
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(runtime.background_wait_state().check_ins_used(), 3);
    assert_eq!(runtime.test_marked_goal_continuations().len(), 3);
    assert_eq!(sink.warnings().len(), 1);
    assert_eq!(mock.requests().len(), 3);
    Ok(())
}

/// Policy activation on a capable host: pending opted-in work gates automatic
/// goal continuation, while explicit user input is still admitted.
/// Production entry points: `GoalExtension::on_thread_start` (activation) ->
/// `GoalRuntimeHandle::continue_if_idle` (Wait arm) gates
/// `CodexThread::emit_thread_idle_lifecycle_if_idle`, and
/// `CodexThread::start_or_steer_turn` admits the user turn.
#[tokio::test(flavor = "current_thread")]
async fn background_wait_activation_gates_goal_but_admits_user_input() -> Result<()> {
    let server = responses::start_mock_server().await;
    let mock =
        responses::mount_sse_sequence(&server, vec![responses::sse_completed("should-stay-quiet")])
            .await;
    let clock: Arc<dyn CheckInClock> = Arc::new(PausedClock::new(Duration::from_secs(600_000)));
    let sink = Arc::new(RecordingWarningSink::default());
    let (test, db, service) = fixture(
        &server,
        Arc::clone(&clock),
        Arc::clone(&sink),
        Some(AsyncNotificationSupport::Available),
    )
    .await?;
    let thread_id = test.session_configured.thread_id;

    let runtime = test
        .codex
        .thread_extension_data()
        .get::<GoalRuntimeHandle>()
        .expect("goal runtime should exist");
    assert!(
        runtime.background_wait_state().is_enabled(),
        "capable host should activate the policy without manual enable"
    );
    assert!(
        test.codex.test_arm_exec_receipt_for_background_wait().await,
        "stalled Armed receipt should arm",
    );

    let outcome = service
        .set_thread_goal(
            &db,
            GoalSetRequest {
                thread_id,
                objective: GoalObjectiveUpdate::Set("activation gated work"),
                status: None,
                token_budget: GoalTokenBudgetUpdate::Keep,
                max_goal_token_budget: None,
            },
        )
        .await
        .expect("goal creation should succeed");
    outcome.apply_runtime_effects(&service).await;

    test.codex
        .emit_thread_idle_lifecycle_if_idle(codex_extension_api::ThreadIdleCause::Completed)
        .await;
    assert_eq!(
        mock.requests().len(),
        0,
        "pending opted-in work should gate automatic continuation"
    );
    assert!(runtime.test_marked_goal_continuations().is_empty());

    // Explicit user input is still admitted while the gate holds.
    let user_mock =
        responses::mount_sse_sequence(&server, vec![responses::sse_completed("user-turn")]).await;
    test.codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "continue please".to_string(),
            text_elements: Vec::new(),
        }]))
        .await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert_eq!(
        user_mock.requests().len(),
        1,
        "user input should be admitted while gated"
    );
    Ok(())
}

/// Without the host capability the policy stays inactive and automatic goal
/// continuation proceeds ungated.
#[tokio::test(flavor = "current_thread")]
async fn background_wait_inactive_on_unavailable_host_auto_continues() -> Result<()> {
    let server = responses::start_mock_server().await;
    let mock = responses::mount_sse_sequence(
        &server,
        vec![responses::sse_completed("ungated-continuation")],
    )
    .await;
    let clock: Arc<dyn CheckInClock> = Arc::new(PausedClock::new(Duration::from_secs(600_000)));
    let sink = Arc::new(RecordingWarningSink::default());
    let (test, db, service) = fixture(
        &server,
        Arc::clone(&clock),
        Arc::clone(&sink),
        Some(AsyncNotificationSupport::Unavailable),
    )
    .await?;
    let thread_id = test.session_configured.thread_id;

    let runtime = test
        .codex
        .thread_extension_data()
        .get::<GoalRuntimeHandle>()
        .expect("goal runtime should exist");
    assert!(
        !runtime.background_wait_state().is_enabled(),
        "headless host should leave the policy inactive"
    );
    assert!(
        test.codex.test_arm_exec_receipt_for_background_wait().await,
        "stalled Armed receipt should arm",
    );

    let outcome = service
        .set_thread_goal(
            &db,
            GoalSetRequest {
                thread_id,
                objective: GoalObjectiveUpdate::Set("ungated work"),
                status: None,
                token_budget: GoalTokenBudgetUpdate::Keep,
                max_goal_token_budget: None,
            },
        )
        .await
        .expect("goal creation should succeed");
    outcome.apply_runtime_effects(&service).await;

    test.codex
        .emit_thread_idle_lifecycle_if_idle(codex_extension_api::ThreadIdleCause::Completed)
        .await;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert_eq!(
        mock.requests().len(),
        1,
        "without the policy the goal should auto-continue"
    );
    assert_eq!(runtime.test_marked_goal_continuations().len(), 1);
    Ok(())
}
