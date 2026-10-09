//! Goal-owned durable-sleep lease for native subagent waits.
//!
//! Production entry points: `GoalExtension::on_thread_idle` ->
//! `GoalRuntimeHandle::continue_if_idle` (owned-children inspection via
//! `CodexThread::inspect_directly_owned_native_children`, marker insert BEFORE
//! mail check via `try_register_goal_wait_sleep`, recheck via
//! `CodexThread::recheck_pending_work_for_goal_wait`) -> V2 queue-only
//! completion mail wakes via the marker (`has_outstanding_durable_sleep`).
//! Removal is conditional (`remove_goal_wait_sleep`) on turn start, clear,
//! disable, stop, and release. `Interrupted` notices are queue-only, once per
//! transition, without redefining `is_final`. Real interruption is driven via
//! `Op::Interrupt` on the child thread (the app-server/TUI entry point),
//! which emits `TurnAborted(Interrupted)` through `turn_finished`.
//!
//! The goal is set after the parent spawns its child (not before the user
//! turn): an external set on an idle thread without pending work would start
//! a spurious goal continuation before the scripted work begins. Setting it
//! after `TurnComplete` still drives the production idle path (the set itself
//! calls `continue_if_idle`, and the suite then emits idle explicitly).
//!
//! Hosted only: these tests need model-scripted native spawns and real turns.

use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use anyhow::Result;
use codex_analytics::AnalyticsEventsClient;
use codex_core::OwnedChildInspection;
use codex_core::ThreadManager;
use codex_core::TurnInputRequest;
use codex_core::config::Config;
use codex_extension_api::AsyncNotificationSupport;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::ThreadStartInput;
use codex_extension_items::sleep::SleepItem;
use codex_features::Feature;
use codex_features::SleepToolMode;
use codex_goal_extension::GoalObjectiveUpdate;
use codex_goal_extension::GoalRuntimeHandle;
use codex_goal_extension::GoalService;
use codex_goal_extension::GoalSetRequest;
use codex_goal_extension::GoalTokenBudgetUpdate;
use codex_goal_extension::SystemCheckInClock;
use codex_goal_extension::has_goal_wait_sleep;
use codex_goal_extension::install_with_backend_and_clock;
use codex_protocol::ThreadId;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;

const MULTI_AGENT_V2_NAMESPACE: &str = "collaboration";

fn body_contains(request: &wiremock::Request, text: &str) -> bool {
    String::from_utf8_lossy(&request.body).contains(text)
}

fn has_function_call_output(request: &wiremock::Request, call_id: &str) -> bool {
    serde_json::from_slice::<serde_json::Value>(&request.body).is_ok_and(|body| {
        body.get("input")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| {
                items.iter().any(|item| {
                    item.get("type").and_then(serde_json::Value::as_str)
                        == Some("function_call_output")
                        && item.get("call_id").and_then(serde_json::Value::as_str) == Some(call_id)
                })
            })
    })
}

fn install_goal(
    registry: &mut ExtensionRegistryBuilder<Config>,
    db: &Arc<codex_state::StateRuntime>,
    service: &Arc<GoalService>,
    manager: Weak<ThreadManager>,
) {
    install_with_backend_and_clock(
        registry,
        db.clone(),
        AnalyticsEventsClient::disabled(),
        /*metrics_client*/ None,
        manager,
        service.clone(),
        |c: &Config| codex_goal_extension::GoalExtensionConfig {
            enabled: c.features.enabled(Feature::Goals),
            max_goal_token_budget: c.max_goal_token_budget,
        },
        Arc::new(SystemCheckInClock),
    );
}

async fn submit_user_input(codex: &codex_core::CodexThread, text: &str) {
    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }]))
        .await
        .expect("submit user input");
}

async fn fixture(
    server: &wiremock::MockServer,
) -> Result<(TestCodex, Arc<codex_state::StateRuntime>, Arc<GoalService>)> {
    let home = Arc::new(TempDir::new()?);
    let mut config = core_test_support::load_default_config_for_test(&home).await;
    config.features.disable(Feature::CurrentTimeReminder)?;
    let db = codex_core::init_state_db(&config)
        .await
        .expect("state database");
    let service = Arc::new(GoalService::new());
    let mut registry = ExtensionRegistryBuilder::<Config>::new();
    install_goal(&mut registry, &db, &service, Weak::new());
    let test = test_codex()
        .with_home(home)
        .with_extensions(Arc::new(registry.build()))
        .with_config(|c| {
            c.features
                .disable(Feature::CurrentTimeReminder)
                .expect("feature");
            c.features.enable(Feature::Collab).expect("feature");
            c.features.enable(Feature::MultiAgentV2).expect("feature");
            c.sleep_tool_mode = SleepToolMode::AlwaysOn;
        })
        .build_with_auto_env(server)
        .await?;
    test.codex
        .thread_extension_data()
        .remove::<GoalRuntimeHandle>();
    test.codex
        .thread_extension_data()
        .insert(AsyncNotificationSupport::Available);
    let mut bound = ExtensionRegistryBuilder::<Config>::new();
    install_goal(
        &mut bound,
        &db,
        &service,
        Arc::downgrade(&test.thread_manager),
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

async fn set_goal(
    service: &GoalService,
    db: &Arc<codex_state::StateRuntime>,
    thread_id: ThreadId,
) -> Result<()> {
    let outcome = service
        .set_thread_goal(
            db,
            GoalSetRequest {
                thread_id,
                objective: GoalObjectiveUpdate::Set("wait for native worker"),
                status: None,
                token_budget: GoalTokenBudgetUpdate::Keep,
                max_goal_token_budget: None,
            },
        )
        .await?;
    outcome.apply_runtime_effects(service).await;
    Ok(())
}

fn spawn_call(call_id: &str) -> serde_json::Value {
    spawn_call_with_message(call_id, "do slow work", "worker")
}

fn spawn_call_with_message(call_id: &str, message: &str, task_name: &str) -> serde_json::Value {
    responses::ev_function_call_with_namespace(
        call_id,
        MULTI_AGENT_V2_NAMESPACE,
        "spawn_agent",
        &serde_json::to_string(&json!({
            "message": message,
            "task_name": task_name,
            "fork_turns": "none",
        }))
        .expect("spawn args"),
    )
}

fn sleep_call(call_id: &str, duration_ms: u64) -> serde_json::Value {
    responses::ev_function_call_with_namespace(
        call_id,
        "clock",
        "sleep",
        &serde_json::to_string(&json!({ "duration_ms": duration_ms })).expect("sleep args"),
    )
}

/// Returns the directly owned native child once it is observably `Running`.
///
/// Interrupting before the child's turn starts emits no `TurnAborted` and no
/// notice, so both interruption tests synchronize on `Running` first. Bounded
/// with a clear failure message: nothing here may hang the harness.
async fn running_owned_child_id(test: &TestCodex) -> ThreadId {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let inspections = test
                .codex
                .inspect_directly_owned_native_children()
                .await
                .expect("owned-children inspection should succeed");
            let running = inspections
                .iter()
                .find(|inspection| matches!(inspection.status(), Some(AgentStatus::Running)));
            if let Some(running) = running {
                break running.thread_id();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for the native child to reach Running")
}

/// AC1 (+AC6, +AC8 inherited settings): a Running native child registers the
/// owned marker at idle; its queue-only completion wakes with a real model
/// request. Storing the marker emits no sleep UI, no history item, and no
/// reminder change; the wake preserves inherited turn settings.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn goal_wait_registers_marker_and_wakes_on_child_completion() -> Result<()> {
    let server = responses::start_mock_server().await;
    let parent_initial = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "wait for native worker")
                && !has_function_call_output(request, "call-spawn-1")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-parent-spawn"),
            spawn_call("call-spawn-1"),
            responses::ev_completed("resp-parent-spawn"),
        ]),
    )
    .await;
    let parent_after_spawn = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            has_function_call_output(request, "call-spawn-1")
                && !body_contains(request, "worker completed")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-parent-idle"),
            responses::ev_assistant_message("resp-parent-idle", "parent ending turn"),
            responses::ev_completed("resp-parent-idle"),
        ]),
    )
    .await;
    let child_sleep = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "do slow work")
                && !body_contains(request, "wait for native worker")
                && !has_function_call_output(request, "call-sleep-1")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-child-sleep"),
            sleep_call("call-sleep-1", 2_000),
            responses::ev_completed("resp-child-sleep"),
        ]),
    )
    .await;
    let child_final = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| has_function_call_output(request, "call-sleep-1"),
        responses::sse(vec![
            responses::ev_response_created("resp-child-done"),
            responses::ev_assistant_message("resp-child-done", "worker completed"),
            responses::ev_completed("resp-child-done"),
        ]),
    )
    .await;
    let parent_wake = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, "worker completed"),
        responses::sse(vec![
            responses::ev_response_created("resp-parent-wake"),
            responses::ev_assistant_message("resp-parent-wake", "parent saw completion"),
            responses::ev_completed("resp-parent-wake"),
        ]),
    )
    .await;

    let (test, db, service) = fixture(&server).await?;
    let thread_id = test.session_configured.thread_id;

    submit_user_input(&test.codex, "wait for native worker").await;

    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    // Goal set after the spawn so no pre-work continuation starts; the set
    // itself runs `continue_if_idle`, and the explicit idle below re-enters it.
    set_goal(&service, &db, thread_id).await?;
    test.codex
        .emit_thread_idle_lifecycle_if_idle(codex_extension_api::ThreadIdleCause::Completed)
        .await;

    assert!(has_goal_wait_sleep(test.codex.thread_extension_data()));
    // Storing the marker emits no UI events: the idle parent stays silent
    // until the child completes (no turn, no item events from the insert).
    assert!(
        tokio::time::timeout(Duration::from_millis(500), test.codex.next_event())
            .await
            .is_err(),
        "marker insert must emit no parent events"
    );
    assert_eq!(parent_initial.requests().len(), 1);
    assert_eq!(parent_after_spawn.requests().len(), 1);
    assert_eq!(child_sleep.requests().len(), 1);

    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnStarted(_)) && parent_wake.requests().len() == 1
    })
    .await;
    assert_eq!(child_final.requests().len(), 1);
    assert_eq!(parent_wake.requests().len(), 1);
    let wake = parent_wake.single_request();
    let wake_body = wake.body_json().to_string();
    assert!(wake_body.contains("worker completed"));
    assert!(!wake_body.contains("goal-wait:"));
    assert!(!wake_body.contains("current_time"));
    assert!(!has_goal_wait_sleep(test.codex.thread_extension_data()));
    Ok(())
}

/// AC3: completion between the idle inspection and the insert still wakes.
///
/// Two children: the fast one finishes while registration is paused at the
/// gate (or just before arrival — either way its mail is confirmed enqueued
/// before release); the slow one stays Running so the post-release
/// evaluation waits instead of starting a goal turn. Correct order (insert
/// BEFORE mail check + recheck) starts a mail-wake turn for the fast
/// child's mail; the mutant (recheck before insert, no post-insert recheck)
/// sees no marker, then waits on the slow child, and stays silent. Every
/// wait is bounded with a clear failure message: nothing here may hang the
/// harness.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn goal_wait_latch_closes_completion_before_registration() -> Result<()> {
    let server = responses::start_mock_server().await;
    let _parent_initial = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "latch worker")
                && !has_function_call_output(request, "call-latch-spawn-fast")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-latch-spawn"),
            spawn_call_with_message("call-latch-spawn-fast", "do fast latch work", "fast"),
            spawn_call_with_message("call-latch-spawn-slow", "do slow latch work", "slow"),
            responses::ev_completed("resp-latch-spawn"),
        ]),
    )
    .await;
    let _parent_after_spawn = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            has_function_call_output(request, "call-latch-spawn-fast")
                && has_function_call_output(request, "call-latch-spawn-slow")
                && !body_contains(request, "child one completed")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-latch-idle"),
            responses::ev_assistant_message("resp-latch-idle", "parent ending turn"),
            responses::ev_completed("resp-latch-idle"),
        ]),
    )
    .await;
    let _child_fast_sleep = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "do fast latch work")
                && !body_contains(request, "latch worker")
                && !has_function_call_output(request, "call-latch-fast-sleep")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-latch-fast-sleep"),
            sleep_call("call-latch-fast-sleep", 3_000),
            responses::ev_completed("resp-latch-fast-sleep"),
        ]),
    )
    .await;
    // Mounts follow causal request order: wiremock evaluates oldest-first
    // and `ResponseMock` records every evaluated request (even when the
    // custom matcher then rejects it), so an earlier-mounted still-active
    // mock phantom-records requests meant for a later one. The slow
    // child's prompt request arrives before the fast final; mounting the
    // fast-final mock first would inflate its recorded count by one.
    let _child_slow_sleep = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "do slow latch work")
                && !body_contains(request, "latch worker")
                && !has_function_call_output(request, "call-latch-slow-sleep")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-latch-slow-sleep"),
            sleep_call("call-latch-slow-sleep", 60_000),
            responses::ev_completed("resp-latch-slow-sleep"),
        ]),
    )
    .await;
    let child_fast_final = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| has_function_call_output(request, "call-latch-fast-sleep"),
        responses::sse(vec![
            responses::ev_response_created("resp-latch-fast-done"),
            responses::ev_assistant_message("resp-latch-fast-done", "child one completed"),
            responses::ev_completed("resp-latch-fast-done"),
        ]),
    )
    .await;
    // No final mock for the slow child: its 60 s sleep outlives the test, so
    // it stays Running from the idle inspection through the wake assertions.
    let parent_wake = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, "child one completed"),
        responses::sse(vec![
            responses::ev_response_created("resp-latch-wake"),
            responses::ev_assistant_message("resp-latch-wake", "parent saw fast completion"),
            responses::ev_completed("resp-latch-wake"),
        ]),
    )
    .await;

    let (test, db, service) = fixture(&server).await?;
    let thread_id = test.session_configured.thread_id;

    let gate = test
        .codex
        .thread_extension_data()
        .get_or_init(codex_goal_extension::TestNativeWaitRegistrationGate::new);
    submit_user_input(&test.codex, "latch worker").await;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;

    // The set itself runs `continue_if_idle`, which pauses at the gate after
    // observing Running work. Bounded: a missed gate, or a set that sails
    // through without pausing, fails here instead of hanging the harness.
    let set_goal_fut = set_goal(&service, &db, thread_id);
    tokio::pin!(set_goal_fut);
    let mut set_goal_fut = set_goal_fut.as_mut();
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = &mut set_goal_fut => {
                panic!(
                    "goal set should pause at the registration gate while the slow child is Running"
                )
            }
            _ = gate.wait_arrived() => {}
        }
    })
    .await
    .expect("timed out waiting for the idle path to pause at the registration gate");

    // Release only after the fast child's completion mail is confirmed
    // enqueued in the parent mailbox: the candidate's post-insert recheck
    // then deterministically observes it, while the mutant's pre-insert
    // recheck (no marker yet) deterministically misses it. Bounded poll,
    // not a fixed sleep.
    tokio::time::timeout(Duration::from_secs(15), async {
        while !test.codex.test_has_pending_mailbox_mail().await {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect(
        "timed out waiting for the fast child's completion mail before releasing the registration gate",
    );
    gate.release();
    // The gate served its single latch purpose: later idles must not pause
    // (the slow child stays Running, so a retained gate would stall them).
    test.codex
        .thread_extension_data()
        .remove::<codex_goal_extension::TestNativeWaitRegistrationGate>();
    tokio::time::timeout(Duration::from_secs(10), set_goal_fut)
        .await
        .expect("goal set hung after the registration gate was released")?;

    // No explicit idle emit here: the set's own `continue_if_idle` is the
    // single registration attempt under test. A second idle pass would
    // re-run registration with mail already pending and mask the mutant.
    //
    // The post-insert recheck must start a mail-wake turn promptly. The
    // mutant (recheck before insert, then waiting on the slow child) stays
    // silent and fails here with a clear message instead of hanging. Events
    // queue, so a turn that started during the set above is still observed.
    // A custom loop (not `wait_for_event`, whose inner per-event timeout
    // would preempt this message) keeps the failure latch-specific.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let event = test
                .codex
                .next_event()
                .await
                .expect("event stream ended while waiting for the parent wake turn");
            if matches!(event.msg, EventMsg::TurnStarted(_)) {
                break;
            }
        }
    })
    .await
    .expect(
        "timed out waiting for the parent wake turn: the post-insert recheck \
         should start a mail-wake turn for the fast child's completion mail",
    );
    // Admitted turn start removes the owned marker (asserted while the wake
    // turn runs, before any later idle can reinsert it).
    assert!(!has_goal_wait_sleep(test.codex.thread_extension_data()));
    tokio::time::timeout(Duration::from_secs(15), async {
        while parent_wake.requests().len() != 1 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for the parent wake request after its turn started");
    // Full-match counts, not raw recorded counts: `ResponseMock` also
    // records requests it only partially evaluated (see the mount-order
    // comment), so count only the requests that actually carry the sleep
    // output / completion text. The recorded requests print on failure to
    // identify any unexpected extra sampling request.
    let fast_final_count = child_fast_final
        .requests()
        .into_iter()
        .filter(|request| {
            request
                .function_call_output_text("call-latch-fast-sleep")
                .is_some()
        })
        .count();
    assert_eq!(
        fast_final_count,
        1,
        "expected exactly one fast-child final request, got {:?}",
        child_fast_final.requests()
    );
    let wakes: Vec<_> = parent_wake
        .requests()
        .into_iter()
        .filter(|request| request.body_contains_text("child one completed"))
        .collect();
    assert_eq!(
        wakes.len(),
        1,
        "expected exactly one parent wake request, got {:?}",
        parent_wake.requests()
    );
    let wake_body = wakes[0].body_json().to_string();
    assert!(wake_body.contains("child one completed"));
    assert!(!wake_body.contains("goal-wait:"));
    // The wake turn is a mail wake, not a goal continuation: no steering
    // item (which would render the goal objective) is attached. A goal turn
    // draining the mail instead fails here.
    assert!(!wake_body.contains("wait for native worker"));
    // No goal continuation turn ran: the evaluation waited on the slow
    // child, so the wake came only from the post-insert recheck.
    let runtime = test
        .codex
        .thread_extension_data()
        .get::<GoalRuntimeHandle>()
        .expect("goal runtime should exist");
    assert!(
        runtime.test_marked_goal_continuations().is_empty(),
        "no goal continuation turn should run while the latch wake is pending"
    );
    // The slow child stays Running: the wait branch, not Proceed, applied.
    let inspections = test
        .codex
        .inspect_directly_owned_native_children()
        .await
        .expect("owned-children inspection should succeed");
    assert!(
        inspections
            .iter()
            .any(OwnedChildInspection::is_pending_native_work),
        "slow child should still be pending native work, got {inspections:?}"
    );
    Ok(())
}

/// AC4: a foreign SleepItem blocks registration without being clobbered, and
/// turn-start removal preserves it. The unconditional-remove mutant deletes the
/// foreign marker and fails the preservation assertions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn goal_wait_preserves_foreign_sleep_across_register_and_turn_start() -> Result<()> {
    let server = responses::start_mock_server().await;
    let _parent_initial = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "foreign worker")
                && !has_function_call_output(request, "call-foreign-spawn")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-foreign-spawn"),
            spawn_call("call-foreign-spawn"),
            responses::ev_completed("resp-foreign-spawn"),
        ]),
    )
    .await;
    let _parent_after_spawn = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            has_function_call_output(request, "call-foreign-spawn")
                && !body_contains(request, "foreign worker completed")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-foreign-idle"),
            responses::ev_assistant_message("resp-foreign-idle", "parent ending turn"),
            responses::ev_completed("resp-foreign-idle"),
        ]),
    )
    .await;
    let _child_sleep = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "do slow work")
                && !body_contains(request, "foreign worker")
                && !has_function_call_output(request, "call-foreign-sleep")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-foreign-child-sleep"),
            sleep_call("call-foreign-sleep", 2_000),
            responses::ev_completed("resp-foreign-child-sleep"),
        ]),
    )
    .await;
    let _child_final = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| has_function_call_output(request, "call-foreign-sleep"),
        responses::sse(vec![
            responses::ev_response_created("resp-foreign-child-done"),
            responses::ev_assistant_message("resp-foreign-child-done", "foreign worker completed"),
            responses::ev_completed("resp-foreign-child-done"),
        ]),
    )
    .await;
    let parent_wake = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, "foreign worker completed"),
        responses::sse(vec![
            responses::ev_response_created("resp-foreign-wake"),
            responses::ev_assistant_message("resp-foreign-wake", "parent saw foreign completion"),
            responses::ev_completed("resp-foreign-wake"),
        ]),
    )
    .await;

    let (test, db, service) = fixture(&server).await?;
    let thread_id = test.session_configured.thread_id;

    let foreign = SleepItem {
        id: "clock-wait-1".to_string(),
        duration_ms: 60_000,
    };
    test.codex.thread_extension_data().insert(foreign.clone());
    submit_user_input(&test.codex, "foreign worker").await;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    set_goal(&service, &db, thread_id).await?;
    test.codex
        .emit_thread_idle_lifecycle_if_idle(codex_extension_api::ThreadIdleCause::Completed)
        .await;

    assert_eq!(
        test.codex
            .thread_extension_data()
            .get::<SleepItem>()
            .as_deref(),
        Some(&foreign)
    );
    assert!(!has_goal_wait_sleep(test.codex.thread_extension_data()));

    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnStarted(_)) && parent_wake.requests().len() == 1
    })
    .await;
    assert_eq!(parent_wake.requests().len(), 1);
    assert_eq!(
        test.codex
            .thread_extension_data()
            .get::<SleepItem>()
            .as_deref(),
        Some(&foreign)
    );
    Ok(())
}

/// AC5 (clear/release) + AC9 (resume never revives): explicit lifecycle paths
/// remove only the owned marker, and resume starts without one.
#[tokio::test(flavor = "current_thread")]
async fn goal_wait_removed_on_clear_release_and_resume() -> Result<()> {
    let server = responses::start_mock_server().await;
    let (test, db, service) = fixture(&server).await?;
    let thread_id = test.session_configured.thread_id;
    set_goal(&service, &db, thread_id).await?;

    let runtime = test
        .codex
        .thread_extension_data()
        .get::<GoalRuntimeHandle>()
        .expect("goal runtime should exist");
    assert!(codex_goal_extension::try_register_goal_wait_sleep(
        test.codex.thread_extension_data(),
        runtime.background_wait_state().generation(),
    ));
    assert!(has_goal_wait_sleep(test.codex.thread_extension_data()));

    runtime.release_native_wait().await;
    assert!(!has_goal_wait_sleep(test.codex.thread_extension_data()));

    assert!(codex_goal_extension::try_register_goal_wait_sleep(
        test.codex.thread_extension_data(),
        runtime.background_wait_state().generation(),
    ));
    assert!(service.clear_thread_goal(&db, thread_id).await?);
    assert!(!has_goal_wait_sleep(test.codex.thread_extension_data()));

    assert!(codex_goal_extension::try_register_goal_wait_sleep(
        test.codex.thread_extension_data(),
        runtime.background_wait_state().generation(),
    ));
    service
        .restore_thread_runtime_after_resume(thread_id)
        .await?;
    assert!(!has_goal_wait_sleep(test.codex.thread_extension_data()));
    Ok(())
}

/// AC7: interrupting a real Running native child of a goal parent that holds
/// the marker delivers exactly one queue-only INTERRUPTED notice and one wake
/// model request for that transition, without claiming success. The child's
/// global `is_final` is unchanged (`Interrupted` stays non-final).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn goal_wait_registered_parent_gets_single_interrupted_notice() -> Result<()> {
    let server = responses::start_mock_server().await;
    let parent_initial = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "supervise native worker")
                && !has_function_call_output(request, "call-int-spawn")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-int-spawn"),
            spawn_call("call-int-spawn"),
            responses::ev_completed("resp-int-spawn"),
        ]),
    )
    .await;
    let parent_after_spawn = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            has_function_call_output(request, "call-int-spawn")
                && !body_contains(request, "Message Type: INTERRUPTED")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-int-idle"),
            responses::ev_assistant_message("resp-int-idle", "parent ending turn"),
            responses::ev_completed("resp-int-idle"),
        ]),
    )
    .await;
    let child_sleep = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "do slow work")
                && !body_contains(request, "supervise native worker")
                && !has_function_call_output(request, "call-int-sleep")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-int-child-sleep"),
            sleep_call("call-int-sleep", 60_000),
            responses::ev_completed("resp-int-child-sleep"),
        ]),
    )
    .await;
    let parent_wake = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, "Message Type: INTERRUPTED"),
        responses::sse(vec![
            responses::ev_response_created("resp-int-wake"),
            responses::ev_assistant_message("resp-int-wake", "parent saw interruption"),
            responses::ev_completed("resp-int-wake"),
        ]),
    )
    .await;

    let (test, db, service) = fixture(&server).await?;
    let thread_id = test.session_configured.thread_id;

    submit_user_input(&test.codex, "supervise native worker").await;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    set_goal(&service, &db, thread_id).await?;
    test.codex
        .emit_thread_idle_lifecycle_if_idle(codex_extension_api::ThreadIdleCause::Completed)
        .await;

    assert!(has_goal_wait_sleep(test.codex.thread_extension_data()));
    assert_eq!(parent_initial.requests().len(), 1);
    assert_eq!(parent_after_spawn.requests().len(), 1);
    assert_eq!(child_sleep.requests().len(), 1);

    let child_thread_id = running_owned_child_id(&test).await;
    let child = test
        .thread_manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be loaded");

    // Real production interrupt entry point: the same `Op` the app-server and
    // the TUI submit. It aborts the child's sleep turn with `TurnAborted` for
    // `Interrupted`, which reaches `turn_finished` as a gated queue-only mail.
    child.submit(Op::Interrupt).await.expect("interrupt child");
    wait_for_event(&child, |event| {
        matches!(event, EventMsg::TurnAborted(_))
    })
    .await;

    // The child's `TurnAborted` is emitted after parent delivery completes
    // (`send_event` awaits the terminal-turn notify), so the notice mail is
    // already enqueued here; the wake turn follows via the marker.
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnStarted(_)) && parent_wake.requests().len() == 1
    })
    .await;
    assert!(!has_goal_wait_sleep(test.codex.thread_extension_data()));
    let wakes: Vec<_> = parent_wake
        .requests()
        .into_iter()
        .filter(|request| request.body_contains_text("Message Type: INTERRUPTED"))
        .collect();
    assert_eq!(
        wakes.len(),
        1,
        "expected exactly one parent wake request, got {:?}",
        parent_wake.requests()
    );
    let wake_body = wakes[0].body_json().to_string();
    assert_eq!(
        wake_body.matches("Message Type: INTERRUPTED").count(),
        1,
        "expected exactly one interrupted notice in the wake request: {wake_body}"
    );
    assert!(wake_body.contains("without a final answer"));
    assert!(!wake_body.contains("FINAL_ANSWER"));
    assert!(!wake_body.contains("completed successfully"));

    // The child's runtime status is `Interrupted`; the global `is_final`
    // stays unchanged (`Interrupted` non-final), pinned by the in-crate
    // unit test `agent::status_tests::interrupted_stays_non_final`.
    let child_status = child.agent_status().await;
    assert_eq!(child_status, AgentStatus::Interrupted);
    Ok(())
}

/// AC7 negative: interrupting a Running native child of an ordinary parent
/// (no goal, no marker) enqueues no notice and starts no wake turn. The
/// child's global `is_final` is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn goal_wait_ordinary_parent_stays_quiet_on_child_interrupt() -> Result<()> {
    let server = responses::start_mock_server().await;
    let parent_initial = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "ordinary worker")
                && !has_function_call_output(request, "call-quiet-spawn")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-quiet-spawn"),
            spawn_call("call-quiet-spawn"),
            responses::ev_completed("resp-quiet-spawn"),
        ]),
    )
    .await;
    let parent_after_spawn = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            has_function_call_output(request, "call-quiet-spawn")
                && !body_contains(request, "Message Type: INTERRUPTED")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-quiet-idle"),
            responses::ev_assistant_message("resp-quiet-idle", "parent ending turn"),
            responses::ev_completed("resp-quiet-idle"),
        ]),
    )
    .await;
    let child_sleep = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "do slow work")
                && !body_contains(request, "ordinary worker")
                && !has_function_call_output(request, "call-quiet-sleep")
        },
        responses::sse(vec![
            responses::ev_response_created("resp-quiet-child-sleep"),
            sleep_call("call-quiet-sleep", 60_000),
            responses::ev_completed("resp-quiet-child-sleep"),
        ]),
    )
    .await;
    // No wake mock: any parent wake request would have no responder.

    let (test, _db, _service) = fixture(&server).await?;

    // No goal is set: this parent is ordinary and registers no marker.
    submit_user_input(&test.codex, "ordinary worker").await;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    test.codex
        .emit_thread_idle_lifecycle_if_idle(codex_extension_api::ThreadIdleCause::Completed)
        .await;
    assert!(!has_goal_wait_sleep(test.codex.thread_extension_data()));

    let child_thread_id = running_owned_child_id(&test).await;
    let child = test
        .thread_manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be loaded");
    child.submit(Op::Interrupt).await.expect("interrupt child");
    wait_for_event(&child, |event| {
        matches!(event, EventMsg::TurnAborted(_))
    })
    .await;

    // The gated delivery returns before sending, so no notice mail is
    // enqueued, and no wake turn starts within the silence window.
    assert!(!test.codex.test_has_pending_mailbox_mail().await);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), test.codex.next_event())
            .await
            .is_err(),
        "ordinary parent must stay quiet after a child interrupt"
    );
    assert_eq!(parent_initial.requests().len(), 1);
    assert_eq!(parent_after_spawn.requests().len(), 1);
    assert_eq!(child_sleep.requests().len(), 1);

    // `Interrupted` here too; `is_final` non-finality is pinned by
    // `agent::status_tests::interrupted_stays_non_final`.
    let child_status = child.agent_status().await;
    assert_eq!(child_status, AgentStatus::Interrupted);
    Ok(())
}
