//! App-server vertical test: completion notifications plus the goal background wait.
//!
//! Drives the production JSON-RPC surface (`thread/start`, `thread/goal/set`,
//! `turn/start`) with real tool dispatch against a scripted mock model server.
//! Setting a goal triggers an automatic turn in which the scripted model
//! launches a barrier-controlled process with `notify_on_exit`, then ends the
//! turn. While the barrier is held the goal background-wait gate must hold
//! automatic continuation (zero new model requests); releasing the barrier
//! must produce exactly one wake request carrying the exec-completion fragment
//! for the acknowledged receipt, followed by goal progress (`update_goal`).
//!
//! Production entry points exercised:
//!
//! - `GoalExtension::on_thread_idle` -> `GoalRuntimeHandle::continue_if_idle`
//!   (the background-wait gate holds while the receipt is `Armed`).
//! - exit watcher -> `publish_exit` -> `enqueue_published_completion` (mailbox
//!   wake after the barrier release).
//! - `TurnInput::ExecCompletion` recording -> `ExecCompletionFragment` in the
//!   wake request (sampling acknowledgement on submit).
//! - `update_goal` tool -> goal completion observed via `thread/goal/get`.
//!
//! Synchronization is barrier-based, never sleep-based. The wedged child
//! process polls for a release file the test writes; every test wait is a
//! server notification, an RPC round-trip, or a bounded notification timeout.
//! No probe file is needed: the process spawn precedes the tool yield, which
//! precedes turn completion, so by the time the turn ends the child is
//! guaranteed to be wedged on the barrier.
//!
//! Silence proof (AC1): after `turn/completed` the test performs
//! `thread/goal/get` round-trips and only then asserts the mock saw no new
//! request. Goal re-entry is immediate (the first check-in deadline is 30
//! minutes out), so an ungated continuation would have POSTed long before the
//! check. The release write and the wake ordering assertions close the
//! remainder: a continuation racing the release would either inflate the
//! pre-release count, displace the fragment-bearing wake request, or inflate
//! the final count, and every one of those fails the test.

use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::ThreadGoalGetResponse;
use codex_app_server_protocol::ThreadGoalSetResponse;
use codex_app_server_protocol::ThreadGoalStatus;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStartedNotification;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_features::Feature;
use core_test_support::responses;
use core_test_support::responses::ResponseMock;
use core_test_support::skip_if_remote;
use core_test_support::skip_if_wine_exec;
use pretty_assertions::assert_eq;
use pretty_assertions::assert_ne;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;
use wiremock::MockServer;

const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Barrier-gated child command: loops until `release_file` exists, then prints
/// a marker and exits 0. The child-side poll is the barrier mechanism (same
/// shape as the `process/spawn` probe/release handshake); the test itself
/// never sleeps.
fn barrier_command(release_file: &Path) -> String {
    let path = release_file.to_string_lossy();
    if cfg!(windows) {
        let quoted = path.replace('\'', "''");
        format!(
            "powershell.exe -NoProfile -NonInteractive -Command \"while (!(Test-Path -LiteralPath '{quoted}')) {{ Start-Sleep -Milliseconds 20 }}; [Console]::Out.Write('barrier-released')\""
        )
    } else {
        let quoted = path.replace('\'', "'\\''");
        format!("while [ ! -e '{quoted}' ]; do sleep 0.05; done; echo barrier-released")
    }
}

fn exec_call_sse(
    response_id: &str,
    call_id: &str,
    cmd: &str,
    notify_on_exit: bool,
    yield_time_ms: u64,
) -> String {
    let arguments = json!({
        "cmd": cmd,
        "yield_time_ms": yield_time_ms,
        "notify_on_exit": notify_on_exit,
    })
    .to_string();
    responses::sse(vec![
        responses::ev_response_created(response_id),
        responses::ev_function_call(call_id, "exec_command", &arguments),
        responses::ev_completed(response_id),
    ])
}

fn final_message_sse(response_id: &str, message_id: &str, text: &str) -> String {
    responses::sse(vec![
        responses::ev_response_created(response_id),
        responses::ev_assistant_message(message_id, text),
        responses::ev_completed(response_id),
    ])
}

fn update_goal_complete_sse(response_id: &str, call_id: &str) -> String {
    responses::sse(vec![
        responses::ev_response_created(response_id),
        responses::ev_function_call(call_id, "update_goal", r#"{"status":"complete"}"#),
        responses::ev_completed(response_id),
    ])
}

/// Finds a plain (non-namespaced) function tool by name in a `/responses`
/// request body.
fn function_tool<'body>(body: &'body Value, name: &str) -> Option<&'body Value> {
    body.get("tools")?.as_array()?.iter().find(|tool| {
        tool.get("type").and_then(Value::as_str) == Some("function")
            && tool.get("name").and_then(Value::as_str) == Some(name)
    })
}

fn tool_has_parameter(body: &Value, tool_name: &str, parameter: &str) -> bool {
    function_tool(body, tool_name)
        .and_then(|tool| tool.get("parameters"))
        .and_then(|parameters| parameters.get("properties"))
        .and_then(|properties| properties.get(parameter))
        .is_some()
}

/// Extracts the receipt handle from the exec tool-result ack text
/// ("Completion notification armed with receipt ID {uuid}; ...").
fn parse_receipt_handle(tool_output: &str) -> Result<String> {
    let marker = "receipt ID ";
    let start = tool_output
        .find(marker)
        .map(|index| index + marker.len())
        .context("tool result should acknowledge the receipt")?;
    let handle = tool_output
        .get(start..start + 36)
        .context("receipt handle should be a hyphenated UUID")?;
    let is_uuid =
        handle.len() == 36 && handle.chars().all(|ch| ch.is_ascii_hexdigit() || ch == '-');
    anyhow::ensure!(is_uuid, "receipt handle should be a hyphenated UUID");
    Ok(handle.to_string())
}

struct GoalHarness {
    mock: ResponseMock,
    app: TestAppServer,
    thread_id: String,
    _home: TempDir,
    // The mock model server must outlive the test: dropping it stops the
    // listener that serves the scripted responses.
    _server: MockServer,
}

/// Starts a server with auto env, a thread, and a goal. Setting the goal
/// triggers the first automatic turn; callers await its notifications.
async fn start_goal_harness(
    scripts: Vec<String>,
    extra_server_args: &[&str],
) -> Result<GoalHarness> {
    let server = responses::start_mock_server().await;
    let mock = responses::mount_sse_sequence(&server, scripts).await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.4")
        .enable_feature(Feature::Goals)
        .write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .with_args(extra_server_args)
        .build_initialized()
        .await?;
    let request = app
        .send_thread_start_request_with_auto_env(ThreadStartParams::default())
        .await?;
    let ThreadStartResponse { thread, .. } = app.read_response(request).await?;
    let thread_id = thread.id.clone();
    let request = app
        .send_raw_request(
            "thread/goal/set",
            Some(json!({"threadId": thread_id, "objective": "finish the background work"})),
        )
        .await?;
    let _: ThreadGoalSetResponse = app.read_response(request).await?;
    Ok(GoalHarness {
        mock,
        app,
        thread_id,
        _home: home,
        _server: server,
    })
}

async fn await_turn_started(app: &mut TestAppServer) -> Result<TurnStartedNotification> {
    timeout(READ_TIMEOUT, app.read_notification("turn/started")).await?
}

async fn await_turn_completed(app: &mut TestAppServer) -> Result<TurnCompletedNotification> {
    timeout(READ_TIMEOUT, app.read_notification("turn/completed")).await?
}

async fn goal_status(app: &mut TestAppServer, thread_id: &str) -> Result<ThreadGoalStatus> {
    let request = app
        .send_raw_request("thread/goal/get", Some(json!({"threadId": thread_id})))
        .await?;
    let result: ThreadGoalGetResponse = timeout(READ_TIMEOUT, app.read_response(request)).await??;
    Ok(result.goal.context("goal should exist")?.status)
}

async fn start_user_turn(app: &mut TestAppServer, thread_id: &str, text: &str) -> Result<()> {
    let request = app
        .send_turn_start_request(TurnStartParams {
            thread_id: thread_id.to_string(),
            input: vec![UserInput::Text {
                text: text.to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse = timeout(READ_TIMEOUT, app.read_response(request)).await??;
    Ok(())
}

fn user_message_texts(items: &[ThreadItem]) -> Vec<String> {
    let mut texts = Vec::new();
    for item in items {
        if let ThreadItem::UserMessage { content, .. } = item {
            for input in content {
                if let UserInput::Text { text, .. } = input {
                    texts.push(text.clone());
                }
            }
        }
    }
    texts
}

/// AC1/AC2/AC3: the goal gates continuation while the notified process runs,
/// then wakes exactly once with the receipt fragment and completes the goal.
///
/// A stalled runtime (no wake after exit) fails at the wake `turn/started`
/// wait; the `drop-wake` mutant proves it.
#[tokio::test]
async fn notified_exec_exit_wakes_gated_goal_with_receipt_fragment() -> Result<()> {
    skip_if_remote!(
        Ok(()),
        "the file-barrier handshake needs a filesystem shared with the exec host"
    );
    skip_if_wine_exec!(
        Ok(()),
        "the barrier child needs a native shell on the exec host"
    );
    // The barrier directory is independent of the harness: scripts must embed
    // the release path before the harness (and its mock server) is built.
    let barrier_dir = TempDir::new()?;
    let release_file = barrier_dir.path().join("release");
    let scripts = vec![
        exec_call_sse(
            "wait-r1",
            "model-exec",
            &barrier_command(&release_file),
            /*notify_on_exit*/ true,
            /*yield_time_ms*/ 500,
        ),
        final_message_sse(
            "wait-r2",
            "wait-m2",
            "ending the turn; background work is running",
        ),
        update_goal_complete_sse("wait-r3", "model-complete"),
        final_message_sse(
            "wait-r4",
            "wait-m4",
            "background work finished; goal complete",
        ),
    ];
    let mut harness = start_goal_harness(scripts, &[]).await?;

    let first_started = await_turn_started(&mut harness.app).await?;
    let first_completed = await_turn_completed(&mut harness.app).await?;
    assert_eq!(first_completed.turn.id, first_started.turn.id);
    assert_eq!(TurnStatus::Completed, first_completed.turn.status);
    assert_eq!(None, first_completed.turn.error);

    // The barrier held from the start: the tool yielded a live session and
    // acknowledged the subscription.
    let requests = harness.mock.requests();
    assert_eq!(requests.len(), 2);
    let initial_body = requests[0].body_json();
    assert!(
        tool_has_parameter(&initial_body, "exec_command", "notify_on_exit"),
        "capable host should advertise notify_on_exit"
    );
    assert!(
        function_tool(&initial_body, "exec_notification").is_some(),
        "capable host should advertise exec_notification"
    );
    let ack = requests[1]
        .function_call_output_text("model-exec")
        .context("exec tool result should be attached")?;
    assert!(
        ack.contains("Process running with session ID "),
        "barrier process should still run at yield: {ack}"
    );
    let receipt = parse_receipt_handle(&ack)?;
    for request in &requests {
        assert!(
            !request.body_contains_text("source=\"exec_completion\""),
            "no completion fragment before the exit"
        );
    }

    // AC1: quiescence round-trips, then zero continuation requests while the
    // barrier is held. Goal re-entry is immediate, so an ungated continuation
    // would have POSTed before this check.
    assert_eq!(
        ThreadGoalStatus::Active,
        goal_status(&mut harness.app, &harness.thread_id).await?
    );
    assert_eq!(
        harness.mock.requests().len(),
        2,
        "no continuation while the notified process runs"
    );

    std::fs::write(&release_file, "release")?;

    // AC3: a stalled runtime fails here waiting for the wake turn.
    let wake_started = await_turn_started(&mut harness.app).await?;
    assert_ne!(wake_started.turn.id, first_started.turn.id);
    let wake_completed = await_turn_completed(&mut harness.app).await?;
    assert_eq!(wake_completed.turn.id, wake_started.turn.id);
    assert_eq!(TurnStatus::Completed, wake_completed.turn.status);
    assert_eq!(None, wake_completed.turn.error);

    // AC2: exactly one wake request carries the fragment for the right
    // receipt, and the goal then makes progress.
    let requests = harness.mock.requests();
    assert_eq!(requests.len(), 4, "exactly one wake turn after release");
    assert!(
        requests[2].body_contains_text("source=\"exec_completion\""),
        "wake request should carry the completion fragment"
    );
    assert!(
        requests[2].body_contains_text(&format!("receipt_id: {receipt}")),
        "wake fragment should name the acknowledged receipt"
    );
    assert!(
        requests[2].body_contains_text("exit_code: 0"),
        "wake fragment should report the barrier exit"
    );
    assert_eq!(
        ThreadGoalStatus::Complete,
        goal_status(&mut harness.app, &harness.thread_id).await?
    );
    assert_eq!(
        harness.mock.requests().len(),
        4,
        "no further turns after goal completion"
    );
    Ok(())
}

/// AC6: user turns started while the wait holds are admitted immediately and
/// delivered exactly once each; the wake still follows the release.
///
/// If user input were gated on the receipt, the `turn/started` waits below
/// would time out: the barrier is released only after both user turns
/// complete.
#[tokio::test]
async fn user_burst_during_background_wait_admitted_without_loss_or_duplication() -> Result<()> {
    skip_if_remote!(
        Ok(()),
        "the file-barrier handshake needs a filesystem shared with the exec host"
    );
    skip_if_wine_exec!(
        Ok(()),
        "the barrier child needs a native shell on the exec host"
    );
    let barrier_dir = TempDir::new()?;
    let release_file = barrier_dir.path().join("release");
    let scripts = vec![
        exec_call_sse(
            "burst-r1",
            "burst-exec",
            &barrier_command(&release_file),
            /*notify_on_exit*/ true,
            /*yield_time_ms*/ 500,
        ),
        final_message_sse(
            "burst-r2",
            "burst-m2",
            "ending the turn; background work is running",
        ),
        final_message_sse("burst-r3", "burst-m3", "user one acknowledged"),
        final_message_sse("burst-r4", "burst-m4", "user two acknowledged"),
        update_goal_complete_sse("burst-r5", "burst-complete"),
        final_message_sse(
            "burst-r6",
            "burst-m6",
            "background work finished; goal complete",
        ),
    ];
    let mut harness = start_goal_harness(scripts, &[]).await?;

    await_turn_started(&mut harness.app).await?;
    let first_completed = await_turn_completed(&mut harness.app).await?;
    assert_eq!(TurnStatus::Completed, first_completed.turn.status);
    assert_eq!(harness.mock.requests().len(), 2);
    let ack = harness.mock.requests()[1]
        .function_call_output_text("burst-exec")
        .context("exec tool result should be attached")?;
    let receipt = parse_receipt_handle(&ack)?;

    for (text, expected_requests) in [("wait-user-one", 3_usize), ("wait-user-two", 4_usize)] {
        start_user_turn(&mut harness.app, &harness.thread_id, text).await?;
        let started = await_turn_started(&mut harness.app).await?;
        let completed = await_turn_completed(&mut harness.app).await?;
        assert_eq!(completed.turn.id, started.turn.id);
        assert_eq!(TurnStatus::Completed, completed.turn.status);
        let requests = harness.mock.requests();
        assert_eq!(requests.len(), expected_requests);
        let last = requests
            .last()
            .context("user turn should send one request")?;
        assert!(
            last.message_input_texts("user")
                .iter()
                .any(|input| input.contains(text)),
            "user turn request should carry its input"
        );
    }

    std::fs::write(&release_file, "release")?;
    await_turn_started(&mut harness.app).await?;
    let wake_completed = await_turn_completed(&mut harness.app).await?;
    assert_eq!(TurnStatus::Completed, wake_completed.turn.status);

    let requests = harness.mock.requests();
    assert_eq!(requests.len(), 6, "two user turns plus exactly one wake");
    assert!(
        requests[4].body_contains_text(&format!("receipt_id: {receipt}")),
        "wake fragment should name the acknowledged receipt"
    );
    assert_eq!(
        ThreadGoalStatus::Complete,
        goal_status(&mut harness.app, &harness.thread_id).await?
    );

    let request = harness
        .app
        .send_raw_request(
            "thread/read",
            Some(json!({"threadId": harness.thread_id, "includeTurns": true})),
        )
        .await?;
    let read: ThreadReadResponse =
        timeout(READ_TIMEOUT, harness.app.read_response(request)).await??;
    let texts: Vec<String> = read
        .thread
        .turns
        .iter()
        .flat_map(|turn| user_message_texts(&turn.items))
        .collect();
    assert_eq!(
        texts,
        vec!["wait-user-one".to_string(), "wait-user-two".to_string()]
    );
    Ok(())
}

/// AC5: a running process launched without `notify_on_exit` ("server" work
/// the runtime was never asked to watch) does not gate goal continuation.
///
/// The `always-subscribe` mutant proves it: if default launches armed
/// receipts, the continuation wait below times out.
#[tokio::test]
async fn unopted_server_process_does_not_gate_goal_continuation() -> Result<()> {
    skip_if_remote!(
        Ok(()),
        "the file-barrier handshake needs a filesystem shared with the exec host"
    );
    skip_if_wine_exec!(
        Ok(()),
        "the barrier child needs a native shell on the exec host"
    );
    let barrier_dir = TempDir::new()?;
    let release_file = barrier_dir.path().join("release");
    let scripts = vec![
        exec_call_sse(
            "server-r1",
            "server-exec",
            &barrier_command(&release_file),
            /*notify_on_exit*/ false,
            /*yield_time_ms*/ 500,
        ),
        final_message_sse(
            "server-r2",
            "server-m2",
            "ending the turn; server is running",
        ),
        update_goal_complete_sse("server-r3", "server-complete"),
        final_message_sse(
            "server-r4",
            "server-m4",
            "server work finished; goal complete",
        ),
    ];
    let mut harness = start_goal_harness(scripts, &[]).await?;

    await_turn_started(&mut harness.app).await?;
    let first_completed = await_turn_completed(&mut harness.app).await?;
    assert_eq!(TurnStatus::Completed, first_completed.turn.status);
    let requests = harness.mock.requests();
    assert_eq!(requests.len(), 2);
    let output = requests[1]
        .function_call_output_text("server-exec")
        .context("exec tool result should be attached")?;
    assert!(
        output.contains("Process running with session ID "),
        "server process should still run at yield: {output}"
    );
    assert!(
        !output.contains("receipt ID "),
        "unopted launch should arm no subscription: {output}"
    );

    // The continuation must arrive promptly even though the server child is
    // still wedged on the barrier; gating would time out here.
    await_turn_started(&mut harness.app).await?;
    let continued = await_turn_completed(&mut harness.app).await?;
    assert_eq!(TurnStatus::Completed, continued.turn.status);
    let requests = harness.mock.requests();
    assert_eq!(requests.len(), 4);
    assert!(
        !requests[2].body_contains_text("source=\"exec_completion\""),
        "unopted continuation carries no completion fragment"
    );
    assert_eq!(
        ThreadGoalStatus::Complete,
        goal_status(&mut harness.app, &harness.thread_id).await?
    );

    // Release the wedged child so no process outlives the test; with no
    // receipt there is nothing to wake for.
    std::fs::write(&release_file, "release")?;
    assert_eq!(
        ThreadGoalStatus::Complete,
        goal_status(&mut harness.app, &harness.thread_id).await?
    );
    assert_eq!(harness.mock.requests().len(), 4);
    Ok(())
}

/// AC4: on a headless host `notify_on_exit` is not advertised and no wake is
/// promised: the opt-in is refused before execution, `exec_notification` is
/// absent, and the goal continues ungated (the background-wait policy stays
/// inactive).
///
/// This test never executes a child process (the refusal precedes execution),
/// so it runs on every lane, including remote and Wine exec.
#[tokio::test]
async fn headless_host_refuses_notify_on_exit_and_promises_no_wake() -> Result<()> {
    let scripts = vec![
        exec_call_sse(
            "headless-r1",
            "headless-exec",
            "echo headless-probe",
            /*notify_on_exit*/ true,
            /*yield_time_ms*/ 500,
        ),
        final_message_sse("headless-r2", "headless-m2", "ending the turn"),
        update_goal_complete_sse("headless-r3", "headless-complete"),
        final_message_sse("headless-r4", "headless-m4", "goal complete"),
    ];
    let mut harness = start_goal_harness(scripts, &["--session-source", "exec"]).await?;

    await_turn_started(&mut harness.app).await?;
    let first_completed = await_turn_completed(&mut harness.app).await?;
    assert_eq!(TurnStatus::Completed, first_completed.turn.status);
    let requests = harness.mock.requests();
    assert_eq!(requests.len(), 2);
    let initial_body = requests[0].body_json();
    assert!(
        !tool_has_parameter(&initial_body, "exec_command", "notify_on_exit"),
        "headless host should not advertise notify_on_exit"
    );
    assert!(
        function_tool(&initial_body, "exec_notification").is_none(),
        "headless host should not advertise exec_notification"
    );
    let refusal = requests[1]
        .function_call_output_text("headless-exec")
        .context("exec tool result should be attached")?;
    assert!(
        refusal.contains("notify_on_exit is not supported on this host"),
        "opt-in should be refused on a headless host: {refusal}"
    );
    assert!(
        refusal.contains("no completion wake can be promised"),
        "refusal should promise no wake: {refusal}"
    );

    // The policy is inactive on an incapable host, so the goal continues
    // without waiting for anything.
    await_turn_started(&mut harness.app).await?;
    let continued = await_turn_completed(&mut harness.app).await?;
    assert_eq!(TurnStatus::Completed, continued.turn.status);
    let requests = harness.mock.requests();
    assert_eq!(requests.len(), 4);
    assert!(
        !requests[2].body_contains_text("source=\"exec_completion\""),
        "headless continuation carries no completion fragment"
    );
    assert_eq!(
        ThreadGoalStatus::Complete,
        goal_status(&mut harness.app, &harness.thread_id).await?
    );
    assert_eq!(harness.mock.requests().len(), 4);
    Ok(())
}
