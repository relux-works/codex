use anyhow::Context;
use anyhow::Result;
use codex_core::TurnInputRequest;
use codex_extension_api::AsyncNotificationSupport;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Settings;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::skip_if_sandbox;
use core_test_support::skip_if_target_windows;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::test_codex::turn_permission_fields;
use core_test_support::wait_for_event;
use core_test_support::wait_for_event_match;
use pretty_assertions::assert_eq;
use serde_json::json;

const EXEC_COMPLETION_WRAPPER: &str = "source=\"exec_completion\"";
const ARMED_RECEIPT_PREFIX: &str = "Completion notification armed with receipt ID ";
const SESSION_ID_PREFIX: &str = "Process running with session ID ";

async fn submit_exec_turn(test: &TestCodex, prompt: &str) -> Result<()> {
    let session_model = test.session_configured.model.clone();
    let (sandbox_policy, permission_profile) =
        turn_permission_fields(PermissionProfile::Disabled, test.config.cwd.as_path());

    test.codex
        .start_or_steer_turn(
            TurnInputRequest::user_input(vec![UserInput::Text {
                text: prompt.into(),
                text_elements: Vec::new(),
            }])
            .with_thread_settings(ThreadSettingsOverrides {
                approval_policy: Some(AskForApproval::Never),
                sandbox_policy: Some(sandbox_policy),
                permission_profile,
                collaboration_mode: Some(CollaborationMode {
                    mode: ModeKind::Default,
                    settings: Settings {
                        model: session_model,
                        reasoning_effort: None,
                        developer_instructions: None,
                    },
                }),
                ..Default::default()
            }),
        )
        .await?;
    Ok(())
}

async fn wait_for_turn_complete(test: &TestCodex) {
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
}

async fn wait_for_function_output(test: &TestCodex, call_id: &str) -> String {
    wait_for_event_match(&test.codex, |event| match event {
        EventMsg::RawResponseItem(raw) => match &raw.item {
            ResponseItem::FunctionCallOutput {
                call_id: Some(output_call_id),
                output,
                ..
            } if output_call_id == call_id => output.text_content().map(str::to_string),
            _ => None,
        },
        _ => None,
    })
    .await
}

fn armed_receipt_handle(output: &str) -> Option<String> {
    output.lines().find_map(|line| {
        line.strip_prefix(ARMED_RECEIPT_PREFIX)
            .and_then(|rest| rest.split(';').next())
            .map(|handle| handle.trim().to_string())
    })
}

fn running_session_id(output: &str) -> Option<i64> {
    output
        .lines()
        .find_map(|line| line.strip_prefix(SESSION_ID_PREFIX))
        .and_then(|id| id.trim().parse::<i64>().ok())
}

/// Barrier proving the session has settled before asserting silence.
async fn settle_session(test: &TestCodex) {
    test.codex
        .submit(Op::RealtimeConversationListVoices)
        .await
        .expect("submit settle barrier");
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::RealtimeConversationListVoicesResponse(_))
    })
    .await;
}

fn seed_available_host(test: &TestCodex) {
    test.codex
        .thread_extension_data()
        .insert(AsyncNotificationSupport::Available);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_notification_opt_in_arms_and_read_returns_terminal_output() -> Result<()> {
    skip_if_target_windows!(Ok(()), "uses POSIX sleep/echo");
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));

    let server = start_mock_server().await;
    let mut builder = test_codex().with_model("gpt-5.2");
    let test = builder.build_with_auto_env(&server).await?;
    seed_available_host(&test);

    let launch_args = json!({
        "cmd": "sleep 2; echo done-mark".to_string(),
        "yield_time_ms": 250,
        "notify_on_exit": true,
    });
    let mock = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call(
                    "call-launch",
                    "exec_command",
                    &serde_json::to_string(&launch_args)?,
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![
                ev_response_created("resp-2"),
                ev_assistant_message("msg-1", "launched"),
                ev_completed("resp-2"),
            ]),
            // Idle wake for the opted-in exit.
            sse(vec![
                ev_response_created("resp-3"),
                ev_assistant_message("msg-2", "noted the completion"),
                ev_completed("resp-3"),
            ]),
        ],
    )
    .await;

    submit_exec_turn(&test, "launch with notification").await?;
    let launch_output = wait_for_function_output(&test, "call-launch").await;
    let handle = armed_receipt_handle(&launch_output)
        .with_context(|| format!("launch should acknowledge a receipt: {launch_output}"))?;
    wait_for_turn_complete(&test).await;

    // The exit (~2s) wakes an idle turn before the explicit read below.
    wait_for_turn_complete(&test).await;
    let requests = mock.requests();
    assert_eq!(requests.len(), 3, "launch, final, then the wake");
    assert!(
        requests[2]
            .body_json()
            .to_string()
            .contains(EXEC_COMPLETION_WRAPPER),
        "wake request should carry the completion fragment"
    );

    // The read call needs the acknowledged handle, so it mounts after the
    // launch turn revealed it.
    let read_args = json!({ "action": "read", "receipt_id": handle });
    mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-6"),
                ev_function_call(
                    "call-read",
                    "exec_notification",
                    &serde_json::to_string(&read_args)?,
                ),
                ev_completed("resp-6"),
            ]),
            sse(vec![
                ev_response_created("resp-7"),
                ev_assistant_message("msg-4", "read it"),
                ev_completed("resp-7"),
            ]),
        ],
    )
    .await;
    submit_exec_turn(&test, "read the retained output").await?;
    let read_output = wait_for_function_output(&test, "call-read").await;
    assert!(
        read_output.contains("done-mark"),
        "read should return terminal output: {read_output}"
    );
    wait_for_turn_complete(&test).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_notification_default_launch_arms_and_wakes_nothing() -> Result<()> {
    skip_if_target_windows!(Ok(()), "uses POSIX sleep");
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));

    let server = start_mock_server().await;
    let mut builder = test_codex().with_model("gpt-5.2");
    let test = builder.build_with_auto_env(&server).await?;
    seed_available_host(&test);

    let launch_args = json!({
        "cmd": "sleep 2".to_string(),
        "yield_time_ms": 250,
    });
    let mock = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call(
                    "call-launch",
                    "exec_command",
                    &serde_json::to_string(&launch_args)?,
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![
                ev_response_created("resp-2"),
                ev_assistant_message("msg-1", "launched"),
                ev_completed("resp-2"),
            ]),
        ],
    )
    .await;

    submit_exec_turn(&test, "launch without notification").await?;
    let launch_output = wait_for_function_output(&test, "call-launch").await;
    assert!(
        !launch_output.contains(ARMED_RECEIPT_PREFIX),
        "default launch should acknowledge no receipt: {launch_output}"
    );
    wait_for_turn_complete(&test).await;

    // The exit produces no wake: after the terminal event and a settle
    // barrier, no further sampling request exists.
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::ExecCommandEnd(_))
    })
    .await;
    settle_session(&test).await;
    assert_eq!(
        mock.requests().len(),
        2,
        "default launch should wake nothing after exit"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_notification_opt_in_refused_on_headless_exec() -> Result<()> {
    skip_if_target_windows!(Ok(()), "uses POSIX echo");
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));

    let server = start_mock_server().await;
    let mut builder = test_codex().with_model("gpt-5.2");
    let test = builder.build_with_auto_env(&server).await?;
    // No capability seeded: the test host reports SessionSource::Exec, which
    // seeds Unavailable, exactly like headless exec.

    let launch_args = json!({
        "cmd": "echo hi".to_string(),
        "yield_time_ms": 250,
        "notify_on_exit": true,
    });
    let mock = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call(
                    "call-launch",
                    "exec_command",
                    &serde_json::to_string(&launch_args)?,
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![
                ev_response_created("resp-2"),
                ev_assistant_message("msg-1", "refused"),
                ev_completed("resp-2"),
            ]),
        ],
    )
    .await;

    submit_exec_turn(&test, "opt in on a headless host").await?;
    let launch_output = wait_for_function_output(&test, "call-launch").await;
    assert!(
        launch_output.contains("notify_on_exit is not supported on this host"),
        "opt-in should be refused before execution: {launch_output}"
    );
    wait_for_turn_complete(&test).await;

    let request = mock.requests().into_iter().next().context("one request")?;
    let tools = request
        .body_json()
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        !tools
            .iter()
            .any(|tool| tool.get("name").and_then(serde_json::Value::as_str)
                == Some("exec_notification")),
        "headless host should not advertise exec_notification"
    );
    let exec_command = tools
        .iter()
        .find(|tool| tool.get("name").and_then(serde_json::Value::as_str) == Some("exec_command"))
        .context("exec_command should be advertised")?;
    assert!(
        exec_command
            .pointer("/parameters/properties/notify_on_exit")
            .is_none(),
        "headless host schema should omit notify_on_exit"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_notification_capacity_refuses_65th_and_default_still_runs() -> Result<()> {
    skip_if_target_windows!(Ok(()), "uses POSIX echo");
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));

    let server = start_mock_server().await;
    let mut builder = test_codex().with_model("gpt-5.2");
    let test = builder.build_with_auto_env(&server).await?;
    seed_available_host(&test);

    for _ in 0..64 {
        assert!(
            test.codex.test_arm_exec_receipt_for_background_wait().await,
            "first 64 reservations should succeed"
        );
    }
    assert!(
        !test.codex.test_arm_exec_receipt_for_background_wait().await,
        "65th direct reservation should fail"
    );

    let refused_args = json!({
        "cmd": "echo refused".to_string(),
        "yield_time_ms": 250,
        "notify_on_exit": true,
    });
    mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call(
                    "call-refused",
                    "exec_command",
                    &serde_json::to_string(&refused_args)?,
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![
                ev_response_created("resp-2"),
                ev_assistant_message("msg-1", "refused"),
                ev_completed("resp-2"),
            ]),
        ],
    )
    .await;
    submit_exec_turn(&test, "opt in past capacity").await?;
    let refused_output = wait_for_function_output(&test, "call-refused").await;
    assert!(
        refused_output.contains("completion notification capacity is full"),
        "65th opted-in launch should be refused: {refused_output}"
    );
    wait_for_turn_complete(&test).await;

    // The process cap is independent: a default launch still runs while
    // receipt slots are full.
    let default_args = json!({
        "cmd": "echo still-runs".to_string(),
        "yield_time_ms": 5_000,
    });
    mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-3"),
                ev_function_call(
                    "call-default",
                    "exec_command",
                    &serde_json::to_string(&default_args)?,
                ),
                ev_completed("resp-3"),
            ]),
            sse(vec![
                ev_response_created("resp-4"),
                ev_assistant_message("msg-2", "ran"),
                ev_completed("resp-4"),
            ]),
        ],
    )
    .await;
    submit_exec_turn(&test, "run a default launch").await?;
    let default_output = wait_for_function_output(&test, "call-default").await;
    assert!(
        default_output.contains("still-runs"),
        "default launch should run while receipts are full: {default_output}"
    );
    wait_for_turn_complete(&test).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_notification_release_disarms_without_killing() -> Result<()> {
    skip_if_target_windows!(Ok(()), "uses POSIX sleep");
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));

    let server = start_mock_server().await;
    let mut builder = test_codex().with_model("gpt-5.2");
    let test = builder.build_with_auto_env(&server).await?;
    seed_available_host(&test);

    let launch_args = json!({
        "cmd": "sleep 5".to_string(),
        "yield_time_ms": 250,
        "notify_on_exit": true,
    });
    mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call(
                    "call-launch",
                    "exec_command",
                    &serde_json::to_string(&launch_args)?,
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![
                ev_response_created("resp-2"),
                ev_assistant_message("msg-1", "launched"),
                ev_completed("resp-2"),
            ]),
        ],
    )
    .await;
    submit_exec_turn(&test, "launch with notification").await?;
    let launch_output = wait_for_function_output(&test, "call-launch").await;
    let handle = armed_receipt_handle(&launch_output)
        .with_context(|| format!("launch should acknowledge a receipt: {launch_output}"))?;
    let session_id = running_session_id(&launch_output)
        .with_context(|| format!("launch should report a session: {launch_output}"))?;
    wait_for_turn_complete(&test).await;

    let release_args = json!({ "action": "release", "receipt_id": handle });
    mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-3"),
                ev_function_call(
                    "call-release",
                    "exec_notification",
                    &serde_json::to_string(&release_args)?,
                ),
                ev_completed("resp-3"),
            ]),
            sse(vec![
                ev_response_created("resp-4"),
                ev_assistant_message("msg-2", "released"),
                ev_completed("resp-4"),
            ]),
        ],
    )
    .await;
    submit_exec_turn(&test, "release the subscription").await?;
    let release_output = wait_for_function_output(&test, "call-release").await;
    assert!(
        release_output.contains("disarmed") && release_output.contains("not terminated"),
        "release should disarm without killing: {release_output}"
    );
    wait_for_turn_complete(&test).await;

    // The process survived release: an empty poll still reaches the live
    // session.
    let poll_args = json!({ "session_id": session_id, "yield_time_ms": 500 });
    mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-5"),
                ev_function_call(
                    "call-poll",
                    "write_stdin",
                    &serde_json::to_string(&poll_args)?,
                ),
                ev_completed("resp-5"),
            ]),
            sse(vec![
                ev_response_created("resp-6"),
                ev_assistant_message("msg-3", "polled"),
                ev_completed("resp-6"),
            ]),
        ],
    )
    .await;
    submit_exec_turn(&test, "poll the live session").await?;
    let poll_output = wait_for_function_output(&test, "call-poll").await;
    assert!(
        poll_output.contains(&format!("session ID {session_id}")),
        "live session should still answer after release: {poll_output}"
    );
    wait_for_turn_complete(&test).await;

    // The released receipt is stale: a later read is rejected.
    let stale_args = json!({ "action": "read", "receipt_id": handle });
    let stale_mock = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-7"),
                ev_function_call(
                    "call-stale",
                    "exec_notification",
                    &serde_json::to_string(&stale_args)?,
                ),
                ev_completed("resp-7"),
            ]),
            sse(vec![
                ev_response_created("resp-8"),
                ev_assistant_message("msg-4", "stale"),
                ev_completed("resp-8"),
            ]),
        ],
    )
    .await;
    submit_exec_turn(&test, "read after release").await?;
    let stale_output = wait_for_function_output(&test, "call-stale").await;
    assert!(
        stale_output.contains("unknown or expired"),
        "released receipt should read as expired: {stale_output}"
    );
    wait_for_turn_complete(&test).await;

    // The later exit produces no wake.
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::ExecCommandEnd(_))
    })
    .await;
    settle_session(&test).await;
    assert_eq!(
        stale_mock.requests().len(),
        2,
        "no wake should follow the released exit"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_notification_unknown_receipt_rejected() -> Result<()> {
    skip_if_target_windows!(Ok(()), "uses POSIX echo");
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));

    let server = start_mock_server().await;
    let mut builder = test_codex().with_model("gpt-5.2");
    let test = builder.build_with_auto_env(&server).await?;
    seed_available_host(&test);

    let read_args = json!({
        "action": "read",
        "receipt_id": "01234567-89ab-cdef-0123-456789abcdef",
    });
    mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call(
                    "call-read",
                    "exec_notification",
                    &serde_json::to_string(&read_args)?,
                ),
                ev_completed("resp-1"),
            ]),
            sse(vec![
                ev_response_created("resp-2"),
                ev_assistant_message("msg-1", "rejected"),
                ev_completed("resp-2"),
            ]),
        ],
    )
    .await;
    submit_exec_turn(&test, "read an unknown receipt").await?;
    let read_output = wait_for_function_output(&test, "call-read").await;
    assert!(
        read_output.contains("unknown or expired"),
        "unknown receipt should be rejected: {read_output}"
    );
    wait_for_turn_complete(&test).await;
    Ok(())
}
