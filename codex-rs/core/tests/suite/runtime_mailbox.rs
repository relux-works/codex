use anyhow::Context;
use codex_core::TurnInputRequest;
use codex_core::TurnStartOptions;
use codex_login::CodexAuth;
use codex_protocol::protocol::EventMsg;
use codex_protocol::turn_input::CyberAccessProgram;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::Value;

async fn wait_for_turn_complete(codex: &codex_core::CodexThread) -> String {
    let event = wait_for_event(codex, |event| matches!(event, EventMsg::TurnComplete(_))).await;
    let EventMsg::TurnComplete(completed) = event else {
        unreachable!("wait predicate only accepts turn-complete events");
    };
    completed.turn_id
}

fn turn_trigger_of(request: &core_test_support::responses::ResponsesRequest) -> Option<String> {
    let raw = request.header("x-codex-turn-metadata")?;
    let metadata: Value = serde_json::from_str(&raw).ok()?;
    metadata.get("turn_trigger")?.as_str().map(str::to_string)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_runtime_entry_starts_one_wake_turn_with_exec_completion() -> anyhow::Result<()> {
    let server = responses::start_mock_server().await;
    let requests = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse_completed("resp-initial"),
            responses::sse_completed("resp-wake"),
        ],
    )
    .await;
    let codex = test_codex()
        .with_model("gpt-5.4")
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .build_with_auto_env(&server)
        .await?
        .codex;

    codex
        .start_or_steer_turn(
            TurnInputRequest::user_input(vec![UserInput::Text {
                text: "run a background job".to_string(),
                text_elements: Vec::new(),
            }])
            .on_start(TurnStartOptions {
                cyber_access_program: Some(CyberAccessProgram::Standard),
                ..Default::default()
            }),
        )
        .await?;
    let initial_turn_id = wait_for_turn_complete(&codex).await;

    assert!(
        codex.test_enqueue_exec_completion_notification().await,
        "runtime notification should enqueue"
    );
    let wake_turn_id = wait_for_turn_complete(&codex).await;

    let captured = requests.requests();
    assert_eq!(captured.len(), 2);
    // The wake turn preserves the thread's execution settings.
    for request in &captured {
        assert_eq!(
            request.body_json()["access_programs"],
            serde_json::json!({"cyber": "standard"}),
        );
    }
    responses::assert_root_turn(&captured[0].body_json(), Some(&initial_turn_id))?;
    responses::assert_parent_turn(&captured[0].body_json(), /*expected*/ None)?;
    responses::assert_root_turn(&captured[1].body_json(), Some(&wake_turn_id))?;
    // No parent lineage and no initiating agent: the wake invents neither.
    responses::assert_parent_turn(&captured[1].body_json(), /*expected*/ None)?;
    // The wake turn carries the exec_completion trigger and no inter-agent
    // author: it is a named internal wake, never fake agent mail.
    assert_eq!(
        turn_trigger_of(&captured[1]).as_deref(),
        Some("exec_completion")
    );
    assert!(
        captured[1].inputs_of_type("agent_message").is_empty(),
        "runtime wake must not fabricate agent mail"
    );
    // The wake adds exactly one contextual fragment and no real user
    // message: it resets no human quota.
    let initial_user_texts = captured[0].message_input_texts("user");
    let wake_user_texts = captured[1].message_input_texts("user");
    assert_eq!(wake_user_texts.len(), initial_user_texts.len() + 1);
    assert_eq!(
        wake_user_texts[..initial_user_texts.len()],
        initial_user_texts
    );
    assert!(
        wake_user_texts
            .last()
            .is_some_and(|fragment| fragment.contains("source=\"exec_completion\""))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_runtime_entries_still_start_one_wake_turn() -> anyhow::Result<()> {
    let server = responses::start_mock_server().await;
    let requests = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse_completed("resp-initial"),
            responses::sse_completed("resp-wake"),
        ],
    )
    .await;
    let codex = test_codex()
        .with_model("gpt-5.4")
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .build_with_auto_env(&server)
        .await?
        .codex;

    // Stage both entries before any turn starts: no wake can fire mid-batch
    // while the session has no turn lifecycle running, and the busy initial
    // turn gates every wake until it ends — so the post-turn idle wake leases
    // both deterministically. (Enqueueing after the initial turn raced the
    // trailing maybe_start across the TurnComplete-to-idle window and flaked
    // with the first wake leasing only one entry.)
    assert!(
        codex
            .test_enqueue_exec_completion_notifications_without_wake(2)
            .await,
        "both runtime notifications should enqueue"
    );

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "run two background jobs".to_string(),
            text_elements: Vec::new(),
        }]))
        .await?;
    wait_for_turn_complete(&codex).await;
    wait_for_turn_complete(&codex).await;

    // Both entries lease into the same wake turn; no second wake follows.
    // The two-item sequence would fail a third sampling request.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let captured = requests.requests();
    assert_eq!(captured.len(), 2);
    assert_eq!(
        turn_trigger_of(&captured[1]).as_deref(),
        Some("exec_completion")
    );
    assert!(
        captured[1].inputs_of_type("agent_message").is_empty(),
        "runtime wake must not fabricate agent mail"
    );
    assert_eq!(
        captured[1]
            .message_input_texts("user")
            .iter()
            .filter(|text| text.contains("source=\"exec_completion\""))
            .count(),
        2,
        "one wake records both leased completions"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_only_inter_agent_mail_still_never_starts_a_turn_alone() -> anyhow::Result<()> {
    let server = responses::start_mock_server().await;
    let requests =
        responses::mount_sse_sequence(&server, vec![responses::sse_completed("resp-1")]).await;
    let codex = test_codex().build_with_auto_env(&server).await?.codex;

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "hello".to_string(),
            text_elements: Vec::new(),
        }]))
        .await?;
    wait_for_turn_complete(&codex).await;
    assert_eq!(requests.requests().len(), 1);

    codex
        .submit(codex_protocol::protocol::Op::InterAgentCommunication {
            communication: codex_protocol::protocol::InterAgentCommunication::new(
                codex_protocol::AgentPath::try_from("/root/worker").expect("worker path"),
                codex_protocol::AgentPath::root(),
                Vec::new(),
                "queued child update".to_string(),
                /*trigger_turn*/ false,
            ),
            start_options: Default::default(),
        })
        .await
        .context("submit queue-only mail")?;
    // Barrier so the mail is enqueued before asserting no wake follows.
    codex
        .submit(codex_protocol::protocol::Op::RealtimeConversationListVoices)
        .await?;
    wait_for_event(codex.as_ref(), |event| {
        matches!(event, EventMsg::RealtimeConversationListVoicesResponse(_))
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        requests.requests().len(),
        1,
        "queue-only mail must not start a turn without durable sleep"
    );

    Ok(())
}
