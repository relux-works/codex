use super::*;
use crate::session::step_context::StepContext;
use crate::session::tests::make_session_and_context;
use crate::tools::context::ToolCallSource;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolExecutor;
use crate::turn_diff_tracker::TurnDiffTracker;
use crate::unified_exec::UnifiedExecContext;
use crate::unified_exec::completion_receipt::InitialResponseDecision;
use crate::unified_exec::completion_receipt::ReceiptOwner;
use crate::unified_exec::completion_receipt::TerminalCompletion;
use codex_extension_api::AsyncNotificationSupport;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tokio::sync::Mutex;

async fn invocation_with_session(
    call_id: &str,
    arguments: serde_json::Value,
) -> (ToolInvocation, Arc<crate::session::session::Session>) {
    let (session, turn) = make_session_and_context().await;
    let turn = Arc::new(turn);
    let session = Arc::new(session);
    session
        .services
        .thread_extension_data
        .insert(AsyncNotificationSupport::Available);
    let invocation = ToolInvocation {
        session: Arc::clone(&session),
        step_context: StepContext::for_test(Arc::clone(&turn)),
        turn,
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        tracker: Arc::new(Mutex::new(TurnDiffTracker::new())),
        call_id: call_id.to_string(),
        tool_name: codex_tools::ToolName::plain("exec_notification"),
        source: ToolCallSource::Direct,
        payload: ToolPayload::Function {
            arguments: arguments.to_string(),
        },
    };
    (invocation, session)
}

fn launch_owner(
    session: &Arc<crate::session::session::Session>,
    invocation: &ToolInvocation,
    call_id: &str,
) -> ReceiptOwner {
    let context = UnifiedExecContext::new(
        Arc::clone(session),
        Arc::clone(&invocation.step_context),
        invocation.cancellation_token.clone(),
        call_id.to_string(),
    );
    session
        .services
        .unified_exec_manager
        .receipt_owner_for(&context)
        .expect("launch owner should build")
}

/// Reserves, arms, publishes, and retains output: exactly what an opted-in
/// launch plus a watched exit leaves behind.
async fn exited_subscription(
    session: &Arc<crate::session::session::Session>,
    invocation: &ToolInvocation,
    output: &[u8],
    omitted_bytes: usize,
) -> String {
    let manager = &session.services.unified_exec_manager;
    let owner = launch_owner(session, invocation, "launch-call");
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), /*process_id*/ 4242)
        .await
        .expect("reservation should succeed");
    manager
        .receipt_store()
        .resolve_initial_response(receipt_id, &owner, InitialResponseDecision::Arm)
        .expect("arm should succeed");
    manager
        .receipt_store()
        .publish_exit(
            receipt_id,
            &owner,
            TerminalCompletion {
                exit_code: Some(0),
                timed_out: false,
            },
        )
        .expect("publish should succeed");
    manager
        .watcher_receipt_hook(receipt_id, owner.clone())
        .hooks
        .lock()
        .await
        .retention
        .insert_pending(receipt_id, owner, output.to_vec(), omitted_bytes);
    receipt_id.model_handle()
}

/// Drives the handler on a fresh session and returns the refusal message.
/// Only for rejections that need no stored subscription (unknown handles,
/// malformed input).
async fn read_rejection_text(arguments: serde_json::Value) -> String {
    let (invocation, _) = invocation_with_session("read-call", arguments.clone()).await;
    let Err(error) = ExecNotificationHandler.handle(invocation).await else {
        panic!("expected a refusal for {arguments}");
    };
    let FunctionCallError::RespondToModel(message) = error else {
        panic!("expected a model-facing refusal, got {error:?}");
    };
    message
}

#[tokio::test]
async fn read_returns_retained_terminal_output() {
    let (invocation, session) = invocation_with_session("read-call", serde_json::json!({})).await;
    let handle = exited_subscription(&session, &invocation, b"hello terminal", 0).await;
    let (invocation, _) = invocation_with_session(
        "read-call-2",
        serde_json::json!({ "action": "read", "receipt_id": handle }),
    )
    .await;
    // Rebind to the session holding the subscription.
    let invocation = ToolInvocation {
        session,
        ..invocation
    };
    let output = ExecNotificationHandler
        .handle(invocation)
        .await
        .expect("read should succeed");
    let text = output.log_output();
    assert!(
        text.contains("hello terminal"),
        "read should return retained output: {text}"
    );
    assert!(
        text.contains(&handle),
        "read should name the receipt: {text}"
    );
    assert!(
        !text.contains("truncated") && !text.contains("omitted"),
        "untruncated output should not claim truncation: {text}"
    );
}

#[tokio::test]
async fn read_indicates_truncation_and_stays_under_token_bound() {
    let (invocation, session) = invocation_with_session("read-call", serde_json::json!({})).await;
    let output = vec![b'x'; 100_000];
    let handle = exited_subscription(&session, &invocation, &output, 12_345).await;
    for max_output_tokens in [None, Some(1_000_000)] {
        let mut arguments = serde_json::json!({ "action": "read", "receipt_id": handle });
        if let Some(max) = max_output_tokens {
            arguments["max_output_tokens"] = serde_json::json!(max);
        }
        let (invocation, _) = invocation_with_session("read-call-budget", arguments).await;
        let invocation = ToolInvocation {
            session: Arc::clone(&session),
            ..invocation
        };
        let text = ExecNotificationHandler
            .handle(invocation)
            .await
            .expect("read should succeed")
            .log_output();
        assert!(
            text.contains("12345 bytes omitted to fit the retention cap"),
            "retention truncation should be indicated: {}",
            text.lines().next().unwrap_or_default()
        );
        assert!(
            text.contains("truncated output"),
            "read truncation should be indicated"
        );
        assert!(
            codex_utils_output_truncation::approx_token_count(&text) < 10_000,
            "read response must stay under 10K tokens"
        );
    }
}

#[tokio::test]
async fn read_rejects_unknown_receipt() {
    let text = read_rejection_text(serde_json::json!({
        "action": "read",
        "receipt_id": "01234567-89ab-cdef-0123-456789abcdef",
    }))
    .await;
    assert!(text.contains("unknown or expired"), "got: {text}");
}

#[tokio::test]
async fn read_rejects_malformed_receipt_handle() {
    let text = read_rejection_text(serde_json::json!({
        "action": "read",
        "receipt_id": "not-a-receipt",
    }))
    .await;
    assert!(text.contains("unknown or expired"), "got: {text}");
}

#[tokio::test]
async fn read_rejects_foreign_receipt() {
    let (_, session) = invocation_with_session("read-call", serde_json::json!({})).await;
    let manager = &session.services.unified_exec_manager;
    let foreign_owner = ReceiptOwner::new(
        codex_protocol::ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_00c3),
        /*runtime_generation*/ 999_999,
        "foreign-call",
    )
    .expect("foreign owner should build");
    let receipt_id = manager
        .reserve_completion_receipt(foreign_owner, /*process_id*/ 4343)
        .await
        .expect("reservation should succeed");
    let handle = receipt_id.model_handle();
    let (invocation, _) = invocation_with_session(
        "read-call-foreign",
        serde_json::json!({ "action": "read", "receipt_id": handle }),
    )
    .await;
    let invocation = ToolInvocation {
        session,
        ..invocation
    };
    let Err(error) = ExecNotificationHandler.handle(invocation).await else {
        panic!("foreign receipt read should be rejected");
    };
    let FunctionCallError::RespondToModel(message) = error else {
        panic!("expected a model-facing refusal, got {error:?}");
    };
    assert!(
        message.contains("different thread or runtime generation"),
        "unexpected refusal: {message}"
    );
}

#[tokio::test]
async fn read_rejects_stale_receipt_after_release() {
    let (invocation, session) = invocation_with_session("setup-call", serde_json::json!({})).await;
    let handle = exited_subscription(&session, &invocation, b"gone", 0).await;
    let (invocation, _) = invocation_with_session(
        "release-call",
        serde_json::json!({ "action": "release", "receipt_id": handle }),
    )
    .await;
    ExecNotificationHandler
        .handle(ToolInvocation {
            session: Arc::clone(&session),
            ..invocation
        })
        .await
        .expect("release should succeed");
    let (invocation, _) = invocation_with_session(
        "read-call-stale",
        serde_json::json!({ "action": "read", "receipt_id": handle }),
    )
    .await;
    let Err(error) = ExecNotificationHandler
        .handle(ToolInvocation {
            session,
            ..invocation
        })
        .await
    else {
        panic!("read after release should be rejected");
    };
    let FunctionCallError::RespondToModel(message) = error else {
        panic!("expected a model-facing refusal, got {error:?}");
    };
    assert!(
        message.contains("unknown or expired"),
        "released receipt should read as expired: {message}"
    );
}

#[tokio::test]
async fn read_before_exit_is_rejected_without_waiting() {
    let (invocation, session) = invocation_with_session("setup-call", serde_json::json!({})).await;
    let manager = &session.services.unified_exec_manager;
    let owner = launch_owner(&session, &invocation, "launch-call");
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), /*process_id*/ 4444)
        .await
        .expect("reservation should succeed");
    manager
        .receipt_store()
        .resolve_initial_response(receipt_id, &owner, InitialResponseDecision::Arm)
        .expect("arm should succeed");
    let handle = receipt_id.model_handle();
    let (invocation, _) = invocation_with_session(
        "read-call-early",
        serde_json::json!({ "action": "read", "receipt_id": handle }),
    )
    .await;
    let Err(error) = ExecNotificationHandler
        .handle(ToolInvocation {
            session,
            ..invocation
        })
        .await
    else {
        panic!("read before exit should be rejected");
    };
    let FunctionCallError::RespondToModel(message) = error else {
        panic!("expected a model-facing refusal, got {error:?}");
    };
    assert!(
        message.contains("has not exited yet") && message.contains("never waits"),
        "unexpected refusal: {message}"
    );
}

#[tokio::test]
async fn release_disarms_frees_slot_and_cancels_pending_wake() {
    let (invocation, session) = invocation_with_session("setup-call", serde_json::json!({})).await;
    let manager = &session.services.unified_exec_manager;
    let handle = exited_subscription(&session, &invocation, b"bye", 0).await;
    assert_eq!(
        manager
            .receipt_capacity_used()
            .await
            .expect("capacity should read"),
        1
    );
    // Stage a pending wake as the watcher would, then release before it fires.
    let owner = launch_owner(&session, &invocation, "launch-call");
    let receipt_id = crate::unified_exec::completion_receipt::ReceiptId::from_model_handle(&handle)
        .expect("handle should parse");
    session
        .input_queue
        .enqueue_runtime_notification(
            receipt_id,
            owner,
            crate::context::ExecCompletion {
                process_id: 4242,
                exit_code: Some(0),
                timed_out: false,
                failure: None,
                retention: crate::context::ExecOutputRetention::Absent,
            },
        )
        .await;
    assert!(session.input_queue.has_pending_mailbox_items().await);

    let (invocation, _) = invocation_with_session(
        "release-call",
        serde_json::json!({ "action": "release", "receipt_id": handle }),
    )
    .await;
    let text = ExecNotificationHandler
        .handle(ToolInvocation {
            session: Arc::clone(&session),
            ..invocation
        })
        .await
        .expect("release should succeed")
        .log_output();
    assert!(
        text.contains("disarmed") && text.contains("not terminated"),
        "release should promise disarm-without-kill: {text}"
    );
    assert_eq!(
        manager
            .receipt_capacity_used()
            .await
            .expect("capacity should read"),
        0,
        "release should free the slot"
    );
    assert!(
        !session.input_queue.has_pending_mailbox_items().await,
        "release should cancel the pending wake"
    );
}

#[tokio::test]
async fn release_rejects_unknown_receipt() {
    let (invocation, _) = invocation_with_session(
        "release-call",
        serde_json::json!({
            "action": "release",
            "receipt_id": "01234567-89ab-cdef-0123-456789abcdef",
        }),
    )
    .await;
    let Err(error) = ExecNotificationHandler.handle(invocation).await else {
        panic!("unknown receipt release should be rejected");
    };
    let FunctionCallError::RespondToModel(message) = error else {
        panic!("expected a model-facing refusal, got {error:?}");
    };
    assert!(message.contains("unknown or expired"), "got: {message}");
}

#[tokio::test]
async fn unknown_action_is_rejected() {
    let (invocation, session) = invocation_with_session("setup-call", serde_json::json!({})).await;
    let handle = exited_subscription(&session, &invocation, b"data", 0).await;
    let (invocation, _) = invocation_with_session(
        "bogus-call",
        serde_json::json!({ "action": "poll", "receipt_id": handle }),
    )
    .await;
    let Err(error) = ExecNotificationHandler
        .handle(ToolInvocation {
            session,
            ..invocation
        })
        .await
    else {
        panic!("unknown action should be rejected");
    };
    let FunctionCallError::RespondToModel(message) = error else {
        panic!("expected a model-facing refusal, got {error:?}");
    };
    assert!(
        message.contains("unknown exec_notification action"),
        "unexpected refusal: {message}"
    );
}

#[tokio::test]
async fn handler_refused_on_unavailable_host() {
    let (session, turn) = make_session_and_context().await;
    let turn = Arc::new(turn);
    let session = Arc::new(session);
    // No capability marker: nothing could have been acknowledged here.
    let invocation = ToolInvocation {
        session,
        step_context: StepContext::for_test(Arc::clone(&turn)),
        turn,
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        tracker: Arc::new(Mutex::new(TurnDiffTracker::new())),
        call_id: "notify-unavailable".to_string(),
        tool_name: codex_tools::ToolName::plain("exec_notification"),
        source: ToolCallSource::Direct,
        payload: ToolPayload::Function {
            arguments: serde_json::json!({
                "action": "read",
                "receipt_id": "01234567-89ab-cdef-0123-456789abcdef",
            })
            .to_string(),
        },
    };
    let Err(error) = ExecNotificationHandler.handle(invocation).await else {
        panic!("handler should be refused on an unavailable host");
    };
    let FunctionCallError::RespondToModel(message) = error else {
        panic!("expected a model-facing refusal, got {error:?}");
    };
    assert!(
        message.contains("not supported on this host"),
        "unexpected refusal: {message}"
    );
}

#[test]
fn receipt_error_map_covers_stale_states() {
    assert!(
        receipt_error_message(ReceiptError::Retired).contains("retired"),
        "retired output should be named"
    );
    assert!(
        receipt_error_message(ReceiptError::Cancelled {
            reason: CancellationReason::Released
        })
        .contains("already released"),
        "released receipts should be named"
    );
    assert!(
        receipt_error_message(ReceiptError::InvalidTransition {
            actual: ReceiptStatus::Armed
        })
        .contains("has not exited yet"),
        "armed receipts have no terminal output yet"
    );
    assert!(
        receipt_error_message(ReceiptError::InvalidTransition {
            actual: ReceiptStatus::InlineResult
        })
        .contains("no subscription was armed"),
        "inline results armed nothing"
    );
}
