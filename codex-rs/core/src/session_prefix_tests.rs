use codex_protocol::AgentPath;
use codex_protocol::protocol::AgentStatus;
use codex_utils_output_truncation::approx_token_count;
use pretty_assertions::assert_eq;

use super::COMPLETION_MESSAGE_MAX_TOKENS;
use super::ERROR_NEXT_ACTION;
use super::format_inter_agent_completion_message;
use super::format_inter_agent_interrupted_message;

#[test]
fn error_completion_message_stays_below_manual_review_threshold() {
    let message = format_inter_agent_completion_message(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("valid agent path"),
        &AgentStatus::Errored("stream disconnected ".repeat(1_000)),
    )
    .expect("error status should produce a completion message");

    assert!(approx_token_count(&message) < COMPLETION_MESSAGE_MAX_TOKENS);
    assert!(message.contains(ERROR_NEXT_ACTION));
}

#[test]
fn interrupted_notice_is_queue_only_without_claiming_success() {
    assert_eq!(
        format_inter_agent_completion_message(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("valid agent path"),
            &AgentStatus::Interrupted,
        ),
        None,
    );

    let message = format_inter_agent_interrupted_message(
        &AgentPath::root(),
        &AgentPath::try_from("/root/worker").expect("valid agent path"),
    );
    assert!(message.contains("INTERRUPTED"));
    assert!(message.contains("without a final answer"));
    assert!(!message.contains("FINAL_ANSWER"));
    assert!(!message.to_lowercase().contains("completed"));
    assert!(!message.to_lowercase().contains("success"));
    assert!(approx_token_count(&message) < COMPLETION_MESSAGE_MAX_TOKENS);
}
