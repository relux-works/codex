use codex_protocol::AgentPath;
use pretty_assertions::assert_eq;

use super::InterAgentInterruptedMessage;
use crate::context::ContextualUserFragment;

#[test]
fn interrupted_fragment_renders_exact_notice_body() {
    let fragment = InterAgentInterruptedMessage::new(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("valid agent path"),
    );
    assert_eq!(
        fragment.render(),
        "Message Type: INTERRUPTED\nTask name: /root\nSender: /root/worker\nPayload:\nAgent interrupted. Its turn ended without a final answer; resend work if still needed."
    );
}
