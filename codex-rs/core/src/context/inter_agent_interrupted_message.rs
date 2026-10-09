use codex_protocol::AgentPath;

use super::ContextualUserFragment;
use codex_protocol::models::ContentItemKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InterAgentInterruptedMessage {
    task_name: AgentPath,
    sender: AgentPath,
}

impl InterAgentInterruptedMessage {
    pub(crate) fn new(task_name: AgentPath, sender: AgentPath) -> Self {
        Self { task_name, sender }
    }
}

impl ContextualUserFragment for InterAgentInterruptedMessage {
    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("multi_agent.inter_agent_interrupted_message".to_string())
    }

    fn role(&self) -> &'static str {
        "assistant"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("", "")
    }

    fn body(&self) -> String {
        format!(
            "Message Type: INTERRUPTED\nTask name: {}\nSender: {}\nPayload:\nAgent interrupted. Its turn ended without a final answer; resend work if still needed.",
            self.task_name, self.sender,
        )
    }
}

#[cfg(test)]
#[path = "inter_agent_interrupted_message_tests.rs"]
mod tests;
