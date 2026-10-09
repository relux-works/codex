//! Delivers terminal child results and completion activity to the agent tree.
//!
//! Sessions capture terminal state; the controller owns routing and queue-only delivery.
//! Delivery remains best effort, with tracing recorded only after the parent accepts it.

use super::LocalAgentControl;
use crate::TurnStartOptions;
use crate::agent::api::AgentTurnOutcome;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::session_prefix::format_inter_agent_completion_message;
use crate::session_prefix::format_inter_agent_interrupted_message;
use codex_extension_items::sleep::SleepItem;
use codex_extension_items::sleep::is_goal_wait_sleep_id;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::items::SubAgentActivityItem;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentActivityKind;
use codex_protocol::protocol::SubAgentSource;
use codex_rollout_trace::AgentResultTracePayload;
use codex_rollout_trace::ThreadTraceContext;
use tracing::debug;

impl LocalAgentControl {
    /// Routes a captured terminal outcome without retaining the child's live turn context.
    pub(crate) async fn notify_parent_of_terminal_turn(
        &self,
        outcome: AgentTurnOutcome,
        trace: &ThreadTraceContext,
    ) {
        let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            agent_path: Some(child_agent_path),
            ..
        }) = &outcome.source
        else {
            return;
        };
        let parent_thread_id = *parent_thread_id;
        let status = outcome.status;
        let Some(parent_agent_path) = child_agent_path
            .as_str()
            .rsplit_once('/')
            .and_then(|(parent, _)| AgentPath::try_from(parent).ok())
        else {
            return;
        };

        if matches!(status, AgentStatus::Completed(_))
            && let Some(parent_turn_id) = outcome.parent_turn_id
        {
            let initiating_thread_id = match outcome.initiating_agent_path.as_ref() {
                Some(initiating_agent_path) if initiating_agent_path != &parent_agent_path => {
                    self.runtime.resolve_agent_reference(
                        outcome.thread_id,
                        &outcome.source,
                        initiating_agent_path.as_str(),
                    )
                    .await
                    .inspect_err(|err| {
                        debug!(
                            "failed to resolve completed activity initiator {initiating_agent_path}: {err}"
                        );
                    })
                    .ok()
                }
                _ => Some(parent_thread_id),
            };
            if let Some(initiating_thread_id) = initiating_thread_id
                && let Err(err) = self
                    .emit_sub_agent_activity(
                        initiating_thread_id,
                        parent_turn_id,
                        SubAgentActivityItem {
                            id: format!("subagent-completed-{}", outcome.turn_id),
                            kind: SubAgentActivityKind::Completed,
                            agent_thread_id: outcome.thread_id,
                            agent_path: child_agent_path.clone(),
                        },
                    )
                    .await
            {
                debug!(
                    "failed to emit completed activity to initiating thread {initiating_thread_id}: {err}"
                );
            }
        }

        // `Interrupted` is non-final (`is_final` unchanged): notify only when
        // the direct parent holds a registered goal wait, once per transition,
        // without claiming success. Ordinary parents stay quiet.
        let message = if matches!(status, AgentStatus::Interrupted) {
            if !self.parent_holds_goal_wait_marker(parent_thread_id).await {
                return;
            }
            format_inter_agent_interrupted_message(&parent_agent_path, child_agent_path)
        } else {
            let Some(message) = format_inter_agent_completion_message(
                parent_agent_path.clone(),
                child_agent_path.clone(),
                &status,
            ) else {
                return;
            };
            message
        };
        // `communication` owns the message. Keep a second copy only when the
        // recorder will actually need it after parent delivery succeeds.
        let trace_message = trace.is_enabled().then(|| message.clone());
        let communication = InterAgentCommunication::new(
            child_agent_path.clone(),
            parent_agent_path,
            Vec::new(),
            message,
            /*trigger_turn*/ false,
        );
        // Mutant: deliver the interrupted notice twice per transition.
        let duplicate = matches!(status, AgentStatus::Interrupted).then(|| communication.clone());
        let context =
            AgentCommunicationContext::new(AgentCommunicationKind::Result, outcome.thread_id);
        if let Err(err) = self
            .send_inter_agent_communication(
                parent_thread_id,
                communication,
                context,
                TurnStartOptions::default(),
            )
            .await
        {
            debug!("failed to notify parent thread {parent_thread_id}: {err}");
            return;
        }
        if let Some(duplicate) = duplicate {
            let context =
                AgentCommunicationContext::new(AgentCommunicationKind::Result, outcome.thread_id);
            let _ = self
                .send_inter_agent_communication(
                    parent_thread_id,
                    duplicate,
                    context,
                    TurnStartOptions::default(),
                )
                .await;
        }
        if let Some(message) = trace_message {
            trace.record_agent_result_interaction(
                outcome.turn_id.as_str(),
                parent_thread_id,
                &AgentResultTracePayload {
                    child_agent_path: child_agent_path.as_str(),
                    message: &message,
                    status: &status,
                },
            );
        }
    }

    /// Reports whether the direct parent holds a goal-owned wait marker.
    ///
    /// Missing manager or parent is quiet (`false`): interrupted notices are
    /// best effort for registered waits only.
    async fn parent_holds_goal_wait_marker(&self, parent_thread_id: ThreadId) -> bool {
        let Ok(manager) = self.runtime.upgrade() else {
            return false;
        };
        let Ok(parent) = manager.get_thread(parent_thread_id).await else {
            return false;
        };
        parent
            .thread_extension_data()
            .get::<SleepItem>()
            .is_some_and(|item| is_goal_wait_sleep_id(item.id.as_str()))
    }
}
