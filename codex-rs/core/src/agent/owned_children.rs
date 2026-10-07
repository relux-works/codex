//! Typed inspection of directly owned native subagent children.
//!
//! Goal waiting queries the goal thread's directly owned native children
//! through [`LocalAgentRuntime::inspect_directly_owned_native_children`]
//! (defined in `control/runtime_context.rs`). Each owned child reports as
//! [`OwnedChildInspection::Loaded`] (with its current [`AgentStatus`]) or
//! [`OwnedChildInspection::Unloaded`] (known but not currently loaded).
//! Unknown identities and backend failures return
//! [`OwnedChildrenReadError`], never an empty list: callers distinguish "no
//! work" from "unknown", and errors propagate instead of being silently
//! skipped like `list_agents`.
//!
//! [`LocalAgentRuntime::inspect_directly_owned_native_children`]: crate::agent::control::LocalAgentRuntime
//! [`AgentStatus`]: codex_protocol::protocol::AgentStatus

use std::fmt::Display;
use std::fmt::Formatter;

use codex_protocol::ThreadId;
use codex_protocol::protocol::AgentStatus;

/// Inspection of one directly owned native child.
///
/// Reused children report their current status; `Completed` means the child's
/// turn ended, not that the parent's goal is achieved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedChildInspection {
    /// The child runtime is loaded; `status` is its current agent status.
    Loaded {
        /// Directly owned child thread identity.
        thread_id: ThreadId,
        /// Current status observed from the loaded runtime.
        status: AgentStatus,
    },
    /// Membership is known, but no runtime is currently loaded.
    Unloaded {
        /// Directly owned child thread identity.
        thread_id: ThreadId,
    },
}

impl OwnedChildInspection {
    /// Directly owned child thread identity.
    pub fn thread_id(&self) -> ThreadId {
        match self {
            Self::Loaded { thread_id, .. } | Self::Unloaded { thread_id } => *thread_id,
        }
    }

    /// Current status when loaded, `None` when unloaded.
    pub fn status(&self) -> Option<&AgentStatus> {
        match self {
            Self::Loaded { status, .. } => Some(status),
            Self::Unloaded { .. } => None,
        }
    }

    /// Reports whether this loaded child is pending native work.
    ///
    /// Only loaded `PendingInit`/`Running` permits goal-wait registration.
    /// `Completed` means the child's turn ended; `Interrupted` is observed via
    /// queue-only notices for an already-registered wait, not via new
    /// registration; all other statuses are terminal or unknown work.
    pub fn is_pending_native_work(&self) -> bool {
        matches!(
            self.status(),
            Some(AgentStatus::PendingInit | AgentStatus::Running)
        )
    }
}

/// Explicit failure inspecting directly owned native children.
///
/// Callers must treat `Err` as unknown, never as "no work".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedChildrenReadError {
    /// The owning thread runtime is gone.
    SessionUnavailable,
    /// The owned child identity is no longer known.
    UnknownChild {
        /// Child thread identity that is no longer registered.
        thread_id: ThreadId,
    },
    /// The child runtime could not be inspected.
    InspectionFailed {
        /// Child thread identity that failed inspection.
        thread_id: ThreadId,
        /// Backend failure reason.
        reason: String,
    },
}

impl Display for OwnedChildrenReadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionUnavailable => {
                write!(formatter, "owned-children session is unavailable")
            }
            Self::UnknownChild { thread_id } => {
                write!(formatter, "owned child {thread_id} is unknown")
            }
            Self::InspectionFailed { thread_id, reason } => {
                write!(
                    formatter,
                    "owned child {thread_id} inspection failed: {reason}"
                )
            }
        }
    }
}

impl std::error::Error for OwnedChildrenReadError {}

#[cfg(test)]
#[path = "owned_children_tests.rs"]
mod tests;
