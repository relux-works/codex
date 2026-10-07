//! Host capability for asynchronous exec-completion notifications.
//!
//! Core and the goal extension read this host-seeded marker from the thread's
//! [`ExtensionData`][data] to decide whether an exec completion wake can be
//! promised: the `notify_on_exit` schema, the `exec_notification` tool, and the
//! goal background-wait policy are exposed only where this marker is
//! [`AsyncNotificationSupport::Available`]. Absence means
//! [`AsyncNotificationSupport::Unavailable`]; no wake is ever promised on a
//! host whose persistence was not verified.
//!
//! Only verified persistent hosts enable this. Headless exec shuts its runtime
//! down at `TurnCompleted`, so it and every descendant stay `Unavailable`.
//! Children inherit their parent's stored value explicitly; a child's own
//! session source (subagents report `SubAgent`, never `Exec`) must not be used
//! to derive availability.
//!
//! [data]: crate::ExtensionData

use codex_protocol::protocol::SessionSource;

use crate::ExtensionData;

/// Whether the host can deliver an exec completion wake after this turn.
///
/// Host-seeded through [`crate::ExtensionDataInit`] before thread startup;
/// defaults to [`AsyncNotificationSupport::Unavailable`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AsyncNotificationSupport {
    /// The host cannot promise a completion wake. `notify_on_exit` is refused,
    /// `exec_notification` is not advertised, and the goal background-wait
    /// policy stays inactive.
    #[default]
    Unavailable,
    /// The host persists past turn end and can wake for opted-in exec exits.
    Available,
}

impl AsyncNotificationSupport {
    /// Reports whether the host can promise a completion wake.
    pub fn is_available(self) -> bool {
        matches!(self, Self::Available)
    }

    /// Root-thread decision for a host running as `session_source`.
    ///
    /// Only verified persistent interactive hosts (`Cli`, `VSCode`) enable
    /// notifications. Headless exec is explicitly unavailable, and every other
    /// source (MCP, custom, unknown, internal, subagent) stays unavailable
    /// until its persistence is verified. This decides roots only; children
    /// inherit their parent's stored value instead of consulting any source.
    pub fn for_host_session_source(session_source: &SessionSource) -> Self {
        match session_source {
            SessionSource::Cli | SessionSource::VSCode => Self::Available,
            SessionSource::Exec
            | SessionSource::Mcp
            | SessionSource::Custom(_)
            | SessionSource::Internal(_)
            | SessionSource::SubAgent(_)
            | SessionSource::Unknown => Self::Unavailable,
        }
    }

    /// Reads the marker from `thread_store`, defaulting to `Unavailable`.
    ///
    /// Absence is not an error: threads started before this marker existed,
    /// and hosts that seed nothing, simply cannot promise a wake.
    pub fn read_from(thread_store: &ExtensionData) -> Self {
        thread_store
            .get::<Self>()
            .map(|support| *support)
            .unwrap_or_default()
    }
}
