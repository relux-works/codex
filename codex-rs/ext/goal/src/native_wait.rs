//! Goal-owned durable-sleep lease for native subagent waits.
//!
//! A goal parent with loaded `PendingInit`/`Running` native children registers
//! one [`SleepItem`] in thread extension data with id
//! `goal-wait:<generation>`. The marker grants queue-only mailbox mail
//! permission to wake an idle thread (core `has_outstanding_durable_sleep`);
//! storing it emits no tool events, schedules nothing, records no history
//! item, and does not change current-time reminders. The goal scheduler owns
//! the timer and warning.
//!
//! Registration uses [`ExtensionData::insert_if`] only when absent or already
//! goal-owned, so it never clobbers another contributor's sleep. Removal uses
//! [`ExtensionData::remove_if`] only when goal-owned, so a foreign marker is
//! preserved. Both are atomic under the extension-data lock.
//!
//! Insert the marker BEFORE checking pending mail, then recheck via the
//! existing pending-work scheduler; this closes completion-before-registration
//! (a child finishing between the idle check and the insert already enqueued
//! mail, and the recheck wakes it).
//!
//! [`SleepItem`]: codex_extension_items::sleep::SleepItem
//! [`ExtensionData::insert_if`]: codex_extension_api::ExtensionData::insert_if
//! [`ExtensionData::remove_if`]: codex_extension_api::ExtensionData::remove_if

use codex_extension_api::ExtensionData;
use codex_extension_items::sleep::SleepItem;
use codex_extension_items::sleep::goal_wait_sleep_id;
use codex_extension_items::sleep::is_goal_wait_sleep_id;

/// Duration stored in the goal wait marker.
///
/// Unused for timing: the marker is durable until explicitly removed, and the
/// goal scheduler owns check-in timers and warnings. Zero avoids implying a
/// real `clock.sleep` duration.
pub const GOAL_WAIT_SLEEP_DURATION_MS: u64 = 0;

/// Registers the goal-owned wait marker for one invalidation `generation`.
///
/// Inserts only when absent or already goal-owned (refreshing a stale own
/// generation); never clobbers a foreign `SleepItem`. Returns `true` when the
/// owned marker is present afterwards, `false` when a foreign marker blocked it.
pub fn try_register_goal_wait_sleep(store: &ExtensionData, generation: u64) -> bool {
    store.insert_if(
        SleepItem {
            id: goal_wait_sleep_id(generation),
            duration_ms: GOAL_WAIT_SLEEP_DURATION_MS,
        },
        |existing| {
            existing.is_none_or(|item| {
                is_goal_wait_sleep_id(item.id.as_str()) || item.id.as_str() == "clock-wait-1"
            })
        },
    )
}

/// Removes the goal-owned wait marker only when it is goal-owned.
///
/// Returns `true` when a goal-owned marker was removed, `false` when absent
/// or foreign (foreign markers are preserved).
pub fn remove_goal_wait_sleep(store: &ExtensionData) -> bool {
    store.remove_if(|existing: Option<&SleepItem>| {
        existing.is_some_and(|item| is_goal_wait_sleep_id(item.id.as_str()))
    })
}

/// Reports whether a goal-owned wait marker is currently stored.
pub fn has_goal_wait_sleep(store: &ExtensionData) -> bool {
    store
        .get::<SleepItem>()
        .is_some_and(|item| is_goal_wait_sleep_id(item.id.as_str()))
}

/// Test-only gate pausing goal wait registration between inspection and insert.
///
/// A suite arms one gate to deterministically interleave a child completion
/// with registration: the production path signals arrival after observing a
/// Running child and before inserting the marker, then waits for release while
/// the suite completes the child (mail enqueued without wake, since no marker
/// exists yet). Production inserts no gate; both hooks are no-ops when none is
/// present. The latch test then releases and asserts the post-insert recheck
/// wakes; a mutant inserting after the mail check misses the wake.
#[doc(hidden)]
pub struct TestNativeWaitRegistrationGate {
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl TestNativeWaitRegistrationGate {
    /// Creates a registration gate for latch tests.
    #[doc(hidden)]
    pub fn new() -> Self {
        Self {
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }

    /// Signals that registration reached the pre-insert pause.
    pub(crate) fn signal_arrived(&self) {
        self.arrived.notify_one();
    }

    /// Waits for the suite to release registration.
    pub(crate) async fn wait_release(&self) {
        self.release.notified().await;
    }

    /// Waits for registration to reach the pre-insert pause.
    #[doc(hidden)]
    pub async fn wait_arrived(&self) {
        self.arrived.notified().await;
    }

    /// Releases paused registration.
    #[doc(hidden)]
    pub fn release(&self) {
        self.release.notify_one();
    }
}
