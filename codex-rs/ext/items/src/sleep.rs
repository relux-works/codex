use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use ts_rs::TS;

/// Display item emitted by the interruptible `clock.sleep` tool.
#[derive(Debug, Clone, Deserialize, Serialize, TS, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct SleepItem {
    pub id: String,
    #[ts(type = "number")]
    pub duration_ms: u64,
}

/// Prefix for goal-owned durable-sleep wait markers.
///
/// A `SleepItem` stored in thread extension data with an id starting with this
/// prefix grants queue-only mailbox mail permission to wake an idle thread
/// (see core `has_outstanding_durable_sleep`). Storing it emits no tool
/// events, schedules nothing, records no history item, and does not change
/// current-time reminders; the goal scheduler owns the timer and warning.
pub const GOAL_WAIT_SLEEP_ID_PREFIX: &str = "goal-wait:";

/// Builds the goal-owned wait marker id for one invalidation generation.
pub fn goal_wait_sleep_id(generation: u64) -> String {
    format!("{GOAL_WAIT_SLEEP_ID_PREFIX}{generation}")
}

/// Reports whether a `SleepItem` id is owned by the goal wait lease.
pub fn is_goal_wait_sleep_id(id: &str) -> bool {
    id.starts_with(GOAL_WAIT_SLEEP_ID_PREFIX)
}
