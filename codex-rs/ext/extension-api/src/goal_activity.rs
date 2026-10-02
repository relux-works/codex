/// Describes the goal whose work is currently active on a thread.
///
/// Extensions publish this as typed, thread-scoped data so core capabilities
/// can respond to goal activity without depending on a goal implementation.
/// The marker is ephemeral and is not persisted. Publishers should reconcile
/// it with committed goal state and advance `revision` when that state changes,
/// allowing consumers to distinguish fresh state from stale callbacks.
/// Presence grants goal-related tool capability, not permission to admit an
/// automatic continuation; consumers must still apply their own feature gates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoalActivity {
    /// Stable identity of the active goal.
    pub goal_id: String,
    /// Publisher revision for the authoritative goal snapshot.
    pub revision: u64,
    /// Whether the goal is active or awaiting recovery from a budget limit.
    pub state: GoalActivityState,
}

/// Goal states that retain active work on a thread.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoalActivityState {
    /// The goal can continue normally.
    Active,
    /// The goal remains present while continuation is limited by its budget.
    BudgetLimited,
}
