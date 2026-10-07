//! Goal-owned background waiting admission contract.
//!
//! Goal publishes a synchronous admission checker into thread extension data.
//! Core invokes it for automatic goal continuations only, after trigger-mail
//! priority and before settings. Inactive by default: absence of the checker
//! means no gate, and every other turn kind or trigger bypasses it.
//!
//! The checker receives the current pending-work read outcome. It compares
//! against the revision recorded at continuation time, enforces single-use
//! check-in tickets, and returns [`GoalAdmissionDecision::Wait`] on read
//! failure (safe: a failed read is never treated as empty). A ticket bypasses
//! only this work gate; status, Plan, shutdown, capacity, input, and
//! newer-turn checks still apply in Core.

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::PendingWorkReadError;
use crate::PendingWorkSnapshot;

/// Decision returned by the goal admission checker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GoalAdmissionDecision {
    /// Proceed with turn start. Other Core admission checks still apply.
    Allow,
    /// Wait for subscribed work or human input; do not start.
    Wait,
}

/// Synchronous checker for automatic goal continuation.
///
/// Published by goal when its background-wait policy is explicitly enabled.
/// Core calls [`GoalBackgroundWaitAdmission::check`] with the current
/// pending-work read outcome; the checker enforces revision, ticket, and
/// read-failure policy owned by goal.
#[derive(Clone)]
pub struct GoalBackgroundWaitAdmission {
    check: Arc<
        dyn Fn(Result<PendingWorkSnapshot, PendingWorkReadError>) -> GoalAdmissionDecision
            + Send
            + Sync,
    >,
}

impl GoalBackgroundWaitAdmission {
    /// Creates an admission checker from a synchronous decision closure.
    pub fn new(
        check: impl Fn(Result<PendingWorkSnapshot, PendingWorkReadError>) -> GoalAdmissionDecision
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            check: Arc::new(check),
        }
    }

    /// Runs the goal-owned admission decision on the current read outcome.
    pub fn check(
        &self,
        outcome: Result<PendingWorkSnapshot, PendingWorkReadError>,
    ) -> GoalAdmissionDecision {
        (self.check)(outcome)
    }
}

impl Debug for GoalBackgroundWaitAdmission {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("GoalBackgroundWaitAdmission(<opaque>)")
    }
}
