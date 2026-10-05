//! The sole writer of the live goal capability. Database reads are serialized
//! with goal mutations; the publication revision also invalidates reads across
//! synchronous feature changes and teardown.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use codex_extension_api::ExtensionData;
use codex_extension_api::GoalActivity;
use codex_extension_api::GoalActivityState;
use codex_state::ThreadGoal;
use codex_state::ThreadGoalStatus;

#[derive(Debug, PartialEq, Eq)]
enum ReconciledGoal {
    Unknown,
    Known(Option<ThreadGoal>),
}

struct Publication {
    enabled: bool,
    stopped: bool,
    revision: u64,
    goal: ReconciledGoal,
}

pub(crate) struct GoalActivityPublisher(Mutex<Publication>);

impl GoalActivityPublisher {
    pub(crate) fn new(enabled: bool) -> Self {
        Self(Mutex::new(Publication {
            enabled,
            stopped: false,
            revision: 0,
            goal: ReconciledGoal::Unknown,
        }))
    }

    pub(crate) fn revision(&self) -> u64 {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .revision
    }

    pub(crate) fn set_enabled(&self, enabled: bool, store: &ExtensionData) {
        let mut publication = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if publication.enabled != enabled {
            publication.enabled = enabled;
            publication.revision += 1;
            publication.goal = ReconciledGoal::Unknown;
            store.remove::<GoalActivity>();
        }
        if !enabled {
            store.remove::<GoalActivity>();
        }
    }

    pub(crate) fn stop(&self, store: &ExtensionData) {
        let mut publication = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        publication.stopped = true;
        publication.revision += 1;
        publication.goal = ReconciledGoal::Unknown;
        store.remove::<GoalActivity>();
    }

    pub(crate) fn publish(
        &self,
        store: &ExtensionData,
        read_revision: u64,
        goal: Result<Option<ThreadGoal>, String>,
    ) -> Result<Option<ThreadGoal>, String> {
        let mut publication = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if publication.revision != read_revision || publication.stopped || !publication.enabled {
            return Err("goal activity reconciliation invalidated by lifecycle change".to_string());
        }
        let goal = match goal {
            Ok(goal) => goal,
            Err(error) => {
                publication.revision += 1;
                publication.goal = ReconciledGoal::Unknown;
                store.remove::<GoalActivity>();
                tracing::warn!(%error, "goal activity reconciliation is unknown");
                return Err(error);
            }
        };
        let reconciled = ReconciledGoal::Known(goal.clone());
        if publication.goal != reconciled {
            publication.revision += 1;
            publication.goal = reconciled;
        }
        match &goal {
            Some(goal)
                if matches!(
                    goal.status,
                    ThreadGoalStatus::Active
                        | ThreadGoalStatus::BudgetLimited
                        | ThreadGoalStatus::Complete
                ) =>
            {
                store.insert(GoalActivity {
                    goal_id: goal.goal_id.clone(),
                    revision: publication.revision,
                    state: if goal.status == ThreadGoalStatus::Active {
                        GoalActivityState::Active
                    } else {
                        GoalActivityState::BudgetLimited
                    },
                });
            }
            Some(_) | None => {
                store.remove::<GoalActivity>();
            }
        }
        Ok(goal)
    }

    pub(crate) fn clear(&self, store: &ExtensionData) {
        let mut publication = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        publication.revision += 1;
        publication.goal = ReconciledGoal::Known(None);
        store.remove::<GoalActivity>();
    }
}

// Automatic start awaits a before-registration lifecycle callback on the
// session loop. Share ownership of its read permit with that callback rather
// than reacquiring the same semaphore. The lease cannot authorize mutations.
pub(crate) struct GoalTurnStartPermit(pub(crate) Arc<tokio::sync::OwnedSemaphorePermit>);

pub(crate) struct GoalTurnStartLease<'a>(pub(crate) &'a ExtensionData);

impl Drop for GoalTurnStartLease<'_> {
    fn drop(&mut self) {
        self.0.remove::<GoalTurnStartPermit>();
    }
}
