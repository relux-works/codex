use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use codex_core::OwnedChildInspection;
use codex_core::StartIfIdleSubmission;
use codex_core::ThreadManager;
use codex_core::TurnInput;
use codex_core::TurnInputRequest;
use codex_core::TurnStartOptions;
use codex_extension_api::ExtensionData;
use codex_protocol::ThreadId;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::ThreadGoal;

use crate::accounting::BudgetLimitedGoalDisposition;
use crate::accounting::GoalAccountingState;
use crate::activity::GoalActivityPublisher;
use crate::activity::GoalTurnStartLease;
use crate::activity::GoalTurnStartPermit;
use crate::analytics::GoalAnalytics;
use crate::analytics::GoalEventAttribution;
use crate::background_wait::BackgroundWaitEvaluation;
use crate::background_wait::BackgroundWaitState;
use crate::background_wait::GoalWaitStatus;
use crate::check_in_clock::CheckInClock;
use crate::check_in_clock::CheckInTimer;
use crate::events::GoalEventEmitter;
use crate::metrics::GoalMetrics;
use crate::steering::continuation_steering_item;
use crate::steering::objective_updated_steering_item;
use crate::tool::protocol_goal_from_state;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct GoalRuntimeHandle {
    inner: Arc<GoalRuntimeInner>,
}

pub(crate) struct GoalRuntimeConfig {
    pub(crate) analytics: GoalAnalytics,
    pub(crate) enabled: bool,
    pub(crate) tools_available_for_thread: bool,
    pub(crate) tools_visible_for_thread: bool,
    pub(crate) root_accounting_state: Option<Arc<GoalAccountingState>>,
    pub(crate) check_in_clock: Arc<dyn CheckInClock>,
}

pub(crate) enum ActiveGoalStopReason {
    TurnError,
    UsageLimit,
    ExecutionUnavailable { expected_goal_id: String },
    EmptyResponse,
}

struct GoalRuntimeInner {
    thread_id: ThreadId,
    state_dbs: Arc<codex_state::StateRuntime>,
    analytics: GoalAnalytics,
    event_emitter: GoalEventEmitter,
    metrics: GoalMetrics,
    thread_manager: Weak<ThreadManager>,
    accounting_state: Arc<GoalAccountingState>,
    root_accounting_state: Option<Arc<GoalAccountingState>>,
    enabled: AtomicBool,
    tools_available_for_thread: bool,
    tools_visible_for_thread: bool,
    goal_state_lock: Arc<Semaphore>,
    activity: GoalActivityPublisher,
    background_wait: Arc<BackgroundWaitState>,
    check_in_timer: CheckInTimer,
    test_marked_continuations: std::sync::Mutex<Vec<String>>,
}

pub(crate) struct AccountedGoalProgress {
    pub(crate) goal: ThreadGoal,
    pub(crate) goal_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreviousGoalSnapshot {
    pub goal_id: String,
    pub status: codex_state::ThreadGoalStatus,
    pub objective: String,
}

impl From<&codex_state::ThreadGoal> for PreviousGoalSnapshot {
    fn from(goal: &codex_state::ThreadGoal) -> Self {
        Self {
            goal_id: goal.goal_id.clone(),
            status: goal.status,
            objective: goal.objective.clone(),
        }
    }
}

impl std::fmt::Debug for GoalRuntimeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoalRuntimeHandle").finish_non_exhaustive()
    }
}

impl GoalRuntimeHandle {
    pub(crate) fn new(
        thread_id: ThreadId,
        state_dbs: Arc<codex_state::StateRuntime>,
        event_emitter: GoalEventEmitter,
        metrics: GoalMetrics,
        thread_manager: Weak<ThreadManager>,
        accounting_state: Arc<GoalAccountingState>,
        config: GoalRuntimeConfig,
    ) -> Self {
        let background_wait = Arc::new(BackgroundWaitState::new());
        let check_in_timer = CheckInTimer::new(config.check_in_clock);
        background_wait.set_invalidation_hook(check_in_timer.abort_hook());
        Self {
            inner: Arc::new(GoalRuntimeInner {
                thread_id,
                state_dbs,
                analytics: config.analytics,
                event_emitter,
                metrics,
                thread_manager,
                accounting_state,
                root_accounting_state: config.root_accounting_state,
                enabled: AtomicBool::new(config.enabled),
                tools_available_for_thread: config.tools_available_for_thread,
                tools_visible_for_thread: config.tools_visible_for_thread,
                goal_state_lock: Arc::new(Semaphore::new(/*permits*/ 1)),
                activity: GoalActivityPublisher::new(config.enabled),
                background_wait,
                check_in_timer,
                test_marked_continuations: std::sync::Mutex::new(Vec::new()),
            }),
        }
    }

    pub(crate) fn set_enabled(&self, enabled: bool, store: &ExtensionData) {
        self.inner.enabled.store(enabled, Ordering::Relaxed);
        self.inner.activity.set_enabled(enabled, store);
        if !enabled {
            self.inner.accounting_state.clear_active_goal();
            crate::native_wait::remove_goal_wait_sleep(store);
            store.remove::<TurnStartOptions>();
            store.remove::<GoalTurnStartPermit>();
        }
    }

    pub(crate) fn stop(&self, store: &ExtensionData) {
        self.inner.enabled.store(false, Ordering::Relaxed);
        self.inner.activity.stop(store);
        self.inner.accounting_state.clear_active_goal();
        self.inner.background_wait.note_stop();
        crate::native_wait::remove_goal_wait_sleep(store);
        store.remove::<TurnStartOptions>();
        store.remove::<GoalTurnStartPermit>();
    }

    /// Removes the goal-owned native-wait marker from the live thread, if present.
    ///
    /// Only the owned `goal-wait:*` id is removed; foreign sleep markers are
    /// preserved. Missing manager or thread is a no-op (nothing to clean).
    pub(crate) async fn remove_native_wait_marker(&self) {
        let Some(manager) = self.inner.thread_manager.upgrade() else {
            return;
        };
        let Ok(thread) = manager.get_thread(self.inner.thread_id).await else {
            return;
        };
        crate::native_wait::remove_goal_wait_sleep(thread.thread_extension_data());
    }

    /// Releases an explicitly released wait: resets the check-in epoch and
    /// removes the owned marker without killing any child work.
    pub async fn release_native_wait(&self) {
        self.inner.background_wait.note_release();
        self.remove_native_wait_marker().await;
    }

    /// Goal-owned background waiting policy for this thread.
    pub fn background_wait_state(&self) -> Arc<BackgroundWaitState> {
        Arc::clone(&self.inner.background_wait)
    }

    /// Turn IDs marked as automatic goal continuations, in order.
    ///
    /// Test-only: suites asserting scheduled check-ins ran through the
    /// production `continue_if_idle` -> `start_turn_if_idle` ->
    /// `mark_goal_continuation` path observe automatic-turn accounting here.
    #[doc(hidden)]
    pub fn test_marked_goal_continuations(&self) -> Vec<String> {
        self.inner
            .test_marked_continuations
            .lock()
            .map(|marked| marked.clone())
            .unwrap_or_default()
    }

    // Callers hold the goal-state permit across the committed mutation/read and
    // publication. Always re-read: delayed tool/API callbacks are not authority.
    pub(crate) async fn reconcile_activity(
        &self,
        store: &ExtensionData,
        _permit: &OwnedSemaphorePermit,
    ) -> Result<Option<codex_state::ThreadGoal>, String> {
        if !self.is_enabled() {
            return Ok(None);
        }
        let revision = self.inner.activity.revision();
        let goal = self
            .inner
            .state_dbs
            .thread_goals()
            .get_thread_goal(self.thread_id())
            .await
            .map_err(|err| err.to_string());
        self.inner.activity.publish(store, revision, goal)
    }

    pub(crate) async fn reconcile_live_activity(
        &self,
        _permit: &OwnedSemaphorePermit,
    ) -> Result<Option<codex_state::ThreadGoal>, String> {
        let revision = self.inner.activity.revision();
        let goal = self
            .inner
            .state_dbs
            .thread_goals()
            .get_thread_goal(self.thread_id())
            .await
            .map_err(|err| err.to_string());
        if self.is_enabled()
            && let Some(manager) = self.inner.thread_manager.upgrade()
            && let Ok(thread) = manager.get_thread(self.thread_id()).await
        {
            return self
                .inner
                .activity
                .publish(thread.thread_extension_data(), revision, goal);
        }
        goal
    }

    pub(crate) async fn clear_activity(&self, _permit: &OwnedSemaphorePermit) {
        if let Some(manager) = self.inner.thread_manager.upgrade()
            && let Ok(thread) = manager.get_thread(self.thread_id()).await
        {
            self.inner.activity.clear(thread.thread_extension_data());
        }
    }

    /// Revoke the marker through the sole publisher after a goal-store read
    /// failure observed outside reconciliation. Records Unknown and reports
    /// the error; the next legitimate lifecycle event reconciles again.
    pub(crate) fn revoke_activity_on_read_failure(&self, store: &ExtensionData, error: &str) {
        let revision = self.inner.activity.revision();
        let _ = self
            .inner
            .activity
            .publish(store, revision, Err(error.to_string()));
    }

    /// Revoke the live marker after a goal-store read failure observed on a
    /// path without direct access to the thread store (external set, fork
    /// flush). Records Unknown and reports the error; the next legitimate
    /// lifecycle event reconciles again.
    pub(crate) async fn revoke_live_activity_on_read_failure(&self, error: &str) {
        if let Some(manager) = self.inner.thread_manager.upgrade()
            && let Ok(thread) = manager.get_thread(self.thread_id()).await
        {
            self.revoke_activity_on_read_failure(thread.thread_extension_data(), error);
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.inner.enabled.load(Ordering::Relaxed)
    }

    pub(crate) fn tools_visible(&self) -> bool {
        self.is_enabled() && self.inner.tools_visible_for_thread
    }

    pub(crate) fn tools_available(&self) -> bool {
        self.is_enabled() && self.inner.tools_available_for_thread
    }

    pub(crate) fn thread_id(&self) -> ThreadId {
        self.inner.thread_id
    }

    pub(crate) fn accounting_state(&self) -> Arc<GoalAccountingState> {
        Arc::clone(&self.inner.accounting_state)
    }

    pub(crate) fn root_accounting_state(&self) -> Option<Arc<GoalAccountingState>> {
        self.inner.root_accounting_state.clone()
    }

    pub(crate) async fn clear_pending_turn_start_options(&self) {
        let Some(thread_manager) = self.inner.thread_manager.upgrade() else {
            return;
        };
        let Ok(thread) = thread_manager.get_thread(self.inner.thread_id).await else {
            return;
        };
        thread.thread_extension_data().remove::<TurnStartOptions>();
    }

    pub(crate) async fn goal_state_permit(&self) -> Result<OwnedSemaphorePermit, String> {
        self.inner
            .goal_state_lock
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| err.to_string())
    }

    pub async fn prepare_external_goal_mutation(&self) -> Result<(), String> {
        let permit = self.goal_state_permit().await?;
        self.prepare_external_goal_mutation_locked(&permit).await
    }

    pub(crate) async fn prepare_external_goal_mutation_locked(
        &self,
        permit: &OwnedSemaphorePermit,
    ) -> Result<(), String> {
        if !self.is_enabled() {
            return Ok(());
        }
        // Invalidate the old turn before the persisted objective/status changes.
        self.inner.accounting_state.reset_empty_responses();

        if let Some(turn_id) = self.inner.accounting_state.current_turn_id() {
            self.account_active_goal_progress_locked(
                permit,
                turn_id.as_str(),
                &format!("{turn_id}:external-goal-mutation"),
                codex_state::GoalAccountingMode::ActiveOnly,
                BudgetLimitedGoalDisposition::ClearActive,
            )
            .await?;
            return Ok(());
        }

        self.account_idle_goal_progress(
            permit,
            &format!("{}:external-goal-mutation", self.inner.thread_id),
            codex_state::GoalAccountingMode::ActiveOnly,
            BudgetLimitedGoalDisposition::ClearActive,
        )
        .await?;
        Ok(())
    }

    pub async fn apply_external_goal_set(
        &self,
        goal: codex_state::ThreadGoal,
        previous_goal: Option<PreviousGoalSnapshot>,
    ) -> Result<(), String> {
        let permit = self.goal_state_permit().await?;
        let committed = self.reconcile_live_activity(&permit).await?;
        if !self.is_enabled() {
            return Ok(());
        }
        let Some(committed) = committed else {
            return Ok(());
        };
        if committed != goal {
            return Ok(());
        }
        self.inner.accounting_state.reset_empty_responses();
        let replaced_existing_goal = previous_goal
            .as_ref()
            .is_some_and(|previous_goal| previous_goal.goal_id != goal.goal_id);
        if previous_goal.is_none() || replaced_existing_goal {
            self.inner.metrics.record_created();
            self.inner
                .analytics
                .created(&goal, GoalEventAttribution::NoTurn);
        }
        let previous_status = previous_goal
            .as_ref()
            .and_then(|previous_goal| (!replaced_existing_goal).then_some(previous_goal.status));
        self.inner
            .metrics
            .record_resumed_if_status_changed(previous_status, goal.status);
        self.inner
            .metrics
            .record_terminal_if_status_changed(previous_status, &goal);
        self.inner
            .analytics
            .status_changed(&goal, previous_status, GoalEventAttribution::NoTurn);
        self.inner.background_wait.note_goal_mutation();
        let objective_changed = previous_goal.as_ref().is_some_and(|previous_goal| {
            !replaced_existing_goal && previous_goal.objective != goal.objective
        });
        match goal.status {
            codex_state::ThreadGoalStatus::Active => {
                if self.inner.accounting_state.current_turn_id().is_some() {
                    let _ = self
                        .inner
                        .accounting_state
                        .mark_current_turn_goal_active(goal.goal_id.clone());
                } else {
                    self.inner
                        .accounting_state
                        .mark_idle_goal_active(goal.goal_id.clone());
                }
                if objective_changed {
                    let item = objective_updated_steering_item(&protocol_goal_from_state(goal));
                    self.inject_active_turn_steering(item).await;
                }
                drop(permit);
                self.continue_if_idle().await?;
            }
            codex_state::ThreadGoalStatus::BudgetLimited => {
                if self.inner.accounting_state.current_turn_id().is_none() {
                    self.inner.accounting_state.clear_active_goal();
                }
                drop(permit);
                self.remove_native_wait_marker().await;
            }
            codex_state::ThreadGoalStatus::Paused
            | codex_state::ThreadGoalStatus::Blocked
            | codex_state::ThreadGoalStatus::UsageLimited
            | codex_state::ThreadGoalStatus::Complete => {
                self.inner.accounting_state.clear_active_goal();
                drop(permit);
                self.remove_native_wait_marker().await;
            }
        }
        Ok(())
    }

    pub async fn apply_external_goal_clear(
        &self,
        goal: codex_state::ThreadGoal,
    ) -> Result<(), String> {
        let permit = self.goal_state_permit().await?;
        let committed = self.reconcile_live_activity(&permit).await?;
        self.inner.analytics.cleared(&goal);
        self.inner.background_wait.note_clear();
        if committed.is_none() {
            self.inner.accounting_state.clear_active_goal();
        }
        drop(permit);
        self.remove_native_wait_marker().await;
        Ok(())
    }

    pub async fn usage_limit_active_goal_for_turn(&self, turn_id: &str) -> Result<(), String> {
        self.stop_active_goal_for_turn(turn_id, ActiveGoalStopReason::UsageLimit)
            .await
    }

    /// Accounts the ending turn and stops its active goal after an error or repeated empty output.
    pub(crate) async fn stop_active_goal_for_turn(
        &self,
        turn_id: &str,
        reason: ActiveGoalStopReason,
    ) -> Result<(), String> {
        if !self.is_enabled() {
            return Ok(());
        }

        // Hold this through accounting and the status update so external goal
        // mutations and idle continuation cannot interleave between them.
        let goal_state_permit = self.goal_state_permit().await?;
        let Some(accounting_goal_id) = self
            .inner
            .accounting_state
            .current_active_goal_id_for_turn(turn_id)
        else {
            return Ok(());
        };
        if let ActiveGoalStopReason::ExecutionUnavailable { expected_goal_id } = &reason
            && accounting_goal_id != *expected_goal_id
        {
            return Ok(());
        }

        let (event_name, status, expected_goal_id) = match reason {
            ActiveGoalStopReason::TurnError => {
                ("turn-error", codex_state::ThreadGoalStatus::Blocked, None)
            }
            ActiveGoalStopReason::UsageLimit => (
                "usage-limit",
                codex_state::ThreadGoalStatus::UsageLimited,
                None,
            ),
            ActiveGoalStopReason::EmptyResponse => {
                let Some(expected_goal_id) =
                    self.inner.accounting_state.empty_response_goal(turn_id)
                else {
                    return Ok(());
                };
                if accounting_goal_id != expected_goal_id {
                    return Ok(());
                }
                (
                    "empty-response",
                    codex_state::ThreadGoalStatus::Blocked,
                    Some(expected_goal_id),
                )
            }
            ActiveGoalStopReason::ExecutionUnavailable { expected_goal_id } => (
                "execution-unavailable",
                codex_state::ThreadGoalStatus::Blocked,
                Some(expected_goal_id),
            ),
        };
        self.account_active_goal_progress_locked(
            &goal_state_permit,
            turn_id,
            &format!("{turn_id}:{event_name}-progress"),
            codex_state::GoalAccountingMode::ActiveOnly,
            BudgetLimitedGoalDisposition::ClearActive,
        )
        .await?;

        let Some(active_goal) = self
            .inner
            .state_dbs
            .thread_goals()
            .get_thread_goal(self.thread_id())
            .await
            .map_err(|err| err.to_string())?
        else {
            self.inner.accounting_state.clear_active_goal();
            return Ok(());
        };
        if expected_goal_id
            .as_ref()
            .is_some_and(|expected_goal_id| active_goal.goal_id != *expected_goal_id)
        {
            return Ok(());
        }
        let can_stop = active_goal.status == codex_state::ThreadGoalStatus::Active
            || (active_goal.status == codex_state::ThreadGoalStatus::BudgetLimited
                && status == codex_state::ThreadGoalStatus::UsageLimited);
        if !can_stop {
            self.inner.accounting_state.clear_active_goal();
            return Ok(());
        }
        let previous_status = Some(active_goal.status);
        let Some(goal) = self
            .inner
            .state_dbs
            .thread_goals()
            .update_thread_goal(
                self.thread_id(),
                codex_state::GoalUpdate {
                    objective: None,
                    status: Some(status),
                    token_budget: None,
                    expected_goal_id: Some(active_goal.goal_id),
                },
            )
            .await
            .map_err(|err| err.to_string())?
        else {
            return Ok(());
        };
        self.reconcile_live_activity(&goal_state_permit).await?;
        self.inner
            .metrics
            .record_terminal_if_status_changed(previous_status, &goal);
        self.inner.analytics.status_changed(
            &goal,
            previous_status,
            GoalEventAttribution::Turn(turn_id),
        );
        self.inner.accounting_state.clear_active_goal();
        self.inner.background_wait.note_goal_mutation();
        let goal = protocol_goal_from_state(goal);
        self.inner.event_emitter.thread_goal_updated(
            format!("{turn_id}:{event_name}"),
            Some(turn_id.to_string()),
            goal,
        );
        drop(goal_state_permit);
        self.remove_native_wait_marker().await;
        Ok(())
    }

    pub async fn restore_after_resume(&self) -> Result<(), String> {
        let permit = self.goal_state_permit().await?;
        let goal = self.reconcile_live_activity(&permit).await?;
        self.inner.background_wait.note_resume();
        // Live-generation-only lease: resume never revives a marker from a
        // rendered old SleepItem or for a now-unloaded child. A fresh wait, if
        // still needed, registers at the next idle via `continue_if_idle`.
        drop(permit);
        self.remove_native_wait_marker().await;
        if !self.is_enabled() {
            return Ok(());
        }

        match goal {
            Some(goal) if goal.status == codex_state::ThreadGoalStatus::Active => {
                self.inner
                    .accounting_state
                    .mark_idle_goal_active(goal.goal_id);
                self.inner.metrics.record_resumed();
            }
            Some(_) | None => self.inner.accounting_state.clear_active_goal(),
        }
        Ok(())
    }

    /// Schedules one re-entry of [`GoalRuntimeHandle::continue_if_idle`] at `deadline`.
    ///
    /// Replaces any pending timer of the same or older generation; a stale
    /// install for an older generation is rejected without disturbing the live
    /// timer. The timer task holds no goal semaphore permit while sleeping and
    /// only a weak runtime handle, so dropping the runtime never leaks through
    /// a pending timer. On fire it detaches itself from the cancellable slot
    /// before claiming the state's registration for `(deadline, generation)`;
    /// a superseded or invalidated timer exits quietly instead of re-entering.
    /// Callers hold the goal permit across evaluation and this install so
    /// concurrent continuations serialize; the spawn itself never awaits.
    fn spawn_check_in_timer(&self, deadline: Duration, generation: u64) {
        let state = Arc::clone(&self.inner.background_wait);
        let clock = Arc::clone(self.inner.check_in_timer.clock());
        let inner = Arc::downgrade(&self.inner);
        let installed = self
            .inner
            .check_in_timer
            .spawn(deadline, generation, move || async move {
                if !state.claim_due_deadline(deadline, generation, clock.now()) {
                    return;
                }
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                let runtime = GoalRuntimeHandle { inner };
                if let Err(err) = runtime.continue_if_idle().await {
                    tracing::warn!("scheduled goal check-in failed: {err}");
                }
            });
        if !installed {
            tracing::debug!(
                ?deadline,
                generation,
                "stale check-in timer install rejected; live timer kept"
            );
        }
    }

    pub(crate) async fn continue_if_idle(&self) -> Result<(), String> {
        if !self.tools_available() {
            self.inner.accounting_state.clear_active_goal();
            self.remove_native_wait_marker().await;
            return Ok(());
        }
        // Hold this through the read/start window so external set/clear cannot
        // change the goal after we read it but before the continuation launches.
        let goal_state_permit = Arc::new(self.goal_state_permit().await?);

        let goal = self.reconcile_live_activity(&goal_state_permit).await?;
        if self
            .inner
            .state_dbs
            .thread_goals()
            .has_thread_goal_continuation_deferral(self.thread_id())
            .await
            .map_err(|err| err.to_string())?
        {
            return Ok(());
        }

        let Some(thread_manager) = self.inner.thread_manager.upgrade() else {
            tracing::debug!("skipping goal continuation because thread manager is unavailable");
            return Ok(());
        };
        let Ok(thread) = thread_manager.get_thread(self.inner.thread_id).await else {
            tracing::debug!("skipping goal continuation because live thread is unavailable");
            return Ok(());
        };

        let Some(goal) = goal else {
            self.inner.accounting_state.clear_active_goal();
            crate::native_wait::remove_goal_wait_sleep(thread.thread_extension_data());
            return Ok(());
        };
        if goal.status != codex_state::ThreadGoalStatus::Active {
            self.inner.accounting_state.clear_active_goal();
            crate::native_wait::remove_goal_wait_sleep(thread.thread_extension_data());
            return Ok(());
        }
        // Native subagent wait: only on a persistent host (background-wait
        // enabled). Non-goal, unloaded, and unknown work gets no marker;
        // failed inspection propagates instead of looking empty.
        let wait_enabled = self.inner.background_wait.is_enabled();
        let native_pending = if wait_enabled {
            let inspections = thread
                .inspect_directly_owned_native_children()
                .await
                .map_err(|err| {
                    format!(
                        "goal native-wait inspection failed for {}: {err}",
                        self.thread_id()
                    )
                })?;
            let pending = inspections
                .iter()
                .any(OwnedChildInspection::is_pending_native_work);
            if pending {
                // Test-only latch: pause between observing Running work and
                // inserting the marker so the suite can complete the child in
                // the window. Production inserts no gate.
                if let Some(gate) = thread
                    .thread_extension_data()
                    .get::<crate::native_wait::TestNativeWaitRegistrationGate>()
                {
                    gate.signal_arrived();
                    gate.wait_release().await;
                }
                // Insert BEFORE checking pending mail, then recheck via the
                // existing scheduler; this closes completion-before-registration.
                crate::native_wait::try_register_goal_wait_sleep(
                    thread.thread_extension_data(),
                    self.inner.background_wait.generation(),
                );
                // The recheck can synchronously start a wake turn, whose
                // `on_turn_start` needs this same goal-state permit: share
                // ownership with that callback rather than letting it
                // reacquire the semaphore this task holds (self-deadlock).
                // The lease removes the entry when the block ends without a
                // turn consuming it, so no stale permit outlives this
                // critical section; the goal-continuation insert below
                // installs a fresh entry for its own start attempt.
                {
                    thread
                        .thread_extension_data()
                        .insert(GoalTurnStartPermit(Arc::clone(&goal_state_permit)));
                    let _recheck_start_lease = GoalTurnStartLease(thread.thread_extension_data());
                    thread.recheck_pending_work_for_goal_wait().await;
                }
            } else {
                crate::native_wait::remove_goal_wait_sleep(thread.thread_extension_data());
            }
            pending
        } else {
            crate::native_wait::remove_goal_wait_sleep(thread.thread_extension_data());
            false
        };
        if wait_enabled {
            let snapshot = codex_extension_api::read_pending_work(thread.thread_extension_data());
            let now = self.inner.check_in_timer.clock().now();
            match self
                .inner
                .background_wait
                .evaluate_continuation_with_native_pending(
                    goal.goal_id.as_str(),
                    GoalWaitStatus::Active,
                    snapshot,
                    native_pending,
                    now,
                ) {
                BackgroundWaitEvaluation::ProceedWithoutGate
                | BackgroundWaitEvaluation::ProceedNormal { .. }
                | BackgroundWaitEvaluation::ProceedWithTicket { .. } => {
                    thread
                        .thread_extension_data()
                        .insert(self.inner.background_wait.admission_checker());
                }
                BackgroundWaitEvaluation::Wait {
                    next_check_in,
                    emit_warning,
                } => {
                    if emit_warning {
                        self.inner.event_emitter.background_wait_warning(
                            self.thread_id().to_string(),
                            crate::background_wait::CHECK_INS_STOPPED_WARNING.to_string(),
                        );
                    }
                    tracing::debug!(
                        ?next_check_in,
                        "goal continuation waiting for subscribed work"
                    );
                    // Hold the goal permit across the install so concurrent
                    // continuations serialize; spawning never awaits, so the
                    // permit is never held across the delay itself.
                    if let Some(deadline) = next_check_in
                        && let Some((armed_deadline, armed_generation)) =
                            self.inner.background_wait.armed_deadline()
                        && armed_deadline == deadline
                    {
                        self.spawn_check_in_timer(deadline, armed_generation);
                    }
                    return Ok(());
                }
                BackgroundWaitEvaluation::WaitOnReadFailure { error } => {
                    tracing::warn!(
                        %error,
                        "goal continuation waiting: pending-work read failed, \
                         safe behavior treats failure as unknown rather than empty"
                    );
                    drop(goal_state_permit);
                    return Ok(());
                }
            }
        }
        let start_options = thread
            .thread_extension_data()
            .get::<TurnStartOptions>()
            .map(|options| options.as_ref().clone())
            .unwrap_or_default();
        let item = continuation_steering_item(
            &protocol_goal_from_state(goal),
            thread.config().await.update_plan_enabled,
        );

        thread
            .thread_extension_data()
            .insert(GoalTurnStartPermit(Arc::clone(&goal_state_permit)));
        let _start_lease = GoalTurnStartLease(thread.thread_extension_data());
        match thread
            .start_turn_if_idle(
                TurnInputRequest::new(TurnInput::ResponseItem(item)).on_start(TurnStartOptions {
                    turn_trigger: Some("goal".to_string()),
                    ..start_options
                }),
            )
            .await
        {
            Ok(StartIfIdleSubmission::Started { turn_id }) => {
                // Turn-stop evaluation takes the same permit, so even a fast response
                // cannot finish before this host-admitted continuation is identified.
                self.inner
                    .accounting_state
                    .mark_goal_continuation(turn_id.clone());
                if let Ok(mut marked) = self.inner.test_marked_continuations.lock() {
                    marked.push(turn_id);
                }
            }
            Ok(StartIfIdleSubmission::NotSubmitted { reason }) => {
                tracing::debug!(
                    ?reason,
                    "skipping goal continuation because automatic idle work was rejected"
                );
            }
            Err(error) => {
                tracing::debug!(
                    %error,
                    "skipping goal continuation because turn input submission failed"
                );
            }
        }

        let current_turn_is_goal_active = self
            .inner
            .accounting_state
            .current_turn_id()
            .is_some_and(|turn_id| {
                self.inner
                    .accounting_state
                    .current_active_goal_id_for_turn(turn_id.as_str())
                    .is_some()
            });
        if !current_turn_is_goal_active {
            self.inner
                .accounting_state
                .reset_idle_progress_baseline_and_clear_active_goal();
        }
        Ok(())
    }

    pub(crate) async fn inject_active_turn_steering(&self, item: ResponseItem) {
        self.inner.background_wait.note_steering();
        let Some(thread_manager) = self.inner.thread_manager.upgrade() else {
            tracing::debug!("skipping goal steering because thread manager is unavailable");
            return;
        };
        let Ok(thread) = thread_manager.get_thread(self.inner.thread_id).await else {
            tracing::debug!("skipping goal steering because live thread is unavailable");
            return;
        };
        if thread.inject_if_running(vec![item]).await.is_err() {
            tracing::debug!("skipping goal steering because no turn is active");
        }
    }

    pub(crate) async fn account_active_goal_progress(
        &self,
        turn_id: &str,
        event_id: &str,
        mode: codex_state::GoalAccountingMode,
        budget_limited_goal_disposition: BudgetLimitedGoalDisposition,
    ) -> Result<Option<AccountedGoalProgress>, String> {
        let permit = self.goal_state_permit().await?;
        self.account_active_goal_progress_locked(
            &permit,
            turn_id,
            event_id,
            mode,
            budget_limited_goal_disposition,
        )
        .await
    }

    async fn account_active_goal_progress_locked(
        &self,
        permit: &OwnedSemaphorePermit,
        turn_id: &str,
        event_id: &str,
        mode: codex_state::GoalAccountingMode,
        budget_limited_goal_disposition: BudgetLimitedGoalDisposition,
    ) -> Result<Option<AccountedGoalProgress>, String> {
        let accounting = self.accounting_state();
        let _accounting_permit = accounting
            .progress_accounting_permit()
            .await
            .map_err(|err| err.to_string())?;
        let Some(snapshot) = accounting.progress_snapshot(turn_id) else {
            return Ok(None);
        };
        let previous_status = self
            .current_goal_status_for_metrics(Some(snapshot.expected_goal_id.as_str()))
            .await?;
        let outcome = self
            .inner
            .state_dbs
            .thread_goals()
            .account_thread_goal_usage(
                self.thread_id(),
                snapshot.time_delta_seconds,
                snapshot.token_delta,
                mode,
                Some(snapshot.expected_goal_id.as_str()),
            )
            .await
            .map_err(|err| err.to_string())?;
        self.reconcile_live_activity(permit).await?;
        Ok(match outcome {
            codex_state::GoalAccountingOutcome::Updated(goal) => {
                let goal_id = goal.goal_id.clone();
                self.inner
                    .metrics
                    .record_terminal_if_status_changed(previous_status, &goal);
                self.inner
                    .analytics
                    .usage_accounted(&goal, GoalEventAttribution::Turn(turn_id));
                self.inner.analytics.status_changed(
                    &goal,
                    previous_status,
                    GoalEventAttribution::Turn(turn_id),
                );
                accounting.mark_progress_accounted_for_status(
                    turn_id,
                    &snapshot,
                    goal.status,
                    budget_limited_goal_disposition,
                );
                let goal = protocol_goal_from_state(goal);
                self.inner.event_emitter.thread_goal_updated(
                    event_id.to_string(),
                    Some(turn_id.to_string()),
                    goal.clone(),
                );
                Some(AccountedGoalProgress { goal, goal_id })
            }
            codex_state::GoalAccountingOutcome::Unchanged(_) => None,
        })
    }

    async fn account_idle_goal_progress(
        &self,
        permit: &OwnedSemaphorePermit,
        event_id: &str,
        mode: codex_state::GoalAccountingMode,
        budget_limited_goal_disposition: BudgetLimitedGoalDisposition,
    ) -> Result<Option<AccountedGoalProgress>, String> {
        let accounting = self.accounting_state();
        let _accounting_permit = accounting
            .progress_accounting_permit()
            .await
            .map_err(|err| err.to_string())?;
        let Some(snapshot) = accounting.idle_progress_snapshot() else {
            return Ok(None);
        };
        let previous_status = self
            .current_goal_status_for_metrics(Some(snapshot.expected_goal_id.as_str()))
            .await?;
        let outcome = self
            .inner
            .state_dbs
            .thread_goals()
            .account_thread_goal_usage(
                self.thread_id(),
                snapshot.time_delta_seconds,
                snapshot.token_delta,
                mode,
                Some(snapshot.expected_goal_id.as_str()),
            )
            .await
            .map_err(|err| err.to_string())?;
        self.reconcile_live_activity(permit).await?;
        Ok(match outcome {
            codex_state::GoalAccountingOutcome::Updated(goal) => {
                let goal_id = goal.goal_id.clone();
                self.inner
                    .metrics
                    .record_terminal_if_status_changed(previous_status, &goal);
                self.inner
                    .analytics
                    .usage_accounted(&goal, GoalEventAttribution::NoTurn);
                self.inner.analytics.status_changed(
                    &goal,
                    previous_status,
                    GoalEventAttribution::NoTurn,
                );
                accounting.mark_idle_progress_accounted_for_status(
                    &snapshot,
                    goal.status,
                    budget_limited_goal_disposition,
                );
                let goal = protocol_goal_from_state(goal);
                self.inner.event_emitter.thread_goal_updated(
                    event_id.to_string(),
                    /*turn_id*/ None,
                    goal.clone(),
                );
                Some(AccountedGoalProgress { goal, goal_id })
            }
            codex_state::GoalAccountingOutcome::Unchanged(_) => {
                accounting.reset_idle_progress_baseline_and_clear_active_goal();
                None
            }
        })
    }

    async fn current_goal_status_for_metrics(
        &self,
        expected_goal_id: Option<&str>,
    ) -> Result<Option<codex_state::ThreadGoalStatus>, String> {
        let goal = self
            .inner
            .state_dbs
            .thread_goals()
            .get_thread_goal(self.thread_id())
            .await
            .map_err(|err| err.to_string())?;
        Ok(goal.and_then(|goal| {
            expected_goal_id
                .is_none_or(|expected_goal_id| goal.goal_id == expected_goal_id)
                .then_some(goal.status)
        }))
    }
}
