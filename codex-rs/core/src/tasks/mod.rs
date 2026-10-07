mod compact;
mod lifecycle;
mod regular;
mod review;
mod user_shell;

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use codex_diagnostics::Gauge;
use codex_extension_api::ThreadIdleCause;
use futures::future::BoxFuture;
use tokio::select;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::Instrument;
use tracing::Span;
use tracing::field;
use tracing::info_span;
use tracing::trace;
use tracing::trace_span;
use tracing::warn;

use crate::codex_thread::BackgroundTerminalInfo;
use crate::codex_thread::TestGoalStartTaskGate;
use crate::codex_thread::TestWakeLeaseGate;
use crate::config::Config;
use crate::context::ContextualUserFragment;
use crate::context::MAX_EXEC_COMPLETION_FRAGMENTS_PER_REQUEST;
use crate::hook_runtime::run_turn_interrupt_hooks;
use crate::session::TurnInput;
use crate::session::exec_completion_ack;
use crate::session::session::Session;
use crate::session::turn::run_hooks_and_record_inputs;
use crate::session::turn_context::NewTurnContextOptions;
use crate::session::turn_context::TurnContext;
use crate::state::ActiveTurn;
use crate::state::RunningTask;
use crate::state::TaskKind;
use crate::state::TurnState;
use codex_analytics::TurnProfileFact;
use codex_analytics::TurnTokenUsageFact;
use codex_context_fragments::RenderedFragment;
use codex_otel::SessionTelemetry;
use codex_otel::TURN_E2E_DURATION_METRIC;
use codex_otel::TURN_MEMORY_METRIC;
use codex_otel::TURN_NETWORK_PROXY_METRIC;
use codex_otel::TURN_TOOL_CALL_METRIC;
use codex_otel::TURN_UNIFIED_EXEC_RUNNING_PROCESSES_METRIC;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::ErrorEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::TokenUsage;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnAbortedEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::WarningEvent;
use codex_thread_store::PersistContext;

use codex_features::Feature;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result as CodexResult;
pub(crate) use compact::CompactTask;
pub(crate) use regular::RegularTask;
pub(crate) use review::ReviewTask;
pub(crate) use user_shell::UserShellCommandMode;
pub(crate) use user_shell::UserShellCommandTask;
pub(crate) use user_shell::execute_user_shell_command;

pub(crate) const GRACEFULL_INTERRUPTION_TIMEOUT_MS: u64 = 100;
const TASK_COMPACT_METRIC: &str = "codex.task.compact";
static ACTIVE_TURNS: Gauge = Gauge::new("core.turns.active");

pub(crate) type SessionTaskResult = CodexResult<Option<String>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InterruptedTurnHistoryMarker {
    Disabled,
    ContextualUser,
    Developer,
}

impl InterruptedTurnHistoryMarker {
    pub(crate) fn from_config_and_version(
        config: &Config,
        multi_agent_version: MultiAgentVersion,
    ) -> Self {
        if !config.agent_interrupt_message_enabled {
            return Self::Disabled;
        }
        if multi_agent_version == MultiAgentVersion::V2 {
            Self::Developer
        } else {
            Self::ContextualUser
        }
    }
}

/// Shared model-visible marker used by both the real interrupt path and
/// interrupted fork snapshots.
pub(crate) fn interrupted_turn_history_marker(
    marker: InterruptedTurnHistoryMarker,
) -> Option<ResponseItem> {
    match marker {
        InterruptedTurnHistoryMarker::Disabled => None,
        InterruptedTurnHistoryMarker::ContextualUser => Some(ContextualUserFragment::into(
            crate::context::TurnAborted::new(crate::context::TurnAborted::INTERRUPTED_GUIDANCE),
        )),
        InterruptedTurnHistoryMarker::Developer => {
            let marker = crate::context::TurnAborted::new(
                crate::context::TurnAborted::INTERRUPTED_DEVELOPER_GUIDANCE,
            );
            let (_, content) = marker.render_fragment().into_parts();
            Some(RenderedFragment::new("developer", content).into())
        }
    }
}

fn emit_turn_network_proxy_metric(
    session_telemetry: &SessionTelemetry,
    network_proxy_active: bool,
    tmp_mem: (&str, &str),
) {
    let active = if network_proxy_active {
        "true"
    } else {
        "false"
    };
    session_telemetry.counter(
        TURN_NETWORK_PROXY_METRIC,
        /*inc*/ 1,
        &[("active", active), tmp_mem],
    );
}

fn emit_turn_memory_metric(
    session_telemetry: &SessionTelemetry,
    feature_enabled: bool,
    config_enabled: bool,
    has_citations: bool,
) {
    let read_allowed = feature_enabled && config_enabled;
    session_telemetry.counter(
        TURN_MEMORY_METRIC,
        /*inc*/ 1,
        &[
            ("read_allowed", bool_tag(read_allowed)),
            ("feature_enabled", bool_tag(feature_enabled)),
            ("config_use_memories", bool_tag(config_enabled)),
            ("has_citations", bool_tag(has_citations)),
        ],
    );
}

pub(crate) fn emit_compact_metric(
    session_telemetry: &SessionTelemetry,
    compact_type: &'static str,
    manual: bool,
) {
    session_telemetry.counter(
        TASK_COMPACT_METRIC,
        /*inc*/ 1,
        &[("type", compact_type), ("manual", bool_tag(manual))],
    );
}

fn bool_tag(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

/// Async task that drives a [`Session`] turn.
///
/// Implementations encapsulate a specific Codex workflow (regular chat,
/// reviews, ghost snapshots, etc.). Each task instance is owned by a
/// [`Session`] and executed on a background Tokio task. The trait is
/// intentionally small: implementers identify themselves via
/// [`SessionTask::kind`], perform their work in [`SessionTask::run`], and may
/// release resources in [`SessionTask::abort`].
pub(crate) trait SessionTask: Send + Sync + 'static {
    /// Describes the type of work the task performs so the session can
    /// surface it in telemetry and UI.
    fn kind(&self) -> TaskKind;

    /// Returns the tracing name for a spawned task span.
    fn span_name(&self) -> &'static str;

    /// Executes the task until completion or cancellation.
    ///
    /// Implementations typically stream protocol events using `session` and
    /// `ctx`, returning an optional final agent message when finished. The
    /// provided `cancellation_token` is cancelled when the session requests an
    /// abort; implementers should watch for it and terminate quickly once it
    /// fires. Returning [`Some`] yields a final message that
    /// [`Session::on_task_finished`] will emit to the client. Returning
    /// [`CodexErr::TurnAborted`] completes the task through the aborted-turn
    /// lifecycle instead.
    fn run(
        self: Arc<Self>,
        session: Arc<Session>,
        ctx: Arc<TurnContext>,
        input: Vec<TurnInput>,
        cancellation_token: CancellationToken,
    ) -> impl std::future::Future<Output = SessionTaskResult> + Send;

    /// Gives the task a chance to perform cleanup after an abort.
    ///
    /// The default implementation is a no-op; override this if additional
    /// teardown or notifications are required once
    /// [`Session::abort_all_tasks`] cancels the task.
    fn abort(
        &self,
        session: Arc<Session>,
        ctx: Arc<TurnContext>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let _ = (session, ctx);
        }
    }
}

pub(crate) trait AnySessionTask: Send + Sync + 'static {
    fn kind(&self) -> TaskKind;

    fn span_name(&self) -> &'static str;

    fn run(
        self: Arc<Self>,
        session: Arc<Session>,
        ctx: Arc<TurnContext>,
        input: Vec<TurnInput>,
        cancellation_token: CancellationToken,
    ) -> BoxFuture<'static, SessionTaskResult>;

    fn abort<'a>(&'a self, session: Arc<Session>, ctx: Arc<TurnContext>) -> BoxFuture<'a, ()>;
}

impl<T> AnySessionTask for T
where
    T: SessionTask,
{
    fn kind(&self) -> TaskKind {
        SessionTask::kind(self)
    }

    fn span_name(&self) -> &'static str {
        SessionTask::span_name(self)
    }

    fn run(
        self: Arc<Self>,
        session: Arc<Session>,
        ctx: Arc<TurnContext>,
        input: Vec<TurnInput>,
        cancellation_token: CancellationToken,
    ) -> BoxFuture<'static, SessionTaskResult> {
        Box::pin(SessionTask::run(
            self,
            session,
            ctx,
            input,
            cancellation_token,
        ))
    }

    fn abort<'a>(&'a self, session: Arc<Session>, ctx: Arc<TurnContext>) -> BoxFuture<'a, ()> {
        Box::pin(SessionTask::abort(self, session, ctx))
    }
}

/// Vacancy a turn start may claim on the active turn.
///
/// A wake reserves a bare turn, awaits turn setup, then starts. A
/// concurrent submission can abort that bare reservation in between, so the
/// wake must verify its reservation is still the active turn when it claims
/// it instead of starting on whatever turn won the race.
pub(crate) enum TurnStartClaim {
    /// Claim any vacancy, creating one when idle. Used after aborting the
    /// previous turn, where no reservation can be lost.
    AnyVacancy,
    /// Claim only this reservation. A lost reservation backs off so the
    /// caller can fail its leases back for a later wake.
    Reserved(Arc<Mutex<TurnState>>),
}

impl TurnStartClaim {
    /// Claims the vacancy to start on.
    ///
    /// Returns `None` when a lost reservation must back off, leaving the
    /// session untouched: a reservation claim never creates a turn, so a
    /// backed-off start cannot wedge the next wake behind an ownerless
    /// reservation.
    fn claim_turn<'a>(&self, active: &'a mut Option<ActiveTurn>) -> Option<&'a mut ActiveTurn> {
        match self {
            TurnStartClaim::AnyVacancy => {
                let turn = active.get_or_insert_with(ActiveTurn::default);
                debug_assert!(turn.task.is_none());
                Some(turn)
            }
            TurnStartClaim::Reserved(expected) => {
                let turn = active.as_mut()?;
                if turn.task.is_none() && Arc::ptr_eq(&turn.turn_state, expected) {
                    Some(turn)
                } else {
                    None
                }
            }
        }
    }
}

impl Session {
    pub async fn spawn_task<T: SessionTask>(
        self: &Arc<Self>,
        turn_context: Arc<TurnContext>,
        input: Vec<TurnInput>,
        task: T,
    ) {
        self.abort_all_tasks(TurnAbortReason::Replaced).await;
        self.clear_connector_selection().await;
        self.start_task(
            turn_context,
            input,
            task,
            TurnStartClaim::AnyVacancy,
            /*goal_admitted_revision*/ None,
        )
        .await;
    }

    /// Starts `task`, claiming the active turn for `claim`.
    ///
    /// Returns `false` leaving the session untouched when a
    /// [`TurnStartClaim::Reserved`] reservation was lost to a concurrent
    /// submission; the caller fails its leases back so a later wake retries
    /// them. [`TurnStartClaim::AnyVacancy`] always starts, except an automatic
    /// goal continuation carrying `goal_admitted_revision`, which returns
    /// `false` with its reservation cleared when the work revision changed
    /// after admission (the caller reports `GoalBackgroundWait`).
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "claim, record, and drain atomically with the active reservation"
    )]
    pub(crate) async fn start_task<T: SessionTask>(
        self: &Arc<Self>,
        turn_context: Arc<TurnContext>,
        input: Vec<TurnInput>,
        task: T,
        claim: TurnStartClaim,
        goal_admitted_revision: Option<u64>,
    ) -> bool {
        // Inherited or recovered roots are applied before task start. Otherwise this
        // task owns its turn, including background work. Later mail cannot change it.
        turn_context
            .turn_metadata_state
            .set_root_turn_id(turn_context.sub_id.clone());
        let task: Arc<dyn AnySessionTask> = Arc::new(task);
        let task_kind = task.kind();
        let span_name = task.span_name();
        let started_at = Instant::now();
        let turn_started_at_unix_ms = turn_context
            .turn_timing_state
            .mark_turn_started(started_at)
            .await;
        turn_context
            .turn_metadata_state
            .set_turn_started_at_unix_ms(turn_started_at_unix_ms);
        let token_usage_at_turn_start = self.total_token_usage().await.unwrap_or_default();

        let cancellation_token = CancellationToken::new();
        let done = Arc::new(Notify::new());

        let (turn_state, pending_items) = {
            let mut active = self.active_turn.lock().await;
            let Some(turn) = claim.claim_turn(&mut active) else {
                return false;
            };
            // Test-only pause for the AC7 residual window (goal path only).
            // Production inserts no gate. The suite arms a receipt here, after
            // the late admission recheck and before publication.
            if goal_admitted_revision.is_some()
                && let Some(gate) = self
                    .services
                    .thread_extension_data
                    .get::<TestGoalStartTaskGate>()
            {
                gate.signal_arrived();
                gate.wait_release().await;
            }
            // LINEARIZATION POINT for automatic goal continuation (AC7): the
            // helper compares the admitted work revision serialized with every
            // receipt-store and mailbox transition (active-turn held here plus
            // session-state, store, and mailbox locks), immediately before the
            // publication, with NO await between the comparison and the
            // publication. See `publish_goal_turn_if_revision_matches` for the
            // ordering rule.
            if !self
                .publish_goal_turn_if_revision_matches(
                    turn_context.sub_id.as_str(),
                    goal_admitted_revision,
                )
                .await
            {
                *active = None;
                return false;
            }
            let turn_state = Arc::clone(&turn.turn_state);
            // Drain under the same lock so a backed-off start drops no mail.
            let (pending_items, _) = self.input_queue.drain_mailbox_input_items().await;
            (turn_state, pending_items)
        };
        // Apply plugin selection only for a claimed turn: a backed-off start
        // must not overwrite the winning turn's session plugin state.
        self.activate_plugin_selection(&turn_context).await;
        turn_state.lock().await.token_usage_at_turn_start = token_usage_at_turn_start.clone();
        self.input_queue
            .extend_pending_input_for_turn_state(turn_state.as_ref(), pending_items)
            .await;
        self.emit_turn_start_lifecycle(
            turn_context.as_ref(),
            Some(&token_usage_at_turn_start),
            codex_extension_api::TurnStartPhase::BeforeTaskRegistration,
        )
        .await;

        let mut active = self.active_turn.lock().await;
        let Some(turn) = claim.claim_turn(&mut active) else {
            return false;
        };
        let agent_execution_guard = self.services.agent_control.admit_turn(
            turn_context.multi_agent_version,
            &turn_context.session_source,
        );
        let done_clone = Arc::clone(&done);
        let session = Arc::clone(self);
        let ctx = Arc::clone(&turn_context);
        let task_for_run = Arc::clone(&task);
        let task_input = input;
        let task_cancellation_token = cancellation_token.child_token();
        // Task-owned turn spans keep a core-owned span open for the
        // full task lifecycle after the submission dispatch span ends.
        let reasoning_effort = turn_context.effective_reasoning_effort_for_tracing();
        let task_span = info_span!(
            "turn",
            otel.name = span_name,
            thread.id = %self.thread_id,
            turn.id = %turn_context.sub_id,
            model = %turn_context.model_info().slug,
            codex.turn.reasoning_effort = %reasoning_effort,
            codex.turn.token_usage.input_tokens = field::Empty,
            codex.turn.token_usage.cached_input_tokens = field::Empty,
            codex.turn.token_usage.cache_write_input_tokens = field::Empty,
            codex.turn.token_usage.non_cached_input_tokens = field::Empty,
            codex.turn.token_usage.output_tokens = field::Empty,
            codex.turn.token_usage.reasoning_output_tokens = field::Empty,
            codex.turn.token_usage.total_tokens = field::Empty,
        );
        let handle = tokio::spawn(
            async move {
                let ctx_for_finish = Arc::clone(&ctx);
                let task_result = task_for_run
                    .run(
                        Arc::clone(&session),
                        ctx,
                        task_input,
                        task_cancellation_token.child_token(),
                    )
                    .instrument(trace_span!("session_task.run"))
                    .await;
                let sess = Arc::clone(&session);
                // Private reviewers save their transcript together with the terminal event.
                // Errors and cancellation retain their existing save path.
                if (!sess.is_private_guardian_reviewer().await
                    || task_cancellation_token.is_cancelled()
                    || task_result.is_err())
                    && let Err(err) = sess.flush_rollout().await
                {
                    warn!("failed to flush rollout before completing turn: {err}");
                    sess.send_event(
                        ctx_for_finish.as_ref(),
                        EventMsg::Warning(WarningEvent {
                            message: format!(
                                "Failed to save the conversation transcript; Codex will continue retrying. Error: {err}"
                            ),
                        }),
                    )
                    .await;
                }
                if !task_cancellation_token.is_cancelled() {
                    // Finish uniformly from the spawn site so all tasks share the same lifecycle.
                    sess.on_task_finished(Arc::clone(&ctx_for_finish), task_result)
                        .await;
                }
                done_clone.notify_waiters();
            }
            .instrument(task_span),
        );
        let timer = turn_context
            .session_telemetry
            .start_timer(TURN_E2E_DURATION_METRIC, &[])
            .ok();
        let running_task = RunningTask {
            done,
            handle: AbortOnDropHandle::new(handle),
            kind: task_kind,
            task,
            cancellation_token,
            turn_context: Arc::clone(&turn_context),
            _agent_execution_guard: agent_execution_guard,
            _diagnostics_guard: ACTIVE_TURNS.track(),
            _timer: timer,
        };
        turn.task = Some(running_task);
        true
    }

    /// Returns whether an extension has marked this thread as durably asleep.
    pub(crate) fn has_outstanding_durable_sleep(&self) -> bool {
        self.services
            .thread_extension_data
            .get::<codex_extension_items::sleep::SleepItem>()
            .is_some()
    }

    /// Starts a regular turn when the session is idle and pending work is waiting.
    ///
    /// Pending work includes mailbox mail marked with `trigger_turn`, or any mailbox mail while
    /// an outstanding durable sleep is attached to the thread.
    ///
    /// This helper generates a fresh sub-id for the synthetic turn before delegating to the
    /// explicit-sub-id variant.
    pub(crate) fn maybe_start_turn_for_pending_work(self: &Arc<Self>) -> BoxFuture<'static, ()> {
        let session = Arc::clone(self);
        Box::pin(async move {
            session
                .maybe_start_turn_for_pending_work_with_sub_id(uuid::Uuid::new_v4().to_string())
                .await;
        })
    }

    /// Starts a regular turn with the provided sub-id when pending work should wake an idle
    /// session.
    ///
    /// The turn is created only when the session is idle and mailbox mail either requests a turn,
    /// a runtime exec-completion entry is pending, or mail can wake an outstanding durable sleep.
    /// A reservation lost to a concurrent submission backs off instead of
    /// starting on the winner's turn; taken leases fail back for a later wake.
    pub(crate) async fn maybe_start_turn_for_pending_work_with_sub_id(
        self: &Arc<Self>,
        sub_id: String,
    ) {
        if !self.input_queue.has_pending_mailbox_items().await
            || (!self.input_queue.has_trigger_turn_mailbox_items().await
                && !self.has_outstanding_durable_sleep())
        {
            return;
        }

        let turn_state = {
            let mut active_turn = self.active_turn.lock().await;
            if active_turn.is_some() {
                return;
            }
            let active_turn = active_turn.get_or_insert_with(ActiveTurn::default);
            Arc::clone(&active_turn.turn_state)
        };

        self.services
            .models_manager
            .refresh_after_auth_change(self.get_config().await.http_client_factory())
            .await;
        // A completion-triggered wakeup can be interrupted while discovery waits.
        if self
            .active_turn
            .lock()
            .await
            .as_ref()
            .is_none_or(|turn| !Arc::ptr_eq(&turn.turn_state, &turn_state))
        {
            return;
        }
        let (input, mut start_options) =
            self.input_queue.get_pending_input(&self.active_turn).await;
        let runtime_leases = self
            .input_queue
            .lease_runtime_notifications_up_to(MAX_EXEC_COMPLETION_FRAGMENTS_PER_REQUEST)
            .await;
        // Test-only lease gate: a suite pauses a wake here, after leasing and
        // before attaching, so a winner turn can finish while the entry is
        // still leased. No gate is inserted in production; the check is a
        // no-op then. Leaseless wakes never wait, so the winner's own
        // teardown scheduler pass is unaffected.
        if !runtime_leases.is_empty()
            && let Some(gate) = self
                .services
                .thread_extension_data
                .get::<TestWakeLeaseGate>()
        {
            gate.signal_wake_arrived();
            gate.wait_release().await;
        }
        let has_trigger_mail = input.iter().any(
            |item| matches!(item, TurnInput::InterAgentCommunication(mail) if mail.trigger_turn),
        );
        if !has_trigger_mail {
            // Queue-only mail wakes durable sleep without selecting a new task's settings.
            // Runtime wakes likewise preserve the thread's execution settings.
            start_options.cyber_access_program = self
                .reference_context_item()
                .await
                .and_then(|context| context.cyber_access_program);
            if !runtime_leases.is_empty() {
                start_options.turn_trigger =
                    Some(crate::session::runtime_mailbox::EXEC_COMPLETION_TURN_TRIGGER.to_string());
            }
        }
        let turn_context = self
            .new_turn_with_default_settings(
                sub_id,
                NewTurnContextOptions {
                    final_output_json_schema: start_options.final_output_json_schema,
                    cyber_access_program: start_options.cyber_access_program,
                },
            )
            .await;
        if let Some(trigger) = start_options.turn_trigger {
            turn_context.turn_metadata_state.set_turn_trigger(trigger);
        }
        if let Some(id) = start_options.parent_turn_id {
            if let Some(initiating_agent_path) = input.iter().find_map(|item| {
                let TurnInput::InterAgentCommunication(communication) = item else {
                    return None;
                };
                communication
                    .trigger_turn
                    .then(|| communication.author.clone())
            }) {
                turn_context
                    .turn_metadata_state
                    .set_initiating_agent_path(initiating_agent_path);
            }
            turn_context.turn_metadata_state.set_parent_turn_id(id);
        }
        if let Some(id) = start_options.root_turn_id {
            turn_context.turn_metadata_state.set_root_turn_id(id);
        }
        self.maybe_emit_model_warnings_for_turn(turn_context.as_ref())
            .await;
        // Task completion must still save this mail if pre-turn compaction fails.
        self.input_queue
            .extend_pending_input_for_turn_state(turn_state.as_ref(), input)
            .await;
        // Leased completions ride as one internal input; the record path
        // renders them as contextual items. Entries beyond the cap stay
        // retained in the mailbox for a later wake.
        if !runtime_leases.is_empty() {
            self.input_queue
                .extend_pending_input_for_turn_state(
                    turn_state.as_ref(),
                    vec![TurnInput::ExecCompletion(runtime_leases.clone())],
                )
                .await;
        }
        if !self
            .start_task(
                turn_context,
                Vec::new(),
                RegularTask::new(),
                TurnStartClaim::Reserved(turn_state),
                /*goal_admitted_revision*/ None,
            )
            .await
        {
            // A concurrent submission aborted our bare reservation and won
            // the turn. Its abort path fails attached leases; anything
            // taken but not yet attached is failed here so a later wake
            // retries it instead of stranding it leased. Failing an
            // already-failed lease is a no-op.
            exec_completion_ack::fail_leases(self, /*turn_context*/ None, &runtime_leases).await;
            if !runtime_leases.is_empty() {
                // The winner may already have finished: its teardown
                // scheduler skipped these entries while they were still
                // leased, so without a pass here the fail-back leaves an
                // idle session with pending receipts and no future wake.
                // This cannot double-wake a live winner (the scheduler
                // returns on an active turn) nor spin on suspended entries
                // (they no longer count as pending).
                self.maybe_start_turn_for_pending_work().await;
            }
        }
    }

    pub async fn abort_all_tasks(self: &Arc<Self>, reason: TurnAbortReason) {
        let mut aborted_turn = false;
        let mut active_turn_to_clear = None;
        let mut turn_context = None;
        let mut dropped_leases = Vec::new();
        if let Some(mut active_turn) = self.take_active_turn(&reason).await {
            // Leases that never reached the record path must fail rather than
            // drop with the turn, or their entries stay leased forever.
            dropped_leases = self
                .input_queue
                .take_pending_exec_completion_leases(&active_turn)
                .await;
            let task = active_turn.task.take();
            aborted_turn = task.is_some();
            turn_context = task.as_ref().map(|task| Arc::clone(&task.turn_context));
            if let Some(task) = task {
                self.handle_task_abort(
                    task,
                    reason.clone(),
                    &active_turn.turn_state,
                    /*error*/ None,
                )
                .await;
            }
            if aborted_turn {
                active_turn_to_clear = Some(active_turn);
            }
        }

        if let Some(turn_context) = turn_context.as_deref() {
            self.emit_turn_abort_lifecycle(reason.clone(), turn_context.extension_data.as_ref())
                .await;
        }
        if let Some(active_turn) = active_turn_to_clear {
            // Let interrupted tasks observe cancellation before dropping pending approvals, or an
            // in-flight approval wait can surface as a model-visible rejection before TurnAborted.
            self.input_queue.clear_pending(&active_turn).await;
        }
        exec_completion_ack::fail_leases(self, turn_context.as_deref(), &dropped_leases).await;
        if reason == TurnAbortReason::Interrupted && aborted_turn {
            self.maybe_start_turn_for_pending_work().await;
        }
    }

    pub(crate) async fn abort_turn_if_active(
        self: &Arc<Self>,
        turn_id: &str,
        reason: TurnAbortReason,
        error: Option<ErrorEvent>,
    ) -> bool {
        let active_turn = {
            let mut active = self.active_turn.lock().await;
            if active
                .as_ref()
                .and_then(|active_turn| active_turn.task.as_ref())
                .is_some_and(|task| task.turn_context.sub_id == turn_id)
            {
                if matches!(
                    reason,
                    TurnAbortReason::Interrupted | TurnAbortReason::BudgetLimited
                ) {
                    self.mark_interrupted();
                }
                active.take()
            } else {
                None
            }
        };
        let Some(active_turn) = active_turn else {
            return false;
        };

        self.finish_turn_abort(active_turn, reason, error).await;
        true
    }

    pub(crate) async fn finish_turn_abort(
        self: &Arc<Self>,
        mut active_turn: ActiveTurn,
        reason: TurnAbortReason,
        error: Option<ErrorEvent>,
    ) {
        // Leases that never reached the record path must fail rather than
        // drop with the turn, or their entries stay leased forever.
        let dropped_leases = self
            .input_queue
            .take_pending_exec_completion_leases(&active_turn)
            .await;
        let task = active_turn.task.take();
        let turn_context = task.as_ref().map(|task| Arc::clone(&task.turn_context));
        if let Some(task) = task {
            self.handle_task_abort(task, reason.clone(), &active_turn.turn_state, error)
                .await;
        }
        if let Some(turn_context) = turn_context.as_deref() {
            self.emit_turn_abort_lifecycle(reason.clone(), turn_context.extension_data.as_ref())
                .await;
        }
        // Let interrupted tasks observe cancellation before dropping pending approvals, or an
        // in-flight approval wait can surface as a model-visible rejection before TurnAborted.
        self.input_queue.clear_pending(&active_turn).await;
        exec_completion_ack::fail_leases(self, turn_context.as_deref(), &dropped_leases).await;

        if reason == TurnAbortReason::Interrupted {
            self.maybe_start_turn_for_pending_work().await;
        }
    }

    pub async fn on_task_finished(
        self: &Arc<Self>,
        turn_context: Arc<TurnContext>,
        task_result: SessionTaskResult,
    ) {
        let (last_agent_message, abort_reason) = match task_result {
            Ok(last_agent_message) => (last_agent_message, None),
            Err(err) if matches!(err.details(), CodexErrorDetails::TurnAborted) => {
                (None, Some(TurnAbortReason::Interrupted))
            }
            Err(err) => {
                warn!(%err, "session task returned an unexpected error");
                self.emit_turn_error_lifecycle(
                    turn_context.as_ref(),
                    err.to_codex_protocol_error(),
                    err.details(),
                )
                .await;
                self.track_turn_codex_error(turn_context.as_ref(), &err);
                self.send_event(
                    turn_context.as_ref(),
                    EventMsg::Error(err.to_error_event(/*message_prefix*/ None)),
                )
                .await;
                (None, None)
            }
        };
        turn_context
            .turn_metadata_state
            .cancel_git_enrichment_task();

        let turn_state = {
            let mut active = self.active_turn.lock().await;
            active.as_mut().and_then(|active_turn| {
                let task = active_turn.task.take()?;
                task.handle.detach();
                Some(Arc::clone(&active_turn.turn_state))
            })
        };
        let Some(turn_state) = turn_state else {
            return;
        };
        let pending_input = self
            .input_queue
            .take_pending_input_for_turn_state(turn_state.as_ref())
            .await;
        let (
            turn_had_memory_citation,
            turn_tool_calls,
            token_usage_at_turn_start,
            token_usage_by_model,
        ) = {
            let mut ts = turn_state.lock().await;
            (
                ts.has_memory_citation,
                ts.tool_calls,
                ts.token_usage_at_turn_start.clone(),
                std::mem::take(&mut ts.token_usage_by_model),
            )
        };
        run_hooks_and_record_inputs(
            self,
            &turn_context,
            &turn_context.capture_current_model_info(),
            &pending_input,
            PersistContext::Standard,
        )
        .await;
        // Leases recorded but never observed in a submitted prompt return to
        // unleased so a later wake retries them; entries that exhaust their
        // sampling budget suspend visibly instead of spinning further wakes.
        // This must precede the idle wake below so retries are re-offered.
        exec_completion_ack::fail_unsubmitted(self, &turn_context).await;
        let turn_telemetry = &turn_context.session_telemetry;
        // Emit token usage metrics.
        {
            // TODO(jif): drop this
            let tmp_mem = (
                "tmp_mem_enabled",
                if self.enabled(Feature::MemoryTool) {
                    "true"
                } else {
                    "false"
                },
            );
            let network_proxy = self.services.network_proxy.load_full();
            let network_proxy_active = match network_proxy.as_ref() {
                Some(started_network_proxy) => {
                    match started_network_proxy.proxy().current_cfg().await {
                        Ok(config) => config.enabled,
                        Err(err) => {
                            warn!(
                                "failed to read managed network proxy state for turn metrics: {err:#}"
                            );
                            false
                        }
                    }
                }
                None => false,
            };
            emit_turn_network_proxy_metric(turn_telemetry, network_proxy_active, tmp_mem);
            turn_telemetry.histogram(
                TURN_TOOL_CALL_METRIC,
                i64::try_from(turn_tool_calls).unwrap_or(i64::MAX),
                &[tmp_mem],
            );
            let total_token_usage = self.total_token_usage().await.unwrap_or_default();
            let turn_token_usage = TokenUsage {
                input_tokens: (total_token_usage.input_tokens
                    - token_usage_at_turn_start.input_tokens)
                    .max(0),
                cached_input_tokens: (total_token_usage.cached_input_tokens
                    - token_usage_at_turn_start.cached_input_tokens)
                    .max(0),
                cache_write_input_tokens: (total_token_usage.cache_write_input_tokens
                    - token_usage_at_turn_start.cache_write_input_tokens)
                    .max(0),
                output_tokens: (total_token_usage.output_tokens
                    - token_usage_at_turn_start.output_tokens)
                    .max(0),
                reasoning_output_tokens: (total_token_usage.reasoning_output_tokens
                    - token_usage_at_turn_start.reasoning_output_tokens)
                    .max(0),
                total_tokens: (total_token_usage.total_tokens
                    - token_usage_at_turn_start.total_tokens)
                    .max(0),
                codex_rollout_budget_units: None,
            };
            let current_span = Span::current();
            current_span.record(
                "codex.turn.token_usage.input_tokens",
                turn_token_usage.input_tokens,
            );
            current_span.record(
                "codex.turn.token_usage.cached_input_tokens",
                turn_token_usage.cached_input(),
            );
            current_span.record(
                "codex.turn.token_usage.cache_write_input_tokens",
                turn_token_usage.cache_write_input_tokens,
            );
            current_span.record(
                "codex.turn.token_usage.non_cached_input_tokens",
                turn_token_usage.non_cached_input(),
            );
            current_span.record(
                "codex.turn.token_usage.output_tokens",
                turn_token_usage.output_tokens,
            );
            current_span.record(
                "codex.turn.token_usage.reasoning_output_tokens",
                turn_token_usage.reasoning_output_tokens,
            );
            current_span.record(
                "codex.turn.token_usage.total_tokens",
                turn_token_usage.total_tokens,
            );
            self.services
                .analytics_events_client
                .track_turn_token_usage(TurnTokenUsageFact {
                    turn_id: turn_context.sub_id.clone(),
                    thread_id: self.thread_id.to_string(),
                    token_usage: turn_token_usage,
                });
            token_usage_by_model.emit(turn_telemetry, tmp_mem);
        }
        emit_turn_memory_metric(
            turn_telemetry,
            turn_context.config.features.enabled(Feature::MemoryTool),
            turn_context.config.memories.use_memories,
            turn_had_memory_citation,
        );
        turn_telemetry.counter(
            TURN_UNIFIED_EXEC_RUNNING_PROCESSES_METRIC,
            i64::try_from(self.list_background_terminals().await.len()).unwrap_or(i64::MAX),
            &[],
        );
        let started_at = turn_context.turn_timing_state.started_at_unix_secs().await;
        let (completed_at, duration_ms, profile) = turn_context
            .turn_timing_state
            .complete_profile_and_duration_ms()
            .await;
        self.services
            .analytics_events_client
            .track_turn_profile(TurnProfileFact {
                turn_id: turn_context.sub_id.clone(),
                profile,
            });
        let idle_cause = if matches!(
            abort_reason.as_ref(),
            Some(TurnAbortReason::Interrupted | TurnAbortReason::BudgetLimited)
        ) {
            ThreadIdleCause::Interrupted
        } else if abort_reason.is_none() && turn_context.terminal_error.lock().await.is_some() {
            ThreadIdleCause::Failed
        } else {
            ThreadIdleCause::Completed
        };
        let event = if let Some(reason) = abort_reason {
            if reason == TurnAbortReason::Interrupted {
                run_turn_interrupt_hooks(self, &turn_context, &turn_state).await;
            }
            self.emit_turn_abort_lifecycle(reason.clone(), turn_context.extension_data.as_ref())
                .await;
            EventMsg::TurnAborted(TurnAbortedEvent {
                turn_id: Some(turn_context.sub_id.clone()),
                reason,
                error: None,
                started_at,
                completed_at,
                duration_ms,
            })
        } else {
            let time_to_first_token_ms = turn_context
                .turn_timing_state
                .time_to_first_token_ms()
                .await;
            let error = turn_context.terminal_error.lock().await.clone();
            self.emit_turn_stop_lifecycle(turn_context.extension_data.as_ref())
                .await;
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: turn_context.sub_id.clone(),
                last_agent_message,
                error,
                started_at,
                completed_at,
                duration_ms,
                time_to_first_token_ms,
            })
        };
        let saved_guardian_completion =
            matches!(event, EventMsg::TurnComplete(_)) && self.is_private_guardian_reviewer().await;
        if !saved_guardian_completion {
            self.send_event(turn_context.as_ref(), event.clone()).await;
        }

        let cleared_active_turn = {
            let mut active = self.active_turn.lock().await;
            if let Some(active_turn) = active.as_ref()
                && active_turn.task.is_none()
                && Arc::ptr_eq(&active_turn.turn_state, &turn_state)
            {
                *active = None;
                true
            } else {
                false
            }
        };
        if saved_guardian_completion {
            // The parent can request another review as soon as it receives this event.
            self.send_event(turn_context.as_ref(), event).await;
        }
        if cleared_active_turn {
            self.emit_thread_idle_lifecycle_if_idle(idle_cause).await;
        }
        // Private reviewers already flushed the terminal event before delivering it.
        // Other buffering writers still need a barrier for the terminal event.
        if !saved_guardian_completion && let Err(err) = self.flush_rollout().await {
            warn!("failed to flush rollout after emitting terminal turn event: {err}");
        }
        if cleared_active_turn {
            self.maybe_start_turn_for_pending_work().await;
            // Test-only scheduler signal: a suite waiting on the wake lease
            // gate observes the winner's teardown scheduler pass here. No
            // gate is inserted in production; the check is a no-op then.
            if let Some(gate) = self
                .services
                .thread_extension_data
                .get::<TestWakeLeaseGate>()
            {
                gate.signal_scheduler_done();
            }
        }
    }

    async fn take_active_turn(&self, reason: &TurnAbortReason) -> Option<ActiveTurn> {
        let mut active = self.active_turn.lock().await;
        if matches!(
            reason,
            TurnAbortReason::Interrupted | TurnAbortReason::BudgetLimited
        ) && active
            .as_ref()
            .is_some_and(|active_turn| active_turn.task.is_some())
        {
            self.mark_interrupted();
        }
        active.take()
    }

    pub(crate) async fn close_unified_exec_processes(&self) {
        self.services
            .unified_exec_manager
            .terminate_all_processes()
            .await;
    }

    pub(crate) async fn list_background_terminals(&self) -> Vec<BackgroundTerminalInfo> {
        self.services.unified_exec_manager.list_processes().await
    }

    pub(crate) async fn terminate_background_terminal(&self, process_id: i32) -> bool {
        self.services
            .unified_exec_manager
            .terminate_process(process_id)
            .await
    }

    async fn handle_task_abort(
        self: &Arc<Self>,
        task: RunningTask,
        reason: TurnAbortReason,
        turn_state: &Mutex<TurnState>,
        error: Option<ErrorEvent>,
    ) {
        let sub_id = task.turn_context.sub_id.clone();
        if task.cancellation_token.is_cancelled() {
            return;
        }

        trace!(task_kind = ?task.kind, sub_id, "aborting running task");
        task.cancellation_token.cancel();
        if reason == TurnAbortReason::Interrupted
            && task
                .turn_context
                .config
                .features
                .enabled(Feature::CodeModeInterrupt)
        {
            self.services
                .code_mode_service
                .interrupt_active_cells()
                .await;
        }
        task.turn_context
            .turn_metadata_state
            .cancel_git_enrichment_task();
        let session_task = task.task;

        select! {
            _ = task.done.notified() => {
            },
            _ = tokio::time::sleep(Duration::from_millis(GRACEFULL_INTERRUPTION_TIMEOUT_MS)) => {
                warn!("task {sub_id} didn't complete gracefully after {}ms", GRACEFULL_INTERRUPTION_TIMEOUT_MS);
            }
        }

        task.handle.abort();

        session_task
            .abort(Arc::clone(self), Arc::clone(&task.turn_context))
            .await;

        if reason == TurnAbortReason::Interrupted
            && let Some(marker) = interrupted_turn_history_marker(
                InterruptedTurnHistoryMarker::from_config_and_version(
                    task.turn_context.config.as_ref(),
                    task.turn_context.multi_agent_version,
                ),
            )
        {
            self.record_conversation_items(
                task.turn_context.as_ref(),
                task.turn_context.model_info(),
                std::slice::from_ref(&marker),
            )
            .await;
            // Ensure the marker is durably visible before emitting TurnAborted: some clients
            // synchronously re-read the rollout on receipt of the abort event.
            if let Err(err) = self.flush_rollout().await {
                warn!("failed to flush interrupted-turn marker before emitting TurnAborted: {err}");
            }
        }

        if reason == TurnAbortReason::Interrupted {
            run_turn_interrupt_hooks(self, &task.turn_context, turn_state).await;
        }

        let started_at = task
            .turn_context
            .turn_timing_state
            .started_at_unix_secs()
            .await;
        let (completed_at, duration_ms, profile) = task
            .turn_context
            .turn_timing_state
            .complete_profile_and_duration_ms()
            .await;
        self.services
            .analytics_events_client
            .track_turn_profile(TurnProfileFact {
                turn_id: task.turn_context.sub_id.clone(),
                profile,
            });
        // An aborted submission is not a sampling proof: recorded leases
        // return to unleased so the post-abort wake can retry them.
        exec_completion_ack::fail_unsubmitted(self, &task.turn_context).await;
        let event = EventMsg::TurnAborted(TurnAbortedEvent {
            turn_id: Some(task.turn_context.sub_id.clone()),
            reason,
            error,
            started_at,
            completed_at,
            duration_ms,
        });
        self.send_event(task.turn_context.as_ref(), event).await;
        // Regular items were flushed before this terminal event was appended; buffering
        // thread writers may not flush it without another explicit barrier.
        if let Err(err) = self.flush_rollout().await {
            warn!("failed to flush rollout after emitting terminal turn event: {err}");
        }
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
