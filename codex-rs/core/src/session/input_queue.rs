use super::runtime_mailbox::RuntimeLease;
use super::runtime_mailbox::RuntimeMailbox;
use crate::state::ActiveTurn;
use crate::state::MailboxDeliveryPhase;
use crate::state::TurnState;
use crate::unified_exec::completion_receipt::ReceiptId;
use crate::unified_exec::completion_receipt::ReceiptOwner;
use codex_diagnostics::Gauge;
use codex_diagnostics::GaugeGuard;
use codex_history::ResponseItemEnvelope;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::turn_input::TurnStartOptions;
use codex_protocol::user_input::UserInput;
use serde::Deserialize;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

static PENDING_MAILBOX_MESSAGES: Gauge = Gauge::new("core.mailbox.pending");

/// Host capture metadata belonging to one input, including steers within another turn.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserInputMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_order: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "codex_history::UserInputOrigin::is_user"
    )]
    pub origin: codex_history::UserInputOrigin,
}

/// Input consumed by a regular turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TurnInput {
    UserInput {
        content: Vec<UserInput>,
        client_id: Option<String>,
        #[serde(flatten)]
        metadata: UserInputMetadata,
    },
    FunctionCallOutput(#[serde(with = "turn_input_response_item")] ResponseItemEnvelope),
    // Preserve the existing serialized format while carrying injection API metadata
    // through the in-memory queue.
    ResponseItem(#[serde(with = "turn_input_response_item")] ResponseItemEnvelope),
    InterAgentCommunication(InterAgentCommunication),
}

mod turn_input_response_item {
    use super::ResponseItem;
    use super::ResponseItemEnvelope;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;
    use serde::ser::Error as _;

    pub(super) fn serialize<S>(
        item: &ResponseItemEnvelope,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if item.metadata.is_some() {
            return Err(S::Error::custom(
                "annotated response items cannot cross the turn-input serialization boundary",
            ));
        }
        item.item.serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<ResponseItemEnvelope, D::Error>
    where
        D: Deserializer<'de>,
    {
        ResponseItem::deserialize(deserializer).map(ResponseItemEnvelope::new)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InputQueueActivity {
    Mailbox,
    Steer,
}

/// Turn-local pending input storage owned by the input queue flow.
#[derive(Default)]
pub(crate) struct TurnInputQueue {
    items: Vec<TurnInput>,
}

/// Session-scoped pending input storage and active-turn mailbox delivery coordination.
pub(crate) struct InputQueue {
    activity_tx: watch::Sender<InputQueueActivity>,
    mailbox_pending_mails: Mutex<VecDeque<PendingMailboxCommunication>>,
    runtime_notifications: Mutex<RuntimeMailbox>,
}

struct PendingMailboxCommunication {
    communication: InterAgentCommunication,
    start_options: TurnStartOptions,
    _diagnostics_guard: GaugeGuard,
}

impl InputQueue {
    pub(crate) fn new() -> Self {
        let (activity_tx, _) = watch::channel(InputQueueActivity::Mailbox);
        Self {
            activity_tx,
            mailbox_pending_mails: Mutex::new(VecDeque::new()),
            runtime_notifications: Mutex::new(RuntimeMailbox::new()),
        }
    }

    pub(crate) async fn subscribe_activity(
        &self,
        turn_state: Option<&Mutex<TurnState>>,
    ) -> (
        watch::Receiver<InputQueueActivity>,
        Option<InputQueueActivity>,
    ) {
        let activity_rx = self.activity_tx.subscribe();
        let turn_activity = if let Some(turn_state) = turn_state {
            turn_state.lock().await.pending_input.pending_activity()
        } else {
            None
        };
        let pending_activity = if let Some(activity) = turn_activity {
            Some(activity)
        } else if self.has_pending_mailbox_items().await {
            Some(InputQueueActivity::Mailbox)
        } else {
            None
        };
        (activity_rx, pending_activity)
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "active turn checks and turn state updates must remain atomic"
    )]
    pub(crate) async fn deliver_mailbox_communication_to_current_turn(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        communication: InterAgentCommunication,
    ) -> bool {
        let active = active_turn.lock().await;
        let Some(active_turn) = active.as_ref().filter(|turn| turn.task.is_some()) else {
            return false;
        };
        let mut turn_state = active_turn.turn_state.lock().await;
        if !turn_state.accepts_mailbox_delivery_for_current_turn() {
            return false;
        }
        turn_state
            .pending_input
            .items
            .push(TurnInput::InterAgentCommunication(communication));
        self.activity_tx.send_replace(InputQueueActivity::Mailbox);
        true
    }

    pub(crate) async fn enqueue_mailbox_communication(
        &self,
        communication: InterAgentCommunication,
        start_options: TurnStartOptions,
    ) {
        self.mailbox_pending_mails
            .lock()
            .await
            .push_back(PendingMailboxCommunication {
                communication,
                start_options,
                _diagnostics_guard: PENDING_MAILBOX_MESSAGES.track(),
            });
        self.activity_tx.send_replace(InputQueueActivity::Mailbox);
    }

    /// Enqueues an internal runtime notification for an exec completion.
    ///
    /// Returns `false` without duplicating when the receipt is already
    /// present. Callers start idle work via `maybe_start_turn_for_pending_work`.
    pub(crate) async fn enqueue_runtime_notification(
        &self,
        receipt_id: ReceiptId,
        owner: ReceiptOwner,
    ) -> bool {
        let enqueued = self
            .runtime_notifications
            .lock()
            .await
            .enqueue(receipt_id, owner);
        if enqueued {
            self.activity_tx.send_replace(InputQueueActivity::Mailbox);
        }
        enqueued
    }

    /// Leases unleased, non-suspended runtime entries without removing them.
    ///
    /// Idle wake hands these leases to the turn starter. Active-turn delivery
    /// arrives with the internal `TurnInput` variant in leaf 2.
    pub(crate) async fn lease_runtime_notifications(&self) -> Vec<RuntimeLease> {
        self.runtime_notifications.lock().await.lease_available()
    }

    /// Acknowledges a runtime lease after its contents were sampled.
    ///
    /// Production callers arrive with sampling acknowledgement (story D).
    #[allow(dead_code)]
    pub(crate) async fn acknowledge_runtime_lease(&self, lease: &RuntimeLease) -> bool {
        self.runtime_notifications.lock().await.acknowledge(lease)
    }

    /// Returns a runtime lease to the unleased state after a failed attempt.
    ///
    /// Production callers arrive with sampling acknowledgement (story D).
    #[allow(dead_code)]
    pub(crate) async fn fail_runtime_lease(&self, lease: &RuntimeLease) -> bool {
        let failed = self.runtime_notifications.lock().await.fail(lease);
        if failed {
            self.activity_tx.send_replace(InputQueueActivity::Mailbox);
        }
        failed
    }

    /// Removes a runtime entry whether leased or not.
    ///
    /// Production callers arrive with receipt cancellation (story E).
    #[allow(dead_code)]
    pub(crate) async fn cancel_runtime_notification(&self, receipt_id: ReceiptId) -> bool {
        self.runtime_notifications.lock().await.cancel(receipt_id)
    }

    /// Marks a runtime entry suspended after retries are exhausted.
    ///
    /// Production callers arrive with bounded sampling retries (story D).
    #[allow(dead_code)]
    pub(crate) async fn suspend_runtime_notification(&self, receipt_id: ReceiptId) -> bool {
        self.runtime_notifications.lock().await.suspend(receipt_id)
    }

    pub(crate) async fn has_pending_mailbox_items(&self) -> bool {
        if !self.mailbox_pending_mails.lock().await.is_empty() {
            return true;
        }
        self.runtime_notifications.lock().await.has_pending()
    }

    pub(crate) async fn has_trigger_turn_mailbox_items(&self) -> bool {
        if self
            .mailbox_pending_mails
            .lock()
            .await
            .iter()
            .any(|mail| mail.communication.trigger_turn)
        {
            return true;
        }
        self.runtime_notifications.lock().await.has_trigger()
    }

    /// Drains inter-agent mail, removing entries. Runtime notifications are
    /// leased separately via [`Self::lease_runtime_notifications`] and stay
    /// until acknowledged or cancelled.
    pub(crate) async fn drain_mailbox_input_items(&self) -> (Vec<TurnInput>, TurnStartOptions) {
        let pending_mails = self
            .mailbox_pending_mails
            .lock()
            .await
            .drain(..)
            .collect::<Vec<_>>();
        // A later follow-up supersedes the earlier choice, including an omitted choice.
        let mut start_options = pending_mails
            .iter()
            .rev()
            .find(|mail| mail.communication.trigger_turn)
            .map(|mail| mail.start_options.clone())
            .unwrap_or_default();
        start_options.parent_turn_id = pending_mails
            .iter()
            .filter(|mail| mail.communication.trigger_turn)
            .map(|mail| mail.start_options.parent_turn_id.as_deref())
            .reduce(|expected, candidate| expected.filter(|id| candidate == Some(*id)))
            .and_then(|id| id.filter(|id| !id.trim().is_empty()).map(str::to_string));
        start_options.root_turn_id = pending_mails
            .iter()
            .find(|mail| mail.communication.trigger_turn)
            .and_then(|mail| {
                mail.start_options
                    .parent_turn_id
                    .as_deref()
                    .filter(|id| !id.trim().is_empty())
                    .and(mail.start_options.root_turn_id.as_deref())
                    .filter(|id| !id.trim().is_empty())
            })
            .map(str::to_string);
        let items = pending_mails
            .into_iter()
            .map(|mail| TurnInput::InterAgentCommunication(mail.communication))
            .collect();
        (items, start_options)
    }

    pub(crate) async fn turn_state_for_sub_id(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        sub_id: &str,
    ) -> Option<Arc<Mutex<TurnState>>> {
        let active = active_turn.lock().await;
        active.as_ref().and_then(|active_turn| {
            active_turn
                .task
                .as_ref()
                .is_some_and(|task| task.turn_context.sub_id == sub_id)
                .then(|| Arc::clone(&active_turn.turn_state))
        })
    }

    /// Signal once a user message is queued for this sampling request.
    pub(crate) async fn watch_user_input(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        sub_id: &str,
        interrupt: CancellationToken,
    ) -> Option<AbortOnDropHandle<()>> {
        let turn_state = self.turn_state_for_sub_id(active_turn, sub_id).await?;
        // Subscribe before inspecting the queue so an arrival cannot be missed.
        let mut activity = self.activity_tx.subscribe();
        Some(AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                if turn_state.lock().await.pending_input.has_user_input() {
                    interrupt.cancel();
                    return;
                }
                if activity.changed().await.is_err() {
                    return;
                }
            }
        })))
    }

    /// Clear any pending waiters and input buffered for the current turn.
    pub(crate) async fn clear_pending(&self, active_turn: &ActiveTurn) {
        let mut turn_state = active_turn.turn_state.lock().await;
        turn_state.clear_pending_waiters();
        turn_state.pending_input.items.clear();
    }

    pub(crate) async fn defer_mailbox_delivery_to_next_turn(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        sub_id: &str,
    ) {
        let turn_state = self.turn_state_for_sub_id(active_turn, sub_id).await;
        let Some(turn_state) = turn_state else {
            return;
        };
        let mut turn_state = turn_state.lock().await;
        // Explicit same-turn work still needs a follow-up. Queue-only child mail does not: keep
        // it pending so task completion records it for the next turn without sampling again.
        if turn_state.pending_input.items.iter().any(|input| {
            !matches!(
                input,
                TurnInput::InterAgentCommunication(communication) if !communication.trigger_turn
            )
        }) {
            return;
        }
        turn_state.set_mailbox_delivery_phase(MailboxDeliveryPhase::NextTurn);
    }

    pub(crate) async fn accept_mailbox_delivery_for_current_turn(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        sub_id: &str,
    ) {
        let turn_state = self.turn_state_for_sub_id(active_turn, sub_id).await;
        let Some(turn_state) = turn_state else {
            return;
        };
        self.accept_mailbox_delivery_for_turn_state(turn_state.as_ref())
            .await;
    }

    pub(super) async fn accept_mailbox_delivery_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
    ) {
        turn_state
            .lock()
            .await
            .accept_mailbox_delivery_for_current_turn();
    }

    pub(super) async fn extend_pending_input_and_accept_mailbox_delivery_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
        input: Vec<TurnInput>,
    ) {
        {
            let mut turn_state = turn_state.lock().await;
            turn_state.pending_input.items.extend(input);
            turn_state.accept_mailbox_delivery_for_current_turn();
        }
        self.activity_tx.send_replace(InputQueueActivity::Steer);
    }

    pub(crate) async fn extend_pending_input_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
        input: Vec<TurnInput>,
    ) {
        turn_state.lock().await.pending_input.items.extend(input);
    }

    pub(crate) async fn take_pending_input_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
    ) -> Vec<TurnInput> {
        turn_state.lock().await.pending_input.items.split_off(0)
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "active turn checks and turn state updates must remain atomic"
    )]
    pub(crate) async fn get_pending_input(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
    ) -> (Vec<TurnInput>, TurnStartOptions) {
        let (pending_input, accepts_mailbox_delivery) = {
            let mut active = active_turn.lock().await;
            match active.as_mut() {
                Some(active_turn) => {
                    let mut turn_state = active_turn.turn_state.lock().await;
                    let accepts_mailbox_delivery =
                        turn_state.accepts_mailbox_delivery_for_current_turn();
                    let pending_input = if accepts_mailbox_delivery {
                        turn_state.pending_input.items.split_off(0)
                    } else {
                        Vec::new()
                    };
                    (pending_input, accepts_mailbox_delivery)
                }
                None => (Vec::new(), true),
            }
        };
        if !accepts_mailbox_delivery {
            return (pending_input, TurnStartOptions::default());
        }
        let (mailbox_items, start_options) = self.drain_mailbox_input_items().await;
        if pending_input.is_empty() {
            (mailbox_items, start_options)
        } else {
            let mut pending_input = pending_input;
            pending_input.extend(mailbox_items);
            (pending_input, start_options)
        }
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "active turn checks and turn state reads must remain atomic"
    )]
    pub(crate) async fn has_pending_input(&self, active_turn: &Mutex<Option<ActiveTurn>>) -> bool {
        let (has_turn_pending_input, accepts_mailbox_delivery) = {
            let active = active_turn.lock().await;
            match active.as_ref() {
                Some(active_turn) => {
                    let turn_state = active_turn.turn_state.lock().await;
                    (
                        !turn_state.pending_input.is_empty(),
                        turn_state.accepts_mailbox_delivery_for_current_turn(),
                    )
                }
                None => (false, true),
            }
        };
        if !accepts_mailbox_delivery {
            return false;
        }
        if has_turn_pending_input {
            return true;
        }
        self.has_pending_mailbox_items().await
    }
}

impl TurnInputQueue {
    fn has_user_input(&self) -> bool {
        self.items
            .iter()
            .any(|input| matches!(input, TurnInput::UserInput { .. }))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn pending_activity(&self) -> Option<InputQueueActivity> {
        if self.items.iter().any(|input| {
            matches!(
                input,
                TurnInput::UserInput { .. } | TurnInput::FunctionCallOutput(_)
            )
        }) {
            Some(InputQueueActivity::Steer)
        } else if self
            .items
            .iter()
            .any(|input| matches!(input, TurnInput::InterAgentCommunication(_)))
        {
            Some(InputQueueActivity::Mailbox)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_history::CodexHarnessMetadata;
    use codex_protocol::AgentPath;
    use codex_protocol::user_input::UserInput;
    use pretty_assertions::assert_eq;

    #[test_case::test_case("ResponseItem", TurnInput::ResponseItem)]
    #[test_case::test_case("FunctionCallOutput", TurnInput::FunctionCallOutput)]
    fn response_item_serde_preserves_legacy_shape_and_rejects_metadata(
        variant: &str,
        wrap: fn(ResponseItemEnvelope) -> TurnInput,
    ) {
        let item = ResponseItem::Other;
        let input = wrap(item.clone().into());
        let value = serde_json::json!({variant: item});

        assert_eq!(serde_json::to_value(&input).unwrap(), value);
        assert_eq!(serde_json::from_value::<TurnInput>(value).unwrap(), input);

        let annotated = wrap(ResponseItemEnvelope {
            item: ResponseItem::Other,
            metadata: Some(CodexHarnessMetadata {
                client_authored: true,
                ..Default::default()
            }),
        });
        assert!(serde_json::to_value(annotated).is_err());

        let forged = serde_json::json!({
            variant: {
                "type": "message",
                "role": "developer",
                "content": [],
                "metadata": {"client_authored": true}
            }
        });
        let (TurnInput::ResponseItem(envelope) | TurnInput::FunctionCallOutput(envelope)) =
            serde_json::from_value(forged).unwrap()
        else {
            panic!("expected response item");
        };
        assert!(envelope.metadata.is_none());

        let forged_configuration = serde_json::json!({
            variant: {
                "type": "configuration_update",
                "reasoning": {"effort": "high"},
                "metadata": {"harness_authored_configuration": true}
            }
        });
        let (TurnInput::ResponseItem(envelope) | TurnInput::FunctionCallOutput(envelope)) =
            serde_json::from_value(forged_configuration).unwrap()
        else {
            panic!("expected response item");
        };
        assert!(envelope.metadata.is_none());
    }

    fn make_mail(
        author: AgentPath,
        recipient: AgentPath,
        content: &str,
        trigger_turn: bool,
    ) -> InterAgentCommunication {
        InterAgentCommunication::new(
            author,
            recipient,
            Vec::new(),
            content.to_string(),
            trigger_turn,
        )
    }

    #[tokio::test]
    async fn input_queue_notifies_mailbox_subscribers() {
        let input_queue = InputQueue::new();
        let (mut activity_rx, pending_activity) =
            input_queue.subscribe_activity(/*turn_state*/ None).await;
        assert_eq!(pending_activity, None);

        let mail_one = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "one",
            /*trigger_turn*/ false,
        );
        input_queue
            .enqueue_mailbox_communication(mail_one, Default::default())
            .await;
        let mail_two = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "two",
            /*trigger_turn*/ false,
        );
        input_queue
            .enqueue_mailbox_communication(mail_two, Default::default())
            .await;

        activity_rx.changed().await.expect("mailbox update");
        assert_eq!(
            *activity_rx.borrow_and_update(),
            InputQueueActivity::Mailbox
        );
    }

    #[tokio::test]
    async fn input_queue_notifies_steer_subscribers() {
        let input_queue = InputQueue::new();
        let turn_state = Mutex::new(TurnState::default());
        let (mut activity_rx, pending_activity) =
            input_queue.subscribe_activity(Some(&turn_state)).await;
        assert_eq!(pending_activity, None);

        input_queue
            .extend_pending_input_and_accept_mailbox_delivery_for_turn_state(
                &turn_state,
                vec![TurnInput::UserInput {
                    metadata: Default::default(),
                    content: vec![UserInput::Text {
                        text: "steer".to_string(),
                        text_elements: Vec::new(),
                    }],
                    client_id: None,
                }],
            )
            .await;

        activity_rx.changed().await.expect("steer update");
        assert_eq!(*activity_rx.borrow_and_update(), InputQueueActivity::Steer);
    }

    #[tokio::test]
    async fn input_queue_reports_already_pending_steer() {
        let input_queue = InputQueue::new();
        let turn_state = Mutex::new(TurnState::default());
        let passive_output = serde_json::from_value(serde_json::json!({
            "ResponseItem": {"type": "function_call_output", "name": "notify", "output": "passive"}
        }))
        .unwrap();
        input_queue
            .extend_pending_input_for_turn_state(&turn_state, vec![passive_output])
            .await;
        assert_eq!(
            input_queue.subscribe_activity(Some(&turn_state)).await.1,
            None
        );
        let communication = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "already pending mail",
            /*trigger_turn*/ false,
        );
        input_queue
            .extend_pending_input_for_turn_state(
                &turn_state,
                vec![TurnInput::InterAgentCommunication(communication)],
            )
            .await;
        assert_eq!(
            input_queue.subscribe_activity(Some(&turn_state)).await.1,
            Some(InputQueueActivity::Mailbox)
        );
        input_queue
            .extend_pending_input_and_accept_mailbox_delivery_for_turn_state(
                &turn_state,
                vec![TurnInput::UserInput {
                    metadata: Default::default(),
                    content: vec![UserInput::Text {
                        text: "already pending".to_string(),
                        text_elements: Vec::new(),
                    }],
                    client_id: None,
                }],
            )
            .await;

        let (_activity_rx, pending_activity) =
            input_queue.subscribe_activity(Some(&turn_state)).await;

        assert_eq!(pending_activity, Some(InputQueueActivity::Steer));
    }

    #[tokio::test]
    async fn input_queue_drains_mailbox_in_delivery_order() {
        let input_queue = InputQueue::new();
        let mail_one = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "one",
            /*trigger_turn*/ false,
        );
        let mail_two = make_mail(
            AgentPath::try_from("/root/worker").expect("agent path"),
            AgentPath::root(),
            "two",
            /*trigger_turn*/ true,
        );

        input_queue
            .enqueue_mailbox_communication(mail_one.clone(), Default::default())
            .await;
        input_queue
            .enqueue_mailbox_communication(mail_two.clone(), Default::default())
            .await;

        assert_eq!(
            input_queue.drain_mailbox_input_items().await.0,
            vec![
                TurnInput::InterAgentCommunication(mail_one),
                TurnInput::InterAgentCommunication(mail_two)
            ]
        );
        assert!(!input_queue.has_pending_mailbox_items().await);
    }

    #[tokio::test]
    async fn input_queue_uses_unambiguous_trigger_parent_and_first_root() {
        let (parent, peer, root, root2) = (Some("a"), Some("b"), Some("r"), Some("s"));
        for (pending_mails, expected_parent_turn_id, expected_root_turn_id) in [
            (Vec::new(), None, None),
            (vec![(false, Some("q"), root)], None, None),
            (vec![(true, Some(""), root)], None, None),
            (vec![(true, Some("   "), root)], None, None),
            (vec![(true, None, root)], None, None),
            (vec![(true, parent, None)], parent, None),
            (vec![(true, parent, Some(""))], parent, None),
            (vec![(true, parent, root), (true, peer, root)], None, root),
            (vec![(true, parent, root), (true, peer, root2)], None, root),
            (vec![(true, parent, root), (true, None, root)], None, root),
            (
                vec![(true, parent, root), (true, parent, root)],
                parent,
                root,
            ),
            (
                vec![(false, Some("q"), root2), (true, parent, root)],
                parent,
                root,
            ),
        ] {
            let input_queue = InputQueue::new();
            for (trigger_turn, parent_turn_id, root_turn_id) in pending_mails {
                input_queue
                    .enqueue_mailbox_communication(
                        make_mail(AgentPath::root(), AgentPath::root(), "task", trigger_turn),
                        TurnStartOptions {
                            parent_turn_id: parent_turn_id.map(str::to_string),
                            root_turn_id: root_turn_id.map(str::to_string),
                            ..Default::default()
                        },
                    )
                    .await;
            }
            let (_, start_options) = input_queue.drain_mailbox_input_items().await;
            assert_eq!(
                start_options.parent_turn_id.as_deref(),
                expected_parent_turn_id
            );
            assert_eq!(start_options.root_turn_id.as_deref(), expected_root_turn_id);
        }
    }

    #[tokio::test]
    async fn input_queue_uses_latest_followup_choice_and_ignores_queue_only_mail() {
        use codex_protocol::turn_input::CyberAccessProgram;

        for latest in [Some(CyberAccessProgram::Standard), None] {
            let input_queue = InputQueue::new();
            for (trigger_turn, program) in [
                (true, Some(CyberAccessProgram::DaybreakBlue)),
                (true, latest),
                (false, Some(CyberAccessProgram::DaybreakRed)),
            ] {
                input_queue
                    .enqueue_mailbox_communication(
                        make_mail(AgentPath::root(), AgentPath::root(), "task", trigger_turn),
                        TurnStartOptions {
                            cyber_access_program: program,
                            ..Default::default()
                        },
                    )
                    .await;
            }
            let (_, start_options) = input_queue.drain_mailbox_input_items().await;
            assert_eq!(start_options.cyber_access_program, latest);
        }
    }

    #[tokio::test]
    async fn input_queue_tracks_pending_trigger_turn_mail() {
        let input_queue = InputQueue::new();

        let queued_mail = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "queued",
            /*trigger_turn*/ false,
        );
        input_queue
            .enqueue_mailbox_communication(queued_mail, Default::default())
            .await;
        assert!(!input_queue.has_trigger_turn_mailbox_items().await);

        let trigger_mail = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "wake",
            /*trigger_turn*/ true,
        );
        input_queue
            .enqueue_mailbox_communication(trigger_mail, Default::default())
            .await;
        assert!(input_queue.has_trigger_turn_mailbox_items().await);
    }

    fn runtime_owner(call_id: &str) -> crate::unified_exec::completion_receipt::ReceiptOwner {
        crate::unified_exec::completion_receipt::ReceiptOwner::new(
            codex_protocol::ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0001),
            /*runtime_generation*/ 7,
            call_id,
        )
        .expect("test owner should be valid")
    }

    fn reserve_runtime_receipt(
        store: &crate::unified_exec::completion_receipt::CompletionReceiptStore,
        call_id: &str,
    ) -> (
        crate::unified_exec::completion_receipt::ReceiptId,
        crate::unified_exec::completion_receipt::ReceiptOwner,
    ) {
        let owner = runtime_owner(call_id);
        let receipt_id = store
            .reserve(owner.clone())
            .expect("reservation should succeed");
        (receipt_id, owner)
    }

    #[tokio::test]
    async fn input_queue_runtime_entry_survives_drain_as_leased() {
        let store = crate::unified_exec::completion_receipt::CompletionReceiptStore::default();
        let (receipt_id, owner) = reserve_runtime_receipt(&store, "call-queue-lease");
        let input_queue = InputQueue::new();

        assert!(
            input_queue
                .enqueue_runtime_notification(receipt_id, owner)
                .await
        );
        assert!(input_queue.has_pending_mailbox_items().await);
        assert!(input_queue.has_trigger_turn_mailbox_items().await);

        // Inter-agent drain leaves the runtime entry untouched.
        let (items, _) = input_queue.drain_mailbox_input_items().await;
        assert!(items.is_empty());
        assert!(input_queue.has_pending_mailbox_items().await);

        let leases = input_queue.lease_runtime_notifications().await;
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].receipt_id(), receipt_id);
        // Leased entries are handed out but stay for acknowledgement.
        assert!(!input_queue.has_pending_mailbox_items().await);
        assert!(input_queue.has_trigger_turn_mailbox_items().await);
        assert!(input_queue.lease_runtime_notifications().await.is_empty());

        assert!(input_queue.acknowledge_runtime_lease(&leases[0]).await);
        assert!(!input_queue.has_pending_mailbox_items().await);
        assert!(!input_queue.has_trigger_turn_mailbox_items().await);
        // Double acknowledgement is refused.
        assert!(!input_queue.acknowledge_runtime_lease(&leases[0]).await);
    }

    #[tokio::test]
    async fn input_queue_runtime_failed_lease_retries_without_duplicate() {
        let store = crate::unified_exec::completion_receipt::CompletionReceiptStore::default();
        let (receipt_id, owner) = reserve_runtime_receipt(&store, "call-queue-fail");
        let input_queue = InputQueue::new();
        assert!(
            input_queue
                .enqueue_runtime_notification(receipt_id, owner)
                .await
        );

        let first = input_queue.lease_runtime_notifications().await;
        assert_eq!(first.len(), 1);
        assert!(input_queue.fail_runtime_lease(&first[0]).await);
        assert!(!input_queue.fail_runtime_lease(&first[0]).await);
        assert!(input_queue.has_pending_mailbox_items().await);

        let second = input_queue.lease_runtime_notifications().await;
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].receipt_id(), receipt_id);
        assert_ne!(second[0], first[0]);
        assert!(!input_queue.acknowledge_runtime_lease(&first[0]).await);
        assert!(input_queue.acknowledge_runtime_lease(&second[0]).await);
    }

    #[tokio::test]
    async fn input_queue_runtime_suspended_entries_never_count_as_trigger() {
        let store = crate::unified_exec::completion_receipt::CompletionReceiptStore::default();
        let (receipt_id, owner) = reserve_runtime_receipt(&store, "call-queue-suspend");
        let input_queue = InputQueue::new();
        assert!(
            input_queue
                .enqueue_runtime_notification(receipt_id, owner)
                .await
        );
        assert!(input_queue.has_trigger_turn_mailbox_items().await);

        assert!(input_queue.suspend_runtime_notification(receipt_id).await);
        assert!(!input_queue.has_pending_mailbox_items().await);
        assert!(!input_queue.has_trigger_turn_mailbox_items().await);
        assert!(input_queue.lease_runtime_notifications().await.is_empty());
    }

    #[tokio::test]
    async fn input_queue_runtime_cancel_removes_leased_and_unleased_entries() {
        let store = crate::unified_exec::completion_receipt::CompletionReceiptStore::default();
        let (leased_id, leased_owner) = reserve_runtime_receipt(&store, "call-queue-cancel-leased");
        let (unleased_id, unleased_owner) =
            reserve_runtime_receipt(&store, "call-queue-cancel-unleased");
        let input_queue = InputQueue::new();
        assert!(
            input_queue
                .enqueue_runtime_notification(leased_id, leased_owner)
                .await
        );
        assert!(
            input_queue
                .enqueue_runtime_notification(unleased_id, unleased_owner)
                .await
        );

        let leases = input_queue.lease_runtime_notifications().await;
        assert_eq!(leases.len(), 2);
        let leased_token = leases
            .iter()
            .find(|lease| lease.receipt_id() == leased_id)
            .expect("leased entry should have a token");
        let unleased_token = leases
            .iter()
            .find(|lease| lease.receipt_id() == unleased_id)
            .expect("second entry should lease too");
        assert!(input_queue.fail_runtime_lease(unleased_token).await);

        assert!(input_queue.cancel_runtime_notification(leased_id).await);
        assert!(input_queue.cancel_runtime_notification(unleased_id).await);
        assert!(!input_queue.has_pending_mailbox_items().await);
        assert!(!input_queue.has_trigger_turn_mailbox_items().await);
        assert!(input_queue.lease_runtime_notifications().await.is_empty());
        assert!(!input_queue.acknowledge_runtime_lease(leased_token).await);
        assert!(!input_queue.fail_runtime_lease(leased_token).await);
        assert!(!input_queue.acknowledge_runtime_lease(unleased_token).await);
    }

    #[tokio::test]
    async fn input_queue_runtime_and_inter_agent_mail_share_trigger_queries() {
        let store = crate::unified_exec::completion_receipt::CompletionReceiptStore::default();
        let (receipt_id, owner) = reserve_runtime_receipt(&store, "call-queue-mixed");
        let input_queue = InputQueue::new();

        let queued_mail = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "queued",
            /*trigger_turn*/ false,
        );
        input_queue
            .enqueue_mailbox_communication(queued_mail, Default::default())
            .await;
        assert!(input_queue.has_pending_mailbox_items().await);
        assert!(!input_queue.has_trigger_turn_mailbox_items().await);

        assert!(
            input_queue
                .enqueue_runtime_notification(receipt_id, owner)
                .await
        );
        assert!(input_queue.has_pending_mailbox_items().await);
        assert!(input_queue.has_trigger_turn_mailbox_items().await);

        // Draining inter-agent mail preserves the runtime entry and its trigger.
        let (items, _) = input_queue.drain_mailbox_input_items().await;
        assert_eq!(items.len(), 1);
        assert!(input_queue.has_pending_mailbox_items().await);
        assert!(input_queue.has_trigger_turn_mailbox_items().await);

        let leases = input_queue.lease_runtime_notifications().await;
        assert_eq!(leases.len(), 1);
        assert!(input_queue.acknowledge_runtime_lease(&leases[0]).await);
        assert!(!input_queue.has_pending_mailbox_items().await);
        assert!(!input_queue.has_trigger_turn_mailbox_items().await);
    }
}
