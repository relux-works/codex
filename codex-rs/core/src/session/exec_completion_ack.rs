//! Sampling acknowledgement for leased exec-completion notifications.
//!
//! A runtime lease is acknowledged as sampled when the transport observes the
//! server accepting a sampling request that contains its fragment -- response
//! creation or output, on either transport -- before response completion and
//! before tool draining. Reservation, drain, pending-input recording, and
//! history append never acknowledge: the record path only renders the fragment
//! and tracks the lease on the turn, server acceptance acknowledges the
//! members of the submitted prompt, and turn end fails whatever is still
//! tracked.
//!
//! Membership is checked against the submitted prompt items using the trusted
//! lease: the expected fragment text is re-rendered from the lease's receipt
//! id and completion snapshot and compared for exact equality. Nothing is
//! parsed back from fragment text, so forged markers in user content or
//! resumed history acknowledge nothing and authorize nothing.
//!
//! A failed or aborted submission fails tracked leases back to unleased
//! through [`RuntimeMailbox::fail`][fail], so a later wake retries them; the
//! record path skips fragments already present in history, so a retry never
//! appends the same fragment twice. Each failed attempt counts against
//! [`MAX_RUNTIME_SAMPLING_ATTEMPTS`][max]; the exhausting failure suspends the
//! entry visibly with a warning instead of spinning further wake turns.
//! Nothing after acceptance -- stream error, EOF, cancellation, budget
//! failure, tool drain, turn abort -- can un-acknowledge a lease:
//! acknowledgement removes it from tracking and from the mailbox, and failing
//! an acknowledged lease is a no-op.
//!
//! [fail]: super::runtime_mailbox::RuntimeMailbox::fail
//! [max]: super::runtime_mailbox::MAX_RUNTIME_SAMPLING_ATTEMPTS

use std::sync::Mutex;
use std::sync::PoisonError;

use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::WarningEvent;

use super::runtime_mailbox::MAX_RUNTIME_SAMPLING_ATTEMPTS;
use super::runtime_mailbox::RuntimeLease;
use super::session::Session;
use super::turn_context::TurnContext;
use crate::client_common::ResponseEvent;
use crate::context::ContextualUserFragment;
use crate::context::ExecCompletionFragment;

/// Runtime leases recorded by this turn and still awaiting sampling proof.
///
/// Stored on the turn's extension data by the record path, consumed by the
/// submit path (members acknowledged) and by turn end (remainder failed).
#[derive(Debug, Default)]
struct PendingExecCompletionAcks {
    leases: Mutex<Vec<RuntimeLease>>,
}

/// Tracks leases whose fragments were recorded for this turn.
///
/// Recording is not acknowledgement; every tracked lease still needs a
/// submitted prompt containing its fragment. Leases already tracked (a retry
/// re-recording the same receipt) are not duplicated.
pub(crate) fn note_recorded(turn_context: &TurnContext, leases: &[RuntimeLease]) {
    if leases.is_empty() {
        return;
    }
    let tracked = turn_context
        .extension_data
        .get_or_init(PendingExecCompletionAcks::default);
    let mut guard = tracked
        .leases
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    for lease in leases {
        if !guard
            .iter()
            .any(|known| known.receipt_id() == lease.receipt_id())
        {
            guard.push(lease.clone());
        }
    }
}

/// Reports whether the trusted fragment of `lease` is among `items`.
///
/// The expected text is re-rendered from the lease itself; prompt and history
/// items are compared for exact equality and never parsed for receipt ids.
pub(crate) fn items_contain_lease<'a>(
    mut items: impl Iterator<Item = &'a ResponseItem>,
    lease: &RuntimeLease,
) -> bool {
    let fragment =
        ExecCompletionFragment::new(lease.receipt_id().model_handle(), lease.completion());
    let role = fragment.role();
    let expected = fragment.render();
    items.any(|item| match item {
        ResponseItem::Message {
            role: item_role,
            content,
            ..
        } if item_role == role => content
            .iter()
            .any(|entry| matches!(entry, ContentItem::InputText { text } if text == &expected)),
        _ => false,
    })
}

/// Reports whether a stream event proves the server accepted the request.
///
/// Response creation is the canonical signal; output and completion prove the
/// same for transports and bodies that skip creation. Header-synthesized and
/// auxiliary metadata events are excluded: they are emitted before (or
/// independently of) the body's accept/reject verdict, so they prove contact,
/// not acceptance. `response.failed` never reaches this classifier: the stream
/// parser surfaces it as an error, and a rejected request is not a sampling
/// proof. The match is exhaustive on purpose: a new event variant must make an
/// explicit acceptance choice here.
pub(crate) fn is_acceptance_event(event: &ResponseEvent) -> bool {
    match event {
        ResponseEvent::Created { .. }
        | ResponseEvent::OutputItemDone(_)
        | ResponseEvent::OutputItemAdded(_)
        | ResponseEvent::OutputTextDelta(_)
        | ResponseEvent::ToolCallInputDelta { .. }
        | ResponseEvent::ReasoningSummaryDelta { .. }
        | ResponseEvent::ReasoningSummaryDone { .. }
        | ResponseEvent::ReasoningContentDelta { .. }
        | ResponseEvent::ReasoningSummaryPartAdded { .. }
        | ResponseEvent::Completed { .. } => true,
        ResponseEvent::ServerModel(_)
        | ResponseEvent::ModelVerifications(_)
        | ResponseEvent::TurnModerationMetadata(_)
        | ResponseEvent::SafetyBuffering(_)
        | ResponseEvent::ServerReasoningIncluded(_)
        | ResponseEvent::RateLimits(_)
        | ResponseEvent::ModelsEtag(_) => false,
    }
}

/// Acknowledges tracked leases whose fragments the submitted prompt contains.
///
/// Called when the transport observes the server accepting a sampling request
/// (see [`is_acceptance_event`]), on any transport. Members are acknowledged
/// and untracked; non-members (omitted from the prompt, e.g. by compaction)
/// stay tracked so a later request in this turn can still sample them, and
/// turn end fails whatever remains.
pub(crate) async fn acknowledge_submitted(
    sess: &Session,
    turn_context: &TurnContext,
    prompt_input: &[ResponseItem],
) {
    if turn_context.config.model_provider.supports_websockets {
        return;
    }
    let Some(tracked) = turn_context
        .extension_data
        .get::<PendingExecCompletionAcks>()
    else {
        return;
    };
    let members: Vec<RuntimeLease> = tracked
        .leases
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|lease| items_contain_lease(prompt_input.iter(), lease))
        .cloned()
        .collect();
    if members.is_empty() {
        return;
    }
    for lease in &members {
        sess.input_queue.acknowledge_runtime_lease(lease).await;
    }
    tracked
        .leases
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .retain(|lease| !members.contains(lease));
}

/// Fails tracked leases that no submitted prompt in this turn contained.
///
/// Called once when the turn ends (completion, error, or abort): the leases
/// return to unleased so a later wake retries them. Entries that exhaust
/// [`MAX_RUNTIME_SAMPLING_ATTEMPTS`][max] suspend visibly with a warning.
/// [max]: super::runtime_mailbox::MAX_RUNTIME_SAMPLING_ATTEMPTS
pub(crate) async fn fail_unsubmitted(sess: &Session, turn_context: &TurnContext) {
    let Some(tracked) = turn_context
        .extension_data
        .remove::<PendingExecCompletionAcks>()
    else {
        return;
    };
    let leases = tracked
        .leases
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    fail_leases(sess, Some(turn_context), &leases).await;
}

/// Fails leases that never reached the record path of a finished turn.
///
/// Abort paths call this with leases dropped from turn-pending input. When
/// the aborted turn never started a task there is no turn context to warn on;
/// suspension still applies, and the entry stays retained but starts no
/// further wake turns.
pub(crate) async fn fail_leases(
    sess: &Session,
    turn_context: Option<&TurnContext>,
    leases: &[RuntimeLease],
) {
    if leases.is_empty() {
        return;
    }
    let mut suspended = 0usize;
    for lease in leases {
        if sess.input_queue.fail_runtime_lease(lease).await
            && sess
                .input_queue
                .is_runtime_notification_suspended(lease.receipt_id())
                .await
        {
            suspended += 1;
        }
    }
    if suspended == 0 {
        return;
    }
    let Some(turn_context) = turn_context else {
        return;
    };
    sess.send_event(
        turn_context,
        EventMsg::Warning(WarningEvent {
            message: suspension_warning(suspended),
        }),
    )
    .await;
}

fn suspension_warning(suspended: usize) -> String {
    let completions = if suspended == 1 {
        "completion"
    } else {
        "completions"
    };
    format!(
        "Background exec completion delivery suspended for {suspended} {completions} after {MAX_RUNTIME_SAMPLING_ATTEMPTS} failed sampling attempts; the wake will not retry automatically."
    )
}

#[cfg(test)]
#[path = "exec_completion_ack_tests.rs"]
mod tests;
