use crate::function_tool::FunctionCallError;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::parse_arguments;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::PostToolUsePayload;
use crate::tools::registry::PreToolUsePayload;
use crate::tools::registry::ToolExecutor;
use crate::unified_exec::UnifiedExecContext;
use crate::unified_exec::completion_receipt::CancellationReason;
use crate::unified_exec::completion_receipt::ReceiptError;
use crate::unified_exec::completion_receipt::ReceiptId;
use crate::unified_exec::completion_receipt::ReceiptStatus;
use codex_extension_api::AsyncNotificationSupport;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use codex_utils_output_truncation::TruncationPolicy;
use codex_utils_output_truncation::formatted_truncate_text;
use serde::Deserialize;

use super::super::shell_spec::EXEC_NOTIFICATION_READ_DEFAULT_MAX_TOKENS;
use super::super::shell_spec::EXEC_NOTIFICATION_READ_MAX_TOKENS;
use super::super::shell_spec::create_exec_notification_tool;

#[derive(Debug, Deserialize)]
struct ExecNotificationArgs {
    action: String,
    receipt_id: String,
    #[serde(
        default,
        deserialize_with = "codex_tools::arguments::option_usize::deserialize"
    )]
    max_output_tokens: Option<usize>,
}

pub struct ExecNotificationHandler;

impl ToolExecutor<ToolInvocation> for ExecNotificationHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("exec_notification")
    }

    fn spec(&self) -> ToolSpec {
        create_exec_notification_tool()
    }

    fn supports_parallel_tool_calls(&self) -> bool {
        true
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(self.handle_call(invocation))
    }
}

impl ExecNotificationHandler {
    async fn handle_call(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
        let ToolInvocation {
            session,
            step_context,
            cancellation_token,
            call_id,
            payload,
            ..
        } = invocation;

        let arguments = match payload {
            ToolPayload::Function { arguments } => arguments,
            _ => {
                return Err(FunctionCallError::RespondToModel(
                    "exec_notification handler received unsupported payload".to_string(),
                ));
            }
        };

        // The tool is advertised only on capable hosts; refuse outright if it
        // is somehow invoked where no wake could have been promised.
        if !AsyncNotificationSupport::read_from(&session.services.thread_extension_data)
            .is_available()
        {
            return Err(FunctionCallError::RespondToModel(
                "exec_notification is not supported on this host; no completion subscription could have been acknowledged here."
                    .to_string(),
            ));
        }

        let args: ExecNotificationArgs = parse_arguments(&arguments)?;
        let receipt_id = ReceiptId::from_model_handle(&args.receipt_id)
            .ok_or_else(|| FunctionCallError::RespondToModel(unknown_receipt_message()))?;
        let context =
            UnifiedExecContext::new(session.clone(), step_context, cancellation_token, call_id);
        let manager = &session.services.unified_exec_manager;
        let owner = manager
            .notification_owner_for_receipt(receipt_id, &context)
            .await
            .map_err(|err| FunctionCallError::RespondToModel(receipt_error_message(err)))?;

        match args.action.as_str() {
            "read" => {
                let budget = args
                    .max_output_tokens
                    .unwrap_or(EXEC_NOTIFICATION_READ_DEFAULT_MAX_TOKENS)
                    .clamp(1, EXEC_NOTIFICATION_READ_MAX_TOKENS);
                let snapshot = manager
                    .read_retained_output(receipt_id, &owner)
                    .await
                    .map_err(|err| FunctionCallError::RespondToModel(receipt_error_message(err)))?;
                Ok(boxed_tool_output(FunctionToolOutput::from_text(
                    render_read_response(
                        &args.receipt_id,
                        &snapshot.bytes,
                        snapshot.truncated,
                        snapshot.omitted_bytes,
                        budget,
                    ),
                    Some(true),
                )))
            }
            "release" => {
                manager
                    .release_completion_receipt(receipt_id, &owner)
                    .await
                    .map_err(|err| FunctionCallError::RespondToModel(receipt_error_message(err)))?;
                // Drop a pending wake for the disarmed subscription, if any.
                // A later exit publishes nothing for a released receipt, so
                // together these guarantee no wake after release.
                session
                    .input_queue
                    .cancel_runtime_notification(receipt_id)
                    .await;
                Ok(boxed_tool_output(FunctionToolOutput::from_text(
                    format!(
                        "Released receipt {}; the completion wake is disarmed and the process was not terminated.",
                        args.receipt_id.trim(),
                    ),
                    Some(true),
                )))
            }
            action => Err(FunctionCallError::RespondToModel(format!(
                "unknown exec_notification action \"{action}\"; use \"read\" or \"release\"."
            ))),
        }
    }
}

impl CoreToolRuntime for ExecNotificationHandler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }

    fn pre_tool_use_payload(&self, _invocation: &ToolInvocation) -> Option<PreToolUsePayload> {
        // Control-plane reads and releases act on an acknowledged
        // subscription, not a new command, so no command review hook applies.
        None
    }

    fn post_tool_use_payload(
        &self,
        _invocation: &ToolInvocation,
        _result: &dyn crate::tools::context::ToolOutput,
    ) -> Option<PostToolUsePayload> {
        None
    }
}

fn render_read_response(
    handle: &str,
    bytes: &[u8],
    truncated: bool,
    omitted_bytes: usize,
    budget_tokens: usize,
) -> String {
    let text = String::from_utf8_lossy(bytes).into_owned();
    let mut header = format!(
        "Receipt {}: terminal output, {} bytes retained.",
        handle.trim(),
        bytes.len()
    );
    if truncated {
        header.push_str(&format!(
            " {omitted_bytes} bytes omitted to fit the retention cap."
        ));
    }
    let output = formatted_truncate_text(&text, TruncationPolicy::Tokens(budget_tokens));
    format!("{header}\n{output}")
}

fn unknown_receipt_message() -> String {
    "unknown or expired exec receipt id; only receipts acknowledged on this thread can be read or released."
        .to_string()
}

/// Maps a receipt error to the model-facing refusal for this tool.
///
/// Every stale state (released, cancelled, consumed, retired, never armed)
/// is rejected; only a live binding with retained terminal output reads, and
/// only a live or sampled binding releases.
fn receipt_error_message(err: ReceiptError) -> String {
    match err {
        ReceiptError::UnknownReceipt => unknown_receipt_message(),
        ReceiptError::ForeignOwner => {
            "exec receipt belongs to a different thread or runtime generation and cannot be used here."
                .to_string()
        }
        ReceiptError::Retired => {
            "exec receipt output was retired to free capacity; the subscription is gone."
                .to_string()
        }
        ReceiptError::AlreadyConsumed => {
            "exec receipt was already consumed; no output remains.".to_string()
        }
        ReceiptError::Cancelled { reason } => match reason {
            CancellationReason::Released => {
                "exec receipt was already released; no subscription remains.".to_string()
            }
            CancellationReason::OwnerStopped
            | CancellationReason::Shutdown
            | CancellationReason::Interrupted => {
                format!("exec receipt was cancelled ({reason:?}); no subscription remains.")
            }
        },
        ReceiptError::InvalidTransition { actual } => match actual {
            ReceiptStatus::Reserved | ReceiptStatus::Armed => {
                "the process has not exited yet; read returns retained terminal output and never waits."
                    .to_string()
            }
            ReceiptStatus::Queued | ReceiptStatus::LeasedToSampling { .. } => {
                "terminal output is not retained yet; try again.".to_string()
            }
            ReceiptStatus::InlineResult => {
                "the process finished during the launching call; no subscription was armed."
                    .to_string()
            }
            ReceiptStatus::Sampled { .. } => {
                "exec receipt was already consumed; no output remains.".to_string()
            }
            ReceiptStatus::Cancelled { .. } => {
                "exec receipt is no longer active; no subscription remains.".to_string()
            }
        },
        ReceiptError::LockPoisoned | ReceiptError::LockContended => {
            "the completion receipt store is unavailable; try again.".to_string()
        }
        ReceiptError::CapacityExceeded { .. }
        | ReceiptError::InvalidOwner
        | ReceiptError::AlreadyLeased
        | ReceiptError::StaleLease
        | ReceiptError::AlreadyTerminal
        | ReceiptError::IdGenerationFailed => {
            format!("cannot use this exec receipt right now: {err}.")
        }
    }
}

#[cfg(test)]
#[path = "exec_notification_tests.rs"]
mod tests;
