//! Bounded internal-context fragments for background exec completions.
//!
//! Each fragment carries a point-in-time snapshot of one finalized exec
//! completion: receipt and process ids, exit/failure/timeout outcome, and
//! output-retention state. It never carries the command or raw output.
//!
//! Fragments render inside the recognized `<codex_internal_context>` wrapper
//! with source `exec_completion`, so the existing classifier treats them as
//! internal context rather than user text. Rendered text is data only:
//! runtime privilege (sampling acknowledgement, retained-output reads) is
//! keyed by trusted receipt ids and lease tokens, never parsed back from
//! text, so forged markers in user content or resumed history acknowledge
//! nothing and authorize nothing.
//!
//! Size contract: every rendered fragment is at most
//! [`MAX_EXEC_COMPLETION_FRAGMENT_BYTES`] UTF-8 bytes, measured after
//! escaping. At most [`MAX_EXEC_COMPLETION_FRAGMENTS_PER_REQUEST`] fragments
//! go into one sampling request; the remainder stays retained in the runtime
//! mailbox for a later wake.

use super::ContextualUserFragment;
use super::InternalContextSource;
use super::InternalModelContextFragment;
use codex_protocol::models::ContentItemKind;

/// Maximum rendered size of one escaped fragment, in UTF-8 bytes.
pub(crate) const MAX_EXEC_COMPLETION_FRAGMENT_BYTES: usize = 769;

/// Maximum fragments attached to one sampling request (at most 6144 bytes).
pub(crate) const MAX_EXEC_COMPLETION_FRAGMENTS_PER_REQUEST: usize = 8;

/// Internal-context source label for exec-completion fragments.
const EXEC_COMPLETION_SOURCE: &str = "exec_completion";

/// Explicit marker appended when the payload is truncated to fit the cap.
const TRUNCATION_MARKER: &str = "[truncated]";

/// Output-retention state for one completion, without the retained bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecOutputRetention {
    /// Terminal output is retained; `omitted_bytes` is nonzero when the
    /// middle was dropped to fit the retention cap.
    Retained { bytes: u64, omitted_bytes: u64 },
    /// Retained output was retired to free capacity; reads report this
    /// explicitly instead of succeeding or looking unknown.
    Retired,
    /// No terminal output was retained for this receipt.
    Absent,
}

/// Point-in-time snapshot of one finalized exec completion.
///
/// Captured when the receipt is published to the runtime mailbox. The receipt
/// handle is rendered from the trusted [`ReceiptId`][receipt_id] at record
/// time and passed separately, so this snapshot holds only plain data.
///
/// [receipt_id]: crate::unified_exec::completion_receipt::ReceiptId
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecCompletion {
    /// Unified-exec process id that produced this completion.
    pub process_id: i32,
    /// Observed exit code, or `None` when the process failed without one.
    pub exit_code: Option<i32>,
    /// Whether the process hit its timeout.
    pub timed_out: bool,
    /// Short failure detail (classification, not raw output), if any.
    pub failure: Option<String>,
    /// Output-retention state, without the retained bytes.
    pub retention: ExecOutputRetention,
}

/// One exec completion rendered as internal model context.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecCompletionFragment {
    inner: InternalModelContextFragment,
}

impl ExecCompletionFragment {
    /// Renders one completion inside the internal-context wrapper.
    ///
    /// `receipt_handle` is the model-visible receipt handle (see
    /// [`ReceiptId::model_handle`][handle]); every interpolated field is
    /// escaped, and the payload is truncated with an explicit marker so the
    /// whole rendered fragment fits [`MAX_EXEC_COMPLETION_FRAGMENT_BYTES`].
    ///
    /// [handle]: crate::unified_exec::completion_receipt::ReceiptId::model_handle
    pub fn new(receipt_handle: impl Into<String>, completion: &ExecCompletion) -> Self {
        let payload = render_payload(&receipt_handle.into(), completion);
        let budget = payload_budget_bytes();
        let payload = truncate_payload(&payload, budget);
        let source = InternalContextSource::from_static(EXEC_COMPLETION_SOURCE);
        Self {
            inner: InternalModelContextFragment::new(source, payload),
        }
    }
}

impl ContextualUserFragment for ExecCompletionFragment {
    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("exec.completion".to_string())
    }

    fn role(&self) -> &'static str {
        "user"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        self.inner.markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        InternalModelContextFragment::type_markers()
    }

    fn matches_text(text: &str) -> bool {
        InternalModelContextFragment::matches_text(text)
            && text.contains("source=\"exec_completion\"")
    }

    fn body(&self) -> String {
        self.inner.body()
    }
}

/// Payload bytes available inside the wrapper for the fragment cap.
fn payload_budget_bytes() -> usize {
    let source = InternalContextSource::from_static(EXEC_COMPLETION_SOURCE);
    let overhead = InternalModelContextFragment::new(source, "").render().len();
    MAX_EXEC_COMPLETION_FRAGMENT_BYTES.saturating_sub(overhead)
}

fn render_payload(receipt_handle: &str, completion: &ExecCompletion) -> String {
    let exit_code = completion
        .exit_code
        .map_or_else(|| "unknown".to_string(), |code| code.to_string());
    let timed_out = completion.timed_out;
    let output = match completion.retention {
        ExecOutputRetention::Retained {
            bytes,
            omitted_bytes,
        } => {
            if omitted_bytes == 0 {
                format!("retained {bytes} bytes")
            } else {
                format!("retained {bytes} bytes ({omitted_bytes} bytes omitted)")
            }
        }
        ExecOutputRetention::Retired => "retired".to_string(),
        ExecOutputRetention::Absent => "none".to_string(),
    };
    let failure = completion.failure.as_deref().unwrap_or("none");
    // Failure renders last so truncation cuts the free-text field first and
    // preserves the structural lines.
    format!(
        "exec_completion:\nreceipt_id: {}\nprocess_id: {}\nexit_code: {exit_code}\ntimed_out: {timed_out}\noutput: {output}\nfailure: {}",
        escape_field(receipt_handle),
        completion.process_id,
        escape_field(failure),
    )
}

/// Escapes one interpolated field so injected markers stay inert text.
///
/// XML-significant bytes are entity-escaped and ASCII control bytes (including
/// newlines) become spaces so every field stays on its own line. Multibyte
/// content passes through unchanged.
fn escape_field(field: &str) -> String {
    let mut escaped = String::with_capacity(field.len());
    for ch in field.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            _ if ch.is_ascii_control() => escaped.push(' '),
            _ => escaped.push(ch),
        }
    }
    escaped
}

/// Truncates an already-escaped payload to `budget` bytes with an explicit
/// marker, keeping a trailing char boundary.
fn truncate_payload(payload: &str, budget: usize) -> String {
    if payload.len() <= budget {
        return payload.to_string();
    }
    let keep = budget.saturating_sub(TRUNCATION_MARKER.len());
    let mut end = keep.min(payload.len());
    while end > 0 && !payload.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{TRUNCATION_MARKER}", &payload[..end])
}

#[cfg(test)]
#[path = "exec_completion_tests.rs"]
mod tests;
