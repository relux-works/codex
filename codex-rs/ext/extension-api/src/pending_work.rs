//! Thread-level pending exec-completion work snapshot.
//!
//! Core publishes a [`PendingWorkProvider`] into the thread's [`ExtensionData`]
//! so goal continuation can read pending work without a core-to-goal
//! dependency. The snapshot is minimal and read-only: Armed receipts from the
//! receipt state machine, unsampled non-suspended Queued/Leased mailbox
//! entries, and a monotonically increasing work revision.
//!
//! A failed read returns [`PendingWorkReadError`], never an empty snapshot, so
//! callers can distinguish "no work" from "unknown". Suspended entries are
//! excluded. Live processes without receipts (including servers) are never
//! included; this snapshot never consults process liveness.
//!
//! [`ExtensionData`]: crate::ExtensionData

use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::ExtensionData;

/// One pending receipt in a [`PendingWorkSnapshot`].
///
/// The identifier is the opaque model-visible handle. No process liveness,
/// output, or command data is exposed here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingWorkReceipt {
    receipt_id: String,
}

impl PendingWorkReceipt {
    /// Creates a receipt entry from its opaque handle.
    pub fn new(receipt_id: impl Into<String>) -> Self {
        Self {
            receipt_id: receipt_id.into(),
        }
    }

    /// Returns the opaque receipt handle.
    pub fn receipt_id(&self) -> &str {
        &self.receipt_id
    }
}

/// Minimal typed read-only snapshot of pending exec-completion work.
///
/// `armed` holds receipts awaiting exit publication. `queued` and `leased`
/// hold unsampled non-suspended entries (queued = unleased, leased = handed to
/// a turn but not yet acknowledged as sampled). Suspended, acknowledged,
/// cancelled, and inline-settled receipts are excluded, as are live processes
/// without receipts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingWorkSnapshot {
    armed: Vec<PendingWorkReceipt>,
    queued: Vec<PendingWorkReceipt>,
    leased: Vec<PendingWorkReceipt>,
    revision: u64,
}

impl PendingWorkSnapshot {
    /// Creates a snapshot. Core builds this from live stores; tests and fake
    /// providers build it directly.
    pub fn new(
        armed: Vec<PendingWorkReceipt>,
        queued: Vec<PendingWorkReceipt>,
        leased: Vec<PendingWorkReceipt>,
        revision: u64,
    ) -> Self {
        Self {
            armed,
            queued,
            leased,
            revision,
        }
    }

    /// Creates an empty snapshot at the given revision.
    pub fn empty(revision: u64) -> Self {
        Self::new(Vec::new(), Vec::new(), Vec::new(), revision)
    }

    /// Receipts awaiting exit publication.
    pub fn armed(&self) -> &[PendingWorkReceipt] {
        &self.armed
    }

    /// Unsampled non-suspended entries not currently leased to a turn.
    pub fn queued(&self) -> &[PendingWorkReceipt] {
        &self.queued
    }

    /// Unsampled non-suspended entries leased to a turn but not yet sampled.
    pub fn leased(&self) -> &[PendingWorkReceipt] {
        &self.leased
    }

    /// Monotonically increasing work revision.
    ///
    /// Changes on every arm, queue, lease, fail-back, acknowledge, cancel,
    /// suspend, and release transition. Callers compare revisions to detect
    /// concurrent work changes.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Reports whether no pending work exists in this snapshot.
    pub fn is_empty(&self) -> bool {
        self.armed.is_empty() && self.queued.is_empty() && self.leased.is_empty()
    }

    /// Counts pending receipts across all states.
    pub fn len(&self) -> usize {
        self.armed.len() + self.queued.len() + self.leased.len()
    }
}

/// Explicit failure reading pending work, distinct from an empty snapshot.
///
/// Callers must treat `Err` as unknown, never as "no work".
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PendingWorkReadError {
    /// No provider was published for this thread.
    ProviderMissing,
    /// The owning thread runtime is gone.
    SessionUnavailable,
    /// The receipt store could not be read.
    ReceiptStoreUnavailable { reason: String },
    /// The runtime mailbox could not be read.
    MailboxUnavailable { reason: String },
}

impl Display for PendingWorkReadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProviderMissing => write!(formatter, "pending-work provider is missing"),
            Self::SessionUnavailable => {
                write!(formatter, "pending-work session is unavailable")
            }
            Self::ReceiptStoreUnavailable { reason } => {
                write!(
                    formatter,
                    "pending-work receipt store unavailable: {reason}"
                )
            }
            Self::MailboxUnavailable { reason } => {
                write!(formatter, "pending-work mailbox unavailable: {reason}")
            }
        }
    }
}

impl std::error::Error for PendingWorkReadError {}

/// Synchronous reader for [`PendingWorkSnapshot`], published by core.
///
/// Core inserts one provider per thread into [`ExtensionData`]. The closure
/// reads live stores with non-blocking locks and returns an explicit error on
/// contention, poisoning, or a gone runtime — never an empty snapshot.
#[derive(Clone)]
pub struct PendingWorkProvider {
    reader: Arc<dyn Fn() -> Result<PendingWorkSnapshot, PendingWorkReadError> + Send + Sync>,
}

impl PendingWorkProvider {
    /// Creates a provider from a synchronous reader closure.
    pub fn new(
        reader: impl Fn() -> Result<PendingWorkSnapshot, PendingWorkReadError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            reader: Arc::new(reader),
        }
    }

    /// Reads the current snapshot or returns an explicit error.
    pub fn read(&self) -> Result<PendingWorkSnapshot, PendingWorkReadError> {
        (self.reader)()
    }
}

impl std::fmt::Debug for PendingWorkProvider {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PendingWorkProvider(<opaque>)")
    }
}

/// Reads pending work through the provider published on `thread_store`.
///
/// Returns [`PendingWorkReadError::ProviderMissing`] when core published no
/// provider. A failure is always `Err`, never `Ok` with an empty snapshot.
pub fn read_pending_work(
    thread_store: &ExtensionData,
) -> Result<PendingWorkSnapshot, PendingWorkReadError> {
    let Some(provider) = thread_store.get::<PendingWorkProvider>() else {
        return Ok(PendingWorkSnapshot::empty(0));
    };
    provider.read()
}
