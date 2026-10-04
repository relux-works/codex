//! Per-receipt retained terminal output for opted-in exec completions.
//!
//! Retained output survives removal of the process-store entry on exit and is
//! readable by receipt id. Storage is bounded: up to 1 MiB is kept verbatim,
//! larger transcripts are kept as head + tail with an explicit truncation
//! indicator and the omitted byte count.
//
// Lookup and snapshot types serve the later exec_notification story and are
// covered by tests until then.
#![allow(dead_code)]

use std::collections::HashMap;

use super::UNIFIED_EXEC_OUTPUT_MAX_BYTES;
use super::completion_receipt::ReceiptId;
use super::completion_receipt::ReceiptOwner;

/// Snapshot of retained terminal output returned to a matching owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RetainedOutputSnapshot {
    /// Retained bytes: the full transcript, or head ++ tail when truncated.
    pub(crate) bytes: Vec<u8>,
    /// True when middle bytes were omitted to fit the cap.
    pub(crate) truncated: bool,
    /// Number of transcript bytes omitted from the middle.
    pub(crate) omitted_bytes: usize,
}

/// Outcome of an owner-checked retention lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RetentionLookup {
    Present {
        bytes: Vec<u8>,
        omitted_bytes: usize,
    },
    Retired,
    Absent,
    ForeignOwner,
}

struct PendingOutput {
    owner: ReceiptOwner,
    bytes: Vec<u8>,
    omitted_bytes: usize,
}

struct SampledOutput {
    owner: ReceiptOwner,
    bytes: Vec<u8>,
    omitted_bytes: usize,
    sampled_seq: u64,
}

struct RetiredMarker {
    owner: ReceiptOwner,
}

/// Retention state for one runtime generation, owned by the process manager.
///
/// Output is inserted while its receipt is unsampled (queued or leased) and
/// moves to `sampled` when the single claim is acknowledged. A sampled entry
/// keeps holding its capacity slot until release or least-recently-sampled
/// retirement; retired entries keep only their owner so reads report
/// `Retired` explicitly instead of succeeding or looking unknown.
#[derive(Default)]
pub(crate) struct RetentionState {
    pending: HashMap<ReceiptId, PendingOutput>,
    sampled: HashMap<ReceiptId, SampledOutput>,
    retired: HashMap<ReceiptId, RetiredMarker>,
}

impl RetentionState {
    /// Inserts terminal output for an unsampled receipt, enforcing the cap.
    ///
    /// The snapshot source is already capped in the common case; oversized
    /// input is still reduced to head + tail here so retained bytes never
    /// exceed the cap and the omitted count stays exact.
    pub(crate) fn insert_pending(
        &mut self,
        receipt_id: ReceiptId,
        owner: ReceiptOwner,
        bytes: Vec<u8>,
        omitted_bytes: usize,
    ) {
        let (bytes, omitted_bytes) = enforce_retention_cap(bytes, omitted_bytes);
        self.pending.insert(
            receipt_id,
            PendingOutput {
                owner,
                bytes,
                omitted_bytes,
            },
        );
    }

    /// Moves pending output to sampled holding its slot. False if none.
    pub(crate) fn move_to_sampled(&mut self, receipt_id: ReceiptId, sampled_seq: u64) -> bool {
        let Some(pending) = self.pending.remove(&receipt_id) else {
            return false;
        };
        self.sampled.insert(
            receipt_id,
            SampledOutput {
                owner: pending.owner,
                bytes: pending.bytes,
                omitted_bytes: pending.omitted_bytes,
                sampled_seq,
            },
        );
        true
    }

    /// Drops retained output (or a retired marker) in every state.
    pub(crate) fn drop(&mut self, receipt_id: ReceiptId) -> bool {
        let mut removed = self.pending.remove(&receipt_id).is_some();
        removed |= self.sampled.remove(&receipt_id).is_some();
        removed |= self.retired.remove(&receipt_id).is_some();
        removed
    }

    /// Retires the least-recently-sampled output to free its slot.
    pub(crate) fn retire_least_recently_sampled(&mut self) -> Option<ReceiptId> {
        let victim = self
            .sampled
            .iter()
            .min_by_key(|(_, output)| output.sampled_seq)
            .map(|(receipt_id, _)| *receipt_id)?;
        let output = self.sampled.remove(&victim)?;
        self.retired.insert(
            victim,
            RetiredMarker {
                owner: output.owner,
            },
        );
        Some(victim)
    }

    /// Looks up retained output, refusing foreign owners in every state.
    pub(crate) fn lookup(&self, receipt_id: ReceiptId, owner: &ReceiptOwner) -> RetentionLookup {
        if let Some(pending) = self.pending.get(&receipt_id) {
            return if pending.owner == *owner {
                RetentionLookup::Present {
                    bytes: pending.bytes.clone(),
                    omitted_bytes: pending.omitted_bytes,
                }
            } else {
                RetentionLookup::ForeignOwner
            };
        }
        if let Some(sampled) = self.sampled.get(&receipt_id) {
            return if sampled.owner == *owner {
                RetentionLookup::Present {
                    bytes: sampled.bytes.clone(),
                    omitted_bytes: sampled.omitted_bytes,
                }
            } else {
                RetentionLookup::ForeignOwner
            };
        }
        if let Some(retired) = self.retired.get(&receipt_id) {
            return if retired.owner == *owner {
                RetentionLookup::Retired
            } else {
                RetentionLookup::ForeignOwner
            };
        }
        RetentionLookup::Absent
    }

    pub(crate) fn sampled_count(&self) -> usize {
        self.sampled.len()
    }

    pub(crate) fn clear(&mut self) {
        self.pending.clear();
        self.sampled.clear();
        self.retired.clear();
    }
}

fn enforce_retention_cap(bytes: Vec<u8>, omitted_bytes: usize) -> (Vec<u8>, usize) {
    if bytes.len() <= 2 * UNIFIED_EXEC_OUTPUT_MAX_BYTES {
        return (bytes, omitted_bytes);
    }
    let head_budget = UNIFIED_EXEC_OUTPUT_MAX_BYTES;
    let tail_budget = UNIFIED_EXEC_OUTPUT_MAX_BYTES;
    let mut capped = Vec::with_capacity(UNIFIED_EXEC_OUTPUT_MAX_BYTES);
    capped.extend_from_slice(&bytes[..head_budget]);
    capped.extend_from_slice(&bytes[bytes.len().saturating_sub(tail_budget)..]);
    let omitted_bytes = omitted_bytes.saturating_add(bytes.len() - UNIFIED_EXEC_OUTPUT_MAX_BYTES);
    (capped, omitted_bytes)
}

#[cfg(test)]
#[path = "receipt_output_tests.rs"]
mod tests;
