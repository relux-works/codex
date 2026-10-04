use super::*;
use crate::unified_exec::completion_receipt::CompletionReceiptStore;
use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;

fn test_owner(call_id: &str) -> ReceiptOwner {
    ReceiptOwner::new(
        ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_00a1),
        /*runtime_generation*/ 7,
        call_id.to_string(),
    )
    .expect("test owner should be valid")
}

fn reserve_id(store: &CompletionReceiptStore, owner: &ReceiptOwner) -> ReceiptId {
    store
        .reserve(owner.clone())
        .expect("reservation should succeed")
}

#[test]
fn retained_output_within_cap_is_verbatim() {
    let store = CompletionReceiptStore::default();
    let owner = test_owner("call-verbatim");
    let receipt_id = reserve_id(&store, &owner);
    let mut retention = RetentionState::default();

    retention.insert_pending(
        receipt_id,
        owner.clone(),
        b"verbatim-bytes".to_vec(),
        /*omitted_bytes*/ 0,
    );

    assert_eq!(
        retention.lookup(receipt_id, &owner),
        RetentionLookup::Present {
            bytes: b"verbatim-bytes".to_vec(),
            omitted_bytes: 0,
        }
    );
    assert_eq!(retention.sampled_count(), 0);
}

#[test]
fn retained_output_over_cap_keeps_head_tail_and_omitted_count() {
    let store = CompletionReceiptStore::default();
    let owner = test_owner("call-head-tail");
    let receipt_id = reserve_id(&store, &owner);
    let mut retention = RetentionState::default();
    let mut oversized = vec![b'H'; UNIFIED_EXEC_OUTPUT_MAX_BYTES / 2];
    oversized.extend(vec![b'M'; UNIFIED_EXEC_OUTPUT_MAX_BYTES]);
    oversized.extend(vec![b'T'; UNIFIED_EXEC_OUTPUT_MAX_BYTES / 2]);

    retention.insert_pending(
        receipt_id,
        owner.clone(),
        oversized,
        /*omitted_bytes*/ 100,
    );

    let RetentionLookup::Present {
        bytes,
        omitted_bytes,
    } = retention.lookup(receipt_id, &owner)
    else {
        panic!("oversized output should be retained as capped head and tail");
    };
    assert_eq!(bytes.len(), UNIFIED_EXEC_OUTPUT_MAX_BYTES);
    assert_eq!(&bytes[..8], b"HHHHHHHH");
    assert_eq!(&bytes[bytes.len() - 8..], b"TTTTTTTT");
    assert_eq!(
        omitted_bytes,
        100 + UNIFIED_EXEC_OUTPUT_MAX_BYTES,
        "middle bytes plus the incoming omitted count stay exact"
    );
}

#[test]
fn retained_bytes_never_exceed_cap() {
    let store = CompletionReceiptStore::default();
    for (index, size) in [
        UNIFIED_EXEC_OUTPUT_MAX_BYTES,
        UNIFIED_EXEC_OUTPUT_MAX_BYTES + 1,
        3 * UNIFIED_EXEC_OUTPUT_MAX_BYTES,
    ]
    .into_iter()
    .enumerate()
    {
        let owner = test_owner(&format!("call-cap-{index}"));
        let receipt_id = reserve_id(&store, &owner);
        let mut retention = RetentionState::default();
        retention.insert_pending(
            receipt_id,
            owner.clone(),
            vec![b'x'; size],
            /*omitted_bytes*/ 0,
        );
        let RetentionLookup::Present {
            bytes,
            omitted_bytes,
        } = retention.lookup(receipt_id, &owner)
        else {
            panic!("output of {size} bytes should be retained");
        };
        assert!(bytes.len() <= UNIFIED_EXEC_OUTPUT_MAX_BYTES);
        assert_eq!(
            bytes.len() + omitted_bytes,
            size,
            "retained plus omitted must account for every byte"
        );
    }
}

#[test]
fn pending_output_moves_to_sampled_and_retires_least_recently_sampled() {
    let store = CompletionReceiptStore::default();
    let first_owner = test_owner("call-first");
    let second_owner = test_owner("call-second");
    let first_id = reserve_id(&store, &first_owner);
    let second_id = reserve_id(&store, &second_owner);
    let mut retention = RetentionState::default();
    retention.insert_pending(first_id, first_owner.clone(), b"first".to_vec(), 0);
    retention.insert_pending(second_id, second_owner.clone(), b"second".to_vec(), 0);

    assert!(retention.move_to_sampled(first_id, /*sampled_seq*/ 1));
    assert!(retention.move_to_sampled(second_id, /*sampled_seq*/ 2));
    assert!(!retention.move_to_sampled(first_id, /*sampled_seq*/ 3));
    assert_eq!(retention.sampled_count(), 2);

    assert_eq!(retention.retire_least_recently_sampled(), Some(first_id));
    assert_eq!(retention.sampled_count(), 1);
    assert_eq!(
        retention.lookup(first_id, &first_owner),
        RetentionLookup::Retired
    );
    assert_eq!(
        retention.lookup(second_id, &second_owner),
        RetentionLookup::Present {
            bytes: b"second".to_vec(),
            omitted_bytes: 0,
        }
    );
    assert_eq!(retention.retire_least_recently_sampled(), Some(second_id));
    assert_eq!(retention.retire_least_recently_sampled(), None);
}

#[test]
fn retention_lookup_refuses_foreign_owners_in_every_state() {
    let store = CompletionReceiptStore::default();
    let owner = test_owner("call-owned");
    let foreign = ReceiptOwner::new(
        ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_00b2),
        /*runtime_generation*/ 7,
        "call-owned",
    )
    .expect("foreign owner should be valid");
    let receipt_id = reserve_id(&store, &owner);
    let mut retention = RetentionState::default();
    retention.insert_pending(receipt_id, owner.clone(), b"owned".to_vec(), 0);

    assert_eq!(
        retention.lookup(receipt_id, &foreign),
        RetentionLookup::ForeignOwner
    );
    assert!(retention.move_to_sampled(receipt_id, /*sampled_seq*/ 1));
    assert_eq!(
        retention.lookup(receipt_id, &foreign),
        RetentionLookup::ForeignOwner
    );
    assert_eq!(retention.retire_least_recently_sampled(), Some(receipt_id));
    assert_eq!(
        retention.lookup(receipt_id, &foreign),
        RetentionLookup::ForeignOwner
    );
    assert_eq!(
        retention.lookup(receipt_id, &owner),
        RetentionLookup::Retired
    );
}

#[test]
fn retention_drop_removes_output_in_every_state() {
    let store = CompletionReceiptStore::default();
    let mut retention = RetentionState::default();
    let pending_owner = test_owner("call-pending");
    let sampled_owner = test_owner("call-sampled");
    let retired_owner = test_owner("call-retired");
    let pending_id = reserve_id(&store, &pending_owner);
    let sampled_id = reserve_id(&store, &sampled_owner);
    let retired_id = reserve_id(&store, &retired_owner);
    retention.insert_pending(pending_id, pending_owner.clone(), b"p".to_vec(), 0);
    retention.insert_pending(sampled_id, sampled_owner.clone(), b"s".to_vec(), 0);
    retention.insert_pending(retired_id, retired_owner.clone(), b"r".to_vec(), 0);
    assert!(retention.move_to_sampled(sampled_id, /*sampled_seq*/ 1));
    assert!(retention.move_to_sampled(retired_id, /*sampled_seq*/ 2));
    assert_eq!(retention.retire_least_recently_sampled(), Some(sampled_id));

    assert!(retention.drop(pending_id));
    assert!(retention.drop(sampled_id));
    assert!(retention.drop(retired_id));
    assert!(!retention.drop(pending_id));
    for (receipt_id, owner) in [
        (pending_id, &pending_owner),
        (sampled_id, &sampled_owner),
        (retired_id, &retired_owner),
    ] {
        assert_eq!(retention.lookup(receipt_id, owner), RetentionLookup::Absent);
    }
    assert_eq!(retention.sampled_count(), 0);
}
