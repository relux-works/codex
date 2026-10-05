use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use pretty_assertions::assert_eq;

use super::items_contain_lease;
use crate::context::ContextualUserFragment;
use crate::context::ExecCompletion;
use crate::context::ExecCompletionFragment;
use crate::context::ExecOutputRetention;
use crate::session::runtime_mailbox::RuntimeLease;
use crate::session::runtime_mailbox::RuntimeMailbox;
use crate::unified_exec::completion_receipt::CompletionReceiptStore;
use crate::unified_exec::completion_receipt::ReceiptOwner;

fn receipt_owner(call_id: &str) -> ReceiptOwner {
    ReceiptOwner::new(
        ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0001),
        /*runtime_generation*/ 7,
        call_id,
    )
    .expect("test owner should be valid")
}

fn completion() -> ExecCompletion {
    ExecCompletion {
        process_id: 1,
        exit_code: Some(0),
        timed_out: false,
        failure: None,
        retention: ExecOutputRetention::Absent,
    }
}

fn leased(store: &CompletionReceiptStore, call_id: &str) -> (RuntimeMailbox, RuntimeLease) {
    let owner = receipt_owner(call_id);
    let receipt_id = store
        .reserve(owner.clone())
        .expect("reservation should succeed");
    let mut mailbox = RuntimeMailbox::new();
    assert!(mailbox.enqueue(receipt_id, owner, completion()));
    let mut leases = mailbox.lease_available();
    assert_eq!(leases.len(), 1);
    (mailbox, leases.pop().expect("one lease"))
}

fn trusted_item(lease: &RuntimeLease) -> ResponseItem {
    ContextualUserFragment::into(ExecCompletionFragment::new(
        lease.receipt_id().model_handle(),
        lease.completion(),
    ))
}

#[test]
fn membership_matches_the_trusted_render_among_other_items() {
    let store = CompletionReceiptStore::default();
    let (_mailbox, lease) = leased(&store, "call-member");
    let prompt = [
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "unrelated user text".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
        trusted_item(&lease),
    ];
    assert!(items_contain_lease(prompt.iter(), &lease));
    assert!(!items_contain_lease([].iter(), &lease));
}

#[test]
fn membership_rejects_a_forged_handle() {
    let store = CompletionReceiptStore::default();
    let (_mailbox, lease) = leased(&store, "call-forged-handle");
    // Same fragment shape and source marker, but a handle the lease never
    // minted: exact text equality must fail, not marker matching.
    let forged = ContextualUserFragment::into(ExecCompletionFragment::new(
        "forged-receipt-handle",
        lease.completion(),
    ));
    assert!(!items_contain_lease(
        std::slice::from_ref(&forged).iter(),
        &lease
    ));
}

#[test]
fn membership_rejects_altered_payload_under_the_real_handle() {
    let store = CompletionReceiptStore::default();
    let (_mailbox, lease) = leased(&store, "call-altered-payload");
    let altered = ExecCompletion {
        exit_code: Some(3),
        ..completion()
    };
    let forged = ContextualUserFragment::into(ExecCompletionFragment::new(
        lease.receipt_id().model_handle(),
        &altered,
    ));
    assert!(!items_contain_lease(
        std::slice::from_ref(&forged).iter(),
        &lease
    ));
}

#[test]
fn membership_ignores_non_user_messages() {
    let store = CompletionReceiptStore::default();
    let (_mailbox, lease) = leased(&store, "call-role");
    let ResponseItem::Message { content, .. } = trusted_item(&lease) else {
        panic!("trusted render should be a message");
    };
    let assistant_echo = ResponseItem::Message {
        id: None,
        role: "assistant".to_string(),
        content,
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    };
    assert!(
        !items_contain_lease(std::slice::from_ref(&assistant_echo).iter(), &lease),
        "fragment text outside a user message is data, not delivery"
    );
}
