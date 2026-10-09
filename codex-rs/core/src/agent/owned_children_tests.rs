use super::OwnedChildInspection;
use codex_protocol::ThreadId;
use codex_protocol::protocol::AgentStatus;
use pretty_assertions::assert_eq;

fn loaded(status: AgentStatus) -> OwnedChildInspection {
    OwnedChildInspection::Loaded {
        thread_id: ThreadId::new(),
        status,
    }
}

#[test]
fn pending_native_work_is_only_pending_init_and_running() {
    assert!(loaded(AgentStatus::PendingInit).is_pending_native_work());
    assert!(loaded(AgentStatus::Running).is_pending_native_work());

    for status in [
        AgentStatus::Interrupted,
        AgentStatus::Completed(None),
        AgentStatus::Completed(Some("done".to_string())),
        AgentStatus::Errored("boom".to_string()),
        AgentStatus::Shutdown,
        AgentStatus::NotFound,
    ] {
        assert!(
            !loaded(status.clone()).is_pending_native_work(),
            "status {status:?} must not permit registration"
        );
    }

    let unloaded = OwnedChildInspection::Unloaded {
        thread_id: ThreadId::new(),
    };
    assert!(!unloaded.is_pending_native_work());
    assert_eq!(unloaded.status(), None);
}
