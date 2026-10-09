use super::is_final;
use codex_protocol::protocol::AgentStatus;

#[test]
fn interrupted_stays_non_final() {
    assert!(!is_final(&AgentStatus::PendingInit));
    assert!(!is_final(&AgentStatus::Running));
    assert!(!is_final(&AgentStatus::Interrupted));

    assert!(is_final(&AgentStatus::Completed(None)));
    assert!(is_final(&AgentStatus::Completed(Some("done".to_string()))));
    assert!(is_final(&AgentStatus::Errored("boom".to_string())));
    assert!(is_final(&AgentStatus::Shutdown));
    assert!(is_final(&AgentStatus::NotFound));
}
