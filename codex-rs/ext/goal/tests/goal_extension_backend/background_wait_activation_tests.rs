use std::sync::Arc;
use std::sync::Weak;

use codex_analytics::AnalyticsEventsClient;
use codex_extension_api::AsyncNotificationSupport;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::ThreadStartInput;
use codex_goal_extension::GoalExtensionConfig;
use codex_goal_extension::GoalRuntimeHandle;
use codex_goal_extension::GoalService;
use codex_goal_extension::install_with_backend;
use codex_protocol::protocol::SessionSource;
use pretty_assertions::assert_eq;

use super::test_runtime;
use super::test_thread_id;

async fn background_wait_enabled_with_support(
    support: Option<AsyncNotificationSupport>,
) -> anyhow::Result<bool> {
    let runtime = test_runtime().await?;
    let thread_id = test_thread_id()?;
    let mut builder = ExtensionRegistryBuilder::<()>::new();
    install_with_backend(
        &mut builder,
        runtime,
        AnalyticsEventsClient::disabled(),
        /*metrics_client*/ None,
        Weak::new(),
        Arc::new(GoalService::new()),
        |_| GoalExtensionConfig {
            enabled: true,
            max_goal_token_budget: None,
        },
    );
    let registry = builder.build();
    let session_store = ExtensionData::new("session-1");
    let thread_store = ExtensionData::new(thread_id.to_string());
    if let Some(support) = support {
        thread_store.insert(support);
    }
    for contributor in registry.thread_lifecycle_contributors() {
        contributor
            .on_thread_start(ThreadStartInput {
                config: &(),
                session_source: &SessionSource::Cli,
                persistent_thread_state_available: true,
                environments: &[],
                mcp_resource_client: None,
                extension_metrics: None,
                session_store: &session_store,
                thread_store: &thread_store,
            })
            .await;
    }
    let runtime = thread_store
        .get::<GoalRuntimeHandle>()
        .expect("goal runtime should exist");
    Ok(runtime.background_wait_state().is_enabled())
}

#[tokio::test]
async fn background_wait_activates_only_on_available_hosts() -> anyhow::Result<()> {
    assert_eq!(
        background_wait_enabled_with_support(Some(AsyncNotificationSupport::Available)).await?,
        true,
        "capable hosts should activate the background-wait policy"
    );
    assert_eq!(
        background_wait_enabled_with_support(Some(AsyncNotificationSupport::Unavailable)).await?,
        false,
        "headless hosts should leave the policy inactive"
    );
    assert_eq!(
        background_wait_enabled_with_support(None).await?,
        false,
        "threads without the marker should leave the policy inactive"
    );
    Ok(())
}
