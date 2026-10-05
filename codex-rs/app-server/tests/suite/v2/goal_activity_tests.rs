use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_features::Feature;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;

#[test_case::test_case(true; "created")]
#[test_case::test_case(false; "refused")]
#[tokio::test]
async fn v2_create_goal_exposes_sleep_only_after_committed_success(success: bool) -> Result<()> {
    let server = responses::start_mock_server().await;
    let mock = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse(vec![
                responses::ev_function_call(
                    "create",
                    "create_goal",
                    &json!({"objective": if success {"work"} else {""}}).to_string(),
                ),
                responses::ev_completed("r1"),
            ]),
            responses::sse(vec![
                responses::ev_function_call("complete", "update_goal", r#"{"status":"complete"}"#),
                responses::ev_completed("r2"),
            ]),
            responses::sse(vec![responses::ev_completed("r3")]),
        ],
    )
    .await;
    let home = TempDir::new()?;
    let mut catalog = codex_models_manager::bundled_models_response()?;
    let model = catalog
        .models
        .iter_mut()
        .find(|m| m.slug == "gpt-5.5")
        .expect("model");
    model.experimental_supported_tools.retain(|t| t != "clock");
    let path = home.path().join("models.json");
    std::fs::write(&path, serde_json::to_vec(&catalog)?)?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.5")
        .enable_feature(Feature::Goals)
        .disable_feature(Feature::CurrentTimeReminder)
        .with_root_config(&format!(
            "model_catalog_json = {}",
            serde_json::to_string(&path)?
        ))
        .write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .build_initialized()
        .await?;
    let request = app
        .send_thread_start_request_with_auto_env(ThreadStartParams::default())
        .await?;
    let ThreadStartResponse { thread, .. } = app.read_response(request).await?;
    let request = app
        .send_turn_start_request(TurnStartParams {
            thread_id: thread.id,
            input: vec![UserInput::Text {
                text: "create then complete".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse = app.read_response(request).await?;
    let _: TurnCompletedNotification = app.read_notification("turn/completed").await?;
    let requests = mock.requests();
    assert_eq!(
        requests
            .iter()
            .map(|r| r.tool_by_name("clock", "sleep").is_some())
            .collect::<Vec<_>>(),
        [false, success, false]
    );
    let output = requests[1]
        .function_call_output_text("create")
        .expect("create result");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&output)
            .ok()
            .map(|v| v["goal"]["status"].clone()),
        success.then(|| json!("active"))
    );
    Ok(())
}
