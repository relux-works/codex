use super::*;
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;

fn windows_shell_guidance_description() -> String {
    format!("\n\n{}", windows_shell_guidance())
}

fn has_parameter(tool: &ToolSpec, parameter_name: &str) -> bool {
    serde_json::to_value(tool)
        .expect("tool spec should serialize")
        .pointer(&format!("/parameters/properties/{parameter_name}"))
        .is_some()
}

#[test]
fn exec_command_tool_matches_expected_spec() {
    let tool = create_exec_command_tool(CommandToolOptions {
        allow_login_shell: true,
        exec_permission_approvals_enabled: false,
        async_notifications_available: false,
    });

    let description = if cfg!(windows) {
        format!(
            "Runs a command in a PTY, returning output or a session ID for ongoing interaction.{}",
            windows_shell_guidance_description()
        )
    } else {
        "Runs a command in a PTY, returning output or a session ID for ongoing interaction."
            .to_string()
    };
    let yield_time_ms_description = if cfg!(windows) {
        "Maximum time to wait before returning a session ID for a still-running command. Commands that finish sooner return immediately. For ordinary commands, omit this parameter to use the 10000 ms default. Effective range on Windows is 10000-30000 ms."
    } else {
        "Wait before yielding output. Defaults to 10000 ms; effective range is 250-30000 ms."
    };

    let mut properties = BTreeMap::from([
        (
            "cmd".to_string(),
            JsonSchema::string(Some("Shell command to execute.".to_string())),
        ),
        (
            "workdir".to_string(),
            JsonSchema::string(Some(
                    "Working directory for the command. Defaults to the turn cwd."
                        .to_string(),
                )),
        ),
        (
            "shell".to_string(),
            JsonSchema::string(Some(
                    "Shell binary to launch. Defaults to the user's default shell.".to_string(),
                )),
        ),
        (
            "tty".to_string(),
            JsonSchema::boolean(Some(
                    "True allocates a PTY for the command; false or omitted uses plain pipes."
                        .to_string(),
                )),
        ),
        (
            "yield_time_ms".to_string(),
            JsonSchema::integer(Some(yield_time_ms_description.to_string())),
        ),
        (
            "max_output_tokens".to_string(),
            JsonSchema::integer(Some(
                    "Output token budget. Defaults to 10000 tokens; larger requests may be capped by policy.".to_string(),
                )),
        ),
        (
            "login".to_string(),
            JsonSchema::boolean(Some(
                    "True runs the shell with -l/-i semantics; false disables them. Defaults to true.".to_string(),
                )),
        ),
    ]);
    properties.extend(create_approval_parameters(
        /*exec_permission_approvals_enabled*/ false,
    ));

    assert_eq!(
        tool,
        ToolSpec::Function(ResponsesApiTool {
            name: "exec_command".to_string(),
            description,
            strict: false,
            defer_loading: None,
            parameters: JsonSchema::object(
                properties,
                Some(vec!["cmd".to_string()]),
                Some(false.into())
            ),
            output_schema: Some(unified_exec_output_schema().into()),
        })
    );
}

#[test]
fn exec_command_tool_can_hide_shell_parameter() {
    let tool = create_exec_command_tool_with_environment_id(
        CommandToolOptions {
            allow_login_shell: true,
            exec_permission_approvals_enabled: false,
            async_notifications_available: false,
        },
        /*include_environment_id*/ false,
        /*include_shell_parameter*/ false,
        /*include_windows_shell_guidance*/ cfg!(windows),
    );

    assert!(!has_parameter(&tool, "shell"));
    assert!(has_parameter(&tool, "cmd"));
}

#[test]
fn write_stdin_tool_matches_expected_spec() {
    let tool = create_write_stdin_tool();

    let properties = BTreeMap::from([
        (
            "session_id".to_string(),
            JsonSchema::integer(Some(
                "Identifier of the running unified exec session.".to_string(),
            )),
        ),
        (
            "chars".to_string(),
            JsonSchema::string(Some(
                "Bytes to write to stdin. Defaults to empty, which polls without writing.".to_string(),
            )),
        ),
        (
            "yield_time_ms".to_string(),
            JsonSchema::integer(Some(
                "Wait before yielding output. Non-empty writes default to 250 ms and cap at 30000 ms; empty polls wait 5000-300000 ms by default.".to_string(),
            )),
        ),
        (
            "max_output_tokens".to_string(),
            JsonSchema::integer(Some(
                "Output token budget. Defaults to 10000 tokens; larger requests may be capped by policy.".to_string(),
            )),
        ),
    ]);

    assert_eq!(
        tool,
        ToolSpec::Function(ResponsesApiTool {
            name: "write_stdin".to_string(),
            description:
                "Writes characters to an existing unified exec session and returns recent output."
                    .to_string(),
            strict: false,
            defer_loading: None,
            parameters: JsonSchema::object(
                properties,
                Some(vec!["session_id".to_string()]),
                Some(false.into())
            ),
            output_schema: Some(unified_exec_output_schema().into()),
        })
    );
}

#[test]
fn request_permissions_tool_includes_full_permission_schema() {
    let tool =
        create_request_permissions_tool("Request extra permissions for this turn.".to_string());

    let properties = BTreeMap::from([
        (
            "reason".to_string(),
            JsonSchema::string(Some(
                "Optional short explanation for why additional permissions are needed.".to_string(),
            )),
        ),
        (
            "environment_id".to_string(),
            JsonSchema::string(Some(
                "Environment id from <environment_context>. Omit to use the primary environment."
                    .to_string(),
            )),
        ),
        ("permissions".to_string(), permission_profile_schema()),
    ]);

    assert_eq!(
        tool,
        ToolSpec::Function(ResponsesApiTool {
            name: "request_permissions".to_string(),
            description: "Request extra permissions for this turn.".to_string(),
            strict: false,
            defer_loading: None,
            parameters: JsonSchema::object(
                properties,
                Some(vec!["permissions".to_string()]),
                Some(false.into())
            ),
            output_schema: None,
        })
    );
}

fn command_options(async_notifications_available: bool) -> CommandToolOptions {
    CommandToolOptions {
        allow_login_shell: false,
        exec_permission_approvals_enabled: false,
        async_notifications_available,
    }
}

fn spec_description(tool: &ToolSpec) -> &str {
    match tool {
        ToolSpec::Function(spec) => spec.description.as_str(),
        ToolSpec::Namespace(_)
        | ToolSpec::ToolSearch { .. }
        | ToolSpec::WebSearch { .. }
        | ToolSpec::Freeform(_) => panic!("expected a function spec"),
    }
}

#[test]
fn exec_command_schema_gates_notify_on_exit_on_host_capability() {
    let available = create_exec_command_tool(command_options(true));
    assert!(has_parameter(&available, "notify_on_exit"));
    let description = spec_description(&available);
    assert!(
        description.contains("notify_on_exit")
            && description.contains("without terminating the process")
            && description.contains("A session ID alone does not promise a wake"),
        "available description must state the wake promise exactly: {description}"
    );
    assert!(
        !description.contains("end the turn"),
        "no-wait/end-turn guidance is deferred: {description}"
    );

    let unavailable = create_exec_command_tool(command_options(false));
    assert!(!has_parameter(&unavailable, "notify_on_exit"));
    let base_description = if cfg!(windows) {
        format!(
            "Runs a command in a PTY, returning output or a session ID for ongoing interaction.{}",
            windows_shell_guidance_description()
        )
    } else {
        "Runs a command in a PTY, returning output or a session ID for ongoing interaction."
            .to_string()
    };
    assert_eq!(
        spec_description(&unavailable),
        base_description.as_str(),
        "incapable hosts keep the base description with no wake promise"
    );
    assert!(
        !spec_description(&unavailable).contains("notify_on_exit"),
        "incapable hosts promise no wake"
    );
}

#[test]
fn exec_notification_tool_matches_expected_spec() {
    let tool = create_exec_notification_tool();

    assert!(has_parameter(&tool, "action"));
    assert!(has_parameter(&tool, "receipt_id"));
    assert!(has_parameter(&tool, "max_output_tokens"));
    let description = spec_description(&tool);
    assert!(
        description
            .contains("Releasing disarms the completion wake without terminating the process"),
        "release must promise disarm-without-kill: {description}"
    );

    let parameters = serde_json::to_value(&tool)
        .expect("tool spec should serialize")
        .pointer("/parameters")
        .expect("function spec should have parameters")
        .clone();
    assert_eq!(
        parameters
            .pointer("/required")
            .expect("required should exist"),
        &serde_json::json!(["action", "receipt_id"])
    );
    assert_eq!(
        parameters
            .pointer("/properties/action/enum")
            .expect("action should be an enum"),
        &serde_json::json!(["read", "release"])
    );
}
