use anyhow::Context;
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_repeating_assistant;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::CommandExecParams;
use codex_app_server_protocol::CommandExecResponse;
use codex_app_server_protocol::McpServerToolCallParams;
use codex_app_server_protocol::McpServerToolCallResponse;
use codex_app_server_protocol::ProcessExitedNotification;
use codex_app_server_protocol::ProcessKillParams;
use codex_app_server_protocol::ProcessKillResponse;
use codex_app_server_protocol::ProcessSpawnParams;
use codex_app_server_protocol::ProcessSpawnResponse;
use codex_app_server_protocol::ServerDiagnosticsParams;
use codex_app_server_protocol::ServerDiagnosticsResponse;
use codex_app_server_protocol::ThreadItemsListParams;
use codex_app_server_protocol::ThreadItemsListResponse;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadUnsubscribeParams;
use codex_app_server_protocol::ThreadUnsubscribeResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;
#[cfg(target_os = "macos")]
use tokio::process::Command;
use tokio::time::timeout;

use super::connection_handling_websocket::DEFAULT_READ_TIMEOUT;
use super::mcp_tool::TEST_SERVER_NAME;
use super::mcp_tool::TEST_TOOL_NAME;
use super::mcp_tool::start_mcp_server;

const FD_PRESSURE_TARGET: usize = 256;
const MAX_HELPERS: usize = 180;

#[tokio::test]
async fn app_server_keeps_working_above_256_fds_and_releases_named_helpers() -> Result<()> {
    let responses_server = create_mock_responses_server_repeating_assistant("Done").await;
    let (mcp_server_url, mcp_server_handle) = start_mcp_server(/*sensitive_action*/ None).await?;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&responses_server.uri())
        .with_extra_config(&format!(
            "[mcp_servers.{TEST_SERVER_NAME}]\nurl = \"{mcp_server_url}/mcp\""
        ))
        .write(codex_home.path())?;

    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let diagnostics: ServerDiagnosticsResponse = app_server
        .request(|request_id| ClientRequest::ServerDiagnostics {
            request_id,
            params: ServerDiagnosticsParams::default(),
        })
        .await?;
    let pid = i32::try_from(diagnostics.process.id)?;
    let baseline_fds = app_server_fd_count(pid).await?;
    let mut helpers = Vec::new();

    for index in 0..MAX_HELPERS {
        let process_handle = format!("fd-pressure-helper-{index:03}");
        let request_id = app_server
            .send_process_spawn_request(ProcessSpawnParams {
                command: vec!["/bin/sleep".to_string(), "120".to_string()],
                process_handle: process_handle.clone(),
                cwd: AbsolutePathBuf::try_from(codex_home.path())?,
                tty: false,
                stream_stdin: false,
                stream_stdout_stderr: false,
                output_bytes_cap: Some(None),
                timeout_ms: Some(None),
                env: None,
                size: None,
            })
            .await?;
        let _: ProcessSpawnResponse =
            timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;
        helpers.push(process_handle);

        if index % 8 == 7 && app_server_fd_count(pid).await? > FD_PRESSURE_TARGET {
            break;
        }
    }

    let pressure_fds = app_server_fd_count(pid).await?;
    assert!(
        pressure_fds > FD_PRESSURE_TARGET,
        "app-server did not cross the descriptor-pressure boundary: baseline={baseline_fds}, pressure={pressure_fds}, helpers={}",
        helpers.len()
    );

    let command_request_id = app_server
        .send_command_exec_request(CommandExecParams {
            command: vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "printf command-exec-under-fd-pressure".to_string(),
            ],
            process_id: None,
            tty: false,
            stream_stdin: false,
            stream_stdout_stderr: false,
            output_bytes_cap: None,
            disable_output_cap: false,
            disable_timeout: false,
            timeout_ms: None,
            cwd: Some(codex_home.path().to_path_buf()),
            env: None,
            size: None,
            sandbox_policy: None,
            permission_profile: None,
        })
        .await?;
    let command: CommandExecResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.read_response(command_request_id),
    )
    .await??;
    assert_eq!(command.exit_code, 0);
    assert_eq!(command.stdout, "command-exec-under-fd-pressure");

    let thread = app_server
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?
        .thread;
    let thread_id = thread.id.clone();

    let tool_call: McpServerToolCallResponse = app_server
        .request(|request_id| ClientRequest::McpServerToolCall {
            request_id,
            params: McpServerToolCallParams {
                thread_id: thread_id.clone(),
                server: TEST_SERVER_NAME.to_string(),
                tool: TEST_TOOL_NAME.to_string(),
                arguments: Some(json!({"message": "fd pressure"})),
                meta: None,
            },
        })
        .await?;
    assert_eq!(tool_call.is_error, Some(false));

    app_server
        .start_turn_and_wait_for_completion(TurnStartParams {
            thread_id: thread_id.clone(),
            input: vec![UserInput::Text {
                text: "record history under fd pressure".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;

    let ThreadReadResponse { thread, .. } = app_server
        .request(|request_id| ClientRequest::ThreadRead {
            request_id,
            params: ThreadReadParams {
                thread_id: thread_id.clone(),
                include_turns: true,
            },
        })
        .await?;
    assert_eq!(thread.id, thread_id);

    let items: ThreadItemsListResponse = app_server
        .request(|request_id| ClientRequest::ThreadItemsList {
            request_id,
            params: ThreadItemsListParams {
                thread_id: thread_id.clone(),
                turn_id: None,
                cursor: None,
                limit: None,
                sort_direction: None,
            },
        })
        .await?;
    assert!(
        !items.data.is_empty(),
        "history item list should retain the completed turn"
    );

    let unsubscribe: ThreadUnsubscribeResponse = app_server
        .request(|request_id| ClientRequest::ThreadUnsubscribe {
            request_id,
            params: ThreadUnsubscribeParams { thread_id },
        })
        .await?;
    assert_eq!(
        unsubscribe.status,
        codex_app_server_protocol::ThreadUnsubscribeStatus::Unsubscribed
    );

    for helper in helpers {
        kill_helper(&mut app_server, helper).await?;
    }

    let final_fds = wait_for_fd_count_at_most(pid, baseline_fds + 64).await?;
    assert!(
        final_fds <= baseline_fds + 64,
        "named helper descriptors were not released: baseline={baseline_fds}, final={final_fds}"
    );

    mcp_server_handle.abort();
    let _ = mcp_server_handle.await;
    assert!(app_server.shutdown_gracefully().await?.success());
    Ok(())
}

async fn kill_helper(app_server: &mut TestAppServer, process_handle: String) -> Result<()> {
    let request_id = app_server
        .send_process_kill_request(ProcessKillParams {
            process_handle: process_handle.clone(),
        })
        .await?;
    let _: ProcessKillResponse =
        timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;

    let notification = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.read_stream_until_notification_message("process/exited"),
    )
    .await??;
    let params = notification
        .params
        .context("process/exited notification should include params")?;
    let exited: ProcessExitedNotification = serde_json::from_value(params)?;
    assert_eq!(exited.process_handle, process_handle);
    Ok(())
}

async fn wait_for_fd_count_at_most(pid: i32, limit: usize) -> Result<usize> {
    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let count = app_server_fd_count(pid).await?;
            if count <= limit {
                return Ok(count);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?
}

async fn app_server_fd_count(pid: i32) -> Result<usize> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("/usr/sbin/lsof")
            .args(["-a", "-p", &pid.to_string(), "-d", "0-8191", "-F", "f"])
            .output()
            .await?;
        if !output.status.success() {
            anyhow::bail!("lsof failed for app-server pid {pid}");
        }
        Ok(String::from_utf8(output.stdout)?
            .lines()
            .filter(|line| {
                line.strip_prefix('f').is_some_and(|fd| {
                    !fd.is_empty() && fd.chars().all(|char| char.is_ascii_digit())
                })
            })
            .count())
    }

    #[cfg(target_os = "linux")]
    {
        return Ok(std::fs::read_dir(format!("/proc/{pid}/fd"))?.count());
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = pid;
        anyhow::bail!("descriptor counting is unsupported on this Unix target");
    }
}
