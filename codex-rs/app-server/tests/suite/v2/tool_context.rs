// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use anyhow::Context;
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::DaemonToolContextParams;
use codex_app_server_protocol::DaemonToolContextResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_features::Feature;
use core_test_support::responses;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn tool_context_uses_the_live_tool_owner_and_rejects_unrelated_processes() -> Result<()> {
    let workspace = TempDir::new()?;
    let pid_path = workspace.path().join("tool.pid");
    let arguments = json!({
        "cmd": format!("export CODEX_THREAD_ID=unrelated-ambient; sleep 30 & context_pid=$!; echo $context_pid > '{}'; echo context-ready; wait $context_pid", pid_path.display()),
        "yield_time_ms": 1000,
        "login": false,
    });
    let server = create_mock_responses_server_sequence_unchecked(vec![
        responses::sse(vec![
            responses::ev_response_created("tool-context"),
            responses::ev_function_call("owned-tool-call", "exec_command", &arguments.to_string()),
            responses::ev_completed("tool-context"),
        ]),
        create_final_assistant_message_sse_response("Done")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_sandbox_mode("danger-full-access")
        .with_approval_policy("never")
        .enable_feature(Feature::UnifiedExec)
        .disable_feature(Feature::ShellSnapshot)
        .write(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let thread = mcp
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?
        .thread;
    let other = mcp
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?
        .thread;
    let accepted: TurnStartResponse = mcp
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: thread.id.clone(),
                input: vec![UserInput::Text {
                    text: "run the tool".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?;
    let pid: u32 = timeout(Duration::from_secs(10), async {
        loop {
            match tokio::fs::read_to_string(&pid_path).await {
                Ok(value) => return Ok::<_, anyhow::Error>(value.trim().parse::<u32>()?),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    })
    .await??;
    let identity = codex_utils_pty::local_process_identity(pid).context("tool process identity")?;
    let context = timeout(Duration::from_secs(3), async {
        loop {
            let response: DaemonToolContextResponse = mcp
                .request(|request_id| ClientRequest::DaemonToolContext {
                    request_id,
                    params: DaemonToolContextParams {
                        pid: pid as i32,
                        started_at_unix_micros: identity.started_at_unix_micros,
                    },
                })
                .await?;
            if let Some(context) = response.context {
                return Ok::<_, anyhow::Error>(context);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert_eq!(context.thread_id, thread.id);
    assert_ne!(context.thread_id, other.id);
    assert_eq!(context.turn_id, accepted.turn.id);
    assert_eq!(context.tool_call_id, "owned-tool-call");
    assert_ne!(context.root_process_identity.pid, pid as i32);
    assert!(
        codex_utils_pty::verified_process_ancestry(identity)
            .context("tool ancestry")?
            .iter()
            .any(
                |process| process.pid as i32 == context.root_process_identity.pid
                    && process.started_at_unix_micros
                        == context.root_process_identity.started_at_unix_micros
            )
    );
    let wrong_birth: DaemonToolContextResponse = mcp
        .request(|request_id| ClientRequest::DaemonToolContext {
            request_id,
            params: DaemonToolContextParams {
                pid: pid as i32,
                started_at_unix_micros: identity.started_at_unix_micros + 1,
            },
        })
        .await?;
    assert!(wrong_birth.context.is_none());
    let unrelated = codex_utils_pty::local_process_identity(std::process::id())
        .context("test process identity")?;
    let unrelated: DaemonToolContextResponse = mcp
        .request(|request_id| ClientRequest::DaemonToolContext {
            request_id,
            params: DaemonToolContextParams {
                pid: unrelated.pid as i32,
                started_at_unix_micros: unrelated.started_at_unix_micros,
            },
        })
        .await?;
    assert!(unrelated.context.is_none());
    timeout(
        Duration::from_secs(10),
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;
    let _: codex_app_server_protocol::ThreadBackgroundTerminalsCleanResponse = mcp
        .request(|request_id| ClientRequest::ThreadBackgroundTerminalsClean {
            request_id,
            params: codex_app_server_protocol::ThreadBackgroundTerminalsCleanParams {
                thread_id: thread.id,
            },
        })
        .await?;
    let terminated: DaemonToolContextResponse = mcp
        .request(|request_id| ClientRequest::DaemonToolContext {
            request_id,
            params: DaemonToolContextParams {
                pid: pid as i32,
                started_at_unix_micros: identity.started_at_unix_micros,
            },
        })
        .await?;
    assert!(terminated.context.is_none());
    mcp.shutdown_gracefully().await?;
    Ok(())
}
