// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
//! Exact restart recovery of a cold thread whose provider exists only in the request.

use super::connection_handling_websocket::DEFAULT_READ_TIMEOUT;
use super::connection_handling_websocket::create_config_toml;
use super::daemon_update_recovery::connect_daemon_client;
use super::daemon_update_recovery::request;
use anyhow::Context;
use anyhow::Result;
use app_test_support::DISABLE_PLUGIN_STARTUP_TASKS_ARG;
use app_test_support::create_final_assistant_message_sse_response;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_transport::daemon_recovery_file_path;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::StreamingSseServer;
use core_test_support::streaming_sse::start_streaming_sse_server;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command as StdCommand;
use std::process::Stdio;
use tempfile::TempDir;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio::time::sleep;
use tokio::time::timeout;

#[tokio::test]
async fn fenced_recovery_loads_a_cold_thread_on_a_provider_the_request_defines() -> Result<()> {
    let home = TempDir::new()?;
    let (_hold_original, original_gate) = oneshot::channel();
    let (default_provider, _) = start_streaming_sse_server(Vec::new()).await;
    let (relay, _completions) = start_streaming_sse_server(vec![
        vec![chunk(Some(original_gate), "Original")?],
        vec![chunk(/*gate*/ None, "Recovered")?],
    ])
    .await;
    create_config_toml(home.path(), default_provider.uri(), "never")?;
    let provider = json!({
        "model_providers.relay": {
            "name": "Relay",
            "base_url": format!("{}/v1", relay.uri()),
            "wire_api": "responses",
            "request_max_retries": 0,
            "stream_max_retries": 0
        }
    });
    let socket = home.path().join("control/server.sock");
    let capabilities = InitializeCapabilities {
        experimental_api: true,
        ..Default::default()
    };
    let mut server = app_server(home.path(), &socket)?.spawn()?;
    let mut client = connect_daemon_client(&socket, capabilities.clone()).await?;
    let thread = request(
        &mut client,
        /*id*/ 2,
        "thread/start",
        json!({"modelProvider": "relay", "config": provider}),
    )
    .await?["thread"]["id"]
        .as_str()
        .context("thread ID")?
        .to_string();
    let turn = request(
        &mut client,
        /*id*/ 3,
        "turn/start",
        json!({"threadId": thread, "input": [{"type": "text", "text": "Hello"}]}),
    )
    .await?["turn"]["id"]
        .as_str()
        .context("turn ID")?
        .to_string();
    requests(&relay, /*count*/ 1).await?;
    signal(&server, "-HUP")?;
    let suspend = json!({"threadId": thread, "turnId": turn});
    let suspended = timeout(DEFAULT_READ_TIMEOUT, async {
        let mut id = 4;
        loop {
            id += 1;
            match request(&mut client, id, "thread/restart/suspend", suspend.clone()).await {
                Err(error)
                    if error
                        .to_string()
                        .contains("settled new-work admission fence") =>
                {
                    sleep(Duration::from_millis(25)).await;
                }
                result => return result,
            }
        }
    })
    .await??;
    assert_eq!(suspended["outcome"]["status"], "suspended", "{suspended}");
    drop(client);
    signal(&server, "-TERM")?;
    let status = timeout(DEFAULT_READ_TIMEOUT, server.wait()).await??;
    anyhow::ensure!(status.success(), "suspended server exited with {status}");
    let saved = daemon_recovery_file_path(home.path());
    if saved.exists() {
        std::fs::remove_file(saved)?;
    }

    let capability = "0123456789abcdef0123456789abcdef";
    let grant = home.path().join("grant.json");
    std::fs::write(
        &grant,
        json!({
            "schemaVersion": 1,
            "generationId": "generation-1",
            "capability": capability,
            "targets": [{"threadId": thread, "turnId": turn}]
        })
        .to_string(),
    )?;
    std::fs::set_permissions(&grant, std::fs::Permissions::from_mode(0o600))?;
    let mut successor = app_server(home.path(), &socket)?
        .env("CODEX_APP_SERVER_START_FENCED", "1")
        .env("CODEX_INTERNAL_RESTART_RECOVERY_GRANT_FILE", &grant)
        .spawn()?;
    let mut client = connect_daemon_client(&socket, capabilities).await?;
    let recover = |config: serde_json::Value| json!({"threadId": thread, "turnId": turn, "capability": capability, "config": config});
    let selection = request(
        &mut client,
        /*id*/ 2,
        "thread/restart/recover",
        recover(json!({"model": "mock-model", "model_providers.relay": provider["model_providers.relay"]})),
    )
    .await
    .expect_err("recovery config cannot select the model");
    assert!(selection.to_string().contains("not model"), "{selection}");
    request(
        &mut client,
        /*id*/ 3,
        "thread/restart/recover",
        recover(serde_json::Value::Null),
    )
    .await
    .expect_err("a cold thread needs its provider definition");
    let recovered = request(
        &mut client,
        /*id*/ 4,
        "thread/restart/recover",
        recover(provider),
    )
    .await?;
    assert_eq!(recovered["turn"]["id"], turn);
    requests(&relay, /*count*/ 2).await?;
    let completed = timeout(DEFAULT_READ_TIMEOUT, async {
        let mut id = 5;
        loop {
            id += 1;
            let read = request(
                &mut client,
                id,
                "thread/read",
                json!({"threadId": thread, "includeTurns": true}),
            )
            .await?;
            let last = read["thread"]["turns"]
                .as_array()
                .and_then(|turns| turns.last())
                .cloned()
                .context("recovered turn")?;
            if last["status"] == "completed" {
                return Ok::<_, anyhow::Error>(last);
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;
    assert_eq!(completed["id"], turn);
    assert!(completed.to_string().contains("Recovered"), "{completed}");
    assert!(default_provider.requests().await.is_empty());
    successor.kill().await?;
    Ok(())
}

fn app_server(home: &Path, socket_path: &Path) -> Result<Command> {
    let mut command = Command::new(codex_utils_cargo_bin::cargo_bin("codex-app-server")?);
    command
        .args(["--listen", &format!("unix://{}", socket_path.display())])
        .arg(DISABLE_PLUGIN_STARTUP_TASKS_ARG)
        .arg("--managed-daemon")
        .env("CODEX_HOME", home)
        .env(
            "CODEX_APP_SERVER_MANAGED_CONFIG_PATH",
            home.join("managed_config.toml"),
        )
        .env(
            codex_app_server_transport::DAEMON_SHUTDOWN_SOCKET_ENV,
            socket_path,
        )
        .current_dir(home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    Ok(command)
}

fn signal(server: &Child, signal: &str) -> Result<()> {
    let pid = server.id().context("server pid")?;
    let status = StdCommand::new("kill")
        .arg(signal)
        .arg(pid.to_string())
        .status()?;
    anyhow::ensure!(status.success(), "failed to signal server {pid}: {status}");
    Ok(())
}

fn chunk(gate: Option<oneshot::Receiver<()>>, body: &str) -> Result<StreamingSseChunk> {
    Ok(StreamingSseChunk {
        gate,
        body: create_final_assistant_message_sse_response(body)?,
    })
}

async fn requests(mock: &StreamingSseServer, count: usize) -> Result<()> {
    timeout(Duration::from_secs(5), mock.wait_for_request_count(count)).await?;
    Ok(())
}
