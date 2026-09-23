use std::time::Duration;

use anyhow::Result;
use app_test_support::DEFAULT_CLIENT_NAME;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_repeating_assistant;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerDiagnosticsGauge;
use codex_app_server_protocol::ServerDiagnosticsParams;
use codex_app_server_protocol::ServerDiagnosticsResourceLimit;
use codex_app_server_protocol::ServerDiagnosticsResponse;
use codex_app_server_protocol::ThreadStartParams;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;

const READ_TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::test]
async fn server_diagnostics_exposes_process_and_registered_thread_gauge() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    app_server
        .start_thread(ThreadStartParams::default())
        .await?;

    let diagnostics: ServerDiagnosticsResponse = app_server
        .request(|request_id| ClientRequest::ServerDiagnostics {
            request_id,
            params: ServerDiagnosticsParams::default(),
        })
        .await?;

    assert!(diagnostics.process.id > 0);
    assert!(diagnostics.process.resident_memory_bytes.is_some());
    #[cfg(target_os = "macos")]
    assert!(diagnostics.process.physical_footprint_bytes.is_some());
    #[cfg(not(target_os = "macos"))]
    assert_eq!(diagnostics.process.physical_footprint_bytes, None);
    let file_descriptor_limits = diagnostics
        .process
        .file_descriptor_limits
        .expect("diagnostics should report file-descriptor limit coverage");
    #[cfg(unix)]
    for (name, value) in [
        ("soft", &file_descriptor_limits.soft),
        ("hard", &file_descriptor_limits.hard),
    ] {
        match value {
            ServerDiagnosticsResourceLimit::Finite { value } => {
                let value = value
                    .parse::<u64>()
                    .expect("finite resource limit should be decimal text");
                if name == "soft" {
                    assert!(
                        value >= 8_192,
                        "app-server soft file-descriptor limit is too low: {value}"
                    );
                }
            }
            ServerDiagnosticsResourceLimit::Unlimited => {}
            ServerDiagnosticsResourceLimit::Unknown => {
                panic!("app-server {name} file-descriptor limit is unknown")
            }
            ServerDiagnosticsResourceLimit::Unsupported => {
                panic!("app-server {name} file-descriptor limit is unsupported")
            }
        }
    }
    #[cfg(not(unix))]
    assert_eq!(
        &file_descriptor_limits.soft,
        &ServerDiagnosticsResourceLimit::Unsupported
    );
    #[cfg(not(unix))]
    assert_eq!(
        &file_descriptor_limits.hard,
        &ServerDiagnosticsResourceLimit::Unsupported
    );
    // Startup may still be finishing after its response was queued.
    assert!(matches!(
        diagnostics
            .gauges
            .iter()
            .find(|gauge| gauge.name == "app.requests.in_flight")
            .map(|gauge| gauge.value),
        Some(1 | 2)
    ));
    for expected_gauge in [
        ServerDiagnosticsGauge {
            name: "core.threads.live".to_string(),
            value: 1,
        },
        ServerDiagnosticsGauge {
            name: "app.work.new_admission_fenced".to_string(),
            value: 0,
        },
        ServerDiagnosticsGauge {
            name: "app.work.new_admission_closed".to_string(),
            value: 0,
        },
    ] {
        assert_eq!(
            diagnostics
                .gauges
                .iter()
                .find(|gauge| gauge.name == expected_gauge.name),
            Some(&expected_gauge)
        );
    }

    Ok(())
}

#[tokio::test]
async fn server_diagnostics_requires_experimental_capability() -> Result<()> {
    let codex_home = TempDir::new()?;
    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build()
        .await?;
    let initialization = app_server
        .initialize_with_capabilities(
            ClientInfo {
                name: DEFAULT_CLIENT_NAME.to_string(),
                title: None,
                version: "0.1.0".to_string(),
            },
            Some(InitializeCapabilities {
                experimental_api: false,
                ..Default::default()
            }),
        )
        .await?;
    assert!(matches!(initialization, JSONRPCMessage::Response(_)));

    let request_id = app_server
        .send_raw_request("server/diagnostics", Some(json!({})))
        .await?;
    let error = timeout(
        READ_TIMEOUT,
        app_server.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;

    assert_eq!(error.error.code, -32600);
    assert_eq!(
        error.error.message,
        "server/diagnostics requires experimentalApi capability"
    );
    assert_eq!(error.error.data, None);

    Ok(())
}
