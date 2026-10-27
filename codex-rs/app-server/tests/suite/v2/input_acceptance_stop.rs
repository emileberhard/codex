// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ThreadInputStopParams;
use codex_app_server_protocol::ThreadInputStopResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use tempfile::TempDir;

#[tokio::test]
async fn stopped_input_ids_survive_unloaded_checkpoint_without_starting_a_turn() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_final_assistant_message_sse_response("unexpected")?,
    ])
    .await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let thread = app
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?
        .thread;
    let stopped: ThreadInputStopResponse = app
        .request(|request_id| ClientRequest::ThreadInputStop {
            request_id,
            params: ThreadInputStopParams {
                thread_id: thread.id.clone(),
                client_user_message_id: "stopped-while-loaded".into(),
            },
        })
        .await?;
    assert!(stopped.acceptance.is_none());
    app.shutdown().await?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let stopped: ThreadInputStopResponse = app
        .request(|request_id| ClientRequest::ThreadInputStop {
            request_id,
            params: ThreadInputStopParams {
                thread_id: thread.id.clone(),
                client_user_message_id: "stopped-while-unloaded".into(),
            },
        })
        .await?;
    assert!(stopped.acceptance.is_none());
    let _: ThreadResumeResponse = app
        .request(|request_id| ClientRequest::ThreadResume {
            request_id,
            params: ThreadResumeParams {
                thread_id: thread.id.clone(),
                ..Default::default()
            },
        })
        .await?;
    for client_id in ["stopped-while-loaded", "stopped-while-unloaded"] {
        let rejected: Result<TurnStartResponse> = app
            .request(|request_id| ClientRequest::TurnStart {
                request_id,
                params: TurnStartParams {
                    thread_id: thread.id.clone(),
                    client_user_message_id: Some(client_id.to_owned()),
                    input: vec![UserInput::Text {
                        text: "never execute".to_string(),
                        text_elements: Vec::new(),
                    }],
                    ..Default::default()
                },
            })
            .await;
        assert!(
            rejected
                .unwrap_err()
                .to_string()
                .contains("durably stopped before acceptance")
        );
    }
    app.shutdown().await?;
    Ok(())
}
