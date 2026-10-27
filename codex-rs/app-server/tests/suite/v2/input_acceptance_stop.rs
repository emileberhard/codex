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
    assert!(app.shutdown_gracefully().await?.success());
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
        let request = app
            .send_request(
                "turn/start",
                Some(serde_json::to_value(TurnStartParams {
                    thread_id: thread.id.clone(),
                    client_user_message_id: Some(client_id.to_owned()),
                    input: vec![UserInput::Text {
                        text: "never execute".to_string(),
                        text_elements: Vec::new(),
                    }],
                    ..Default::default()
                })?),
            )
            .await?;
        let rejected = app
            .read_stream_until_error_message(codex_app_server_protocol::RequestId::Integer(request))
            .await?;
        assert!(
            rejected
                .error
                .message
                .contains("durably stopped before acceptance")
        );
    }
    assert!(app.shutdown_gracefully().await?.success());
    Ok(())
}

#[tokio::test]
async fn explicit_persistence_preserves_an_empty_thread_before_input() -> Result<()> {
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
    let path = thread.path.as_ref().expect("persistent thread path");
    assert!(!path.exists());
    let _: codex_app_server_protocol::ThreadPersistResponse = app
        .request(|request_id| ClientRequest::ThreadPersist {
            request_id,
            params: codex_app_server_protocol::ThreadPersistParams {
                thread_id: thread.id.clone(),
            },
        })
        .await?;
    assert!(path.exists());
    let stored = std::fs::read_to_string(path)?;
    assert!(stored.contains(&thread.id));
    assert!(!stored.contains("user_message"));
    assert!(app.shutdown_gracefully().await?.success());
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let resumed: ThreadResumeResponse = app
        .request(|request_id| ClientRequest::ThreadResume {
            request_id,
            params: ThreadResumeParams {
                thread_id: thread.id.clone(),
                ..Default::default()
            },
        })
        .await?;
    assert_eq!(resumed.thread.id, thread.id);
    assert!(resumed.thread.turns.is_empty());
    assert!(
        server
            .received_requests()
            .await
            .expect("mock requests")
            .iter()
            .all(|request| !request.url.path().ends_with("/responses"))
    );
    assert!(app.shutdown_gracefully().await?.success());
    Ok(())
}
