// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use std::path::Path;

use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_fake_paginated_rollout;
use app_test_support::create_fake_rollout;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::rollout_path;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_protocol::ResponseItemId;
use codex_protocol::ThreadId;
use codex_protocol::items::AgentMessageContent;
use codex_protocol::items::AgentMessageItem;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::ContentItem;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::ItemStartedEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_rollout::CompactedItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::append_rollout_item_to_path;
use pretty_assertions::assert_eq;
use serde_json::Value;
use tempfile::TempDir;
use test_case::test_case;
use tokio::time::timeout;

#[cfg(windows)]
const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(25);
#[cfg(not(windows))]
const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

const TURN_ID: &str = "source-turn";

#[derive(Clone, Copy)]
enum Source {
    Running,
    Finished,
    CompactedRunning,
}

#[test_case(Source::Running, Some("m1"), &["user-1", "m1"], Some("m2"); "running turn")]
#[test_case(Source::Finished, Some("m1"), &["user-1", "m1"], Some("m2"); "finished turn")]
#[test_case(Source::CompactedRunning, Some("m2"), &["user-1", "m1", "m2"], Some("m3"); "running turn behind compaction")]
#[test_case(Source::CompactedRunning, None, &["user-1", "m1", "m2", "m3"], None; "latest behind compaction")]
#[tokio::test]
async fn thread_fork_cuts_the_source_turn_as_interrupted(
    source: Source,
    last_item_id: Option<&str>,
    expected_item_ids: &[&str],
    omitted_message: Option<&str>,
) -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let source_thread_id = create_fake_paginated_rollout(
        codex_home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "Saved user message",
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let source_path = rollout_path(codex_home.path(), "2025-01-05T12-00-00", &source_thread_id);
    let source_id = ThreadId::from_string(source_thread_id.as_str())?;
    let mut items = vec![turn_started(), user_input(source_id, "user-1")];
    items.extend(assistant_message(source_id, "m1"));
    if matches!(source, Source::CompactedRunning) {
        items.push(compacted());
    }
    items.extend(assistant_message(source_id, "m2"));
    if matches!(source, Source::CompactedRunning) {
        items.extend(assistant_message(source_id, "m3"));
    }
    if matches!(source, Source::Finished) {
        items.push(turn_completed());
    }
    append_all(source_path.as_path(), &items).await?;
    let source_before = std::fs::read(source_path.as_path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let ThreadForkResponse { thread: fork, .. } = mcp
        .request(|request_id| ClientRequest::ThreadFork {
            request_id,
            params: ThreadForkParams {
                thread_id: source_thread_id.clone(),
                last_turn_id: last_item_id.map(|_| TURN_ID.to_string()),
                last_item_id: last_item_id.map(str::to_string),
                ..Default::default()
            },
        })
        .await?;

    assert_eq!(std::fs::read(source_path.as_path())?, source_before);
    let fork_path = fork.path.expect("fork rollout path");
    let fork_rollout = std::fs::read_to_string(fork_path.as_path())?
        .lines()
        .map(codex_rollout::parse_rollout_line)
        .collect::<Result<Vec<_>, _>>()?;
    assert!(matches!(
        fork_rollout.as_slice(),
        [
            RolloutLine { item: RolloutItem::SessionMeta(_), .. },
            RolloutLine {
                item: RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(_)),
                ..
            },
            RolloutLine { item: RolloutItem::ResponseItem(_), .. },
            RolloutLine {
                item: RolloutItem::EventMsg(EventMsg::TurnAborted(aborted)),
                ..
            },
        ] if aborted.turn_id.as_deref() == Some(TURN_ID)
    ));

    let ThreadTurnsListResponse { data: turns, .. } = mcp
        .request(|request_id| ClientRequest::ThreadTurnsList {
            request_id,
            params: ThreadTurnsListParams {
                thread_id: fork.id.clone(),
                cursor: None,
                limit: None,
                sort_direction: None,
                items_view: Some(TurnItemsView::Full),
            },
        })
        .await?;
    let turn_summaries = turns
        .iter()
        .map(|turn| {
            (
                turn.id.as_str(),
                turn.status.clone(),
                turn.items
                    .iter()
                    .map(codex_app_server_protocol::ThreadItem::id)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        turn_summaries,
        vec![(TURN_ID, TurnStatus::Interrupted, expected_item_ids.to_vec())]
    );

    let _: TurnStartResponse = mcp
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: fork.id,
                input: vec![UserInput::Text {
                    text: "Continue in the fork".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;
    let requests = server.received_requests().await.expect("response requests");
    let input = requests
        .iter()
        .rev()
        .find(|request| request.url.path().ends_with("/responses"))
        .expect("fork model request")
        .body_json::<Value>()?["input"]
        .clone();
    let serialized_input = serde_json::to_string(&input)?;
    let kept_message = last_item_id.unwrap_or("m3");
    assert!(serialized_input.contains(&format!("{kept_message} answer")));
    if let Some(omitted_message) = omitted_message {
        assert!(!serialized_input.contains(&format!("{omitted_message} answer")));
    }
    assert!(serialized_input.contains("<turn_aborted>"));
    Ok(())
}

#[tokio::test]
async fn thread_fork_refuses_item_boundaries_it_cannot_honor() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let paginated_thread_id = create_fake_paginated_rollout(
        codex_home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "Saved user message",
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let paginated_path = rollout_path(
        codex_home.path(),
        "2025-01-05T12-00-00",
        &paginated_thread_id,
    );
    let paginated_id = ThreadId::from_string(paginated_thread_id.as_str())?;
    let mut items = vec![turn_started(), user_input(paginated_id, "user-1")];
    items.extend(assistant_message(paginated_id, "m1"));
    append_all(paginated_path.as_path(), &items).await?;
    let legacy_thread_id = create_fake_rollout(
        codex_home.path(),
        "2025-01-06T12-00-00",
        "2025-01-06T12:00:00Z",
        "Legacy user message",
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    for (thread_id, last_turn_id, last_item_id, expected) in [
        (
            paginated_thread_id.as_str(),
            None,
            "m1",
            "`lastItemId` requires `lastTurnId`",
        ),
        (
            legacy_thread_id.as_str(),
            Some(TURN_ID),
            "m1",
            "`lastItemId` requires a paginated thread",
        ),
        (
            paginated_thread_id.as_str(),
            Some(TURN_ID),
            "user-1",
            "lastItemId 'user-1' is not an assistant message",
        ),
    ] {
        let request_id = mcp
            .send_thread_fork_request(ThreadForkParams {
                thread_id: thread_id.to_string(),
                last_turn_id: last_turn_id.map(str::to_string),
                last_item_id: Some(last_item_id.to_string()),
                ..Default::default()
            })
            .await?;
        let error = timeout(
            DEFAULT_READ_TIMEOUT,
            mcp.read_stream_until_error_message(RequestId::Integer(request_id)),
        )
        .await??;
        assert_eq!(error.error.message, expected);
    }
    Ok(())
}

async fn append_all(path: &Path, items: &[RolloutItem]) -> Result<()> {
    for item in items {
        append_rollout_item_to_path(path, item).await?;
    }
    Ok(())
}

fn turn_started() -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_attribution: None,
        turn_id: TURN_ID.to_string(),
        root_turn_id: None,
        trace_id: None,
        started_at: Some(10),
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }))
}

fn turn_completed() -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        root_turn_id: None,
        turn_id: TURN_ID.to_string(),
        last_agent_message: None,
        error: None,
        started_at: Some(10),
        completed_at: Some(20),
        duration_ms: Some(10_000),
        time_to_first_token_ms: None,
    }))
}

fn user_input(thread_id: ThreadId, id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id,
        turn_id: TURN_ID.to_string(),
        item: TurnItem::UserMessage(UserMessageItem {
            id: id.to_string(),
            client_id: None,
            content: vec![codex_protocol::user_input::UserInput::Text {
                text: format!("{id} question"),
                text_elements: Vec::new(),
            }],
        }),
        started_at_ms: Some(0),
        completed_at_ms: 1,
    }))
}

fn assistant_message(thread_id: ThreadId, id: &str) -> [RolloutItem; 3] {
    let item = TurnItem::AgentMessage(AgentMessageItem {
        id: id.to_string(),
        content: vec![AgentMessageContent::Text {
            text: format!("{id} answer"),
        }],
        phase: Some(MessagePhase::Commentary),
        memory_citation: None,
        delivery: None,
        questions: None,
    });
    [
        RolloutItem::EventMsg(EventMsg::ItemStarted(ItemStartedEvent {
            thread_id,
            turn_id: TURN_ID.to_string(),
            item: item.clone(),
            started_at_ms: 0,
        })),
        RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
            thread_id,
            turn_id: TURN_ID.to_string(),
            item,
            started_at_ms: Some(0),
            completed_at_ms: 1,
        })),
        RolloutItem::ResponseItem(
            ResponseItem::Message {
                status: None,
                encrypted_content: None,
                id: Some(ResponseItemId::from_server(id.to_string())),
                role: "assistant".to_string(),
                content: vec![ContentItem::OutputText {
                    text: format!("{id} answer"),
                    annotations: None,
                    logprobs: None,
                }],
                phase: Some(MessagePhase::Commentary),
                internal_chat_message_metadata_passthrough: None,
            }
            .into(),
        ),
    ]
}

fn compacted() -> RolloutItem {
    let user = ResponseItem::Message {
        status: None,
        encrypted_content: None,
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::InputText {
            text: "user-1 question".to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    };
    RolloutItem::Compacted(Box::new(CompactedItem {
        message: "summary".to_string(),
        replacement_history: Some(vec![user.into()]),
        retained_context: None,
        guardian_history: None,
        mcp_resource_origins: None,
        window_number: Some(1),
        first_window_id: None,
        previous_window_id: None,
        window_id: None,
        compaction_response_id: None,
        latest_token_usage_record: None,
        resume_metadata: None,
    }))
}
