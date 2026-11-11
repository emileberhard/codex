// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use std::fs;
use std::path::Path;

use codex_protocol::ResponseItemId;
use codex_protocol::ThreadId;
use codex_protocol::items::AgentMessageContent;
use codex_protocol::items::AgentMessageItem;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::ItemStartedEvent;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_rollout::CompactedItem;
use codex_rollout::RolloutItem;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::super::LocalThreadStore;
use super::super::test_support::test_config;
use crate::AppendThreadItemsParams;
use crate::CreateThreadParams;
use crate::ForkBoundary;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::ThreadPersistenceMetadata;
use crate::ThreadStore;
use crate::ThreadStoreError;

#[tokio::test]
async fn through_item_forks_finished_and_running_turns_at_the_message_record() {
    let home = TempDir::new().expect("temp dir");
    let (store, thread_id) = paginated_source(home.path()).await;
    let mut items = vec![turn_started("turn-1"), user_message("first question")];
    items.extend(assistant_message(thread_id, "turn-1", "m1"));
    items.extend(assistant_message(thread_id, "turn-1", "m2"));
    items.push(turn_completed("turn-1"));
    items.extend([turn_started("turn-2"), user_message("second question")]);
    items.extend(assistant_message(thread_id, "turn-2", "m3"));
    items.extend(assistant_message(thread_id, "turn-2", "m4"));
    append(&store, thread_id, items).await;
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rollout path");

    for (turn_id, item_id, kept, omitted) in [
        ("turn-1", "m1", vec!["m1"], vec!["m2", "m3", "m4"]),
        ("turn-2", "m3", vec!["m1", "m2", "m3"], vec!["m4"]),
    ] {
        let fork = prepare(&store, thread_id, through(turn_id, item_id))
            .await
            .expect("prepare item fork");
        let (record_ordinal, record_end) = message_record_line(&rollout_path, item_id);
        assert_eq!(
            fork.history_base,
            Some(HistoryPosition {
                thread_id,
                end_ordinal_exclusive: record_ordinal + 1,
                end_byte_offset: record_end,
            })
        );
        let messages = assistant_record_ids(&fork.model_context);
        assert_eq!(messages, kept);
        assert!(omitted.iter().all(|id| !messages.contains(id)));
        assert_eq!(turn_starts(&fork.model_context, turn_id), 1);
    }
}

#[tokio::test]
async fn fork_keeps_the_open_turn_start_behind_a_compaction() {
    let home = TempDir::new().expect("temp dir");
    let (store, thread_id) = paginated_source(home.path()).await;
    let mut items = vec![turn_started("turn-1"), user_message("question")];
    items.extend(assistant_message(thread_id, "turn-1", "m1"));
    items.push(compacted());
    items.extend(assistant_message(thread_id, "turn-1", "m2"));
    items.extend(assistant_message(thread_id, "turn-1", "m3"));
    append(&store, thread_id, items).await;

    for (boundary, kept) in [
        (ForkBoundary::Latest, vec!["m2", "m3"]),
        (through("turn-1", "m2"), vec!["m2"]),
    ] {
        let fork = prepare(&store, thread_id, boundary)
            .await
            .expect("prepare fork behind a compaction");
        let context = fork.model_context.as_slice();
        assert!(matches!(context.first(), Some(RolloutItem::SessionMeta(_))));
        assert!(matches!(
            context.get(1),
            Some(RolloutItem::EventMsg(EventMsg::TurnStarted(event))) if event.turn_id == "turn-1"
        ));
        assert!(matches!(context.get(2), Some(RolloutItem::Compacted(_))));
        assert_eq!(turn_starts(context, "turn-1"), 1);
        assert_eq!(assistant_record_ids(context), kept);
    }
}

#[tokio::test]
async fn through_item_refuses_boundaries_it_cannot_honor_exactly() {
    let home = TempDir::new().expect("temp dir");
    let (store, thread_id) = paginated_source(home.path()).await;
    let mut items = vec![turn_started("turn-1"), user_message("question")];
    items.extend(assistant_message(thread_id, "turn-1", "m1"));
    let user_item = TurnItem::UserMessage(UserMessageItem {
        id: "user-item".to_string(),
        client_id: None,
        content: Vec::new(),
    });
    items.push(item_started(thread_id, "turn-1", user_item.clone()));
    items.push(item_completed(thread_id, "turn-1", user_item));
    items.push(item_completed(
        thread_id,
        "turn-1",
        agent_message("m-orphan"),
    ));
    items.push(user_message("not the assistant record"));
    items.push(item_completed(
        thread_id,
        "turn-1",
        agent_message("m-idless"),
    ));
    items.push(assistant_record(/*id*/ None, "m-idless"));
    items.push(item_completed(
        thread_id,
        "turn-1",
        agent_message("m-other-id"),
    ));
    items.push(assistant_record(Some("m-different"), "m-other-id"));
    items.push(item_started(
        thread_id,
        "turn-1",
        agent_message("m-pending"),
    ));
    append(&store, thread_id, items).await;

    let no_adjacent_record = |id: &str| {
        format!("lastItemId '{id}' has no model-visible message directly after its completion")
    };
    for (turn_id, item_id, expected) in [
        (
            "turn-1",
            "missing",
            "lastItemId 'missing' was not found in turn 'turn-1'".to_string(),
        ),
        (
            "turn-2",
            "m1",
            "lastItemId 'm1' was not found in turn 'turn-2'".to_string(),
        ),
        (
            "turn-1",
            "user-item",
            "lastItemId 'user-item' is not an assistant message".to_string(),
        ),
        (
            "turn-1",
            "m-pending",
            "lastItemId 'm-pending' has not completed".to_string(),
        ),
        ("turn-1", "m-orphan", no_adjacent_record("m-orphan")),
        ("turn-1", "m-idless", no_adjacent_record("m-idless")),
        ("turn-1", "m-other-id", no_adjacent_record("m-other-id")),
    ] {
        let error = prepare(&store, thread_id, through(turn_id, item_id))
            .await
            .expect_err("refuse an inexact item boundary");
        assert!(
            matches!(&error, ThreadStoreError::InvalidRequest { message } if *message == expected),
            "{item_id}: {error:?}"
        );
    }
}

async fn paginated_source(codex_home: &Path) -> (LocalThreadStore, ThreadId) {
    let config = test_config(codex_home);
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state database");
    let store = LocalThreadStore::new(config, Some(state_db));
    let thread_id = ThreadId::default();
    store
        .create_thread(CreateThreadParams {
            attached_thread: false,
            creator_user_id: None,
            creator_account_id: None,
            session_id: thread_id.into(),
            thread_id,
            extra_config: None,
            forked_from_id: None,
            parent_thread_id: None,
            source: SessionSource::Exec,
            thread_source: None,
            originator: "test_originator".to_string(),
            base_instructions: BaseInstructions::default(),
            dynamic_tools: Vec::new(),
            selected_capability_roots: Vec::new(),
            multi_agent_version: None,
            history_mode: ThreadHistoryMode::Paginated,
            history_base: None,
            subagent_history_start_ordinal: None,
            initial_window_id: "window-1".to_string(),
            runtime_workspace_roots: None,
            metadata: ThreadPersistenceMetadata {
                cwd: Some(std::env::current_dir().expect("cwd")),
                model_provider: "test-provider".to_string(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        })
        .await
        .expect("create paginated thread");
    (store, thread_id)
}

async fn append(store: &LocalThreadStore, thread_id: ThreadId, items: Vec<RolloutItem>) {
    store
        .append_items(AppendThreadItemsParams { thread_id, items })
        .await
        .expect("append source history");
}

async fn prepare(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    boundary: ForkBoundary,
) -> Result<PreparedFork, ThreadStoreError> {
    store
        .prepare_fork(PrepareForkParams {
            thread_id,
            boundary,
        })
        .await
}

fn through(turn_id: &str, item_id: &str) -> ForkBoundary {
    ForkBoundary::ThroughItem {
        turn_id: turn_id.to_string(),
        item_id: item_id.to_string(),
    }
}

fn assistant_message(thread_id: ThreadId, turn_id: &str, id: &str) -> [RolloutItem; 3] {
    [
        item_started(thread_id, turn_id, agent_message(id)),
        item_completed(thread_id, turn_id, agent_message(id)),
        assistant_record(Some(id), id),
    ]
}

fn agent_message(id: &str) -> TurnItem {
    TurnItem::AgentMessage(AgentMessageItem {
        id: id.to_string(),
        content: vec![AgentMessageContent::Text {
            text: id.to_string(),
        }],
        phase: Some(MessagePhase::Commentary),
        memory_citation: None,
        delivery: None,
        questions: None,
    })
}

fn assistant_record(id: Option<&str>, text: &str) -> RolloutItem {
    RolloutItem::ResponseItem(
        ResponseItem::Message {
            status: None,
            encrypted_content: None,
            id: id.map(|id| ResponseItemId::from_server(id.to_string())),
            role: "assistant".to_string(),
            content: vec![ContentItem::OutputText {
                text: text.to_string(),
                annotations: None,
                logprobs: None,
            }],
            phase: Some(MessagePhase::Commentary),
            internal_chat_message_metadata_passthrough: None,
        }
        .into(),
    )
}

fn user_message(text: &str) -> RolloutItem {
    RolloutItem::ResponseItem(
        ResponseItem::Message {
            status: None,
            encrypted_content: None,
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: text.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }
        .into(),
    )
}

fn item_started(thread_id: ThreadId, turn_id: &str, item: TurnItem) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemStarted(ItemStartedEvent {
        thread_id,
        turn_id: turn_id.to_string(),
        item,
        started_at_ms: 0,
    }))
}

fn item_completed(thread_id: ThreadId, turn_id: &str, item: TurnItem) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id,
        turn_id: turn_id.to_string(),
        item,
        started_at_ms: Some(0),
        completed_at_ms: 1,
    }))
}

fn turn_started(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_attribution: None,
        turn_id: turn_id.to_string(),
        root_turn_id: None,
        trace_id: None,
        started_at: Some(10),
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }))
}

fn turn_completed(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        root_turn_id: None,
        turn_id: turn_id.to_string(),
        last_agent_message: None,
        error: None,
        started_at: Some(10),
        completed_at: Some(20),
        duration_ms: Some(10_000),
        time_to_first_token_ms: None,
    }))
}

fn compacted() -> RolloutItem {
    RolloutItem::Compacted(Box::new(CompactedItem {
        message: "summary".to_string(),
        replacement_history: Some(vec![match user_message("question") {
            RolloutItem::ResponseItem(item) => item,
            _ => unreachable!("user_message builds a response item"),
        }]),
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

fn assistant_record_ids(items: &[RolloutItem]) -> Vec<&str> {
    items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::ResponseItem(response_item) => match &response_item.item {
                ResponseItem::Message { role, id, .. } if role == "assistant" => {
                    id.as_ref().map(ResponseItemId::as_str)
                }
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn turn_starts(items: &[RolloutItem], turn_id: &str) -> usize {
    items
        .iter()
        .filter(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::TurnStarted(event)) if event.turn_id == turn_id
            )
        })
        .count()
}

fn message_record_line(path: &Path, item_id: &str) -> (u64, u64) {
    let bytes = fs::read(path).expect("read rollout");
    let mut end = 0;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        end += u64::try_from(line.len()).expect("line length fits u64");
        let Ok(parsed) = codex_rollout::parse_rollout_line_bytes(line) else {
            continue;
        };
        if let RolloutItem::ResponseItem(response_item) = &parsed.item
            && let ResponseItem::Message { id: Some(id), .. } = &response_item.item
            && id.as_str() == item_id
        {
            return (parsed.ordinal.expect("paginated ordinal"), end);
        }
    }
    panic!("missing message record {item_id}");
}
