use super::*;
use crate::ItemSortKey;
use crate::ListItemsParams;
use codex_protocol::protocol::ItemStartedEvent;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn incomplete_item_is_pageable_and_completion_never_moves_it_behind_a_cursor() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::new();
    create_paginated_thread(&store, thread_id).await;
    seed_paginated_thread_metadata(&store, thread_id).await;
    let first = agent_message("first", MessagePhase::Commentary);
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1"),
                RolloutItem::EventMsg(EventMsg::ItemStarted(ItemStartedEvent {
                    thread_id,
                    turn_id: "turn-1".to_string(),
                    item: first.clone(),
                    started_at_ms: 10,
                })),
                completed_item(
                    thread_id,
                    "turn-1",
                    agent_message("second", MessagePhase::FinalAnswer),
                ),
            ],
        })
        .await
        .expect("append start before later completed item");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist");
    store.flush_thread(thread_id).await.expect("flush");
    let params = ListItemsParams {
        thread_id,
        turn_id: Some("turn-1".to_string()),
        item_id: None,
        include_archived: true,
        cursor: None,
        page_size: 1,
        sort_direction: SortDirection::Asc,
        sort_key: ItemSortKey::CreatedAtOrdinal,
        after_updated_at_ordinal: None,
    };
    let first_page = store
        .list_items(params.clone())
        .await
        .expect("first page while streaming");
    assert_eq!(first_page.items.len(), 1);
    assert_eq!(first_page.items[0].item_id, "first");
    assert!(!first_page.items[0].is_complete);
    let start_position = first_page.items[0].started_at_ordinal;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![completed_item(thread_id, "turn-1", first)],
        })
        .await
        .expect("late completion");
    store
        .flush_thread(thread_id)
        .await
        .expect("flush completion");
    let next = store
        .list_items(ListItemsParams {
            cursor: first_page.next_cursor,
            ..params.clone()
        })
        .await
        .expect("continue cursor");
    assert_eq!(
        next.items
            .iter()
            .map(|item| item.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["second"]
    );
    let refreshed = store
        .list_items(params.clone())
        .await
        .expect("refresh first page");
    assert_eq!(refreshed.items[0].started_at_ordinal, start_position);
    assert!(refreshed.items[0].is_complete);
    let descending = store
        .list_items(ListItemsParams {
            sort_direction: SortDirection::Desc,
            page_size: 2,
            ..params.clone()
        })
        .await
        .expect("reverse order");
    assert_eq!(
        descending
            .items
            .iter()
            .map(|item| item.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["second", "first"]
    );
    let exact = store
        .list_items(ListItemsParams {
            item_id: Some("first".to_string()),
            ..params
        })
        .await
        .expect("live position lookup");
    assert_eq!(exact.items, refreshed.items);
}

#[tokio::test]
async fn fork_prefix_preserves_the_item_snapshot_before_later_completion_and_update() {
    let home = TempDir::new().expect("temp dir");
    let store = projection_store(home.path()).await;
    let source = ThreadId::new();
    create_paginated_thread(&store, source).await;
    seed_paginated_thread_metadata(&store, source).await;
    let initial = agent_message("item", MessagePhase::FinalAnswer);
    store
        .append_items(AppendThreadItemsParams {
            thread_id: source,
            items: vec![
                turn_started("turn"),
                RolloutItem::EventMsg(EventMsg::ItemStarted(ItemStartedEvent {
                    thread_id: source,
                    turn_id: "turn".to_string(),
                    item: initial.clone(),
                    started_at_ms: 10,
                })),
            ],
        })
        .await
        .expect("append start");
    store
        .persist_thread(source, PersistContext::Standard)
        .await
        .expect("persist");
    store.flush_thread(source).await.expect("flush");
    let source_path = store.live_rollout_path(source).await.expect("source path");
    let (_, start_end) = rollout_line_byte_offsets(source_path.as_path(), 2);
    let mut first_completion = initial.clone();
    if let TurnItem::AgentMessage(message) = &mut first_completion {
        message.content = vec![AgentMessageContent::Text {
            text: "First final body".to_string(),
        }];
    }
    store
        .append_items(AppendThreadItemsParams {
            thread_id: source,
            items: vec![completed_item(source, "turn", first_completion.clone())],
        })
        .await
        .expect("first completion");
    store
        .flush_thread(source)
        .await
        .expect("flush first completion");
    let (_, completed_end) = rollout_line_byte_offsets(source_path.as_path(), 3);
    let mut later = first_completion.clone();
    if let TurnItem::AgentMessage(message) = &mut later {
        message.content = vec![AgentMessageContent::Text {
            text: "Later update".to_string(),
        }];
    }
    store
        .append_items(AppendThreadItemsParams {
            thread_id: source,
            items: vec![completed_item(source, "turn", later)],
        })
        .await
        .expect("later completion update");
    store.flush_thread(source).await.expect("flush update");
    for (end_ordinal, end_bytes, expected_complete, expected_text) in [
        (3, start_end, false, "item"),
        (4, completed_end, true, "First final body"),
    ] {
        let child = ThreadId::new();
        create_paginated_subagent_thread(
            &store,
            child,
            Some(HistoryPosition {
                thread_id: source,
                end_ordinal_exclusive: end_ordinal,
                end_byte_offset: u64::try_from(end_bytes).expect("byte offset"),
            }),
            None,
        )
        .await;
        store
            .persist_thread(child, PersistContext::Standard)
            .await
            .expect("persist child");
        seed_paginated_thread_metadata(&store, child).await;
        let page = store
            .list_items(ListItemsParams {
                thread_id: child,
                turn_id: Some("turn".to_string()),
                item_id: None,
                include_archived: true,
                cursor: None,
                page_size: 10,
                sort_direction: SortDirection::Asc,
                sort_key: ItemSortKey::CreatedAtOrdinal,
                after_updated_at_ordinal: None,
            })
            .await
            .expect("read fork prefix");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].is_complete, expected_complete);
        let item: serde_json::Value =
            serde_json::from_slice(&page.items[0].item_json).expect("item JSON");
        assert_eq!(item["text"], expected_text);
        assert_eq!(page.items[0].started_at_ordinal, 2);
        let summaries = store
            .list_turns(ListTurnsParams {
                thread_id: child,
                include_archived: true,
                cursor: None,
                page_size: 10,
                sort_direction: SortDirection::Asc,
                items_view: StoredTurnItemsView::Summary,
            })
            .await
            .expect("read summary inside fork prefix");
        if expected_complete {
            let item: serde_json::Value =
                serde_json::from_slice(&summaries.turns[0].items[0].item_json)
                    .expect("summary JSON");
            assert_eq!(item["text"], expected_text);
        } else {
            assert!(summaries.turns[0].items.is_empty());
        }
    }
}
