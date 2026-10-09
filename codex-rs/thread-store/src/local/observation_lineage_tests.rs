use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn observation_keeps_lineage_and_lifecycle_before_enrichment() {
    let home = TempDir::new().unwrap();
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    seed_paginated_thread_metadata(&store, thread_id).await;
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .unwrap();
    let message = agent_message("answer", MessagePhase::FinalAnswer);
    let completed = |started_at_ms| {
        RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
            thread_id,
            turn_id: "turn-1".into(),
            item: message.clone(),
            started_at_ms,
            completed_at_ms: 2000,
        }))
    };
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1"),
                completed(/*started_at_ms*/ None),
                turn_completed("turn-1"),
            ],
        })
        .await
        .unwrap();
    let checkpoint = store.observation_cursors(thread_id).await.unwrap();
    let turns = ListTurnsParams {
        thread_id,
        include_archived: false,
        cursor: checkpoint.turns,
        page_size: 10,
        sort_direction: SortDirection::Desc,
        items_view: StoredTurnItemsView::Summary,
    };
    let items = crate::ListItemsParams {
        thread_id,
        include_archived: false,
        item_id: None,
        page_size: 10,
        sort_direction: SortDirection::Desc,
        turn_id: None,
        after_updated_at_ordinal: None,
        position: checkpoint.items.map(crate::ListItemsPosition::Cursor),
        sort_key: crate::ItemSortKey::CreatedAtOrdinal,
    };
    let before_turns = store.list_turns(turns.clone()).await.unwrap();
    let before_items = store.list_items(items.clone()).await.unwrap();
    assert_eq!(before_turns.turns[0].root_turn_id, None);
    assert_eq!(before_items.items[0].started_at_ms, None);
    let mut ended = turn_completed("turn-1");
    if let RolloutItem::EventMsg(EventMsg::TurnComplete(event)) = &mut ended {
        event.root_turn_id = Some("root-lineage".into());
    }
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started_with_root("turn-1", "root-lineage"),
                completed(Some(1000)),
                ended,
            ],
        })
        .await
        .unwrap();
    let latest_turns = store
        .list_turns(ListTurnsParams {
            cursor: None,
            ..turns.clone()
        })
        .await
        .unwrap();
    let latest_items = store
        .list_items(crate::ListItemsParams {
            position: None,
            ..items.clone()
        })
        .await
        .unwrap();
    assert_eq!(
        latest_turns.turns[0].root_turn_id.as_deref(),
        Some("root-lineage")
    );
    assert_eq!(latest_items.items[0].started_at_ms, Some(1000));
    for rebuild in [false, true] {
        if rebuild {
            super::super::super::thread_history::delete_thread(&store, thread_id)
                .await
                .unwrap();
        }
        assert_eq!(
            serde_json::to_value(store.list_turns(turns.clone()).await.unwrap()).unwrap(),
            serde_json::to_value(&before_turns).unwrap()
        );
        assert_eq!(
            serde_json::to_value(store.list_items(items.clone()).await.unwrap()).unwrap(),
            serde_json::to_value(&before_items).unwrap()
        );
    }
    let command = CommandExecutionItem {
        sandbox_type: None,
        model_context: None,
        id: "command-aggregate".into(),
        plugin_id: None,
        script_path: None,
        process_id: None,
        command: vec!["synthetic".into()],
        cwd: home.path().abs().into(),
        parsed_cmd: Vec::new(),
        source: ExecCommandSource::Agent,
        interaction_input: None,
        status: CommandExecutionStatus::Completed,
        aggregated_output: Some("aggregate without attributed streams".into()),
        exit_code: Some(0),
        duration: None,
    };
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started_with_root("turn-command", "root-command"),
                completed_item(
                    thread_id,
                    "turn-command",
                    TurnItem::UserMessage(UserMessageItem {
                        id: "user-command".into(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                completed_item(
                    thread_id,
                    "turn-command",
                    agent_message("native-partial-command", MessagePhase::PartialAnswer),
                ),
                completed_item(
                    thread_id,
                    "turn-command",
                    TurnItem::CommandExecution(command),
                ),
                completed_item(
                    thread_id,
                    "turn-command",
                    agent_message("native-final-command", MessagePhase::FinalAnswer),
                ),
                turn_completed("turn-command"),
            ],
        })
        .await
        .unwrap();
    let current = store
        .list_items(crate::ListItemsParams {
            position: None,
            ..items
        })
        .await
        .unwrap();
    let command_json = serde_json::from_slice::<serde_json::Value>(
        &current
            .items
            .iter()
            .find(|item| item.item_id == "command-aggregate")
            .unwrap()
            .item_json,
    )
    .unwrap();
    assert_eq!(
        command_json["aggregatedOutput"],
        "aggregate without attributed streams"
    );
    assert!(
        command_json
            .get("stdout")
            .is_none_or(serde_json::Value::is_null)
    );
    assert!(
        command_json
            .get("stderr")
            .is_none_or(serde_json::Value::is_null)
    );
    if let Some(destination) = std::env::var_os("CODEX_OBSERVATION_LINEAGE_FIXTURE") {
        let rollout_path = store.live_rollout_path(thread_id).await.unwrap();
        let fixture = serde_json::json!({
            "threadId": thread_id,
            "rollout": fs::read_to_string(rollout_path).unwrap(),
            "frozenTurns": before_turns,
            "frozenItems": before_items,
            "currentItems": current,
            "command": command_json,
            "origin": "actual native paginated append/rebuild test; synthetic typed payloads",
        });
        fs::write(destination, serde_json::to_vec_pretty(&fixture).unwrap()).unwrap();
    }
    store.shutdown_thread(thread_id).await.unwrap();
}
