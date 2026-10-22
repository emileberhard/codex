// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
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
            items: vec![turn_started("turn-1"), completed(/*started_at_ms*/ None)],
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
        item_ids: None,
        thread_id,
        include_archived: false,
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

#[tokio::test]
async fn observation_retains_known_lifecycle_after_unknown_completion() {
    assert_effective_item_version(LifecycleUpdate::UnknownCompletion).await;
}

#[tokio::test]
async fn observation_retains_completed_item_after_late_incomplete() {
    assert_effective_item_version(LifecycleUpdate::LateIncomplete).await;
}

enum LifecycleUpdate {
    UnknownCompletion,
    LateIncomplete,
}

async fn assert_effective_item_version(update: LifecycleUpdate) {
    let home = TempDir::new().unwrap();
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    seed_paginated_thread_metadata(&store, thread_id).await;
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .unwrap();
    let completed = |started_at_ms, completed_at_ms| {
        RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
            thread_id,
            turn_id: "turn-lifecycle".into(),
            item: agent_message("known-lifecycle", MessagePhase::FinalAnswer),
            started_at_ms,
            completed_at_ms,
        }))
    };
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started_with_root("turn-lifecycle", "root-lifecycle"),
                completed(Some(1000), 2000),
            ],
        })
        .await
        .unwrap();
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![match update {
                LifecycleUpdate::UnknownCompletion => completed(/*started_at_ms*/ None, 3000),
                LifecycleUpdate::LateIncomplete => {
                    let mut item = agent_message("known-lifecycle", MessagePhase::FinalAnswer);
                    if let TurnItem::AgentMessage(message) = &mut item {
                        message.content = vec![AgentMessageContent::Text {
                            text: "late incomplete payload".into(),
                        }];
                    }
                    RolloutItem::EventMsg(EventMsg::ItemStarted(
                        codex_protocol::protocol::ItemStartedEvent {
                            thread_id,
                            turn_id: "turn-lifecycle".into(),
                            item,
                            started_at_ms: 1500,
                        },
                    ))
                }
            }],
        })
        .await
        .unwrap();
    let checkpoint = store.observation_cursors(thread_id).await.unwrap();
    let items = crate::ListItemsParams {
        item_ids: None,
        thread_id,
        include_archived: false,
        page_size: 10,
        sort_direction: SortDirection::Desc,
        turn_id: None,
        after_updated_at_ordinal: None,
        position: None,
        sort_key: crate::ItemSortKey::CreatedAtOrdinal,
    };
    let live = store.list_items(items.clone()).await.unwrap();
    assert_eq!(live.items.len(), 1);
    assert_eq!(live.items[0].started_at_ms, Some(1000));
    assert_eq!(live.items[0].completed_at_ms, Some(2000));
    let frozen_items = crate::ListItemsParams {
        position: checkpoint.items.map(crate::ListItemsPosition::Cursor),
        ..items
    };
    for rebuild in [false, true] {
        if rebuild {
            super::super::super::thread_history::delete_thread(&store, thread_id)
                .await
                .unwrap();
        }
        let frozen = store.list_items(frozen_items.clone()).await.unwrap();
        assert_eq!(frozen.items.len(), 1);
        assert_eq!(frozen.items[0].started_at_ms, live.items[0].started_at_ms);
        assert_eq!(
            frozen.items[0].completed_at_ms,
            live.items[0].completed_at_ms
        );
        assert_eq!(frozen.items[0].item_json, live.items[0].item_json);
        assert_eq!(frozen.items[0].is_complete, live.items[0].is_complete);
    }
    store.shutdown_thread(thread_id).await.unwrap();
}

#[tokio::test]
async fn observation_migrates_e520_lifecycle_versions_from_canonical_history() {
    let home = TempDir::new().unwrap();
    let store = projection_store(home.path()).await;
    let thread_id = ThreadId::default();
    create_paginated_thread(&store, thread_id).await;
    seed_paginated_thread_metadata(&store, thread_id).await;
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .unwrap();
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-migration"),
                RolloutItem::EventMsg(EventMsg::ItemStarted(
                    codex_protocol::protocol::ItemStartedEvent {
                        thread_id,
                        turn_id: "turn-migration".into(),
                        item: agent_message("known-source-start", MessagePhase::Commentary),
                        started_at_ms: 1000,
                    },
                )),
                RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                    thread_id,
                    turn_id: "turn-migration".into(),
                    item: agent_message("unknown-source-start", MessagePhase::FinalAnswer),
                    started_at_ms: None,
                    completed_at_ms: 2000,
                })),
            ],
        })
        .await
        .unwrap();
    let checkpoint = store.observation_cursors(thread_id).await.unwrap();
    let items = crate::ListItemsParams {
        item_ids: None,
        thread_id,
        include_archived: false,
        page_size: 10,
        sort_direction: SortDirection::Desc,
        turn_id: None,
        after_updated_at_ordinal: None,
        position: checkpoint.items.map(crate::ListItemsPosition::Cursor),
        sort_key: crate::ItemSortKey::CreatedAtOrdinal,
    };
    let before = store.list_items(items.clone()).await.unwrap();
    assert_eq!(before.items.len(), 2);
    assert_eq!(
        before
            .items
            .iter()
            .find(|item| item.item_id == "known-source-start")
            .unwrap()
            .started_at_ms,
        Some(1000)
    );
    assert_eq!(
        before
            .items
            .iter()
            .find(|item| item.item_id == "unknown-source-start")
            .unwrap()
            .started_at_ms,
        None
    );
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                    thread_id,
                    turn_id: "turn-migration".into(),
                    item: agent_message("known-source-start", MessagePhase::FinalAnswer),
                    started_at_ms: None,
                    completed_at_ms: 3000,
                })),
                RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                    thread_id,
                    turn_id: "turn-migration".into(),
                    item: agent_message("unknown-source-start", MessagePhase::FinalAnswer),
                    started_at_ms: Some(1500),
                    completed_at_ms: 3000,
                })),
                turn_completed("turn-migration"),
            ],
        })
        .await
        .unwrap();
    let rollout = store.live_rollout_path(thread_id).await.unwrap();
    let canonical = fs::read(&rollout).unwrap();
    store.shutdown_thread(thread_id).await.unwrap();
    drop(store);
    let pool = codex_state::open_thread_history_db(&codex_state::SqliteConfig::new_for_testing(
        home.path().abs(),
    ))
    .await
    .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    sqlx::raw_sql(
        "DROP TRIGGER thread_observation_turn_insert; DROP TRIGGER thread_observation_turn_update;",
    )
    .execute(&mut *transaction)
    .await
    .unwrap();
    let old = include_str!(
        "../../../state/thread_history_migrations/1000002_thread_observation_versions.sql"
    );
    sqlx::raw_sql(&old[old.find("CREATE TRIGGER").unwrap()..])
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::raw_sql("ALTER TABLE thread_item_versions DROP COLUMN started_at_ms; ALTER TABLE thread_turn_versions DROP COLUMN root_turn_id; ALTER TABLE thread_turns DROP COLUMN root_turn_id; DELETE FROM _sqlx_migrations WHERE version IN (8, 1000004);").execute(&mut *transaction).await.unwrap();
    transaction.commit().await.unwrap();
    pool.close().await;
    let migrated = projection_store(home.path()).await;
    let after = migrated.list_items(items).await.unwrap();
    assert_eq!(
        serde_json::to_value(after).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    assert_eq!(fs::read(rollout).unwrap(), canonical);
}
