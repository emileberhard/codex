//! Attach the persisted first position to canonical item lifecycle notifications.
use codex_app_server_protocol::ServerNotification;
use codex_protocol::ThreadId;
use codex_thread_store::ItemSortKey;
use codex_thread_store::ListItemsParams;
use codex_thread_store::SortDirection;
use codex_thread_store::ThreadStore;

pub(crate) async fn attach(
    notification: &mut ServerNotification,
    store: Option<&dyn ThreadStore>,
    thread_id: ThreadId,
) {
    let Some(store) = store else {
        return;
    };
    let (turn_id, item_id) = match notification {
        ServerNotification::ItemStarted(note) => (note.turn_id.clone(), note.item.id().to_string()),
        ServerNotification::ItemCompleted(note) => {
            (note.turn_id.clone(), note.item.id().to_string())
        }
        _ => return,
    };
    let page = store
        .list_items(ListItemsParams {
            thread_id,
            turn_id: Some(turn_id),
            item_ids: Some(vec![item_id.clone()]),
            include_archived: true,
            position: None,
            page_size: 1,
            sort_direction: SortDirection::Asc,
            sort_key: ItemSortKey::CreatedAtOrdinal,
            after_updated_at_ordinal: None,
        })
        .await;
    match page {
        Ok(page) => {
            if let Some(item) = page.items.first().filter(|item| item.item_id == item_id) {
                match notification {
                    ServerNotification::ItemStarted(note) => {
                        note.started_at_ordinal = Some(item.started_at_ordinal)
                    }
                    ServerNotification::ItemCompleted(note) => {
                        note.started_at_ordinal = Some(item.started_at_ordinal)
                    }
                    _ => unreachable!(),
                }
            }
        }
        Err(codex_thread_store::ThreadStoreError::Unsupported { .. }) => {}
        Err(error) => {
            tracing::warn!(%thread_id, %item_id, %error, "item source position unavailable")
        }
    }
}
