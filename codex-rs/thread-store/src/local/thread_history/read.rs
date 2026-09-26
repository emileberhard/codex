use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;
use serde::Deserialize;
use serde::Serialize;
use sqlx::Row;
use tracing::error;

use super::super::LocalThreadStore;
use super::super::rollout_lineage::RolloutLineage;
use super::segment_paging::page_item_rows;
use super::segment_paging::page_turn_rows;
use super::segment_paging::validate_page_size;
use super::sqlite_integer;
use super::turn_lookup::find_source_turn;
use crate::ItemPage;
use crate::ListItemsParams;
use crate::ListTurnsParams;
use crate::StoredThreadItem;
use crate::StoredTurn;
use crate::StoredTurnError;
use crate::StoredTurnItemsView;
use crate::StoredTurnStatus;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::TurnPage;

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;

#[derive(Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub(super) enum CursorScope {
    Turns,
    ItemsByFirstProjectionV2,
    ItemsByUpdatedAtOrdinal,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct HistoryCursor {
    pub requested_thread_id: ThreadId,
    pub rollout_ordinal: u64,
    pub include_anchor: bool,
    pub scope: CursorScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boundary: Option<HistoryPosition>,
}

#[derive(Clone, Copy)]
pub(super) struct RolloutHistoryPosition {
    pub rollout_ordinal: i64,
}

pub(super) struct StoredTurnRow {
    pub position: RolloutHistoryPosition,
    pub turn_id: String,
    pub status: StoredTurnStatus,
    pub error: Option<StoredTurnError>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub duration_ms: Option<i64>,
    pub first_user_item_id: Option<String>,
    pub final_agent_item_id: Option<String>,
    pub summary_items: Vec<StoredThreadItem>,
}

#[derive(sqlx::FromRow)]
pub(super) struct StoredSummaryColumns {
    summary_first_user_turn_id: Option<String>,
    summary_first_user_item_id: Option<String>,
    summary_first_user_rollout_ordinal: Option<i64>,
    summary_first_user_updated_at_ordinal: Option<i64>,
    summary_first_user_created_at_ms: Option<i64>,
    summary_first_user_item_json: Option<String>,
    summary_final_agent_turn_id: Option<String>,
    summary_final_agent_item_id: Option<String>,
    summary_final_agent_rollout_ordinal: Option<i64>,
    summary_final_agent_updated_at_ordinal: Option<i64>,
    summary_final_agent_created_at_ms: Option<i64>,
    summary_final_agent_item_json: Option<String>,
}

struct StoredSummaryItemColumns {
    turn_id: Option<String>,
    item_id: Option<String>,
    rollout_ordinal: Option<i64>,
    updated_at_ordinal: Option<i64>,
    created_at_ms: Option<i64>,
    item_json: Option<String>,
}

pub(super) struct StoredThreadItemRow {
    pub position: RolloutHistoryPosition,
    pub item: StoredThreadItem,
}

pub(in crate::local) async fn observation_cursors(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<crate::ThreadObservationCursors> {
    {
        let _writer = store.live_writer_locks.lock(thread_id).await;
        let recorder = store
            .live_recorders
            .lock()
            .await
            .get(&thread_id)
            .map(|entry| entry.recorder.clone());
        if let Some(recorder) = recorder {
            // Flush retries an unwritten suffix; a deferred empty writer stays unmaterialized.
            recorder
                .flush()
                .await
                .map_err(super::thread_history_error)?;
            if !tokio::fs::try_exists(recorder.rollout_path())
                .await
                .map_err(super::thread_history_error)?
            {
                return Ok(crate::ThreadObservationCursors {
                    turns: None,
                    items: None,
                });
            }
        }
    }
    validate_thread_for_paginated_reads(store, thread_id, true, "thread/subscribe").await?;
    let lineage = store.resolve_rollout_lineage(thread_id).await?;
    let segment = lineage
        .segments()
        .last()
        .ok_or_else(|| invalid_cursor("empty lineage"))?;
    let state = super::projection_state(store, segment.rollout_id())
        .await?
        .ok_or_else(|| invalid_cursor("history projection unavailable"))?;
    let boundary = HistoryPosition {
        thread_id: segment.rollout_id(),
        end_ordinal_exclusive: state.next_ordinal,
        end_byte_offset: state.next_byte_offset,
    };
    let anchor = lineage.segments().iter().rev().find_map(|segment| {
        segment
            .end_ordinal()
            .unwrap_or(state.next_ordinal)
            .checked_sub(1)
            .filter(|ordinal| *ordinal >= segment.start_ordinal())
    });
    let Some(anchor) = anchor else {
        return Ok(crate::ThreadObservationCursors {
            turns: None,
            items: None,
        });
    };
    let make_cursor = |scope| {
        serde_json::to_string(&HistoryCursor {
            requested_thread_id: thread_id,
            rollout_ordinal: anchor,
            include_anchor: true,
            scope,
            boundary: Some(boundary),
        })
        .map_err(super::thread_history_error)
    };
    Ok(crate::ThreadObservationCursors {
        turns: Some(make_cursor(CursorScope::Turns)?),
        items: Some(make_cursor(CursorScope::ItemsByFirstProjectionV2)?),
    })
}

/// Latest terminal turn in the canonical visible lineage, without reading item JSON.
pub(in crate::local) async fn latest_turn_completion(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Option<i64>> {
    validate_thread_for_paginated_reads(
        store,
        thread_id,
        true,
        "thread/list.includeCompletionTimes",
    )
    .await?;
    let lineage = store.resolve_rollout_lineage(thread_id).await?;
    let page = page_turn_rows(
        store.thread_history_db().await?,
        thread_id,
        &lineage,
        None,
        1,
        crate::SortDirection::Desc,
        super::segment_paging::TurnSelection::Terminal,
    )
    .await?;
    Ok(page.rows.first().and_then(|turn| turn.completed_at))
}

pub(in crate::local) async fn list_turns(
    store: &LocalThreadStore,
    params: ListTurnsParams,
) -> ThreadStoreResult<TurnPage> {
    validate_thread_for_paginated_reads(
        store,
        params.thread_id,
        params.include_archived,
        "list_turns",
    )
    .await?;
    validate_page_size(params.page_size)?;
    let lineage = store.resolve_rollout_lineage(params.thread_id).await?;
    let pool = store.thread_history_db().await?;
    let page = page_turn_rows(
        pool,
        params.thread_id,
        &lineage,
        params.cursor.as_deref(),
        params.page_size,
        params.sort_direction,
        super::segment_paging::TurnSelection::All(params.items_view),
    )
    .await?;
    let mut turns = Vec::with_capacity(page.rows.len());
    for turn in page.rows {
        let items = match params.items_view {
            StoredTurnItemsView::NotLoaded => Vec::new(),
            StoredTurnItemsView::Summary
                if matches!(turn.status, StoredTurnStatus::Interrupted)
                    && turn.first_user_item_id.is_none()
                    && turn.final_agent_item_id.is_none() =>
            {
                // Synthetic fork-boundary rows are interrupted without local summary IDs.
                // Load their summary from the earliest visible source turn.
                load_inherited_summary_items(pool, &lineage, &turn).await?
            }
            StoredTurnItemsView::Summary => turn.summary_items,
        };
        turns.push(StoredTurn {
            turn_id: turn.turn_id,
            items,
            items_view: params.items_view,
            status: turn.status,
            error: turn.error,
            started_at: turn.started_at,
            completed_at: turn.completed_at,
            duration_ms: turn.duration_ms,
        });
    }

    Ok(TurnPage {
        is_observation_snapshot: parse_cursor(
            params.cursor.as_deref(),
            params.thread_id,
            CursorScope::Turns,
        )?
        .is_some_and(|cursor| cursor.boundary.is_some()),
        turns,
        next_cursor: page.next_cursor,
        backwards_cursor: page.backwards_cursor,
    })
}

pub(in crate::local) async fn list_items(
    store: &LocalThreadStore,
    params: ListItemsParams,
) -> ThreadStoreResult<ItemPage> {
    validate_thread_for_paginated_reads(
        store,
        params.thread_id,
        params.include_archived,
        "list_items",
    )
    .await?;
    validate_page_size(params.page_size)?;
    let lineage = store.resolve_rollout_lineage(params.thread_id).await?;
    let pool = store.thread_history_db().await?;
    let page = page_item_rows(pool, &lineage, &params).await?;

    Ok(ItemPage {
        items: page.rows.into_iter().map(|row| row.item).collect(),
        next_cursor: page.next_cursor,
        backwards_cursor: page.backwards_cursor,
    })
}

pub(super) async fn validate_thread_for_paginated_reads(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    include_archived: bool,
    operation: &'static str,
) -> ThreadStoreResult<()> {
    let Some(state_db) = store.state_db().await else {
        return Err(ThreadStoreError::Unsupported { operation });
    };
    let Some(metadata) =
        state_db
            .get_thread(thread_id)
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to read thread metadata: {err}"),
            })?
    else {
        return Err(ThreadStoreError::Unsupported { operation });
    };
    if metadata.archived_at.is_some() && !include_archived {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("thread {thread_id} is archived"),
        });
    }

    // JSONL is canonical and SQLite is only its paginated read projection. A failed projection
    // must not turn into a successful read of an old prefix: retry the projection under the same
    // per-thread lock used by writers, then surface any persistent integrity error to the client.
    let current_rollout_id = {
        let _writer_guard = store.live_writer_locks.lock(thread_id).await;
        let resolved = super::super::thread_rollout_resolver::resolve_current_including_archived(
            store, thread_id,
        )
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        if metadata.history_mode == ThreadHistoryMode::Legacy {
            return super::super::legacy_history_materialization::materialize(
                store,
                resolved.rollout_id,
                &resolved.path,
            )
            .await;
        }
        materialize_paginated_rollout(
            store,
            resolved.rollout_id,
            resolved.path.as_path(),
            ProjectionTarget::Current,
        )
        .await?;
        resolved.rollout_id
    };

    // Imported ancestors have canonical JSONL but may have no local SQLite projection. Read
    // every selected lineage segment before paging; otherwise its turns silently disappear.
    // Keep only one writer lock at a time so concurrent forks and reads cannot deadlock.
    let lineage = store.resolve_rollout_lineage(thread_id).await?;
    for segment in lineage.segments() {
        if segment.rollout_id() == current_rollout_id {
            continue;
        }
        let end = segment.end.ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "ancestor rollout {} has no lineage cutoff",
                segment.rollout_id()
            ),
        })?;
        materialize_inherited_rollout(store, segment.rollout_id(), end).await?;
    }
    Ok(())
}

async fn materialize_inherited_rollout(
    store: &LocalThreadStore,
    rollout_id: ThreadId,
    end: HistoryPosition,
) -> ThreadStoreResult<()> {
    let _writer_guard = store.live_writer_locks.lock(rollout_id).await;
    if super::projection_state(store, rollout_id)
        .await?
        .as_ref()
        .is_some_and(|state| {
            state.next_byte_offset >= end.end_byte_offset
                && state.next_ordinal >= end.end_ordinal_exclusive
        })
    {
        return Ok(());
    }
    // Archive and unarchive can move an ancestor after lineage resolution. Re-resolve its
    // immutable identity under the writer lock instead of using the earlier path snapshot.
    let path = codex_rollout::find_rollout_path_by_rollout_id(
        store.config.codex_home.as_path(),
        rollout_id,
    )
    .await
    .map_err(|error| ThreadStoreError::Internal {
        message: format!("failed to locate inherited rollout {rollout_id}: {error}"),
    })?
    .ok_or_else(|| ThreadStoreError::Internal {
        message: format!("missing inherited rollout {rollout_id}"),
    })?;
    materialize_paginated_rollout(
        store,
        rollout_id,
        path.as_path(),
        ProjectionTarget::InheritedThrough(end),
    )
    .await
}

#[derive(Clone, Copy)]
enum ProjectionTarget {
    Current,
    InheritedThrough(HistoryPosition),
}

async fn materialize_paginated_rollout(
    store: &LocalThreadStore,
    rollout_id: ThreadId,
    rollout_path: &std::path::Path,
    target: ProjectionTarget,
) -> ThreadStoreResult<()> {
    // Cold compressed rollouts with an existing projection need no decoding. A newly imported
    // compressed ancestor has no projection, so replay its decoded native bytes below.
    let projection_state = super::projection_state(store, rollout_id).await?;
    if let ProjectionTarget::InheritedThrough(end) = target
        && projection_state.as_ref().is_some_and(|state| {
            state.next_byte_offset >= end.end_byte_offset
                && state.next_ordinal >= end.end_ordinal_exclusive
        })
    {
        return Ok(());
    }
    if rollout_path.extension().and_then(std::ffi::OsStr::to_str) == Some("zst")
        && projection_state.is_some()
        && matches!(target, ProjectionTarget::Current)
    {
        return Ok(());
    }
    if let Some(projection_state) = projection_state
        && rollout_path.extension().and_then(std::ffi::OsStr::to_str) != Some("zst")
        && !super::super::thread_history_materialization::projection_checkpoint_is_line_boundary(
            rollout_path,
            projection_state.next_byte_offset,
        )
        .await?
    {
        error!(
            thread_id = %rollout_id,
            rollout_path = %rollout_path.display(),
            checkpoint_byte_offset = projection_state.next_byte_offset,
            checkpoint_ordinal = projection_state.next_ordinal,
            "rebuilding a misaligned paginated history projection from canonical JSONL"
        );
        super::delete_thread(store, rollout_id).await?;
    }
    super::super::thread_history_materialization::materialize_to_sqlite(
        store,
        rollout_id,
        rollout_path,
    )
    .await?;
    if let ProjectionTarget::InheritedThrough(end) = target {
        let reached = super::projection_state(store, rollout_id)
            .await?
            .is_some_and(|state| {
                state.next_byte_offset >= end.end_byte_offset
                    && state.next_ordinal >= end.end_ordinal_exclusive
            });
        if !reached {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "inherited rollout {rollout_id} projection did not reach ordinal {} at byte {}",
                    end.end_ordinal_exclusive, end.end_byte_offset
                ),
            });
        }
    }
    Ok(())
}

async fn load_inherited_summary_items(
    pool: &sqlx::SqlitePool,
    lineage: &RolloutLineage,
    turn: &StoredTurnRow,
) -> ThreadStoreResult<Vec<StoredThreadItem>> {
    let source = find_source_turn(pool, lineage, turn.turn_id.as_str()).await?;
    let Some(segment) = lineage
        .segments()
        .iter()
        .find(|segment| segment.rollout_id() == source.rollout_id)
    else {
        return Ok(Vec::new());
    };
    let start_ordinal = sqlite_integer(segment.start_ordinal(), "rollout ordinal")?;
    let end_ordinal = segment
        .end_ordinal()
        .map(|ordinal| sqlite_integer(ordinal, "rollout ordinal"))
        .transpose()?;
    let rows = sqlx::query(
        r#"
SELECT turn_id, item_id, rollout_ordinal AS started_at_ordinal, updated_at_ordinal, created_at_ms, item_json, is_complete, completed_at_ms
FROM thread_items
WHERE thread_id = ?
  AND turn_id = ?
  AND rollout_ordinal >= ?
  AND (? IS NULL OR updated_at_ordinal < ?)
  AND (item_id = ? OR item_id = ?)
ORDER BY rollout_ordinal ASC
        "#,
    )
    .bind(source.rollout_id.to_string())
    .bind(turn.turn_id.as_str())
    .bind(start_ordinal)
    .bind(end_ordinal)
    .bind(end_ordinal)
    .bind(source.first_user_item_id)
    .bind(source.final_agent_item_id)
    .fetch_all(pool)
    .await
    .map_err(super::thread_history_error)?;
    rows.into_iter().map(stored_thread_item).collect()
}

pub(super) fn parse_cursor(
    cursor: Option<&str>,
    requested_thread_id: ThreadId,
    scope: CursorScope,
) -> ThreadStoreResult<Option<HistoryCursor>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let cursor_value: HistoryCursor =
        serde_json::from_str(cursor).map_err(|_| invalid_cursor(cursor))?;
    if cursor_value.requested_thread_id != requested_thread_id || cursor_value.scope != scope {
        return Err(invalid_cursor(cursor));
    }
    Ok(Some(cursor_value))
}

pub(super) fn serialize_cursor(
    requested_thread_id: ThreadId,
    scope: CursorScope,
    rollout_ordinal: i64,
    include_anchor: bool,
) -> ThreadStoreResult<String> {
    let rollout_ordinal =
        u64::try_from(rollout_ordinal).map_err(|_| invalid_cursor("negative rollout ordinal"))?;
    serde_json::to_string(&HistoryCursor {
        requested_thread_id,
        rollout_ordinal,
        include_anchor,
        scope,
        boundary: None,
    })
    .map_err(super::thread_history_error)
}

pub(super) fn stored_turn_row(row: sqlx::sqlite::SqliteRow) -> ThreadStoreResult<StoredTurnRow> {
    let status = match row.try_get::<String, _>("status")?.as_str() {
        "completed" => StoredTurnStatus::Completed,
        "interrupted" => StoredTurnStatus::Interrupted,
        "failed" => StoredTurnStatus::Failed,
        "inProgress" => StoredTurnStatus::InProgress,
        status => {
            return Err(ThreadStoreError::Internal {
                message: format!("unknown stored turn status: {status}"),
            });
        }
    };
    let error_json = row.try_get::<Option<String>, _>("error_json")?;
    let error = error_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(super::thread_history_error)?;
    Ok(StoredTurnRow {
        position: RolloutHistoryPosition {
            rollout_ordinal: row.try_get("rollout_ordinal")?,
        },
        turn_id: row.try_get("turn_id")?,
        status,
        error,
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
        duration_ms: row.try_get("duration_ms")?,
        first_user_item_id: row.try_get("first_user_item_id")?,
        final_agent_item_id: row.try_get("final_agent_item_id")?,
        summary_items: Vec::new(),
    })
}

impl StoredSummaryColumns {
    pub(super) fn into_stored_items(self) -> ThreadStoreResult<Vec<StoredThreadItem>> {
        let mut summary_items = [
            StoredSummaryItemColumns {
                turn_id: self.summary_first_user_turn_id,
                item_id: self.summary_first_user_item_id,
                rollout_ordinal: self.summary_first_user_rollout_ordinal,
                updated_at_ordinal: self.summary_first_user_updated_at_ordinal,
                created_at_ms: self.summary_first_user_created_at_ms,
                item_json: self.summary_first_user_item_json,
            }
            .into_stored_item()?,
            StoredSummaryItemColumns {
                turn_id: self.summary_final_agent_turn_id,
                item_id: self.summary_final_agent_item_id,
                rollout_ordinal: self.summary_final_agent_rollout_ordinal,
                updated_at_ordinal: self.summary_final_agent_updated_at_ordinal,
                created_at_ms: self.summary_final_agent_created_at_ms,
                item_json: self.summary_final_agent_item_json,
            }
            .into_stored_item()?,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        summary_items.sort_by_key(|(rollout_ordinal, _)| *rollout_ordinal);
        Ok(summary_items.into_iter().map(|(_, item)| item).collect())
    }
}

impl StoredSummaryItemColumns {
    fn into_stored_item(self) -> ThreadStoreResult<Option<(i64, StoredThreadItem)>> {
        let Some(item_id) = self.item_id else {
            return Ok(None);
        };
        let (
            Some(turn_id),
            Some(rollout_ordinal),
            Some(updated_at_ordinal),
            Some(created_at_ms),
            Some(item_json),
        ) = (
            self.turn_id,
            self.rollout_ordinal,
            self.updated_at_ordinal,
            self.created_at_ms,
            self.item_json,
        )
        else {
            return Err(ThreadStoreError::Internal {
                message: "stored summary item is missing joined columns".to_string(),
            });
        };
        Ok(Some((
            rollout_ordinal,
            StoredThreadItem {
                started_at_ordinal: stored_updated_at_ordinal(rollout_ordinal)?,
                is_complete: true,
                completed_at_ms: None,
                turn_id,
                item_id,
                updated_at_ordinal: stored_updated_at_ordinal(updated_at_ordinal)?,
                created_at_ms,
                item_json: item_json.into_bytes(),
            },
        )))
    }
}

pub(super) fn stored_thread_item_row(
    row: sqlx::sqlite::SqliteRow,
) -> ThreadStoreResult<StoredThreadItemRow> {
    let rollout_ordinal = row.try_get::<i64, _>("rollout_ordinal")?;
    if rollout_ordinal < 0 {
        return Err(ThreadStoreError::Internal {
            message: format!("invalid stored item rollout ordinal: {rollout_ordinal}"),
        });
    }
    Ok(StoredThreadItemRow {
        position: RolloutHistoryPosition { rollout_ordinal },
        item: stored_thread_item(row)?,
    })
}

fn stored_thread_item(row: sqlx::sqlite::SqliteRow) -> ThreadStoreResult<StoredThreadItem> {
    let updated_at_ordinal = stored_updated_at_ordinal(row.try_get("updated_at_ordinal")?)?;
    Ok(StoredThreadItem {
        started_at_ordinal: stored_updated_at_ordinal(row.try_get("started_at_ordinal")?)?,
        is_complete: row.try_get("is_complete")?,
        completed_at_ms: row.try_get("completed_at_ms")?,
        turn_id: row.try_get("turn_id")?,
        item_id: row.try_get("item_id")?,
        updated_at_ordinal,
        created_at_ms: row.try_get("created_at_ms")?,
        item_json: row.try_get::<String, _>("item_json")?.into_bytes(),
    })
}

fn stored_updated_at_ordinal(updated_at_ordinal: i64) -> ThreadStoreResult<u64> {
    u64::try_from(updated_at_ordinal).map_err(|_| ThreadStoreError::Internal {
        message: format!("invalid stored item updated-at ordinal: {updated_at_ordinal}"),
    })
}

pub(super) fn invalid_cursor(cursor: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("invalid cursor: {cursor}"),
    }
}
