// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use std::io::BufRead;
use std::io::BufReader;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::TurnStartedEvent;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use serde::Deserialize;
use sqlx::Row;

use super::rollout_lineage::RolloutLineage;
use super::rollout_lineage::RolloutLineageSegment;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub(super) async fn position_after_message_record(
    pool: &sqlx::SqlitePool,
    visible: &RolloutLineage,
    turn_id: &str,
    item_id: &str,
) -> ThreadStoreResult<HistoryPosition> {
    let (segment, completed_ordinal) =
        completed_assistant_message(pool, visible, turn_id, item_id).await?;
    let turn_start_byte_offset = turn_start_byte_offset(pool, segment, turn_id)
        .await?
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: format!("turn {turn_id} does not have a persisted start boundary"),
        })?;
    let record_ordinal = completed_ordinal + 1;
    let no_adjacent_record = || ThreadStoreError::InvalidRequest {
        message: format!(
            "lastItemId '{item_id}' has no model-visible message directly after its completion"
        ),
    };
    let (line, end_byte_offset) = read_line(
        segment.rollout_path.clone(),
        turn_start_byte_offset,
        LineTarget::Ordinal(record_ordinal),
    )
    .await?
    .ok_or_else(no_adjacent_record)?;
    let is_message_record = matches!(
        &line.item,
        RolloutItem::ResponseItem(response_item)
            if matches!(
                &response_item.item,
                ResponseItem::Message { role, id: Some(id), .. }
                    if role == "assistant" && id.as_str() == item_id
            )
    );
    if !is_message_record {
        return Err(no_adjacent_record());
    }
    Ok(HistoryPosition {
        thread_id: segment.rollout_id(),
        end_ordinal_exclusive: record_ordinal + 1,
        end_byte_offset,
    })
}

pub(super) async fn open_turn_start(
    pool: &sqlx::SqlitePool,
    lineage: &RolloutLineage,
    history_base: HistoryPosition,
) -> ThreadStoreResult<Option<TurnStartedEvent>> {
    let segment = lineage
        .segments()
        .iter()
        .find(|segment| segment.rollout_id() == history_base.thread_id)
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "fork position is outside the source lineage".to_string(),
        })?;
    let end_ordinal = sqlite_integer(history_base.end_ordinal_exclusive)?;
    let Some(open_turn) = sqlx::query(
        r#"
SELECT turn_id, rollout_byte_offset
FROM thread_turns
WHERE thread_id = ?
  AND rollout_ordinal >= ?
  AND rollout_ordinal < ?
  AND rollout_byte_offset IS NOT NULL
  AND (rollout_end_ordinal IS NULL OR rollout_end_ordinal >= ?)
  AND (rollout_end_ordinal IS NULL OR rollout_end_ordinal != rollout_ordinal)
ORDER BY rollout_ordinal DESC
LIMIT 1
        "#,
    )
    .bind(segment.rollout_id().to_string())
    .bind(sqlite_integer(segment.start_ordinal())?)
    .bind(end_ordinal)
    .bind(end_ordinal)
    .fetch_optional(pool)
    .await
    .map_err(history_error)?
    else {
        return Ok(None);
    };
    let turn_id: String = open_turn.get("turn_id");
    let byte_offset =
        u64::try_from(open_turn.get::<i64, _>("rollout_byte_offset")).map_err(history_error)?;
    let line = read_line(segment.rollout_path.clone(), byte_offset, LineTarget::First).await?;
    match line.map(|(line, _)| line.item) {
        Some(RolloutItem::EventMsg(EventMsg::TurnStarted(event))) if event.turn_id == turn_id => {
            Ok(Some(event))
        }
        _ => Err(ThreadStoreError::Internal {
            message: format!(
                "turn {turn_id} has no persisted start record at its projected position"
            ),
        }),
    }
}

pub(super) fn keep_turn_start(
    model_context: &mut Vec<RolloutItem>,
    turn_started: TurnStartedEvent,
) {
    if model_context.iter().any(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::TurnStarted(event))
                if event.turn_id == turn_started.turn_id
        )
    }) {
        return;
    }
    let after_session_meta = usize::from(matches!(
        model_context.first(),
        Some(RolloutItem::SessionMeta(_))
    ));
    model_context.insert(
        after_session_meta,
        RolloutItem::EventMsg(EventMsg::TurnStarted(turn_started)),
    );
}

async fn completed_assistant_message<'a>(
    pool: &sqlx::SqlitePool,
    visible: &'a RolloutLineage,
    turn_id: &str,
    item_id: &str,
) -> ThreadStoreResult<(&'a RolloutLineageSegment, u64)> {
    for segment in visible.segments().iter().rev() {
        let end_ordinal = segment.end_ordinal().map(sqlite_integer).transpose()?;
        let Some(row) = sqlx::query(
            r#"
SELECT
    COALESCE(NULLIF(i.item_type, ''), json_extract(i.item_json, '$.type')) AS item_type,
    (
        SELECT MIN(v.rollout_ordinal)
        FROM thread_item_versions v
        WHERE v.thread_id = i.thread_id
          AND v.turn_id = i.turn_id
          AND v.item_id = i.item_id
          AND v.is_complete = 1
          AND (? IS NULL OR v.rollout_ordinal < ?)
    ) AS completed_ordinal
FROM thread_items i
WHERE i.thread_id = ?
  AND i.turn_id = ?
  AND i.item_id = ?
  AND i.rollout_ordinal >= ?
  AND (? IS NULL OR i.rollout_ordinal < ?)
            "#,
        )
        .bind(end_ordinal)
        .bind(end_ordinal)
        .bind(segment.rollout_id().to_string())
        .bind(turn_id)
        .bind(item_id)
        .bind(sqlite_integer(segment.start_ordinal())?)
        .bind(end_ordinal)
        .bind(end_ordinal)
        .fetch_optional(pool)
        .await
        .map_err(history_error)?
        else {
            continue;
        };
        if row.get::<Option<String>, _>("item_type").as_deref() != Some("agentMessage") {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("lastItemId '{item_id}' is not an assistant message"),
            });
        }
        let completed_ordinal =
            row.get::<Option<i64>, _>("completed_ordinal")
                .ok_or_else(|| ThreadStoreError::InvalidRequest {
                    message: format!("lastItemId '{item_id}' has not completed"),
                })?;
        let completed_ordinal = u64::try_from(completed_ordinal).map_err(history_error)?;
        return Ok((segment, completed_ordinal));
    }
    Err(ThreadStoreError::InvalidRequest {
        message: format!("lastItemId '{item_id}' was not found in turn '{turn_id}'"),
    })
}

async fn turn_start_byte_offset(
    pool: &sqlx::SqlitePool,
    segment: &RolloutLineageSegment,
    turn_id: &str,
) -> ThreadStoreResult<Option<u64>> {
    sqlx::query_scalar::<_, Option<i64>>(
        r#"
SELECT rollout_byte_offset
FROM thread_turns
WHERE thread_id = ?
  AND turn_id = ?
  AND (rollout_end_ordinal IS NULL OR rollout_end_ordinal != rollout_ordinal)
        "#,
    )
    .bind(segment.rollout_id().to_string())
    .bind(turn_id)
    .fetch_optional(pool)
    .await
    .map_err(history_error)?
    .flatten()
    .map(|offset| u64::try_from(offset).map_err(history_error))
    .transpose()
}

#[derive(Clone, Copy)]
enum LineTarget {
    First,
    Ordinal(u64),
}

#[derive(Deserialize)]
struct LineOrdinal {
    ordinal: Option<u64>,
}

async fn read_line(
    path: PathBuf,
    start_byte_offset: u64,
    target: LineTarget,
) -> ThreadStoreResult<Option<(RolloutLine, u64)>> {
    let (stop, keep_running) = tokio::sync::oneshot::channel::<()>();
    let result = tokio::task::spawn_blocking(move || {
        read_line_blocking(path.as_path(), start_byte_offset, target, || {
            stop.is_closed()
        })
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join fork boundary read: {err}"),
    })?;
    drop(keep_running);
    result
}

fn read_line_blocking(
    path: &Path,
    start_byte_offset: u64,
    target: LineTarget,
    is_cancelled: impl Fn() -> bool,
) -> ThreadStoreResult<Option<(RolloutLine, u64)>> {
    let mut file =
        codex_rollout::open_rollout_seekable_reader(path, &is_cancelled).map_err(history_error)?;
    file.seek(SeekFrom::Start(start_byte_offset))
        .map_err(history_error)?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut end_byte_offset = start_byte_offset;
    loop {
        if is_cancelled() {
            return Err(history_error("rollout read cancelled"));
        }
        line.clear();
        let read = reader.read_until(b'\n', &mut line).map_err(history_error)?;
        if read == 0 || !line.ends_with(b"\n") {
            return Ok(None);
        }
        end_byte_offset += u64::try_from(read).map_err(history_error)?;
        let is_target = match target {
            LineTarget::First => true,
            LineTarget::Ordinal(ordinal) => match serde_json::from_slice::<LineOrdinal>(&line) {
                Ok(LineOrdinal {
                    ordinal: Some(line_ordinal),
                }) if line_ordinal > ordinal => return Ok(None),
                Ok(LineOrdinal {
                    ordinal: Some(line_ordinal),
                }) => line_ordinal == ordinal,
                Ok(LineOrdinal { ordinal: None }) | Err(_) => false,
            },
        };
        if is_target {
            return Ok(codex_rollout::parse_rollout_line_bytes(&line)
                .ok()
                .map(|parsed| (parsed, end_byte_offset)));
        }
    }
}

fn sqlite_integer(value: u64) -> ThreadStoreResult<i64> {
    i64::try_from(value).map_err(|_| ThreadStoreError::Internal {
        message: "rollout ordinal exceeds SQLite integer range".to_string(),
    })
}

fn history_error(err: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to resolve mid-turn fork boundary: {err}"),
    }
}

#[cfg(test)]
#[path = "mid_turn_fork_tests.rs"]
mod tests;
