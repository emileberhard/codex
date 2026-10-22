// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use std::path::Path;

use codex_app_server_protocol::LegacyThreadHistoryProjection;
use codex_protocol::ThreadId;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::BufReader;

use super::LocalThreadStore;
use super::thread_history::ProjectedRolloutLine;
use super::thread_history::RolloutProjectionStep;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

const MAX_RECORD_BYTES: usize = 32 * 1024 * 1024;
const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
const MAX_BATCH_RECORDS: usize = 512;

pub(super) async fn materialize(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    path: &Path,
) -> ThreadStoreResult<()> {
    let saved = super::thread_history::projection_state(store, thread_id).await?;
    let mut projection = match saved.as_ref() {
        Some(saved) => LegacyThreadHistoryProjection::restore(
            saved
                .legacy_state_json
                .as_deref()
                .ok_or_else(|| failure("legacy projection metadata is missing"))?,
        )
        .map_err(failure)?,
        None => LegacyThreadHistoryProjection::default(),
    };
    let mut offset = saved.as_ref().map_or(0, |saved| saved.next_byte_offset);
    let mut ordinal = saved.as_ref().map_or(0, |saved| saved.next_ordinal);
    let owned_path = path.to_owned();
    let file = tokio::task::spawn_blocking(move || {
        codex_rollout::open_rollout_seekable_reader(&owned_path)
    })
    .await
    .map_err(failure)?
    .map_err(failure)?;
    let mut file = tokio::fs::File::from_std(file);
    let length = file.metadata().await.map_err(failure)?.len();
    let remaining = length
        .checked_sub(offset)
        .ok_or_else(|| failure("legacy rollout shrank behind its projection"))?;
    file.seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(failure)?;
    let mut reader = BufReader::new(file.take(remaining));
    let mut batch = Vec::new();
    let mut batch_start = offset;
    let mut bytes: usize = 0;
    loop {
        let mut record = Vec::new();
        loop {
            let available = reader.fill_buf().await.map_err(failure)?;
            if available.is_empty() {
                break;
            }
            let take = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(available.len(), |index| index + 1);
            if record.len() + take > MAX_RECORD_BYTES {
                return Err(failure("legacy rollout record exceeds projection capacity"));
            }
            record.extend_from_slice(&available[..take]);
            reader.consume(take);
            if record.last() == Some(&b'\n') {
                break;
            }
        }
        if record.last() != Some(&b'\n') {
            break;
        }
        let end = offset
            .checked_add(record.len() as u64)
            .ok_or_else(|| failure("legacy byte offset overflow"))?;
        if record.iter().all(u8::is_ascii_whitespace) {
            offset = end;
            continue;
        }
        let value = serde_json::from_slice(&record).map_err(failure)?;
        let line = codex_rollout::decode_rollout_line(value).map_err(failure)?;
        let forecast = record
            .len()
            .saturating_mul(2)
            .saturating_add(projection.retained_payload_bytes())
            .saturating_add(4096);
        if !batch.is_empty() && bytes.saturating_add(forecast) > MAX_BATCH_BYTES {
            batch.push(RolloutProjectionStep::LegacyState(
                projection.checkpoint().map_err(failure)?,
            ));
            super::thread_history::apply_projection(
                store,
                thread_id,
                batch_start,
                offset,
                0,
                std::mem::take(&mut batch),
            )
            .await?;
            batch_start = offset;
            bytes = 0;
        }
        let changes = projection.project(&line.item).map_err(failure)?;
        let projected_bytes = changes
            .changed_items
            .iter()
            .try_fold(0usize, |size, change| {
                serde_json::to_vec(&change.item).map(|data| size.saturating_add(data.len()))
            })
            .map_err(failure)?;
        let timestamp = chrono::DateTime::parse_from_rfc3339(&line.timestamp)
            .map_err(failure)?
            .timestamp_millis();
        batch.push(RolloutProjectionStep::Line(Box::new(
            ProjectedRolloutLine {
                ordinal,
                start_byte_offset: offset,
                end_byte_offset: end,
                fallback_created_at_ms: Some(timestamp),
                changes,
                realtime_item: None,
            },
        )));
        bytes = bytes
            .saturating_add(record.len())
            .saturating_add(projected_bytes);
        ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| failure("legacy record position overflow"))?;
        offset = end;
        if bytes >= MAX_BATCH_BYTES || batch.len() >= MAX_BATCH_RECORDS {
            batch.push(RolloutProjectionStep::LegacyState(
                projection.checkpoint().map_err(failure)?,
            ));
            super::thread_history::apply_projection(
                store,
                thread_id,
                batch_start,
                offset,
                0,
                std::mem::take(&mut batch),
            )
            .await?;
            batch_start = offset;
            bytes = 0;
        }
    }
    if offset != batch_start {
        batch.push(RolloutProjectionStep::LegacyState(
            projection.checkpoint().map_err(failure)?,
        ));
        super::thread_history::apply_projection(store, thread_id, batch_start, offset, 0, batch)
            .await?;
    }
    Ok(())
}

fn failure(error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("legacy history projection: {error}"),
    }
}
