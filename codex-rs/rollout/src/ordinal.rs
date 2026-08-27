use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;

use crate::RolloutItem;
use crate::RolloutLine;
use crate::decode_rollout_line;
use crate::reverse_jsonl_scanner::ReverseJsonlScanner;
use crate::reverse_jsonl_scanner::ScanOutcome;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RolloutOrdinalState {
    Legacy,
    Paginated { next: Option<u64> },
}

impl RolloutOrdinalState {
    pub(crate) fn for_new_rollout(
        history_mode: ThreadHistoryMode,
        history_base: Option<HistoryPosition>,
    ) -> Self {
        match history_mode {
            ThreadHistoryMode::Legacy => Self::Legacy,
            ThreadHistoryMode::Paginated => Self::Paginated {
                next: Some(history_base.map_or(0, |base| base.end_ordinal_exclusive)),
            },
        }
    }

    pub(crate) fn current(&self) -> io::Result<Option<u64>> {
        match self {
            Self::Legacy => Ok(None),
            Self::Paginated { next } => {
                let ordinal = (*next)
                    .ok_or_else(|| io::Error::other("paginated rollout record ordinal overflow"))?;
                Ok(Some(ordinal))
            }
        }
    }

    pub(crate) fn advance(&mut self) {
        if let Self::Paginated { next } = self
            && let Some(ordinal) = *next
        {
            *next = ordinal.checked_add(1);
        }
    }
}

pub(crate) fn ordinal_state_for_rollout(
    file: &mut File,
    path: &Path,
) -> io::Result<RolloutOrdinalState> {
    let Some((history_mode, subagent_history_start_ordinal)) = read_history_metadata(file, path)?
    else {
        return Ok(RolloutOrdinalState::Legacy);
    };
    if matches!(history_mode, ThreadHistoryMode::Legacy) {
        return Ok(RolloutOrdinalState::Legacy);
    }

    let mut scanner = ReverseJsonlScanner::new(file)?;
    let record = loop {
        match scanner.scan_next::<serde_json::Value>()? {
            Some(ScanOutcome::Parsed(value)) => match decode_rollout_line(value) {
                Ok(record) => break record,
                Err(_) => continue,
            },
            Some(ScanOutcome::Rejected(_)) => continue,
            None => {
                return Err(io::Error::other(format!(
                    "rollout at {} contains no valid records",
                    path.display()
                )));
            }
        }
    };
    let ordinal = record.ordinal.ok_or_else(|| {
        io::Error::other(format!(
            "final paginated rollout record at {} is missing an ordinal",
            path.display()
        ))
    })?;
    // Child records must start at `subagent_history_start_ordinal`. If initialization died while
    // copying the inherited parent records, resuming would append child records before that
    // boundary.
    if let Some(prefix_end) = subagent_history_start_ordinal.and_then(|start| start.checked_sub(1))
        && ordinal < prefix_end
    {
        return Err(io::Error::other(format!(
            "paginated subagent rollout at {} is incomplete: expected inherited prefix through ordinal {prefix_end}, found final durable ordinal {ordinal}",
            path.display()
        )));
    }
    Ok(RolloutOrdinalState::Paginated {
        next: ordinal.checked_add(1),
    })
}

fn read_history_metadata(
    file: &mut File,
    path: &Path,
) -> io::Result<Option<(ThreadHistoryMode, Option<u64>)>> {
    file.seek(SeekFrom::Start(0))?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: RolloutLine = serde_json::from_str(line.as_str()).map_err(|error| {
            io::Error::other(format!(
                "failed to parse first rollout record at {}: {error}",
                path.display()
            ))
        })?;
        let RolloutItem::SessionMeta(session_meta) = record.item else {
            return Err(io::Error::other(format!(
                "rollout at {} does not start with session metadata",
                path.display()
            )));
        };
        return Ok(Some((
            session_meta.meta.history_mode,
            session_meta.meta.subagent_history_start_ordinal,
        )));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::ThreadId;
    use codex_protocol::protocol::SessionMeta;
    use codex_protocol::protocol::SessionMetaLine;
    use codex_protocol::protocol::SessionSource;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn float_bearing_final_record_advances_paginated_ordinal() {
        let directory = tempdir().expect("temporary rollout directory");
        let path = directory.path().join("rollout.jsonl");
        let thread_id = ThreadId::new();
        let session_meta = RolloutLine {
            timestamp: "2026-08-27T00:00:00Z".to_string(),
            ordinal: Some(0),
            item: RolloutItem::SessionMeta(SessionMetaLine {
                meta: SessionMeta {
                    session_id: thread_id.into(),
                    id: thread_id,
                    timestamp: "2026-08-27T00:00:00Z".to_string(),
                    cwd: directory.path().to_path_buf(),
                    originator: "test".to_string(),
                    cli_version: "test".to_string(),
                    source: SessionSource::Exec,
                    history_mode: ThreadHistoryMode::Paginated,
                    ..SessionMeta::default()
                },
                git: None,
            }),
        };
        let mut writer = File::create(&path).expect("create rollout");
        writeln!(
            writer,
            "{}",
            serde_json::to_string(&session_meta).expect("serialize session metadata")
        )
        .expect("write session metadata");
        writeln!(
            writer,
            r#"{{"timestamp":"2026-08-27T00:00:01Z","ordinal":1,"type":"event_msg","payload":{{"type":"token_count","info":null,"rate_limits":{{"limit_id":null,"limit_name":null,"primary":{{"used_percent":46.0,"window_minutes":null,"resets_at":null}},"secondary":null,"credits":null,"individual_limit":null,"spend_control_reached":null,"plan_type":null,"rate_limit_reached_type":null}}}}}}"#
        )
        .expect("write float-bearing token count");
        drop(writer);

        let mut file = File::open(&path).expect("open rollout");
        assert_eq!(
            ordinal_state_for_rollout(&mut file, &path).expect("read ordinal state"),
            RolloutOrdinalState::Paginated { next: Some(2) }
        );
    }
}
