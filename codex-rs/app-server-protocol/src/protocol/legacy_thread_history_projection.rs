use super::ThreadHistoryBuilder;
use super::ThreadHistoryChangeSet;
use super::ThreadItem;
use codex_rollout::RolloutItem;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;

const MAX_RETAINED_PAYLOAD_BYTES: usize = 32 * 1024 * 1024;

/// Streams the existing legacy reducer into a durable projection. Its released
/// payloads are inaccessible: this owner exposes changes and checkpoints only.
#[derive(Default, Serialize, Deserialize)]
pub struct LegacyThreadHistoryProjection {
    builder: ThreadHistoryBuilder,
    retained: HashSet<(String, String)>,
    pending_parts: HashSet<(String, String)>,
    retained_payload_bytes: usize,
}

impl LegacyThreadHistoryProjection {
    pub fn restore(checkpoint: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(checkpoint)
    }

    pub fn retained_payload_bytes(&self) -> usize {
        self.retained_payload_bytes
    }

    pub fn checkpoint(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn project(&mut self, item: &RolloutItem) -> Result<ThreadHistoryChangeSet, String> {
        let source = match item {
            RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::AgentMessage(event)) => {
                event.source_item.as_ref()
            }
            RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::AgentReasoning(event)) => {
                event.source_item.as_ref()
            }
            RolloutItem::EventMsg(
                codex_protocol::protocol::EventMsg::AgentReasoningRawContent(event),
            ) => event.source_item.as_ref(),
            _ => None,
        };
        if source.is_some_and(|source| source.index >= 200) {
            return Err("legacy source item exceeds the section capacity".into());
        }
        let changes = self.builder.handle_rollout_item_with_changes(item);
        if let Some(source) = source
            && let Some(turn) = self.builder.current_turn.as_ref()
        {
            let key = (turn.id.clone(), source.id.clone());
            if source.last {
                self.pending_parts.remove(&key);
            } else {
                self.pending_parts.insert(key);
            }
        }
        for removed in &changes.removed_turn_ids {
            self.pending_parts.retain(|(turn, _)| turn != removed);
        }
        let mut keep = self.pending_parts.clone();
        if let Some(turn) = &self.builder.current_turn
            && let Some(item @ ThreadItem::Reasoning { .. }) = turn.items.last()
        {
            keep.insert((turn.id.clone(), item.id().to_owned()));
        }
        let release = self
            .retained
            .iter()
            .cloned()
            .chain(
                changes
                    .changed_items
                    .iter()
                    .map(|change| (change.turn_id.clone(), change.item.id().to_owned())),
            )
            .filter(|key| !keep.contains(key))
            .collect::<HashSet<_>>();
        for (turn, item) in release {
            self.builder.release_projected_payload(&turn, &item);
        }
        self.retained = keep;
        let retained_bytes =
            self.retained
                .iter()
                .try_fold(0usize, |bytes, (turn_id, item_id)| {
                    let item = self
                        .builder
                        .current_turn
                        .iter()
                        .chain(self.builder.turns.iter())
                        .find(|turn| &turn.id == turn_id)
                        .and_then(|turn| turn.items.iter().find(|item| item.id() == item_id));
                    let size = item
                        .map(serde_json::to_vec)
                        .transpose()
                        .map_err(|error| error.to_string())?
                        .map_or(0, |data| data.len());
                    Ok::<_, String>(bytes.saturating_add(size))
                })?;
        self.retained_payload_bytes = retained_bytes;
        if retained_bytes > MAX_RETAINED_PAYLOAD_BYTES {
            return Err("legacy incremental item exceeds the projection capacity".into());
        }
        Ok(changes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::protocol::AgentMessageEvent;
    use codex_protocol::protocol::AgentReasoningEvent;
    use codex_protocol::protocol::AgentReasoningRawContentEvent;
    use codex_protocol::protocol::EventMsg;
    use codex_protocol::protocol::LegacyItemSource;
    use codex_protocol::protocol::ThreadRolledBackEvent;
    use codex_protocol::protocol::TurnStartedEvent;
    use std::collections::BTreeMap;

    fn start(id: &str) -> RolloutItem {
        RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
            turn_id: id.into(),
            turn_attribution: None,
            root_turn_id: None,
            trace_id: None,
            started_at: Some(1),
            model_context_window: None,
            collaboration_mode_kind: Default::default(),
        }))
    }

    fn message(text: &str, source_item: Option<LegacyItemSource>) -> RolloutItem {
        RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
            source_item,
            message: text.into(),
            phase: None,
            memory_citation: None,
            delivery: None,
            questions: None,
        }))
    }

    fn source(id: &str, index: usize, last: bool) -> Option<LegacyItemSource> {
        Some(LegacyItemSource {
            id: id.into(),
            index,
            last,
        })
    }

    #[test]
    fn legacy_projection_reopen_preserves_source_grouping_empty_final_and_rollback_allocation() {
        let records = vec![
            start("first"),
            message("old", None),
            message("hello", source("canonical", 0, false)),
            message("", source("canonical", 1, true)),
            RolloutItem::EventMsg(EventMsg::AgentReasoning(AgentReasoningEvent {
                source_item: source("reason", 0, false),
                text: "summary".into(),
            })),
            RolloutItem::EventMsg(EventMsg::AgentReasoning(AgentReasoningEvent {
                source_item: source("reason", 1, false),
                text: "".into(),
            })),
            RolloutItem::EventMsg(EventMsg::AgentReasoningRawContent(
                AgentReasoningRawContentEvent {
                    source_item: source("reason", 0, true),
                    text: "".into(),
                },
            )),
            start("removed"),
            message("remove", None),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            start("last"),
            message("after rollback", None),
        ];
        let mut projection = LegacyThreadHistoryProjection::default();
        let mut actual = BTreeMap::new();
        for record in &records {
            let changes = projection.project(record).unwrap();
            for removed in changes.removed_turn_ids {
                actual.retain(|(turn, _), _| turn != &removed);
            }
            for change in changes.changed_items {
                actual.insert(
                    (change.turn_id, change.item.id().to_owned()),
                    (change.item, change.is_complete),
                );
            }
            projection =
                LegacyThreadHistoryProjection::restore(&projection.checkpoint().unwrap()).unwrap();
        }
        let expected = super::super::build_turns_from_rollout_items(&records)
            .into_iter()
            .flat_map(|turn| {
                turn.items
                    .into_iter()
                    .map(move |item| ((turn.id.clone(), item.id().to_owned()), (item, true)))
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, expected);
        assert!(actual.contains_key(&("last".into(), "item-4".into())));
        assert!(actual[&("first".into(), "canonical".into())].1);
        assert!(
            matches!(&actual[&("first".into(), "reason".into())].0, ThreadItem::Reasoning { summary, content, .. } if summary == &["summary", ""] && content == &[""])
        );
    }

    #[test]
    fn legacy_projection_does_not_checkpoint_large_historical_message_payloads() {
        let mut projection = LegacyThreadHistoryProjection::default();
        projection.project(&start("large")).unwrap();
        let text = "x".repeat(512 * 1024);
        let started = std::time::Instant::now();
        let mut peak_checkpoint = 0;
        for _ in 0..128 {
            let changes = projection.project(&message(&text, None)).unwrap();
            assert_eq!(changes.changed_items.len(), 1);
            let checkpoint = projection.checkpoint().unwrap();
            peak_checkpoint = peak_checkpoint.max(checkpoint.len());
            projection = LegacyThreadHistoryProjection::restore(&checkpoint).unwrap();
        }
        assert!(
            peak_checkpoint < 64 * 1024,
            "metadata grew to {peak_checkpoint} bytes"
        );
        eprintln!(
            "legacy projection: input={} bytes, peak checkpoint={peak_checkpoint} bytes, elapsed={:?}",
            text.len() * 128,
            started.elapsed()
        );
    }
}
