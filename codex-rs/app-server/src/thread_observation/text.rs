use codex_app_server_protocol::ThreadItem;
use codex_protocol::protocol::EventMsg;
use std::collections::BTreeMap;

pub(super) const MAX_UNFINISHED_ITEMS: usize = 200;
pub(super) const MAX_UNFINISHED_BYTES: usize = 4 * 1024 * 1024;

/// Listener-owned canonical items not available from durable history.
#[derive(Default)]
pub(crate) struct ObservationItems {
    items: BTreeMap<(String, String), TextItem>,
    bytes: usize,
    lost_turn: Option<String>,
    next_start: u64,
    ephemeral: bool,
    pub(crate) history_unavailable: bool,
}

struct TextItem {
    start_sequence: u64,
    item: Option<ThreadItem>,
    started_at_ms: Option<i64>,
    bytes: usize,
    completed_at_ms: Option<i64>,
    is_complete: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ObservedItem {
    pub(crate) turn_id: String,
    pub(crate) item: ThreadItem,
    pub(crate) started_at_ms: Option<i64>,
    pub(crate) completed_at_ms: Option<i64>,
    pub(crate) is_complete: bool,
}

impl ObservationItems {
    pub(crate) fn new(ephemeral: bool) -> Self {
        Self {
            ephemeral,
            ..Default::default()
        }
    }
    pub(crate) fn snapshot(&self) -> Result<Vec<ObservedItem>, &'static str> {
        if self.lost_turn.is_some() || self.items.values().any(|item| item.item.is_none()) {
            return Err("unfinished text exceeds the observation checkpoint capacity");
        }
        let mut items = self.items.iter().collect::<Vec<_>>();
        items.sort_by_key(|(_, item)| item.start_sequence);
        Ok(items
            .into_iter()
            .filter_map(|((turn_id, _), item)| {
                item.item.clone().map(|value| ObservedItem {
                    turn_id: turn_id.clone(),
                    item: value,
                    started_at_ms: item.started_at_ms,
                    completed_at_ms: item.completed_at_ms,
                    is_complete: item.is_complete,
                })
            })
            .collect())
    }

    pub(crate) fn track(&mut self, turn_id: &str, event: &EventMsg) {
        match event {
            EventMsg::ItemStarted(start) => {
                let item = ThreadItem::from(start.item.clone());
                if self.ephemeral
                    || matches!(
                        item,
                        ThreadItem::AgentMessage { .. }
                            | ThreadItem::Reasoning { .. }
                            | ThreadItem::Plan { .. }
                    )
                {
                    self.retain(
                        start.turn_id.clone(),
                        item,
                        Some(start.started_at_ms),
                        None,
                        false,
                    );
                }
            }
            EventMsg::ItemCompleted(end) => {
                let key = (end.turn_id.clone(), end.item.id().to_owned());
                if self.ephemeral {
                    self.retain(
                        end.turn_id.clone(),
                        ThreadItem::from(end.item.clone()),
                        end.started_at_ms,
                        Some(end.completed_at_ms),
                        true,
                    );
                } else if let Some(item) = self.items.remove(&key) {
                    self.bytes -= item.bytes;
                }
            }
            EventMsg::AgentMessageContentDelta(delta) => self.append(
                &delta.turn_id,
                &delta.item_id,
                &delta.delta,
                TextPart::Message,
            ),
            EventMsg::PlanDelta(delta) => {
                self.append(&delta.turn_id, &delta.item_id, &delta.delta, TextPart::Plan)
            }
            EventMsg::ReasoningContentDelta(delta) => self.append(
                &delta.turn_id,
                &delta.item_id,
                &delta.delta,
                TextPart::Summary(delta.summary_index),
            ),
            EventMsg::ReasoningRawContentDelta(delta) => self.append(
                &delta.turn_id,
                &delta.item_id,
                &delta.delta,
                TextPart::Content(delta.content_index),
            ),
            EventMsg::AgentReasoningSectionBreak(part) => self.append(
                turn_id,
                &part.item_id,
                "",
                TextPart::Summary(part.summary_index),
            ),
            EventMsg::TurnComplete(_) | EventMsg::TurnAborted(_) => {
                self.items.retain(|(owner, _), item| {
                    if owner == turn_id && (!self.ephemeral || item.item.is_none()) {
                        self.bytes -= item.bytes;
                        self.history_unavailable |= self.ephemeral;
                        false
                    } else {
                        true
                    }
                });
                if self.lost_turn.as_deref() == Some(turn_id) {
                    self.history_unavailable |= self.ephemeral;
                    self.lost_turn = None;
                }
            }
            _ => {}
        }
    }

    pub(crate) fn track_changes(
        &mut self,
        changes: codex_app_server_protocol::ThreadHistoryChangeSet,
    ) {
        if !self.ephemeral {
            return;
        }
        for change in changes.changed_items {
            // Canonical ItemStarted/Completed own these payloads. Their legacy
            // mirrors can otherwise replace a multi-part item with one part.
            if matches!(
                change.item,
                ThreadItem::AgentMessage { .. }
                    | ThreadItem::Reasoning { .. }
                    | ThreadItem::Plan { .. }
                    | ThreadItem::UserMessage { .. }
            ) {
                continue;
            }
            self.retain(
                change.turn_id,
                change.item,
                change.started_at_ms,
                change.completed_at_ms,
                change.is_complete,
            );
        }
        for turn_id in changes.removed_turn_ids {
            self.items.retain(|(owner, _), item| {
                if owner == &turn_id {
                    self.bytes -= item.bytes;
                    false
                } else {
                    true
                }
            });
        }
    }

    fn retain(
        &mut self,
        turn_id: String,
        item: ThreadItem,
        started_at_ms: Option<i64>,
        completed_at_ms: Option<i64>,
        is_complete: bool,
    ) {
        let key = (turn_id.clone(), item.id().to_owned());
        let previous = self.items.remove(&key);
        if let Some(previous) = &previous {
            self.bytes -= previous.bytes;
        }
        let bytes = serde_json::to_vec(&item).map_or(MAX_UNFINISHED_BYTES + 1, |value| value.len());
        self.evict_completed(bytes, 1);
        if self.items.len() >= MAX_UNFINISHED_ITEMS
            || bytes > MAX_UNFINISHED_BYTES.saturating_sub(self.bytes)
        {
            if is_complete && self.ephemeral {
                self.history_unavailable = true;
            } else if self.items.len() < MAX_UNFINISHED_ITEMS {
                let start_sequence = self.next_start;
                self.next_start += 1;
                self.items.insert(
                    key,
                    TextItem {
                        start_sequence,
                        item: None,
                        started_at_ms,
                        bytes: 0,
                        completed_at_ms,
                        is_complete,
                    },
                );
            } else {
                self.lost_turn = Some(turn_id);
            }
            return;
        }
        let start_sequence = previous.map_or_else(
            || {
                let sequence = self.next_start;
                self.next_start += 1;
                sequence
            },
            |entry| entry.start_sequence,
        );
        self.bytes += bytes;
        self.items.insert(
            key,
            TextItem {
                start_sequence,
                item: Some(item),
                started_at_ms,
                bytes,
                completed_at_ms,
                is_complete,
            },
        );
    }

    fn evict_completed(&mut self, added_bytes: usize, added_items: usize) {
        while self.ephemeral
            && (added_bytes > MAX_UNFINISHED_BYTES.saturating_sub(self.bytes)
                || self.items.len() + added_items > MAX_UNFINISHED_ITEMS)
        {
            let oldest = self
                .items
                .iter()
                .filter(|(_, entry)| entry.is_complete)
                .min_by_key(|(_, entry)| entry.start_sequence)
                .map(|(key, _)| key.clone());
            let Some(key) = oldest else {
                break;
            };
            if let Some(entry) = self.items.remove(&key) {
                self.bytes -= entry.bytes;
                self.history_unavailable = true;
            }
        }
    }

    fn append(&mut self, turn_id: &str, item_id: &str, delta: &str, part: TextPart) {
        self.evict_completed(delta.len() + 8, 0);
        let Some(entry) = self
            .items
            .get_mut(&(turn_id.to_owned(), item_id.to_owned()))
        else {
            self.lost_turn = Some(turn_id.to_owned());
            return;
        };
        let Some(item) = entry.item.as_mut() else {
            return;
        };
        // Reserve section overhead as well as UTF-8 bytes before growing either vector.
        let added = delta.len()
            + match part {
                TextPart::Summary(_) | TextPart::Content(_) => 8,
                _ => 0,
            };
        let target = match (item, part) {
            (ThreadItem::AgentMessage { text, .. }, TextPart::Message)
            | (ThreadItem::Plan { text, .. }, TextPart::Plan) => Some(text),
            (ThreadItem::Reasoning { summary, .. }, TextPart::Summary(index)) => {
                section(summary, index)
            }
            (ThreadItem::Reasoning { content, .. }, TextPart::Content(index)) => {
                section(content, index)
            }
            _ => None,
        };
        if let Some(target) = target
            && added <= MAX_UNFINISHED_BYTES.saturating_sub(self.bytes)
        {
            target.push_str(delta);
            entry.bytes += added;
            self.bytes += added;
        } else {
            self.bytes -= entry.bytes;
            entry.bytes = 0;
            entry.item = None;
        }
    }
}

#[derive(Clone, Copy)]
enum TextPart {
    Message,
    Plan,
    Summary(i64),
    Content(i64),
}

fn section(parts: &mut Vec<String>, index: i64) -> Option<&mut String> {
    let index = usize::try_from(index)
        .ok()
        .filter(|index| *index < MAX_UNFINISHED_ITEMS)?;
    if parts.len() <= index {
        parts.resize_with(index + 1, String::new);
    }
    parts.get_mut(index)
}

#[cfg(test)]
#[path = "text_tests.rs"]
mod tests;
