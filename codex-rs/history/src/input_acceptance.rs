// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use codex_protocol::ThreadId;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

pub const MAX_IDENTIFIED_INPUTS: usize = 65_536;
pub const MAX_INPUT_ID_BYTES: usize = 512;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InputAcceptance {
    pub thread_id: ThreadId,
    pub client_id: String,
    pub fingerprint: String,
    pub kind: InputAcceptanceKind,
    pub turn_id: String,
    pub root_turn_id: String,
    pub request: serde_json::Value,
    pub context: codex_protocol::protocol::TurnContextItem,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InputAcceptanceKind {
    Started,
    Steered,
}

impl InputAcceptance {
    pub fn submission(&self) -> codex_protocol::turn_input::TurnInputSubmission {
        match self.kind {
            InputAcceptanceKind::Started => {
                codex_protocol::turn_input::TurnInputSubmission::Started {
                    turn_id: self.turn_id.clone(),
                    root_turn_id: self.root_turn_id.clone(),
                }
            }
            InputAcceptanceKind::Steered => {
                codex_protocol::turn_input::TurnInputSubmission::Steered {
                    turn_id: self.turn_id.clone(),
                    root_turn_id: self.root_turn_id.clone(),
                }
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum InputAcceptanceEvent {
    Accepted(InputAcceptance),
    Recorded {
        thread_id: ThreadId,
        client_id: String,
    },
    Rejected {
        thread_id: ThreadId,
        client_id: String,
    },
    Suspended {
        thread_id: ThreadId,
        turn_id: String,
    },
    Fenced {
        thread_id: ThreadId,
    },
    Finished {
        thread_id: ThreadId,
        turn_id: String,
    },
    Stopped {
        thread_id: ThreadId,
        turn_id: String,
    },
}

pub struct InputAcceptanceState<'a> {
    pub acceptance: &'a InputAcceptance,
    pub recorded: bool,
    pub rejected: bool,
    pub terminal: bool,
    pub suspended: bool,
    pub finished: bool,
    pub stopped: bool,
}

pub struct InputAcceptanceEvidence {
    pub turn_id: String,
    pub root_turn_id: String,
    pub fingerprint: String,
    pub recorded: bool,
    pub rejected: bool,
    pub terminal: bool,
    pub suspended: bool,
    pub finished: bool,
    pub stopped: bool,
}

impl InputAcceptanceState<'_> {
    pub fn evidence(&self) -> InputAcceptanceEvidence {
        InputAcceptanceEvidence {
            turn_id: self.acceptance.turn_id.clone(),
            root_turn_id: self.acceptance.root_turn_id.clone(),
            fingerprint: self.acceptance.fingerprint.clone(),
            recorded: self.recorded,
            rejected: self.rejected,
            terminal: self.terminal,
            suspended: self.suspended,
            finished: self.finished,
            stopped: self.stopped,
        }
    }
}

#[derive(Default)]
struct TurnAcceptanceState {
    terminal: bool,
    suspended: bool,
    finished: bool,
    stopped: bool,
}

pub fn input_acceptance_states(
    items: &[crate::RolloutItem],
    thread_id: ThreadId,
) -> Result<Vec<InputAcceptanceState<'_>>, String> {
    use crate::RolloutItem;
    use codex_protocol::items::TurnItem;
    use codex_protocol::protocol::EventMsg;
    use codex_protocol::protocol::TurnAbortReason;
    let mut accepted_inputs: Vec<(&InputAcceptance, usize)> = Vec::new();
    let mut client_indices = std::collections::HashMap::<&str, usize>::new();
    let mut recorded = std::collections::HashMap::new();
    let mut rejected = std::collections::HashMap::new();
    let mut turns = std::collections::HashMap::<&str, TurnAcceptanceState>::new();
    let mut latest_fence = None;
    let mut latest_start: Option<(&str, usize)> = None;
    let mut previous_distinct_start: Option<(&str, usize)> = None;
    for (index, item) in items.iter().enumerate() {
        match item {
            RolloutItem::InputAcceptance(InputAcceptanceEvent::Accepted(accepted))
                if accepted.thread_id == thread_id =>
            {
                if let Some(previous_index) = client_indices.get(accepted.client_id.as_str()) {
                    let previous: &InputAcceptance = accepted_inputs[*previous_index].0;
                    if previous.fingerprint != accepted.fingerprint
                        || previous.submission() != accepted.submission()
                    {
                        return Err("conflicting persisted input acceptance".to_string());
                    }
                } else {
                    if rejected.contains_key(accepted.client_id.as_str()) {
                        return Err("identified input was accepted after its durable Stop fence"
                            .to_string());
                    }
                    client_indices.insert(accepted.client_id.as_str(), accepted_inputs.len());
                    accepted_inputs.push((accepted, index));
                }
            }
            RolloutItem::InputAcceptance(InputAcceptanceEvent::Recorded {
                thread_id: owner,
                client_id,
            }) if *owner == thread_id => {
                recorded.insert(client_id.as_str(), index);
            }
            RolloutItem::InputAcceptance(InputAcceptanceEvent::Rejected {
                thread_id: owner,
                client_id,
            }) if *owner == thread_id => {
                rejected.insert(client_id.as_str(), index);
            }
            RolloutItem::InputAcceptance(InputAcceptanceEvent::Suspended {
                thread_id: owner,
                turn_id,
            }) if *owner == thread_id => {
                turns.entry(turn_id).or_default().suspended = true;
            }
            RolloutItem::InputAcceptance(InputAcceptanceEvent::Finished {
                thread_id: owner,
                turn_id,
            }) if *owner == thread_id => {
                let turn = turns.entry(turn_id).or_default();
                turn.terminal = true;
                turn.finished = true;
            }
            RolloutItem::InputAcceptance(InputAcceptanceEvent::Stopped {
                thread_id: owner,
                turn_id,
            }) if *owner == thread_id => {
                let turn = turns.entry(turn_id).or_default();
                turn.terminal = true;
                turn.stopped = true;
            }
            RolloutItem::InputAcceptance(InputAcceptanceEvent::Fenced { thread_id: owner })
                if *owner == thread_id =>
            {
                latest_fence = Some(index)
            }
            RolloutItem::EventMsg(EventMsg::UserMessage(item)) => {
                if let Some(client_id) = &item.client_id {
                    recorded.insert(client_id.as_str(), index);
                }
            }
            RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) => {
                if let TurnItem::UserMessage(item) = &event.item
                    && let Some(client_id) = &item.client_id
                {
                    recorded.insert(client_id.as_str(), index);
                }
            }
            RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => {
                turns.entry(&event.turn_id).or_default().suspended = false;
                if latest_start.is_some_and(|(turn_id, _)| turn_id != event.turn_id) {
                    previous_distinct_start = latest_start;
                }
                latest_start = Some((&event.turn_id, index));
            }
            RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => {
                let turn = turns.entry(&event.turn_id).or_default();
                turn.terminal = true;
                turn.finished = true;
            }
            RolloutItem::EventMsg(EventMsg::TurnAborted(event)) => {
                if let Some(turn_id) = &event.turn_id {
                    let turn = turns.entry(turn_id).or_default();
                    if !turn.suspended
                        || event.reason != TurnAbortReason::Interrupted
                        || event.error.is_some()
                    {
                        turn.terminal = true;
                    }
                } else {
                    latest_fence = Some(index);
                }
            }
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(_)) => latest_fence = Some(index),
            _ => {}
        }
    }
    Ok(accepted_inputs
        .into_iter()
        .map(|(acceptance, index)| {
            let turn = turns.get(acceptance.turn_id.as_str());
            let later_other_start = latest_start
                .filter(|(turn_id, _)| *turn_id != acceptance.turn_id)
                .or(previous_distinct_start.filter(|(turn_id, _)| *turn_id != acceptance.turn_id));
            let terminal = turn.is_some_and(|turn| turn.terminal)
                || latest_fence.is_some_and(|fence| fence > index)
                || later_other_start.is_some_and(|(_, started)| started > index);
            InputAcceptanceState {
                recorded: recorded
                    .get(acceptance.client_id.as_str())
                    .is_some_and(|recorded| *recorded > index),
                rejected: rejected
                    .get(acceptance.client_id.as_str())
                    .is_some_and(|rejected| *rejected > index),
                terminal,
                suspended: !terminal && turn.is_some_and(|turn| turn.suspended),
                finished: turn.is_some_and(|turn| turn.finished),
                stopped: turn.is_some_and(|turn| turn.stopped),
                acceptance,
            }
        })
        .collect())
}
