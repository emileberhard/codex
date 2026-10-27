// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use codex_protocol::ThreadId;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

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
