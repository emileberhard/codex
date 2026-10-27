// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::session::Session;
use super::turn_context::TurnContext;
use codex_history::InputAcceptance;
use codex_history::InputAcceptanceEvent;
use codex_history::InputAcceptanceKind;
use codex_history::RolloutItem;
use codex_history::input_acceptance_states;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::EventMsg;
use codex_protocol::turn_input::TurnInput;
use codex_protocol::turn_input::TurnInputRequest;
use codex_protocol::turn_input::TurnInputSubmission;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashMap;

mod control;
mod request;

use request::DurableInputRequest;
use request::restore_settings;
use request::settings_fingerprint;

const MAX_ACCEPTED_INPUTS: usize = codex_history::MAX_IDENTIFIED_INPUTS;
const MAX_ACCEPTANCE_BYTES: usize = 32 * 1024 * 1024;
const MAX_CLIENT_ID_BYTES: usize = codex_history::MAX_INPUT_ID_BYTES;

#[derive(Default)]
enum AcceptanceHistoryState {
    #[default]
    Unloaded,
    Unsupported,
    Loaded,
}

#[derive(Default)]
pub(super) struct InputAcceptances {
    history_state: AcceptanceHistoryState,
    suspending_turns: std::collections::HashSet<String>,
    live_inputs: std::collections::HashSet<String>,
    entries: HashMap<String, AcceptedInput>,
}

enum AcceptedInput {
    Accepted {
        fingerprint: String,
        submission: TurnInputSubmission,
    },
    Rejected,
}

impl AcceptedInput {
    fn turn_id(&self) -> Option<&str> {
        match self {
            Self::Accepted { submission, .. } => match submission {
                TurnInputSubmission::Started { turn_id, .. }
                | TurnInputSubmission::Steered { turn_id, .. } => Some(turn_id),
                TurnInputSubmission::NotSubmitted { .. } => None,
            },
            Self::Rejected => None,
        }
    }
}

pub(super) struct PreparedInputAcceptance {
    pub(super) client_id: String,
    fingerprint: String,
    request: DurableInputRequest,
}

impl PreparedInputAcceptance {
    pub(super) fn prepare(request: &TurnInputRequest) -> CodexResult<Option<Self>> {
        let TurnInput::UserInput {
            client_id: Some(client_id),
            ..
        } = &request.input
        else {
            return Ok(None);
        };
        if client_id.is_empty() || client_id.len() > MAX_CLIENT_ID_BYTES {
            return Err(CodexErr::InvalidRequest(format!(
                "client user message id must contain 1 to {MAX_CLIENT_ID_BYTES} bytes"
            )));
        }
        let settings = settings_fingerprint(&request.thread_settings)?;
        let request = DurableInputRequest {
            input: request.input.clone(),
            settings,
            start: request.start.clone(),
            additional_context: request.additional_context.clone(),
            responsesapi_client_metadata: request.responsesapi_client_metadata.clone(),
        };
        let mut canonical = json!({"request": request});
        canonical.sort_all_objects();
        let encoded = serde_json::to_vec(&canonical)
            .map_err(|error| CodexErr::InvalidRequest(error.to_string()))?;
        if encoded.len() > MAX_ACCEPTANCE_BYTES {
            return Err(CodexErr::InvalidRequest(format!(
                "identified user input exceeds {MAX_ACCEPTANCE_BYTES} acceptance bytes"
            )));
        }
        Ok(Some(Self {
            client_id: client_id.clone(),
            fingerprint: format!("{:x}", Sha256::digest(encoded)),
            request,
        }))
    }

    pub(super) async fn previous(
        &self,
        session: &Session,
    ) -> CodexResult<Option<TurnInputSubmission>> {
        if matches!(
            session.input_queue.acceptances.lock().await.history_state,
            AcceptanceHistoryState::Unloaded
        ) {
            let live = session
                .live_thread_for_persistence("read input acceptance")
                .map_err(|error| CodexErr::Fatal(error.to_string()))?;
            live.persist_durable()
                .await
                .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        }
        if !session.load_input_acceptances().await? {
            return Err(CodexErr::InvalidRequest(
                "thread store does not support identified input acceptance".to_string(),
            ));
        }
        let acceptances = session.input_queue.acceptances.lock().await;
        if let Some(AcceptedInput::Accepted {
            fingerprint,
            submission,
        }) = acceptances.entries.get(&self.client_id)
        {
            if fingerprint != &self.fingerprint {
                return Err(CodexErr::InvalidRequest(format!(
                    "client user message id {} already identifies different input",
                    self.client_id
                )));
            }
            return Ok(Some(submission.clone()));
        }
        if matches!(
            acceptances.entries.get(&self.client_id),
            Some(AcceptedInput::Rejected)
        ) {
            return Err(CodexErr::InvalidRequest(
                "this identified input was durably stopped before acceptance".to_string(),
            ));
        }
        if acceptances.entries.len() >= MAX_ACCEPTED_INPUTS {
            return Err(CodexErr::InvalidRequest(format!(
                "thread has reached its {MAX_ACCEPTED_INPUTS} identified input limit"
            )));
        }
        Ok(None)
    }

    pub(super) async fn persist(
        &self,
        session: &Session,
        context: &TurnContext,
        submission: TurnInputSubmission,
    ) -> CodexResult<()> {
        let live = session
            .live_thread_for_persistence("accept identified user input")
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        let (kind, turn_id, root_turn_id) = match &submission {
            TurnInputSubmission::Started {
                turn_id,
                root_turn_id,
            } => (
                InputAcceptanceKind::Started,
                turn_id.clone(),
                root_turn_id.clone(),
            ),
            TurnInputSubmission::Steered {
                turn_id,
                root_turn_id,
            } => (
                InputAcceptanceKind::Steered,
                turn_id.clone(),
                root_turn_id.clone(),
            ),
            TurnInputSubmission::NotSubmitted { .. } => {
                return Err(CodexErr::Fatal(
                    "cannot persist rejected input as accepted".to_string(),
                ));
            }
        };
        let accepted = InputAcceptance {
            thread_id: session.thread_id,
            client_id: self.client_id.clone(),
            fingerprint: self.fingerprint.clone(),
            kind,
            turn_id,
            root_turn_id,
            request: serde_json::to_value(&self.request)
                .map_err(|error| CodexErr::Fatal(error.to_string()))?,
            context: context.to_turn_context_item(),
        };
        if serde_json::to_vec(&accepted)
            .map_err(|error| CodexErr::Fatal(error.to_string()))?
            .len()
            > MAX_ACCEPTANCE_BYTES
        {
            return Err(CodexErr::InvalidRequest(format!(
                "identified input and captured context exceed {MAX_ACCEPTANCE_BYTES} acceptance bytes"
            )));
        }
        live.append_items(&[RolloutItem::InputAcceptance(
            InputAcceptanceEvent::Accepted(accepted),
        )])
        .await
        .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        session.input_queue.acceptances.lock().await.entries.insert(
            self.client_id.clone(),
            AcceptedInput::Accepted {
                fingerprint: self.fingerprint.clone(),
                submission,
            },
        );
        live.persist_durable()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))
    }
}

impl Session {
    pub(super) async fn remember_input_acceptances(&self, history: &[RolloutItem]) {
        let mut acceptances = self.input_queue.acceptances.lock().await;
        for item in history {
            if let RolloutItem::InputAcceptance(InputAcceptanceEvent::Accepted(accepted)) = item
                && accepted.thread_id == self.thread_id
            {
                acceptances
                    .entries
                    .entry(accepted.client_id.clone())
                    .or_insert_with(|| AcceptedInput::Accepted {
                        fingerprint: accepted.fingerprint.clone(),
                        submission: accepted.submission(),
                    });
            } else if let RolloutItem::InputAcceptance(InputAcceptanceEvent::Rejected {
                thread_id,
                client_id,
            }) = item
                && *thread_id == self.thread_id
            {
                acceptances
                    .entries
                    .entry(client_id.clone())
                    .or_insert(AcceptedInput::Rejected);
            }
        }
    }

    async fn load_input_acceptances(&self) -> CodexResult<bool> {
        {
            let acceptances = self.input_queue.acceptances.lock().await;
            match acceptances.history_state {
                AcceptanceHistoryState::Loaded => return Ok(true),
                AcceptanceHistoryState::Unsupported => return Ok(false),
                AcceptanceHistoryState::Unloaded => {}
            }
        }
        let Some(live) = self.live_thread() else {
            return Ok(false);
        };
        live.flush()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        let history = match live.load_input_acceptance_history().await {
            Ok(Some(history)) => history.items,
            Ok(None) => {
                self.input_queue.acceptances.lock().await.history_state =
                    AcceptanceHistoryState::Unsupported;
                return Ok(false);
            }
            Err(codex_thread_store::ThreadStoreError::ThreadNotFound { .. }) => Vec::new(),
            Err(error) => return Err(CodexErr::Fatal(error.to_string())),
        };
        let mut entries = HashMap::new();
        for item in &history {
            if let RolloutItem::InputAcceptance(InputAcceptanceEvent::Accepted(accepted)) = item
                && accepted.thread_id == self.thread_id
            {
                let submission = accepted.submission();
                if let Some(AcceptedInput::Accepted {
                    fingerprint,
                    submission: previous,
                }) = entries.get(&accepted.client_id)
                    && (fingerprint != &accepted.fingerprint || previous != &submission)
                {
                    return Err(CodexErr::Fatal(
                        "conflicting persisted input acceptance".to_string(),
                    ));
                }
                if matches!(
                    entries.get(&accepted.client_id),
                    Some(AcceptedInput::Rejected)
                ) {
                    return Err(CodexErr::Fatal(
                        "identified input was accepted after its durable Stop fence".to_string(),
                    ));
                }
                entries.insert(
                    accepted.client_id.clone(),
                    AcceptedInput::Accepted {
                        fingerprint: accepted.fingerprint.clone(),
                        submission,
                    },
                );
            } else if let RolloutItem::InputAcceptance(InputAcceptanceEvent::Rejected {
                thread_id,
                client_id,
            }) = item
                && *thread_id == self.thread_id
            {
                entries
                    .entry(client_id.clone())
                    .or_insert(AcceptedInput::Rejected);
            }
        }
        if entries.len() > MAX_ACCEPTED_INPUTS {
            return Err(CodexErr::Fatal(
                "identified input history exceeds its bound".to_string(),
            ));
        }
        let mut acceptances = self.input_queue.acceptances.lock().await;
        acceptances.entries = entries;
        acceptances.history_state = AcceptanceHistoryState::Loaded;
        Ok(true)
    }

    pub(super) async fn accepted_turn_allows_recovery(&self, turn_id: &str) -> CodexResult<bool> {
        if !self.load_input_acceptances().await? {
            return Ok(true);
        }
        let live = self
            .live_thread_for_persistence("check accepted turn recovery")
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        live.flush()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        let history = live
            .load_input_acceptance_history()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?
            .ok_or_else(|| {
                CodexErr::Fatal("identified input history became unsupported".to_string())
            })?;
        Ok(!input_acceptance_states(&history.items, self.thread_id)
            .map_err(CodexErr::Fatal)?
            .iter()
            .any(|state| state.acceptance.turn_id == turn_id && state.terminal))
    }

    pub(super) async fn prepare_accepted_input_restart(&self, turn_id: &str) -> CodexResult<()> {
        let _admission = self.input_queue.acceptance_submission.lock().await;
        if !self.load_input_acceptances().await? {
            return Ok(());
        }
        if !self
            .input_queue
            .acceptances
            .lock()
            .await
            .entries
            .values()
            .any(|entry| entry.turn_id() == Some(turn_id))
        {
            return Ok(());
        }
        {
            let active = self.active_turn.lock().await;
            if !active
                .as_ref()
                .and_then(|turn| turn.task.as_ref())
                .is_some_and(|task| task.turn_context.sub_id == turn_id)
            {
                return Ok(());
            }
        }
        let live = self
            .live_thread_for_persistence("checkpoint accepted input for restart")
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        self.input_queue
            .acceptances
            .lock()
            .await
            .suspending_turns
            .insert(turn_id.to_string());
        let result = async {
            live.append_items(&[RolloutItem::InputAcceptance(
                InputAcceptanceEvent::Suspended {
                    thread_id: self.thread_id,
                    turn_id: turn_id.to_string(),
                },
            )])
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
            live.persist_durable()
                .await
                .map_err(|error| CodexErr::Fatal(error.to_string()))
        }
        .await;
        if result.is_err() {
            self.input_queue
                .acceptances
                .lock()
                .await
                .suspending_turns
                .remove(turn_id);
        }
        result
    }

    pub(super) async fn mark_acceptance_turn_resumed(&self, turn_id: &str) {
        self.input_queue
            .acceptances
            .lock()
            .await
            .suspending_turns
            .remove(turn_id);
    }

    pub(super) async fn fence_accepted_inputs(&self) -> CodexResult<()> {
        let _admission = self.input_queue.acceptance_submission.lock().await;
        self.load_input_acceptances().await?;
        let records = self.finished_input_acceptances(None).await;
        if records.is_empty() {
            return Ok(());
        }
        let live = self
            .live_thread_for_persistence("fence identified user inputs")
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        live.append_items(&records)
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        live.persist_durable()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))
    }

    pub(super) async fn input_acceptance_lifecycle_records(
        &self,
        event: &EventMsg,
    ) -> Vec<RolloutItem> {
        let turn_id = match event {
            EventMsg::TurnComplete(event) => Some(event.turn_id.as_str()),
            EventMsg::TurnAborted(event) => {
                if event.reason == codex_protocol::protocol::TurnAbortReason::Interrupted
                    && event.error.is_none()
                    && self
                        .input_queue
                        .acceptances
                        .lock()
                        .await
                        .suspending_turns
                        .iter()
                        .any(|id| event.turn_id.as_ref() == Some(id))
                {
                    return Vec::new();
                }
                event.turn_id.as_deref()
            }
            EventMsg::ThreadRolledBack(_) => None,
            _ => return Vec::new(),
        };
        self.finished_input_acceptances(turn_id).await
    }

    async fn finished_input_acceptances(&self, turn_id: Option<&str>) -> Vec<RolloutItem> {
        let acceptances = self.input_queue.acceptances.lock().await;
        let mut turn_ids = std::collections::HashSet::new();
        for id in acceptances
            .entries
            .values()
            .filter_map(AcceptedInput::turn_id)
        {
            if turn_id.is_none_or(|turn_id| turn_id == id) {
                turn_ids.insert(id.to_owned());
            }
        }
        turn_ids
            .into_iter()
            .map(|turn_id| {
                RolloutItem::InputAcceptance(InputAcceptanceEvent::Finished {
                    thread_id: self.thread_id,
                    turn_id,
                })
            })
            .collect()
    }

    pub(super) async fn mark_input_live(&self, client_id: &str) {
        self.input_queue
            .acceptances
            .lock()
            .await
            .live_inputs
            .insert(client_id.to_string());
    }

    pub(super) async fn pending_accepted_requests(
        &self,
        turn_id: &str,
    ) -> CodexResult<Vec<TurnInputRequest>> {
        if !self.load_input_acceptances().await? {
            return Ok(Vec::new());
        }
        if !self
            .input_queue
            .acceptances
            .lock()
            .await
            .entries
            .values()
            .any(|entry| entry.turn_id() == Some(turn_id))
        {
            return Ok(Vec::new());
        }
        let live = self
            .live_thread_for_persistence("recover pending accepted inputs")
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        live.persist_durable()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        let history = live
            .load_input_acceptance_history()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?
            .ok_or_else(|| {
                CodexErr::InvalidRequest(
                    "thread store does not support identified input acceptance".to_string(),
                )
            })?;
        let pending =
            input_acceptance_states(&history.items, self.thread_id).map_err(CodexErr::Fatal)?;
        let acceptances = self.input_queue.acceptances.lock().await;
        pending
            .into_iter()
            .filter(|state| {
                state.acceptance.turn_id == turn_id
                    && !state.recorded
                    && !state.rejected
                    && !state.terminal
                    && !acceptances
                        .live_inputs
                        .contains(&state.acceptance.client_id)
            })
            .map(|state| {
                let accepted = state.acceptance;
                let request: DurableInputRequest = serde_json::from_value(accepted.request.clone())
                    .map_err(|error| CodexErr::Fatal(error.to_string()))?;
                Ok(TurnInputRequest::new(request.input)
                    .with_thread_settings(restore_settings(&request.settings)?)
                    .on_start(request.start)
                    .with_additional_context(request.additional_context)
                    .with_responses_metadata(request.responsesapi_client_metadata))
            })
            .collect()
    }

    pub(super) async fn input_acceptance_recovery(
        &self,
        client_id: &str,
    ) -> CodexResult<Option<(String, TurnInputRequest)>> {
        if self
            .input_queue
            .acceptances
            .lock()
            .await
            .live_inputs
            .contains(client_id)
            || self.state.lock().await.shutting_down
        {
            return Ok(None);
        }
        let live = self
            .live_thread_for_persistence("reconcile input acceptance")
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        live.persist_durable()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        let history = live
            .load_input_acceptance_history()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?
            .ok_or_else(|| {
                CodexErr::InvalidRequest(
                    "thread store does not support identified input acceptance".to_string(),
                )
            })?;
        let states =
            input_acceptance_states(&history.items, self.thread_id).map_err(CodexErr::Fatal)?;
        let state = states
            .into_iter()
            .find(|state| state.acceptance.client_id == client_id)
            .ok_or_else(|| CodexErr::Fatal("accepted input has no persisted record".to_string()))?;
        if state.terminal || state.rejected || state.recorded || state.suspended {
            return Ok(None);
        }
        let accepted = state.acceptance;
        let turn_id = &accepted.turn_id;
        let request: DurableInputRequest = serde_json::from_value(accepted.request.clone())
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        let mut request = TurnInputRequest::new(request.input)
            .on_start(request.start)
            .with_thread_settings(restore_settings(&request.settings)?)
            .with_additional_context(request.additional_context)
            .with_responses_metadata(request.responsesapi_client_metadata);
        request.thread_settings.model = Some(accepted.context.model.clone());
        request.thread_settings.effort = Some(accepted.context.effort.clone());
        request.thread_settings.approval_policy = Some(accepted.context.approval_policy);
        request.thread_settings.approvals_reviewer = accepted.context.approvals_reviewer;
        request.thread_settings.permission_profile = Some(accepted.context.permission_profile());
        request.thread_settings.active_permission_profile =
            accepted.context.active_permission_profile.clone();
        request.thread_settings.collaboration_mode = accepted.context.collaboration_mode.clone();
        request.start.root_turn_id = Some(accepted.root_turn_id.clone());
        request.input = TurnInput::UserInput {
            content: Vec::new(),
            client_id: None,
        };
        request.additional_context.clear();
        Ok(Some((turn_id.clone(), request)))
    }

    pub(crate) async fn record_input_disposition(
        &self,
        client_id: &str,
        rejected: bool,
    ) -> CodexResult<()> {
        if !self
            .input_queue
            .acceptances
            .lock()
            .await
            .entries
            .contains_key(client_id)
        {
            return Ok(());
        }
        let event = if rejected {
            InputAcceptanceEvent::Rejected {
                thread_id: self.thread_id,
                client_id: client_id.to_string(),
            }
        } else {
            InputAcceptanceEvent::Recorded {
                thread_id: self.thread_id,
                client_id: client_id.to_string(),
            }
        };
        let live = self
            .live_thread_for_persistence("record identified input disposition")
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        live.append_items(&[RolloutItem::InputAcceptance(event)])
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        live.persist_durable()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))
    }
}
