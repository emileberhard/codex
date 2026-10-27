// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::*;

#[derive(Debug, PartialEq)]
enum InputStopAction {
    Interrupt(String),
    Fence,
    Unavailable,
}

impl InputAcceptances {
    fn stop_action(&self, client_id: &str) -> InputStopAction {
        match self.entries.get(client_id) {
            Some(entry) => match entry.turn_id() {
                Some(turn_id) => InputStopAction::Interrupt(turn_id.to_owned()),
                None => InputStopAction::Unavailable,
            },
            None if self.entries.len() < MAX_ACCEPTED_INPUTS => InputStopAction::Fence,
            None => InputStopAction::Unavailable,
        }
    }
}

impl crate::CodexThread {
    pub async fn stop_identified_input(
        &self,
        client_id: &str,
    ) -> CodexResult<Option<codex_history::InputAcceptanceEvidence>> {
        if client_id.is_empty() || client_id.len() > MAX_CLIENT_ID_BYTES {
            return Err(CodexErr::InvalidRequest(format!(
                "client user message id must contain 1 to {MAX_CLIENT_ID_BYTES} bytes"
            )));
        }
        let session = &self.session;
        let turn_id = {
            let _admission = session.input_queue.acceptance_submission.lock().await;
            let live = session
                .live_thread_for_persistence("stop identified input")
                .map_err(|error| CodexErr::Fatal(error.to_string()))?;
            live.persist_durable()
                .await
                .map_err(|error| CodexErr::Fatal(error.to_string()))?;
            if !session.load_input_acceptances().await? {
                return Err(CodexErr::InvalidRequest(
                    "thread store does not support identified input fences".to_string(),
                ));
            }
            let action = session
                .input_queue
                .acceptances
                .lock()
                .await
                .stop_action(client_id);
            let turn_id = match action {
                InputStopAction::Interrupt(turn_id) => Some(turn_id),
                InputStopAction::Fence => {
                    live.append_items(&[RolloutItem::InputAcceptance(
                        InputAcceptanceEvent::Rejected {
                            thread_id: session.thread_id,
                            client_id: client_id.to_owned(),
                        },
                    )])
                    .await
                    .map_err(|error| CodexErr::Fatal(error.to_string()))?;
                    session
                        .input_queue
                        .acceptances
                        .lock()
                        .await
                        .entries
                        .insert(client_id.to_owned(), AcceptedInput::Rejected);
                    None
                }
                InputStopAction::Unavailable => None,
            };
            if turn_id.is_none() {
                live.persist_durable()
                    .await
                    .map_err(|error| CodexErr::Fatal(error.to_string()))?;
            }
            turn_id
        };
        if let Some(turn_id) = turn_id {
            self.interrupt_accepted_turn(&turn_id).await?;
            self.read_input_acceptance(client_id).await
        } else {
            Ok(None)
        }
    }

    pub async fn read_input_acceptance(
        &self,
        client_id: &str,
    ) -> CodexResult<Option<codex_history::InputAcceptanceEvidence>> {
        let session = &self.session;
        let _admission = session.input_queue.acceptance_submission.lock().await;
        let live = session
            .live_thread_for_persistence("read input acceptance")
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        live.flush()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        let history = match live.load_input_acceptance_history().await {
            Ok(Some(history)) => history,
            Err(codex_thread_store::ThreadStoreError::ThreadNotFound { .. }) => return Ok(None),
            Ok(None) => {
                return Err(CodexErr::InvalidRequest(
                    "thread store does not support input acceptance reads".to_string(),
                ));
            }
            Err(error) => return Err(CodexErr::Fatal(error.to_string())),
        };
        let evidence = input_acceptance_states(&history.items, session.thread_id)
            .map_err(CodexErr::Fatal)?
            .into_iter()
            .find(|state| state.acceptance.client_id == client_id)
            .map(|state| state.evidence());
        if evidence.is_some() {
            live.persist_durable()
                .await
                .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        }
        Ok(evidence)
    }

    pub async fn prepare_accepted_input_restart(&self, turn_id: &str) -> CodexResult<()> {
        self.session.prepare_accepted_input_restart(turn_id).await
    }

    pub async fn interrupt_accepted_turn(&self, turn_id: &str) -> CodexResult<bool> {
        let session = &self.session;
        let _admission = session.input_queue.acceptance_submission.lock().await;
        let Some(live) = session.live_thread() else {
            return Ok(false);
        };
        live.flush()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        let history = match live.load_input_acceptance_history().await {
            Ok(Some(history)) => history,
            Ok(None) => return Ok(false),
            Err(codex_thread_store::ThreadStoreError::ThreadNotFound { .. }) => return Ok(false),
            Err(error) => return Err(CodexErr::Fatal(error.to_string())),
        };
        if !history.items.iter().any(|item| {
            matches!(item,
                RolloutItem::InputAcceptance(InputAcceptanceEvent::Accepted(accepted))
                    if accepted.thread_id == session.thread_id && accepted.turn_id == turn_id
            )
        }) {
            return Ok(false);
        }
        if input_acceptance_states(&history.items, session.thread_id)
            .map_err(CodexErr::Fatal)?
            .iter()
            .any(|state| state.acceptance.turn_id == turn_id && state.terminal)
        {
            return Ok(true);
        }
        live.append_items(&[RolloutItem::InputAcceptance(
            InputAcceptanceEvent::Stopped {
                thread_id: session.thread_id,
                turn_id: turn_id.to_string(),
            },
        )])
        .await
        .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        live.persist_durable()
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        if !session
            .abort_turn_if_active(
                turn_id,
                codex_protocol::protocol::TurnAbortReason::Interrupted,
                /*error*/ None,
            )
            .await
        {
            let active = session.active_turn.lock().await;
            if active.is_none() {
                session.mark_interrupted();
                live.append_items(&[RolloutItem::EventMsg(EventMsg::TurnAborted(
                    codex_protocol::protocol::TurnAbortedEvent {
                        turn_id: Some(turn_id.to_string()),
                        root_turn_id: None,
                        reason: codex_protocol::protocol::TurnAbortReason::Interrupted,
                        error: None,
                        started_at: None,
                        completed_at: None,
                        duration_ms: None,
                    },
                ))])
                .await
                .map_err(|error| CodexErr::Fatal(error.to_string()))?;
                live.persist_durable()
                    .await
                    .map_err(|error| CodexErr::Fatal(error.to_string()))?;
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
