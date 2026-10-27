// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::*;
use codex_app_server_protocol::InputAcceptanceFact;
use codex_app_server_protocol::ThreadInputAcceptance;
use codex_app_server_protocol::ThreadInputAcceptanceReadParams;
use codex_app_server_protocol::ThreadInputAcceptanceReadResponse;
use codex_app_server_protocol::ThreadInputStopParams;
use codex_app_server_protocol::ThreadInputStopResponse;
use codex_history::InputAcceptanceEvidence;
use codex_history::input_acceptance_states;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::ThreadStoreError;

impl ThreadRequestProcessor {
    pub(crate) async fn thread_input_stop(
        &self,
        params: ThreadInputStopParams,
    ) -> Result<ThreadInputStopResponse, JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|error| invalid_request(format!("invalid thread id: {error}")))?;
        if params.client_user_message_id.is_empty() || params.client_user_message_id.len() > 512 {
            return Err(invalid_request(
                "client user message id must contain 1 to 512 bytes".to_string(),
            ));
        }
        let evidence = if let Ok(thread) = self.thread_manager.get_thread(thread_id).await {
            thread
                .stop_identified_input(&params.client_user_message_id)
                .await
                .map_err(|error| {
                    internal_error(format!("failed to fence identified input: {error}"))
                })?
        } else {
            self.thread_store
                .stop_unloaded_identified_input(thread_id, params.client_user_message_id)
                .await
                .map_err(|error| {
                    internal_error(format!("failed to fence unloaded input: {error}"))
                })?
        };
        Ok(ThreadInputStopResponse {
            acceptance: evidence.map(wire_evidence),
        })
    }

    pub(crate) async fn thread_input_acceptance_read(
        &self,
        params: ThreadInputAcceptanceReadParams,
    ) -> Result<ThreadInputAcceptanceReadResponse, JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|error| invalid_request(format!("invalid thread id: {error}")))?;
        if params.client_user_message_id.is_empty() || params.client_user_message_id.len() > 512 {
            return Err(invalid_request(
                "client user message id must contain 1 to 512 bytes".to_string(),
            ));
        }
        let evidence = if let Ok(thread) = self.thread_manager.get_thread(thread_id).await {
            thread
                .read_input_acceptance(&params.client_user_message_id)
                .await
                .map_err(|error| {
                    internal_error(format!("failed to read input acceptance: {error}"))
                })?
        } else {
            match self
                .thread_store
                .load_input_acceptance_history(LoadThreadHistoryParams {
                    thread_id,
                    include_archived: true,
                })
                .await
            {
                Ok(Some(history)) => input_acceptance_states(&history.items, thread_id)
                    .map_err(internal_error)?
                    .into_iter()
                    .find(|state| state.acceptance.client_id == params.client_user_message_id)
                    .map(|state| state.evidence()),
                Err(ThreadStoreError::ThreadNotFound { .. }) => None,
                Ok(None) => {
                    return Err(internal_error(
                        "thread store does not support input acceptance reads".to_string(),
                    ));
                }
                Err(error) => {
                    return Err(internal_error(format!(
                        "failed to read input acceptance: {error}"
                    )));
                }
            }
        };
        Ok(ThreadInputAcceptanceReadResponse {
            acceptance: evidence.map(wire_evidence),
        })
    }
}

fn wire_evidence(evidence: InputAcceptanceEvidence) -> ThreadInputAcceptance {
    let facts = [
        (evidence.recorded, InputAcceptanceFact::Recorded),
        (evidence.rejected, InputAcceptanceFact::Rejected),
        (evidence.stopped, InputAcceptanceFact::Stopped),
        (evidence.finished, InputAcceptanceFact::Finished),
        (evidence.terminal, InputAcceptanceFact::Terminal),
        (evidence.suspended, InputAcceptanceFact::Suspended),
    ]
    .into_iter()
    .filter_map(|(present, fact)| present.then_some(fact))
    .collect();
    ThreadInputAcceptance {
        turn_id: evidence.turn_id,
        root_turn_id: evidence.root_turn_id,
        fingerprint: evidence.fingerprint,
        facts,
    }
}
