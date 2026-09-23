use super::thread_processor::ThreadRequestProcessor;
use super::thread_processor::thread_read_view_error;
use crate::error_code::internal_error;
use crate::error_code::invalid_request;
use crate::outgoing_message::ConnectionRequestId;
use crate::outgoing_message::OutgoingMessageSender;
use crate::thread_state::ThreadListenerCommand;
use crate::thread_state::ThreadState;
use crate::thread_state::ThreadStateManager;
use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadItemEntry;
use codex_app_server_protocol::ThreadObservationCheckpoint;
use codex_app_server_protocol::ThreadSubscribeParams;
use codex_app_server_protocol::ThreadSubscribeResponse;
use codex_app_server_protocol::ThreadSubscriptionCloseParams;
use codex_app_server_protocol::ThreadSubscriptionCloseResponse;
use codex_core::CodexThread;
use codex_protocol::ThreadId;
use codex_thread_store::ThreadObservationCursors;
use codex_thread_store::ThreadStore;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

impl ThreadRequestProcessor {
    pub(crate) async fn thread_subscribe(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadSubscribeParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        Box::pin(self.thread_subscribe_inner(request_id, params)).await
    }

    async fn thread_subscribe_inner(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadSubscribeParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;
        let Ok(conversation) = self.thread_manager.get_thread(thread_id).await else {
            return Ok(Some(ThreadSubscribeResponse::NotLoaded.into()));
        };
        let thread = self
            .read_thread_view(thread_id, false)
            .await
            .map_err(thread_read_view_error)?;
        let state = self.thread_state_manager.thread_state(thread_id).await;
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        {
            let state = state.lock().await;
            if !state.listener_matches(&conversation) {
                return Err(closing_error());
            }
            let sender = state.listener_command_tx().ok_or_else(closing_error)?;
            sender
                .send(ThreadListenerCommand::SendThreadSubscribeResponse {
                    request_id,
                    thread: Box::new(thread),
                    store: Arc::clone(&self.thread_store),
                    completion_tx,
                })
                .map_err(|_| closing_error())?;
        }
        #[cfg(debug_assertions)]
        crate::thread_observation::test_barrier("subscribe", thread_id).await;
        let _ = completion_rx.await;
        Ok(None)
    }

    pub(crate) async fn thread_subscription_close(
        &self,
        request_id: &ConnectionRequestId,
        params: ThreadSubscriptionCloseParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        self.thread_state_manager
            .close_observer(request_id.connection_id, &params.subscription_id)
            .await;
        Ok(Some(ThreadSubscriptionCloseResponse {}.into()))
    }
}

fn closing_error() -> JSONRPCErrorError {
    JSONRPCErrorError {
        code: -32002,
        message: "thread is closing; retry observation after closure".into(),
        data: Some(serde_json::json!({"reason":"threadClosing"})),
    }
}

#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "Unload and listener fences must cover membership replacement and its acknowledgement atomically."
)]
pub(super) async fn send_subscribe_response(
    thread_id: ThreadId,
    conversation: &Arc<CodexThread>,
    manager: &ThreadStateManager,
    state: &Arc<Mutex<ThreadState>>,
    outgoing: &Arc<OutgoingMessageSender>,
    pending_unloads: &Arc<Mutex<HashSet<ThreadId>>>,
    request_id: ConnectionRequestId,
    mut thread: Thread,
    store: &dyn ThreadStore,
) {
    let result = async {
        // This command runs between whole listener events. Core may already have
        // persisted queued events, so completed checkpoint versions dominate the
        // unfinished listener prefixes when clients merge the bounded pages.
        let (live_items, history_unavailable, active_turn, listener_generation) = {
            let baseline = state.lock().await;
            if !baseline.listener_matches(conversation) {
                return Err(closing_error());
            }
            let live_items = baseline
                .observation_items
                .snapshot()
                .map_err(|message| JSONRPCErrorError {
                    code: -32003,
                    message: message.into(),
                    data: Some(serde_json::json!({"reason":"observationCapacity"})),
                })?
                .into_iter()
                .map(|item| ThreadItemEntry {
                    started_at_ordinal: None,
                    started_at_ms: item.started_at_ms,
                    completed_at_ms: item.completed_at_ms,
                    is_complete: Some(item.is_complete),
                    turn_id: item.turn_id,
                    item: item.item,
                })
                .collect();
            let history_unavailable = baseline.observation_items.history_unavailable;
            let active_turn = baseline.active_turn_metadata();
            let listener_generation = baseline.listener_generation;
            (
                live_items,
                history_unavailable,
                active_turn,
                listener_generation,
            )
        };
        let cursors = if conversation.rollout_path().is_none() {
            ThreadObservationCursors {
                turns: None,
                items: None,
            }
        } else {
            match store.observation_cursors(thread_id).await {
                Ok(cursors) => cursors,
                Err(err) => {
                    return Err(internal_error(format!(
                        "failed to capture observation history: {err}"
                    )));
                }
            }
        };
        thread.turns.clear();
        let checkpoint = ThreadObservationCheckpoint {
            turns_backwards_cursor: cursors.turns,
            items_backwards_cursor: cursors.items,
            active_turn,
            live_items,
            history_unavailable,
        };
        let bytes = serde_json::to_vec(&checkpoint)
            .map_err(|err| internal_error(err.to_string()))?
            .len();
        if bytes > 24 * 1024 * 1024 {
            return Err(JSONRPCErrorError {
                code: -32003,
                message: "observation checkpoint exceeds response capacity".into(),
                data: None,
            });
        }
        let fence = pending_unloads.lock().await;
        if fence.contains(&thread_id) {
            return Err(closing_error());
        }
        let current = state.lock().await;
        if current.listener_generation != listener_generation
            || !current.listener_matches(conversation)
        {
            return Err(closing_error());
        }
        let Some(subscription_id) = manager
            .replace_observer(thread_id, request_id.connection_id, state)
            .await
        else {
            return Err(closing_error());
        };
        // Membership and acknowledgement happen before the listener consumes the
        // next queued Core event. Passive membership never replays server requests.
        outgoing
            .send_response(
                request_id.clone(),
                ThreadSubscribeResponse::Subscribed {
                    subscription_id,
                    thread: Box::new(thread),
                    checkpoint: Box::new(checkpoint),
                },
            )
            .await;
        drop(current);
        drop(fence);
        Ok(())
    }
    .await;
    if let Err(error) = result {
        outgoing.send_error(request_id, error).await;
    }
}
