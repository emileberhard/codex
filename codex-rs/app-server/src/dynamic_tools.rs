// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use codex_app_server_protocol::DynamicToolCallOutputContentItem;
use codex_app_server_protocol::DynamicToolCallResponse;
use codex_core::CodexThread;
use codex_protocol::dynamic_tools::DynamicToolCallOutputContentItem as CoreDynamicToolCallOutputContentItem;
use codex_protocol::dynamic_tools::DynamicToolResponse as CoreDynamicToolResponse;
use codex_protocol::protocol::Op;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::error;

use crate::image_url::REMOTE_IMAGE_URL_ERROR;
use crate::image_url::is_remote_image_url;
use crate::outgoing_message::ClientRequestResult;
use crate::server_request_error::is_turn_transition_server_request_error;

const INVALID_AUDIO_URL_ERROR: &str = "audio URLs must use an inline data URL";
const RECOVERED_TOOL_RESPONSE_TIMEOUT: Duration = Duration::from_secs(45);
const RECOVERED_TOOL_TIMEOUT_ERROR: &str =
    "no client answered this dynamic tool after restart recovery; do not retry";

pub(crate) async fn on_call_response(
    call_id: String,
    receiver: oneshot::Receiver<ClientRequestResult>,
    conversation: Arc<CodexThread>,
    recovered_after_restart: bool,
) {
    let response = receive_call_response(
        receiver,
        recovered_after_restart.then_some(RECOVERED_TOOL_RESPONSE_TIMEOUT),
    )
    .await;
    let (response, _error) = match response {
        Some(Ok(Ok(value))) => decode_response(value),
        Some(Ok(Err(err))) if is_turn_transition_server_request_error(&err) => return,
        Some(Ok(Err(err))) => {
            error!("request failed with client error: {err:?}");
            fallback_response("dynamic tool request failed")
        }
        Some(Err(err)) => {
            error!("request failed: {err:?}");
            fallback_response("dynamic tool request failed")
        }
        None => {
            error!(
                call_id,
                timeout_seconds = RECOVERED_TOOL_RESPONSE_TIMEOUT.as_secs(),
                "dynamic tool request timed out after restart recovery"
            );
            fallback_response(RECOVERED_TOOL_TIMEOUT_ERROR)
        }
    };

    let DynamicToolCallResponse {
        content_items,
        success,
    } = response.clone();
    let core_response = CoreDynamicToolResponse {
        content_items: content_items
            .into_iter()
            .map(CoreDynamicToolCallOutputContentItem::from)
            .collect(),
        success,
    };
    if let Err(err) = conversation
        .submit(Op::DynamicToolResponse {
            id: call_id.clone(),
            response: core_response,
        })
        .await
    {
        error!("failed to submit DynamicToolResponse: {err}");
    }
}

async fn receive_call_response(
    receiver: oneshot::Receiver<ClientRequestResult>,
    timeout: Option<Duration>,
) -> Option<Result<ClientRequestResult, oneshot::error::RecvError>> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, receiver).await.ok(),
        None => Some(receiver.await),
    }
}

fn decode_response(value: serde_json::Value) -> (DynamicToolCallResponse, Option<String>) {
    match serde_json::from_value::<DynamicToolCallResponse>(value) {
        Ok(response)
            if response.content_items.iter().any(|item| {
                matches!(
                    item,
                    DynamicToolCallOutputContentItem::InputImage { image_url }
                        if is_remote_image_url(image_url)
                )
            }) =>
        {
            error!(
                message = REMOTE_IMAGE_URL_ERROR,
                "dynamic tool response was invalid"
            );
            fallback_response(REMOTE_IMAGE_URL_ERROR)
        }
        Ok(response)
            if response.content_items.iter().any(|item| {
                matches!(
                    item,
                    DynamicToolCallOutputContentItem::InputAudio { audio_url }
                        if !audio_url
                            .get(.."data:".len())
                            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("data:"))
                )
            }) =>
        {
            error!(
                message = INVALID_AUDIO_URL_ERROR,
                "dynamic tool response was invalid"
            );
            fallback_response(INVALID_AUDIO_URL_ERROR)
        }
        Ok(response) => (response, None),
        Err(err) => {
            error!("failed to deserialize DynamicToolCallResponse: {err}");
            fallback_response("dynamic tool response was invalid")
        }
    }
}

fn fallback_response(message: &str) -> (DynamicToolCallResponse, Option<String>) {
    (
        DynamicToolCallResponse {
            content_items: vec![DynamicToolCallOutputContentItem::InputText {
                text: message.to_string(),
            }],
            success: false,
        },
        Some(message.to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn recovered_call_stops_waiting_when_its_responder_never_returns() {
        let (_sender, receiver) = oneshot::channel();

        let response = receive_call_response(receiver, Some(Duration::from_millis(1))).await;

        assert!(response.is_none());
    }

    #[tokio::test]
    async fn ordinary_call_keeps_waiting_without_a_recovery_deadline() {
        let (sender, receiver) = oneshot::channel();
        let waiter = tokio::spawn(receive_call_response(receiver, None));
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        drop(sender);
        assert!(waiter.await.expect("waiter task").is_some());
    }
}
