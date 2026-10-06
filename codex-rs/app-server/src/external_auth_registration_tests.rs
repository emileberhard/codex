use super::*;
use codex_app_server_protocol::ChatgptAuthTokensRefreshParams;
use codex_app_server_protocol::ChatgptAuthTokensRefreshReason;
use std::future::Future;
use std::task::Context;
use std::task::Waker;

#[tokio::test]
async fn external_auth_disconnect_during_registration_cancels_without_delivery() {
    let (sender, mut messages) = mpsc::channel(4);
    let outgoing = OutgoingMessageSender::new(sender, AnalyticsEventsClient::disabled());
    let owner = ConnectionId(1);
    outgoing.connection_initialized(owner).await;
    let owners = [owner];
    let mut registration = std::pin::pin!(outgoing.send_request_to_connections(
        Some(&owners),
        ServerRequestPayload::ChatgptAuthTokensRefresh(ChatgptAuthTokensRefreshParams {
            reason: ChatgptAuthTokensRefreshReason::Unauthorized,
            previous_account_id: Some("phase0-account".into()),
        }),
        /*thread_id*/ None,
    ));
    let mut disconnect = std::pin::pin!(outgoing.connection_closed(owner));
    {
        let _callbacks = outgoing.request_id_to_callback.lock().await;
        let mut context = Context::from_waker(Waker::noop());
        assert!(registration.as_mut().poll(&mut context).is_pending());
        assert!(disconnect.as_mut().poll(&mut context).is_pending());
    }
    let ((_, response), ()) = tokio::join!(registration, disconnect);
    assert!(response.await.is_err());
    assert!(messages.try_recv().is_err());
    assert!(outgoing.request_id_to_callback.lock().await.is_empty());
}
