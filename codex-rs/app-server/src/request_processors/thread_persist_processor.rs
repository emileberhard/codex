// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::*;
use codex_app_server_protocol::ThreadPersistParams;
use codex_app_server_protocol::ThreadPersistResponse;

impl ThreadRequestProcessor {
    pub(crate) async fn thread_persist(
        &self,
        params: ThreadPersistParams,
    ) -> Result<ThreadPersistResponse, JSONRPCErrorError> {
        let (thread_id, thread) = self.load_thread(&params.thread_id).await?;
        if thread.config_snapshot().await.ephemeral {
            return Err(invalid_request("ephemeral threads cannot be persisted"));
        }
        thread.checkpoint_preparation().await.map_err(|error| {
            internal_error(format!(
                "failed to checkpoint thread initialization: {error}"
            ))
        })?;
        self.thread_store
            .persist_thread_durable(thread_id)
            .await
            .map_err(|error| {
                internal_error(format!("failed to durably persist thread: {error}"))
            })?;
        Ok(ThreadPersistResponse {})
    }
}
