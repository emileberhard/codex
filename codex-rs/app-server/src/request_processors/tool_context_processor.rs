// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::*;

impl ThreadRequestProcessor {
    pub(crate) async fn daemon_tool_context(
        &self,
        params: codex_app_server_protocol::DaemonToolContextParams,
    ) -> Result<codex_app_server_protocol::DaemonToolContextResponse, JSONRPCErrorError> {
        let Ok(pid) = u32::try_from(params.pid) else {
            return Ok(codex_app_server_protocol::DaemonToolContextResponse { context: None });
        };
        let context = self
            .thread_manager
            .local_tool_context(codex_utils_pty::LocalProcessIdentity {
                pid,
                started_at_unix_micros: params.started_at_unix_micros,
            })
            .await;
        Ok(codex_app_server_protocol::DaemonToolContextResponse {
            context: context.map(|context| codex_app_server_protocol::DaemonToolContext {
                thread_id: context.thread_id.to_string(),
                turn_id: context.turn_id,
                tool_call_id: context.tool_call_id,
                root_process_identity: codex_app_server_protocol::LocalProcessIdentity {
                    pid: context.root_process_identity.pid as i32,
                    started_at_unix_micros: context.root_process_identity.started_at_unix_micros,
                },
            }),
        })
    }
}
