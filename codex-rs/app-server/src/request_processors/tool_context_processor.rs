// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::*;

impl ThreadRequestProcessor {
    pub(crate) async fn daemon_tool_context(
        &self,
        params: codex_app_server_protocol::DaemonToolContextParams,
    ) -> Result<codex_app_server_protocol::DaemonToolContextResponse, JSONRPCErrorError> {
        use codex_utils_pty::ProcessBirth;
        let birth = match (params.started_at_unix_micros, params.linux_birth) {
            (Some(micros), None) if cfg!(target_os = "macos") => {
                Some(ProcessBirth::UnixMicros(micros))
            }
            (None, Some(birth)) if cfg!(target_os = "linux") => {
                codex_utils_pty::parse_linux_boot_id(&birth.boot_id).map(|boot_id| {
                    ProcessBirth::Linux {
                        boot_id,
                        start_ticks: birth.start_ticks,
                    }
                })
            }
            _ => None,
        };
        let Some((pid, birth)) = u32::try_from(params.pid).ok().zip(birth) else {
            return Ok(codex_app_server_protocol::DaemonToolContextResponse { context: None });
        };
        let context = self
            .thread_manager
            .local_tool_context(codex_utils_pty::LocalProcessIdentity { pid, birth })
            .await;
        Ok(codex_app_server_protocol::DaemonToolContextResponse {
            context: context.map(|context| {
                let (started_at_unix_micros, linux_birth) =
                    match context.root_process_identity.birth {
                        ProcessBirth::UnixMicros(value) => (Some(value), None),
                        ProcessBirth::Linux {
                            boot_id,
                            start_ticks,
                        } => (
                            None,
                            Some(codex_app_server_protocol::LinuxProcessBirth {
                                boot_id: String::from_utf8_lossy(&boot_id).into_owned(),
                                start_ticks,
                            }),
                        ),
                    };
                codex_app_server_protocol::DaemonToolContext {
                    thread_id: context.thread_id.to_string(),
                    turn_id: context.turn_id,
                    tool_call_id: context.tool_call_id,
                    root_process_identity: codex_app_server_protocol::LocalProcessIdentity {
                        pid: context.root_process_identity.pid as i32,
                        started_at_unix_micros,
                        linux_birth,
                    },
                }
            }),
        })
    }
}
