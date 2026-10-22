// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::restart_recovery_settings::latest_restart_recovery_settings;
use super::*;
use codex_core::RestartRecoveryThread;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::turn_input::CyberAccessProgram;

impl ThreadRequestProcessor {
    pub(crate) async fn thread_restart_suspend(
        &self,
        params: ThreadRestartSuspendParams,
    ) -> Result<ThreadRestartSuspendResponse, JSONRPCErrorError> {
        Box::pin(self.thread_restart_suspend_inner(params)).await
    }

    async fn thread_restart_suspend_inner(
        &self,
        params: ThreadRestartSuspendParams,
    ) -> Result<ThreadRestartSuspendResponse, JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|error| invalid_request(format!("invalid thread id: {error}")))?;
        let thread = match self.thread_manager.get_thread(thread_id).await {
            Ok(thread) => thread,
            Err(_) => {
                let already_suspended = self
                    .thread_turns_list_response_inner(ThreadTurnsListParams {
                        thread_id: params.thread_id,
                        cursor: None,
                        limit: Some(1),
                        sort_direction: Some(SortDirection::Desc),
                        items_view: Some(TurnItemsView::Summary),
                    })
                    .await?
                    .data
                    .into_iter()
                    .any(|turn| turn.id == params.turn_id && turn.status == TurnStatus::InProgress);
                return Ok(ThreadRestartSuspendResponse {
                    outcome: if already_suspended {
                        ThreadRestartSuspendOutcome::Suspended {
                            turn_id: params.turn_id,
                        }
                    } else {
                        ThreadRestartSuspendOutcome::NotActive
                    },
                });
            }
        };
        let outcome = thread
            .suspend_turn_and_shutdown(params.turn_id)
            .await
            .map_err(|error| {
                internal_error(format!("failed to suspend thread {thread_id}: {error}"))
            })?;
        let outcome = match outcome {
            SuspendTurnOutcome::Suspended { turn_id } => {
                self.thread_manager.remove_thread(&thread_id).await;
                self.finalize_thread_teardown(thread_id).await;
                ThreadRestartSuspendOutcome::Suspended { turn_id }
            }
            SuspendTurnOutcome::NotActive => ThreadRestartSuspendOutcome::NotActive,
            SuspendTurnOutcome::TurnChanged { actual_turn_id } => {
                ThreadRestartSuspendOutcome::TurnChanged { actual_turn_id }
            }
            SuspendTurnOutcome::HasLiveDescendants => {
                ThreadRestartSuspendOutcome::HasLiveDescendants
            }
            SuspendTurnOutcome::UnsupportedTask => ThreadRestartSuspendOutcome::UnsupportedTask,
        };
        Ok(ThreadRestartSuspendResponse { outcome })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn thread_restart_recover(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadRestartRecoverParams,
        generation_id: String,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
    ) -> Result<ThreadRestartRecoverResponse, JSONRPCErrorError> {
        Box::pin(self.thread_restart_recover_inner(
            request_id,
            params,
            generation_id,
            app_server_client_name,
            app_server_client_version,
            client_mcp_extensions,
        ))
        .await
    }

    async fn thread_restart_recover_inner(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadRestartRecoverParams,
        generation_id: String,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
    ) -> Result<ThreadRestartRecoverResponse, JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|error| invalid_request(format!("invalid thread id: {error}")))?;
        if let Some(turn) = self
            .thread_turns_list_response_inner(ThreadTurnsListParams {
                thread_id: params.thread_id.clone(),
                cursor: None,
                limit: Some(1),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Summary),
            })
            .await?
            .data
            .into_iter()
            .find(|turn| turn.id == params.turn_id)
            .filter(|turn| matches!(turn.status, TurnStatus::Completed | TurnStatus::Failed))
        {
            return Ok(ThreadRestartRecoverResponse {
                generation_id,
                turn,
            });
        }
        let (thread, thread_settings, cyber_access_program) = self
            .load_restart_recovery_thread(
                &request_id,
                thread_id,
                app_server_client_name,
                app_server_client_version,
                client_mcp_extensions,
            )
            .await?;
        self.thread_state_manager
            .thread_state(thread_id)
            .await
            .lock()
            .await
            .recovered_turn_ids
            .insert(params.turn_id.clone());
        let submission = thread
            .recover_turn_if_idle(RecoverTurnRequest {
                turn_id: params.turn_id.clone(),
                thread_settings,
                trace: self.request_trace_context(&request_id).await,
                cyber_access_program,
            })
            .await
            .map_err(|error| {
                internal_error(format!("failed to recover thread {thread_id}: {error}"))
            })?;
        let (turn_id, root_turn_id) = match submission {
            StartIfIdleSubmission::Started {
                turn_id,
                root_turn_id,
            } => (turn_id, Some(root_turn_id)),
            StartIfIdleSubmission::NotSubmitted {
                reason: NotSubmittedReason::NotIdle | NotSubmittedReason::PendingTriggerTurn,
            } => {
                let thread_state = self.thread_state_manager.thread_state(thread_id).await;
                let active_turn = thread_state.lock().await.active_turn_snapshot();
                if active_turn.as_ref().map(|turn| turn.id.as_str())
                    != Some(params.turn_id.as_str())
                {
                    return Err(invalid_request(format!(
                        "thread {thread_id} is running a different turn"
                    )));
                }
                (
                    params.turn_id,
                    active_turn.and_then(|turn| turn.root_turn_id),
                )
            }
            StartIfIdleSubmission::NotSubmitted { reason } => {
                return Err(internal_error(format!(
                    "Core declined restart recovery for thread {thread_id}: {reason:?}"
                )));
            }
        };
        self.outgoing
            .record_request_turn_id(&request_id, &turn_id)
            .await;
        Ok(ThreadRestartRecoverResponse {
            generation_id,
            turn: in_progress_turn(turn_id, root_turn_id),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_restart_recovery_thread(
        &self,
        request_id: &ConnectionRequestId,
        thread_id: ThreadId,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
    ) -> Result<
        (
            RestartRecoveryThread,
            ThreadSettingsOverrides,
            Option<CyberAccessProgram>,
        ),
        JSONRPCErrorError,
    > {
        let _thread_list_state_permit = self.acquire_thread_list_state_permit().await?;
        let (thread, thread_settings, cyber_access_program) = match self
            .thread_manager
            .loaded_thread_for_restart_recovery(thread_id)
            .await
        {
            Ok(thread) => (thread, ThreadSettingsOverrides::default(), None),
            Err(_) => {
                let stored_thread = self
                    .read_stored_thread_for_resume(
                        &thread_id.to_string(),
                        /*path*/ None,
                        /*include_history*/ false,
                    )
                    .await?;
                let (thread_history, _) = self
                    .load_resume_initial_history_from_stored_thread(stored_thread)
                    .await?;
                let persisted_settings =
                    latest_restart_recovery_settings(thread_history.get_rollout_items());
                let thread_settings = persisted_settings
                    .as_ref()
                    .map(|settings| settings.thread_settings.clone())
                    .unwrap_or_default();
                let cyber_access_program = persisted_settings
                    .as_ref()
                    .and_then(|settings| settings.cyber_access_program);
                let history_cwd = persisted_settings
                    .as_ref()
                    .map(|settings| settings.cwd.clone().into_path_buf())
                    .or_else(|| thread_history.session_cwd());
                let mut request_overrides = None;
                let mut typesafe_overrides = self.build_thread_config_overrides(
                    thread_settings.model.clone(),
                    persisted_settings
                        .as_ref()
                        .and_then(|settings| settings.model_provider.clone()),
                    thread_settings.service_tier.clone(),
                    thread_settings.environments.as_ref().map(|environments| {
                        environments
                            .legacy_fallback_cwd
                            .as_path()
                            .to_string_lossy()
                            .into_owned()
                    }),
                    persisted_settings
                        .as_ref()
                        .and_then(|settings| settings.workspace_roots.clone()),
                    /*approval_policy*/ None,
                    /*approvals_reviewer*/ None,
                    /*sandbox*/ None,
                    /*permissions*/ None,
                    /*base_instructions*/ None,
                    /*developer_instructions*/ None,
                    thread_settings.personality,
                );
                let persisted_resume_settings =
                    super::persisted_resume_settings::latest_persisted_resume_settings(
                        thread_history.get_rollout_items(),
                    );
                let persisted_metadata = self
                    .load_and_apply_persisted_resume_metadata(
                        &thread_history,
                        persisted_resume_settings.as_ref(),
                        &mut request_overrides,
                        &mut typesafe_overrides,
                    )
                    .await;
                let mut config = self
                    .config_manager
                    .load_for_cwd(request_overrides, typesafe_overrides, history_cwd)
                    .await
                    .map_err(|error| config_load_error(&error))?;
                if persisted_metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.reasoning_effort.is_none())
                {
                    config.model_reasoning_effort = None;
                }
                let multi_agent_version = thread_history.get_multi_agent_version();
                let session_source = thread_history
                    .get_resumed_session_sources()
                    .map(|(source, _)| source);
                if let Some(session_source) = session_source
                    && session_source.is_non_root_agent()
                {
                    let thread = self
                        .thread_manager
                        .resume_agent_thread_after_restart(
                            config,
                            thread_id,
                            session_source,
                            multi_agent_version,
                        )
                        .await
                        .map_err(|error| {
                            internal_error(format!(
                                "error resuming child agent thread {thread_id}: {error}"
                            ))
                        })?;
                    (thread, thread_settings, cyber_access_program)
                } else {
                    let NewThread {
                        thread_id: resumed_thread_id,
                        ..
                    } = self
                        .thread_manager
                        .resume_thread_with_history_after_restart(
                            config,
                            thread_history,
                            self.auth_manager.clone(),
                            self.request_trace_context(request_id).await,
                            client_mcp_extensions,
                        )
                        .await
                        .map_err(|error| {
                            internal_error(format!("error resuming thread {thread_id}: {error}"))
                        })?;
                    if resumed_thread_id != thread_id {
                        return Err(internal_error(format!(
                            "restart recovery resumed thread {resumed_thread_id} instead of {thread_id}"
                        )));
                    }
                    let thread = self
                        .thread_manager
                        .loaded_thread_for_restart_recovery(thread_id)
                        .await
                        .map_err(|error| {
                            internal_error(format!(
                                "error pinning recovered thread {thread_id}: {error}"
                            ))
                        })?;
                    (thread, thread_settings, cyber_access_program)
                }
            }
        };
        Self::set_app_server_client_info(
            thread.thread().as_ref(),
            app_server_client_name,
            app_server_client_version,
        )
        .await?;
        let thread_state = self.thread_state_manager.thread_state(thread_id).await;
        self.ensure_listener_task_running(thread_id, Arc::clone(thread.thread()), thread_state)
            .await?;
        self.ensure_conversation_listener(
            thread_id,
            request_id.connection_id,
            /*raw_events_enabled*/ false,
        )
        .await?;
        self.thread_watch_manager
            .upsert_thread(&thread_id.to_string())
            .await;
        Ok((thread, thread_settings, cyber_access_program))
    }
}

fn in_progress_turn(turn_id: String, root_turn_id: Option<String>) -> Turn {
    Turn {
        id: turn_id,
        root_turn_id,
        items: Vec::new(),
        items_view: TurnItemsView::NotLoaded,
        error: None,
        status: TurnStatus::InProgress,
        started_at: None,
        completed_at: None,
        duration_ms: None,
    }
}
