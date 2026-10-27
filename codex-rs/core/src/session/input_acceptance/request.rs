// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::turn_input::TurnInput;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct DurableInputRequest {
    pub(super) input: TurnInput,
    pub(super) settings: serde_json::Value,
    pub(super) start: codex_protocol::turn_input::TurnStartOptions,
    pub(super) additional_context:
        std::collections::BTreeMap<String, codex_protocol::protocol::AdditionalContextEntry>,
    pub(super) responsesapi_client_metadata: Option<HashMap<String, String>>,
}

pub(super) fn settings_fingerprint(
    settings: &codex_protocol::protocol::ThreadSettingsOverrides,
) -> CodexResult<serde_json::Value> {
    let environments = settings.environments.as_ref().map(|environments| -> CodexResult<serde_json::Value> {
        let requests = environments.environment_requests.iter().map(|environment| {
            let config = match &environment.config {
                codex_protocol::protocol::EnvironmentConfigState::FromThread => json!("fromThread"),
                codex_protocol::protocol::EnvironmentConfigState::Pending => json!("pending"),
                codex_protocol::protocol::EnvironmentConfigState::Failed(error) => json!({"failed": error}),
                codex_protocol::protocol::EnvironmentConfigState::Ready(_) => return Err(CodexErr::InvalidRequest("identified input requires environment configuration to be applied separately before submission".to_string())),
            };
            Ok(json!({"id": environment.environment_id, "cwd": environment.cwd, "roots": environment.workspace_roots, "config": config}))
        }).collect::<CodexResult<Vec<_>>>()?;
        Ok(json!({"cwd": environments.legacy_fallback_cwd, "requests": requests}))
    }).transpose()?;
    Ok(json!({
        "environments": environments,
        "runtimeWorkspaceRoots": settings.runtime_workspace_roots,
        "profileWorkspaceRoots": settings.profile_workspace_roots.as_ref().map(|roots| roots.iter().map(codex_protocol::models::ProfileWorkspaceRoot::as_uri).collect::<Vec<_>>()),
        "approvalPolicy": settings.approval_policy,
        "approvalsReviewer": settings.approvals_reviewer,
        "sandboxPolicy": settings.sandbox_policy,
        "permissionProfile": settings.permission_profile,
        "activePermissionProfile": settings.active_permission_profile,
        "windowsSandboxLevel": settings.windows_sandbox_level,
        "model": settings.model,
        "effort": settings.effort.iter().collect::<Vec<_>>(),
        "summary": settings.summary,
        "serviceTier": settings.service_tier.iter().collect::<Vec<_>>(),
        "collaborationMode": settings.collaboration_mode,
        "personality": settings.personality,
        "disabledPluginIds": settings.disabled_plugin_ids,
    }))
}

pub(super) fn restore_settings(
    value: &serde_json::Value,
) -> CodexResult<codex_protocol::protocol::ThreadSettingsOverrides> {
    fn read<T: serde::de::DeserializeOwned>(
        value: &serde_json::Value,
        field: &str,
    ) -> CodexResult<T> {
        let value = value
            .get(field)
            .ok_or_else(|| CodexErr::Fatal(format!("accepted input is missing {field}")))?;
        serde_json::from_value(value.clone())
            .map_err(|error| CodexErr::Fatal(format!("invalid accepted input {field}: {error}")))
    }
    use codex_protocol::protocol::EnvironmentConfigState;
    use codex_protocol::protocol::ThreadSettingsOverrides;
    use codex_protocol::protocol::TurnEnvironmentRequest;
    use codex_protocol::protocol::TurnEnvironmentRequests;
    let environments = match value.get("environments").filter(|value| !value.is_null()) {
        None => None,
        Some(environments) => {
            let requests = environments["requests"].as_array().ok_or_else(|| {
                CodexErr::Fatal("accepted input has invalid environment requests".to_string())
            })?;
            let requests = requests
                .iter()
                .map(|request| {
                    let config = match &request["config"] {
                        serde_json::Value::String(state) if state == "fromThread" => {
                            EnvironmentConfigState::FromThread
                        }
                        serde_json::Value::String(state) if state == "pending" => {
                            EnvironmentConfigState::Pending
                        }
                        serde_json::Value::Object(_) => {
                            EnvironmentConfigState::Failed(read(&request["config"], "failed")?)
                        }
                        _ => {
                            return Err(CodexErr::Fatal(
                                "accepted input has invalid environment configuration".to_string(),
                            ));
                        }
                    };
                    Ok(TurnEnvironmentRequest {
                        environment_id: read(request, "id")?,
                        cwd: read(request, "cwd")?,
                        workspace_roots: read(request, "roots")?,
                        config,
                    })
                })
                .collect::<CodexResult<Vec<_>>>()?;
            Some(TurnEnvironmentRequests::new(
                read(environments, "cwd")?,
                requests,
            ))
        }
    };
    let effort: Vec<Option<codex_protocol::openai_models::ReasoningEffort>> =
        read(value, "effort")?;
    let service_tier: Vec<Option<String>> = read(value, "serviceTier")?;
    if effort.len() > 1 || service_tier.len() > 1 {
        return Err(CodexErr::Fatal(
            "accepted input has multiple effort or service tier values".to_string(),
        ));
    }
    let profile_roots: Option<Vec<codex_utils_path_uri::PathUri>> =
        read(value, "profileWorkspaceRoots")?;
    Ok(ThreadSettingsOverrides {
        environments,
        runtime_workspace_roots: read(value, "runtimeWorkspaceRoots")?,
        profile_workspace_roots: profile_roots
            .map(|roots| roots.into_iter().map(Into::into).collect()),
        approval_policy: read(value, "approvalPolicy")?,
        approvals_reviewer: read(value, "approvalsReviewer")?,
        sandbox_policy: read(value, "sandboxPolicy")?,
        permission_profile: read(value, "permissionProfile")?,
        active_permission_profile: read(value, "activePermissionProfile")?,
        windows_sandbox_level: read(value, "windowsSandboxLevel")?,
        model: read(value, "model")?,
        effort: effort.into_iter().next(),
        summary: read(value, "summary")?,
        service_tier: service_tier.into_iter().next(),
        collaboration_mode: read(value, "collaborationMode")?,
        personality: read(value, "personality")?,
        disabled_plugin_ids: read(value, "disabledPluginIds")?,
    })
}

#[cfg(test)]
#[path = "request_tests.rs"]
mod tests;
