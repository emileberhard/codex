use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TurnContextItem;
use codex_protocol::turn_input::CyberAccessProgram;
use codex_rollout::RolloutItem;
use codex_utils_absolute_path::AbsolutePathBuf;

pub(super) struct RestartRecoverySettings {
    pub(super) thread_settings: ThreadSettingsOverrides,
    pub(super) cwd: AbsolutePathBuf,
    pub(super) model_provider: Option<String>,
    pub(super) workspace_roots: Option<Vec<AbsolutePathBuf>>,
    pub(super) cyber_access_program: Option<CyberAccessProgram>,
}

pub(super) fn latest_restart_recovery_settings(
    history: &[RolloutItem],
) -> Option<RestartRecoverySettings> {
    history
        .iter()
        .enumerate()
        .rev()
        .find_map(|(index, item)| match item {
            RolloutItem::TurnContext(turn_context) => {
                let previous_history = &history[..index];
                Some(settings_from_turn_context(
                    turn_context,
                    latest_approvals_reviewer(previous_history),
                    latest_settings_snapshot(previous_history),
                ))
            }
            RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(event)) => {
                Some(settings_from_snapshot(&event.thread_settings))
            }
            _ => None,
        })
}

fn latest_settings_snapshot(history: &[RolloutItem]) -> Option<&ThreadSettingsSnapshot> {
    history.iter().rev().find_map(|item| match item {
        RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(event)) => {
            Some(&event.thread_settings)
        }
        _ => None,
    })
}

fn latest_approvals_reviewer(history: &[RolloutItem]) -> Option<ApprovalsReviewer> {
    history.iter().rev().find_map(|item| match item {
        RolloutItem::TurnContext(turn_context) => turn_context.approvals_reviewer,
        RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(event)) => {
            Some(event.thread_settings.approvals_reviewer)
        }
        _ => None,
    })
}

fn settings_from_snapshot(snapshot: &ThreadSettingsSnapshot) -> RestartRecoverySettings {
    RestartRecoverySettings {
        thread_settings: ThreadSettingsOverrides {
            approval_policy: Some(snapshot.approval_policy),
            approvals_reviewer: Some(snapshot.approvals_reviewer),
            permission_profile: Some(snapshot.permission_profile.clone()),
            active_permission_profile: snapshot.active_permission_profile.clone(),
            model: Some(snapshot.model.clone()),
            effort: Some(snapshot.reasoning_effort.clone()),
            summary: snapshot.reasoning_summary,
            service_tier: Some(snapshot.service_tier.clone()),
            collaboration_mode: Some(snapshot.collaboration_mode.clone()),
            personality: snapshot.personality,
            ..Default::default()
        },
        cwd: snapshot.cwd.clone(),
        model_provider: Some(snapshot.model_provider_id.clone()),
        workspace_roots: snapshot.runtime_workspace_roots.clone(),
        cyber_access_program: None,
    }
}

fn settings_from_turn_context(
    turn_context: &TurnContextItem,
    previous_approvals_reviewer: Option<ApprovalsReviewer>,
    previous_snapshot: Option<&ThreadSettingsSnapshot>,
) -> RestartRecoverySettings {
    RestartRecoverySettings {
        thread_settings: ThreadSettingsOverrides {
            approval_policy: Some(turn_context.approval_policy),
            approvals_reviewer: turn_context
                .approvals_reviewer
                .or(previous_approvals_reviewer),
            permission_profile: Some(turn_context.permission_profile()),
            active_permission_profile: turn_context.active_permission_profile.clone(),
            model: Some(turn_context.model.clone()),
            effort: Some(turn_context.effort.clone()),
            // TurnContextItem.summary is a compatibility-only field. The settings
            // snapshot remains authoritative for summary, provider, and service tier.
            summary: previous_snapshot.and_then(|snapshot| snapshot.reasoning_summary),
            service_tier: previous_snapshot.map(|snapshot| snapshot.service_tier.clone()),
            collaboration_mode: turn_context
                .collaboration_mode
                .clone()
                .or_else(|| previous_snapshot.map(|snapshot| snapshot.collaboration_mode.clone())),
            personality: previous_snapshot.and_then(|snapshot| snapshot.personality),
            ..Default::default()
        },
        cwd: turn_context.cwd.clone(),
        model_provider: previous_snapshot.map(|snapshot| snapshot.model_provider_id.clone()),
        workspace_roots: previous_snapshot
            .and_then(|snapshot| snapshot.runtime_workspace_roots.clone()),
        cyber_access_program: turn_context.cyber_access_program,
    }
}

#[cfg(test)]
#[path = "restart_recovery_settings_tests.rs"]
mod tests;
