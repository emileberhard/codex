use super::PersistedResumeSettings;
use super::latest_persisted_resume_settings;
use super::latest_persisted_service_tier;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Settings;
use codex_protocol::models::ActivePermissionProfile;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SandboxPolicy;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TurnContextItem;
use codex_rollout::RolloutItem;
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;

fn cwd() -> AbsolutePathBuf {
    AbsolutePathBuf::try_from(std::env::current_dir().expect("current directory"))
        .expect("absolute current directory")
}

fn settings_item(
    approval_policy: AskForApproval,
    approvals_reviewer: ApprovalsReviewer,
    active_permission_profile: Option<ActivePermissionProfile>,
) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(
        ThreadSettingsAppliedEvent {
            thread_id: None,
            thread_settings: ThreadSettingsSnapshot {
                disabled_plugin_ids: Vec::new(),
                model: "gpt-5".to_string(),
                model_provider_id: "openai".to_string(),
                service_tier: None,
                approval_policy,
                approvals_reviewer,
                permission_profile: PermissionProfile::read_only(),
                active_permission_profile,
                cwd: cwd(),
                runtime_workspace_roots: None,
                reasoning_effort: None,
                reasoning_summary: None,
                personality: None,
                collaboration_mode: CollaborationMode {
                    mode: ModeKind::Default,
                    settings: Settings {
                        model: "gpt-5".to_string(),
                        reasoning_effort: None,
                        developer_instructions: None,
                    },
                },
            },
        },
    ))
}

fn turn_context_item(
    turn_id: &str,
    approval_policy: AskForApproval,
    approvals_reviewer: Option<ApprovalsReviewer>,
    active_permission_profile: Option<ActivePermissionProfile>,
) -> RolloutItem {
    RolloutItem::TurnContext(TurnContextItem {
        turn_id: Some(turn_id.to_string()),
        root_turn_id: None,
        disabled_plugin_ids: None,
        cwd: cwd(),
        workspace_roots: Some(vec![cwd()]),
        current_date: None,
        timezone: None,
        approval_policy,
        approvals_reviewer,
        sandbox_policy: SandboxPolicy::new_read_only_policy(),
        permission_profile: Some(PermissionProfile::read_only()),
        active_permission_profile,
        network: None,
        file_system_sandbox_policy: None,
        model: "gpt-5".to_string(),
        comp_hash: None,
        personality: None,
        collaboration_mode: None,
        multi_agent_version: None,
        multi_agent_mode: None,
        realtime_active: None,
        cyber_access_program: None,
        effort: None,
        summary: codex_protocol::config_types::ReasoningSummary::Auto,
    })
}

#[test]
fn latest_settings_snapshot_wins() {
    let expected = PersistedResumeSettings {
        approval_policy: AskForApproval::OnRequest,
        approvals_reviewer: Some(ApprovalsReviewer::AutoReview),
        active_permission_profile: Some(ActivePermissionProfile::new("dev")),
    };
    let history = vec![
        settings_item(
            AskForApproval::Never,
            ApprovalsReviewer::User,
            /*active_permission_profile*/ None,
        ),
        settings_item(
            AskForApproval::OnRequest,
            ApprovalsReviewer::AutoReview,
            Some(ActivePermissionProfile::new("dev")),
        ),
    ];

    assert_eq!(latest_persisted_resume_settings(&history), Some(expected));
}

#[test]
fn latest_turn_context_wins_over_earlier_settings_update() {
    let expected = PersistedResumeSettings {
        approval_policy: AskForApproval::UnlessTrusted,
        approvals_reviewer: Some(ApprovalsReviewer::User),
        active_permission_profile: Some(ActivePermissionProfile::read_only()),
    };
    let history = vec![
        settings_item(
            AskForApproval::Never,
            ApprovalsReviewer::AutoReview,
            Some(ActivePermissionProfile::new("dev")),
        ),
        turn_context_item(
            "turn-2",
            AskForApproval::UnlessTrusted,
            Some(ApprovalsReviewer::User),
            Some(ActivePermissionProfile::read_only()),
        ),
    ];

    assert_eq!(latest_persisted_resume_settings(&history), Some(expected));
}

#[test]
fn older_reviewer_is_used_when_latest_turn_context_omits_it() {
    let history = vec![
        turn_context_item(
            "turn-1",
            AskForApproval::Never,
            Some(ApprovalsReviewer::AutoReview),
            /*active_permission_profile*/ None,
        ),
        turn_context_item(
            "turn-2",
            AskForApproval::OnRequest,
            /*approvals_reviewer*/ None,
            /*active_permission_profile*/ None,
        ),
    ];

    assert_eq!(
        latest_persisted_resume_settings(&history),
        Some(PersistedResumeSettings {
            approval_policy: AskForApproval::OnRequest,
            approvals_reviewer: Some(ApprovalsReviewer::AutoReview),
            active_permission_profile: None,
        })
    );
}

#[test]
fn service_tier_distinguishes_owned_off_from_missing_owned_snapshot() {
    let source_id = ThreadId::from_u128(1);
    let other_id = ThreadId::from_u128(2);
    let mut priority = settings_item(
        AskForApproval::OnRequest,
        ApprovalsReviewer::User,
        /*active_permission_profile*/ None,
    );
    if let RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(event)) = &mut priority {
        event.thread_id = Some(source_id);
        event.thread_settings.service_tier = Some("priority".to_string());
    }
    let mut unknown = settings_item(
        AskForApproval::OnRequest,
        ApprovalsReviewer::User,
        /*active_permission_profile*/ None,
    );
    if let RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(event)) = &mut unknown {
        event.thread_id = Some(source_id);
    }
    let mut foreign = priority.clone();
    if let RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(event)) = &mut foreign {
        event.thread_id = Some(other_id);
    }

    assert_eq!(
        latest_persisted_service_tier(&[foreign.clone()], source_id),
        None
    );
    assert_eq!(
        latest_persisted_service_tier(&[priority.clone(), foreign], source_id),
        Some(Some("priority".to_string()))
    );
    assert_eq!(
        latest_persisted_service_tier(&[priority, unknown], source_id),
        Some(None)
    );
}
