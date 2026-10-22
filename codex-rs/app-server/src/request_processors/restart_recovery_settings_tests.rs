// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::latest_restart_recovery_settings;
use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Settings;
use codex_protocol::models::ActivePermissionProfile;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TurnContextItem;
use codex_protocol::turn_input::CyberAccessProgram;
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
                runtime_workspace_roots: Some(vec![cwd().join("recovery-root")]),
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
    permission_profile: PermissionProfile,
    active_permission_profile: Option<ActivePermissionProfile>,
) -> RolloutItem {
    RolloutItem::TurnContext(TurnContextItem {
        turn_id: Some(turn_id.to_string()),
        root_turn_id: None,
        disabled_plugin_ids: None,
        cwd: cwd(),
        approval_policy,
        approvals_reviewer,
        sandbox_policy: permission_profile
            .to_legacy_sandbox_policy(cwd().as_path())
            .expect("legacy sandbox policy"),
        permission_profile: Some(permission_profile),
        active_permission_profile,
        file_system_sandbox_policy: None,
        model: "gpt-5".to_string(),
        comp_hash: None,
        collaboration_mode: None,
        multi_agent_version: None,
        realtime_active: None,
        cyber_access_program: None,
        effort: None,
        summary: Some(codex_protocol::config_types::ReasoningSummary::Auto),
    })
}

fn assert_resume_settings(
    history: &[RolloutItem],
    approval_policy: AskForApproval,
    approvals_reviewer: Option<ApprovalsReviewer>,
    permission_profile: PermissionProfile,
    active_permission_profile: Option<ActivePermissionProfile>,
) {
    let persisted = latest_restart_recovery_settings(history).expect("persisted settings");
    let settings = persisted.thread_settings;
    assert_eq!(settings.approval_policy, Some(approval_policy));
    assert_eq!(settings.approvals_reviewer, approvals_reviewer);
    assert_eq!(
        settings.active_permission_profile,
        active_permission_profile
    );
    assert_eq!(settings.permission_profile, Some(permission_profile));
    assert_eq!(settings.model.as_deref(), Some("gpt-5"));
    assert_eq!(settings.effort, Some(None));
    assert_eq!(settings.summary, None);
    assert_eq!(settings.environments, None);
    assert_eq!(persisted.cwd, cwd());
}

#[test]
fn latest_settings_snapshot_wins() {
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

    assert_resume_settings(
        &history,
        AskForApproval::OnRequest,
        Some(ApprovalsReviewer::AutoReview),
        PermissionProfile::read_only(),
        Some(ActivePermissionProfile::new("dev")),
    );
    let persisted = latest_restart_recovery_settings(&history).expect("persisted settings");
    assert_eq!(persisted.model_provider.as_deref(), Some("openai"));
    assert_eq!(
        persisted.workspace_roots,
        Some(vec![cwd().join("recovery-root")])
    );
    assert_eq!(persisted.cyber_access_program, None);
    assert_eq!(persisted.thread_settings.service_tier, Some(None));
    assert_eq!(
        persisted
            .thread_settings
            .collaboration_mode
            .as_ref()
            .map(|mode| mode.mode),
        Some(ModeKind::Default)
    );
}

#[test]
fn latest_turn_context_wins_over_earlier_settings_update() {
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
            PermissionProfile::Disabled,
            Some(ActivePermissionProfile::new(":danger-full-access")),
        ),
    ];

    assert_resume_settings(
        &history,
        AskForApproval::UnlessTrusted,
        Some(ApprovalsReviewer::User),
        PermissionProfile::Disabled,
        Some(ActivePermissionProfile::new(":danger-full-access")),
    );
    let persisted = latest_restart_recovery_settings(&history).expect("persisted settings");
    assert_eq!(persisted.model_provider.as_deref(), Some("openai"));
    assert_eq!(
        persisted.workspace_roots,
        Some(vec![cwd().join("recovery-root")])
    );
    assert_eq!(persisted.cyber_access_program, None);
    assert_eq!(persisted.thread_settings.service_tier, Some(None));
    assert_eq!(
        persisted
            .thread_settings
            .collaboration_mode
            .as_ref()
            .map(|mode| mode.mode),
        Some(ModeKind::Default)
    );
}

#[test]
fn latest_turn_context_preserves_cyber_access_program() {
    let mut item = turn_context_item(
        "turn-1",
        AskForApproval::Never,
        Some(ApprovalsReviewer::User),
        PermissionProfile::read_only(),
        /*active_permission_profile*/ None,
    );
    let RolloutItem::TurnContext(turn_context) = &mut item else {
        panic!("expected turn context");
    };
    turn_context.cyber_access_program = Some(CyberAccessProgram::DaybreakBlue);

    let persisted = latest_restart_recovery_settings(&[item]).expect("persisted settings");
    assert_eq!(
        persisted.cyber_access_program,
        Some(CyberAccessProgram::DaybreakBlue)
    );
}

#[test]
fn older_reviewer_is_used_when_latest_turn_context_omits_it() {
    let history = vec![
        turn_context_item(
            "turn-1",
            AskForApproval::Never,
            Some(ApprovalsReviewer::AutoReview),
            PermissionProfile::read_only(),
            /*active_permission_profile*/ None,
        ),
        turn_context_item(
            "turn-2",
            AskForApproval::OnRequest,
            /*approvals_reviewer*/ None,
            PermissionProfile::read_only(),
            /*active_permission_profile*/ None,
        ),
    ];

    assert_resume_settings(
        &history,
        AskForApproval::OnRequest,
        Some(ApprovalsReviewer::AutoReview),
        PermissionProfile::read_only(),
        None,
    );
}
