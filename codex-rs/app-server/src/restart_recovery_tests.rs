// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use codex_app_server_protocol::ThreadRestartRecoverParams;
use codex_utils_redacted_string::RedactedString;

use super::RestartRecoveryAuthorizer;
use super::RestartRecoveryGrant;
use super::RestartRecoveryGrantFile;

fn grant() -> RestartRecoveryGrant {
    serde_json::from_value::<RestartRecoveryGrantFile>(serde_json::json!({
        "schemaVersion": 1,
        "generationId": "generation-1",
        "capability": "0123456789abcdef0123456789abcdef",
        "targets": [{
            "threadId": "018f0000-0000-7000-8000-000000000001",
            "turnId": "turn-1"
        }]
    }))
    .expect("parse grant")
    .try_into()
    .expect("validate grant")
}

#[test]
fn exact_generation_target_is_authorized() {
    let authorizer = RestartRecoveryAuthorizer::new(Some(grant()));
    let generation = authorizer
        .authorize(&ThreadRestartRecoverParams {
            thread_id: "018f0000-0000-7000-8000-000000000001".to_string(),
            turn_id: "turn-1".to_string(),
            capability: RedactedString::from("0123456789abcdef0123456789abcdef"),
        })
        .expect("authorized target");

    assert_eq!(generation, "generation-1");
}

#[test]
fn capability_and_exact_turn_are_both_required() {
    let authorizer = RestartRecoveryAuthorizer::new(Some(grant()));
    for (turn_id, capability) in [
        ("other-turn", "0123456789abcdef0123456789abcdef"),
        ("turn-1", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
    ] {
        authorizer
            .authorize(&ThreadRestartRecoverParams {
                thread_id: "018f0000-0000-7000-8000-000000000001".to_string(),
                turn_id: turn_id.to_string(),
                capability: RedactedString::from(capability),
            })
            .expect_err("mismatched recovery request");
    }
}

#[test]
fn clearing_authorization_is_fail_closed() {
    let authorizer = RestartRecoveryAuthorizer::new(Some(grant()));
    authorizer.clear();

    authorizer
        .authorize(&ThreadRestartRecoverParams {
            thread_id: "018f0000-0000-7000-8000-000000000001".to_string(),
            turn_id: "turn-1".to_string(),
            capability: RedactedString::from("0123456789abcdef0123456789abcdef"),
        })
        .expect_err("cleared grant");
}
