// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::*;

#[test]
fn restored_settings_reject_multiple_nested_optional_values() {
    for field in ["effort", "serviceTier"] {
        let mut settings =
            settings_fingerprint(&codex_protocol::protocol::ThreadSettingsOverrides::default())
                .unwrap();
        settings[field] = json!([null, null]);
        assert!(
            restore_settings(&settings)
                .unwrap_err()
                .to_string()
                .contains("multiple effort or service tier values")
        );
    }
}

#[test]
fn legacy_durable_request_keeps_its_fingerprint_shape() {
    let legacy = json!({
        "input": null,
        "settings": settings_fingerprint(&codex_protocol::protocol::ThreadSettingsOverrides::default()).unwrap(),
        "start": codex_protocol::turn_input::TurnStartOptions::default(),
        "additional_context": {"app": {"value": "context", "kind": "Application"}},
        "responsesapi_client_metadata": null,
    });
    let mut legacy = legacy;
    legacy["input"] = serde_json::to_value(codex_protocol::turn_input::TurnInput::UserInput {
        content: Vec::new(),
        client_id: Some("legacy-input".into()),
    })
    .unwrap();
    let request: DurableInputRequest = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(serde_json::to_value(&request).unwrap(), legacy);
    assert_eq!(request.into_request().unwrap().user_item_uuid, None);
}

#[test]
fn durable_request_recovers_user_identity_and_metadata_patch() {
    let id = uuid::Uuid::from_u128(42);
    let request = DurableInputRequest {
        input: codex_protocol::turn_input::TurnInput::UserInput {
            content: Vec::new(),
            client_id: Some("input".into()),
        },
        user_item_uuid: Some(id),
        settings: settings_fingerprint(
            &codex_protocol::protocol::ThreadSettingsOverrides::default(),
        )
        .unwrap(),
        start: Default::default(),
        additional_context: Default::default(),
        responsesapi_client_metadata: Some(HashMap::from([("old".into(), "kept".into())])),
        responsesapi_client_metadata_patch: Some(HashMap::from([("new".into(), "added".into())])),
    };
    let recovered: DurableInputRequest =
        serde_json::from_value(serde_json::to_value(request).unwrap()).unwrap();
    let recovered = recovered.into_request().unwrap();
    assert_eq!(recovered.user_item_uuid, Some(id));
    assert_eq!(
        recovered.responsesapi_client_metadata.replacement,
        Some(HashMap::from([("old".into(), "kept".into())]))
    );
    assert_eq!(
        recovered.responsesapi_client_metadata.patch,
        Some(HashMap::from([("new".into(), "added".into())]))
    );
}
