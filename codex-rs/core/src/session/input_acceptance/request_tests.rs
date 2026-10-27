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
