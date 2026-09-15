use super::ServerDiagnosticsFileDescriptorLimits;
use super::ServerDiagnosticsProcess;
use super::ServerDiagnosticsResourceLimit;
use pretty_assertions::assert_eq;
use serde_json::json;

#[test]
fn file_descriptor_limits_round_trip_all_limit_states() {
    for limits in [
        ServerDiagnosticsFileDescriptorLimits {
            soft: ServerDiagnosticsResourceLimit::Finite {
                value: "8192".to_string(),
            },
            hard: ServerDiagnosticsResourceLimit::Unlimited,
        },
        ServerDiagnosticsFileDescriptorLimits {
            soft: ServerDiagnosticsResourceLimit::Unknown,
            hard: ServerDiagnosticsResourceLimit::Unsupported,
        },
    ] {
        let serialized = serde_json::to_value(&limits).expect("limits should serialize");
        assert_eq!(
            serde_json::from_value::<ServerDiagnosticsFileDescriptorLimits>(serialized)
                .expect("limits should deserialize"),
            limits
        );
    }

    assert_eq!(
        serde_json::to_value(ServerDiagnosticsResourceLimit::Finite {
            value: "8192".to_string(),
        })
        .expect("finite limit should serialize"),
        json!({"type": "finite", "value": "8192"})
    );
    assert_eq!(
        serde_json::to_value(ServerDiagnosticsResourceLimit::Unlimited)
            .expect("unlimited limit should serialize"),
        json!({"type": "unlimited"})
    );
    assert_eq!(
        serde_json::to_value(ServerDiagnosticsResourceLimit::Unknown)
            .expect("unknown limit should serialize"),
        json!({"type": "unknown"})
    );
    assert_eq!(
        serde_json::to_value(ServerDiagnosticsResourceLimit::Unsupported)
            .expect("unsupported limit should serialize"),
        json!({"type": "unsupported"})
    );
}

#[test]
fn old_process_diagnostics_without_file_descriptor_limits_remain_readable() {
    let process: ServerDiagnosticsProcess = serde_json::from_value(json!({
        "id": 1234,
        "residentMemoryBytes": null,
        "physicalFootprintBytes": null
    }))
    .expect("legacy process diagnostics should deserialize");

    assert_eq!(process.file_descriptor_limits, None);
}
