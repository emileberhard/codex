// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use codex_app_server_protocol::ServerDiagnosticsFileDescriptorLimits;
use codex_app_server_protocol::ServerDiagnosticsGauge;
use codex_app_server_protocol::ServerDiagnosticsProcess;
use codex_app_server_protocol::ServerDiagnosticsResourceLimit;
use codex_app_server_protocol::ServerDiagnosticsResponse;
use codex_diagnostics::FileDescriptorLimits;
use codex_diagnostics::ResourceLimitValue;

const NEW_WORK_ADMISSION_FENCED_GAUGE: &str = "app.work.new_admission_fenced";
const NEW_WORK_ADMISSION_CLOSED_GAUGE: &str = "app.work.new_admission_closed";

pub(crate) fn read_server_diagnostics(
    new_work_admission_closed: bool,
    new_work_admission_fenced: bool,
) -> ServerDiagnosticsResponse {
    let diagnostics = codex_diagnostics::snapshot();
    let mut gauges = diagnostics
        .gauges
        .into_iter()
        .map(|gauge| ServerDiagnosticsGauge {
            name: gauge.name.to_string(),
            value: gauge.value,
        })
        .collect::<Vec<_>>();
    gauges.push(ServerDiagnosticsGauge {
        name: NEW_WORK_ADMISSION_FENCED_GAUGE.to_string(),
        value: u64::from(new_work_admission_fenced),
    });
    gauges.push(ServerDiagnosticsGauge {
        name: NEW_WORK_ADMISSION_CLOSED_GAUGE.to_string(),
        value: u64::from(new_work_admission_closed),
    });
    gauges.sort_unstable_by(|left, right| left.name.cmp(&right.name));

    ServerDiagnosticsResponse {
        process: ServerDiagnosticsProcess {
            id: diagnostics.process.id,
            resident_memory_bytes: diagnostics.process.resident_memory_bytes,
            physical_footprint_bytes: diagnostics.process.physical_footprint_bytes,
            file_descriptor_limits: Some(map_file_descriptor_limits(
                diagnostics.process.file_descriptor_limits,
            )),
        },
        gauges,
    }
}

fn map_file_descriptor_limits(
    limits: FileDescriptorLimits,
) -> ServerDiagnosticsFileDescriptorLimits {
    ServerDiagnosticsFileDescriptorLimits {
        soft: map_resource_limit(limits.soft),
        hard: map_resource_limit(limits.hard),
    }
}

fn map_resource_limit(value: ResourceLimitValue) -> ServerDiagnosticsResourceLimit {
    match value {
        ResourceLimitValue::Finite(value) => ServerDiagnosticsResourceLimit::Finite {
            value: value.to_string(),
        },
        ResourceLimitValue::Unlimited => ServerDiagnosticsResourceLimit::Unlimited,
        ResourceLimitValue::Unknown => ServerDiagnosticsResourceLimit::Unknown,
        ResourceLimitValue::Unsupported => ServerDiagnosticsResourceLimit::Unsupported,
    }
}
