use codex_app_server_protocol::ServerDiagnosticsGauge;
use codex_app_server_protocol::ServerDiagnosticsProcess;
use codex_app_server_protocol::ServerDiagnosticsResponse;

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
        },
        gauges,
    }
}
