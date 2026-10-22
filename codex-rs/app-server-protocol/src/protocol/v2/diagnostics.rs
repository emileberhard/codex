// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use crate::JsonSchema;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ServerDiagnosticsParams {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ServerDiagnosticsResponse {
    pub process: ServerDiagnosticsProcess,
    pub gauges: Vec<ServerDiagnosticsGauge>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ServerDiagnosticsProcess {
    pub id: u32,
    #[ts(type = "number | null")]
    pub resident_memory_bytes: Option<u64>,
    #[ts(type = "number | null")]
    pub physical_footprint_bytes: Option<u64>,
    /// The process's soft and hard RLIMIT_NOFILE values, when the host can report them.
    ///
    /// Each value explicitly distinguishes a finite limit, an unlimited limit, a failed
    /// operating-system query, and an unsupported platform. The field is optional so clients can
    /// continue to read responses from older app-server versions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub file_descriptor_limits: Option<ServerDiagnosticsFileDescriptorLimits>,
}

/// The soft and hard file-descriptor limits for the app-server process.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ServerDiagnosticsFileDescriptorLimits {
    pub soft: ServerDiagnosticsResourceLimit,
    pub hard: ServerDiagnosticsResourceLimit,
}

/// One explicitly classified operating-system resource-limit value.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "camelCase")]
#[ts(tag = "type", export_to = "v2/")]
pub enum ServerDiagnosticsResourceLimit {
    /// A finite limit in the host's native unit, encoded as decimal text so clients do not lose
    /// precision when the host reports a value larger than JavaScript's safe integer range.
    Finite { value: String },
    /// The host reports no finite limit.
    Unlimited,
    /// The operating-system query failed.
    Unknown,
    /// The current platform does not expose this limit through the diagnostics API.
    Unsupported,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ServerDiagnosticsGauge {
    pub name: String,
    #[ts(type = "number")]
    pub value: u64,
}

#[cfg(test)]
#[path = "diagnostics_tests.rs"]
mod tests;
