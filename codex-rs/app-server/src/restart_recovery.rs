// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use std::collections::HashMap;
use std::sync::Mutex;

use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::ThreadRestartRecoverParams;
use codex_protocol::ThreadId;
use codex_utils_redacted_string::RedactedString;
use serde::Deserialize;

use crate::error_code::internal_error;
use crate::error_code::invalid_request;

const RESTART_RECOVERY_GRANT_SCHEMA_VERSION: u8 = 1;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RestartRecoveryGrantFile {
    schema_version: u8,
    generation_id: String,
    capability: RedactedString,
    targets: Vec<RestartRecoveryTargetFile>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RestartRecoveryTargetFile {
    thread_id: String,
    turn_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartRecoveryGrant {
    generation_id: String,
    capability: RedactedString,
    targets: HashMap<ThreadId, String>,
}

impl TryFrom<RestartRecoveryGrantFile> for RestartRecoveryGrant {
    type Error = std::io::Error;

    fn try_from(file: RestartRecoveryGrantFile) -> Result<Self, Self::Error> {
        if file.schema_version != RESTART_RECOVERY_GRANT_SCHEMA_VERSION {
            return Err(invalid_grant("unsupported schema version"));
        }
        if file.generation_id.is_empty() || file.generation_id.len() > 128 {
            return Err(invalid_grant("generationId is invalid"));
        }
        if file.capability.len() < 32 {
            return Err(invalid_grant("capability is invalid"));
        }
        if file.targets.len() > 10_000 {
            return Err(invalid_grant("too many recovery targets"));
        }
        let mut targets = HashMap::new();
        for target in file.targets {
            if target.thread_id.len() > 128 || target.turn_id.len() > 128 {
                return Err(invalid_grant("recovery target id is too long"));
            }
            let thread_id = ThreadId::from_string(&target.thread_id)
                .map_err(|error| invalid_grant(format!("invalid threadId: {error}")))?;
            if target.turn_id.is_empty() {
                return Err(invalid_grant("turnId is empty"));
            }
            if targets.insert(thread_id, target.turn_id).is_some() {
                return Err(invalid_grant(format!(
                    "duplicate recovery target {thread_id}"
                )));
            }
        }
        Ok(Self {
            generation_id: file.generation_id,
            capability: file.capability,
            targets,
        })
    }
}

fn invalid_grant(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

#[derive(Debug)]
pub(crate) struct RestartRecoveryAuthorizer {
    grant: Mutex<Option<RestartRecoveryGrant>>,
}

impl RestartRecoveryAuthorizer {
    pub(crate) fn new(grant: Option<RestartRecoveryGrant>) -> Self {
        Self {
            grant: Mutex::new(grant),
        }
    }

    pub(crate) fn authorize(
        &self,
        params: &ThreadRestartRecoverParams,
    ) -> Result<String, JSONRPCErrorError> {
        let grant = self
            .grant
            .lock()
            .map_err(|_| internal_error("restart recovery grant lock poisoned"))?;
        let grant = grant
            .as_ref()
            .ok_or_else(|| invalid_request("restart recovery is not authorized"))?;
        let capability_matches = grant.capability.len() == params.capability.len()
            && constant_time_eq::constant_time_eq(
                grant.capability.as_bytes(),
                params.capability.as_bytes(),
            );
        if !capability_matches {
            return Err(invalid_request("invalid restart recovery capability"));
        }
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|error| invalid_request(format!("invalid thread id: {error}")))?;
        if grant.targets.get(&thread_id) != Some(&params.turn_id) {
            return Err(invalid_request(
                "thread and turn are not authorized for this restart generation",
            ));
        }
        Ok(grant.generation_id.clone())
    }

    pub(crate) fn clear(&self) {
        match self.grant.lock() {
            Ok(mut grant) => *grant = None,
            Err(poisoned) => *poisoned.into_inner() = None,
        }
    }
}

#[cfg(test)]
#[path = "restart_recovery_tests.rs"]
mod tests;
