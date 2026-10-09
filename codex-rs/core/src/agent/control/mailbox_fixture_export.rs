use super::*;
use base64::Engine;
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;

impl MailFixture {
    pub(super) async fn export_fixture(
        &self,
        representation: &str,
        mail: &[InterAgentCommunication],
    ) {
        let source_home = self.harness.config.codex_home.to_path_buf();
        let home = tokio::fs::canonicalize(&source_home)
            .await
            .expect("physical fixture home");
        let store = &self.child.session.services.thread_store;
        let stored = store
            .read_thread(codex_thread_store::ReadThreadParams {
                thread_id: self.child_id(),
                include_archived: false,
                include_history: false,
            })
            .await
            .expect("fixture metadata");
        assert_eq!(
            stored.history_mode,
            codex_protocol::protocol::ThreadHistoryMode::Paginated
        );
        let rollout_path =
            tokio::fs::canonicalize(stored.rollout_path.as_ref().expect("canonical child path"))
                .await
                .expect("physical canonical child path");
        let rollout = tokio::fs::read_to_string(&rollout_path)
            .await
            .expect("canonical child rollout");
        let mut lineage = BTreeMap::new();
        let mut turn_ids = std::collections::BTreeSet::new();
        let mut files = BTreeMap::new();
        for entry in walkdir::WalkDir::new(&home) {
            let entry = entry.expect("fixture session path");
            if !entry.file_type().is_file()
                || !entry.file_name().to_string_lossy().starts_with("rollout")
                || entry
                    .path()
                    .extension()
                    .is_none_or(|extension| extension != "jsonl")
            {
                continue;
            }
            let bytes = tokio::fs::read(entry.path())
                .await
                .expect("fixture rollout bytes");
            let data = std::str::from_utf8(&bytes).expect("native JSONL");
            for line in data.lines() {
                let row: serde_json::Value = serde_json::from_str(line).expect("native row");
                if row["type"] == "session_meta" {
                    lineage.insert(
                        row["payload"]["id"]
                            .as_str()
                            .expect("thread id")
                            .to_string(),
                        row["payload"].clone(),
                    );
                }
                if let Some(turn_id) = row["payload"]["turn_id"].as_str() {
                    turn_ids.insert(turn_id.to_string());
                }
            }
            files.insert(
                entry
                    .path()
                    .strip_prefix(&home)
                    .expect("relative rollout")
                    .to_string_lossy()
                    .into_owned(),
                base64::engine::general_purpose::STANDARD.encode(bytes),
            );
        }
        if let Some(state_db) = &self.harness.state_db {
            state_db.close().await;
        }
        let mut directory = tokio::fs::read_dir(&home).await.expect("fixture home");
        while let Some(entry) = directory.next_entry().await.expect("fixture file") {
            let name = entry.file_name().to_string_lossy().into_owned();
            if (name.starts_with("state_") || name.starts_with("thread_history_"))
                && (name.ends_with(".sqlite") || name.ends_with(".sqlite-wal"))
            {
                files.insert(
                    name,
                    base64::engine::general_purpose::STANDARD.encode(
                        tokio::fs::read(entry.path())
                            .await
                            .expect("native store snapshot"),
                    ),
                );
            }
        }
        let child = lineage
            .get(&self.child_id().to_string())
            .expect("child lineage");
        assert_eq!(
            child["parent_thread_id"],
            serde_json::json!(self.root.session.thread_id)
        );
        assert_eq!(child["multi_agent_version"], "v2");
        assert_eq!(child["history_mode"], "paginated");
        println!(
            "NATIVE_V2_MAIL_FIXTURE={}",
            serde_json::json!({
                "representation": representation, "threadId": self.child_id(),
                "parentThreadId": self.root.session.thread_id,
                "sessionId": self.child.session.session_id(),
                "turnIds": turn_ids, "lineage": lineage,
                "sourceHome": home, "sourceHomeAliases": [source_home, home], "rolloutPath": rollout_path.strip_prefix(&home).expect("relative canonical path"),
                "rollout": rollout, "files": files,
                "communications": mail,
                "modelInput": mail.iter().map(InterAgentCommunication::to_model_input_item).collect::<Vec<_>>(),
            })
        );
    }
}
