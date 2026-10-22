-- Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
-- Existing rows were projected only at completion. Preserve their ordinal as the legacy fallback.
ALTER TABLE thread_items ADD COLUMN is_complete INTEGER NOT NULL DEFAULT 1;

-- A fork reads the latest item snapshot inside its immutable source prefix.
CREATE TABLE thread_item_versions (
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    rollout_ordinal INTEGER NOT NULL,
    item_json TEXT NOT NULL,
    is_complete INTEGER NOT NULL,
    completed_at_ms INTEGER,
    PRIMARY KEY (thread_id, turn_id, item_id, rollout_ordinal),
    FOREIGN KEY (thread_id, turn_id, item_id)
        REFERENCES thread_items(thread_id, turn_id, item_id) ON DELETE CASCADE
);
INSERT INTO thread_item_versions (thread_id, turn_id, item_id, rollout_ordinal, item_json, is_complete, completed_at_ms)
SELECT thread_id, turn_id, item_id, updated_at_ordinal, item_json, 1, NULL FROM thread_items;
