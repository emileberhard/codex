-- Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
ALTER TABLE thread_turns ADD COLUMN updated_at_ordinal INTEGER NOT NULL DEFAULT 0;

-- Observation boundaries are committed projection prefixes. Keep the current turn
-- metadata at each such boundary, including interrupted turns that later recover.
CREATE TABLE thread_turn_versions (
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    version_ordinal INTEGER NOT NULL,
    status TEXT NOT NULL,
    error_json TEXT,
    started_at INTEGER,
    completed_at INTEGER,
    duration_ms INTEGER,
    first_user_item_id TEXT,
    final_agent_item_id TEXT,
    PRIMARY KEY (thread_id, turn_id, version_ordinal),
    FOREIGN KEY (thread_id, turn_id) REFERENCES thread_turns(thread_id, turn_id) ON DELETE CASCADE
);
-- Earlier intermediate states are unknown; only seed the authoritative current state.
INSERT INTO thread_turn_versions (thread_id, turn_id, version_ordinal, status, error_json, started_at, completed_at, duration_ms, first_user_item_id, final_agent_item_id)
SELECT t.thread_id, t.turn_id,
    MAX(t.rollout_ordinal, COALESCE(t.rollout_end_ordinal, 0), COALESCE(p.next_rollout_ordinal - 1, 0)),
    t.status, t.error_json, t.started_at, t.completed_at, t.duration_ms,
    t.first_user_item_id, t.final_agent_item_id
FROM thread_turns t LEFT JOIN thread_history_projection_state p ON p.thread_id = t.thread_id;

-- Current writers supply exact mutation ordinals, including during JSONL rebuilds.
-- Older writers omit that column: their updates may coalesce within a transaction,
-- stamped no earlier than its prior committed checkpoint. Observations can only
-- capture committed boundaries, so rollback writers cannot rewrite an old snapshot.
CREATE TRIGGER thread_observation_turn_insert AFTER INSERT ON thread_turns
BEGIN
    INSERT INTO thread_turn_versions (thread_id, turn_id, version_ordinal, status, error_json, started_at, completed_at, duration_ms, first_user_item_id, final_agent_item_id)
    VALUES (NEW.thread_id, NEW.turn_id, MAX(NEW.updated_at_ordinal, NEW.rollout_ordinal, COALESCE(NEW.rollout_end_ordinal, 0), COALESCE((SELECT next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = NEW.thread_id), 0)), NEW.status, NEW.error_json, NEW.started_at, NEW.completed_at, NEW.duration_ms, NEW.first_user_item_id, NEW.final_agent_item_id)
    ON CONFLICT(thread_id, turn_id, version_ordinal) DO UPDATE SET
        status = excluded.status,
        error_json = excluded.error_json,
        started_at = excluded.started_at,
        completed_at = excluded.completed_at,
        duration_ms = excluded.duration_ms,
        first_user_item_id = excluded.first_user_item_id,
        final_agent_item_id = excluded.final_agent_item_id;
END;
CREATE TRIGGER thread_observation_turn_update AFTER UPDATE ON thread_turns
BEGIN
    INSERT INTO thread_turn_versions (thread_id, turn_id, version_ordinal, status, error_json, started_at, completed_at, duration_ms, first_user_item_id, final_agent_item_id)
    VALUES (NEW.thread_id, NEW.turn_id, MAX(NEW.updated_at_ordinal, NEW.rollout_ordinal, COALESCE(NEW.rollout_end_ordinal, 0), COALESCE((SELECT next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = NEW.thread_id), 0)), NEW.status, NEW.error_json, NEW.started_at, NEW.completed_at, NEW.duration_ms, NEW.first_user_item_id, NEW.final_agent_item_id)
    ON CONFLICT(thread_id, turn_id, version_ordinal) DO UPDATE SET
        status = excluded.status,
        error_json = excluded.error_json,
        started_at = excluded.started_at,
        completed_at = excluded.completed_at,
        duration_ms = excluded.duration_ms,
        first_user_item_id = excluded.first_user_item_id,
        final_agent_item_id = excluded.final_agent_item_id;
END;
