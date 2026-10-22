-- Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
ALTER TABLE thread_turn_versions ADD COLUMN root_turn_id TEXT;
ALTER TABLE thread_item_versions ADD COLUMN started_at_ms INTEGER;

DROP TRIGGER thread_observation_turn_insert;
DROP TRIGGER thread_observation_turn_update;

CREATE TRIGGER thread_observation_turn_insert AFTER INSERT ON thread_turns
BEGIN
    INSERT INTO thread_turn_versions (thread_id, turn_id, version_ordinal, root_turn_id, status, error_json, started_at, completed_at, duration_ms, first_user_item_id, final_agent_item_id)
    VALUES (NEW.thread_id, NEW.turn_id, MAX(NEW.updated_at_ordinal, NEW.rollout_ordinal, COALESCE(NEW.rollout_end_ordinal, 0), COALESCE((SELECT next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = NEW.thread_id), 0)), NEW.root_turn_id, NEW.status, NEW.error_json, NEW.started_at, NEW.completed_at, NEW.duration_ms, NEW.first_user_item_id, NEW.final_agent_item_id)
    ON CONFLICT(thread_id, turn_id, version_ordinal) DO UPDATE SET
        root_turn_id = excluded.root_turn_id,
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
    INSERT INTO thread_turn_versions (thread_id, turn_id, version_ordinal, root_turn_id, status, error_json, started_at, completed_at, duration_ms, first_user_item_id, final_agent_item_id)
    VALUES (NEW.thread_id, NEW.turn_id, MAX(NEW.updated_at_ordinal, NEW.rollout_ordinal, COALESCE(NEW.rollout_end_ordinal, 0), COALESCE((SELECT next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = NEW.thread_id), 0)), NEW.root_turn_id, NEW.status, NEW.error_json, NEW.started_at, NEW.completed_at, NEW.duration_ms, NEW.first_user_item_id, NEW.final_agent_item_id)
    ON CONFLICT(thread_id, turn_id, version_ordinal) DO UPDATE SET
        root_turn_id = excluded.root_turn_id,
        status = excluded.status,
        error_json = excluded.error_json,
        started_at = excluded.started_at,
        completed_at = excluded.completed_at,
        duration_ms = excluded.duration_ms,
        first_user_item_id = excluded.first_user_item_id,
        final_agent_item_id = excluded.final_agent_item_id;
END;

DELETE FROM thread_item_versions;
DELETE FROM thread_turn_versions;
DELETE FROM thread_items;
DELETE FROM thread_realtime_items;
DELETE FROM thread_turns;
DELETE FROM thread_history_projection_state;
