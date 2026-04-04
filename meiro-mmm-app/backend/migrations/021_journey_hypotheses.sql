CREATE TABLE IF NOT EXISTS journey_hypotheses (
  id VARCHAR(36) PRIMARY KEY,
  workspace_id VARCHAR(36) NOT NULL DEFAULT 'default',
  journey_definition_id VARCHAR(36) NOT NULL,
  owner_user_id VARCHAR(128) NOT NULL,
  title VARCHAR(255) NOT NULL,
  target_kpi VARCHAR(64),
  hypothesis_text TEXT NOT NULL,
  trigger_json JSON NOT NULL,
  segment_json JSON NOT NULL,
  current_action_json JSON NOT NULL,
  proposed_action_json JSON NOT NULL,
  support_count INTEGER NOT NULL DEFAULT 0,
  baseline_rate FLOAT,
  sample_size_target INTEGER,
  status VARCHAR(32) NOT NULL DEFAULT 'draft',
  linked_experiment_id INTEGER,
  result_json JSON NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  FOREIGN KEY (journey_definition_id) REFERENCES journey_definitions(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_journey_hypotheses_ws_def_created
  ON journey_hypotheses(workspace_id, journey_definition_id, created_at);
