-- Funnel definitions persisted per workspace/user.

CREATE TABLE IF NOT EXISTS funnels (
    id VARCHAR(36) PRIMARY KEY,
    journey_definition_id VARCHAR(36) NOT NULL REFERENCES journey_definitions(id) ON DELETE CASCADE,
    workspace_id VARCHAR(128) NOT NULL DEFAULT 'default',
    user_id VARCHAR(128) NOT NULL DEFAULT 'default',
    name VARCHAR(255) NOT NULL,
    description TEXT NULL,
    steps_json JSONB NOT NULL,
    counting_method VARCHAR(32) NOT NULL DEFAULT 'ordered',
    window_days INTEGER NOT NULL DEFAULT 30,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    created_by VARCHAR(255) NOT NULL DEFAULT 'system',
    updated_by VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_funnels_workspace_user
  ON funnels(workspace_id, user_id);
CREATE INDEX IF NOT EXISTS ix_funnels_journey_definition
  ON funnels(journey_definition_id);
CREATE INDEX IF NOT EXISTS ix_funnels_archived
  ON funnels(is_archived);
