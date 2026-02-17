-- Data sources inventory + secret references for connector credentials.

CREATE TABLE IF NOT EXISTS secret_store (
    id VARCHAR(36) PRIMARY KEY,
    workspace_id VARCHAR(128) NOT NULL DEFAULT 'default',
    kind VARCHAR(64) NOT NULL,
    secret_encrypted TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_secret_store_workspace_kind
  ON secret_store(workspace_id, kind);

CREATE TABLE IF NOT EXISTS data_sources (
    id VARCHAR(36) PRIMARY KEY,
    workspace_id VARCHAR(128) NOT NULL DEFAULT 'default',
    category VARCHAR(32) NOT NULL,
    type VARCHAR(64) NOT NULL,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'connected',
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret_ref VARCHAR(36) NULL REFERENCES secret_store(id) ON DELETE SET NULL,
    last_tested_at TIMESTAMP NULL,
    last_error TEXT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_data_sources_workspace_category
  ON data_sources(workspace_id, category);
CREATE INDEX IF NOT EXISTS ix_data_sources_workspace_type
  ON data_sources(workspace_id, type);
CREATE INDEX IF NOT EXISTS ix_data_sources_status
  ON data_sources(status);
