-- Versioned journeys settings (draft/active/archived) with workspace linkage

CREATE TABLE IF NOT EXISTS journey_settings_versions (
  id VARCHAR(36) PRIMARY KEY,
  status VARCHAR(32) NOT NULL DEFAULT 'draft',
  version_label VARCHAR(64) NOT NULL,
  description TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by VARCHAR(255) NOT NULL DEFAULT 'system',
  activated_at TIMESTAMP,
  activated_by VARCHAR(255),
  settings_json JSON NOT NULL,
  validation_json JSON,
  diff_json JSON
);

CREATE INDEX IF NOT EXISTS ix_journey_settings_versions_status
  ON journey_settings_versions (status);

CREATE TABLE IF NOT EXISTS workspace_settings (
  workspace_id VARCHAR(128) PRIMARY KEY,
  active_journey_settings_version_id VARCHAR(36),
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_by VARCHAR(255),
  FOREIGN KEY (active_journey_settings_version_id)
    REFERENCES journey_settings_versions(id)
);

CREATE INDEX IF NOT EXISTS ix_workspace_settings_active_journeys
  ON workspace_settings (active_journey_settings_version_id);
