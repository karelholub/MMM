CREATE TABLE IF NOT EXISTS journey_definition_audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  journey_definition_id VARCHAR(36) NOT NULL,
  actor VARCHAR(255) NOT NULL,
  action VARCHAR(64) NOT NULL,
  diff_json JSON NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (journey_definition_id) REFERENCES journey_definitions(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_journey_definition_audit_definition_created
  ON journey_definition_audit(journey_definition_id, created_at DESC);
