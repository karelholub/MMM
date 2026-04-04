CREATE TABLE IF NOT EXISTS budget_realization_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  scenario_id VARCHAR(36) NOT NULL,
  run_id VARCHAR(64) NOT NULL,
  snapshot_json JSON NOT NULL,
  created_at TIMESTAMP NOT NULL,
  FOREIGN KEY (scenario_id) REFERENCES budget_scenarios(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_budget_realization_run_created
  ON budget_realization_snapshots(run_id, created_at);
