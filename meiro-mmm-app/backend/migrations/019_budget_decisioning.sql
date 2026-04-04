CREATE TABLE IF NOT EXISTS budget_scenarios (
  id VARCHAR(36) PRIMARY KEY,
  run_id VARCHAR(64) NOT NULL,
  objective VARCHAR(64) NOT NULL,
  total_budget_change_pct FLOAT NOT NULL DEFAULT 0,
  multipliers_json JSON NOT NULL,
  summary_json JSON NOT NULL,
  created_by VARCHAR(255) NOT NULL DEFAULT 'system',
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_budget_scenarios_run_created
  ON budget_scenarios(run_id, created_at);

CREATE TABLE IF NOT EXISTS budget_recommendations (
  id VARCHAR(36) PRIMARY KEY,
  scenario_id VARCHAR(36),
  run_id VARCHAR(64) NOT NULL,
  objective VARCHAR(64) NOT NULL,
  rank INTEGER NOT NULL DEFAULT 0,
  scope VARCHAR(32) NOT NULL DEFAULT 'channel',
  status VARCHAR(32) NOT NULL DEFAULT 'warning',
  title VARCHAR(255) NOT NULL,
  summary TEXT NOT NULL,
  expected_impact_json JSON NOT NULL,
  confidence_json JSON NOT NULL,
  risk_json JSON NOT NULL,
  evidence_json JSON NOT NULL,
  decision_json JSON NOT NULL,
  created_at TIMESTAMP NOT NULL,
  FOREIGN KEY (scenario_id) REFERENCES budget_scenarios(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_budget_recommendations_run_objective
  ON budget_recommendations(run_id, objective);

CREATE TABLE IF NOT EXISTS budget_recommendation_actions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  recommendation_id VARCHAR(36) NOT NULL,
  channel VARCHAR(128) NOT NULL,
  campaign_id VARCHAR(128),
  action VARCHAR(32) NOT NULL,
  delta_pct FLOAT,
  delta_amount FLOAT,
  metadata_json JSON NOT NULL,
  created_at TIMESTAMP NOT NULL,
  FOREIGN KEY (recommendation_id) REFERENCES budget_recommendations(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_budget_recommendation_actions_rec
  ON budget_recommendation_actions(recommendation_id);
