ALTER TABLE experiments ADD COLUMN experiment_type VARCHAR(32) NOT NULL DEFAULT 'holdout';
ALTER TABLE experiments ADD COLUMN source_type VARCHAR(32);
ALTER TABLE experiments ADD COLUMN source_id VARCHAR(64);
ALTER TABLE experiments ADD COLUMN segment_json JSON NOT NULL DEFAULT '{}';
ALTER TABLE experiments ADD COLUMN policy_json JSON NOT NULL DEFAULT '{}';
ALTER TABLE experiments ADD COLUMN guardrails_json JSON NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS ix_experiments_experiment_type ON experiments (experiment_type);
CREATE INDEX IF NOT EXISTS ix_experiments_source_type ON experiments (source_type);
CREATE INDEX IF NOT EXISTS ix_experiments_source_id ON experiments (source_id);
