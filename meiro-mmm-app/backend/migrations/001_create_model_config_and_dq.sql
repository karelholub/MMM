-- Initial schema for model config versioning and data quality.
-- Target: PostgreSQL (JSONB), but compatible with SQLite (JSON).

CREATE TABLE IF NOT EXISTS model_configs (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(16) NOT NULL,
    version INTEGER NOT NULL,
    parent_id VARCHAR(36) NULL REFERENCES model_configs(id),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    change_note TEXT NULL,
    config_json JSONB NOT NULL,
    activated_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS ix_model_configs_name ON model_configs(name);

CREATE TABLE IF NOT EXISTS model_config_audit (
    id SERIAL PRIMARY KEY,
    model_config_id VARCHAR(36) NOT NULL REFERENCES model_configs(id),
    actor VARCHAR(255) NOT NULL,
    action VARCHAR(64) NOT NULL,
    diff_json JSONB NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_model_config_audit_cfg ON model_config_audit(model_config_id);

CREATE TABLE IF NOT EXISTS dq_snapshots (
    id SERIAL PRIMARY KEY,
    ts_bucket TIMESTAMP NOT NULL,
    source VARCHAR(64) NOT NULL,
    metric_key VARCHAR(128) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    meta_json JSONB NULL
);

CREATE INDEX IF NOT EXISTS ix_dq_snapshots_bucket ON dq_snapshots(ts_bucket);
CREATE INDEX IF NOT EXISTS ix_dq_snapshots_source ON dq_snapshots(source);
CREATE INDEX IF NOT EXISTS ix_dq_snapshots_metric ON dq_snapshots(metric_key);

CREATE TABLE IF NOT EXISTS dq_alert_rules (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    metric_key VARCHAR(128) NOT NULL,
    source VARCHAR(64),
    threshold_type VARCHAR(32) NOT NULL,
    threshold_value DOUBLE PRECISION NOT NULL,
    lookback_period_days INTEGER NOT NULL DEFAULT 7,
    severity VARCHAR(32) NOT NULL DEFAULT 'warn',
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS ix_dq_alert_rules_metric ON dq_alert_rules(metric_key);

CREATE TABLE IF NOT EXISTS dq_alerts (
    id SERIAL PRIMARY KEY,
    rule_id INTEGER NOT NULL REFERENCES dq_alert_rules(id),
    triggered_at TIMESTAMP NOT NULL,
    ts_bucket TIMESTAMP NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    baseline_value DOUBLE PRECISION,
    status VARCHAR(32) NOT NULL DEFAULT 'open',
    message TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_dq_alerts_rule ON dq_alerts(rule_id);

CREATE TABLE IF NOT EXISTS notification_endpoints (
    id SERIAL PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    target VARCHAR(512) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL
);

