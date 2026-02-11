-- Overview snapshots and alerts: metric_snapshot, alert_rule, alert_event,
-- notification_channel, user_notification_pref.
-- Backwards compatible: new tables only; no changes to existing tables.

-- 1. metric_snapshot: hourly/daily KPI snapshots for Overview (optional for MVP)
CREATE TABLE IF NOT EXISTS metric_snapshots (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP NOT NULL,
    scope VARCHAR(64) NOT NULL,
    kpi_key VARCHAR(128) NOT NULL,
    kpi_value DOUBLE PRECISION NOT NULL,
    dimensions_json JSONB NULL,
    computed_from VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_metric_snapshots_ts_kpi ON metric_snapshots(ts, kpi_key);
CREATE INDEX IF NOT EXISTS ix_metric_snapshots_scope ON metric_snapshots(scope);

-- 2. alert_rule: rule definitions (anomaly, threshold, freshness, pipeline_health)
CREATE TABLE IF NOT EXISTS alert_rules (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    scope VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    rule_type VARCHAR(64) NOT NULL,
    kpi_key VARCHAR(128) NULL,
    dimension_filters_json JSONB NULL,
    params_json JSONB NULL,
    schedule VARCHAR(32) NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_alert_rules_scope_enabled ON alert_rules(scope, is_enabled);

-- 3. alert_event: fired events from rules (open/ack/snoozed/resolved)
CREATE TABLE IF NOT EXISTS alert_events (
    id SERIAL PRIMARY KEY,
    rule_id INTEGER NOT NULL REFERENCES alert_rules(id),
    ts_detected TIMESTAMP NOT NULL,
    severity VARCHAR(32) NOT NULL,
    title VARCHAR(512) NOT NULL,
    message TEXT NOT NULL,
    context_json JSONB NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'open',
    assignee_user_id VARCHAR(128) NULL,
    related_entities_json JSONB NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_alert_events_ts_detected ON alert_events(ts_detected);
CREATE INDEX IF NOT EXISTS ix_alert_events_status ON alert_events(status);
CREATE INDEX IF NOT EXISTS ix_alert_events_rule ON alert_events(rule_id);

-- 4. notification_channel: delivery channels (email, slack_webhook)
CREATE TABLE IF NOT EXISTS notification_channels (
    id SERIAL PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    config_json JSONB NOT NULL,
    is_verified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL
);

-- 5. user_notification_pref: per-user, per-channel preferences
CREATE TABLE IF NOT EXISTS user_notification_prefs (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(128) NOT NULL,
    channel_id INTEGER NOT NULL REFERENCES notification_channels(id),
    severities_json JSONB NULL,
    digest_mode VARCHAR(32) NOT NULL DEFAULT 'realtime',
    quiet_hours_json JSONB NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    UNIQUE (user_id, channel_id)
);

CREATE INDEX IF NOT EXISTS ix_user_notification_prefs_user ON user_notification_prefs(user_id);
CREATE INDEX IF NOT EXISTS ix_user_notification_prefs_channel ON user_notification_prefs(channel_id);
