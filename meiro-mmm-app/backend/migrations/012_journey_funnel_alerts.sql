-- Journeys/Funnels alert definitions and fired events.
-- Uses dedicated tables to avoid conflict with existing generic alert_events/alert_rules.

CREATE TABLE IF NOT EXISTS journey_alert_definitions (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(64) NOT NULL,
    domain VARCHAR(32) NOT NULL DEFAULT 'journeys',
    scope_json JSONB NOT NULL,
    metric VARCHAR(128) NOT NULL,
    condition_json JSONB NOT NULL,
    schedule_json JSONB NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_by VARCHAR(255) NOT NULL DEFAULT 'system',
    updated_by VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_journey_alert_definitions_type
  ON journey_alert_definitions(type);
CREATE INDEX IF NOT EXISTS ix_journey_alert_definitions_domain
  ON journey_alert_definitions(domain);
CREATE INDEX IF NOT EXISTS ix_journey_alert_definitions_domain_enabled
  ON journey_alert_definitions(domain, is_enabled);

CREATE TABLE IF NOT EXISTS journey_alert_events (
    id VARCHAR(36) PRIMARY KEY,
    alert_definition_id VARCHAR(36) NOT NULL REFERENCES journey_alert_definitions(id) ON DELETE CASCADE,
    domain VARCHAR(32) NOT NULL DEFAULT 'journeys',
    triggered_at TIMESTAMP NOT NULL,
    severity VARCHAR(32) NOT NULL,
    summary TEXT NOT NULL,
    details_json JSONB NOT NULL,
    dedupe_key VARCHAR(128) NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_journey_alert_events_definition
  ON journey_alert_events(alert_definition_id);
CREATE INDEX IF NOT EXISTS ix_journey_alert_events_domain_triggered
  ON journey_alert_events(domain, triggered_at DESC);
CREATE INDEX IF NOT EXISTS ix_journey_alert_events_definition_triggered
  ON journey_alert_events(alert_definition_id, triggered_at DESC);
CREATE INDEX IF NOT EXISTS ix_journey_alert_events_dedupe
  ON journey_alert_events(dedupe_key);
