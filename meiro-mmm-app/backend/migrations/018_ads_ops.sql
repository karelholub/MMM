CREATE TABLE IF NOT EXISTS ads_entity_map (
  id VARCHAR(36) PRIMARY KEY,
  workspace_id VARCHAR(128) NOT NULL DEFAULT 'default',
  provider VARCHAR(32) NOT NULL,
  account_id VARCHAR(128) NOT NULL DEFAULT 'default',
  entity_type VARCHAR(32) NOT NULL,
  entity_id VARCHAR(128) NOT NULL,
  entity_name VARCHAR(255),
  last_seen_ts TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ix_ads_entity_map_ws_provider_entity
  ON ads_entity_map(workspace_id, provider, entity_type, entity_id);
CREATE INDEX IF NOT EXISTS ix_ads_entity_map_ws_provider
  ON ads_entity_map(workspace_id, provider);
CREATE INDEX IF NOT EXISTS ix_ads_entity_map_seen
  ON ads_entity_map(last_seen_ts);

CREATE TABLE IF NOT EXISTS ads_change_requests (
  id VARCHAR(36) PRIMARY KEY,
  workspace_id VARCHAR(128) NOT NULL DEFAULT 'default',
  requested_by_user_id VARCHAR(128) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  account_id VARCHAR(128) NOT NULL,
  entity_type VARCHAR(32) NOT NULL,
  entity_id VARCHAR(128) NOT NULL,
  action_type VARCHAR(64) NOT NULL,
  action_payload_json JSON NOT NULL,
  status VARCHAR(32) NOT NULL,
  approval_required BOOLEAN NOT NULL DEFAULT 1,
  approved_by_user_id VARCHAR(128),
  approved_at TIMESTAMP,
  applied_at TIMESTAMP,
  error_message TEXT,
  idempotency_key VARCHAR(128),
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_ads_change_requests_ws_status
  ON ads_change_requests(workspace_id, status);
CREATE INDEX IF NOT EXISTS ix_ads_change_requests_ws_provider
  ON ads_change_requests(workspace_id, provider);
CREATE INDEX IF NOT EXISTS ix_ads_change_requests_entity
  ON ads_change_requests(provider, entity_type, entity_id);
CREATE INDEX IF NOT EXISTS ix_ads_change_requests_idempotency
  ON ads_change_requests(idempotency_key);

CREATE TABLE IF NOT EXISTS ads_audit_log (
  id VARCHAR(36) PRIMARY KEY,
  workspace_id VARCHAR(128) NOT NULL DEFAULT 'default',
  actor_user_id VARCHAR(128) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  account_id VARCHAR(128) NOT NULL,
  entity_type VARCHAR(32) NOT NULL,
  entity_id VARCHAR(128) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  event_payload_json JSON NOT NULL,
  created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_ads_audit_ws_created
  ON ads_audit_log(workspace_id, created_at);
CREATE INDEX IF NOT EXISTS ix_ads_audit_entity
  ON ads_audit_log(provider, entity_type, entity_id);
