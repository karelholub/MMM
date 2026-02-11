-- Add fingerprint to alert_events for de-duplication (rule_id + dims + period).
-- Used to avoid spamming identical alerts and to update/resolve existing open alerts.

ALTER TABLE alert_events ADD COLUMN fingerprint VARCHAR(64) NULL;

CREATE INDEX IF NOT EXISTS ix_alert_events_fingerprint ON alert_events(fingerprint);
