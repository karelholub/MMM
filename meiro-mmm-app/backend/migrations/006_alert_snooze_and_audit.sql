-- Snooze and audit fields for alert_events and alert_rules.
-- alert_events: snooze_until (for snoozed status), updated_by (audit).
-- alert_rules: updated_by (audit).

ALTER TABLE alert_events ADD COLUMN snooze_until TIMESTAMP NULL;
ALTER TABLE alert_events ADD COLUMN updated_by VARCHAR(255) NULL;

ALTER TABLE alert_rules ADD COLUMN updated_by VARCHAR(255) NULL;
