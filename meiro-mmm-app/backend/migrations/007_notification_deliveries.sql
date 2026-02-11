-- Idempotency for alert notification delivery: one row per (alert_event_id, channel_id).
-- Prevents duplicate sends; supports retry tracking.

CREATE TABLE IF NOT EXISTS notification_deliveries (
    id SERIAL PRIMARY KEY,
    alert_event_id INTEGER NOT NULL REFERENCES alert_events(id) ON DELETE CASCADE,
    channel_id INTEGER NOT NULL REFERENCES notification_channels(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',  -- pending | sent | failed
    delivered_at TIMESTAMP NULL,
    last_error TEXT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    UNIQUE (alert_event_id, channel_id)
);

CREATE INDEX IF NOT EXISTS ix_notification_deliveries_alert ON notification_deliveries(alert_event_id);
CREATE INDEX IF NOT EXISTS ix_notification_deliveries_channel ON notification_deliveries(channel_id);
CREATE INDEX IF NOT EXISTS ix_notification_deliveries_status ON notification_deliveries(status);
