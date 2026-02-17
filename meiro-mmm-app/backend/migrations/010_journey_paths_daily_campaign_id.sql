-- Optional campaign filter support for journey paths daily aggregates.

ALTER TABLE journey_paths_daily
  ADD COLUMN campaign_id VARCHAR(128) NULL;

CREATE INDEX IF NOT EXISTS ix_journey_paths_daily_campaign
  ON journey_paths_daily(campaign_id);
