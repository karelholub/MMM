-- Add missing dimensions to transition aggregates so filters match journey paths API.

ALTER TABLE IF EXISTS journey_transitions_daily
    ADD COLUMN IF NOT EXISTS campaign_id VARCHAR(128);

ALTER TABLE IF EXISTS journey_transitions_daily
    ADD COLUMN IF NOT EXISTS country VARCHAR(64);
