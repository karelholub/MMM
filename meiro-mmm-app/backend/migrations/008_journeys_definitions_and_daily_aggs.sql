-- Journey definitions and daily pre-aggregates for path/transitions ETL.
-- Minimal schema only: no ETL logic in this migration.

CREATE TABLE IF NOT EXISTS journey_definitions (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT NULL,
    conversion_kpi_id VARCHAR(64) NULL,
    lookback_window_days INTEGER NOT NULL DEFAULT 30,
    mode_default VARCHAR(32) NOT NULL DEFAULT 'conversion_only',
    created_by VARCHAR(255) NOT NULL DEFAULT 'system',
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT ck_journey_definitions_mode_default
      CHECK (mode_default IN ('conversion_only', 'all_journeys'))
);

CREATE INDEX IF NOT EXISTS ix_journey_definitions_name
  ON journey_definitions(name);

CREATE TABLE IF NOT EXISTS journey_paths_daily (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    journey_definition_id VARCHAR(36) NOT NULL REFERENCES journey_definitions(id) ON DELETE CASCADE,
    path_hash TEXT NOT NULL,
    path_steps JSONB NOT NULL,
    path_length INTEGER NOT NULL,
    count_journeys INTEGER NOT NULL DEFAULT 0,
    count_conversions INTEGER NOT NULL DEFAULT 0,
    avg_time_to_convert_sec DOUBLE PRECISION NULL,
    p50_time_to_convert_sec DOUBLE PRECISION NULL,
    p90_time_to_convert_sec DOUBLE PRECISION NULL,
    channel_group VARCHAR(128) NULL,
    device VARCHAR(64) NULL,
    country VARCHAR(64) NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_journey_paths_daily_def_date
  ON journey_paths_daily(journey_definition_id, date);
CREATE INDEX IF NOT EXISTS ix_journey_paths_daily_def_date_hash
  ON journey_paths_daily(journey_definition_id, date, path_hash);

CREATE TABLE IF NOT EXISTS journey_transitions_daily (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    journey_definition_id VARCHAR(36) NOT NULL REFERENCES journey_definitions(id) ON DELETE CASCADE,
    from_step TEXT NOT NULL,
    to_step TEXT NOT NULL,
    count_transitions INTEGER NOT NULL DEFAULT 0,
    count_profiles INTEGER NOT NULL DEFAULT 0,
    channel_group VARCHAR(128) NULL,
    device VARCHAR(64) NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_journey_transitions_daily_def_date
  ON journey_transitions_daily(journey_definition_id, date);
CREATE INDEX IF NOT EXISTS ix_journey_transitions_daily_def_date_from
  ON journey_transitions_daily(journey_definition_id, date, from_step);
