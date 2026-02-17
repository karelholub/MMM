-- Add archive/audit fields for journey definitions (soft delete support).

ALTER TABLE journey_definitions
  ADD COLUMN is_archived BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE journey_definitions
  ADD COLUMN archived_at TIMESTAMP NULL;

ALTER TABLE journey_definitions
  ADD COLUMN updated_by VARCHAR(255) NULL;

ALTER TABLE journey_definitions
  ADD COLUMN archived_by VARCHAR(255) NULL;

CREATE INDEX IF NOT EXISTS ix_journey_definitions_archived
  ON journey_definitions(is_archived);
