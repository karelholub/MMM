-- Add note column to dq_alerts for operational context
-- SQLite: run once. PostgreSQL: use IF NOT EXISTS wrapper if needed.
ALTER TABLE dq_alerts ADD COLUMN note TEXT;
