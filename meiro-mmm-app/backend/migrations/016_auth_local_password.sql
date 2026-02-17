-- Local credential login scaffolding (extendable to SSO/OIDC providers)

ALTER TABLE users ADD COLUMN username VARCHAR(64);
ALTER TABLE users ADD COLUMN password_hash VARCHAR(255);
ALTER TABLE users ADD COLUMN password_updated_at TIMESTAMP;

CREATE UNIQUE INDEX IF NOT EXISTS ix_users_username ON users (username);
