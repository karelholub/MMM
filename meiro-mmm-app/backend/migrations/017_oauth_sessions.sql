CREATE TABLE IF NOT EXISTS oauth_sessions (
    id VARCHAR(36) PRIMARY KEY,
    workspace_id VARCHAR(128) NOT NULL DEFAULT 'default',
    user_id VARCHAR(128) NOT NULL,
    provider_key VARCHAR(64) NOT NULL,
    state VARCHAR(128) NOT NULL UNIQUE,
    pkce_verifier_encrypted TEXT NOT NULL,
    return_url VARCHAR(1024) NULL,
    created_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    consumed_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS ix_oauth_sessions_workspace_provider
  ON oauth_sessions(workspace_id, provider_key);
CREATE INDEX IF NOT EXISTS ix_oauth_sessions_state
  ON oauth_sessions(state);
CREATE INDEX IF NOT EXISTS ix_oauth_sessions_expires_at
  ON oauth_sessions(expires_at);
CREATE INDEX IF NOT EXISTS ix_oauth_sessions_consumed_at
  ON oauth_sessions(consumed_at);
