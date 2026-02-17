-- Server-side session storage for cookie auth

CREATE TABLE IF NOT EXISTS auth_sessions (
  id VARCHAR(64) PRIMARY KEY,
  user_id VARCHAR(36) NOT NULL,
  workspace_id VARCHAR(36) NOT NULL,
  csrf_token VARCHAR(64) NOT NULL,
  expires_at TIMESTAMP NOT NULL,
  revoked_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ip VARCHAR(64),
  user_agent VARCHAR(512),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_auth_sessions_user_id
  ON auth_sessions(user_id);
CREATE INDEX IF NOT EXISTS ix_auth_sessions_workspace_id
  ON auth_sessions(workspace_id);
CREATE INDEX IF NOT EXISTS ix_auth_sessions_expires_at
  ON auth_sessions(expires_at);
CREATE INDEX IF NOT EXISTS ix_auth_sessions_revoked_at
  ON auth_sessions(revoked_at);
