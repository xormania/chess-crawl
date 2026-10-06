-- Trusted service credentials are independent of workspace usage and ownership.
CREATE TABLE workspace_credentials (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  token_digest TEXT NOT NULL UNIQUE CHECK(token_digest ~ '^[a-f0-9]{64}$'),
  created_at BIGINT NOT NULL,
  revoked_at BIGINT
);
CREATE INDEX ix_workspace_credentials_owner ON workspace_credentials(workspace_id,created_at,id);

ALTER TABLE workspace_budget_policies ADD COLUMN managed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE workspace_budget_policies ADD COLUMN version BIGINT NOT NULL DEFAULT 1 CHECK(version > 0);
CREATE TABLE workspace_policy_history (
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  version BIGINT NOT NULL CHECK(version > 0),
  policy JSONB NOT NULL,
  managed BOOLEAN NOT NULL,
  changed_at BIGINT NOT NULL,
  PRIMARY KEY(workspace_id,version)
);
INSERT INTO workspace_policy_history(workspace_id,version,policy,managed,changed_at)
  SELECT workspace_id,version,policy,managed,updated_at FROM workspace_budget_policies;
-- Legacy runs did not record a policy revision; do not invent their provenance.
ALTER TABLE work_budgets ADD COLUMN workspace_policy_version BIGINT;
