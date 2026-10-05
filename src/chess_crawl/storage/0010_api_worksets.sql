-- Public source archives are shared. Application work and outputs have owners.
CREATE TABLE workspaces (
  id TEXT PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9_-]{1,100}$'),
  created_at BIGINT NOT NULL
);
INSERT INTO workspaces(id, created_at) VALUES ('local', floor(extract(epoch FROM clock_timestamp()))::bigint);
ALTER TABLE crawl_runs ADD COLUMN workspace_id TEXT NOT NULL DEFAULT 'local' REFERENCES workspaces(id);
ALTER TABLE discovery_jobs ADD COLUMN workspace_id TEXT NOT NULL DEFAULT 'local' REFERENCES workspaces(id);
ALTER TABLE application_submissions ADD COLUMN workspace_id TEXT NOT NULL DEFAULT 'local' REFERENCES workspaces(id);
ALTER TABLE application_submissions DROP CONSTRAINT application_submissions_pkey;
ALTER TABLE application_submissions ADD PRIMARY KEY(workspace_id, idempotency_key);
ALTER TABLE application_submissions DROP CONSTRAINT application_submissions_operation_check;
ALTER TABLE application_submissions ADD CHECK(operation IN ('import', 'crawl', 'upgrade', 'resource'));
CREATE INDEX ix_runs_workspace ON crawl_runs(workspace_id, id);
CREATE INDEX ix_jobs_workspace ON discovery_jobs(workspace_id, id);

CREATE FUNCTION own_run() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.workspace_id := COALESCE(NULLIF(current_setting('chess_crawl.workspace_id', true), ''), NEW.workspace_id);
  RETURN NEW;
END;
$$;
CREATE TRIGGER a_run_owner BEFORE INSERT ON crawl_runs FOR EACH ROW EXECUTE FUNCTION own_run();
CREATE FUNCTION own_job() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.crawl_run_id IS NOT NULL THEN
    SELECT workspace_id INTO NEW.workspace_id FROM crawl_runs WHERE id = NEW.crawl_run_id;
  ELSIF NEW.parent_job_id IS NOT NULL THEN
    SELECT workspace_id INTO NEW.workspace_id FROM discovery_jobs WHERE id = NEW.parent_job_id;
  ELSE
    NEW.workspace_id := COALESCE(NULLIF(current_setting('chess_crawl.workspace_id', true), ''), NEW.workspace_id);
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER a_job_owner BEFORE INSERT ON discovery_jobs FOR EACH ROW EXECUTE FUNCTION own_job();

CREATE FUNCTION scope_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner TEXT;
BEGIN
  IF NEW.event_type = 'job.updated' THEN
    SELECT workspace_id INTO owner FROM discovery_jobs WHERE id = NEW.resource_id;
  ELSE
    SELECT workspace_id INTO owner FROM crawl_runs WHERE id = NEW.resource_id;
  END IF;
  NEW.payload := (NEW.payload::jsonb || jsonb_build_object('workspace_id', COALESCE(owner, 'local')))::text;
  RETURN NEW;
END;
$$;
CREATE TRIGGER event_workspace BEFORE INSERT ON event_outbox FOR EACH ROW EXECUTE FUNCTION scope_event();
UPDATE event_outbox SET payload = (payload::jsonb || '{"workspace_id":"local"}'::jsonb)::text;

CREATE TABLE working_sets (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  name TEXT NOT NULL,
  filters JSONB NOT NULL,
  settings JSONB NOT NULL,
  input_signature TEXT NOT NULL,
  member_count BIGINT NOT NULL,
  created_at BIGINT NOT NULL,
  UNIQUE(workspace_id, id)
);
CREATE INDEX ix_working_sets_workspace ON working_sets(workspace_id, id);
CREATE TABLE working_set_members (
  working_set_id BIGINT NOT NULL REFERENCES working_sets(id),
  ordinal BIGINT NOT NULL,
  game_version_id BIGINT NOT NULL REFERENCES game_versions(id),
  PRIMARY KEY(working_set_id, ordinal),
  UNIQUE(working_set_id, game_version_id)
);
CREATE TABLE working_set_submissions (
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  idempotency_key TEXT NOT NULL,
  request_json TEXT NOT NULL,
  working_set_id BIGINT NOT NULL,
  PRIMARY KEY(workspace_id, idempotency_key),
  FOREIGN KEY(workspace_id, working_set_id) REFERENCES working_sets(workspace_id, id)
);
CREATE TABLE analysis_results (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  input_signature TEXT NOT NULL,
  implementation TEXT NOT NULL,
  implementation_version TEXT NOT NULL,
  settings JSONB NOT NULL,
  result_signature TEXT NOT NULL,
  output JSONB NOT NULL,
  created_at BIGINT NOT NULL,
  UNIQUE(workspace_id, result_signature)
);
CREATE INDEX ix_results_inputs ON analysis_results(workspace_id, input_signature, id);

CREATE FUNCTION immutable_working_set() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.input_signature <> 'pending' THEN
    RAISE EXCEPTION 'A completed working set is immutable';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER immutable_working_set_metadata BEFORE UPDATE ON working_sets
FOR EACH ROW EXECUTE FUNCTION immutable_working_set();
CREATE FUNCTION immutable_working_set_member() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE selection BIGINT;
DECLARE signature TEXT;
BEGIN
  IF TG_OP = 'UPDATE' THEN
    SELECT input_signature INTO signature FROM working_sets WHERE id = OLD.working_set_id;
    IF signature <> 'pending' THEN
      RAISE EXCEPTION 'A completed working set membership is immutable';
    END IF;
  END IF;
  selection := CASE WHEN TG_OP = 'DELETE' THEN OLD.working_set_id ELSE NEW.working_set_id END;
  SELECT input_signature INTO signature FROM working_sets WHERE id = selection;
  IF signature <> 'pending' THEN
    RAISE EXCEPTION 'A completed working set membership is immutable';
  END IF;
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;
CREATE TRIGGER immutable_working_set_membership BEFORE INSERT OR UPDATE OR DELETE ON working_set_members
FOR EACH ROW EXECUTE FUNCTION immutable_working_set_member();
