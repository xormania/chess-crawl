-- Providerless internal work reuses durable admission, ownership, and dispatch.
ALTER TABLE crawl_runs ALTER COLUMN provider DROP NOT NULL;
ALTER TABLE crawl_runs ADD CONSTRAINT service_run_provider CHECK (
  provider IS NOT NULL OR COALESCE(params_json::jsonb->>'strategy' IN ('build_working_set','prepare_export'), false)
);
ALTER TABLE discovery_jobs ALTER COLUMN provider DROP NOT NULL;
ALTER TABLE discovery_jobs DROP CONSTRAINT discovery_jobs_kind_check;
ALTER TABLE discovery_jobs ADD CONSTRAINT discovery_jobs_kind_check CHECK(kind IN (
  'fetch_user_profile','fetch_user_stats','fetch_user_games','fetch_game_by_id',
  'crawl_opponents','fetch_user_resource','normalize_payload','reprocess_archive','expand_opponents',
  'build_working_set','prepare_export'
));
ALTER TABLE discovery_jobs ADD CONSTRAINT service_job_provider CHECK (
  (kind IN ('build_working_set','prepare_export') AND provider IS NULL)
  OR (kind NOT IN ('build_working_set','prepare_export') AND provider IS NOT NULL)
);
ALTER TABLE application_submissions DROP CONSTRAINT application_submissions_operation_check;
ALTER TABLE application_submissions ADD CONSTRAINT application_submissions_operation_check
  CHECK(operation IN ('import','crawl','upgrade','resource','build_working_set','prepare_export'));

CREATE TABLE archive_jobs (
  job_id BIGINT PRIMARY KEY REFERENCES discovery_jobs(id),
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  operation TEXT NOT NULL CHECK(operation IN ('build_working_set','prepare_export')),
  contract_version INTEGER NOT NULL DEFAULT 1 CHECK(contract_version=1),
  request JSONB NOT NULL CHECK(jsonb_typeof(request)='object'),
  working_set_id BIGINT,
  UNIQUE(job_id,workspace_id),
  FOREIGN KEY(workspace_id,working_set_id) REFERENCES working_sets(workspace_id,id)
);
CREATE TABLE archive_artifacts (
  job_id BIGINT PRIMARY KEY,
  FOREIGN KEY(job_id,workspace_id) REFERENCES archive_jobs(job_id,workspace_id),
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','building','ready','pruning','expired')),
  reserved_bytes BIGINT NOT NULL CHECK(reserved_bytes>0),
  created_at BIGINT NOT NULL,
  expires_at BIGINT NOT NULL,
  UNIQUE(job_id,workspace_id),
  manifest JSONB CHECK(manifest IS NULL OR jsonb_typeof(manifest)='object')
);
CREATE INDEX ix_artifacts_workspace ON archive_artifacts(workspace_id,state,job_id);
CREATE INDEX ix_artifacts_expiry ON archive_artifacts(expires_at,job_id) WHERE state<>'expired';
CREATE TABLE artifact_chunks (
  job_id BIGINT NOT NULL REFERENCES archive_artifacts(job_id),
  ordinal INTEGER NOT NULL CHECK(ordinal>0),
  archive_object_id BIGINT NOT NULL REFERENCES archive_objects(id),
  PRIMARY KEY(job_id,ordinal)
);
CREATE TABLE artifact_downloads (
  lease_id TEXT PRIMARY KEY,
  job_id BIGINT NOT NULL REFERENCES archive_artifacts(job_id),
  workspace_id TEXT NOT NULL REFERENCES workspaces(id),
  expires_at DOUBLE PRECISION NOT NULL,
  FOREIGN KEY(job_id,workspace_id) REFERENCES archive_artifacts(job_id,workspace_id)
);
CREATE INDEX ix_artifact_downloads_workspace ON artifact_downloads(workspace_id,expires_at);
CREATE INDEX ix_artifact_downloads_job ON artifact_downloads(job_id,expires_at);

CREATE FUNCTION immutable_ready_artifact() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.state='ready' AND (NEW.state NOT IN ('ready','pruning') OR NEW.manifest IS DISTINCT FROM OLD.manifest OR NEW.reserved_bytes<>OLD.reserved_bytes OR NEW.workspace_id<>OLD.workspace_id OR NEW.job_id<>OLD.job_id OR NEW.created_at<>OLD.created_at OR NEW.expires_at<>OLD.expires_at) THEN
    RAISE EXCEPTION 'A ready archive artifact is immutable';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER immutable_ready_archive_artifact BEFORE UPDATE ON archive_artifacts
FOR EACH ROW EXECUTE FUNCTION immutable_ready_artifact();

-- Async identities cannot collide with caller-selected synchronous keys.
ALTER TABLE working_set_submissions ADD COLUMN submission_namespace TEXT NOT NULL DEFAULT 'api';
ALTER TABLE working_set_submissions DROP CONSTRAINT working_set_submissions_pkey;
ALTER TABLE working_set_submissions ADD PRIMARY KEY(workspace_id,submission_namespace,idempotency_key);

CREATE FUNCTION guard_artifact_chunks() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE artifact_state TEXT;
BEGIN
  SELECT state INTO artifact_state FROM archive_artifacts WHERE job_id=COALESCE(NEW.job_id,OLD.job_id) FOR UPDATE;
  IF artifact_state NOT IN ('building','pruning') THEN
    RAISE EXCEPTION 'A finalized artifact chunk is immutable';
  END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER guard_private_artifact_chunks BEFORE INSERT OR UPDATE OR DELETE ON artifact_chunks
FOR EACH ROW EXECUTE FUNCTION guard_artifact_chunks();

CREATE FUNCTION immutable_archive_job() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.job_id<>OLD.job_id OR NEW.workspace_id<>OLD.workspace_id OR NEW.operation<>OLD.operation OR NEW.contract_version<>OLD.contract_version OR NEW.request<>OLD.request OR (OLD.working_set_id IS NOT NULL AND NEW.working_set_id IS DISTINCT FROM OLD.working_set_id) THEN
    RAISE EXCEPTION 'An admitted archive request and finalized selection are immutable';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER immutable_archive_job_request BEFORE UPDATE ON archive_jobs FOR EACH ROW EXECUTE FUNCTION immutable_archive_job();
