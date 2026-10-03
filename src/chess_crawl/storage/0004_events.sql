ALTER TABLE discovery_jobs ADD COLUMN revision BIGINT NOT NULL DEFAULT 0;
ALTER TABLE crawl_runs ADD COLUMN revision BIGINT NOT NULL DEFAULT 0;

CREATE TABLE event_archive_identity (
  singleton BIGINT PRIMARY KEY CHECK (singleton = 1),
  id TEXT NOT NULL UNIQUE
);
INSERT INTO event_archive_identity(singleton, id) VALUES (1, replace(gen_random_uuid()::text, '-', ''));

CREATE TABLE event_outbox (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_type TEXT NOT NULL CHECK (event_type IN ('job.updated', 'run.updated')),
  resource_id BIGINT NOT NULL,
  revision BIGINT NOT NULL,
  occurred_at BIGINT NOT NULL,
  payload TEXT NOT NULL CHECK (json_typeof(payload::json) = 'object'),
  attempts BIGINT NOT NULL DEFAULT 0,
  next_attempt_at DOUBLE PRECISION NOT NULL DEFAULT 0,
  delivered_at DOUBLE PRECISION,
  last_error TEXT,
  UNIQUE (event_type, resource_id, revision)
);
CREATE INDEX ix_event_outbox_pending ON event_outbox(id) WHERE delivered_at IS NULL;

-- Set revisions before a write, then emit only after an actual insert/update.
-- INSERT ON CONFLICT DO NOTHING must never create events for discarded rows.
-- Both trigger phases belong to the caller's transaction.
CREATE FUNCTION set_job_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    NEW.revision := 1;
  ELSIF ROW(NEW.state, NEW.params_json, NEW.attempts, NEW.retry_count,
            NEW.next_attempt_at, NEW.reason, NEW.started_at, NEW.done_at)
        IS DISTINCT FROM
        ROW(OLD.state, OLD.params_json, OLD.attempts, OLD.retry_count,
            OLD.next_attempt_at, OLD.reason, OLD.started_at, OLD.done_at) THEN
    NEW.revision := OLD.revision + 1;
  ELSE
    NEW.revision := OLD.revision;
  END IF;
  RETURN NEW;
END;
$$;
CREATE FUNCTION record_job_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.revision = OLD.revision THEN
    RETURN NULL;
  END IF;
  INSERT INTO event_outbox(event_type, resource_id, revision, occurred_at, payload)
  VALUES ('job.updated', NEW.id, NEW.revision, floor(extract(epoch FROM clock_timestamp()))::bigint,
    json_build_object(
      'job_id', NEW.id, 'run_id', NEW.crawl_run_id, 'status', NEW.state,
      'provider', NEW.provider, 'kind', NEW.kind,
      'counters', json_build_object('attempts', NEW.attempts, 'retries', NEW.retry_count),
      'next_attempt_at', NEW.next_attempt_at
    )::text);
  RETURN NULL;
END;
$$;
CREATE TRIGGER jobs_event_insert BEFORE INSERT OR UPDATE OF state, params_json, attempts,
  retry_count, next_attempt_at, reason, started_at, done_at ON discovery_jobs
FOR EACH ROW EXECUTE FUNCTION set_job_revision();
CREATE TRIGGER jobs_event_update AFTER INSERT OR UPDATE OF state, params_json, attempts,
  retry_count, next_attempt_at, reason, started_at, done_at ON discovery_jobs
FOR EACH ROW EXECUTE FUNCTION record_job_event();

CREATE FUNCTION set_run_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    NEW.revision := 1;
  ELSIF ROW(NEW.status, NEW.counters, NEW.finished_at)
        IS DISTINCT FROM ROW(OLD.status, OLD.counters, OLD.finished_at) THEN
    NEW.revision := OLD.revision + 1;
  ELSE
    NEW.revision := OLD.revision;
  END IF;
  RETURN NEW;
END;
$$;
CREATE FUNCTION record_run_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.revision = OLD.revision THEN
    RETURN NULL;
  END IF;
  INSERT INTO event_outbox(event_type, resource_id, revision, occurred_at, payload)
  VALUES ('run.updated', NEW.id, NEW.revision, floor(extract(epoch FROM clock_timestamp()))::bigint,
    json_build_object(
      'run_id', NEW.id, 'status', NEW.status, 'provider', NEW.provider,
      'counters', COALESCE(NEW.counters, '{}')::json
    )::text);
  RETURN NULL;
END;
$$;
CREATE TRIGGER runs_event_insert BEFORE INSERT OR UPDATE OF status, counters, finished_at ON crawl_runs
FOR EACH ROW EXECUTE FUNCTION set_run_revision();
CREATE TRIGGER runs_event_update AFTER INSERT OR UPDATE OF status, counters, finished_at ON crawl_runs
FOR EACH ROW EXECUTE FUNCTION record_run_event();
