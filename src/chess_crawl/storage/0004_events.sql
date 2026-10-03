ALTER TABLE discovery_jobs ADD COLUMN revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE crawl_runs ADD COLUMN revision INTEGER NOT NULL DEFAULT 0;

CREATE TABLE event_archive_identity (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  id TEXT NOT NULL UNIQUE
);
INSERT INTO event_archive_identity(singleton, id) VALUES (1, lower(hex(randomblob(16))));

CREATE TABLE event_outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT NOT NULL CHECK (event_type IN ('job.updated', 'run.updated')),
  resource_id INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  occurred_at INTEGER NOT NULL,
  payload TEXT NOT NULL CHECK (json_valid(payload)),
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at REAL NOT NULL DEFAULT 0,
  delivered_at REAL,
  last_error TEXT,
  UNIQUE (event_type, resource_id, revision)
);
CREATE INDEX ix_event_outbox_pending ON event_outbox(id) WHERE delivered_at IS NULL;

-- Each state write and its event share the caller's transaction. Existing
-- archives begin at revision zero; migration does not manufacture past events.
CREATE TRIGGER jobs_event_insert AFTER INSERT ON discovery_jobs
BEGIN
  UPDATE discovery_jobs SET revision = 1 WHERE id = NEW.id;
  INSERT INTO event_outbox(event_type, resource_id, revision, occurred_at, payload)
  VALUES ('job.updated', NEW.id, 1, unixepoch(), json_object(
    'job_id', NEW.id, 'run_id', NEW.crawl_run_id, 'status', NEW.state,
    'provider', NEW.provider, 'kind', NEW.kind,
    'counters', json_object('attempts', NEW.attempts, 'retries', NEW.retry_count),
    'next_attempt_at', NEW.next_attempt_at
  ));
END;

CREATE TRIGGER jobs_event_update
AFTER UPDATE OF state, params_json, attempts, retry_count, next_attempt_at,
  reason, started_at, done_at ON discovery_jobs
WHEN NEW.state IS NOT OLD.state OR NEW.params_json IS NOT OLD.params_json
  OR NEW.attempts IS NOT OLD.attempts OR NEW.retry_count IS NOT OLD.retry_count
  OR NEW.next_attempt_at IS NOT OLD.next_attempt_at OR NEW.reason IS NOT OLD.reason
  OR NEW.started_at IS NOT OLD.started_at OR NEW.done_at IS NOT OLD.done_at
BEGIN
  UPDATE discovery_jobs SET revision = OLD.revision + 1 WHERE id = NEW.id;
  INSERT INTO event_outbox(event_type, resource_id, revision, occurred_at, payload)
  VALUES ('job.updated', NEW.id, OLD.revision + 1, unixepoch(), json_object(
    'job_id', NEW.id, 'run_id', NEW.crawl_run_id, 'status', NEW.state,
    'provider', NEW.provider, 'kind', NEW.kind,
    'counters', json_object('attempts', NEW.attempts, 'retries', NEW.retry_count),
    'next_attempt_at', NEW.next_attempt_at
  ));
END;

CREATE TRIGGER runs_event_insert AFTER INSERT ON crawl_runs
BEGIN
  UPDATE crawl_runs SET revision = 1 WHERE id = NEW.id;
  INSERT INTO event_outbox(event_type, resource_id, revision, occurred_at, payload)
  VALUES ('run.updated', NEW.id, 1, unixepoch(), json_object(
    'run_id', NEW.id, 'status', NEW.status, 'provider', NEW.provider,
    'counters', json(COALESCE(NEW.counters, '{}'))
  ));
END;

CREATE TRIGGER runs_event_update
AFTER UPDATE OF status, counters, finished_at ON crawl_runs
WHEN NEW.status IS NOT OLD.status OR NEW.counters IS NOT OLD.counters
  OR NEW.finished_at IS NOT OLD.finished_at
BEGIN
  UPDATE crawl_runs SET revision = OLD.revision + 1 WHERE id = NEW.id;
  INSERT INTO event_outbox(event_type, resource_id, revision, occurred_at, payload)
  VALUES ('run.updated', NEW.id, OLD.revision + 1, unixepoch(), json_object(
    'run_id', NEW.id, 'status', NEW.status, 'provider', NEW.provider,
    'counters', json(COALESCE(NEW.counters, '{}'))
  ));
END;
