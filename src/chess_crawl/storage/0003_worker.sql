ALTER TABLE discovery_jobs ADD COLUMN retry_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE discovery_jobs ADD COLUMN next_attempt_at DOUBLE PRECISION;

CREATE INDEX ix_jobs_due ON discovery_jobs(state, next_attempt_at, priority);

CREATE TABLE worker_heartbeats (
  id BIGINT PRIMARY KEY CHECK(id = 1),
  worker_id TEXT NOT NULL,
  started_at DOUBLE PRECISION NOT NULL,
  heartbeat_at DOUBLE PRECISION NOT NULL,
  heartbeat_expires_at DOUBLE PRECISION NOT NULL,
  stopped_at DOUBLE PRECISION,
  current_job_id BIGINT REFERENCES discovery_jobs(id),
  status TEXT NOT NULL CHECK(status IN ('running', 'stopping', 'stopped', 'failed'))
);

CREATE TABLE provider_cooldowns (
  provider TEXT PRIMARY KEY REFERENCES providers(key),
  not_before DOUBLE PRECISION NOT NULL,
  reason TEXT,
  updated_at DOUBLE PRECISION NOT NULL
);
