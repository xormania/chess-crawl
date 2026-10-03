ALTER TABLE discovery_jobs ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE discovery_jobs ADD COLUMN next_attempt_at REAL;

CREATE INDEX ix_jobs_due ON discovery_jobs(state, next_attempt_at, priority);

CREATE TABLE worker_heartbeats (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  worker_id TEXT NOT NULL,
  started_at REAL NOT NULL,
  heartbeat_at REAL NOT NULL,
  heartbeat_expires_at REAL NOT NULL,
  stopped_at REAL,
  current_job_id INTEGER REFERENCES discovery_jobs(id),
  status TEXT NOT NULL CHECK(status IN ('running', 'stopping', 'stopped', 'failed'))
);

CREATE TABLE provider_cooldowns (
  provider TEXT PRIMARY KEY REFERENCES providers(key),
  not_before REAL NOT NULL,
  reason TEXT,
  updated_at REAL NOT NULL
);
