ALTER TABLE discovery_jobs ADD COLUMN owner_worker_id TEXT;
ALTER TABLE discovery_jobs ADD COLUMN owner_backend_pid BIGINT;
ALTER TABLE discovery_jobs ADD COLUMN ownership_token TEXT;
ALTER TABLE discovery_jobs ADD COLUMN ownership_generation BIGINT NOT NULL DEFAULT 0;
ALTER TABLE discovery_jobs DROP CONSTRAINT IF EXISTS discovery_jobs_kind_check;
ALTER TABLE discovery_jobs ADD CONSTRAINT discovery_jobs_kind_check CHECK(kind IN (
  'fetch_user_profile','fetch_user_stats','fetch_user_games','fetch_game_by_id',
  'crawl_opponents','fetch_user_resource','normalize_payload','reprocess_archive'
));

CREATE TABLE executor_heartbeats (
  worker_id TEXT PRIMARY KEY,
  started_at DOUBLE PRECISION NOT NULL,
  heartbeat_at DOUBLE PRECISION NOT NULL,
  heartbeat_expires_at DOUBLE PRECISION NOT NULL,
  stopped_at DOUBLE PRECISION,
  current_job_id BIGINT REFERENCES discovery_jobs(id),
  status TEXT NOT NULL CHECK(status IN ('running','stopping','stopped','failed'))
);

CREATE TABLE collection_checkpoints (
  job_id BIGINT PRIMARY KEY REFERENCES discovery_jobs(id),
  cursor JSONB NOT NULL CHECK(jsonb_typeof(cursor) = 'object'),
  updated_at BIGINT NOT NULL
);
CREATE TABLE collection_coverage (
  provider TEXT NOT NULL REFERENCES providers(key),
  username_normalized TEXT NOT NULL,
  unit_key TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('pending','complete','missing','error')),
  sealed BOOLEAN NOT NULL DEFAULT FALSE,
  raw_payload_id BIGINT REFERENCES raw_payloads(id),
  parser_version TEXT,
  fetched_at BIGINT NOT NULL,
  error TEXT,
  window_since_ms BIGINT CHECK(window_since_ms >= 0),
  window_until_ms BIGINT CHECK(window_until_ms >= 0),
  CHECK(window_since_ms IS NULL OR window_until_ms IS NULL OR window_until_ms >= window_since_ms),
  PRIMARY KEY(provider,username_normalized,unit_key)
);
CREATE TABLE collection_followups (
  provider TEXT NOT NULL REFERENCES providers(key),
  username_normalized TEXT NOT NULL,
  game_ref TEXT NOT NULL,
  created_ms BIGINT NOT NULL CHECK(created_ms >= 0),
  updated_at BIGINT NOT NULL,
  PRIMARY KEY(provider,username_normalized,game_ref)
);
CREATE TABLE collection_source_ranges (
  provider TEXT NOT NULL REFERENCES providers(key),
  username_normalized TEXT NOT NULL,
  raw_payload_id BIGINT NOT NULL REFERENCES raw_payloads(id),
  covered_since_ms BIGINT CHECK(covered_since_ms >= 0),
  covered_until_ms BIGINT NOT NULL CHECK(covered_until_ms >= 0),
  CHECK(covered_since_ms IS NULL OR covered_since_ms <= covered_until_ms),
  PRIMARY KEY(provider,username_normalized,raw_payload_id)
);
CREATE INDEX ix_collection_source_ranges_player
  ON collection_source_ranges(provider,username_normalized,covered_until_ms,raw_payload_id);
CREATE TABLE data_upgrades (
  id TEXT PRIMARY KEY,
  provider TEXT REFERENCES providers(key),
  owner_scope TEXT NOT NULL DEFAULT 'public',
  kind TEXT NOT NULL CHECK(kind IN ('normalization','backfill')),
  parser_version TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('pending','running','done','error')),
  high_water_raw_id BIGINT NOT NULL,
  last_raw_id BIGINT NOT NULL DEFAULT 0,
  processed BIGINT NOT NULL DEFAULT 0,
  error TEXT,
  job_id BIGINT REFERENCES discovery_jobs(id),
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE TABLE payload_normalization_runs (
  raw_payload_id BIGINT NOT NULL REFERENCES raw_payloads(id),
  parser_version TEXT NOT NULL,
  observation_id BIGINT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('pending','complete')),
  PRIMARY KEY(raw_payload_id,parser_version)
);
CREATE TABLE normalization_items (
  raw_payload_id BIGINT NOT NULL REFERENCES raw_payloads(id),
  parser_version TEXT NOT NULL,
  json_pointer TEXT NOT NULL,
  observation_id BIGINT NOT NULL,
  game_id BIGINT NOT NULL REFERENCES games(id),
  PRIMARY KEY(raw_payload_id,parser_version,json_pointer)
);

CREATE TABLE dispatch_outbox (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  job_id BIGINT NOT NULL REFERENCES discovery_jobs(id),
  job_revision BIGINT NOT NULL,
  attempts BIGINT NOT NULL DEFAULT 0,
  available_at DOUBLE PRECISION NOT NULL DEFAULT 0,
  delivered_at DOUBLE PRECISION,
  superseded_at DOUBLE PRECISION,
  last_error TEXT,
  UNIQUE(job_id,job_revision)
);
CREATE INDEX ix_dispatch_pending ON dispatch_outbox(available_at,id)
  WHERE delivered_at IS NULL AND superseded_at IS NULL;
CREATE FUNCTION record_dispatch_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.state='pending' OR (NEW.state='blocked' AND NEW.next_attempt_at IS NOT NULL) THEN
    INSERT INTO dispatch_outbox(job_id,job_revision,available_at)
      VALUES(NEW.id,NEW.revision,COALESCE(NEW.next_attempt_at,0)) ON CONFLICT DO NOTHING;
  END IF;
  RETURN NULL;
END;
$$;
CREATE TRIGGER jobs_dispatch AFTER INSERT OR UPDATE OF state, next_attempt_at ON discovery_jobs
  FOR EACH ROW EXECUTE FUNCTION record_dispatch_event();
INSERT INTO dispatch_outbox(job_id,job_revision,available_at)
  SELECT id,revision,COALESCE(next_attempt_at,0) FROM discovery_jobs
   WHERE state='pending' OR (state='blocked' AND next_attempt_at IS NOT NULL);
