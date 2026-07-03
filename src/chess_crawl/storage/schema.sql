PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS providers (
  key TEXT PRIMARY KEY CHECK(key IN ('chess.com','lichess')),
  name TEXT NOT NULL,
  base_url TEXT NOT NULL,
  docs_url TEXT,
  added_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS errors (
  id INTEGER PRIMARY KEY,
  provider TEXT REFERENCES providers(key),
  url TEXT,
  endpoint_type TEXT,
  -- timeout/parse/stream are reserved for future structured failure classification.
  error_kind TEXT NOT NULL
    CHECK(error_kind IN ('http_404','http_410','http_429','timeout','parse','stream','other')),
  status_code INTEGER,
  message TEXT,
  occurred_at INTEGER NOT NULL,
  retry_count INTEGER NOT NULL DEFAULT 0,
  is_dead INTEGER NOT NULL DEFAULT 0 CHECK(is_dead IN (0,1))
);

CREATE INDEX IF NOT EXISTS ix_errors_url ON errors(url);
CREATE INDEX IF NOT EXISTS ix_errors_live ON errors(is_dead, occurred_at);

CREATE TABLE IF NOT EXISTS crawl_runs (
  id INTEGER PRIMARY KEY,
  seed_spec TEXT NOT NULL,
  provider TEXT NOT NULL REFERENCES providers(key),
  params_json TEXT,
  status TEXT NOT NULL CHECK(status IN ('running','paused','done','failed','cancelled')),
  counters TEXT,
  started_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  finished_at INTEGER
);

CREATE INDEX IF NOT EXISTS ix_runs_status ON crawl_runs(status);

CREATE TABLE IF NOT EXISTS discovery_jobs (
  id INTEGER PRIMARY KEY,
  crawl_run_id INTEGER REFERENCES crawl_runs(id),
  parent_job_id INTEGER REFERENCES discovery_jobs(id),
  provider TEXT NOT NULL REFERENCES providers(key),
  kind TEXT NOT NULL,
  target TEXT NOT NULL,
  params_json TEXT NOT NULL DEFAULT '{}',
  state TEXT NOT NULL DEFAULT 'pending'
    CHECK(state IN ('pending','in_progress','done','error','skipped','blocked')),
  priority INTEGER NOT NULL DEFAULT 100,
  depth INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  dedup_key TEXT NOT NULL,
  enqueued_at INTEGER NOT NULL,
  started_at INTEGER,
  done_at INTEGER,
  reason TEXT
);

CREATE INDEX IF NOT EXISTS ix_jobs_runnable ON discovery_jobs(state, priority, enqueued_at);
CREATE INDEX IF NOT EXISTS ix_jobs_run_state ON discovery_jobs(crawl_run_id, state);
CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_dedup_live
  ON discovery_jobs(dedup_key) WHERE state IN ('pending','in_progress');

CREATE TABLE IF NOT EXISTS raw_payloads (
  id INTEGER PRIMARY KEY,
  provider TEXT NOT NULL REFERENCES providers(key),
  endpoint_type TEXT NOT NULL,
  provider_url TEXT,
  canonical_source_key TEXT NOT NULL,
  request_params TEXT,
  response_status INTEGER NOT NULL,
  response_headers TEXT,
  content_type TEXT,
  fetched_at INTEGER NOT NULL,
  body_hash TEXT NOT NULL,
  body_compression TEXT NOT NULL DEFAULT 'none'
    CHECK(body_compression IN ('none','gzip')),
  raw_body BLOB NOT NULL,
  body_bytes INTEGER NOT NULL,
  parser_version TEXT,
  -- stale is reserved for future offline re-normalization of preserved raw payloads.
  normalization_status TEXT NOT NULL DEFAULT 'pending'
    CHECK(normalization_status IN ('pending','parsed','failed','skipped','stale')),
  normalized_at INTEGER,
  error_ref INTEGER REFERENCES errors(id)
);

CREATE INDEX IF NOT EXISTS ix_raw_provider_endpoint ON raw_payloads(provider, endpoint_type);
CREATE INDEX IF NOT EXISTS ix_raw_body_hash ON raw_payloads(body_hash);
CREATE INDEX IF NOT EXISTS ix_raw_norm_status ON raw_payloads(normalization_status)
  WHERE normalization_status IN ('pending','failed','stale');
CREATE INDEX IF NOT EXISTS ix_raw_canonical_key ON raw_payloads(canonical_source_key, fetched_at);

CREATE TABLE IF NOT EXISTS source_records (
  id INTEGER PRIMARY KEY,
  entity_type TEXT NOT NULL,
  entity_id INTEGER NOT NULL,
  provider TEXT NOT NULL REFERENCES providers(key),
  endpoint_type TEXT NOT NULL,
  source_key TEXT,
  json_pointer TEXT,
  raw_payload_id INTEGER NOT NULL REFERENCES raw_payloads(id),
  first_seen_at INTEGER NOT NULL,
  UNIQUE(entity_type, entity_id, raw_payload_id)
);

CREATE INDEX IF NOT EXISTS ix_srcrec_entity ON source_records(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS ix_srcrec_payload ON source_records(raw_payload_id);

CREATE TABLE IF NOT EXISTS time_controls (
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL CHECK(kind IN ('clock','correspondence')),
  initial_seconds INTEGER,
  increment_seconds INTEGER,
  days INTEGER,
  time_class TEXT NOT NULL
    CHECK(time_class IN ('bullet','blitz','rapid','classical','correspondence')),
  raw_label TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_tc_tuple
  ON time_controls(
    kind,
    COALESCE(initial_seconds,-1),
    COALESCE(increment_seconds,-1),
    COALESCE(days,-1),
    time_class,
    raw_label
  );

CREATE TABLE IF NOT EXISTS variants (
  id INTEGER PRIMARY KEY,
  canonical_name TEXT NOT NULL,
  provider TEXT NOT NULL REFERENCES providers(key),
  provider_native_name TEXT NOT NULL,
  mapped INTEGER NOT NULL DEFAULT 1 CHECK(mapped IN (0,1)),
  UNIQUE(provider, provider_native_name)
);

CREATE TABLE IF NOT EXISTS provider_users (
  id INTEGER PRIMARY KEY,
  provider TEXT NOT NULL REFERENCES providers(key),
  provider_user_id TEXT,
  username_normalized TEXT NOT NULL,
  display_username TEXT NOT NULL,
  account_status TEXT,
  title TEXT,
  first_seen_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_pu_provider_pid
  ON provider_users(provider, provider_user_id) WHERE provider_user_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_pu_provider_uname
  ON provider_users(provider, username_normalized);

CREATE TABLE IF NOT EXISTS user_snapshots (
  id INTEGER PRIMARY KEY,
  provider_user_id INTEGER NOT NULL REFERENCES provider_users(id),
  captured_at INTEGER NOT NULL,
  observed_username TEXT NOT NULL,
  status TEXT,
  title TEXT,
  country TEXT,
  followers INTEGER,
  patron INTEGER CHECK(patron IN (0,1)),
  count_all INTEGER,
  count_rated INTEGER,
  count_win INTEGER,
  count_loss INTEGER,
  count_draw INTEGER,
  perfs_or_stats TEXT,
  content_hash TEXT NOT NULL,
  raw_payload_id INTEGER NOT NULL REFERENCES raw_payloads(id),
  UNIQUE(provider_user_id, content_hash)
);

CREATE INDEX IF NOT EXISTS ix_snap_user_time ON user_snapshots(provider_user_id, captured_at);

CREATE TABLE IF NOT EXISTS games (
  id INTEGER PRIMARY KEY,
  provider TEXT NOT NULL REFERENCES providers(key),
  provider_game_id TEXT,
  canonical_url TEXT,
  content_hash TEXT NOT NULL,
  variant_id INTEGER NOT NULL REFERENCES variants(id),
  time_control_id INTEGER NOT NULL REFERENCES time_controls(id),
  rated INTEGER NOT NULL CHECK(rated IN (0,1)),
  outcome TEXT CHECK(outcome IN ('white_win','black_win','draw')),
  is_live INTEGER NOT NULL DEFAULT 0 CHECK(is_live IN (0,1)),
  status_raw TEXT,
  created_at INTEGER,
  ended_at INTEGER,
  -- ply_count and tournament_ref are reserved internal columns; current exports omit unset fields.
  ply_count INTEGER,
  eco TEXT,
  opening_name TEXT,
  opening_ply INTEGER,
  tournament_ref TEXT,
  first_seen_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_games_provider_gid
  ON games(provider, provider_game_id) WHERE provider_game_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_games_url
  ON games(canonical_url) WHERE canonical_url IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_games_content_hash ON games(content_hash);
CREATE INDEX IF NOT EXISTS ix_games_ended ON games(ended_at);
CREATE INDEX IF NOT EXISTS ix_games_provider_time ON games(provider, ended_at);
CREATE INDEX IF NOT EXISTS ix_games_live ON games(provider, is_live) WHERE is_live = 1;

CREATE TABLE IF NOT EXISTS game_participants (
  id INTEGER PRIMARY KEY,
  game_id INTEGER NOT NULL REFERENCES games(id),
  color TEXT NOT NULL CHECK(color IN ('white','black')),
  provider_user_id INTEGER REFERENCES provider_users(id),
  username_normalized TEXT,
  result_raw TEXT,
  is_winner INTEGER CHECK(is_winner IN (0,1)),
  is_ai INTEGER NOT NULL DEFAULT 0 CHECK(is_ai IN (0,1)),
  UNIQUE(game_id, color)
);

CREATE INDEX IF NOT EXISTS ix_gp_user ON game_participants(provider_user_id);
CREATE INDEX IF NOT EXISTS ix_gp_uname ON game_participants(username_normalized);

CREATE TABLE IF NOT EXISTS ratings_at_game (
  game_id INTEGER NOT NULL REFERENCES games(id),
  color TEXT NOT NULL CHECK(color IN ('white','black')),
  rating INTEGER,
  rating_diff INTEGER,
  rd INTEGER,
  PRIMARY KEY(game_id, color)
);

CREATE TABLE IF NOT EXISTS discovery_edges (
  id INTEGER PRIMARY KEY,
  crawl_run_id INTEGER REFERENCES crawl_runs(id),
  provider TEXT NOT NULL REFERENCES providers(key),
  from_user_id INTEGER NOT NULL REFERENCES provider_users(id),
  to_user_id INTEGER NOT NULL REFERENCES provider_users(id),
  via_game_id INTEGER REFERENCES games(id),
  game_count INTEGER NOT NULL DEFAULT 1,
  depth INTEGER NOT NULL,
  edge_kind TEXT NOT NULL DEFAULT 'opponent',
  first_seen_at INTEGER NOT NULL,
  UNIQUE(provider, from_user_id, to_user_id)
);

CREATE INDEX IF NOT EXISTS ix_edge_from ON discovery_edges(from_user_id);
CREATE INDEX IF NOT EXISTS ix_edge_to ON discovery_edges(to_user_id);

CREATE TABLE IF NOT EXISTS fetch_logs (
  id INTEGER PRIMARY KEY,
  provider TEXT NOT NULL REFERENCES providers(key),
  job_id INTEGER REFERENCES discovery_jobs(id),
  crawl_run_id INTEGER REFERENCES crawl_runs(id),
  url TEXT NOT NULL,
  endpoint_type TEXT NOT NULL,
  method TEXT NOT NULL DEFAULT 'GET',
  status_code INTEGER,
  from_cache INTEGER NOT NULL DEFAULT 0 CHECK(from_cache IN (0,1)),
  etag TEXT,
  last_modified TEXT,
  retry_after INTEGER,
  bytes INTEGER,
  duration_ms INTEGER,
  attempt INTEGER NOT NULL DEFAULT 1,
  attempted_at INTEGER NOT NULL,
  raw_payload_id INTEGER REFERENCES raw_payloads(id),
  error_ref INTEGER REFERENCES errors(id)
);

CREATE INDEX IF NOT EXISTS ix_fetchlog_time ON fetch_logs(provider, attempted_at);
CREATE INDEX IF NOT EXISTS ix_fetchlog_status ON fetch_logs(status_code);
CREATE INDEX IF NOT EXISTS ix_fetchlog_url ON fetch_logs(url);

INSERT INTO providers(key, name, base_url, docs_url, added_at)
VALUES
  ('chess.com', 'Chess.com', 'https://api.chess.com/pub', 'https://www.chess.com/news/view/published-data-api', CAST(strftime('%s','now') AS INTEGER)),
  ('lichess', 'Lichess', 'https://lichess.org/api', 'https://lichess.org/api', CAST(strftime('%s','now') AS INTEGER))
ON CONFLICT(key) DO NOTHING;
