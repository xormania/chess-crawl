CREATE TABLE application_submissions (
  idempotency_key TEXT PRIMARY KEY NOT NULL,
  operation TEXT NOT NULL CHECK(operation IN ('import', 'crawl')),
  request_json TEXT NOT NULL,
  crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs(id),
  job_ids_json TEXT NOT NULL,
  created_at BIGINT NOT NULL
);

CREATE INDEX ix_application_submissions_run ON application_submissions(crawl_run_id);

CREATE TABLE run_games (
  crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs(id),
  game_id BIGINT NOT NULL REFERENCES games(id),
  PRIMARY KEY (crawl_run_id, game_id)
);
