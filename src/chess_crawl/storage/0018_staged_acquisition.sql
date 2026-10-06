-- Opponent graph expansion is local processing, independent of provider permits.
ALTER TABLE discovery_jobs DROP CONSTRAINT discovery_jobs_kind_check;
ALTER TABLE discovery_jobs ADD CONSTRAINT discovery_jobs_kind_check CHECK(kind IN (
  'fetch_user_profile','fetch_user_stats','fetch_user_games','fetch_game_by_id',
  'crawl_opponents','fetch_user_resource','normalize_payload','reprocess_archive','expand_opponents'
));
CREATE INDEX ix_jobs_processing_parent ON discovery_jobs(parent_job_id,kind,state)
  WHERE parent_job_id IS NOT NULL AND kind IN ('normalize_payload','expand_opponents');
