-- Keep quota/owned snapshot queries independent of terminal archive history.
CREATE INDEX ix_jobs_budget ON discovery_jobs(work_budget_id,id);
CREATE INDEX ix_jobs_workspace_unfinished ON discovery_jobs(workspace_id,state)
  WHERE state IN ('pending','blocked','in_progress');
-- Exact job capture replay must not scan the shared provider observation log.
CREATE INDEX ix_fetchlog_job_success ON fetch_logs(job_id,attempted_at DESC,id DESC)
  WHERE status_code IN (200,304) AND raw_payload_id IS NOT NULL;
