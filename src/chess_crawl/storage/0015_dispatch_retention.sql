-- Reconcile legacy hints in bounded worker/dispatcher batches, not a migration
-- rewrite. New transitions retire their previous hint without an SQS process.
CREATE OR REPLACE FUNCTION record_dispatch_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF NEW.revision = OLD.revision THEN
      RETURN NULL;
    END IF;
    UPDATE dispatch_outbox
       SET superseded_at=extract(epoch FROM clock_timestamp())
     WHERE id IN (
       SELECT id FROM dispatch_outbox
        WHERE job_id=NEW.id AND job_revision=OLD.revision
          AND delivered_at IS NULL AND superseded_at IS NULL
        FOR UPDATE SKIP LOCKED
     );
  END IF;
  IF NEW.state='pending' OR (NEW.state='blocked' AND NEW.next_attempt_at IS NOT NULL) THEN
    INSERT INTO dispatch_outbox(job_id,job_revision,available_at)
      VALUES(NEW.id,NEW.revision,COALESCE(NEW.next_attempt_at,0)) ON CONFLICT DO NOTHING;
  END IF;
  RETURN NULL;
END;
$$;
DROP TRIGGER jobs_dispatch ON discovery_jobs;
CREATE TRIGGER jobs_dispatch AFTER INSERT OR UPDATE OF state, params_json, attempts,
  retry_count, next_attempt_at, reason, started_at, done_at ON discovery_jobs
  FOR EACH ROW EXECUTE FUNCTION record_dispatch_event();
CREATE INDEX ix_dispatch_history ON dispatch_outbox(COALESCE(delivered_at,superseded_at),id)
  WHERE COALESCE(delivered_at,superseded_at) IS NOT NULL;
CREATE INDEX ix_dispatch_pending_id ON dispatch_outbox(id)
  WHERE delivered_at IS NULL AND superseded_at IS NULL;
