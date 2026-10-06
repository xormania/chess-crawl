-- Delivery is optional; job/run revisions and the work-dispatch outbox are not.
CREATE FUNCTION guard_event_delivery() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('chess_crawl.events_enabled', true) = 'false' THEN
    RETURN NULL;
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER a_event_delivery BEFORE INSERT ON event_outbox
  FOR EACH ROW EXECUTE FUNCTION guard_event_delivery();

CREATE INDEX ix_event_outbox_delivered_retention ON event_outbox(delivered_at,id)
  WHERE delivered_at IS NOT NULL;
CREATE INDEX ix_event_outbox_pending_retention ON event_outbox(occurred_at,id)
  WHERE delivered_at IS NULL;
