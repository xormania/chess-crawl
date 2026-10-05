-- Snapshot candidates and exact liveness counts do not scan terminal history.
CREATE INDEX ix_executor_heartbeat_recent ON executor_heartbeats(heartbeat_at DESC,worker_id);
CREATE INDEX ix_executor_heartbeat_live ON executor_heartbeats(heartbeat_expires_at,worker_id)
  WHERE status IN ('running','stopping');
CREATE INDEX ix_executor_heartbeat_terminal ON executor_heartbeats(heartbeat_at,worker_id)
  WHERE status IN ('stopped','failed');
