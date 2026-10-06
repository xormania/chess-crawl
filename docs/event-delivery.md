# Optional event delivery

`CHESS_CRAWL_EVENTS_ENABLED` defaults to `true`. Every application database
connection applies it to its own PostgreSQL session. With `false`, migration
`0019` suppresses new Mercure notifications while job/run revisions, status
reads, ownership and SQS dispatch continue normally. Use the same setting on
every API and worker replica. Direct SQL clients that bypass the application
connection helper keep delivery enabled unless they explicitly set
`chess_crawl.events_enabled` to `false`.

An installation using only polling can select `false`; it needs neither a
Mercure hub nor a publisher. Changing this deployment setting requires
restarting all writers. Previously stored notifications remain until published
or explicitly discarded. Turning delivery on creates notifications for future
updates; it does not reconstruct updates made while delivery was off. Consumers
must refresh current API state when establishing or recovering a subscription.

The publisher retains delivered notifications for 24 hours by default and
deletes at most 256 per batch. Configure those limits with
`CHESS_CRAWL_EVENTS_RETENTION_SECONDS` and
`CHESS_CRAWL_EVENTS_CLEANUP_BATCH_SIZE`. Undelivered events survive automatic
cleanup, preserving retry ordering and stable event IDs. Failed delivery still
does not stop acquisition or processing. Monitor oldest pending-event age.

Trusted operators can prune a bounded batch independently of the publisher:

```bash
chess-crawl-admin prune-events --delivered-before 1791244800 --batch-size 256
```

An operator choosing to expire undelivered notifications must explicitly supply
`--discard-pending-before` with an occurrence-time cutoff. This discards
notifications, so subscribers must reconcile through current API state. It does
not delete source evidence, jobs, runs, or the SQS dispatch outbox. Each invocation
returns counts; repeat bounded batches until the selected backlog is gone.

Retention takes the same exclusive session lock as event publication. Run a
standalone cleanup after stopping the publisher; cleanup fails if another
publisher owns delivery, preventing deletion of an event while HTTP is in flight.
Apply packaged migrations before using these settings or commands.
