# Concurrent execution and data upgrades

Chess-Crawl keeps job/run state in PostgreSQL. Local polling and SQS both select
the same durable jobs; a message is a delivery hint, never an execution lease.

## Ownership and recovery

Workers claim eligible rows with `FOR UPDATE SKIP LOCKED`, then hold a PostgreSQL
session advisory lock for the claimed job. The row stores worker identity,
backend PID, a random fencing token, and a monotonically increasing generation.
Every outer write transaction verifies both the session locks and that token.
Nested operations share the outer transaction's verified ownership and row lock.
Losing the session or releasing its lock invalidates subsequent writes.

There are no expiring execution leases. A slow engine or expired heartbeat does
not authorize takeover. Recovery changes an in-progress job to pending only
after acquiring its job lock, which PostgreSQL releases on session death.
Recovery also runs while the queue is idle, so a lost delivery cannot strand a
crashed job. Use direct connections or a pool preserving complete sessions.

Acquisition also owns a provider-specific session lock. HTTP pacing and retry
deadlines are committed before sleeping and survive worker changes. Each HTTP
attempt checks execution rights before contacting a provider. Different
providers and local normalization jobs can execute independently. Ordinary
normalization uses a shared provider gate and deterministically ordered locks
for the affected games and accounts. Independent games and player profiles from
the same provider can commit concurrently. A certified rename or placeholder
merge restarts before mutation with the provider gate held exclusively.
Provider gates are never held while waiting for network responses.

PGN interpretation occurs outside write transactions. Each selected game commits
its evidence, source checkpoint, and run association atomically. An interrupted
source retains already committed games and resumes its remaining items. A raw
source becomes parsed only after every item for the current parser and fetch
observation is complete; a capped or windowed new observation stays pending.

Ordinary writes share a migration advisory gate. Migrations take that gate
exclusively; conflicting logical resources use separate transaction locks.
Idempotent submissions lock their key, live job deduplication locks its key,
and run/game attribution locks the run budget. The former archive-wide write
mutex is no longer the concurrency mechanism.

Worker heartbeat state now has one record per worker. `/v1/worker` includes
`workers` and `active_workers` while retaining the representative status fields.
Heartbeat records measure liveness independently from execution ownership.
The status response is capped at 100 records. Starting a worker removes expired
records older than seven days and excess inactive history, so autoscaling and
restart churn cannot grow the table or response without bound.

## Local execution and stages

`python -m chess_crawl.jobs.worker --stage all` polls PostgreSQL. Multiple
workers are supported. `--stage acquisition` selects provider/discovery jobs;
`--stage processing` selects normalization and archive-reprocessing jobs.
Full/incremental collection captures source batches, queues normalization jobs,
and yields between checkpoints. The existing bounded import/crawl path retains
synchronous normalization so its strict total game cap remains enforceable.

## SQS dispatch

Install the `s3` dependency extra (the same Boto3 SDK supplies SQS) and configure
`CHESS_CRAWL_SQS_QUEUE_URL`:

```bash
uv sync --locked --group dev --extra api --extra s3
python -m chess_crawl.jobs.dispatch
python -m chess_crawl.jobs.worker --stage all
```

Both modules accept `--queue-url` and `--once`. The SDK uses its default
credential chain, including ECS task roles. Local polling does not require the
SDK. SQS consumption requests one message with up to twenty seconds of long
polling. The consumer inherits the configured queue visibility timeout without
a per-receive override. Queue visibility is not a job lease; duplicate
deliveries cannot claim an owned or completed job. Failed publication keeps its
outbox entry for retry.
A crash after send and before the outbox acknowledgement can deliver twice.
Workers also poll PostgreSQL when a received hint cannot claim work or the queue
is empty, so expired or dead-lettered hints do not strand pending jobs.

The dispatcher sends only `job_id`. Pending continuations and delayed retries
create new outbox entries in the same transaction as the job transition.
Successful processing acknowledges a message only after that durable state is
committed. Malformed messages remain eligible for the configured dead-letter
policy. Configure queue retention, redrive policy, monitoring, and task roles in
the deployment. Use all-stage workers on a shared queue. Stage-specific pools
set both `CHESS_CRAWL_SQS_ACQUISITION_QUEUE_URL` and
`CHESS_CRAWL_SQS_PROCESSING_QUEUE_URL` for the dispatcher, which routes durable
jobs by kind. Each stage worker uses its matching queue environment variable.

## Offline normalization upgrades

`reprocess_archive` jobs take an upgrade identity, provider, parser version,
owner scope, and bounded `batch_size` between one and one hundred. Parser target
`current` resolves to the complete installed endpoint/parser manifest, which is
persisted with the upgrade. Unknown targets fail before replay; resuming after a
parser change requires a new upgrade identity. The first
execution records a raw-payload high-water mark. Each successfully replayed
payload advances a durable checkpoint; subsequent executions resume there.
Only public source records and the selected workspace's scoped sources are
included. Replay neither opens provider sockets nor fabricates fetch evidence.
An upgrade identity belongs to its original job, provider, and owner scope.
Rejected reuse preserves that job's progress and reports an error on the new job.

Schema migrations contain DDL only. Remote enrichment/backfills are explicit
`fetch_user_games` jobs using `collection_mode=backfill`; reading a report or
applying a schema migration does not download old games.

## Deployment upgrade

Stop old serial workers before applying migration `0009_scalable_execution`.
Run migrations once, then start the new workers and optional dispatcher. The
old singleton heartbeat table is retained for schema compatibility but is no
longer written. Back up PostgreSQL and external source objects together.
