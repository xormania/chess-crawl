# Concurrent execution and data upgrades

Chess-Crawl keeps job/run state in PostgreSQL. Local polling and SQS both select
the same durable jobs; a message is a delivery hint, never an execution lease.

See [durable work budgets](work-budgets.md) for trusted cost ceilings, workspace
admission, fair claims, and explicit incomplete/resume semantics.

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

Acquisition also owns a provider-specific session lock throughout the claimed
job, including network requests and waits. HTTP pacing and retry
deadlines are committed before sleeping and survive worker changes. Each HTTP
attempt checks execution rights before contacting a provider. Different
providers and local normalization jobs can execute independently. Ordinary
normalization uses a separate shared gate for account identity and
deterministically ordered locks for the affected games and accounts. Independent
games and player profiles from the same provider can commit concurrently. A
certified rename or placeholder
merge restarts before mutation with the identity gate held exclusively.
These short identity/write transactions do not span provider network requests.

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
Snapshots return at most 128 workers, prioritizing live workers and then recent
heartbeats from the last 24 hours. `active_workers` counts every live worker,
including workers outside the list; `workers_truncated` identifies a shortened
list and `worker_limit` states its cap. Actual heartbeat ages and running or
stopping states are preserved. Snapshot reads do not mutate history.
Worker start, heartbeat, and stop writes prune at most 256 stopped or failed rows
whose heartbeat is older than 24 hours and whose liveness has expired, skipping
locked rows. Stale running and stopping records are retained because heartbeat
expiry does not prove loss of session ownership; they leave the visible recent
history after 24 hours. Migration `0013` indexes these bounded status and cleanup
queries.

## Local execution and stages

`python -m chess_crawl.jobs.worker --stage all` polls PostgreSQL. Multiple
workers are supported. `--stage acquisition` selects provider/discovery jobs;
`--stage processing` selects normalization and archive-reprocessing jobs.
Full/incremental collection captures source batches, queues normalization jobs,
and yields between checkpoints. The existing bounded import/crawl path retains
synchronous normalization so its strict total game cap remains enforceable.

## SQS dispatch

Install the `s3` dependency extra, which supplies Boto3 for both S3 and SQS,
and configure `CHESS_CRAWL_SQS_QUEUE_URL`:

```bash
uv sync --locked --extra api --extra s3
uv run python -m chess_crawl.jobs.dispatch
uv run python -m chess_crawl.jobs.worker --stage all
```

Both modules accept `--queue-url` and `--once`. The SDK uses its default
credential chain, including ECS task roles. Local polling does not require the
SDK. Workers first claim fair, stage-eligible PostgreSQL work. After a claim,
they poll one hint without waiting to acknowledge completed/owned duplicates
even during continuous database work. Only an idle worker requests up to twenty
seconds of SQS long polling. The consumer inherits the configured queue visibility
timeout without a per-receive override. Queue visibility is not a job lease; duplicate
deliveries cannot claim an owned or completed job. Failed publication keeps its
outbox entry for retry.
A crash after send and before the outbox acknowledgement can deliver twice.
Workers check PostgreSQL again after an idle long poll, so work that became due
during the wait and expired or dead-lettered hints do not strand pending jobs.
Each worker iteration performs one orphan recovery pass, including idle polls.

The dispatcher sends only `job_id`. Pending continuations and delayed retries
create new outbox entries in the same transaction as the job transition.
Successful processing acknowledges a message only after that durable state is
committed. Malformed messages remain eligible for the configured dead-letter
policy. Configure queue retention, redrive policy, monitoring, and task roles in
the deployment. Use all-stage workers on a shared queue. Stage-specific pools
set both `CHESS_CRAWL_SQS_ACQUISITION_QUEUE_URL` and
`CHESS_CRAWL_SQS_PROCESSING_QUEUE_URL` for the dispatcher, which routes durable
jobs by kind. Each stage worker uses its matching queue environment variable.

### Dispatch history retention

Migration `0015` retires the previous undelivered hint in the same transaction as
its job revision changes, including local execution without SQS. Hints currently
locked by a publisher are skipped and reconciled by maintenance, so slow SQS
publication cannot block the job transition. Current pending and delayed-retry
hints remain eligible; delivered/superseded hints are history,
not execution state. The migration adds lookup indexes and does not rewrite the
existing backlog. Workers and dispatchers independently reconcile legacy hints
and prune expired history during their normal loops, including idle loops.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `CHESS_CRAWL_DISPATCH_RETENTION_SECONDS` | `86400` | Keep delivered/superseded hint history for 24 hours. |
| `CHESS_CRAWL_DISPATCH_CLEANUP_INTERVAL_SECONDS` | `60` | Minimum seconds between maintenance batches in each process. |
| `CHESS_CRAWL_DISPATCH_CLEANUP_BATCH_SIZE` | `256` | Maximum pending hints examined and maximum history rows deleted per batch (1–10,000). |

Durations must be finite and positive. Each batch examines a bounded window of
pending hints, retaining a cursor that wraps at the end, and skips locked rows.
This also eventually repairs hints created before migration `0015`. Publication
uses the same batch limit to inspect a bounded due-hint window before joining job
state or taking row locks; obsolete members are retired in that transaction.
A window with no publishable hint advances immediately to the next window,
including when another publisher locks the entire window. Selection restarts at
the oldest available hint after a send, and empty windows wrap the cursor.
Neither publication nor maintenance reconciles the complete backlog per send. Configure the cleanup rate above
the expected hint-history production rate: shorten the interval or increase the
batch size for sustained high throughput or a large historical backlog. History
can remain longer than the retention period while a backlog drains or when no
worker/dispatcher is running. Cleanup never deletes an undelivered current
pending/retry hint merely because it is old. Job state and evidence are retained.

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
