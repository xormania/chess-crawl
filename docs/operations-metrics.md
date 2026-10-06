# Operational measurements

Run the trusted operator command against the authoritative PostgreSQL archive:

```bash
chess-crawl-admin metrics
# An explicit target also works; keep passwords in the usual secret source.
chess-crawl-admin metrics --database-url 'postgresql://operator@database/chess_crawl'
```

The command returns one JSON document with `schema_version: 1`, a Unix-seconds
`observed_at`, `stages`, `outboxes`, and `usage`. It uses one read-only repeatable-read
snapshot, requires the current schema, and neither initializes the database nor
acquires execution ownership. It makes no provider or queue requests. Output has
only fixed stage/reason labels and aggregate numbers: no workspace identifiers,
player targets, credentials, event payloads, or error messages.
Each SQL statement has a ten-second deadline; `--statement-timeout-ms` accepts
1 through 300000 milliseconds for operator-controlled sampling. Timeout failures
return a nonzero exit status with no partial snapshot or success-shaped zeros.

## Job admission signals

Each of `stages.acquisition` and `stages.processing` includes:

| Field | Meaning |
| --- | --- |
| `eligible_jobs` | Pending or scheduled blocked jobs that pass the shared database admission checks. |
| `oldest_eligible_age_seconds` | Age since the oldest eligible job's original enqueue time; null when none are eligible. |
| `active_jobs` | Durable `in_progress` jobs, including orphaned rows until normal recovery resumes them. |
| `pending_jobs`, `blocked_jobs` | Live row-state totals, including eligible and waiting work. |
| `waiting` | Jobs attributed once to the first failing admission condition. |

Waiting attribution follows this order: cancelled run, indefinite pause, future
retry deadline, processing dependency, provider cooldown, workspace active cap.
An indefinite pause whose recorded reason starts with `budget_exhausted:` is
reported as `budget`; other indefinite pauses are `paused`. Budget pauses
include lifetime run and monthly workspace exhaustion. Current persisted
workspace policies supply the active cap; the configured default applies only
when no workspace policy exists. Actual resource reservations still happen
during execution, so an eligible candidate can subsequently exhaust a budget.

`processing_dependency` excludes bounded acquisition waiting on its normalization
children and graph expansion waiting on its completed acquisition parent.
Full/incremental capture can pipeline ahead of normalization within backlog limits.
Processing can remain eligible while a remote provider is cooling down.

Eligibility describes durable candidates before ownership is attempted. A live
worker may hold a provider session lock, or another scheduler may claim a row
after observation. An eligible acquisition backlog therefore does not mean that
more remote requests are currently allowed. Measurements never expire a live
lease or steal work. Ages are clamped to zero when timestamps lie in the future.

## Delivery signals

`outboxes.dispatch` reports undelivered, nonsuperseded hint rows as `pending`.
`ready` counts current job revisions whose delivery and job retry times are due;
`deferred` counts current revisions waiting for either deadline; `obsolete`
counts hints that no longer match a schedulable job revision. Those three counts
partition `pending`. Provider cooldown and workspace admission do not stop hint
delivery, so ready hints are not an acquisition-capacity metric.

`oldest_ready_age_seconds` measures lateness since hint availability. The zero
availability sentinel means immediately available and falls back to the job's
original enqueue time. It is not time since the first failed delivery or since
hint creation: the outbox does not store a creation timestamp. A retry updates
the availability deadline. When no hints are ready, age is null.

`outboxes.events` reports pending count, count whose retry deadline is `due`,
`oldest_pending_age_seconds`, and `head_retry_delay_seconds`. Mercure delivery is
ordered: a delayed head event can block later due events. Monitor both backlog
age and head delay. A deployment that disables event delivery must account for
pending event retention rather than treating the backlog as worker demand.

`usage` sums recorded games, normalization units, remote requests, and remote
bytes across the current UTC monthly workspace budget period. It includes that
period's start and end, excludes historical periods, and does not reset charges
or resume work. Reservations remain charged until settled; uncertain requests
from crashed workers remain spent. The totals measure budgeted resource use,
not successful request or completed-game throughput. Comparing successive
samples can help track resource cost; account for rollover and reservation
refunds when calculating rates.

## Using the signals

An external scheduled exporter can read the JSON and publish selected numeric
fields to CloudWatch or another monitoring system. Give it read-only database
access, verified TLS for external databases, and a sampling interval that fits
the measured query cost. The snapshot scans live work and pending delivery rows;
output size stays fixed, while query cost grows with those backlogs. This command
does not install an exporter or enable AWS autoscaling.

Use processing eligible backlog and age to judge processing capacity, then
compare them with throughput and worker resource use. Compute backlog per task
using the corresponding ECS running-task count from the deployment, including
an explicit bootstrap policy when that count is zero. The job snapshot does not
infer task count from active jobs or heartbeat history. Bound maximum replicas
by database connections and storage throughput.

For acquisition, observe eligible backlog alongside provider cooldowns, request
capacity, latency, and archive reuse. Adding replicas provides resilience and
can serve independent provider work; it does not remove shared provider limits
or guarantee linear network throughput. Scale API and delivery services from
their own latency/resource/backlog measurements. SQS length alone includes
delivery hints and cannot establish how much durable work is executable.
