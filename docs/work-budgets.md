# Durable work budgets

Queued operations share a server-owned budget through their crawl run. Acquisition,
local normalization, offline upgrades, and descendants inherit the same database
budget; job parameters and new delivery hints cannot replace it. A new HTTP
idempotency key creates a new run budget but uses the same workspace's current
UTC calendar-month quota. Replays retain the original lifetime policy and usage.

Default limits are 100,000 distinct games, 100,000 interpretation units, 256 MiB of
application-read remote bytes, and 2,000 actual HTTP attempts per run. Each
workspace has monthly limits of 1,000,000 games, 1,000,000 interpretation units,
2 GiB of application-read remote bytes, and 10,000 attempts. A source/body read
and each selected game interpretation consume units, including repeated local
replay. An HTTP attempt prepays one source interpretation unit; failed attempts
retain that charge. Distinct games count once per run budget, even across pages.

Operator configuration uses `CHESS_CRAWL_` followed by the uppercase `BudgetPolicy`
field name, for example `CHESS_CRAWL_JOB_MAX_REMOTE_REQUESTS` and
`CHESS_CRAWL_WORKSPACE_MAX_GAMES`. Every value must be a positive PostgreSQL bigint.
The API and workers should share the trusted configuration. The database keeps
workspace policy authority: installing a lower quota persists even when admission
is rejected, and a worker or stale server with larger defaults cannot lift it.
Only explicit operator resume extends existing ceilings; spent usage is retained.

Network reservations commit before each actual attempt, including retries. They
reserve the response allowance plus one bounded read slice; unused bytes are
settled once after the response closes. A crash retains uncertain reserved bytes
and attempts. Concurrent runs serialize the brief workspace reservation, so they
cannot overspend near a quota boundary. No quota lock spans provider I/O.

Budgeted HTTP bodies are streamed and limited to 16 MiB by default, including
error responses and false or absent Content-Length headers. Content encoding must
be identity to prevent compressed expansion. The byte metric is bytes exposed by
the HTTP transport to the application; it excludes socket/TLS prefetch. Exposure
can cross the body limit by at most one slice, capped at 64 KiB and at the body
allowance; the reservation covers that slice. Whole-response acquisition also
uses the trusted HTTP timeout, preventing a slow drip from extending the job
indefinitely through per-read timeout resets. Oversized retained archive inputs
are rejected by recorded original size before object reads or decompression.

Workspace claims rotate between eligible workspaces before applying priorities.
At most two jobs per workspace run concurrently by default. Admission and child
enqueue enforce a total unfinished backlog of `workspace_max_queued_jobs` plus
`workspace_max_active_jobs` (32 + 2 = 34 by default), counting pending, blocked,
and running jobs. SQS hints respect the same rotation and active ceilings; an
unclaimable hint falls back to database polling. Provider session ownership and
pacing still apply independently of these workspace limits.

A quota pause reuses the original successful response identified by its job,
source window and fetch occurrence. Bounded games, native full-history pages and
monthly archives resume local normalization without another HTTP request or a
fabricated fetch log. A crash between child enqueue and coverage commit retains
the same occurrence-aware child, including for a sealed month. Atomic opponent
fanout that cannot fit the backlog stays incomplete; an operator may extend the
backlog and resume from the preserved games and frontier.

Exhaustion blocks the job with `budget_exhausted` and explicitly leaves requested
work incomplete. Raw evidence, normalization item checkpoints, acquisition gaps,
and completed run membership remain stored. Monthly rollover does not reset the
run's lifetime budget or silently resume blocked work. Full history has no
implicit date floor, and a cap never certifies missing history as complete.
The operator can show the owned run budget and extend/resume it using
`python -m chess_crawl.jobs.budget show --run-id ID` or
`python -m chess_crawl.jobs.budget resume --run-id ID` with trusted configuration.
There is no tenant-facing policy extension endpoint.

Migration 0014 indexes owned budget membership, unfinished workspace jobs, and
original successful job observations. These lookups remain selective as completed
job and fetch history grows; the planner regression exercises 10,000 terminal
jobs and 10,000 unrelated observations.
