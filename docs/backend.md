# Standalone backend and integration contract

[README](../README.md) · [CLI guide](cli.md) ·
[Contributing](../CONTRIBUTING.md)

`chess-crawl` supplies an authenticated JSON API, a serial acquisition worker,
and a durable Mercure event publisher. Its Docker Compose deployment owns these
services and its archive. A future Symfony application using Symfony Docker
can consume the API and events while keeping its own deployment, configuration,
user authentication, and UI.

## Start the development backend

Use a source checkout with Docker, the Compose v2 plugin, Linux container
support, and Python 3.11+ on the host. The bootstrap script uses only Python's
standard library. From this repository's root:

```bash
python3 scripts/bootstrap_dev.py
docker compose up --build --detach --wait --wait-timeout 120
docker compose ps
```

An empty archive starts without provider requests. Before submitting live work,
set `CHESS_CRAWL_CONTACT` to your contact address in the shell or Compose `.env`
and run `docker compose up -d --wait` to apply the worker configuration.

The bootstrap generates local credentials under `data/dev-secrets/`; these files
are ignored by Git and excluded from image builds. It preserves `api_token` and
`mercure_signing_key` on reruns, and generates scoped
`mercure_publisher_jwt` and `mercure_subscriber_jwt` files. The subscriber token
stays on the host for integration clients. These development JWTs have no expiry;
deployment and end-user token issuance belong to the deploying application.

| Service | Responsibility |
| --- | --- |
| `init` | Apply archive migrations, then exit successfully. |
| `api` | Authenticated HTTP submissions and archive reads. |
| `worker` | Hold the executor lock and acquire provider data serially. |
| `events` | Deliver committed outbox entries to Mercure. |
| `mercure` | Serve private event subscriptions using `dunglas/mercure`. |

The Python services run as UID/GID `10001` and share the named `archive_data`
volume at `/data`, with the database at `/data/archive.sqlite`. Mounting the
directory also preserves SQLite's WAL/SHM files and archive lock files. Mercure
has separate `mercure_data` and `mercure_config` volumes. The API and worker can
start independently of hub availability; the event publisher waits for the hub.

The API is available at `http://127.0.0.1:8000`, with interactive documentation
at `/docs` and its schema at `/openapi.json`. The Mercure subscriber endpoint is
`http://127.0.0.1:3000/.well-known/mercure`. Published ports bind to loopback by
default. The supplied hub requires subscriber authentication and does not enable
cross-origin browser subscriptions.

Compose supports these host settings, in addition to the request ceilings below:

| Variable | Default or purpose |
| --- | --- |
| `CHESS_CRAWL_BIND_ADDRESS` | `127.0.0.1` |
| `CHESS_CRAWL_API_PORT` | `8000` |
| `CHESS_CRAWL_MERCURE_PORT` | `3000` |
| `CHESS_CRAWL_SECRETS_DIR` | `./data/dev-secrets` |
| `CHESS_CRAWL_CONTACT` | Set a real contact before live acquisition. |
| `CHESS_CRAWL_LICHESS_TOKEN` | Optional provider token. |
| `CHESS_CRAWL_MERCURE_TOPIC_PREFIX` | `https://chess-crawl.local` |
| `CHESS_CRAWL_MERCURE_HISTORY_SIZE` | `10000` |

Compose reads this repository's `.env` for interpolation. The CLI and bootstrap
script do not load `.env` themselves. Export matching secrets-directory and
topic-prefix settings before running bootstrap and Compose; JWT topic scopes
must match the publisher's prefix. No Symfony configuration is required.

Stop and restart the services without removing the archive:

```bash
docker compose stop
docker compose start
```

`docker compose down` removes the containers and network while retaining the
archive volume. Run `docker compose up -d --wait` to recreate the services using
that archive. `docker compose down --volumes` removes the named volumes and
their archive and hub data.

## HTTP contract

All `/v1` routes and `/health/ready` require
`Authorization: Bearer <api-token>`. `/health/live`, `/docs`, and `/openapi.json`
are public. The API token is a service credential: a Symfony server should hold
it and make backend requests on behalf of its authenticated users.

The API loads either `CHESS_CRAWL_API_TOKEN` or
`CHESS_CRAWL_API_TOKEN_FILE`; configuring both is an error.
`CHESS_CRAWL_DB` selects the initialized archive. HTTP requests never create or
migrate an archive, and authentication runs before database access.

### Submit bounded work

Both submission routes require `Content-Type: application/json` and an
`Idempotency-Key` header containing 1–128 visible ASCII characters without
spaces. Provider and username are normalized before comparing requests.

| Route | Required JSON fields |
| --- | --- |
| `POST /v1/imports` | `provider`, `username`, `since`, `until`, `max_games` |
| `POST /v1/crawls` | Import fields plus `max_depth`, `max_users`, `max_jobs` |

`provider` is `chess.com` or `lichess`. Timestamps are integer **Unix seconds**.
The interval includes `since` and excludes `until`: `[since, until)`. The game
selection uses each game's end time. A game whose end time cannot establish
membership in the interval is not attributed to that run.

For example, enqueue a January 2024 import:

```bash
CHESS_CRAWL_REQUEST_TOKEN=$(cat "${CHESS_CRAWL_SECRETS_DIR:-data/dev-secrets}/api_token")
curl --fail-with-body http://127.0.0.1:8000/v1/imports \
  -H "Authorization: Bearer $CHESS_CRAWL_REQUEST_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: hikaru-january-2024' \
  --data '{"provider":"chess.com","username":"Hikaru","since":1704067200,"until":1706745600,"max_games":100}'
unset CHESS_CRAWL_REQUEST_TOKEN
```

A successful submission returns HTTP `202`, a `Location: /v1/runs/<id>` header,
and an object such as:

```json
{"run_id":1,"job_ids":[1,2],"replayed":false}
```

The request only queues work. An import creates a profile job and a games job;
a crawl creates its initial opponent-discovery job. The separate worker makes
provider requests. Repeating the same normalized request with the same key
returns the original IDs and `replayed: true`, including after completion.
Using that key for a different request or operation returns HTTP `409`.

`max_games` caps distinct games attributed to the run, including games already
present in the archive. Replaying a chunk does not charge that run twice for
the same game. Chess.com still supplies whole monthly responses: their full raw
bytes are preserved, but only games within the requested interval and remaining
budget are normalized and attributed. A partially processed raw response stays
pending; a run can finish successfully with pending raw payloads remaining.

Default server ceilings are configurable:

| Environment variable | Default |
| --- | ---: |
| `CHESS_CRAWL_MAX_GAMES` | 1000 |
| `CHESS_CRAWL_MAX_DEPTH` | 2 |
| `CHESS_CRAWL_MAX_USERS` | 100 |
| `CHESS_CRAWL_MAX_JOBS` | 200 |
| `CHESS_CRAWL_MAX_DATE_SPAN_DAYS` | 366 |
| `CHESS_CRAWL_PAGE_SIZE` | 100 |

The caller still supplies explicit positive acquisition limits; crawl depth
may be zero. Page size defaults to 50, or the configured ceiling if lower.
Compose passes these settings to the API service.

### Read state and archive data

| Route | Result |
| --- | --- |
| `GET /health/live` | API process responds; no archive or worker check. |
| `GET /health/ready` | Initialized archive is readable at the supported schema version. |
| `GET /v1/worker` | Executor heartbeat, `alive`, status, current job, and heartbeat timestamps. |
| `GET /v1/runs/{id}` | Run snapshot, parameters, job IDs, counters, revision, archive ID, and freshness. |
| `GET /v1/jobs/{id}` | Job snapshot, state, retry/checkpoint information, revision, and archive ID. |
| `GET /v1/providers` | Provider capabilities and request policies. |
| `GET /v1/summary` | Archive, run, and job totals with freshness. |
| `GET /v1/games` | Paginated normalized games; optional `provider`. |
| `GET /v1/users` | Paginated provider-scoped users; optional `provider`. |
| `GET /v1/users/{provider}/{username}/opponents` | Paginated opponents from normalized games. |

Collection routes accept `after` and `limit`. Responses contain `items`,
`next_cursor`, `total`, and `freshness`; pass a non-null `next_cursor` as the
next request's `after`. Freshness describes recorded observations and pending
or failed payloads, rather than asserting that a provider was checked now.

Each response's related reads share a database snapshot. Pagination across
separate requests does not freeze the archive while the worker adds data.
Job and run IDs belong to an archive; retain `archive_id` when caching them.

Errors use `{"error":{"code":"...","message":"..."}}`, with validation
details when applicable. Relevant statuses are `401` for authentication,
`404` for missing resources, `409` for idempotency conflicts, `422` for invalid
requests, and `503` when the archive is unavailable. The OpenAPI schema is the
route and request-shape reference.

### Report result counts

CLI reports and opponent API responses expose two distinct counts:

| Field | Meaning |
| --- | --- |
| `no_result` | Games without a normalized win, loss, or draw, including aborted, unknown-status, and ongoing games. |
| `in_progress` | Games whose provider status is recognized as ongoing. |

These counts overlap. A missing result does not establish that play is ongoing,
and an unrecognized status does not establish that play has finished. The
existing API field `unfinished` remains a compatibility alias for `no_result`;
new consumers should use the explicit fields. CLI labels use the explicit
meanings.

## Worker and recovery

One executor owns an archive at a time. The worker and acquisition CLI share a
kernel-held archive lock; a second executor cannot take ownership merely
because a heartbeat is old. An idle worker remains alive: `/v1/worker` separates
heartbeat liveness from whether any job is currently executing. API readiness
alone does not prove that acquisition is running.

On startup, the worker acquires the lock and recovers orphaned in-progress work.
It preserves cancelled runs. Transient failures have durable retry counters
and deadlines; provider cooldowns survive restarts and apply to later jobs for
the same provider. Retries are bounded, and exhausted jobs become errors.
Provider `Retry-After` requirements and the Lichess cooldown floor are respected.

Graceful shutdown stops new claims and finishes the active acquisition unit.
Completed monthly checkpoints remain durable so a later executor can resume.
A crash before a checkpoint may replay a preserved payload; normalization and
run attribution are transactional and idempotent.

Outside Compose, the worker entry point is:

```bash
uv run python -m chess_crawl.jobs.worker --db /path/to/archive.sqlite
```

`--once` executes at most one due job. Polling, heartbeat, and retry options are
listed by `uv run python -m chess_crawl.jobs.worker --help`. Compose exposes
`CHESS_CRAWL_POLL_INTERVAL` (1 second), `CHESS_CRAWL_HEARTBEAT_INTERVAL`
(5 seconds), `CHESS_CRAWL_JOB_MAX_RETRIES` (3), `CHESS_CRAWL_RETRY_BASE`
(30 seconds), and `CHESS_CRAWL_RETRY_MAX` (3600 seconds). The retry maximum caps
the exponential component; a provider delay can require a longer wait.
Stop the existing worker before intentionally using a separate CLI executor for
the same archive.

The game limit stops further game acquisition, but retained run games still
drive local opponent discovery after a restart. Each run counts the edges it
actually processes, even when another run already discovered the same edge.
The archive-wide graph remains deduplicated.

## Mercure events and client synchronization

Committed job and run changes enter an SQLite outbox in the same transaction
as their state changes. The separate publisher sends private JSON updates to
Mercure. Its retries do not stop chess acquisition. Delivery is at least once:
a publisher crash after hub acceptance can cause the same event to be sent
again.

The publisher uses `CHESS_CRAWL_MERCURE_URL` and either
`CHESS_CRAWL_MERCURE_PUBLISHER_JWT` or
`CHESS_CRAWL_MERCURE_PUBLISHER_JWT_FILE`. Subscribers need a subscriber JWT with
permission for their topics; the HTTP API bearer token is a different credential.

`CHESS_CRAWL_MERCURE_TOPIC_PREFIX` defaults to `https://chess-crawl.local`.
This is a topic identifier, not the hub's network address:

| Event type | Topic | Authoritative snapshot |
| --- | --- | --- |
| `job.updated` | `https://chess-crawl.local/jobs/{id}` | `GET /v1/jobs/{id}` |
| `run.updated` | `https://chess-crawl.local/runs/{id}` | `GET /v1/runs/{id}` |

The JSON envelope includes `schema_version` (currently `1`), `archive_id`,
`event_id`, `type`, `revision`, and `occurred_at`. Resource fields include the
job/run IDs, status, provider, and counters. A job event also carries `kind`
and `next_attempt_at`. Event `status` corresponds to the job snapshot's `state`
or the run snapshot's `status`.

Clients should:

1. Subscribe to the required private topics and retrieve current API snapshots.
2. Deduplicate using the JSON `event_id`; track `revision` per resource within
   its `archive_id` and ignore older or duplicate revisions.
3. Refresh the API snapshot after reconnecting or observing a revision gap.
   Merge snapshots and buffered events using their revisions.

The JSON `event_id` is authoritative for application deduplication. A hub can
override the SSE transport `id`, so the SSE ID is not interchangeable with the
JSON ID. Hub history and a transport reconnection cursor do not replace API
resynchronization.

Treat run events as refresh hints. Their revision identifies persisted run
changes; the API calculates live job counters within its read snapshot, so
counters may change before the next run revision is written. Fetch the run
snapshot when processing related job updates instead of treating run events as
a complete counter history.

A future Symfony application can subscribe to these JSON events and use its
own controllers, templates, and Turbo integration to render UI updates. This
backend supplies data and state notifications; the Symfony application owns
HTML, user permissions, and browser subscriber credentials.

## Offline Compose smoke check

On a fresh, disposable development archive after startup, run:

```bash
python3 scripts/compose_smoke.py
```

This checks authentication, idempotent submission, worker liveness, and private
Mercure delivery. It stops acquisition before creating synthetic jobs, completes
them locally, and restarts the idle worker. It makes no chess-provider requests
but deliberately writes test state; use a separate Compose project and volume
when an existing archive must be retained unchanged.

## Storage and deployment boundary

The current deployment uses SQLite on one host with a local persistent volume.
API, worker, and event publisher share that archive; only one acquisition
executor may operate on it. Run separate archives for separate deployments.
Execution locks require POSIX `flock`; use Linux/WSL or the supplied Linux
containers. The CLI's default `./chess-crawl.db` is a separate archive from
Compose's volume unless you explicitly arrange shared storage.

Schema migration 5 adds per-run discovery-edge membership. Compose's `init`
service applies it at startup; for a CLI archive, run `uv run chess-crawl init`
with that archive's `--db` path before using the upgraded backend. Existing
edges retain their original recorded run. Earlier schemas did not preserve
later runs' shared-edge membership, so the migration does not reconstruct
unrecorded historical counts. New and resumed discovery records membership
for each run explicitly.

PostgreSQL remains a future storage implementation decision. There is no
`DATABASE_URL` switch that makes the SQLite schema, transaction behavior,
locking, or worker coordination run on PostgreSQL. Adopting it requires an
explicit implementation and migration plan.

Keep the future Symfony Docker environment separate. Connect it through the
backend API and Mercure URLs, with application-owned credentials and networking;
it does not need the Python archive mounted into its containers.
