# Standalone backend and integration contract

[README](../README.md) · [CLI guide](cli.md) ·
[Contributing](../CONTRIBUTING.md)

`chess-crawl` supplies an authenticated JSON API, concurrent durable workers,
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
are ignored by Git and excluded from image builds. It preserves
`postgres_password`, `api_token`, and `mercure_signing_key` on reruns, and generates scoped
`mercure_publisher_jwt` and `mercure_subscriber_jwt` files. The subscriber token
stays on the host for integration clients. These development JWTs have no expiry;
deployment and end-user token issuance belong to the deploying application.
Development grants are confined to workspace `local`, matching the single API
token. To replace older topic grants, rerun the bootstrap and recreate `events`
with `docker compose up -d --force-recreate events`; master credentials remain
unchanged.

| Service | Responsibility |
| --- | --- |
| `postgres` | Persist archive data using PostgreSQL 18 on the private Compose network. |
| `init` | Wait for PostgreSQL readiness, apply archive migrations, then exit successfully. |
| `api` | Authenticated HTTP submissions and archive reads. |
| `worker` | Claim fenced jobs, coordinate provider pacing, and process local data. |
| `events` | Deliver committed outbox entries to Mercure. |
| `mercure` | Serve private event subscriptions using `dunglas/mercure`. |

The Python services run as UID/GID `10001` with read-only filesystems and connect
to PostgreSQL using a password-free URL and a mounted password secret. PostgreSQL
18 owns the `postgres_data` volume mounted at `/var/lib/postgresql`; the versioned
server data directory is below that mount. No database port is published to the
host. Mercure has separate `mercure_data` and `mercure_config` volumes.

The migration service waits for healthy PostgreSQL. API, worker, and publisher
start only after migrations succeed; a migration failure blocks application
startup. The API and worker can start independently of hub availability, while
the event publisher also waits for healthy Mercure.

Retain `postgres_password` alongside the persistent database. The PostgreSQL
image uses it to initialize the database role on its first start; changing the
secret file later does not rotate the role's password. Bootstrap reruns retain
the existing value. A PostgreSQL major-version upgrade requires a deliberate
database upgrade procedure; replacing the image tag alone is insufficient.

The API is available at `http://127.0.0.1:8000`, with interactive documentation
at `/docs` and its schema at `/openapi.json`. The Mercure subscriber endpoint is
`http://127.0.0.1:3000/.well-known/mercure`. Published ports bind to loopback by
default. The supplied hub requires subscriber authentication and does not enable
cross-origin browser subscriptions.

Compose supports these host settings, in addition to the request ceilings below:

| Variable | Default or purpose |
| --- | --- |
| `CHESS_CRAWL_DATABASE_URL` | `postgresql://chess_crawl@postgres:5432/chess_crawl` for bundled Compose; external mode requires its own URL. |
| `CHESS_CRAWL_DATABASE_CA_FILE` | Host PEM CA file required by `compose.external.yaml`. |
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


### External PostgreSQL

Use Compose 2.24.4 or newer and the external overlay when the database is
provisioned separately. Changing only the URL in the default stack retains its
bundled PostgreSQL dependency and local transport policy. The external overlay
removes that dependency, excludes the bundled server from the active services,
and keeps API, worker and publisher startup gated on successful migrations.
It requires both an explicit database URL and a CA certificate file:

```bash
export CHESS_CRAWL_DATABASE_URL="postgresql://chess_crawl@database.example:5432/chess_crawl"
export CHESS_CRAWL_DATABASE_CA_FILE="./data/dev-secrets/postgres_ca.pem"
docker compose -f compose.yaml -f compose.external.yaml up --build --detach --wait --wait-timeout 120
```

Create the database and login role on that server first, and supply its password
in the `postgres_password` secret selected by `CHESS_CRAWL_SECRETS_DIR`. Bootstrap
preserves an existing valid password file; it does not configure the external
server's role. Obtain the CA PEM from that server's operator, place it at the
configured host path, and make the file readable by container UID `10001`
(for example, mode `0444` inside the protected secrets directory). The overlay
mounts it read-only at `/run/secrets/postgres_ca` in all four Python services.
A missing or unreadable certificate blocks connection instead of disabling TLS.

Keep using both Compose files for subsequent `up`, `stop`, `start`, `logs` and
`down` commands. For example:

```bash
docker compose -f compose.yaml -f compose.external.yaml ps
docker compose -f compose.yaml -f compose.external.yaml logs --tail 100 init
```

The application defaults to `CHESS_CRAWL_DATABASE_TRANSPORT=verified`, enforcing
TLS for TCP connections with certificate-chain and hostname verification
(`sslmode=verify-full`). Unix sockets remain local and do not use TLS.
`CHESS_CRAWL_DATABASE_SSL_ROOT_CERT_FILE` supplies the CA file for source-run
clients; external Compose sets it to the mounted certificate. Connection URL
options cannot downgrade the enforced verified policy.

The default bundled stack explicitly sets transport `local` and trusts the
exact `postgres` hostname through `CHESS_CRAWL_DATABASE_TRUSTED_HOST`. This
exception is for that Compose service. Source-run local mode otherwise accepts
Unix sockets and loopback addresses. An external hostname does not become local
merely because its URL was substituted; routing overrides such as alternate
host addresses or service definitions cannot bypass this boundary. The external
overlay sets transport `verified` and clears the bundled trust marker.

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
`CHESS_CRAWL_DATABASE_URL` selects the initialized PostgreSQL database. Source-run
services load either `CHESS_CRAWL_DATABASE_PASSWORD` or
`CHESS_CRAWL_DATABASE_PASSWORD_FILE`; configuring both is an error. Compose
mounts its generated database secret and sets the file variant. Credentials do
not belong in connection URLs or command arguments. HTTP requests never create
or migrate an archive, and authentication runs before database access.

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
selection for bounded imports/crawls uses each game's end time. A game whose end time cannot establish
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

The game, user, and opponent collection routes above accept `after` and `limit`
and return `items`, `next_cursor`, `total`, and `freshness`. Other lists use
bounded page envelopes; not every list includes a total or freshness field.
Pass a non-null `next_cursor` as the next request's `after`. Freshness describes
recorded observations and pending or failed payloads, rather than asserting that
a provider was checked now.

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

Multiple workers can process independent jobs. Each job has session-owned
PostgreSQL advisory locks and a fencing token; a second worker cannot take its
ownership merely because a heartbeat is old. Provider acquisition is coordinated
per provider, while local normalization ignores provider cooldowns. An idle
worker remains alive: `/v1/worker` separates heartbeat liveness from activity
and includes individual workers and their aggregate active count. API readiness
alone does not prove that acquisition is running. See [execution](execution.md).

On startup and while idle, the worker recovers orphaned in-progress work only
after obtaining the orphan's job lock.
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
uv run python -m chess_crawl.jobs.worker
```

`--once` executes at most one due job. Polling, heartbeat, and retry options are
listed by `uv run python -m chess_crawl.jobs.worker --help`. Compose exposes
`CHESS_CRAWL_POLL_INTERVAL` (1 second), `CHESS_CRAWL_HEARTBEAT_INTERVAL`
(5 seconds), `CHESS_CRAWL_JOB_MAX_RETRIES` (3), `CHESS_CRAWL_RETRY_BASE`
(30 seconds), and `CHESS_CRAWL_RETRY_MAX` (3600 seconds). The retry maximum caps
the exponential component; a provider delay can require a longer wait.
Use `--stage acquisition` or `--stage processing` for separate local worker
pools. Optional SQS dispatch and offline data upgrades are documented in
[execution and upgrades](execution.md).

The game limit stops further game acquisition, but retained run games still
drive local opponent discovery after a restart. Each run counts the edges it
actually processes, even when another run already discovered the same edge.
The archive-wide graph remains deduplicated.

## Mercure events and client synchronization

Committed job and run changes enter a PostgreSQL outbox in the same transaction
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
| `job.updated` | `https://chess-crawl.local/workspaces/{workspace}/jobs/{id}` | `GET /v1/jobs/{id}` |
| `run.updated` | `https://chess-crawl.local/workspaces/{workspace}/runs/{id}` | `GET /v1/runs/{id}` |

The JSON envelope includes `schema_version` (currently `1`), `archive_id`,
`event_id`, `type`, `revision`, `occurred_at`, and `workspace_id`. Resource fields include the
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

PostgreSQL is the only supported storage engine. Each deployment should use its
own database. API, worker, and publisher use independent connections to the same
database; PostgreSQL session advisory locks retain per-job acquisition/processing
rights and one event publisher. A disconnected ownership session cannot continue
processing with its old token. The database is configured independently of Compose, so a
separately managed PostgreSQL server can be used through
`CHESS_CRAWL_DATABASE_URL` and the password secret.

Compose's `init` service applies the packaged PostgreSQL schema and migrations
at startup. For source-run services, run `uv run chess-crawl init` against the
selected database before starting the upgraded backend. The previous file-based
storage configuration and `--db` option are removed. Existing SQLite archives
are not automatically imported; retain their files separately if needed.

Keep the Symfony Docker environment separate. Connect it through the backend
API and Mercure URLs, with application-owned credentials and networking; it does
not need direct database access or the PostgreSQL volume mounted into its
containers.

## Workspace ownership and player analysis

The public player/game archive is shared. Application submissions, runs, jobs,
working sets, analysis results, and Mercure resource topics belong to a workspace.
Chess Dog owns application users and must select backend credentials on the server.
A request header or JSON field cannot choose a workspace.

For one local installation, `CHESS_CRAWL_API_TOKEN` (or its existing secret file)
binds every authenticated request to workspace `local`. For multiple workspaces,
set `CHESS_CRAWL_API_WORKSPACE_TOKENS_FILE` to a mounted JSON secret file containing
workspace-to-token mappings, for example `{"workspace-a":"unique-secret-a"}`.
Do not also configure the single token. Tokens must be unique, nonempty, and
contain no whitespace; workspace identifiers contain 1–100 letters, digits,
underscores, or hyphens. `public` is reserved for shared source evidence. The
configuration is trusted service authentication, not end-user account management.
Rotating the file requires restarting the API. Do not expose it to browsers.

Existing submissions migrate to workspace `local`. Idempotency keys are durable
and scoped by workspace. Requests for another workspace's job, run, working set,
or result return the same 404 as missing records. Worker liveness remains visible,
but its current job ID is hidden when owned by another workspace. Summary job/run
counts and source freshness exclude other workspace resources. Mercure subscribers
must now receive grants for `/workspaces/<workspace>/jobs/<id>` and
`/workspaces/<workspace>/runs/<id>` topics; old topic grants must be replaced.

The following authenticated routes read only the local archive:

| Route | Response |
| --- | --- |
| `GET /v1/workspace` | Trusted workspace identifier |
| `GET /v1/users/{provider}/{username}` | Account, latest profile facts, aliases, visible resources |
| `GET /v1/users/{provider}/{username}/history` | Observation history, cursor `after`, bounded `limit` |
| `GET /v1/users/{provider}/{username}/resources` | Current public and workspace resource observations |
| `GET /v1/users/{provider}/{username}/resources/history` | Resource history; optional `resource_key` |
| `GET /v1/users/{provider}/{username}/rating-history` | Typed daily ratings; pagination pins returned `snapshot_id` |
| `GET /v1/users/{provider}/{username}/coverage` | Stored/normalized game counts and observed date bounds |
| `GET /v1/users/{provider}/{username}/games` | Player game page with current version IDs |
| `GET /v1/games/{game_id}` | Immutable evidence including tree, clocks, provenance; optional `version_id` |
| `GET /v1/games/{game_id}/versions` | Bounded version metadata page |
| `GET /v1/games/{game_id}/moves` | Bounded nodes with clocks/timings; optional `version_id`, `mainline`, `after`, `limit` |
| `GET /v1/games/{game_id}/pgn` | PGN generated from stored evidence; partial/unsupported requires `allow_partial=true` |
| `GET /v1/resources` | Resource acquisition catalog; optional provider |
| `GET /v1/games/lookup?provider=...&key=...` | Cached game by provider game ID, canonical URL, or content hash |
| `GET /v1/users/{provider}/{username}/summary` | Result/activity counts, rated counts, date bounds and opponent count |
| `GET /v1/reports/games-by-month?provider=...` | UTC month aggregates; bounded `limit`, string cursor `after` |
| `GET /v1/raw` | Cached payload metadata; public and current workspace only; no raw bodies or request credentials |
| `GET /v1/jobs` | Owned queue page; optional owned `run_id`, numeric `after`, bounded `limit` |
| `GET /v1/runs` | Owned run metadata page; numeric `after`, bounded `limit` |
| `GET /v1/jobs/status` | Owned state/kind/depth counts; optional owned `run_id` |
| `GET /v1/exports/games.jsonl` | Stream shared normalized game metadata; optional provider |
| `GET /v1/exports/users.jsonl` | Stream shared normalized public account facts; optional provider |
| `GET /v1/exports/graph.csv` | Stream only current workspace's run-edge memberships; optional provider |

History page sizes must be below 1000 and also within the configured page limit.
Exact normalized clock/timing decimals are JSON strings, preserving source
precision. Missing clocks do not become zero. Coverage counts do not establish
complete provider history. Provider APIs are contacted only by queued collectors,
not by player lookup or report requests.

`POST /v1/resources` queues a registered resource using `provider`, `username`,
`resource_key`, and validated `parameters`. Owner scope comes from authentication.
`POST /v1/upgrades` queues an offline normalization upgrade using `provider`,
`name`, `parser_version` (default `current`), and `batch_size` (1–100). Its response
Location is `/v1/upgrades/<job_id>`, which exposes durable execution progress.
Both use `Idempotency-Key`; SQL schema migrations never contact providers.
Upgrade targets are `current` or the exact installed parser manifest covering
archives, games/evidence, resources, and users. Workers reject unavailable targets.
An upgrade pins that manifest; restarting it with `current` after changing parser
binaries fails identity validation rather than claiming the original target ran.

`POST /v1/profiles/refresh` and `POST /v1/stats/refresh` accept exactly `provider`
and `username`, plus `Idempotency-Key`, and create one owned durable job. Separate
statistics refresh is supported for Chess.com; Lichess performance and daily
history use the registered resources. These handlers do not acquire data.
`POST /v1/games/collect` queues one public Lichess game export, using `provider`
and its eight-character public `game_id` plus `Idempotency-Key`. URLs and player
secret IDs are rejected; Chess.com direct game acquisition is unsupported.

JSONL/CSV exports own a read-only repeatable-read snapshot and stream database
rows with bounded memory. Completion exports all matching rows; an interrupted
HTTP download must be retried. The graph export emits each owned run membership
with that run's ID. Its game counts and representative game IDs are reconstructed
from that run's attributed games, and depth from its discovery jobs. Missing
historical attribution leaves those metrics zero or unknown; another workspace's
global edge provenance and merged depth are never substituted. Usernames starting
with spreadsheet formula characters are escaped in CSV. Raw payload catalog pages
also exclude unassigned legacy evidence. Worker recovery and execution remain
dedicated operator functions; HTTP reads and submissions do not run the worker.

Imports retain bounded mode by default. Optional `collection_mode` values
`full`, `incremental`, and `backfill` use `max_games` as a page budget, not a total
history cap, and `batch_size` (1–12) controls bounded execution units. These modes
can cover a window larger than the bounded-import date-span limit. Provider
timestamps remain inclusive `since` and exclusive `until`. These bounds are optional
for full/incremental/backfill; omit `since` for an incremental watermark refresh.
Lichess history modes select using the provider's native creation timestamp,
while bounded imports/crawls select by normalized game end time. Bounded
collection still requires both timestamps.

### Reproducible working sets

`POST /v1/working-sets` accepts a name, filters, and settings with an
`Idempotency-Key`. Filters can include provider, username, time bounds, time class,
variant, and rated status. A username requires its provider. Selection uses the
current immutable game version and persists ordered membership in PostgreSQL;
subsequent imports or parser upgrades do not change existing working sets.
Players are selected through provider account IDs so retained historical games
survive username changes. Unnormalized games are excluded until evidence exists.

`GET /v1/working-sets/<id>` reads metadata. The `/members` subresource pages through
stored game-version references without copying all games into memory. Workers can
checkpoint a membership cursor and process bounded batches. Creating a newer
selection is explicit. A completed selection and its membership are immutable.
Synchronous creation selects at most the configured limit plus one row. The
default `CHESS_CRAWL_MAX_WORKING_SET_MEMBERS` is 10000; a larger match returns 422
with `working_set_too_large` and rolls back all selection/submission rows. Nothing
is silently truncated. Operators can adjust the limit after measuring latency.
Whole-archive building beyond this limit requires the planned durable async
builder with an explicit immutable as-of snapshot; it is not yet implemented.

`POST /v1/working-sets/<id>/results` stores a calculation's `implementation`,
`implementation_version`, `settings`, and `output`. Its signature includes the
exact input selection, working-set settings, and calculation configuration.
Repeating the same output reuses the stored record; a different output for the
same signature returns 409. `POST /v1/working-sets/<id>/results/lookup` accepts
those calculation fields without output and retrieves an exactly compatible
result before computation, or returns 404. `GET /v1/results/<id>` reads a visible
record. Settings are limited to 64 KiB and inline outputs to 1 MiB; large output
artifacts belong in compressed object storage. Model/engine execution is provided
by later registered analysis workers, not by these result-storage routes.

Private PGN uploads require a separate namespace and access-aware normalization;
this API does not publish private imports into the shared provider archive.
