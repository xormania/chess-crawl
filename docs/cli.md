# Archive operations

[README](../README.md) · [Backend integration](backend.md) ·
[Contributing](../CONTRIBUTING.md)

Chess Dog is the product interface. Collection, archive exploration, exports,
and analysis selections use Chess-Crawl's authenticated HTTP API. The old
`chess-crawl` product CLI and its direct-fetch executor have been retired.

## Initialize and inspect an archive

Run from a source checkout after `uv sync --locked --extra api`. Configure a
password-free `CHESS_CRAWL_DATABASE_URL` and exactly one password source:
`CHESS_CRAWL_DATABASE_PASSWORD_FILE` or `CHESS_CRAWL_DATABASE_PASSWORD`.

```bash
uv run chess-crawl-admin migrate
uv run chess-crawl-admin info
uv run python -m chess_crawl.operations --help
```

`migrate` applies packaged migrations to an existing PostgreSQL database. It
creates neither a server nor a database, fetches no provider data, and does not
run network backfills. `info` reads schema readiness without applying migrations.
Use `--database-url` after the subcommand to select another database for one
operation. Never include credentials in URL or command arguments.

Remote connections require verified TLS and a trusted PEM CA configured with
`CHESS_CRAWL_DATABASE_SSL_ROOT_CERT_FILE`. Local loopback may explicitly select
`CHESS_CRAWL_DATABASE_TRANSPORT=local`. See the
[external PostgreSQL guide](backend.md#external-postgresql) for the Compose overlay.

The supplied stack initializes its schema before starting services. Its API
container already has the database settings and mounted password secret:

```bash
docker compose exec api chess-crawl-admin info
```

## Inspect and resume work budgets

Read anonymous scheduling, delivery and usage measurements with
`chess-crawl-admin metrics`. It uses trusted PostgreSQL access and a bounded
read-only snapshot; see [operations metrics](operations-metrics.md).

Use trusted operator PostgreSQL access; HTTP bearer credentials cannot extend
work ceilings. Inspect a run's stored usage and retained progress, then resume it
with the same trusted ceiling configuration used by the API and workers:

```bash
uv run chess-crawl-admin budgets show --run-id 42
uv run chess-crawl-admin budgets resume --run-id 42
```

These commands delegate to `python -m chess_crawl.jobs.budget` and require an
already migrated archive. `show` is read-only. `resume` extends ceilings without
resetting spent counters or changing retained checkpoints and completed jobs.
It requeues only jobs blocked by budget exhaustion and makes no provider requests.
See [work budgets](work-budgets.md) for finite defaults, monthly quotas, and the
operator steps needed before raising a ceiling.

## Retain analysis results

For notification retention, use trusted `chess-crawl-admin prune-events`;
see [event delivery](event-delivery.md) for delivered and explicitly discarded
pending batches. Retention shares the publisher lock and never removes work
dispatch or source evidence.

Stored calculation results have workspace count and byte ceilings. They are not
automatically expired. After backing up any results you need, use trusted
operator database access to remove one workspace's older results:

```bash
uv run chess-crawl-admin prune-results --workspace-id alpha --before 1767225600 --batch-size 256
```

`--before` is an exclusive Unix creation-time cutoff (the example is
2026-01-01 00:00 UTC). Each invocation deletes at most `--batch-size` rows,
oldest first, and prints `deleted` and `released_bytes`. The default batch is
256 and the maximum is 10000. Repeat with the same cutoff until `deleted` is
zero. The command requires migration `0016` and accepts `--database-url` like
the other operations. It shares admission's workspace lock and transaction, so
pruning and concurrent API saves cannot corrupt quota accounting.

Only derived results in the selected workspace are removed. Source evidence,
working sets, and other workspaces remain intact. Removed result IDs return
404; exact-signature lookup misses until that result is recomputed and saved.
Released bytes refer to the logical payload quota; PostgreSQL reclaims deleted
tuple space through its normal vacuum/reuse process.

## Relocate compressed source backups

Select `CHESS_CRAWL_ARCHIVE_BACKEND=local` with a durable shared
`CHESS_CRAWL_ARCHIVE_DIRECTORY`, or `s3` with a private bucket and the installed
`s3` extra. Apply schema migrations first, then relocate one bounded batch:

```bash
uv run chess-crawl-admin relocate --batch-size 100
```

Repeat until `has_more` is false. `remaining` is null while uncounted pending
work exists; `--count-remaining` requests an optional full count. This is an offline,
resumable data operation; it preserves original bytes and moves bodies only
when publication and integrity verification succeed. Keep the external objects
alongside PostgreSQL when backing up and restoring. The
[archive storage guide](archive-storage.md) covers integrity and failure behavior.

## Transfer existing external objects

Relocation handles bodies still stored inline in PostgreSQL. To move already
referenced local/S3 objects to a different configured backend or location, use
the dedicated operator helper from an environment that can read both stores:

```bash
uv run python -m chess_crawl.storage.archive_transfer --batch-size 100 --after-object-id 0
```

Save the returned `next_after_object_id` for the next batch and repeat while
`has_more` is true. The helper verifies original and compressed bytes, then
atomically replaces raw/import references without changing their IDs, owners,
or metadata. It retains old objects and makes no chess-provider requests.
Pause acquisition/import writers and finish with a sweep starting at cursor zero
before taking the destination backup. This maintenance helper crosses workspace
boundaries and is not a tenant API. See [archive transfer](archive-storage.md#transferring-existing-external-objects)
and [moving a local dataset to AWS](aws-deployment.md#moving-a-local-dataset-to-aws)
for configuration, verification, and backup steps.

## Product and process responsibilities

| Responsibility | Entry point |
| --- | --- |
| Public provider catalog and archive summaries | Authenticated `/v1/providers`, `/v1/summary` |
| Bounded imports and opponent discovery | `POST /v1/imports`, `POST /v1/crawls` |
| Stored player facts, history, resources, and games | `/v1/users/{provider}/{username}` and its archive routes |
| Game evidence, moves, clocks, and PGN export | `/v1/games/{id}` and its archive routes |
| Run/job inspection and archived JSONL exports | `/v1/runs`, `/v1/jobs`, `/v1/exports/*` |
| Immutable game selections and derived results | `/v1/working-sets` |
| Background execution | `python -m chess_crawl.jobs.worker` |
| Private event publication | `python -m chess_crawl.events.publisher` |
| Schema migration, readiness, backup relocation | `chess-crawl-admin migrate`, `info`, `relocate` |
| Trusted budget inspection and checkpoint resume | `chess-crawl-admin budgets show`, `budgets resume` |
| Scoped retention of stored analysis results | `chess-crawl-admin prune-results` |
| Scoped retention of expired private exports | `chess-crawl-admin prune-artifacts` |
| Verified transfer of existing external objects | `python -m chess_crawl.storage.archive_transfer` |

All product reads reuse stored data. HTTP submissions enqueue durable work;
workers perform acquisition separately. API tokens determine workspace access.
A database migration is not permission to reload games or run provider requests.
See the [backend guide](backend.md) for exact route schemas and event contracts.


## Validate configuration

`chess-crawl-admin config validate --role worker` checks effective configuration
without opening network connections. `config show` also prints defaults and
source origins with credentials redacted. Optional TOML configuration uses
`CHESS_CRAWL_CONFIG_FILE`; environment values and explicit worker flags override
it. See [runtime configuration](configuration.md) for supported roles, worker
settings, and source precedence.


## Retain private exports

`chess-crawl-admin prune-artifacts --workspace-id example --before 1791244800
--batch-size 10` deletes one resumable batch of expired private export objects and
releases retained quota only after their tracked chunks are removed. The cutoff
is inclusive and never removes artifacts that have not yet expired. Active jobs
and live download leases defer cleanup. Processing/source evidence and immutable
working sets are retained. See [queued archive operations](archive-jobs.md#retention)
for batch bounds, storage permissions, and retry behavior.

## Shared operating policy

`chess-crawl-admin operating-policy show/set/history --provider lichess` manages
versioned pacing and HTTP retry policy using trusted operator database access.
Updates require `--expected-version`; acquisition workers refresh values at
request boundaries while retaining global provider cooldowns. See
[operating policy](operating-policy.md) for administration and per-process
PostgreSQL session admission.
