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

All product reads reuse stored data. HTTP submissions enqueue durable work;
workers perform acquisition separately. API tokens determine workspace access.
A database migration is not permission to reload games or run provider requests.
See the [backend guide](backend.md) for exact route schemas and event contracts.
