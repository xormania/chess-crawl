# chess-crawl

Build a local archive of public chess data from Chess.com and Lichess. Run the
standalone backend to submit asynchronous work over HTTP and receive job updates
through Mercure. Chess Dog provides the product interface.

The archive stores original provider responses, normalized profiles and games,
and durable job state in PostgreSQL. Accounts remain provider-scoped: matching
usernames on different providers do not imply the same person.

## Capabilities

- Retrieve public profiles, Chess.com statistics, and games within explicit bounds.
- Preserve profile/resource history, precise move clocks, and original source
  evidence; query them without contacting providers.
- Import a player's games or discover opponents with date, depth, user, game,
  and job limits.
- Collect full history or incremental updates with durable checkpoints and reuse
  archived history; request historical provider enrichment through explicit backfill.
- Resume durable work with checkpoints, bounded retries, and provider cooldowns.
- Query local data, export games and users as JSONL, and export discovery edges
  as CSV.
- Submit idempotent jobs through an authenticated JSON API and observe committed
  state changes through private Mercure events.
- Create immutable selections of game versions and reuse computed outputs only
  when their inputs, settings, and implementation version match.

Cheating detection and analysis are intended project goals. The current
implementation provides data collection and archive infrastructure, using
provider APIs and coordinated acquisition with concurrent durable processing.

## Choose how to run it

| Need | Start here |
| --- | --- |
| An API, background worker, and event hub for another application | [Docker Compose backend](#docker-compose-backend) |
| Source-run API, workers, and archive maintenance | [Run from source](#run-from-source) |
| HTTP routes, authentication, event payloads, and recovery behavior | [Backend and integration guide](docs/backend.md) |
| Database migrations, readiness, and compressed backup relocation | [Operations guide](docs/cli.md) |
| A pinned toolchain for contributing from Linux, WSL2, or Apple Silicon macOS | [Devbox development setup](CONTRIBUTING.md#development-setup) |
| Private AWS deployment definitions, source-object storage, and cost evidence | [AWS foundation](docs/aws-deployment.md) |

Both paths start with a source checkout:

```bash
git clone https://github.com/xormania/chess-crawl.git
cd chess-crawl
```

### Docker Compose backend

Requires Python 3.11+ on the host for credential bootstrap and Docker with the
Compose v2 plugin and Linux container support. No host Python dependencies are
needed for this path.

```bash
python3 scripts/bootstrap_dev.py
docker compose up --build --detach --wait --wait-timeout 120
docker compose ps
```

This starts the API, worker, event publisher, and this project's own Mercure hub
after PostgreSQL readiness and archive initialization. With a new, empty archive, the worker waits for
submitted jobs without fetching chess data.

| Endpoint | Purpose |
| --- | --- |
| `http://127.0.0.1:8000/docs` | Interactive API documentation |
| `http://127.0.0.1:8000/openapi.json` | API schema |
| `http://127.0.0.1:3000/.well-known/mercure` | Private event subscriptions |

Bootstrap writes development credentials to the ignored `data/dev-secrets/`
directory. API requests use its `api_token`; Mercure subscriptions use a separate
subscriber JWT. See the [backend guide](docs/backend.md) for authenticated
examples, provider contact configuration, and application integration.

The archive persists in PostgreSQL and compressed objects in the `archive_data`
named volume. Back up both; see [archive storage](docs/archive-storage.md).
`docker compose stop` and
`docker compose down` retain it; `docker compose down --volumes` deletes it.
Use `docker compose exec api chess-crawl-admin info` to inspect the same database. PostgreSQL is available only inside the Compose network by
default; its port is not published on the host.

For a separately managed database, use the
[external PostgreSQL overlay](docs/backend.md#external-postgresql). It omits the
bundled server and mounts a CA certificate for verified TLS.

### Run from source

Requires Python 3.11+, `uv`, and an accessible PostgreSQL 18 database created
for chess-crawl. The recommended local environment is the Compose stack above.
For a separately provisioned server, export its connection settings:

```bash
export CHESS_CRAWL_DATABASE_URL="postgresql://chess_crawl@localhost:5432/chess_crawl"
export CHESS_CRAWL_DATABASE_PASSWORD_FILE="/path/to/database-password"
export CHESS_CRAWL_DATABASE_TRANSPORT="local" # This example uses loopback.
uv sync --locked --extra api
uv run chess-crawl-admin migrate
uv run chess-crawl-admin info
```

For a remote server, keep the default `verified` transport and supply its trusted
PEM CA using `CHESS_CRAWL_DATABASE_SSL_ROOT_CERT_FILE`. The PostgreSQL database
must already exist. `migrate` applies packaged schema migrations without making
provider requests; `info` reads its current readiness.

Configure an API token and start the API with
`uv run uvicorn chess_crawl.api:create_app --factory --host 127.0.0.1 --port 8000`.
Run background execution with `uv run python -m chess_crawl.jobs.worker`, and
private event delivery with `uv run python -m chess_crawl.events.publisher`.
See the [backend guide](docs/backend.md) for authentication, request schemas,
worker configuration, and private Mercure subscriptions. Set your contact
before live acquisition.

Collection, job inspection, archived player/game reads, exports, and analysis
selections use the authenticated API. The retired `chess-crawl` product commands
are replaced by those routes; only operational administration retains a console
entrypoint. The [operations guide](docs/cli.md) maps these responsibilities.

## Integration and storage boundaries

PostgreSQL is the only supported database. Compose provides PostgreSQL 18 with
persistent storage, and application services connect over its private network.
Workers own individual jobs through PostgreSQL session locks and fencing tokens.
Acquisition is serialized per provider; local processing and different providers
can progress independently. Ordinary writes share a migration gate and lock only
conflicting resources; migrations take the gate exclusively. Event publication
retains one publisher per archive. API reads and submissions use separate
connections and never execute provider acquisition or normalization jobs.

An external application connects through the HTTP API and Mercure. A Symfony
application can keep its own Symfony Docker deployment, authentication, and
Turbo rendering. This repository provides the backend and its own hub; it does
not contain a Symfony application.

Date windows use an inclusive `since` and exclusive `until`. Bounded imports and
opponent crawls count distinct games attributed to a run, including archived games.
Full/incremental/backfill imports use `max_games` as a page budget. Preserved source
bytes remain available even when interpretation or run attribution is incomplete.
See [collection](docs/collection.md) for provider timestamp and cache semantics.

## Configuration

| Variable | Purpose |
| --- | --- |
| `CHESS_CRAWL_DATABASE_URL` | Password-free PostgreSQL connection URL; Compose supplies its private server by default. |
| `CHESS_CRAWL_DATABASE_PASSWORD_FILE` | Database password file for source-run commands; Compose mounts its generated secret. |
| `CHESS_CRAWL_DATABASE_PASSWORD` | Alternative database password; configure only one password source. |
| `CHESS_CRAWL_DATABASE_TRANSPORT` | `verified` by default; `local` explicitly permits Unix sockets or loopback connections. Bundled Compose additionally trusts its exact `postgres` hostname. |
| `CHESS_CRAWL_DATABASE_SSL_ROOT_CERT_FILE` | PEM CA file for verified source-run connections; external Compose mounts it from `CHESS_CRAWL_DATABASE_CA_FILE`. |
| `CHESS_CRAWL_CONTACT` | Contact included in the provider User-Agent; set before live acquisition. |
| `CHESS_CRAWL_USER_AGENT` | Optional full User-Agent override for source-run clients. |
| `CHESS_CRAWL_LICHESS_TOKEN` | Optional Lichess account token. |
| `CHESS_CRAWL_LICHESS_TOKEN_OWNER_SCOPE` | Workspace owning that token; defaults to `local`. Required to match for private team resources. |

Source-run services read exported environment variables and do not automatically load
`.env`. Compose reads `.env` for interpolation and passes the configured contact
and Lichess token to its worker. See [.env.example](.env.example) for provider
settings and the [backend guide](docs/backend.md) for service configuration and
request ceilings.

Lichess game requests retain available clocks, evaluations, and accuracy data
in the raw archive by default. The capture switches are listed in
[.env.example](.env.example); disabling one affects subsequent requests.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) for development and contribution
guidance, and [PROJECT.md](PROJECT.md) for the top-level directory organization.

## License

[Apache License 2.0](LICENSE). Copyright 2026 xormania.
