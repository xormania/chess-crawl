# chess-crawl

Build a local archive of public chess data from Chess.com and Lichess. Use the
command line for collection and analysis, or run the standalone backend to
submit asynchronous work over HTTP and receive job updates through Mercure.

The archive stores original provider responses, normalized profiles and games,
and durable job state in SQLite. Accounts remain provider-scoped: matching
usernames on different providers do not imply the same person.

## Capabilities

- Retrieve public profiles, Chess.com statistics, and games within explicit bounds.
- Import a player's games or discover opponents with date, depth, user, game,
  and job limits.
- Resume durable work with checkpoints, bounded retries, and provider cooldowns.
- Query local data, export games and users as JSONL, and export discovery edges
  as CSV.
- Submit idempotent jobs through an authenticated JSON API and observe committed
  state changes through private Mercure events.

Collection uses public provider APIs and one serial acquisition executor per
archive. The project does not scrape pages, infer identities across providers,
or assign misconduct labels.

## Choose how to run it

| Need | Start here |
| --- | --- |
| An API, background worker, and event hub for another application | [Docker Compose backend](#docker-compose-backend) |
| A local archive and command-line queries or exports | [Run from source](#run-from-source) |
| HTTP routes, authentication, event payloads, and recovery behavior | [Backend and integration guide](docs/backend.md) |
| Fetch, queue, inspect, resume, and export commands | [CLI guide](docs/cli.md) |

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
after archive initialization. With a new, empty archive, the worker waits for
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

The archive persists in a Docker named volume. `docker compose stop` and
`docker compose down` retain it; `docker compose down --volumes` deletes it.
The Compose archive is separate from a CLI archive in your checkout.

### Run from source

Requires Python 3.11+ and `uv`. Acquisition and background processes use POSIX
file locks; run them on Linux/WSL or in the supplied Linux containers.

```bash
uv sync --locked
uv run chess-crawl init
uv run chess-crawl provider list
uv run chess-crawl report summary
```

These commands create and inspect `./chess-crawl.db` without contacting a chess
provider. Set your contact before live collection, then queue a bounded import:

```bash
export CHESS_CRAWL_CONTACT="you@example.com"
uv run chess-crawl submit import chess.com Hikaru \
  --since 2024-01-01 --until 2024-02-01 --max-games 100 \
  --idempotency-key hikaru-january-2024
uv run chess-crawl jobs status
```

Submission returns a run ID and job IDs without fetching data. Execute the
queued work when ready:

```bash
uv run chess-crawl jobs resume
uv run chess-crawl report summary
uv run chess-crawl export games --format jsonl --output games.jsonl
```

The [CLI guide](docs/cli.md) covers other providers, custom archive paths,
opponent discovery, and a persistent worker. Use
`uv run chess-crawl <command> --help` for command options.

## Integration and storage boundaries

SQLite is the only implemented database. The Compose services share one local
archive directory, including SQLite's journal files and process lock files.
Only one acquisition executor may run against an archive; API reads, queued
submissions, and event publishing use their own connections.

An external application connects through the HTTP API and Mercure. A Symfony
application can keep its own Symfony Docker deployment, authentication, and
Turbo rendering. This repository provides the backend and its own hub; it does
not contain a Symfony application. PostgreSQL support has not been implemented.

API submissions and CLI queued imports use an inclusive `since` and exclusive
`until` window. Game budgets count distinct games attributed to a run, including
games already archived. A provider may return a larger response; the raw bytes
are retained even when only part of that response fits the run's bounds.

## Configuration

| Variable | Purpose |
| --- | --- |
| `CHESS_CRAWL_CONTACT` | Contact included in the provider User-Agent; set before live acquisition. |
| `CHESS_CRAWL_USER_AGENT` | Optional full User-Agent override for source-run clients. |
| `CHESS_CRAWL_LICHESS_TOKEN` | Optional Lichess account token. |

The CLI reads exported environment variables and does not automatically load
`.env`. Compose reads `.env` for interpolation and passes the configured contact
and Lichess token to its worker. See [.env.example](.env.example) for provider
settings and the [backend guide](docs/backend.md) for service configuration and
request ceilings.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) for setup, architecture ownership,
validation, and branch workflow. [Pull request guidelines](docs/pull-requests.md)
require evidence for both the problem and the proposed solution.

Update [CHANGELOG.md](CHANGELOG.md) for changes beyond CI and tests, including
documentation changes. Keep the changelog entry focused on the resulting
behavior or contributor-facing change.

## License

[Apache License 2.0](LICENSE). Copyright 2026 xormania.
