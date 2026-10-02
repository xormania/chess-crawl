# chess-crawl

A command-line tool for building a local archive of public chess games and profiles from Chess.com and Lichess.

All data stays on your machine in a SQLite database you control. Only public APIs are used.

## What it does

- Fetch public player profiles and stats
- Download games within explicit date or month bounds
- Explore opponents from the games you fetched (bounded, restartable)
- Query your local archive and export normalized data (JSONL/CSV)
- Preserve the original responses alongside cleaned-up records

## Installation

```bash
pip install chess-crawl
```

Or from a source checkout:

```bash
pip install -e .
```

## Quick start

```bash
# 1. Create a local archive (defaults to ./chess-crawl.db)
chess-crawl init

# 2. See supported providers
chess-crawl provider list

# 3. Fetch a public profile
chess-crawl fetch user lichess magnuscarlsen

# 4. Fetch some games (Lichess uses date ranges)
chess-crawl fetch games lichess magnuscarlsen \
  --since 2024-01-01 --until 2024-02-01 --limit 100

# 5. Explore your archive
chess-crawl report summary
chess-crawl export games --format jsonl --output games.jsonl
```

## Common tasks

### Fetch Chess.com data

Chess.com uses monthly archives:

```bash
chess-crawl fetch user chess.com Hikaru
chess-crawl fetch games chess.com Hikaru --month 2024-06
```

### Explore opponents (bounded crawl)

Opponent discovery is intentionally bounded. All limits are required:

```bash
chess-crawl crawl opponents lichess SomePlayer \
  --depth 1 \
  --max-users 30 --max-games 200 --max-jobs 100 \
  --since 2024-01-01 --until 2024-02-01
```

Use `jobs status` / `jobs resume` if you need to pause and continue.

### Inspect your archive

```bash
chess-crawl db info
chess-crawl query user lichess magnuscarlsen
chess-crawl report user lichess magnuscarlsen
```

### Inspect and resume work

```bash
chess-crawl jobs status
chess-crawl jobs resume
```

### Reports

```bash
chess-crawl report summary
chess-crawl report user chess.com Hikaru
chess-crawl report opponents lichess SomePlayer
chess-crawl report games-by-month --provider lichess
```

### Queries and exports

```bash
chess-crawl query user lichess magnuscarlsen
chess-crawl query game chess.com <game-id-or-url>

chess-crawl export users --format jsonl --output users.jsonl
chess-crawl export graph --format csv --output edges.csv
```

## Configuration

Create a `.env` file or export environment variables.

| Variable                    | Purpose                                      |
|-----------------------------|----------------------------------------------|
| `CHESS_CRAWL_CONTACT`       | Used to build a polite User-Agent.           |
| `CHESS_CRAWL_USER_AGENT`    | Optional full User-Agent override.           |
| `CHESS_CRAWL_LICHESS_TOKEN` | Optional Lichess token for your account limits. |

Example `.env`:

```bash
CHESS_CRAWL_CONTACT=you@example.com
CHESS_CRAWL_USER_AGENT=
CHESS_CRAWL_LICHESS_TOKEN=
```

See `.env.example` for the template.

## How it works

- All data lives in a single SQLite file (use `--db path/to.db` to pick a different one).
- You always provide explicit bounds when fetching or crawling.
- Long-running crawls are resumable — interrupt anytime with Ctrl-C and pick up later.
- Original API responses are saved alongside the cleaned records.
- Chess.com and Lichess accounts are kept completely separate (even if usernames match).

## Rate limits and etiquette

The tool uses conservative delays and respects provider rules (including 429 backoffs). Crawls are serial by design. Don't use this to hammer APIs.

## Development

Persistence has explicit owners:

| Module | Responsibility |
|--------|----------------|
| `storage/db.py` | Open and close SQLite connections; enforce read/write access; commit and roll back transactions. |
| `storage/migrations.py` | Initialize the schema using the caller's connection. |
| `storage/raw.py`, `storage/repository.py`, `storage/discovery.py` | Persist raw responses, normalized records, and discovery edges. |
| `storage/queries.py` | Supply rows for queries, reports, and exports. |
| `jobs/state.py` | Own job and crawl-run creation, claiming, checkpoints, recovery, status, and counters. |

Use `open_database(path)` for a read-only archive and
`open_database(path, writable=True)` for a writer. Storage mutations use the
shared `atomic` decorator; services group related mutations with `transaction`.
Nested mutations use savepoints and cannot commit their caller's transaction.
Persist raw responses and fetch logs before starting normalization, and keep
network calls outside database transactions.

`jobs/runner.py` executes work and `jobs/discovery.py` applies crawl bounds;
both delegate durable job transitions to `jobs/state.py`. There is no separate
job store or report-query implementation.

```bash
uv run ruff check .
uv run mypy .
uv run python -m pytest -q
uv run chess-crawl --help
```

Tests are organized by behavior: storage, providers, jobs, reports/exports, and
CLI commands/workflows. Shared database fixtures live in `tests/conftest.py`;
reusable data builders live in `tests/support.py`. Extend these instead of
copying setup into new phase-specific suites. `test_storage_boundaries.py`
checks transaction behavior and guards the SQLite/SQL ownership rules.

The current suite is offline and blocks network sockets. CI runs the same suite
with `-m "not live and not slow"`. To select just CLI workflows, use
`-m "workflow and not live and not slow"`; to measure coverage, add
`--cov=chess_crawl --cov-report=term-missing`.

See `AGENTS.md` for contribution guidelines.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).  
Copyright 2026 xormania (https://github.com/xormania).
