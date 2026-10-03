# CLI guide

[README](../README.md) · [Backend integration](backend.md) ·
[Contributing](../CONTRIBUTING.md)

Run commands from a source checkout after `uv sync --locked`. The examples use
the default `./chess-crawl.db`. Set `CHESS_CRAWL_CONTACT` to your contact address
before making provider requests; optional provider settings are listed in
[.env.example](../.env.example). The CLI does not load that file automatically.

## Initialize and select an archive

```bash
uv run chess-crawl init
uv run chess-crawl provider list
uv run chess-crawl db info
```

`init` creates the archive or applies pending migrations. To use another path,
place `--db` after the specific command, and supply it consistently:

```bash
uv run chess-crawl init --db data/research.sqlite
uv run chess-crawl report summary --db data/research.sqlite
```

CLI commands default to `./chess-crawl.db`; they do not use `CHESS_CRAWL_DB` to
override that default. The Compose services use `/data/archive.sqlite` inside
their shared volume.

## Fetch data directly

These commands make provider requests immediately. Stop any worker that owns
the same archive before running a direct fetch.

```bash
uv run chess-crawl fetch user chess.com Hikaru
uv run chess-crawl fetch stats chess.com Hikaru
uv run chess-crawl fetch archives chess.com Hikaru
uv run chess-crawl fetch games chess.com Hikaru --month 2024-01

uv run chess-crawl fetch user lichess magnuscarlsen
uv run chess-crawl fetch games lichess magnuscarlsen \
  --since 2024-01-01 --until 2024-02-01 --limit 100
```

Chess.com direct game fetches retrieve a whole monthly archive. Use a queued
import when you need a date window and per-run game budget. Lichess direct game
fetches require both dates and a positive game limit. Profile fetches work for
both providers; statistics and monthly archive-index commands are Chess.com
only.

## Queue a bounded import

```bash
uv run chess-crawl submit import chess.com Hikaru \
  --since 2024-01-01 --until 2024-02-01 --max-games 100 \
  --idempotency-key hikaru-january-2024
```

This creates a run with profile and game jobs and prints `run_id`, `job_ids`,
and `replayed`. It does not contact the provider. The same normalized request
and idempotency key return the original run, including after completion; using
that key for a different request fails.

Dates are UTC: `since` is inclusive and `until` is exclusive. `YYYY-MM` is also
accepted and denotes the first day of that month. The shared application
[request ceilings](backend.md#submit-bounded-work) apply to queued imports and
opponent crawls. Each run counts distinct attributed games, including records
already in the archive. Full raw provider responses are preserved even when
only some games fit the run's date window or budget.

## Discover opponents

All crawl bounds are explicit:

```bash
uv run chess-crawl crawl opponents lichess magnuscarlsen \
  --since 2024-01-01 --until 2024-02-01 \
  --depth 1 --max-users 10 --max-games 100 --max-jobs 30 \
  --enqueue-only --idempotency-key magnus-january-opponents
```

`--enqueue-only` leaves execution to a worker or `jobs resume`. Omit it to run
the crawl immediately in the foreground. Use a stable `--idempotency-key` when
retrying a submission; without that option, each crawl command generates a
new key. Discovery uses games attributed to this run and stays within the
selected provider.

## Inspect and execute queued work

```bash
uv run chess-crawl jobs status
uv run chess-crawl jobs list --limit 20
uv run chess-crawl jobs resume --max-jobs 10
```

Use `jobs show <job-id>` for one job and `jobs status --run <run-id>` for one
run. `jobs resume --run <run-id>` limits foreground execution to that run;
without it, due work across runs is eligible. Replace the placeholders with
IDs from submission or job-list output.

For a process that stays running and picks up new or retried jobs:

```bash
uv run python -m chess_crawl.jobs.worker --db chess-crawl.db
```

The worker owns the archive's acquisition lock until it exits. Read commands
and queued submissions can run alongside it; a second executor cannot. Worker
shutdown stops new claims, and restart recovers orphaned work. Durable retry
deadlines and provider cooldowns survive restarts. See the
[worker guide](backend.md#worker-and-recovery) for details and configuration.

## Query, report, and export

These commands read the local archive without contacting providers:

```bash
uv run chess-crawl query user chess.com Hikaru
uv run chess-crawl query raw --provider chess.com --limit 10
uv run chess-crawl report summary
uv run chess-crawl report user chess.com Hikaru
uv run chess-crawl report opponents lichess magnuscarlsen
uv run chess-crawl report games-by-month --provider lichess

uv run chess-crawl export games --format jsonl --output games.jsonl
uv run chess-crawl export users --format jsonl --output users.jsonl
uv run chess-crawl export graph --format csv --output edges.csv
```

Use `query game <provider> <game-id-or-url>` for an individual game. Exports
accept `--provider` to filter one provider and write to stdout when `--output`
is omitted. Graph export contains recorded discovery edges; opponent reports
derive opponents from normalized games.
The graph is deduplicated across runs; its `crawl_run_id` column retains the
original recorded provenance. Run status counts use separate per-run edge
membership, so rediscovering an existing edge is counted for the new run.

## Command reference

```bash
uv run chess-crawl --help
uv run chess-crawl fetch games --help
uv run chess-crawl crawl opponents --help
uv run python -m chess_crawl.jobs.worker --help
```
