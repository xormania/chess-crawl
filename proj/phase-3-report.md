# Phase 3 Report

## Phase 3 Objective

Turn the Phase 2 point-fetching implementation into a usable local archive workflow by adding durable jobs, resumable bounded opponent discovery, practical reports, normalized exports, docs, and hardening while keeping default tests fast and offline.

## Summary Of What Was Implemented

- Added durable job persistence helpers over the existing `discovery_jobs` and `crawl_runs` tables.
- Added serial job runner support for `pending`, `in_progress`, `done`, `error`, `skipped`, and `blocked`.
- Added live job deduplication, parent job links, crawl-run links, params JSON, depth, priority, attempts, and stale `in_progress` resume.
- Added bounded opponent discovery as a strategy using normalized `game_participants`, provider-scoped users, and idempotent `discovery_edges`.
- Added job/run CLI inspection and resume commands.
- Added practical read-side reports for archive summary, user summary, opponents, and games by month.
- Added normalized local exports for games/users JSONL and discovery graph CSV.
- Updated README with end-to-end workflow, bounded crawl examples, jobs/resume, reports, exports, safety boundaries, and limitations.
- Tightened `.gitignore` for SQLite sidecars, generated exports, and local dumps while keeping fixtures trackable.

## Files Changed

- `.gitignore`
- `README.md`
- `src/chess_crawl/cli.py`
- `src/chess_crawl/ingest.py`
- `src/chess_crawl/export/__init__.py`
- `src/chess_crawl/export/writers.py`
- `src/chess_crawl/jobs/__init__.py`
- `src/chess_crawl/jobs/discovery.py`
- `src/chess_crawl/jobs/models.py`
- `src/chess_crawl/jobs/runner.py`
- `src/chess_crawl/jobs/store.py`
- `src/chess_crawl/reports/queries.py`
- `tests/test_phase3_jobs.py`
- `tests/test_phase3_reports_exports.py`
- `proj/phase-3-report.md`

## Commands Added

```bash
chess-crawl crawl opponents PROVIDER USERNAME \
  --depth N --max-users N --max-games N --max-jobs N \
  --since DATE_OR_MONTH --until DATE_OR_MONTH \
  --db ./data/chess-crawl.sqlite

chess-crawl jobs status --db ./data/chess-crawl.sqlite
chess-crawl jobs list --db ./data/chess-crawl.sqlite
chess-crawl jobs show JOB_ID --db ./data/chess-crawl.sqlite
chess-crawl jobs resume --db ./data/chess-crawl.sqlite

chess-crawl report summary --db ./data/chess-crawl.sqlite
chess-crawl report user PROVIDER USERNAME --db ./data/chess-crawl.sqlite
chess-crawl report opponents PROVIDER USERNAME --db ./data/chess-crawl.sqlite
chess-crawl report games-by-month --provider PROVIDER --db ./data/chess-crawl.sqlite

chess-crawl export games --format jsonl --output games.jsonl --db ./data/chess-crawl.sqlite
chess-crawl export users --format jsonl --output users.jsonl --db ./data/chess-crawl.sqlite
chess-crawl export graph --format csv --output graph.csv --db ./data/chess-crawl.sqlite
```

## Job And Crawl Behavior Implemented

- Jobs are inserted with deterministic dedup keys and existing live jobs in `pending`, `in_progress`, or `blocked` are reused.
- Claiming is serial and atomic: one job is marked `in_progress`, `started_at` is set, and `attempts` is incremented.
- Terminal states are marked through explicit helpers; errors are also recorded in the `errors` table for unexpected runner failures.
- `jobs resume` resets stale `in_progress` jobs to `pending`, unblocks `blocked` jobs, and drives the serial runner.
- Fetch logs can now carry `job_id` and `crawl_run_id` when ingestion is driven by jobs.
- Opponent crawl requires explicit depth, date/month window, max users, max games, and max jobs.
- Crawl never crosses providers and reads opponents only from normalized provider-scoped participants.
- Discovery edges are idempotent: repeated discovery updates keep minimum depth and do not inflate `game_count`.
- The runner is serial only; no concurrency was introduced.

## Reports And Exports Implemented

- `report summary`: providers, user/game counts, raw payload count, crawl-run status, job states.
- `report user`: provider-scoped game totals, rated/unrated counts, W/D/L/unfinished, first/last game, distinct opponents, provider-supplied account status label if present.
- `report opponents`: provider-scoped opponent list with NULL-outcome-aware W/D/L/unfinished counts.
- `report games-by-month`: provider-scoped monthly game counts and outcome buckets.
- `export games --format jsonl`: normalized game rows only; no raw payload bodies.
- `export users --format jsonl`: provider-scoped user rows.
- `export graph --format csv`: discovery edges with provider preserved; providers are never merged.

## Tests Added

- Job enqueue and live dedup.
- Job state transitions.
- Stale `in_progress` resume and blocked unblock.
- Idempotent discovery edge insertion.
- Fake fixture-backed mini graph crawl with bounded depth.
- Duplicate/reverse frontier dedupe during crawl.
- `max_users`, `max_jobs`, and `max_games` enforcement.
- Provider boundary for same username across Chess.com/Lichess through report/export data.
- NULL-outcome-aware report queries.
- Export JSONL/CSV content checks.
- CLI smoke for jobs, reports, and exports.

## Validation Commands Run

Preflight:

```bash
pwd
```

Result: `/home/work/projects/xormania/chess-crawl`.

```bash
git status --short --branch
```

Result: branch `dev` with existing modified/untracked Phase 1/2 files before this phase.

```bash
python -m pytest
```

Result: failed because this container has no `python` executable: `/bin/bash: line 1: python: command not found`.

```bash
uv run --extra dev python -m pytest
```

Pre-change result: passed, `21 passed in 0.31s`.

Implementation checks:

```bash
uv run python -m compileall -q src
```

Result: passed.

```bash
uv run --extra dev python -m pytest
```

Final test result: passed, `29 passed in 0.41s` on the last run.

Requested validation commands:

```bash
python -m pytest
```

Result: failed because this container has no `python` executable.

```bash
python -m chess_crawl.cli --help || python -m chess_crawl --help
```

Result: failed because this container has no `python` executable.

```bash
uv run python -m chess_crawl.cli --help
uv run python -m chess_crawl --help
```

Result: both passed and showed `{init,provider,db,fetch,query,crawl,jobs,report,export}`.

```bash
python -m chess_crawl.cli provider list || python -m chess_crawl provider list
```

Result: failed because this container has no `python` executable.

```bash
uv run python -m chess_crawl.cli provider list
```

Result: passed and listed Chess.com plus Lichess with expected caching/rate-limit policies.

Offline smoke:

```bash
python -m chess_crawl.cli init --db /tmp/chess-crawl-smoke.sqlite || python -m chess_crawl init --db /tmp/chess-crawl-smoke.sqlite
```

Result: failed because this container has no `python` executable.

```bash
uv run python -m chess_crawl.cli init --db /tmp/chess-crawl-smoke.sqlite
```

Result: passed, schema version `1`, migration `0001_init`, providers `chess.com, lichess`.

```bash
git status --short
```

Result: showed `.gitignore`, `README.md`, `src/chess_crawl/cli.py`, and Phase 3 files changed, plus the pre-existing Phase 1/2 untracked source/test/proj files still uncommitted. No remotes, branches, or git config were changed.

## Known Limitations

- The CLI remains stdlib `argparse`, consistent with prior phases, rather than Typer from the plan.
- `crawl_opponents` uses a pragmatic serial path: each crawl job fetches bounded games for that user, then expands from normalized participants. The fuller fan-out model with separate child fetch jobs for every archive unit remains backlog.
- Lichess NDJSON remains buffered per bounded request. Per-line stream checkpointing is not implemented.
- Chess.com monthly archives are the public API's game unit. The runner stops scheduling work when `max_games` is reached, but a fetched monthly archive can still contain more games than the remaining budget.
- `fetch_games_by_ids`, local dump import, and Chess.com game-by-id archive resolution are not implemented in this slice.
- Reports are intentionally practical and factual; anomaly/review signals were not implemented.
- Exports are JSONL for games/users and CSV for graph edges only.

## Deviations From `proj/plan.md`

- Acquisition commands from Phase 2 still perform direct bounded fetches; Phase 3 adds job/crawl commands without converting every existing fetch command into a durable job.
- Opponent crawl fetches games inside `crawl_opponents` jobs instead of modeling `crawl_opponents` as a no-fetch scheduler-only job.
- Blocked jobs have no `requeue_after` column in the existing schema, so `jobs resume` explicitly unblocks them rather than waiting for due times.
- Job dedup code treats `blocked` as live even though the schema's partial unique index only covers `pending` and `in_progress`.

## Remaining Backlog

- Convert direct `fetch ...` CLI commands to enqueue and run durable jobs.
- Add full archive-unit fan-out jobs with per-unit cursors.
- Add Lichess incremental NDJSON checkpointing.
- Add Chess.com archive-mediated game-by-id and bounded games-by-ids support.
- Add local dump import.
- Add optional live smoke tests behind an explicit opt-in marker.
- Add richer coverage reports and re-normalization commands.

## Risks And Design Decisions To Preserve

- Provider is part of every job, report, export, and graph edge boundary. Do not infer cross-provider identity.
- Keep default tests offline and fixture/fake backed.
- Keep the runner serial; do not add concurrency without revisiting provider rate-limit policy.
- Preserve raw-first ingestion: provider bodies are stored before normalization.
- Do not add anomaly or cheating-related outputs without explicit flags and the plan's disclaimer.
