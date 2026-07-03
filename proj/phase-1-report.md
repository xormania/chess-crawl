# Phase 1 Report

## Objective

Build the provider-neutral, raw-first archive foundation for `chess-crawl` without real Chess.com or Lichess network fetching.

## Implemented

- Kept the existing `src/chess_crawl/` layout and expanded it into the Phase 1 module structure.
- Added provider-neutral DTOs and protocol seams in `providers/base.py`.
- Added a static provider registry for `chess.com` and `lichess`, including capability notes and Phase 1 fetch policies.
- Added Chess.com and Lichess endpoint-builder modules only; no network clients.
- Added SQLite connection helpers with `foreign_keys=ON`, file-DB WAL, `synchronous=NORMAL`, and `busy_timeout=5000`.
- Added idempotent schema initialization/migration tracking.
- Added raw payload helpers with `sha256:<hex>` body hashes over decompressed bytes, gzip-at-rest for larger bodies, readback verification, duplicate-body short-circuiting, `source_records` insertion, and `fetch_logs` insertion.
- Added provider/user, variant, time-control, game stub, and participant repository helpers.
- Added importable skeletons for `normalize`, `jobs`, `reports`, and `export`.
- Added an argparse CLI with:
  - `init --db PATH`
  - `provider list`
  - `db info --db PATH`
- Updated README with current status, raw-first/provider-neutral/local-first framing, commands, public API/no scraping/no accusations stance, and Phase 1 limitations.
- Added pytest-based offline tests and a dev extra for pytest. Runtime dependencies remain empty.

## Files Changed

- `pyproject.toml`
- `README.md`
- `src/chess_crawl/__init__.py`
- `src/chess_crawl/__main__.py`
- `src/chess_crawl/cli.py`
- `src/chess_crawl/config.py`
- `src/chess_crawl/providers/__init__.py`
- `src/chess_crawl/providers/base.py`
- `src/chess_crawl/providers/registry.py`
- `src/chess_crawl/providers/chesscom/__init__.py`
- `src/chess_crawl/providers/chesscom/endpoints.py`
- `src/chess_crawl/providers/lichess/__init__.py`
- `src/chess_crawl/providers/lichess/endpoints.py`
- `src/chess_crawl/storage/__init__.py`
- `src/chess_crawl/storage/db.py`
- `src/chess_crawl/storage/migrations.py`
- `src/chess_crawl/storage/raw.py`
- `src/chess_crawl/storage/repository.py`
- `src/chess_crawl/storage/schema.sql`
- `src/chess_crawl/normalize/__init__.py`
- `src/chess_crawl/normalize/codes.py`
- `src/chess_crawl/jobs/__init__.py`
- `src/chess_crawl/jobs/models.py`
- `src/chess_crawl/reports/__init__.py`
- `src/chess_crawl/reports/queries.py`
- `src/chess_crawl/export/__init__.py`
- `tests/conftest.py`
- `tests/test_cli.py`
- `tests/test_provider_registry.py`
- `tests/test_storage_foundation.py`
- `proj/phase-1-report.md`

## Schema Tables

Created all Phase 1 required tables:

- `providers`
- `provider_users`
- `user_snapshots`
- `games`
- `game_participants`
- `ratings_at_game`
- `time_controls`
- `variants`
- `raw_payloads`
- `source_records`
- `fetch_logs`
- `discovery_jobs`
- `discovery_edges`
- `crawl_runs`
- `errors`
- `schema_migrations`

## Schema Indexes

Important indexes and uniqueness constraints:

- `provider_users`: partial unique `ux_pu_provider_pid`, unique `ux_pu_provider_uname`
- `games`: partial unique `ux_games_provider_gid`, partial unique `ux_games_url`, unique `ux_games_content_hash`
- `raw_payloads`: provider/endpoint, body hash, normalization queue partial index, canonical key/time
- `source_records`: entity lookup, payload lookup, unique `(entity_type, entity_id, raw_payload_id)`
- `fetch_logs`: provider/time, status, URL
- `discovery_jobs`: runnable queue, run/state, live dedup partial unique index
- `discovery_edges`: from-user, to-user, unique provider-scoped directed edge
- `time_controls`: unique normalized tuple with nullable fields normalized through `COALESCE`
- `user_snapshots`: user/time and unique `(provider_user_id, content_hash)`
- `errors` and `crawl_runs`: operational lookup indexes

## Commands Added

```bash
chess-crawl init --db ./data/chess-crawl.sqlite
chess-crawl provider list
chess-crawl db info --db ./data/chess-crawl.sqlite
```

Equivalent source-tree invocation:

```bash
uv run python -m chess_crawl.cli init --db ./data/chess-crawl.sqlite
uv run python -m chess_crawl.cli provider list
uv run python -m chess_crawl.cli db info --db ./data/chess-crawl.sqlite
```

`python -m chess_crawl` is wired through `src/chess_crawl/__main__.py`.

## Tests Added

- Schema creation includes all canonical tables.
- Initialization is idempotent.
- Providers are seeded.
- Provider registry lists Chess.com and Lichess with expected capability facts.
- Provider-scoped users can coexist with the same username.
- Foreign keys are enforced.
- `games.outcome` can be `NULL` and `games.is_live` is stored.
- Raw payload storage deduplicates repeated bodies and verifies `body_hash`.
- CLI smoke covers `init`, `provider list`, `db info`, and missing DB errors.
- An autouse socket guard prevents accidental network calls in in-process tests.

## Validation Commands Run

The container has no bare `python` command and no system `pytest`.

```bash
python -m pytest
```

Result: failed, `/bin/bash: python: command not found`.

```bash
python3 -m pytest
```

Result: failed, `/usr/bin/python3: No module named pytest`.

```bash
uv run --with pytest python -m pytest
```

Result: passed, `9 passed in 0.16s`.

After adding `dev = ["pytest>=8,<10"]`:

```bash
uv run --extra dev python -m pytest
```

Result: passed, `9 passed in 0.15s`.

Requested help command:

```bash
python -m chess_crawl.cli --help || python -m chess_crawl --help
```

Result: failed because `/bin/bash: python: command not found`.

Equivalent entrypoint checks:

```bash
uv run python -m chess_crawl.cli --help
uv run python -m chess_crawl --help
```

Result: both passed and printed help for `{init,provider,db}`.

Manual CLI smoke:

```bash
uv run python -m chess_crawl.cli provider list
uv run python -m chess_crawl.cli init --db ./data/chess-crawl.sqlite
uv run python -m chess_crawl.cli db info --db ./data/chess-crawl.sqlite
```

Result: all passed. `db info` reported schema version `1`, migration count `1`, table count `16`, and providers `chess.com, lichess`.

## Known Limitations

- No real HTTP clients are implemented.
- No Chess.com or Lichess parser is implemented.
- No fetch, crawl, jobs resume/status, query, or export commands beyond Phase 1 inspection commands.
- `storage/repository.py` contains minimal game/participant helpers only; live-game mutation and Chess.com rename reconciliation need Phase 2/4 refinement.
- Raw dedup currently short-circuits on `(provider, endpoint_type, body_hash)` in helper code. This is sufficient for Phase 1 idempotency tests, but later phases may want more nuanced fetch-log provenance for byte-identical bodies from different logical resources.
- `zstd` is not supported in Phase 1; gzip is the only compression codec.

## Deviations From `proj/plan.md`

- CLI uses stdlib `argparse` instead of Typer to keep Phase 1 dependency-free. The user instruction explicitly allowed this.
- Runtime dependencies remain empty. `pytest` is declared only as a dev extra for test execution.
- `storage/schema.sql` implements one idempotent baseline migration rather than a multi-file migration series.
- Provider modules contain endpoint builders and registry metadata only, not `client.py` or `parser.py`, because Phase 1 excludes network fetching and parsing.
- `jobs`, `reports`, and `export` are importable skeletons, not functional subsystems.

## Next Phase Checklist

- Implement Chess.com endpoint tests and then `providers/chesscom/client.py`.
- Implement Chess.com parser and normalization for profiles, stats, archives, and monthly games.
- Add `normalize/users.py` and `normalize/games.py` with parser-versioned reprocessing.
- Implement versioned per-game `content_hash` and live-game update behavior.
- Add raw-first ingest path that stores raw payloads before normalization and writes `source_records`.
- Add Chess.com `fetch user`, `fetch games`, `query user`, and `query game` CLI commands.
- Add conditional GET/ETag/304 handling and fetch-log rows.
- Expand tests around Chess.com result codes, bughouse raw-only skip, nullable outcomes, and re-normalization without HTTP.

## Risks And Design Notes

- The schema is intentionally close to the plan but still a first DDL. Treat `schema.sql` as the current source of truth and add migrations rather than editing a live DB in place.
- Provider-scoped identity is enforced by schema and tests; do not add cross-provider joins or aliasing in Phase 2.
- Keep default tests offline. If later tests need HTTP behavior, use fake transports and keep live tests behind an explicit opt-in marker.
- The repo uses `src/` layout. In a raw source checkout, use `uv run ...` or set `PYTHONPATH=src`; installed console scripts work through `pyproject.toml`.
