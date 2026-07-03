# Phase 2 Report

## Objective

Add real bounded provider ingestion for Chess.com and Lichess on top of the Phase 1 raw-first, provider-neutral SQLite foundation. Store successful provider response bodies as raw payloads before normalization, keep provider-specific behavior isolated, and keep tests offline and fixture-based.

## Summary

- Added a shared synchronous `httpx` HTTP helper with descriptive User-Agent, configurable timeout, serial request delay, response metadata capture, simple retry/backoff, Chess.com Retry-After handling, and the Lichess hard 60-second 429 wait policy.
- Added Chess.com client/parser support for public profile, stats, archives index, and monthly archive endpoints.
- Added Lichess client/parser support for public user profiles, bounded user games NDJSON, and a single-game JSON service/parser path.
- Added direct bounded ingest service functions used by the CLI. This phase deliberately does not add full durable job/crawl complexity.
- Added raw-first normalization for provider users, user snapshots, games, participants, ratings at game, time controls, and variants where fixture data supports it.
- Added local query commands for users, games, and raw payloads.
- Added offline fixtures and tests for endpoint construction, HTTP statuses, raw-first ordering, normalization, provider-scoped identity, and CLI smoke.

## Files Changed

- `pyproject.toml`
- `uv.lock`
- `README.md`
- `src/chess_crawl/cli.py`
- `src/chess_crawl/ingest.py`
- `src/chess_crawl/providers/base.py`
- `src/chess_crawl/providers/http.py`
- `src/chess_crawl/providers/registry.py`
- `src/chess_crawl/providers/chesscom/client.py`
- `src/chess_crawl/providers/chesscom/parser.py`
- `src/chess_crawl/providers/lichess/client.py`
- `src/chess_crawl/providers/lichess/parser.py`
- `src/chess_crawl/normalize/codes.py`
- `src/chess_crawl/normalize/users.py`
- `src/chess_crawl/normalize/games.py`
- `src/chess_crawl/storage/raw.py`
- `src/chess_crawl/storage/repository.py`
- `src/chess_crawl/reports/queries.py`
- `tests/conftest.py`
- `tests/test_cli.py`
- `tests/test_phase2_providers.py`
- `tests/fixtures/chesscom/player.json`
- `tests/fixtures/chesscom/stats.json`
- `tests/fixtures/chesscom/archives.json`
- `tests/fixtures/chesscom/archive_2024_01.json`
- `tests/fixtures/lichess/user.json`
- `tests/fixtures/lichess/games.ndjson`
- `tests/fixtures/lichess/game.json`
- `proj/phase-2-report.md`

## Provider Coverage

Chess.com:

- `GET /pub/player/{username}`
- `GET /pub/player/{username}/stats`
- `GET /pub/player/{username}/games/archives`
- `GET /pub/player/{username}/games/{YYYY}/{MM}`
- Conditional ETag/Last-Modified headers are sent when a prior raw row has validators.
- `304`, `404`, `410`, `429`, and retryable `5xx` statuses are handled by the shared HTTP layer and logged through `fetch_logs`.
- Monthly archive games normalize to `games`, `game_participants`, `ratings_at_game`, `time_controls`, and `variants`.
- Chess.com single-game-by-id was not invented.

Lichess:

- `GET /api/user/{username}`
- `GET /api/games/user/{username}` with required CLI bounds and NDJSON handling
- `GET /api/game/{gameId}` implemented as a service/parser path, not exposed as a Phase 2 CLI fetch command
- Optional bearer token from `CHESS_CRAWL_LICHESS_TOKEN` is supported through config and not logged.
- `429` uses the hard 60-second wait policy; tests inject a sleeper so no real wait occurs.
- Lichess millisecond timestamps normalize to epoch seconds while raw milliseconds remain in `raw_payloads`.

## Commands Added

```bash
chess-crawl fetch user chess.com USERNAME --db ./data/chess-crawl.sqlite
chess-crawl fetch user lichess USERNAME --db ./data/chess-crawl.sqlite
chess-crawl fetch stats chess.com USERNAME --db ./data/chess-crawl.sqlite
chess-crawl fetch archives chess.com USERNAME --db ./data/chess-crawl.sqlite
chess-crawl fetch games chess.com USERNAME --month YYYY-MM --db ./data/chess-crawl.sqlite
chess-crawl fetch games lichess USERNAME --since YYYY-MM-DD --until YYYY-MM-DD --limit N --db ./data/chess-crawl.sqlite
chess-crawl query user PROVIDER USERNAME --db ./data/chess-crawl.sqlite
chess-crawl query game PROVIDER GAME_ID --db ./data/chess-crawl.sqlite
chess-crawl query raw --provider PROVIDER --limit N --db ./data/chess-crawl.sqlite
```

Bounded retrieval is enforced for game fetches: Chess.com requires `--month`; Lichess requires `--since`, `--until`, and `--limit`.

## Normalization Implemented

- Username normalization and provider-scoped user identity.
- User profile normalization into `provider_users` and `user_snapshots` for both providers.
- Chess.com stats raw storage and minimal snapshot normalization.
- Deterministic content hashes for snapshots and games.
- Variant mapping for both providers.
- Time-class mapping, including Chess.com daily/Lichess correspondence to `correspondence`.
- Basic time-control parsing for clock and correspondence-style labels.
- Chess.com per-color result mapping to shared outcome.
- Lichess winner/status mapping to shared outcome with nullable outcome support.
- `is_live` support for non-terminal Lichess statuses and undecided Chess.com rows.
- Basic PGN preservation in parser DTOs where present; the current schema has no dedicated PGN column, so PGN remains preserved in raw payloads.

## Tests Added

- Chess.com endpoint construction.
- Lichess endpoint construction.
- HTTP mocked `200` raw storage and normalization.
- HTTP mocked `304` with conditional headers and no new raw payload.
- HTTP mocked `404` and `410` logging without raw payload rows.
- HTTP mocked Lichess `429` with exactly one injected `sleep(60.0)` before retry.
- Raw payload exists before normalizer execution.
- User normalization for both providers.
- Chess.com stats snapshot normalization.
- Game normalization for one Chess.com monthly archive fixture.
- Game normalization for one Lichess NDJSON fixture.
- Lichess ms-to-seconds conversion.
- Provider-scoped same username across providers.
- CLI `fetch user`, `query user`, and `query raw` smoke with fixture ingestion.

## Validation Commands Run

```bash
python -m pytest
```

Result: failed because this container has no `python` executable:

```text
/bin/bash: line 1: python: command not found
```

Equivalent source-tree command:

```bash
uv run --extra dev python -m pytest
```

Result: passed, `21 passed in 0.32s`.

Requested help command:

```bash
python -m chess_crawl.cli --help || python -m chess_crawl --help
```

Result: failed because this container has no `python` executable.

Equivalent source-tree checks:

```bash
uv run python -m chess_crawl.cli --help
uv run python -m chess_crawl --help
```

Result: both passed and showed `{init,provider,db,fetch,query}`.

Requested provider-list command:

```bash
python -m chess_crawl.cli provider list || python -m chess_crawl provider list
```

Result: failed because this container has no `python` executable.

Equivalent source-tree checks:

```bash
uv run python -m chess_crawl.cli provider list
uv run python -m chess_crawl provider list
```

Result: both passed and listed Chess.com plus Lichess, including `Retry-After` and `wait 60s`.

## Known Limitations

- No durable job runner, crawl resume, crawl status, opponent discovery, or exports were added in this phase.
- Lichess NDJSON is buffered for the bounded command before normalization. Incremental streaming checkpoint durability remains Phase 3 work.
- Lichess perfs are normalized from the profile payload; there is no separate `fetch stats lichess` command.
- Lichess single-game fetch is implemented in client/service/parser code but not exposed as a required Phase 2 CLI command.
- `games` has no PGN column in the Phase 1 schema, so PGN/header text remains raw-only despite parser DTO preservation.
- Bughouse games are skipped from two-player normalized tables, but no separate skip-reason column exists in the current schema.
- Full Chess.com game-by-id resolution through owning monthly archives is not implemented as a CLI workflow.
- Default tests are offline only; no live API drift tests were added.

## Deviations From `proj/plan.md`

- The plan originally labels Chess.com as Phase 2 and Lichess as Phase 3. The user instruction for this turn explicitly requested both providers, so this pass implements a bounded Lichess slice too.
- The CLI remains stdlib `argparse` from Phase 1 instead of migrating to Typer.
- The direct ingest service performs bounded fetches immediately instead of enqueueing `discovery_jobs`.
- Lichess stream handling is bounded-buffer ingestion rather than fully incremental resumable streaming.
- No by-id Chess.com endpoint was invented; that matches the plan and public API reality.

## Remaining Work For Phase 3

- Add the durable serial job runner and resume/status commands.
- Convert direct fetch commands to enqueue/run jobs or share the same job execution path.
- Add incremental Lichess NDJSON streaming with checkpointing and mid-stream durability.
- Expose Lichess single-game fetch and batch-by-ids if still desired.
- Add import/export commands and graph/game exports.
- Add re-normalization commands for stale parser versions.
- Add richer read-side reports and documented neutral handling of provider status labels.
- Add optional live smoke tests behind an explicit opt-in marker.

## Risks And Design Decisions

- `raw_payloads` dedup now includes `canonical_source_key`, avoiding accidental collapse of byte-identical bodies from different logical resources while preserving idempotency for the same resource.
- Fetch logs are written after raw storage, so successful body attempts can reference the raw payload id. Raw is still committed before normalization.
- Request headers are not logged; optional Lichess bearer tokens are only sent on the wire.
- Chess.com game participant `uuid` is not treated as the stable `provider_user_id`; participant rows link by provider-scoped username until a profile fetch can fill the stable `player_id`.
- The schema remains unchanged. This keeps Phase 2 small but means PGN and bughouse skip reasons are raw-only details for now.
- The worktree already contained Phase 1 untracked/modified files before this phase. This report lists the Phase 2 files touched by this pass.
