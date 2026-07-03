# Testing System Audit

**Date:** 2026-07-03  
**Project:** chess-crawl  
**Command run:** `python -m pytest` (default validation)  
**Result:** 29 passed (0.38s)  
**Coverage:** 72% (branch coverage lower on key modules)  
**Git:** clean on dev at start of audit

## Executive Summary

The testing system is **fast, deterministic, and offline-by-default**. Core invariants (raw-first storage, provider scoping, idempotency, bounded crawl caps, null-outcome handling) are exercised and pass reliably. The `no_network` guard and heavy use of `httpx.MockTransport` + `:memory:` databases keep the suite hermetic and quick.

However, the suite has **significant blind spots**, hygiene problems, duplication, and coverage gaps in the CLI surface and job execution engine. A trivial placeholder "smoke" test exists. Many CLI commands and important branches have no automated test coverage. Resource leaks produce noisy warnings under coverage. The structure has diverged from the plan and will become harder to maintain.

## Test Inventory

| Module                              | Tests | Focus                                      | Notes |
|-------------------------------------|-------|--------------------------------------------|-------|
| `test_smoke.py`                     | 1     | Placeholder                                | `assert True` only |
| `test_cli.py`                       | 3     | init, provider list, db info, one fetch/query smoke | Subprocess + direct mix |
| `test_provider_registry.py`         | 1     | Registry listing + policy constants        | Basic |
| `test_storage_foundation.py`        | 5     | Schema, migrations, raw idempotency, FKs, scoping | Good core coverage |
| `test_phase2_providers.py`          | 12    | Endpoints, raw-before-normalize, 304/4xx/429, stats, games normalize | Strong for Phase 2 |
| `test_phase3_jobs.py`               | 5     | Job enqueue/state, discovery edges, bounded crawl with fake fetcher | Uses `JobRunner` + caps |
| `test_phase3_reports_exports.py`    | 3     | Reports (null outcome, scoping), exports, CLI smoke for jobs/reports | Partial CLI |
| **Total**                           | **29**|                                            | All offline |

No custom pytest markers are defined. The `anyio` marker is from a plugin. No `@pytest.mark.skip`, `xfail`, or live tests exist in the tree.

## Strengths

- Strict offline enforcement inside pytest (socket guard).
- Raw payload written before normalization is asserted in several places.
- Provider identity is always scoped; cross-provider username collision tests exist.
- Bounded crawl caps (`max_users`, `max_games`, `max_jobs`) are explicitly tested.
- 429 handling injects a sleeper instead of sleeping.
- Use of `tmp_path` and in-memory DBs for isolation.
- Fast (<0.5s) full suite; no real-time sleeps in default tests.
- Good exercise of normalization for both providers using real-ish fixture shapes.
- Idempotency and "raw exists before normalizer" ordering verified.

## Issues

### 1. Vestigial Smoke Test

`tests/test_smoke.py` contains only:
```python
def test_smoke():
    assert True
```
It adds no confidence. Named "smoke" tests live inside other files (`test_cli_fetch_user_query_user_and_query_raw_smoke`, `test_cli_smoke_for_jobs_reports_and_exports`).

### 2. Large CLI Surface Is Barely Tested

`src/chess_crawl/cli.py` (446 statements) has only **57%** coverage. The following are **not exercised** by any test:

- `crawl opponents` command + `_cmd_crawl_opponents` + date parsing for crawl bounds
- `jobs list`, `jobs show`, `jobs resume` (except `jobs status` in one smoke)
- `fetch stats`, `fetch archives`, `fetch games` (only `fetch user` via monkeypatch)
- `query game`
- `report opponents`, `report games-by-month`
- `export * --provider FILTER`
- Several error return codes (2 for bad --limit, 4 for blocked, 5 for errors)
- `_parse_month`, `_parse_date`, `_parse_date_or_month` error paths
- `_connect_for_read` / `_connect_for_write` error branches in some paths
- Full `main()` / `run()` argument handling for most subcommands

The existing "smoke" tests only hit a narrow happy-path slice using either subprocess wrappers or direct `cli.run` + `capsys`.

### 3. JobRunner and Complex Execution Paths Under-tested (49% coverage)

`src/chess_crawl/jobs/runner.py` has extensive untested or lightly-tested logic:
- `_fetch_chesscom_bounded_months` + cursor state update
- `_fetch_user_games` limit/remaining budget interaction for real Lichess
- `_fetch_game_by_id`, batch jobs, unknown kinds
- Error paths that insert into `errors` table inside `_execute`
- Real (non-fake) `fetch_*` paths through the runner
- Resume/unblock combinations with actual crawl_run_id filtering

Job tests rely on a narrow `fake_fetcher` and small synthetic graphs. The real monthly chunking, param cursor persistence, and mixed job kinds are not driven end-to-end.

### 4. Resource Leaks and Noisy Test Runs

Running under `--cov` produces many:
```
ResourceWarning: unclosed database in <sqlite3.Connection object ...>
```

Examples appear from:
- `test_cli.py` smoke (direct `cli.run`)
- `test_phase2_providers.py` 304 test
- `test_phase3_reports_exports.py` CLI smoke

Not all `connect(...)` calls are wrapped in `with` or explicitly closed. `initialize_database` + `connect` pairs in CLI paths and some tests leak. This will scale poorly and hides real problems.

### 5. Network Guard Is Incomplete and Test-Only

`tests/conftest.py`:
```python
@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    ... monkeypatch socket.socket.connect
```

- Only active under pytest.
- Subprocess CLI tests (`run_cli`) and plain `python -m chess_crawl` bypass it (and did hit real chess.com during audit).
- Does not prevent higher-level I/O libraries in all cases.
- Real fetches in the tool are intended, but this makes "safe smoke" of fetch paths impossible without additional mocking.

### 6. Duplication and Poor Test Maintainability

- `_seed_game` (or near-identical variants) duplicated in `test_phase3_jobs.py` and `test_phase3_reports_exports.py`.
- Connection + initialize boilerplate repeated across many tests.
- No shared test utilities or `conftest` fixtures beyond `fixtures_dir` and the guard.
- Phase-based file names (`test_phase2_*`, `test_phase3_*`) will become misleading.

### 7. Dead Fixtures and Thin Data Coverage

Unused fixtures:
- `tests/fixtures/chesscom/archives.json`
- `tests/fixtures/lichess/game.json`

Limited variety:
- Few draws, null outcomes, different time controls/variants.
- No malformed payload tests for parsers.
- No tests exercising `parser_version` re-normalization (a key design point).
- `chesscom_outcome` / `lichess_outcome` / time control parsing branches in `normalize/codes.py` and `games.py` have low coverage (58-73%).

### 8. Inconsistent Test Techniques

- Mix of:
  - `subprocess.run` with manual `PYTHONPATH` hack (`run_cli`)
  - Direct `from chess_crawl import cli; cli.run(...)` + `monkeypatch` + `capsys`
- Assertions often on printed strings rather than observable state (fragile).
- Endpoint construction is unit-tested, but deeper client behavior is only covered indirectly.
- No `@pytest.mark.parametrize` for symmetric provider cases.

### 9. Divergence from `proj/plan.md`

The plan described a more modular test layout:
- `test_providers_chesscom.py`, `test_providers_lichess.py`
- `test_storage.py`, `test_normalize.py`
- `tests/live/test_live_smoke.py` with `@pytest.mark.live`
- Suggestion of `respx` / `pytest-httpx` + optional `hypothesis`

Current reality uses coarse phase buckets, direct `MockTransport`, no live marker, no property testing, and no separate normalize module tests. Streaming NDJSON edge cases are covered only at high level.

### 10. Other Gaps

- `config.py`: `from_env()` and custom `user_agent` / contact paths barely touched.
- `storage/repository.py` (66%): many upsert helpers and edge paths uncovered.
- `providers/chesscom/client.py` and `lichess/client.py` have internal retry/UA/token logic with partial coverage.
- `export/writers.py` and reports have some uncovered branches for empty results / provider filters.
- `__main__.py` and direct module execution paths untested.
- No test of schema evolution beyond v1 init idempotency.
- No verification that all 16 tables listed in `CANONICAL_TABLES` stay in sync with `schema.sql`.
- `initialize_database` (CLI path) vs `initialize` (test path) not cross-checked thoroughly.

## Coverage Highlights (from `pytest --cov`)

- Overall: 72%
- `cli.py`: 57%
- `jobs/runner.py`: 49%
- `normalize/codes.py`: 58%
- `providers/chesscom/client.py`: 61%
- `storage/repository.py`: 66%
- Well-covered: migrations (100%), storage foundation, basic provider registry, most Phase 2 ingest paths.

## Recommendations (Prioritized)

1. **Immediate hygiene**
   - Delete or replace `test_smoke.py`.
   - Wrap every `connect(...)` in tests with `with` (or add a `conn` fixture that closes).
   - Silence or eliminate ResourceWarnings.

2. **Expand CLI exercising**
   - Add consistent tests (prefer direct calls or `CliRunner` if switched to typer/click later) for **every** top-level command and major error path.
   - Cover crawl, all fetch subcommands, jobs resume/list/show, query game, filtered exports.

3. **Strengthen job + crawl testing**
   - Drive more paths through real `JobRunner` (not only fake_fetcher).
   - Add tests for monthly cursor advancement, budget exhaustion mid-crawl, error classification, and resume after partial failure.

4. **Reduce duplication**
   - Extract `_seed_game`, common seeding, and DB setup into `conftest.py` or `tests/support.py`.

5. **Improve fixtures and unit coverage**
   - Use or remove dead fixtures.
   - Add focused tests for `normalize/codes.py`, parsers, time control parsing, outcome mappers.
   - Add negative cases (bad JSON, missing fields, unknown variants).

6. **Network isolation**
   - Consider a stronger guard (env var + transport injection) or document that only pytest runs are safe.
   - Make subprocess-based CLI tests also safe (e.g. by forcing a test config that uses mocks).

7. **Future-proof structure**
   - Rename or reorganize phase-named tests.
   - Implement the planned live smoke marker (even if always skipped by default).
   - Add a coverage gate (e.g. fail if core modules drop below 80%).

8. **Test quality**
   - Prefer state assertions over exact stdout string matching where possible.
   - Use parametrization for provider-symmetric behavior.
   - Add a minimal test of `Config.from_env` and user-agent construction.

## Commands Used During Audit

```bash
pwd && git status --short --branch
python -m pytest --collect-only
python -m pytest -q --tb=short
python -m pytest --cov=chess_crawl --cov-report=term-missing --cov-branch -q
python -m pytest tests/test_cli.py::... -q --tb=short
# manual source inspection + fixture usage grep
```

## Files Inspected

**Tests:**
- All 7 modules under `tests/`
- `tests/conftest.py`
- All files under `tests/fixtures/`

**Source (selected for coverage gaps and centrality):**
- `src/chess_crawl/cli.py`
- `src/chess_crawl/jobs/{runner.py,store.py,discovery.py,models.py}`
- `src/chess_crawl/ingest.py`
- `src/chess_crawl/{config.py,providers/{base,http,registry}.py,providers/*/client.py}`
- `src/chess_crawl/normalize/{codes,users,games}.py`
- `src/chess_crawl/storage/{db,migrations,raw,repository}.py`
- `src/chess_crawl/reports/queries.py`
- `src/chess_crawl/export/writers.py`
- `src/chess_crawl/__init__.py`, `__main__.py`

**Docs:**
- `AGENTS.md`, `proj/plan.md`, phase-1/2/3 reports, `README.md`

## Conclusion

The testing system successfully protects the most important architectural properties (raw-first, provider-neutral, bounded, offline) and runs in well under a second. It is **not yet sufficient** to confidently support changes to the CLI, full crawl workflows, or complex job resumption. Addressing the gaps above (especially CLI + runner coverage + leaks + duplication) would make the suite a much stronger safety net.

No code changes were made during this audit; this report is the deliverable.