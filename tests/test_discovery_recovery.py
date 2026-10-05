"""Retain run-specific graph evidence across repeated crawls and interruptions."""

from __future__ import annotations

from chess_crawl.storage.db import Connection, connection, require_row, transaction

import json

import pytest

from support import seed_game
from chess_crawl.ingest import IngestResult
from chess_crawl.jobs import runner as runner_module, state
from chess_crawl.jobs.discovery import CrawlBounds, create_opponent_crawl
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.storage import migrations
from chess_crawl.storage.discovery import discovery_edge_count


def create_run(conn: Connection, *, max_games: int = 10) -> tuple[int, int]:
    return create_opponent_crawl(
        conn, provider="lichess", username="a", since=1704067200, until=1704153600,
        bounds=CrawlBounds(max_depth=1, max_users=2, max_games=max_games, max_jobs=2),
    )


def graph_fetcher(calls: list[str]):
    def fetch(conn, provider, username, params, remaining):
        calls.append(username)
        ids = () if username != "a" else (
            seed_game(conn, provider=provider, game_key="a-b", white="a", black="b")[0],
        )
        return IngestResult(provider, "user_games_stream", 200, None, ids, "fixture")
    return fetch


def test_repeated_crawls_attribute_shared_edges_to_each_run(initialized_conn) -> None:
    conn = initialized_conn
    runs = []
    for _ in range(2):
        run, _ = create_run(conn)
        runs.append(run)
        assert JobRunner(conn, game_fetcher=graph_fetcher([])).run(crawl_run_id=run).done == 2
        snapshot = state.get_run(conn, run)
        assert snapshot is not None
        assert json.loads(snapshot["counters"])["edges"] == 1
    assert require_row(conn.execute("SELECT COUNT(*) FROM discovery_edges"))[0] == 1
    assert [discovery_edge_count(conn, run) for run in runs] == [1, 1]
    # The global graph retains its original discovery provenance.
    assert require_row(conn.execute("SELECT crawl_run_id FROM discovery_edges"))[0] == runs[0]


@pytest.mark.parametrize("crash_point", ["before_frontier", "after_frontier"])
def test_resume_discovers_retained_games_after_crash_fills_budget(initialized_conn, monkeypatch, crash_point) -> None:
    conn = initialized_conn
    run, root = create_run(conn, max_games=1)
    calls: list[str] = []
    runner = JobRunner(conn, game_fetcher=graph_fetcher(calls))
    def crash(*args, **kwargs):
        raise SystemExit("crash after acquisition")

    with monkeypatch.context() as interruption:
        if crash_point == "before_frontier":
            interruption.setattr(runner_module, "record_discovery_edges", crash)
        else:
            interruption.setattr(state, "finish_attempt", crash)
        with pytest.raises(SystemExit, match="crash after acquisition"):
            runner.run(crawl_run_id=run)
    interrupted = state.get_job(conn, root)
    assert interrupted is not None and interrupted.state == "in_progress"
    assert require_row(conn.execute("SELECT COUNT(*) FROM run_games WHERE crawl_run_id=%s", (run,)))[0] == 1

    result = runner.run(crawl_run_id=run, resume_stale=True)
    assert result.stale_resumed == 1
    assert discovery_edge_count(conn, run) == 1
    completed = state.get_job(conn, root)
    assert completed is not None and completed.state == "done"
    assert state.total_jobs_for_run(conn, run) == 2
    assert state.crawl_user_count(conn, run) == 2
    assert calls == ["a"], "a full budget must not issue another acquisition"


def test_edge_membership_migration_backfills_only_recorded_provenance(monkeypatch, uninitialized_database_url: str) -> None:
    available = migrations.migration_resources()
    with connection(uninitialized_database_url, mode="rwc") as conn:
        with monkeypatch.context() as old:
            old.setattr(migrations, "migration_resources", lambda: tuple(item for item in available if item[0] <= 4))
            old.setattr(migrations, "SCHEMA_VERSION", 4)
            migrations.initialize(conn)
        # Seed the legacy schema directly: today's scheduler requires columns
        # introduced after this migration's starting version.
        with transaction(conn):
            runs = [
                require_row(conn.execute(
                    """INSERT INTO crawl_runs(seed_spec, provider, params_json, status,
                                              counters, started_at, updated_at)
                       VALUES ('a', 'lichess', '{}', 'running', '{}', 123, 123)
                       RETURNING id""",
                ))[0]
                for _ in range(2)
            ]
        first, second = runs
        game, a, b = seed_game(conn, provider="lichess", game_key="a-b", white="a", black="b")
        other_game, _, c = seed_game(conn, provider="lichess", game_key="a-c", white="a", black="c")
        with transaction(conn):
            with conn.cursor() as cursor:
                cursor.executemany("INSERT INTO run_games VALUES (%s, %s)", [(first, game), (second, game)])
                cursor.executemany(
                    """INSERT INTO discovery_edges(crawl_run_id, provider, from_user_id, to_user_id,
                               via_game_id, game_count, depth, first_seen_at)
                       VALUES (%s, 'lichess', %s, %s, %s, 1, 1, 123)""",
                    [(first, a, b, game), (None, a, c, other_game)],
                )
        assert migrations.initialize(conn).applied == tuple(name for version, name, _ in available if version > 4)
        assert [discovery_edge_count(conn, run) for run in (first, second)] == [1, 0]
        assert require_row(conn.execute("SELECT COUNT(*) FROM discovery_edges"))[0] == 2
        assert require_row(conn.execute("SELECT COUNT(*) FROM run_edges"))[0] == 1
        assert migrations.initialize(conn).applied == ()
