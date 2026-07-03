from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from typing import Any

import pytest

from conftest import seed_game
from chess_crawl.ingest import IngestResult
import chess_crawl.jobs.runner as runner_module
from chess_crawl.jobs.models import DiscoveryJob
from chess_crawl.jobs.discovery import (
    CrawlBounds,
    create_opponent_crawl,
    record_discovery_edges,
    opponents_of_user,
)
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.jobs import store
from chess_crawl.storage.repository import upsert_provider_user


def test_job_enqueue_dedup_and_terminal_reenqueue(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    first = store.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="SameName")
    duplicate = store.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="samename")

    assert first.inserted is True
    assert duplicate.inserted is False
    assert duplicate.job_id == first.job_id

    store.mark_done(conn, first.job_id, reason="ok")
    after_done = store.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="samename")

    assert after_done.inserted is True
    assert after_done.job_id != first.job_id


def test_job_state_transitions_and_stale_resume(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    job_id = store.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="SameName").job_id
    claimed = store.claim_next_job(conn)

    assert claimed is not None
    assert claimed.id == job_id
    assert claimed.state == "in_progress"
    assert claimed.attempts == 1

    resumed = store.resume_stale_in_progress(conn)
    job = store.get_job(conn, job_id)

    assert resumed == 1
    assert job is not None
    assert job.state == "pending"

    claimed = store.claim_next_job(conn)
    assert claimed is not None
    assert claimed.id is not None
    store.mark_blocked(conn, claimed.id, reason="429")
    blocked = store.get_job(conn, claimed.id)
    assert blocked is not None
    assert blocked.state == "blocked"
    assert store.unblock_jobs(conn) == 1
    pending = store.get_job(conn, claimed.id)
    assert pending is not None
    assert pending.state == "pending"


def test_discovery_edge_insertion_is_idempotent(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    run_id, _ = create_opponent_crawl(
        conn,
        provider="chess.com",
        username="A",
        since=1704067200,
        until=1704153600,
        bounds=CrawlBounds(max_depth=1, max_users=10, max_games=10, max_jobs=10),
    )
    seed_game(conn, provider="chess.com", game_key="a-b", white="A", black="B")
    from_user = upsert_provider_user(conn, provider="chess.com", username="A")
    edges = opponents_of_user(conn, provider="chess.com", user_id=from_user)

    record_discovery_edges(conn, crawl_run_id=run_id, provider="chess.com", from_user_id=from_user, depth=1, edges=edges)
    record_discovery_edges(conn, crawl_run_id=run_id, provider="chess.com", from_user_id=from_user, depth=1, edges=edges)

    row = conn.execute("SELECT game_count, depth FROM discovery_edges").fetchone()
    assert row["game_count"] == 1
    assert row["depth"] == 1


def test_bounded_fake_graph_crawl_depth_and_duplicate_dedupe(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    graph = {"a": ["b", "c"], "b": [], "c": ["d"], "d": []}
    calls: list[str] = []

    def fake_fetcher(
        inner: sqlite3.Connection,
        provider: str,
        username: str,
        params: Mapping[str, object],
        remaining: int | None,
    ) -> IngestResult:
        calls.append(username.lower())
        ids = []
        for index, opponent in enumerate(graph.get(username.lower(), ())):
            if remaining is not None and index >= remaining:
                break
            ids.append(
                seed_game(
                    inner,
                    provider=provider,
                    game_key=f"{username.lower()}-{opponent}",
                    white=username,
                    black=opponent,
                )[0]
            )
        return IngestResult(provider, "user_games_stream", 200, None, tuple(ids), f"fixture {len(ids)}")

    run_id, _ = create_opponent_crawl(
        conn,
        provider="lichess",
        username="A",
        since=1704067200,
        until=1704153600,
        bounds=CrawlBounds(max_depth=2, max_users=10, max_games=10, max_jobs=20),
    )
    result = JobRunner(conn, game_fetcher=fake_fetcher).run(crawl_run_id=run_id)

    assert result.errors == 0
    assert result.done == 4
    assert calls == ["a", "b", "c", "d"]
    edge_rows = conn.execute(
        """
        SELECT fu.username_normalized AS source, tu.username_normalized AS target, e.depth
          FROM discovery_edges e
          JOIN provider_users fu ON fu.id = e.from_user_id
          JOIN provider_users tu ON tu.id = e.to_user_id
         ORDER BY source, target
        """
    ).fetchall()
    assert [(row["source"], row["target"], row["depth"]) for row in edge_rows] == [
        ("a", "b", 1),
        ("a", "c", 1),
        ("c", "d", 2),
    ]

    rerun = JobRunner(conn, game_fetcher=fake_fetcher).run(crawl_run_id=run_id)
    assert rerun.claimed == 0


def test_crawl_enforces_max_users_max_jobs_and_max_games() -> None:
    def run_with_caps(bounds: CrawlBounds) -> dict[str, int]:
        from chess_crawl.storage.db import connect
        from chess_crawl.storage.migrations import initialize

        conn = connect(":memory:")
        initialize(conn)
        graph = {"a": ["b", "c"], "b": ["d"], "c": ["e"]}

        def fake_fetcher(
            inner: sqlite3.Connection,
            provider: str,
            username: str,
            params: Mapping[str, object],
            remaining: int | None,
        ) -> IngestResult:
            ids = []
            for index, opponent in enumerate(graph.get(username.lower(), ())):
                if remaining is not None and index >= remaining:
                    break
                ids.append(
                    seed_game(
                        inner,
                        provider=provider,
                        game_key=f"{username.lower()}-{opponent}",
                        white=username,
                        black=opponent,
                    )[0]
                )
            return IngestResult(provider, "user_games_stream", 200, None, tuple(ids), "fixture")

        run_id, _ = create_opponent_crawl(
            conn,
            provider="chess.com",
            username="A",
            since=1704067200,
            until=1704153600,
            bounds=bounds,
        )
        try:
            JobRunner(conn, game_fetcher=fake_fetcher).run(crawl_run_id=run_id)
            return {
                "crawl_users": store.crawl_user_count(conn, 1),
                "total_jobs": store.total_jobs_for_run(conn, 1),
                "games": int(conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]),
                "edges": int(conn.execute("SELECT COUNT(*) FROM discovery_edges").fetchone()[0]),
            }
        finally:
            conn.close()

    user_capped = run_with_caps(CrawlBounds(max_depth=2, max_users=2, max_games=10, max_jobs=20))
    assert user_capped["crawl_users"] == 2

    job_capped = run_with_caps(CrawlBounds(max_depth=2, max_users=10, max_games=10, max_jobs=1))
    assert job_capped["total_jobs"] == 1

    game_capped = run_with_caps(CrawlBounds(max_depth=2, max_users=10, max_games=1, max_jobs=20))
    assert game_capped["games"] == 1
    assert game_capped["edges"] == 1


def test_runner_fetch_user_games_chesscom_advances_month_cursor(
    initialized_conn: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = initialized_conn
    calls: list[tuple[str, int, int]] = []

    def fake_month(conn, username, year, month, **kwargs):
        calls.append((username, year, month))
        return IngestResult("chess.com", "monthly_archive", 200, 10 + month, (month,), "ok")

    monkeypatch.setattr(runner_module, "fetch_chesscom_month", fake_month)
    job_id = store.enqueue_job(
        conn,
        provider="chess.com",
        kind="fetch_user_games",
        target="SameName",
        params={"since": 1704067200, "until": 1709251200, "max_games": 100},
    ).job_id

    result = JobRunner(conn).run(max_jobs=1)
    job = store.get_job(conn, job_id)

    assert result.done == 1
    assert calls == [("SameName", 2024, 1), ("SameName", 2024, 2)]
    assert job is not None
    assert store.load_params(job.params_json)["cursor_index"] == 2


def test_runner_fetch_game_by_id_uses_lichess_service(
    initialized_conn: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = initialized_conn
    calls: list[str] = []

    def fake_game(conn, game_id, **kwargs):
        calls.append(game_id)
        return IngestResult("lichess", "game", 200, 7, (70,), "game ok")

    monkeypatch.setattr(runner_module, "fetch_lichess_game", fake_game)
    store.enqueue_job(conn, provider="lichess", kind="fetch_game_by_id", target="abc123")

    result = JobRunner(conn).run(max_jobs=1)

    assert result.done == 1
    assert calls == ["abc123"]


@pytest.mark.parametrize("kind", ["fetch_games_by_ids", "import_export_dump"])
def test_unimplemented_job_kinds_are_not_schedulable(
    initialized_conn: sqlite3.Connection,
    kind: str,
) -> None:
    unsupported_kind: Any = kind

    with pytest.raises(ValueError, match="unsupported job kind"):
        store.enqueue_job(initialized_conn, provider="lichess", kind=unsupported_kind, target="abc123")


def test_chesscom_fetch_game_by_id_jobs_are_not_schedulable(initialized_conn: sqlite3.Connection) -> None:
    with pytest.raises(ValueError, match="supported only for lichess"):
        store.enqueue_job(initialized_conn, provider="chess.com", kind="fetch_game_by_id", target="1000000001")


def test_runner_reports_legacy_chesscom_fetch_game_by_id_as_error(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    conn.execute(
        """
        INSERT INTO discovery_jobs(provider, kind, target, params_json, state, priority, depth, attempts, dedup_key, enqueued_at)
        VALUES ('chess.com', 'fetch_game_by_id', '1000000001', '{}', 'pending', 100, 0, 0, 'legacy-chesscom-game', 123)
        """
    )
    conn.commit()

    result = JobRunner(conn).run(max_jobs=1)
    job = conn.execute("SELECT state, reason FROM discovery_jobs WHERE dedup_key = 'legacy-chesscom-game'").fetchone()

    assert result.errors == 1
    assert job["state"] == "error"
    assert "supported only for lichess" in job["reason"]


def test_runner_rejects_claimed_job_without_persisted_id(
    initialized_conn: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_claim_next_job(*args: object, **kwargs: object) -> DiscoveryJob:
        return DiscoveryJob(id=None, provider="lichess", kind="resume", target="local")

    monkeypatch.setattr(runner_module.store, "claim_next_job", fake_claim_next_job)

    with pytest.raises(RuntimeError, match="missing a persisted id"):
        JobRunner(initialized_conn).run(max_jobs=1)


def test_runner_records_unexpected_handler_errors(
    initialized_conn: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = initialized_conn

    def boom(*args, **kwargs):
        raise RuntimeError("handler exploded")

    monkeypatch.setattr(runner_module, "fetch_user_profile", boom)
    job_id = store.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="SameName").job_id

    result = JobRunner(conn).run(max_jobs=1)
    job = store.get_job(conn, job_id)

    assert result.errors == 1
    assert job is not None
    assert job.state == "error"
    row = conn.execute("SELECT error_kind, message FROM errors").fetchone()
    assert row["error_kind"] == "other"
    assert row["message"] == "handler exploded"
