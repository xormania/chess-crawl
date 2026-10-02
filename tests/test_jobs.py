from __future__ import annotations

import sqlite3
from collections.abc import Mapping

import pytest

from support import seed_game
from chess_crawl.ingest import IngestResult
import chess_crawl.jobs.runner as runner_module
from chess_crawl.jobs.models import DiscoveryJob, JobKind, JobState
from chess_crawl.jobs.discovery import (
    CrawlBounds,
    create_opponent_crawl,
)
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.storage.discovery import opponents_of_user, record_discovery_edges
from chess_crawl.storage.db import transaction
from chess_crawl.jobs import state
from chess_crawl.storage.repository import upsert_provider_user


def test_job_enqueue_dedup_and_terminal_reenqueue(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    first = state.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="SameName")
    duplicate = state.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="samename")

    assert first.inserted is True
    assert duplicate.inserted is False
    assert duplicate.job_id == first.job_id

    state.mark_done(conn, first.job_id, reason="ok")
    after_done = state.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="samename")

    assert after_done.inserted is True
    assert after_done.job_id != first.job_id


def test_job_state_transitions_and_stale_resume(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    job_id = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="SameName").job_id
    claimed = state.claim_next_job(conn)

    assert claimed is not None
    assert claimed.id == job_id
    assert claimed.state == "in_progress"
    assert claimed.attempts == 1

    resumed = state.resume_stale_in_progress(conn)
    job = state.get_job(conn, job_id)

    assert resumed == 1
    assert job is not None
    assert job.state == "pending"

    claimed = state.claim_next_job(conn)
    assert claimed is not None
    assert claimed.id is not None
    state.mark_blocked(conn, claimed.id, reason="429")
    blocked = state.get_job(conn, claimed.id)
    assert blocked is not None
    assert blocked.state == "blocked"
    assert state.unblock_jobs(conn) == 1
    pending = state.get_job(conn, claimed.id)
    assert pending is not None
    assert pending.state == "pending"


def test_crawl_creation_rolls_back_run_and_root_job_together(
    initialized_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = initialized_conn
    original_enqueue = state.enqueue_job

    def fail_after_enqueue(*args, **kwargs):
        original_enqueue(*args, **kwargs)
        raise RuntimeError("injected enqueue failure")

    monkeypatch.setattr(state, "enqueue_job", fail_after_enqueue)
    with pytest.raises(RuntimeError, match="injected enqueue failure"):
        create_opponent_crawl(
            conn, provider="lichess", username="A", since=1704067200, until=1704153600,
            bounds=CrawlBounds(max_depth=1, max_users=10, max_games=10, max_jobs=10),
        )

    assert conn.execute("SELECT COUNT(*) FROM crawl_runs").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM discovery_jobs").fetchone()[0] == 0
    assert conn.in_transaction is False


def test_run_refresh_tracks_job_state_and_preserves_cancellation(
    initialized_conn: sqlite3.Connection,
) -> None:
    conn = initialized_conn
    run_id, job_id = create_opponent_crawl(
        conn, provider="lichess", username="A", since=1704067200, until=1704153600,
        bounds=CrawlBounds(max_depth=1, max_users=10, max_games=10, max_jobs=10),
    )
    transitions: list[tuple[JobState, str, bool]] = [
        ("blocked", "paused", False),
        ("error", "failed", True),
        ("pending", "running", False),
        ("done", "done", True),
    ]
    for job_state, run_status, terminal in transitions:
        state.mark_job(conn, job_id, job_state, now=123)
        state.refresh_run_status(conn, run_id)
        job = state.get_job(conn, job_id)
        run = conn.execute("SELECT status, finished_at FROM crawl_runs WHERE id = ?", (run_id,)).fetchone()
        assert job is not None
        assert job.done_at == (123 if terminal else None)
        assert run["status"] == run_status
        assert (run["finished_at"] is not None) is terminal

    state.update_crawl_run(conn, run_id, status="cancelled", finished=True)
    state.refresh_crawl_runs(conn)
    assert state.crawl_runs(conn)[0]["status"] == "cancelled"


@pytest.mark.parametrize("scoped", [True, False], ids=["explicit-run", "global-scheduling"])
def test_cancelled_crawl_excludes_all_live_jobs_from_resume_and_execution(
    initialized_conn: sqlite3.Connection, scoped: bool,
) -> None:
    conn = initialized_conn
    run_id = state.create_crawl_run(conn, provider="lichess", seed_spec="cancelled fixture", params={})
    job_ids = []
    live_states: tuple[JobState, ...] = ("pending", "blocked", "in_progress")
    for job_state in live_states:
        job_id = state.enqueue_job(
            conn, provider="lichess", kind="fetch_user_games", target=job_state,
            params={"limit": 1}, crawl_run_id=run_id,
        ).job_id
        state.mark_job(conn, job_id, job_state, reason="preserve cancelled work")
        job_ids.append(job_id)
    state.update_crawl_run(conn, run_id, status="cancelled", finished=True)
    jobs_before = [state.get_job(conn, job_id) for job_id in job_ids]
    run_before = dict(state.crawl_runs(conn)[0])
    fetches: list[str] = []

    result = JobRunner(conn, game_fetcher=_graph_fetcher({}, fetches)).run(
        crawl_run_id=run_id if scoped else None,
        resume_stale=True,
        unblock=True,
    )

    assert result == runner_module.RunnerResult()
    assert fetches == []
    assert [state.get_job(conn, job_id) for job_id in job_ids] == jobs_before
    assert dict(state.crawl_runs(conn)[0]) == run_before


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


def _graph_fetcher(graph: Mapping[str, list[str]], calls: list[str]):
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

    return fake_fetcher


def test_bounded_fake_graph_crawl_depth_and_duplicate_dedupe(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    graph = {"a": ["b", "c"], "b": [], "c": ["d"], "d": []}
    calls: list[str] = []

    fake_fetcher = _graph_fetcher(graph, calls)

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


@pytest.mark.parametrize(
    ("bounds", "expected"),
    [
        pytest.param(
            CrawlBounds(max_depth=2, max_users=2, max_games=10, max_jobs=20),
            {"crawl_users": 2}, id="user-cap",
        ),
        pytest.param(
            CrawlBounds(max_depth=2, max_users=10, max_games=10, max_jobs=1),
            {"total_jobs": 1}, id="job-cap",
        ),
        pytest.param(
            CrawlBounds(max_depth=2, max_users=10, max_games=1, max_jobs=20),
            {"games": 1, "edges": 1}, id="game-cap",
        ),
    ],
)
def test_crawl_enforces_caps(
    initialized_conn: sqlite3.Connection, bounds: CrawlBounds, expected: dict[str, int],
) -> None:
    conn = initialized_conn
    fetcher = _graph_fetcher({"a": ["b", "c"], "b": ["d"], "c": ["e"]}, [])
    run_id, _ = create_opponent_crawl(
        conn,
        provider="chess.com",
        username="A",
        since=1704067200,
        until=1704153600,
        bounds=bounds,
    )
    JobRunner(conn, game_fetcher=fetcher).run(crawl_run_id=run_id)
    actual = {
        "crawl_users": state.crawl_user_count(conn, run_id),
        "total_jobs": state.total_jobs_for_run(conn, run_id),
        "games": int(conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]),
        "edges": int(conn.execute("SELECT COUNT(*) FROM discovery_edges").fetchone()[0]),
    }
    assert {key: actual[key] for key in expected} == expected


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
    job_id = state.enqueue_job(
        conn,
        provider="chess.com",
        kind="fetch_user_games",
        target="SameName",
        params={"since": 1704067200, "until": 1709251200, "max_games": 100},
    ).job_id

    result = JobRunner(conn).run(max_jobs=1)
    job = state.get_job(conn, job_id)

    assert result.done == 1
    assert calls == [("SameName", 2024, 1), ("SameName", 2024, 2)]
    assert job is not None
    assert state.load_params(job.params_json)["cursor_index"] == 2


def test_runner_resume_retries_failed_month_before_advancing_checkpoint(
    initialized_conn: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = initialized_conn
    calls: list[tuple[int, int]] = []
    statuses = iter([200, 429, 200])

    def fake_month(conn, username, year, month, **kwargs):
        calls.append((year, month))
        return IngestResult("chess.com", "monthly_archive", next(statuses), None, (), "fixture")

    monkeypatch.setattr(runner_module, "fetch_chesscom_month", fake_month)
    job_id = state.enqueue_job(
        conn,
        provider="chess.com",
        kind="fetch_user_games",
        target="SameName",
        params={"since": 1704067200, "until": 1709251200, "max_games": 100},
    ).job_id
    result = JobRunner(conn).run(max_jobs=1)
    blocked = state.get_job(conn, job_id)

    assert result.blocked == 1
    assert blocked is not None
    assert state.load_params(blocked.params_json)["cursor_index"] == 1

    result = JobRunner(conn).run(max_jobs=1, unblock=True)
    completed = state.get_job(conn, job_id)
    assert result.done == 1
    assert completed is not None
    assert completed.state == "done"
    assert state.load_params(completed.params_json)["cursor_index"] == 2
    assert calls == [(2024, 1), (2024, 2), (2024, 2)]


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
    state.enqueue_job(conn, provider="lichess", kind="fetch_game_by_id", target="abc123")

    result = JobRunner(conn).run(max_jobs=1)

    assert result.done == 1
    assert calls == ["abc123"]


@pytest.mark.parametrize(
    ("provider", "kind", "target", "supported_provider"),
    [
        ("chess.com", "fetch_game_by_id", "1000000001", "lichess"),
        ("lichess", "fetch_user_stats", "SameName", "chess.com"),
    ],
)
def test_unsupported_provider_jobs_are_not_schedulable(
    initialized_conn: sqlite3.Connection, provider: str, kind: JobKind, target: str, supported_provider: str,
) -> None:
    with pytest.raises(ValueError, match=f"supported only for {supported_provider}"):
        state.enqueue_job(initialized_conn, provider=provider, kind=kind, target=target)


def test_runner_reports_legacy_chesscom_fetch_game_by_id_as_error(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    with transaction(conn):
        conn.execute(
            """
            INSERT INTO discovery_jobs(provider, kind, target, params_json, state, priority, depth, attempts, dedup_key, enqueued_at)
            VALUES ('chess.com', 'fetch_game_by_id', '1000000001', '{}', 'pending', 100, 0, 0, 'legacy-chesscom-game', 123)
            """
        )

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
        return DiscoveryJob(id=None, provider="lichess", kind="fetch_user_profile", target="SameName")

    monkeypatch.setattr(runner_module.state, "claim_next_job", fake_claim_next_job)

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
    job_id = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="SameName").job_id

    result = JobRunner(conn).run(max_jobs=1)
    job = state.get_job(conn, job_id)

    assert result.errors == 1
    assert job is not None
    assert job.state == "error"
    row = conn.execute("SELECT error_kind, message FROM errors").fetchone()
    assert row["error_kind"] == "other"
    assert row["message"] == "handler exploded"
