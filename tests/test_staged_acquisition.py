"""Captured source units and local processing remain independent across workers."""
from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.jobs import acquisition, state
from chess_crawl.jobs.discovery import CrawlBounds, create_opponent_crawl
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.storage.db import connection, require_row
from chess_crawl.storage.discovery import discovery_edge_count


SINCE = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp())
UNTIL = int(datetime(2024, 3, 1, tzinfo=UTC).timestamp())
CONFIG = Config(chesscom_delay_s=0, lichess_delay_s=0, max_retries=0)


def _monthly(fixtures_dir, key: str) -> dict:
    body = json.loads((fixtures_dir / "chesscom/archive_2024_01.json").read_bytes())
    game = body["games"][0]
    game["uuid"] = key
    game["url"] = f"https://www.chess.com/game/live/{key}"
    return body


def _run(conn, *, cap: int = 1):
    params = {"since": SINCE, "until": UNTIL, "max_games": cap}
    return state.create_crawl_run_with_root_job(
        conn, provider="chess.com", seed_spec="bounded fixture", params=params,
        root_kind="fetch_user_games", root_target="samename",
    )


def _job(conn, job_id):
    job = state.get_job(conn, job_id)
    assert job is not None
    return job


def _count(conn, table: str) -> int:
    assert table in {"games", "run_games", "fetch_logs", "discovery_jobs"}
    return int(require_row(conn.execute(f"SELECT COUNT(*) FROM {table}"))[0])


def test_bounded_capture_releases_provider_without_parsing_and_waits_for_exact_cap(
    database_url, fixtures_dir,
) -> None:
    requested = []
    def provider(request):
        requested.append(request.url.path)
        return httpx.Response(200, json=_monthly(fixtures_dir, "one"))

    with connection(database_url, mode="rw") as acquire, connection(database_url, mode="rw") as process:
        run, parent = _run(acquire)
        fetch = JobRunner(acquire, stage="acquisition", config=CONFIG, transport=httpx.MockTransport(provider))
        assert fetch.run(max_jobs=1).claimed == 1
        assert _count(acquire, "games") == 0
        assert _job(acquire, parent).state == "pending"
        assert fetch.run(max_jobs=1).claimed == 0, "awaiting processing must not spin or fetch the next month"
        other = state.enqueue_job(acquire, provider="chess.com", kind="fetch_user_profile", target="other").job_id
        owned = state.claim_next_job(acquire, worker_id="other-acquisition", job_id=other, stage="acquisition")
        assert owned is not None
        # Another same-provider acquisition can own the provider permit while
        # normalization runs on a disjoint session.
        try:
            assert JobRunner(process, stage="processing").run(max_jobs=1).done == 1
        finally:
            state.mark_done(acquire, other)
            state.release_job_ownership(acquire)
        assert _count(acquire, "run_games") == 1
        assert fetch.run(max_jobs=1).done == 1
        assert requested == ["/pub/player/samename/games/2024/01"]
        snapshot = state.get_run(acquire, run)
        assert snapshot is not None and snapshot["status"] == "done"


def test_one_month_per_claim_and_completed_source_is_not_queued_again_after_crash(
    initialized_conn, fixtures_dir, monkeypatch,
) -> None:
    conn = initialized_conn
    _, parent = _run(conn, cap=3)
    requested = []
    def provider(request):
        requested.append(request.url.path)
        return httpx.Response(200, json=_monthly(fixtures_dir, request.url.path[-2:]))
    fetch = JobRunner(conn, stage="acquisition", config=CONFIG, transport=httpx.MockTransport(provider))
    checkpoint = state.checkpoint_job
    def crash(*args, **kwargs):
        raise SystemExit("captured before checkpoint")
    with monkeypatch.context() as interruption:
        interruption.setattr(acquisition.state, "checkpoint_job", crash)
        with pytest.raises(SystemExit, match="before checkpoint"):
            fetch.run(max_jobs=1)
    assert _count(conn, "fetch_logs") == 1
    assert JobRunner(conn, stage="processing").run(max_jobs=1).done == 1
    monkeypatch.setattr(acquisition.state, "checkpoint_job", checkpoint)
    assert fetch.run(max_jobs=1, resume_stale=True).claimed == 1
    assert len(requested) == 1, "resume must use original response even after child processing completed"
    assert _count(conn, "discovery_jobs") == 2, "completed processing must be reused on parent replay"
    assert state.load_params(_job(conn, parent).params_json)["cursor_index"] == 1
    assert fetch.run(max_jobs=1).claimed == 1
    assert len(requested) == 2
    assert state.load_params(_job(conn, parent).params_json)["cursor_index"] == 2
    assert JobRunner(conn, stage="processing").run(max_jobs=1).done == 1
    assert fetch.run(max_jobs=1).done == 1
    assert _count(conn, "run_games") == 2


def test_failed_processing_stops_later_month_acquisition(initialized_conn, fixtures_dir) -> None:
    conn = initialized_conn
    _, parent = _run(conn, cap=3)
    requested = []
    def provider(request):
        requested.append(request.url.path)
        return httpx.Response(200, json={"games": ["invalid game"]})
    runner = JobRunner(conn, config=CONFIG, transport=httpx.MockTransport(provider))
    result = runner.run()
    assert result.errors == 2  # Processing and its dependent acquisition both fail.
    assert len(requested) == 1
    assert "Processing job" in (_job(conn, parent).reason or "")
    assert _count(conn, "run_games") == 0


def test_crawl_frontier_is_processing_and_does_not_consume_discovery_job_cap(
    initialized_conn, fixtures_dir,
) -> None:
    conn = initialized_conn
    run, root = create_opponent_crawl(
        conn, provider="chess.com", username="samename", since=SINCE, until=UNTIL,
        bounds=CrawlBounds(max_depth=1, max_users=2, max_games=3, max_jobs=2),
    )
    requested = []
    def provider(request):
        requested.append(request.url.path)
        return httpx.Response(200, json=_monthly(fixtures_dir, request.url.path[-2:]))
    acquire = JobRunner(conn, stage="acquisition", config=CONFIG, transport=httpx.MockTransport(provider))
    process = JobRunner(conn, stage="processing")
    assert acquire.run(max_jobs=1).claimed == 1
    assert discovery_edge_count(conn, run) == 0
    assert process.run(max_jobs=1).done == 1
    assert acquire.run(max_jobs=1).claimed == 1
    assert process.run(max_jobs=1).done == 1
    assert acquire.run(max_jobs=1).done == 1
    assert discovery_edge_count(conn, run) == 0
    frontier = require_row(conn.execute("SELECT id FROM discovery_jobs WHERE kind='expand_opponents'"))[0]
    assert _job(conn, frontier).parent_job_id == root
    state.defer_provider(conn, "chess.com", not_before=10**12, reason="remote unavailable")
    assert process.run(max_jobs=1).done == 1
    assert discovery_edge_count(conn, run) > 0
    assert state.crawl_user_count(conn, run) == 2
    assert state.acquisition_job_count(conn, run) == 2
    assert state.total_jobs_for_run(conn, run) == 5
    assert len(requested) == 2


def test_standalone_monthly_selection_keeps_one_game_cap_across_processing(initialized_conn, fixtures_dir) -> None:
    conn = initialized_conn
    parent = state.enqueue_job(
        conn, provider="chess.com", kind="fetch_user_games", target="samename",
        params={"since": SINCE, "until": UNTIL, "max_games": 1},
    ).job_id
    requested = []
    def provider(request):
        requested.append(request.url.path)
        return httpx.Response(200, json=_monthly(fixtures_dir, request.url.path[-2:]))
    runner = JobRunner(conn, config=CONFIG, transport=httpx.MockTransport(provider))
    assert runner.run().errors == 0
    assert _job(conn, parent).state == "done"
    assert len(requested) == 1 and _count(conn, "games") == 1


def test_parallel_frontier_admission_cannot_exceed_discovery_capacity(database_url) -> None:
    import threading
    from support import seed_game
    from chess_crawl.jobs.discovery import enqueue_opponent_children
    from chess_crawl.storage.discovery import OpponentEdge

    with connection(database_url, mode="rw") as setup:
        run, root = create_opponent_crawl(
            setup, provider="lichess", username="a", since=SINCE, until=UNTIL,
            bounds=CrawlBounds(max_depth=1, max_users=2, max_games=3, max_jobs=2),
        )
        params = state.load_params(_job(setup, root).params_json)
        edges = []
        for opponent in ("b", "c"):
            game, _, user = seed_game(setup, provider="lichess", game_key=f"a-{opponent}", white="a", black=opponent)
            edges.append(OpponentEdge(user, opponent, game, 1))
    barrier = threading.Barrier(2)
    errors = []
    counts = []
    def admit(edge):
        try:
            with connection(database_url, mode="rw") as conn:
                barrier.wait(30)
                counts.append(enqueue_opponent_children(
                    conn, crawl_run_id=run, parent_job_id=root, provider="lichess",
                    params=params, next_depth=1, edges=[edge],
                ))
        except BaseException as error:
            errors.append(error)
    workers = [threading.Thread(target=admit, args=(edge,)) for edge in edges]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(45)
    assert all(not worker.is_alive() for worker in workers)
    assert errors == [] and sorted(counts) == [0, 1]
    with connection(database_url, mode="rw") as conn:
        assert state.acquisition_job_count(conn, run) == 2
        assert state.crawl_user_count(conn, run) == 2


@pytest.mark.parametrize("cancelled", [False, True], ids=["shutdown", "cancelled"])
def test_processing_interruption_preserves_partial_game_checkpoint(
    initialized_conn, fixtures_dir, monkeypatch, cancelled,
) -> None:
    from chess_crawl.normalize import games
    conn = initialized_conn
    run, parent = _run(conn, cap=3)
    body = _monthly(fixtures_dir, "one")
    body["games"].extend(_monthly(fixtures_dir, "two")["games"])
    assert JobRunner(conn, config=CONFIG, stage="acquisition", transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=body),
    )).run(max_jobs=1).claimed == 1
    stopping = False
    persist = games._normalize_game
    def stop_after_game(*args, **kwargs):
        nonlocal stopping
        identity = persist(*args, **kwargs)
        if cancelled:
            state.update_crawl_run(conn, run, status="cancelled", finished=True)
        else:
            stopping = True
        return identity
    with monkeypatch.context() as interruption:
        interruption.setattr(games, "_normalize_game", stop_after_game)
        assert JobRunner(conn, stage="processing", stop_requested=lambda: stopping).run(max_jobs=1).claimed == 1
    child = require_row(conn.execute("SELECT id FROM discovery_jobs WHERE parent_job_id=%s", (parent,)))[0]
    assert _job(conn, child).state == "pending"
    assert _count(conn, "run_games") == 1
    assert _count(conn, "fetch_logs") == 1
    assert require_row(conn.execute("SELECT COUNT(*) FROM normalization_items"))[0] == 1
    if cancelled:
        assert JobRunner(conn, stage="processing").run(max_jobs=1).claimed == 0
    else:
        assert JobRunner(conn, stage="processing").run(max_jobs=1).done == 1
        assert _count(conn, "run_games") == 2


def test_processing_retry_does_not_cool_down_unrelated_provider_work(initialized_conn) -> None:
    conn = initialized_conn
    local = state.enqueue_job(conn, provider="lichess", kind="normalize_payload", target="123").job_id
    state.finish_attempt(conn, local, "error", reason="temporary object storage failure", transient=True, now=100)
    assert _job(conn, local).state == "blocked"
    assert _job(conn, local).next_attempt_at is not None
    assert state.provider_ready_at(conn, "lichess") is None
    remote = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="remote").job_id
    selected = state.claim_next_job(conn, worker_id="acquisition", job_id=remote, stage="acquisition", now=100)
    assert selected is not None and selected.id == remote
    state.release_job_ownership(conn)
