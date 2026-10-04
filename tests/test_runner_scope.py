from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.ingest import IngestResult
from chess_crawl.jobs import state
from chess_crawl.jobs.discovery import CrawlBounds, create_opponent_crawl
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.jobs.settings import WorkerSettings
from chess_crawl.storage.acquisition import associate_run_game
from chess_crawl.storage.discovery import opponents_of_user
from support import Clock, seed_game


@pytest.mark.parametrize("provider", ["chess.com", "lichess"])
def test_runner_reuses_provider_pacing_across_jobs_and_months(initialized_conn, fixtures_dir: Path, provider) -> None:
    conn = initialized_conn
    clock = Clock(100.0)
    calls: list[tuple[str, float]] = []
    closed: list[bool] = []
    state.enqueue_job(conn, provider=provider, kind="fetch_user_profile", target="SameName", priority=10)
    if provider == "chess.com":
        state.enqueue_job(conn, provider=provider, kind="fetch_user_stats", target="SameName", priority=20)
    state.enqueue_job(
        conn, provider=provider, kind="fetch_user_games", target="SameName", priority=30,
        params={"since": 1704067200, "until": 1709251200, "max_games": 10},
    )
    if provider == "lichess":
        state.enqueue_job(conn, provider=provider, kind="fetch_game_by_id", target="lichgame1", priority=40)

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls.append((path, clock.now))
        if provider == "chess.com":
            if path.endswith("/stats"):
                body = (fixtures_dir / "chesscom/stats.json").read_bytes()
            elif "/games/" in path:
                body = b'{"games":[]}'
            else:
                body = (fixtures_dir / "chesscom/player.json").read_bytes()
        else:
            fixture = "lichess/user.json" if "/api/user/" in path else "lichess/games.ndjson"
            body = (fixtures_dir / fixture).read_bytes()
        return httpx.Response(200, content=body)

    class Transport(httpx.MockTransport):
        def close(self):
            closed.append(True)

    result = JobRunner(
        conn, config=Config(chesscom_delay_s=3, lichess_delay_s=3, max_retries=0),
        transport=Transport(handler), sleeper=clock.sleep, clock=clock,
    ).run()

    assert result.done == 3
    assert result.errors == 0
    expected = (
        ["/pub/player/samename", "/pub/player/samename/stats", "/pub/player/samename/games/2024/01", "/pub/player/samename/games/2024/02"]
        if provider == "chess.com" else
        ["/api/user/samename", "/api/games/user/samename", "/game/export/lichgame1"]
    )
    assert calls == [(path, 100 + index * 3) for index, path in enumerate(expected)]
    assert clock.sleeps == [3] * (len(expected) - 1)
    assert closed == [True]


@pytest.mark.parametrize(("provider", "delay"), [("chess.com", 7), ("lichess", 60)])
def test_exhausted_429_delays_the_next_job(initialized_conn, fixtures_dir, provider, delay) -> None:
    clock = Clock(100.0)
    times = []
    for target in ("First", "Second"):
        state.enqueue_job(initialized_conn, provider=provider, kind="fetch_user_profile", target=target)

    def handler(request):
        times.append(clock.now)
        if len(times) == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        fixture = "chesscom/player.json" if provider == "chess.com" else "lichess/user.json"
        return httpx.Response(200, content=(fixtures_dir / fixture).read_bytes())

    config = Config(chesscom_delay_s=3, lichess_delay_s=3, max_retries=0)
    settings = WorkerSettings(job_max_retries=0, job_retry_base_s=1, job_retry_max_s=1)

    def run():
        return JobRunner(
            initialized_conn, config=config, settings=settings,
            transport=httpx.MockTransport(handler), sleeper=clock.sleep, clock=clock,
        ).run()

    first = run()
    assert first.errors == first.claimed == 1
    assert first.done == 0
    assert times == [100]
    assert state.provider_ready_at(initialized_conn, provider) == 100 + delay
    # Fresh runners also honor the persisted floor, independent of a session's
    # in-memory pacing state; no claim or HTTP attempt occurs before the deadline.
    clock.now = 100 + delay - 1
    assert run().claimed == 0
    assert times == [100]
    clock.now = 100 + delay
    assert run().done == 1
    assert times == [100, 100 + delay]
    assert clock.sleeps == []


@pytest.mark.parametrize("has_selection", [True, False], ids=["selected-game", "empty-response"])
def test_opponent_frontier_uses_only_games_selected_by_this_run(initialized_conn, has_selection) -> None:
    conn = initialized_conn
    selected, alice, _ = seed_game(
        conn, provider="lichess", game_key="selected", white="Alice", black="Chosen",
    )
    seed_game(conn, provider="lichess", game_key="unselected-same-opponent", white="Alice", black="Chosen")
    excluded, _, _ = seed_game(
        conn, provider="lichess", game_key="unselected-opponent", white="Alice", black="Unselected",
    )
    other_run = state.create_crawl_run(conn, provider="lichess", seed_spec="other", params={"max_games": 10})
    associate_run_game(conn, other_run, excluded)
    run_id, _ = create_opponent_crawl(
        conn, provider="lichess", username="Alice", since=1704067200, until=1704153600,
        bounds=CrawlBounds(max_depth=1, max_users=10, max_games=10, max_jobs=10),
    )

    def acquire(*args):
        return IngestResult("lichess", "user_games_stream", 200, None, (selected,) if has_selection else (), "cached")

    result = JobRunner(conn, game_fetcher=acquire).run(crawl_run_id=run_id, max_jobs=1)
    assert result.done == 1
    targets = [row[0] for row in conn.execute(
        "SELECT target FROM discovery_jobs WHERE crawl_run_id = %s ORDER BY id", (run_id,),
    )]
    assert targets == (["alice", "chosen"] if has_selection else ["alice"])
    edges = opponents_of_user(conn, provider="lichess", user_id=alice, crawl_run_id=run_id)
    assert [(edge.opponent_username, edge.via_game_id, edge.game_count) for edge in edges] == (
        [("chosen", selected, 1)] if has_selection else []
    )
    recorded = conn.execute(
        "SELECT discovery_edge_id FROM run_edges WHERE crawl_run_id = %s", (run_id,),
    ).fetchall()
    assert len(recorded) == (1 if has_selection else 0)
    # Archive-wide reports can still inspect history explicitly without a run filter.
    assert {edge.opponent_username for edge in opponents_of_user(conn, provider="lichess", user_id=alice)} == {
        "chosen", "unselected",
    }
