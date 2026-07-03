from __future__ import annotations

import sqlite3
from collections.abc import Mapping

from chess_crawl.ingest import IngestResult
from chess_crawl.jobs.discovery import (
    CrawlBounds,
    create_opponent_crawl,
    record_discovery_edges,
    opponents_of_user,
)
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.jobs import store
from chess_crawl.storage.db import connect
from chess_crawl.storage.migrations import initialize
from chess_crawl.storage.repository import (
    get_or_create_time_control,
    get_or_create_variant,
    upsert_game,
    upsert_game_participant,
    upsert_provider_user,
)


def _conn() -> sqlite3.Connection:
    conn = connect(":memory:")
    initialize(conn)
    return conn


def _seed_game(
    conn: sqlite3.Connection,
    *,
    provider: str,
    game_key: str,
    white: str,
    black: str,
    outcome: str | None = "white_win",
    ended_at: int = 1704067200,
) -> int:
    white_id = upsert_provider_user(conn, provider=provider, username=white)
    black_id = upsert_provider_user(conn, provider=provider, username=black)
    variant_id = get_or_create_variant(
        conn,
        provider=provider,
        provider_native_name="standard",
        canonical_name="standard",
    )
    time_control_id = get_or_create_time_control(
        conn,
        kind="clock",
        initial_seconds=300,
        increment_seconds=0,
        days=None,
        time_class="blitz",
        raw_label="300",
    )
    game_id = upsert_game(
        conn,
        provider=provider,
        provider_game_id=game_key,
        canonical_url=f"https://example.test/{provider}/{game_key}",
        content_hash=f"sha256:{provider}:{game_key}",
        variant_id=variant_id,
        time_control_id=time_control_id,
        rated=True,
        outcome=outcome,
        ended_at=ended_at,
    )
    upsert_game_participant(conn, game_id=game_id, color="white", provider_user_id=white_id, username_normalized=white.lower())
    upsert_game_participant(conn, game_id=game_id, color="black", provider_user_id=black_id, username_normalized=black.lower())
    conn.commit()
    return game_id


def test_job_enqueue_dedup_and_terminal_reenqueue() -> None:
    conn = _conn()
    first = store.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="SameName")
    duplicate = store.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="samename")

    assert first.inserted is True
    assert duplicate.inserted is False
    assert duplicate.job_id == first.job_id

    store.mark_done(conn, first.job_id, reason="ok")
    after_done = store.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="samename")

    assert after_done.inserted is True
    assert after_done.job_id != first.job_id


def test_job_state_transitions_and_stale_resume() -> None:
    conn = _conn()
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
    store.mark_blocked(conn, claimed.id, reason="429")
    assert store.get_job(conn, claimed.id).state == "blocked"
    assert store.unblock_jobs(conn) == 1
    assert store.get_job(conn, claimed.id).state == "pending"


def test_discovery_edge_insertion_is_idempotent() -> None:
    conn = _conn()
    run_id, _ = create_opponent_crawl(
        conn,
        provider="chess.com",
        username="A",
        since=1704067200,
        until=1704153600,
        bounds=CrawlBounds(max_depth=1, max_users=10, max_games=10, max_jobs=10),
    )
    _seed_game(conn, provider="chess.com", game_key="a-b", white="A", black="B")
    from_user = upsert_provider_user(conn, provider="chess.com", username="A")
    edges = opponents_of_user(conn, provider="chess.com", user_id=from_user)

    record_discovery_edges(conn, crawl_run_id=run_id, provider="chess.com", from_user_id=from_user, depth=1, edges=edges)
    record_discovery_edges(conn, crawl_run_id=run_id, provider="chess.com", from_user_id=from_user, depth=1, edges=edges)

    row = conn.execute("SELECT game_count, depth FROM discovery_edges").fetchone()
    assert row["game_count"] == 1
    assert row["depth"] == 1


def test_bounded_fake_graph_crawl_depth_and_duplicate_dedupe() -> None:
    conn = _conn()
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
                _seed_game(
                    inner,
                    provider=provider,
                    game_key=f"{username.lower()}-{opponent}",
                    white=username,
                    black=opponent,
                )
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
    def run_with_caps(bounds: CrawlBounds) -> sqlite3.Connection:
        conn = _conn()
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
                    _seed_game(
                        inner,
                        provider=provider,
                        game_key=f"{username.lower()}-{opponent}",
                        white=username,
                        black=opponent,
                    )
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
        JobRunner(conn, game_fetcher=fake_fetcher).run(crawl_run_id=run_id)
        return conn

    user_capped = run_with_caps(CrawlBounds(max_depth=2, max_users=2, max_games=10, max_jobs=20))
    assert store.crawl_user_count(user_capped, 1) == 2

    job_capped = run_with_caps(CrawlBounds(max_depth=2, max_users=10, max_games=10, max_jobs=1))
    assert store.total_jobs_for_run(job_capped, 1) == 1

    game_capped = run_with_caps(CrawlBounds(max_depth=2, max_users=10, max_games=1, max_jobs=20))
    assert game_capped.execute("SELECT COUNT(*) FROM games").fetchone()[0] == 1
    assert game_capped.execute("SELECT COUNT(*) FROM discovery_edges").fetchone()[0] == 1
