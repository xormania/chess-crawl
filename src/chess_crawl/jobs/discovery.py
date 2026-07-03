"""Bounded opponent-discovery strategy over normalized local data."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from chess_crawl.jobs import store
from chess_crawl.storage.repository import upsert_provider_user


@dataclass(frozen=True)
class CrawlBounds:
    max_depth: int
    max_users: int
    max_games: int
    max_jobs: int


@dataclass(frozen=True)
class OpponentEdge:
    opponent_user_id: int
    opponent_username: str
    via_game_id: int
    game_count: int


def create_opponent_crawl(
    conn: sqlite3.Connection,
    *,
    provider: str,
    username: str,
    since: int,
    until: int,
    bounds: CrawlBounds,
) -> tuple[int, int]:
    if bounds.max_depth < 0:
        raise ValueError("--depth must be >= 0")
    if min(bounds.max_users, bounds.max_games, bounds.max_jobs) <= 0:
        raise ValueError("--max-users, --max-games, and --max-jobs must be > 0")
    if since >= until:
        raise ValueError("--since must be earlier than --until")

    normalized = username.strip().lower()
    params = {
        "strategy": "opponents",
        "seed": normalized,
        "since": since,
        "until": until,
        "max_depth": bounds.max_depth,
        "max_users": bounds.max_users,
        "max_games": bounds.max_games,
        "max_jobs": bounds.max_jobs,
    }
    seed_spec = f"{provider}/{normalized} depth={bounds.max_depth}"
    run_id = store.create_crawl_run(conn, provider=provider, seed_spec=seed_spec, params=params)
    root = store.enqueue_job(
        conn,
        provider=provider,
        kind="crawl_opponents",
        target=normalized,
        params=params,
        crawl_run_id=run_id,
        depth=0,
        priority=10,
    )
    return run_id, root.job_id


def ensure_local_user(conn: sqlite3.Connection, *, provider: str, username: str, now: int | None = None) -> int:
    return upsert_provider_user(
        conn,
        provider=provider,
        username=username,
        display_username=username,
        now=now or int(time.time()),
    )


def game_count_for_run(
    conn: sqlite3.Connection,
    *,
    crawl_run_id: int,
    provider: str,
    since: int | None = None,
    until: int | None = None,
) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(DISTINCT g.id)
              FROM games g
              JOIN game_participants gp ON gp.game_id = g.id
              JOIN discovery_jobs j
                ON j.crawl_run_id = ?
               AND j.provider = g.provider
               AND j.kind = 'crawl_opponents'
               AND lower(j.target) = gp.username_normalized
             WHERE g.provider = ?
               AND (? IS NULL OR g.ended_at IS NULL OR g.ended_at >= ?)
               AND (? IS NULL OR g.ended_at IS NULL OR g.ended_at < ?)
            """,
            (crawl_run_id, provider, since, since, until, until),
        ).fetchone()[0]
    )


def remaining_game_budget(
    conn: sqlite3.Connection,
    *,
    crawl_run_id: int | None,
    provider: str,
    params: Mapping[str, Any],
) -> int | None:
    max_games = _int_or_none(params.get("max_games"))
    if max_games is None:
        return None
    if crawl_run_id is None:
        return max_games
    current = game_count_for_run(
        conn,
        crawl_run_id=crawl_run_id,
        provider=provider,
        since=_int_or_none(params.get("since")),
        until=_int_or_none(params.get("until")),
    )
    return max(0, max_games - current)


def opponents_of_user(
    conn: sqlite3.Connection,
    *,
    provider: str,
    user_id: int,
    since: int | None = None,
    until: int | None = None,
) -> list[OpponentEdge]:
    rows = conn.execute(
        """
        SELECT gp_o.provider_user_id AS opponent_user_id,
               pu.username_normalized AS opponent_username,
               MIN(g.id) AS via_game_id,
               COUNT(DISTINCT g.id) AS game_count
          FROM games g
          JOIN game_participants gp_m
            ON gp_m.game_id = g.id AND gp_m.provider_user_id = ?
          JOIN game_participants gp_o
            ON gp_o.game_id = g.id AND gp_o.color <> gp_m.color
          JOIN provider_users pu ON pu.id = gp_o.provider_user_id
         WHERE g.provider = ?
           AND gp_o.provider_user_id IS NOT NULL
           AND gp_o.provider_user_id <> ?
           AND pu.provider = ?
           AND (? IS NULL OR g.ended_at IS NULL OR g.ended_at >= ?)
           AND (? IS NULL OR g.ended_at IS NULL OR g.ended_at < ?)
         GROUP BY gp_o.provider_user_id, pu.username_normalized
         ORDER BY game_count DESC, pu.username_normalized
        """,
        (user_id, provider, user_id, provider, since, since, until, until),
    ).fetchall()
    return [
        OpponentEdge(
            opponent_user_id=int(row["opponent_user_id"]),
            opponent_username=row["opponent_username"],
            via_game_id=int(row["via_game_id"]),
            game_count=int(row["game_count"]),
        )
        for row in rows
    ]


def record_discovery_edges(
    conn: sqlite3.Connection,
    *,
    crawl_run_id: int | None,
    provider: str,
    from_user_id: int,
    depth: int,
    edges: list[OpponentEdge],
) -> int:
    now = int(time.time())
    inserted_or_updated = 0
    for edge in edges:
        conn.execute(
            """
            INSERT INTO discovery_edges(
              crawl_run_id, provider, from_user_id, to_user_id, via_game_id,
              game_count, depth, edge_kind, first_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'opponent', ?)
            ON CONFLICT(provider, from_user_id, to_user_id) DO UPDATE SET
              crawl_run_id = COALESCE(discovery_edges.crawl_run_id, excluded.crawl_run_id),
              game_count = MAX(discovery_edges.game_count, excluded.game_count),
              depth = MIN(discovery_edges.depth, excluded.depth),
              via_game_id = COALESCE(discovery_edges.via_game_id, excluded.via_game_id)
            """,
            (
                crawl_run_id,
                provider,
                from_user_id,
                edge.opponent_user_id,
                edge.via_game_id,
                edge.game_count,
                depth,
                now,
            ),
        )
        inserted_or_updated += 1
    conn.commit()
    return inserted_or_updated


def enqueue_opponent_children(
    conn: sqlite3.Connection,
    *,
    crawl_run_id: int,
    parent_job_id: int,
    provider: str,
    params: Mapping[str, Any],
    next_depth: int,
    edges: list[OpponentEdge],
) -> int:
    max_depth = int(params["max_depth"])
    if next_depth > max_depth:
        return 0

    inserted = 0
    for edge in edges:
        known_depth = store.known_crawl_depth(
            conn,
            crawl_run_id=crawl_run_id,
            provider=provider,
            username=edge.opponent_username,
        )
        if known_depth is not None and known_depth <= next_depth:
            continue
        if store.crawl_user_count(conn, crawl_run_id) >= int(params["max_users"]):
            break
        if store.total_jobs_for_run(conn, crawl_run_id) >= int(params["max_jobs"]):
            break
        result = store.enqueue_job(
            conn,
            provider=provider,
            kind="crawl_opponents",
            target=edge.opponent_username,
            params=params,
            crawl_run_id=crawl_run_id,
            parent_job_id=parent_job_id,
            depth=next_depth,
            priority=10 + next_depth,
        )
        if result.inserted:
            inserted += 1
    return inserted


def run_counters(conn: sqlite3.Connection, crawl_run_id: int) -> dict[str, int]:
    counters = {
        "jobs_total": store.total_jobs_for_run(conn, crawl_run_id),
        "users_seen": store.crawl_user_count(conn, crawl_run_id),
        "edges": int(
            conn.execute(
                "SELECT COUNT(*) FROM discovery_edges WHERE crawl_run_id = ?",
                (crawl_run_id,),
            ).fetchone()[0]
        ),
    }
    for row in store.job_state_counts(conn, crawl_run_id=crawl_run_id):
        counters[f"jobs_{row['state']}"] = int(row["count"])
    return counters


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if not isinstance(value, str):
        return None
    return int(value)
