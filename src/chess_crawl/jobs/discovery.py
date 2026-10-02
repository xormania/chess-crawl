"""Bounded opponent-discovery strategy over normalized local data."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from chess_crawl.jobs import state
from chess_crawl.storage.db import atomic
from chess_crawl.storage.discovery import OpponentEdge, game_count_for_run
from chess_crawl.storage.repository import upsert_provider_user


@dataclass(frozen=True)
class CrawlBounds:
    max_depth: int
    max_users: int
    max_games: int
    max_jobs: int


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
    return state.create_crawl_run_with_root_job(
        conn,
        provider=provider,
        seed_spec=seed_spec,
        params=params,
        root_kind="crawl_opponents",
        root_target=normalized,
        priority=10,
    )


def ensure_local_user(conn: sqlite3.Connection, *, provider: str, username: str, now: int | None = None) -> int:
    return upsert_provider_user(
        conn,
        provider=provider,
        username=username,
        display_username=username,
        now=int(time.time()) if now is None else now,
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


@atomic
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
        known_depth = state.known_crawl_depth(
            conn,
            crawl_run_id=crawl_run_id,
            provider=provider,
            username=edge.opponent_username,
        )
        if known_depth is not None and known_depth <= next_depth:
            continue
        if state.crawl_user_count(conn, crawl_run_id) >= int(params["max_users"]):
            break
        if state.total_jobs_for_run(conn, crawl_run_id) >= int(params["max_jobs"]):
            break
        result = state.enqueue_job(
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
