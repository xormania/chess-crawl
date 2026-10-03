"""Exact game attribution and remaining capacity for bounded acquisition runs."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from chess_crawl.storage.db import atomic


def run_game_ids(conn: sqlite3.Connection, crawl_run_id: int) -> set[int]:
    return {
        int(row["game_id"])
        for row in conn.execute("SELECT game_id FROM run_games WHERE crawl_run_id = ?", (crawl_run_id,))
    }


def payload_game_ids(conn: sqlite3.Connection, raw_payload_id: int) -> set[int]:
    return {
        int(row["entity_id"])
        for row in conn.execute(
            "SELECT entity_id FROM source_records WHERE entity_type = 'game' AND raw_payload_id = ?",
            (raw_payload_id,),
        )
    }


@dataclass(frozen=True)
class RunGameBounds:
    remaining: int | None
    since: int | None
    until: int | None

    def includes(self, ended_at: int | None) -> bool:
        if self.since is None and self.until is None:
            return True
        if ended_at is None:
            return False
        return (self.since is None or ended_at >= self.since) and (self.until is None or ended_at < self.until)


def run_game_bounds(
    conn: sqlite3.Connection,
    crawl_run_id: int,
    *,
    provider: str,
    requested: int | None,
) -> RunGameBounds:
    """Read the persisted selection window and clamp capacity under the caller's transaction."""
    row = conn.execute(
        """
        SELECT provider, params_json,
               (SELECT COUNT(*) FROM run_games WHERE crawl_run_id = crawl_runs.id) AS acquired
          FROM crawl_runs WHERE id = ?
        """,
        (crawl_run_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Crawl run not found: {crawl_run_id}")
    if row["provider"] != provider:
        raise ValueError("A crawl run cannot acquire games from a different provider")
    params = json.loads(row["params_json"])
    configured = params.get("max_games")
    remaining = requested
    if configured is not None:
        available = max(0, int(configured) - int(row["acquired"]))
        remaining = available if requested is None else min(requested, available)
    return RunGameBounds(
        remaining=remaining,
        since=int(params["since"]) if params.get("since") is not None else None,
        until=int(params["until"]) if params.get("until") is not None else None,
    )


@atomic
def associate_run_game(conn: sqlite3.Connection, crawl_run_id: int, game_id: int) -> bool:
    """Enforce provider, time window and capacity at the attribution write boundary."""
    game = conn.execute("SELECT provider, ended_at FROM games WHERE id = ?", (game_id,)).fetchone()
    if game is None:
        raise ValueError(f"Game not found: {game_id}")
    existing = conn.execute(
        "SELECT 1 FROM run_games WHERE crawl_run_id = ? AND game_id = ?", (crawl_run_id, game_id),
    ).fetchone()
    if existing is not None:
        return False
    bounds = run_game_bounds(conn, crawl_run_id, provider=game["provider"], requested=None)
    if not bounds.includes(game["ended_at"]):
        raise ValueError("The game does not have an end time within the crawl run's date window")
    if bounds.remaining == 0:
        raise ValueError("The crawl run's game limit has been reached")
    cursor = conn.execute(
        "INSERT INTO run_games(crawl_run_id, game_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
        (crawl_run_id, game_id),
    )
    return cursor.rowcount == 1
