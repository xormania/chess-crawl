"""Exact game attribution and remaining capacity for bounded acquisition runs."""

from __future__ import annotations

import json
from dataclasses import dataclass

from chess_crawl.storage.db import Connection, atomic, operation_lock


def run_game_ids(conn: Connection, crawl_run_id: int) -> set[int]:
    return {
        int(row["game_id"])
        for row in conn.execute("SELECT game_id FROM run_games WHERE crawl_run_id = %s", (crawl_run_id,))
    }


def run_has_game(conn: Connection, crawl_run_id: int, game_id: int) -> bool:
    return conn.execute("SELECT 1 FROM run_games WHERE crawl_run_id=%s AND game_id=%s", (crawl_run_id, game_id)).fetchone() is not None


def payload_game_ids(conn: Connection, raw_payload_id: int) -> set[int]:
    return {
        int(row["entity_id"])
        for row in conn.execute(
            "SELECT entity_id FROM source_records WHERE entity_type = 'game' AND raw_payload_id = %s",
            (raw_payload_id,),
        )
    }


@dataclass(frozen=True)
class RunGameBounds:
    remaining: int | None
    since: int | None
    until: int | None
    created_since_ms: int | None = None
    created_until_ms: int | None = None

    def includes(self, ended_at: int | None, *, created_ms: int | None = None) -> bool:
        if self.created_since_ms is not None or self.created_until_ms is not None:
            if type(created_ms) is not int:
                return False
            if ((self.created_since_ms is not None and created_ms < self.created_since_ms)
                    or (self.created_until_ms is not None and created_ms >= self.created_until_ms)):
                return False
        if self.since is None and self.until is None:
            return True
        if ended_at is None:
            return False
        return (self.since is None or ended_at >= self.since) and (self.until is None or ended_at < self.until)


def run_game_bounds(
    conn: Connection,
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
          FROM crawl_runs WHERE id = %s
        """,
        (crawl_run_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Crawl run not found: {crawl_run_id}")
    if row["provider"] != provider:
        raise ValueError("A crawl run cannot acquire games from a different provider")
    params = json.loads(row["params_json"])
    collection = params.get("collection_mode", "bounded") != "bounded"
    configured = None if collection else params.get("max_games")
    remaining = requested
    if configured is not None:
        available = max(0, int(configured) - int(row["acquired"]))
        remaining = available if requested is None else min(requested, available)
    return RunGameBounds(
        remaining=remaining,
        since=None if collection else (int(params["since"]) if params.get("since") is not None else None),
        until=None if collection else (int(params["until"]) if params.get("until") is not None else None),
        created_since_ms=(int(params["since_ms"]) if params.get("since_ms") is not None else
                          int(params["since"]) * 1000 if params.get("since") is not None else None)
        if collection and provider == "lichess" else None,
        created_until_ms=(int(params["until_ms"]) if params.get("until_ms") is not None else
                          int(params["until"]) * 1000 if params.get("until") is not None else None)
        if collection and provider == "lichess" else None,
    )


@atomic
def associate_run_game(conn: Connection, crawl_run_id: int, game_id: int) -> bool:
    """Enforce provider, time window and capacity at the attribution write boundary."""
    operation_lock(conn, "run-game-budget", crawl_run_id)
    game = conn.execute(
        """SELECT g.provider,g.ended_at,v.source_metadata FROM games g
             LEFT JOIN game_versions v ON v.id=g.current_version_id WHERE g.id=%s""", (game_id,),
    ).fetchone()
    if game is None:
        raise ValueError(f"Game not found: {game_id}")
    existing = conn.execute(
        "SELECT 1 FROM run_games WHERE crawl_run_id = %s AND game_id = %s", (crawl_run_id, game_id),
    ).fetchone()
    if existing is not None:
        return False
    bounds = run_game_bounds(conn, crawl_run_id, provider=game["provider"], requested=None)
    metadata = game["source_metadata"]
    created_ms = metadata.get("createdAt") if isinstance(metadata, dict) else None
    if not bounds.includes(game["ended_at"], created_ms=created_ms):
        raise ValueError("The game does not have an end time within the crawl run's date window")
    if bounds.remaining == 0:
        raise ValueError("The crawl run's game limit has been reached")
    cursor = conn.execute(
        "INSERT INTO run_games(crawl_run_id, game_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (crawl_run_id, game_id),
    )
    return cursor.rowcount == 1
