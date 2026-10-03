"""Provider-neutral discovery graph queries and persistence."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass

from chess_crawl.storage.db import atomic


@dataclass(frozen=True)
class OpponentEdge:
    opponent_user_id: int
    opponent_username: str
    via_game_id: int
    game_count: int



def game_count_for_run(
    conn: sqlite3.Connection,
    *,
    crawl_run_id: int,
    provider: str,
    since: int | None = None,
    until: int | None = None,
) -> int:
    # A run's capacity is based on every attributed game, even if a provider
    # returns dates outside the requested window. Filters cannot replenish it.
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM run_games WHERE crawl_run_id = ?",
            (crawl_run_id,),
        ).fetchone()[0]
    )


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



@atomic
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
    return inserted_or_updated



def discovery_edge_count(conn: sqlite3.Connection, crawl_run_id: int) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM discovery_edges WHERE crawl_run_id = ?",
            (crawl_run_id,),
        ).fetchone()[0]
    )
