"""Bounded evidence reads and coverage, without provider network access."""
from __future__ import annotations

from typing import Any
from decimal import Decimal

from chess_crawl.application.errors import NotFound
from chess_crawl.storage.db import Connection, require_row


def game_row(conn: Connection, game_id: int) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM games WHERE id=%s", (game_id,)).fetchone()
    if row is None:
        raise NotFound("Game not found", code="game_not_found")
    return dict(row)


def version_id(conn: Connection, game_id: int, requested: int | None) -> int:
    game = game_row(conn, game_id)
    selected = game["current_version_id"] if requested is None else requested
    if selected is None:
        raise NotFound("Game evidence has not been normalized", code="game_version_not_found")
    if conn.execute("SELECT 1 FROM game_versions WHERE game_id=%s AND id=%s", (game_id,selected)).fetchone() is None:
        raise NotFound("Game version not found", code="game_version_not_found")
    return int(selected)


def move_page(
    conn: Connection, game_id: int, *, requested: int | None, after: int, limit: int, mainline: bool | None,
) -> dict[str, Any]:
    selected = version_id(conn, game_id, requested)
    rows = [dict(row) for row in conn.execute(
        """SELECT * FROM game_move_nodes WHERE version_id=%s AND node_index>%s
           AND (%s::boolean IS NULL OR is_mainline=%s) ORDER BY node_index LIMIT %s""",
        (selected,after,mainline,mainline,limit+1),
    )]
    total = int(require_row(conn.execute(
        "SELECT COUNT(*) FROM game_move_nodes WHERE version_id=%s AND node_index>0 AND (%s::boolean IS NULL OR is_mainline=%s)",
        (selected,mainline,mainline),
    ))[0])
    indices = [row["node_index"] for row in rows[:limit]]
    clocks: dict[int, list[dict[str, Any]]] = {}
    timings: dict[int, list[dict[str, Any]]] = {}
    if indices:
        for clock in conn.execute(
            "SELECT * FROM game_clock_observations WHERE version_id=%s AND node_index=ANY(%s) ORDER BY id",
            (selected,indices),
        ):
            clocks.setdefault(clock["node_index"], []).append({key:str(value) if isinstance(value,Decimal) else value for key,value in clock.items()})
        for timing in conn.execute(
            "SELECT * FROM game_derived_timings WHERE version_id=%s AND node_index=ANY(%s) ORDER BY node_index,method_version",
            (selected,indices),
        ):
            timings.setdefault(timing["node_index"], []).append({key:str(value) if isinstance(value,Decimal) else value for key,value in timing.items()})
    for row in rows[:limit]:
        row["clock_observations"] = clocks.get(row["node_index"], [])
        row["derived_timings"] = timings.get(row["node_index"], [])
    return {"version_id":selected, "items":rows[:limit], "total":total,
            "next_cursor":rows[limit-1]["node_index"] if len(rows)>limit else None}


def versions(conn: Connection, game_id: int, *, after: int, limit: int) -> dict[str, Any]:
    game_row(conn, game_id)
    rows = [dict(row) for row in conn.execute(
        """SELECT id,game_id,content_hash,parser_version,first_seen_at,variant,parse_status,
           parse_issues,played_ply_count FROM game_versions WHERE game_id=%s AND id>%s ORDER BY id LIMIT %s""",
        (game_id,after,limit+1),
    )]
    return {"items":rows[:limit], "next_cursor":rows[limit-1]["id"] if len(rows)>limit else None}


def player_coverage(conn: Connection, user_id: int) -> dict[str, Any]:
    row = require_row(conn.execute(
        """SELECT COUNT(*) AS stored_games, COUNT(g.current_version_id) AS normalized_games,
           MIN(g.ended_at) AS first_game_at, MAX(g.ended_at) AS last_game_at,
           COUNT(*) FILTER(WHERE gv.parse_status='complete') AS complete_games,
           COUNT(*) FILTER(WHERE gv.parse_status IS NOT NULL AND gv.parse_status<>'complete') AS incomplete_games
           FROM games g LEFT JOIN game_versions gv ON gv.id=g.current_version_id
           WHERE EXISTS(SELECT 1 FROM game_participants gp WHERE gp.game_id=g.id AND gp.provider_user_id=%s)""",
        (user_id,),
    ))
    return {**dict(row), "complete_history":None, "coverage_note":"Stored game counts do not prove complete remote history"}


def games_for_player(conn: Connection, user_id: int, *, after: int, limit: int) -> dict[str, Any]:
    rows = [dict(row) for row in conn.execute(
        """SELECT g.* FROM games g WHERE g.id>%s AND EXISTS(
           SELECT 1 FROM game_participants gp WHERE gp.game_id=g.id AND gp.provider_user_id=%s)
           ORDER BY g.id LIMIT %s""", (after,user_id,limit+1),
    )]
    return {"items":rows[:limit], "next_cursor":rows[limit-1]["id"] if len(rows)>limit else None}


def upgrade_progress(conn: Connection, job_id: int, workspace_id: str) -> dict[str, Any]:
    from chess_crawl.storage.workspaces import require_job
    require_job(conn,job_id,workspace_id)
    row = conn.execute("SELECT * FROM data_upgrades WHERE job_id=%s", (job_id,)).fetchone()
    if row is None:
        job = require_row(conn.execute("SELECT kind,state FROM discovery_jobs WHERE id=%s", (job_id,)))
        if job["kind"] != "reprocess_archive":
            raise NotFound("Upgrade not found",code="upgrade_not_found")
        return {"job_id":job_id,"state":job["state"],"progress":None}
    return {"job_id":job_id,"state":row["state"],"progress":dict(row)}


def rating_history_page(
    conn: Connection, user_id: int, workspace_id: str, *, snapshot_id: int | None,
    performance: str | None, after: int, limit: int,
) -> dict[str,Any]:
    if snapshot_id is None:
        snapshot = conn.execute(
            """SELECT s.id FROM user_resource_snapshots s JOIN user_resource_observations o ON o.snapshot_id=s.id
               WHERE s.provider_user_id=%s AND s.resource_key='rating-history'
                 AND s.owner_scope IN ('public',%s)
               ORDER BY o.captured_at DESC,o.fetch_log_id DESC NULLS LAST,o.id DESC LIMIT 1""",
            (user_id,workspace_id),
        ).fetchone()
        if snapshot is None:
            raise NotFound("Rating history has not been collected",code="rating_history_not_found")
        snapshot_id = int(snapshot["id"])
    elif conn.execute(
        """SELECT 1 FROM user_resource_snapshots WHERE id=%s AND provider_user_id=%s
           AND resource_key='rating-history' AND owner_scope IN ('public',%s)""",
        (snapshot_id,user_id,workspace_id),
    ).fetchone() is None:
        raise NotFound("Rating history snapshot not found",code="rating_history_not_found")
    rows = [dict(row) for row in conn.execute(
        """WITH points AS (SELECT p.*,row_number() OVER(ORDER BY perf_index,point_index) AS ordinal
           FROM rating_history_points p WHERE snapshot_id=%s AND (%s::text IS NULL OR performance=%s))
           SELECT * FROM points WHERE ordinal>%s ORDER BY ordinal LIMIT %s""",
        (snapshot_id,performance,performance,after,limit+1),
    )]
    return {"snapshot_id":snapshot_id,"items":rows[:limit],
            "next_cursor":rows[limit-1]["ordinal"] if len(rows)>limit else None}
