"""Bounded operational catalogs and workspace-owned export reads."""
from __future__ import annotations

from collections.abc import Generator
from typing import Any, cast

from chess_crawl.storage.db import Connection, Row, lock_key, require_row
from chess_crawl.storage.workspaces import require_run


def raw_page(conn: Connection, *, owner_scope: str, provider: str | None, after: int, limit: int) -> dict[str, Any]:
    rows = list(conn.execute(
        """SELECT id,provider,endpoint_type,canonical_source_key,response_status,
           content_type,body_hash,body_bytes,normalization_status,fetched_at,owner_scope
           FROM raw_payloads WHERE owner_scope IN ('public',%s)
           AND (%s::text IS NULL OR provider=%s) AND id>%s ORDER BY id LIMIT %s""",
        (owner_scope,provider,provider,after,limit+1),
    ))
    return _page(rows, limit)


def jobs_page(conn: Connection, *, workspace_id: str, run_id: int | None, after: int, limit: int) -> dict[str, Any]:
    if run_id is not None:
        require_run(conn,run_id,workspace_id)
    rows = list(conn.execute(
        """SELECT id,crawl_run_id,parent_job_id,provider,kind,target,state,priority,depth,
           attempts,retry_count,next_attempt_at,enqueued_at,started_at,done_at,reason,revision
           FROM discovery_jobs WHERE workspace_id=%s AND id>%s
           AND (%s::bigint IS NULL OR crawl_run_id=%s) ORDER BY id LIMIT %s""",
        (workspace_id,after,run_id,run_id,limit+1),
    ))
    return _page(rows,limit)


def runs_page(conn: Connection, *, workspace_id: str, after: int, limit: int) -> dict[str, Any]:
    rows = list(conn.execute(
        """SELECT id,provider,status,seed_spec,started_at,finished_at,revision
           FROM crawl_runs WHERE workspace_id=%s AND id>%s ORDER BY id LIMIT %s""",
        (workspace_id,after,limit+1),
    ))
    return _page(rows,limit)


def jobs_status(conn: Connection, *, workspace_id: str, run_id: int | None) -> dict[str, Any]:
    if run_id is not None:
        require_run(conn,run_id,workspace_id)
    states = list(conn.execute(
        """SELECT state,COUNT(*) AS count FROM discovery_jobs WHERE workspace_id=%s
           AND (%s::bigint IS NULL OR crawl_run_id=%s) GROUP BY state ORDER BY state""",
        (workspace_id,run_id,run_id),
    ))
    kinds = list(conn.execute(
        """SELECT kind,state,depth,COUNT(*) AS count FROM discovery_jobs WHERE workspace_id=%s
           AND (%s::bigint IS NULL OR crawl_run_id=%s) GROUP BY kind,state,depth ORDER BY depth,kind,state""",
        (workspace_id,run_id,run_id),
    ))
    return {"states":[dict(row) for row in states],"by_kind":[dict(row) for row in kinds]}


def months_page(conn: Connection, *, provider: str, after: str, limit: int) -> dict[str, Any]:
    rows = list(conn.execute(
        """WITH months AS (
           SELECT CASE WHEN ended_at>=-62135596800 AND ended_at<253402300800
                  THEN to_char(to_timestamp(ended_at) AT TIME ZONE 'UTC','YYYY-MM')
                  ELSE 'unknown' END AS month,
                  COUNT(*) AS games,
                  COUNT(*) FILTER(WHERE outcome='white_win') AS white_wins,
                  COUNT(*) FILTER(WHERE outcome='black_win') AS black_wins,
                  COUNT(*) FILTER(WHERE outcome='draw') AS draws,
                  COUNT(*) FILTER(WHERE outcome IS NULL) AS unfinished,
                  COUNT(*) FILTER(WHERE outcome IS NULL) AS no_result,
                  COUNT(*) FILTER(WHERE is_live=1) AS in_progress
           FROM games WHERE provider=%s GROUP BY month)
           SELECT * FROM months WHERE month>%s ORDER BY month LIMIT %s""",
        (provider,after,limit+1),
    ))
    return {"items":[dict(row) for row in rows[:limit]],
            "next_cursor":rows[limit-1]["month"] if len(rows)>limit else None}


def check_export_schema(conn: Connection) -> None:
    """Fail before response headers when the archive has no usable schema."""
    require_row(conn.execute("SELECT COUNT(*) FROM games WHERE false"))


def admit_export_snapshot(conn: Connection, *, workspace_id: str, slots: int, timeout_ms: int) -> bool:
    """Bound preparation across API replicas; locks end with this snapshot."""
    require_row(conn.execute("SELECT set_config('statement_timeout',%s,true)", (str(timeout_ms),)))
    for slot in range(slots):
        key = lock_key("export-preparation", f"{workspace_id}:{slot}")
        if bool(require_row(conn.execute("SELECT pg_try_advisory_xact_lock(%s)", (key,)))[0]):
            return True
    return False


def set_export_timeout(conn: Connection, timeout_ms: int) -> None:
    require_row(conn.execute("SELECT set_config('statement_timeout',%s,true)", (str(timeout_ms),)))


def iter_owned_graph(conn: Connection, *, workspace_id: str, provider: str | None) -> Generator[Row,None,None]:
    """Membership and traversal metadata come only from the caller's runs.

    Global edges contain the first run's private provenance and combined depth.
    Recover metrics from owned run games/jobs rather than disclosing those fields.
    """
    with conn.cursor() as cursor:
        stream = cast(Generator[Row,None,None],cursor.stream(
            """SELECT e.provider,re.crawl_run_id,
               fu.username_normalized AS from_username,tu.username_normalized AS to_username,
               e.from_user_id,e.to_user_id,facts.via_game_id,facts.game_count,
               traversal.depth,e.edge_kind
               FROM run_edges re JOIN crawl_runs r ON r.id=re.crawl_run_id AND r.workspace_id=%s
               JOIN discovery_edges e ON e.id=re.discovery_edge_id
               JOIN provider_users fu ON fu.id=e.from_user_id AND fu.provider=e.provider
               JOIN provider_users tu ON tu.id=e.to_user_id AND tu.provider=e.provider
               LEFT JOIN LATERAL (
                 SELECT MIN(g.id) AS via_game_id,COUNT(DISTINCT g.id) AS game_count
                 FROM run_games rg JOIN games g ON g.id=rg.game_id AND g.provider=e.provider
                 JOIN game_participants fm ON fm.game_id=g.id AND fm.provider_user_id=e.from_user_id
                 JOIN game_participants tm ON tm.game_id=g.id AND tm.provider_user_id=e.to_user_id
                   AND tm.color<>fm.color WHERE rg.crawl_run_id=re.crawl_run_id
               ) facts ON true
               LEFT JOIN LATERAL (
                 SELECT MIN(j.depth)+1 AS depth FROM discovery_jobs j
                 WHERE j.crawl_run_id=re.crawl_run_id AND j.workspace_id=%s
                   AND j.kind='crawl_opponents' AND lower(j.target)=fu.username_normalized
               ) traversal ON true
               WHERE (%s::text IS NULL OR e.provider=%s)
               ORDER BY e.provider COLLATE "C",re.crawl_run_id,e.id""",
            (workspace_id,workspace_id,provider,provider),
        ))
        try:
            yield from stream
        finally:
            stream.close()


def _page(rows: list[Row], limit: int) -> dict[str, Any]:
    return {"items":[dict(row) for row in rows[:limit]],
            "next_cursor":int(rows[limit-1]["id"]) if len(rows)>limit else None}
