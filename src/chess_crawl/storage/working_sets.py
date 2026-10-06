"""Immutable game-version selections and exactly versioned reusable outputs."""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from chess_crawl.application.errors import Conflict, NotFound, ValidationError
from chess_crawl.application.models import Limits
from chess_crawl.jobs.budget import QuotaExceeded
from chess_crawl.storage.db import Connection, atomic, operation_lock, require_row
from chess_crawl.storage.workspaces import submission_context, validate_workspace


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


@atomic
def create_working_set(
    conn: Connection, *, workspace_id: str, name: str, filters: dict[str, Any],
    settings: dict[str, Any], idempotency_key: str, max_members: int = 10000,
    submission_namespace: str = "api",
) -> dict[str, Any]:
    if type(max_members) is not int or max_members < 1:
        raise ValidationError("Working-set member limit must be a positive integer",code="invalid_working_set_limit")
    submission_context(conn, workspace_id)
    request_json = canonical({"name": name, "filters": filters, "settings": settings})
    # Scope the lock to the durable submission identity. Lock release follows commit.
    operation_lock(conn, "working-set", f"{workspace_id}:{submission_namespace}:{idempotency_key}")
    existing = conn.execute(
        "SELECT * FROM working_set_submissions WHERE workspace_id = %s AND submission_namespace=%s AND idempotency_key = %s",
        (workspace_id, submission_namespace, idempotency_key),
    ).fetchone()
    if existing:
        if existing["request_json"] != request_json:
            raise Conflict("Idempotency key already identifies a different working set", code="idempotency_conflict")
        return {**get_working_set(conn, int(existing["working_set_id"]), workspace_id), "replayed": True}
    player_id = None
    if filters.get("username") is not None:
        candidates = conn.execute(
            """SELECT pu.id,pu.username_normalized=%s AS current_name FROM provider_users pu
               WHERE pu.provider=%s AND (pu.username_normalized=%s OR EXISTS(
                 SELECT 1 FROM provider_user_aliases a WHERE a.provider_user_id=pu.id
                   AND a.username_normalized=%s))
               ORDER BY current_name DESC,pu.id LIMIT 2""",
            (filters["username"],filters["provider"],filters["username"],filters["username"]),
        ).fetchall()
        if len(candidates)>1 and not candidates[0]["current_name"]:
            raise ValidationError("Historical player alias is ambiguous",code="ambiguous_player_alias")
        if candidates:
            player_id = int(candidates[0]["id"])
    row = require_row(conn.execute(
        """INSERT INTO working_sets(workspace_id,name,filters,settings,input_signature,member_count,created_at)
           VALUES(%s,%s,%s::jsonb,%s::jsonb,'pending',0,%s) RETURNING id""",
        (workspace_id, name, canonical(filters), canonical(settings), int(time.time())),
    ))
    working_set_id = int(row["id"])
    # All identifiers are literal; filters and settings never enter SQL text.
    conn.execute(
        """INSERT INTO working_set_members(working_set_id, ordinal, game_version_id)
           SELECT %s, row_number() OVER(ORDER BY g.id), g.current_version_id FROM games g
           JOIN time_controls tc ON tc.id = g.time_control_id
           JOIN variants v ON v.id = g.variant_id
           WHERE g.current_version_id IS NOT NULL
             AND (%s::text IS NULL OR g.provider = %s)
             AND (%s::bigint IS NULL OR g.ended_at >= %s)
             AND (%s::bigint IS NULL OR g.ended_at < %s)
             AND (%s::text IS NULL OR tc.time_class = %s)
             AND (%s::text IS NULL OR v.canonical_name = %s)
             AND (%s::integer IS NULL OR g.rated = %s)
             AND (%s::text IS NULL OR EXISTS(
               SELECT 1 FROM game_participants gp WHERE gp.game_id = g.id
                 AND (gp.provider_user_id=%s
                   OR (gp.provider_user_id IS NULL AND gp.username_normalized=%s))))
           ORDER BY g.id LIMIT %s""",
        (working_set_id, filters.get("provider"), filters.get("provider"),
         filters.get("since"), filters.get("since"), filters.get("until"), filters.get("until"),
         filters.get("time_class"), filters.get("time_class"), filters.get("variant"), filters.get("variant"),
         None if filters.get("rated") is None else int(filters["rated"]),
         None if filters.get("rated") is None else int(filters["rated"]),
         filters.get("username"), player_id,filters.get("username"),max_members+1),
    )
    selected_count = int(require_row(conn.execute(
        "SELECT COUNT(*) FROM working_set_members WHERE working_set_id=%s", (working_set_id,),
    ))[0])
    if selected_count > max_members:
        raise ValidationError(
            f"Selection exceeds the configured limit of {max_members} games; use a smaller selection or an operator-configured limit",
            code="working_set_too_large",
        )
    hash_state = hashlib.sha256(canonical({"schema": 1, "filters": filters}).encode())
    after = count = 0
    while True:
        rows = list(conn.execute(
            "SELECT ordinal,game_version_id FROM working_set_members WHERE working_set_id=%s AND ordinal>%s ORDER BY ordinal LIMIT 1000",
            (working_set_id, after),
        ))
        if not rows:
            break
        for member in rows:
            hash_state.update(f"\n{member['ordinal']}:{member['game_version_id']}".encode())
        count += len(rows)
        after = int(rows[-1]["ordinal"])
    conn.execute(
        "UPDATE working_sets SET input_signature=%s,member_count=%s WHERE id=%s",
        (hash_state.hexdigest(), count, working_set_id),
    )
    conn.execute(
        "INSERT INTO working_set_submissions(workspace_id,submission_namespace,idempotency_key,request_json,working_set_id) VALUES(%s,%s,%s,%s,%s)",
        (workspace_id, submission_namespace, idempotency_key, request_json, working_set_id),
    )
    return {**get_working_set(conn, working_set_id, workspace_id), "replayed": False}


def get_working_set(conn: Connection, working_set_id: int, workspace_id: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM working_sets WHERE id=%s AND workspace_id=%s", (working_set_id, workspace_id),
    ).fetchone()
    if row is None:
        raise NotFound("Working set not found", code="working_set_not_found")
    return dict(row)


def member_page(
    conn: Connection, working_set_id: int, workspace_id: str, *, after: int = 0, limit: int = 100,
) -> dict[str, Any]:
    working_set = get_working_set(conn, working_set_id, workspace_id)
    rows = [dict(row) for row in conn.execute(
        """SELECT m.ordinal,m.game_version_id,gv.game_id FROM working_set_members m
           JOIN game_versions gv ON gv.id=m.game_version_id
           WHERE m.working_set_id=%s AND m.ordinal>%s ORDER BY m.ordinal LIMIT %s""",
        (working_set_id, after, limit + 1),
    )]
    return {"items": rows[:limit], "next_cursor": rows[limit-1]["ordinal"] if len(rows)>limit else None,
            "total": working_set["member_count"], "input_signature": working_set["input_signature"]}


def result_signature(working_set: dict[str, Any], implementation: str, version: str, settings: dict[str, Any]) -> str:
    return digest({"schema": 1, "inputs": working_set["input_signature"], "implementation": implementation,
                   "version": version, "settings": {"working_set": working_set["settings"], "calculation": settings}})


@atomic
def save_result(
    conn: Connection, working_set_id: int, workspace_id: str, *, implementation: str,
    implementation_version: str, settings: dict[str, Any], output: dict[str, Any],
    limits: Limits | None = None,
) -> dict[str, Any]:
    policy = limits if limits is not None else Limits.from_env()
    working_set = get_working_set(conn, working_set_id, workspace_id)
    signature = result_signature(working_set, implementation, implementation_version, settings)
    # All result writers and retention operations share this transaction-scoped
    # workspace lock, including independent API processes and working sets.
    operation_lock(conn, "analysis-results", workspace_id)
    existing = conn.execute(
        "SELECT * FROM analysis_results WHERE workspace_id=%s AND result_signature=%s", (workspace_id, signature),
    ).fetchone()
    if existing is not None:
        if canonical(existing["output"]) != canonical(output):
            raise Conflict("Calculation signature already has a different output", code="result_conflict")
        return {**dict(existing), "replayed": True}
    settings_json = canonical({"working_set": working_set["settings"], "calculation": settings})
    output_json = canonical(output)
    incoming_bytes = int(require_row(conn.execute(
        "SELECT analysis_result_bytes(%s::jsonb,%s::jsonb,%s,%s,%s)",
        (settings_json, output_json, implementation, implementation_version, workspace_id),
    ))[0])
    usage = require_row(conn.execute(
        """SELECT COUNT(*), COALESCE(SUM(stored_bytes),0) FROM (
             SELECT stored_bytes FROM analysis_results WHERE workspace_id=%s
             ORDER BY created_at,id LIMIT %s
           ) retained""",
        # Reaching the count ceiling already denies admission, so legacy
        # over-quota workspaces need no unbounded scan or JSONB decompression.
        (workspace_id, policy.max_analysis_results),
    ))
    for dimension, used, requested, ceiling in (
        ("analysis_results", int(usage[0]), 1, policy.max_analysis_results),
        ("analysis_result_bytes", int(usage[1]), incoming_bytes, policy.max_analysis_result_bytes),
    ):
        if used + requested > ceiling:
            raise QuotaExceeded(dimension, remaining=max(0, ceiling-used))
    row = require_row(conn.execute(
        """INSERT INTO analysis_results(workspace_id,input_signature,implementation,implementation_version,
           settings,result_signature,output,created_at) VALUES(%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb,%s)
           RETURNING *""",
        (workspace_id, working_set["input_signature"], implementation, implementation_version,
         settings_json, signature, output_json, int(time.time())),
    ))
    return {**dict(row), "replayed": False}


@atomic
def prune_results(conn: Connection, workspace_id: str, *, before: int, limit: int = 256) -> dict[str, int]:
    """Explicit operator retention: remove one bounded batch for exactly one owner."""
    validate_workspace(workspace_id)
    if type(before) is not int or not 0 <= before <= 253402300799:
        raise ValueError("Result retention cutoff must be a nonnegative Unix timestamp through year 9999")
    if type(limit) is not int or not 1 <= limit <= 10000:
        raise ValueError("Result retention batch size must be between 1 and 10000")
    operation_lock(conn, "analysis-results", workspace_id)
    rows = conn.execute(
        """WITH obsolete AS (
             SELECT id FROM analysis_results WHERE workspace_id=%s AND created_at<%s
             ORDER BY created_at,id LIMIT %s FOR UPDATE
           ) DELETE FROM analysis_results r USING obsolete WHERE r.id=obsolete.id
             RETURNING r.stored_bytes""",
        (workspace_id, before, limit),
    ).fetchall()
    return {"deleted": len(rows), "released_bytes": sum(int(row[0]) for row in rows)}


def read_result(conn: Connection, result_id: int, workspace_id: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM analysis_results WHERE id=%s AND workspace_id=%s", (result_id,workspace_id)).fetchone()
    if row is None:
        raise NotFound("Analysis result not found", code="result_not_found")
    return dict(row)


def lookup_result(
    conn: Connection, working_set_id: int, workspace_id: str, *, implementation: str,
    implementation_version: str, settings: dict[str, Any],
) -> dict[str, Any]:
    working_set = get_working_set(conn,working_set_id,workspace_id)
    signature = result_signature(working_set,implementation,implementation_version,settings)
    row = conn.execute(
        "SELECT * FROM analysis_results WHERE workspace_id=%s AND result_signature=%s", (workspace_id,signature),
    ).fetchone()
    if row is None:
        raise NotFound("Compatible analysis result not found",code="result_not_found")
    return dict(row)
