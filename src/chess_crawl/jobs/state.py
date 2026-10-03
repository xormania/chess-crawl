"""Single owner of durable job and crawl-run state."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections.abc import Mapping
from typing import Any

from chess_crawl.jobs.models import DiscoveryJob, EnqueueResult, JOB_KINDS, JobKind, JobState
from chess_crawl.storage.db import atomic
from chess_crawl.storage.discovery import discovery_edge_count


LIVE_STATES = ("pending", "in_progress", "blocked")
TERMINAL_STATES = ("done", "error", "skipped")
_RUN_ALLOWS_WORK = """
    NOT EXISTS (
        SELECT 1 FROM crawl_runs
         WHERE crawl_runs.id = discovery_jobs.crawl_run_id
           AND crawl_runs.status = 'cancelled'
    )
"""


def canonical_params(params: Mapping[str, Any] | None) -> str:
    return json.dumps(params or {}, sort_keys=True, separators=(",", ":"))


def load_params(params_json: str | None) -> dict[str, Any]:
    if not params_json:
        return {}
    parsed = json.loads(params_json)
    if not isinstance(parsed, dict):
        raise ValueError("job params_json must decode to an object")
    return parsed


def make_dedup_key(
    *,
    provider: str,
    kind: str,
    target: str,
    params: Mapping[str, Any] | None = None,
    crawl_run_id: int | None = None,
) -> str:
    payload = {
        "provider": provider,
        "kind": kind,
        "target": _normalize_target(kind, target),
        "params": params or {},
        "crawl_run_id": crawl_run_id,
    }
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(body).hexdigest()


@atomic
def enqueue_job(
    conn: sqlite3.Connection,
    *,
    provider: str,
    kind: JobKind,
    target: str,
    params: Mapping[str, Any] | None = None,
    crawl_run_id: int | None = None,
    parent_job_id: int | None = None,
    depth: int = 0,
    priority: int = 100,
    now: int | None = None,
    dedup_key: str | None = None,
) -> EnqueueResult:
    _validate_schedulable_job(provider=provider, kind=kind)
    timestamp = int(time.time()) if now is None else now
    params_text = canonical_params(params)
    dedup = dedup_key or make_dedup_key(
        provider=provider,
        kind=kind,
        target=target,
        params=params,
        crawl_run_id=crawl_run_id,
    )
    existing = conn.execute(
        """
        SELECT id FROM discovery_jobs
         WHERE dedup_key = ? AND state IN ('pending','in_progress','blocked')
         ORDER BY id LIMIT 1
        """,
        (dedup,),
    ).fetchone()
    if existing is not None:
        return EnqueueResult(job_id=int(existing["id"]), inserted=False)

    cursor = conn.execute(
        """
        INSERT INTO discovery_jobs(
          crawl_run_id, parent_job_id, provider, kind, target, params_json,
          state, priority, depth, attempts, dedup_key, enqueued_at
        )
        VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, 0, ?, ?)
        """,
        (
            crawl_run_id,
            parent_job_id,
            provider,
            kind,
            target.strip(),
            params_text,
            priority,
            depth,
            dedup,
            timestamp,
        ),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("job insert did not return a row id")
    return EnqueueResult(job_id=int(cursor.lastrowid), inserted=True)


def _validate_schedulable_job(*, provider: str, kind: str) -> None:
    if kind not in JOB_KINDS:
        raise ValueError(f"unsupported job kind: {kind}")
    if kind == "fetch_user_stats" and provider != "chess.com":
        raise ValueError("fetch_user_stats jobs are supported only for chess.com")
    if kind == "fetch_game_by_id" and provider != "lichess":
        raise ValueError("fetch_game_by_id jobs are supported only for lichess")


@atomic
def claim_next_job(
    conn: sqlite3.Connection,
    *,
    crawl_run_id: int | None = None,
    now: int | None = None,
) -> DiscoveryJob | None:
    timestamp = int(time.time()) if now is None else now
    row = conn.execute(
        f"""
        UPDATE discovery_jobs
           SET state = 'in_progress',
               started_at = ?,
               attempts = attempts + 1,
               reason = NULL,
               done_at = NULL
         WHERE id = (
           SELECT id
             FROM discovery_jobs
            WHERE state = 'pending'
              AND (? IS NULL OR crawl_run_id = ?)
              AND {_RUN_ALLOWS_WORK}
            ORDER BY priority ASC, depth ASC, enqueued_at ASC, id ASC
            LIMIT 1
         )
         RETURNING *
        """,
        (timestamp, crawl_run_id, crawl_run_id),
    ).fetchone()
    return None if row is None else row_to_job(row)


@atomic
def mark_job(
    conn: sqlite3.Connection,
    job_id: int,
    state: JobState,
    *,
    reason: str | None = None,
    now: int | None = None,
) -> None:
    timestamp = int(time.time()) if now is None else now
    done_at = timestamp if state in TERMINAL_STATES else None
    conn.execute(
        """
        UPDATE discovery_jobs
           SET state = ?,
               done_at = ?,
               reason = ?
         WHERE id = ?
        """,
        (state, done_at, reason, job_id),
    )


def mark_done(conn: sqlite3.Connection, job_id: int, *, reason: str | None = None) -> None:
    mark_job(conn, job_id, "done", reason=reason)


def mark_error(conn: sqlite3.Connection, job_id: int, *, reason: str) -> None:
    mark_job(conn, job_id, "error", reason=reason)


def mark_skipped(conn: sqlite3.Connection, job_id: int, *, reason: str) -> None:
    mark_job(conn, job_id, "skipped", reason=reason)


def mark_blocked(conn: sqlite3.Connection, job_id: int, *, reason: str) -> None:
    mark_job(conn, job_id, "blocked", reason=reason)


@atomic
def update_job_params(
    conn: sqlite3.Connection,
    job_id: int,
    params: Mapping[str, Any],
) -> None:
    conn.execute(
        "UPDATE discovery_jobs SET params_json = ? WHERE id = ?",
        (canonical_params(params), job_id),
    )


@atomic
def resume_stale_in_progress(
    conn: sqlite3.Connection,
    *,
    crawl_run_id: int | None = None,
    stale_seconds: int = 0,
    now: int | None = None,
) -> int:
    timestamp = int(time.time()) if now is None else now
    cutoff = timestamp - stale_seconds
    cursor = conn.execute(
        f"""
        UPDATE discovery_jobs
           SET state = 'pending',
               started_at = NULL,
               done_at = NULL,
               reason = COALESCE(reason, 'resumed stale in_progress job')
         WHERE state = 'in_progress'
           AND (? IS NULL OR crawl_run_id = ?)
           AND {_RUN_ALLOWS_WORK}
           AND (? = 0 OR started_at IS NULL OR started_at <= ?)
        """,
        (crawl_run_id, crawl_run_id, stale_seconds, cutoff),
    )
    return int(cursor.rowcount)


@atomic
def unblock_jobs(conn: sqlite3.Connection, *, crawl_run_id: int | None = None) -> int:
    cursor = conn.execute(
        f"""
        UPDATE discovery_jobs
           SET state = 'pending',
               started_at = NULL,
               done_at = NULL,
               reason = COALESCE(reason, 'unblocked by jobs resume')
         WHERE state = 'blocked'
           AND (? IS NULL OR crawl_run_id = ?)
           AND {_RUN_ALLOWS_WORK}
        """,
        (crawl_run_id, crawl_run_id),
    )
    return int(cursor.rowcount)


@atomic
def create_crawl_run(
    conn: sqlite3.Connection,
    *,
    provider: str,
    seed_spec: str,
    params: Mapping[str, Any],
    now: int | None = None,
) -> int:
    timestamp = int(time.time()) if now is None else now
    cursor = conn.execute(
        """
        INSERT INTO crawl_runs(seed_spec, provider, params_json, status, counters, started_at, updated_at)
        VALUES (?, ?, ?, 'running', '{}', ?, ?)
        """,
        (seed_spec, provider, canonical_params(params), timestamp, timestamp),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("crawl run insert did not return a row id")
    return int(cursor.lastrowid)


@atomic
def update_crawl_run(
    conn: sqlite3.Connection,
    crawl_run_id: int,
    *,
    status: str | None = None,
    counters: Mapping[str, Any] | None = None,
    finished: bool = False,
    now: int | None = None,
) -> None:
    timestamp = int(time.time()) if now is None else now
    conn.execute(
        """
        UPDATE crawl_runs
           SET status = COALESCE(?, status),
               counters = COALESCE(?, counters),
               updated_at = ?,
               finished_at = CASE
                 WHEN ? THEN COALESCE(finished_at, ?)
                 WHEN ? IN ('running', 'paused') THEN NULL
                 ELSE finished_at
               END
         WHERE id = ?
        """,
        (
            status,
            None if counters is None else canonical_params(counters),
            timestamp,
            1 if finished else 0,
            timestamp,
            status,
            crawl_run_id,
        ),
    )


@atomic
def create_crawl_run_with_root_job(
    conn: sqlite3.Connection,
    *,
    provider: str,
    seed_spec: str,
    params: Mapping[str, Any],
    root_kind: JobKind,
    root_target: str,
    priority: int = 100,
) -> tuple[int, int]:
    """Create a run and its initial job as one durable operation."""
    run_id = create_crawl_run(conn, provider=provider, seed_spec=seed_spec, params=params)
    root = enqueue_job(
        conn,
        provider=provider,
        kind=root_kind,
        target=root_target,
        params=params,
        crawl_run_id=run_id,
        priority=priority,
    )
    return run_id, root.job_id


@atomic
def checkpoint_job(
    conn: sqlite3.Connection,
    job_id: int,
    params: Mapping[str, Any],
    *,
    cursor_index: int,
    status_code: int,
) -> bool:
    """Persist a completed acquisition unit; failed units remain resumable."""
    if status_code not in {200, 304}:
        return False
    update_job_params(conn, job_id, {**params, "cursor_index": cursor_index})
    return True


def run_counters(conn: sqlite3.Connection, crawl_run_id: int) -> dict[str, int]:
    counters = {
        "jobs_total": total_jobs_for_run(conn, crawl_run_id),
        "users_seen": crawl_user_count(conn, crawl_run_id),
        "edges": discovery_edge_count(conn, crawl_run_id),
    }
    for row in job_state_counts(conn, crawl_run_id=crawl_run_id):
        counters[f"jobs_{row['state']}"] = int(row["count"])
    return counters


@atomic
def refresh_run_status(conn: sqlite3.Connection, crawl_run_id: int) -> None:
    """Derive run state from its jobs without reopening an explicit cancellation."""
    run = conn.execute("SELECT status FROM crawl_runs WHERE id = ?", (crawl_run_id,)).fetchone()
    if run is None or run["status"] == "cancelled":
        return
    counts = {row["state"]: int(row["count"]) for row in job_state_counts(conn, crawl_run_id=crawl_run_id)}
    if counts.get("pending", 0) or counts.get("in_progress", 0):
        status = "running"
    elif counts.get("blocked", 0):
        status = "paused"
    elif counts.get("error", 0):
        status = "failed"
    else:
        status = "done"
    update_crawl_run(
        conn,
        crawl_run_id,
        status=status,
        counters=run_counters(conn, crawl_run_id),
        finished=status in {"failed", "done"},
    )


@atomic
def refresh_crawl_runs(conn: sqlite3.Connection, *, crawl_run_id: int | None = None) -> None:
    if crawl_run_id is not None:
        refresh_run_status(conn, crawl_run_id)
    else:
        for row in crawl_runs(conn):
            refresh_run_status(conn, int(row["id"]))


def get_job(conn: sqlite3.Connection, job_id: int) -> DiscoveryJob | None:
    row = conn.execute("SELECT * FROM discovery_jobs WHERE id = ?", (job_id,)).fetchone()
    return None if row is None else row_to_job(row)


def list_jobs(conn: sqlite3.Connection, *, limit: int = 100) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT id, crawl_run_id, parent_job_id, provider, kind, target, state,
                   priority, depth, attempts, enqueued_at, started_at, done_at, reason
              FROM discovery_jobs
             ORDER BY id
             LIMIT ?
            """,
            (limit,),
        )
    )


def crawl_runs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute("SELECT * FROM crawl_runs ORDER BY id"))


def job_state_counts(conn: sqlite3.Connection, *, crawl_run_id: int | None = None) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT state, COUNT(*) AS count
              FROM discovery_jobs
             WHERE (? IS NULL OR crawl_run_id = ?)
             GROUP BY state
             ORDER BY state
            """,
            (crawl_run_id, crawl_run_id),
        )
    )


def job_kind_state_counts(conn: sqlite3.Connection, *, crawl_run_id: int | None = None) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT kind, state, depth, COUNT(*) AS count
              FROM discovery_jobs
             WHERE (? IS NULL OR crawl_run_id = ?)
             GROUP BY kind, state, depth
             ORDER BY depth, kind, state
            """,
            (crawl_run_id, crawl_run_id),
        )
    )


def total_jobs_for_run(conn: sqlite3.Connection, crawl_run_id: int) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM discovery_jobs WHERE crawl_run_id = ?",
            (crawl_run_id,),
        ).fetchone()[0]
    )


def crawl_user_count(conn: sqlite3.Connection, crawl_run_id: int) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(DISTINCT lower(target))
              FROM discovery_jobs
             WHERE crawl_run_id = ? AND kind = 'crawl_opponents'
            """,
            (crawl_run_id,),
        ).fetchone()[0]
    )


def known_crawl_depth(
    conn: sqlite3.Connection,
    *,
    crawl_run_id: int,
    provider: str,
    username: str,
) -> int | None:
    row = conn.execute(
        """
        SELECT MIN(depth) AS depth
          FROM discovery_jobs
         WHERE crawl_run_id = ?
           AND provider = ?
           AND kind = 'crawl_opponents'
           AND lower(target) = lower(?)
        """,
        (crawl_run_id, provider, username),
    ).fetchone()
    if row is None or row["depth"] is None:
        return None
    return int(row["depth"])


def row_to_job(row: sqlite3.Row) -> DiscoveryJob:
    return DiscoveryJob(
        id=int(row["id"]),
        crawl_run_id=None if row["crawl_run_id"] is None else int(row["crawl_run_id"]),
        parent_job_id=None if row["parent_job_id"] is None else int(row["parent_job_id"]),
        provider=row["provider"],
        kind=row["kind"],
        target=row["target"],
        params_json=row["params_json"],
        state=row["state"],
        priority=int(row["priority"]),
        depth=int(row["depth"]),
        attempts=int(row["attempts"]),
        dedup_key=row["dedup_key"],
        enqueued_at=None if row["enqueued_at"] is None else int(row["enqueued_at"]),
        started_at=None if row["started_at"] is None else int(row["started_at"]),
        done_at=None if row["done_at"] is None else int(row["done_at"]),
        reason=row["reason"],
    )


def _normalize_target(kind: str, target: str) -> str:
    stripped = target.strip()
    if kind in {"fetch_user_profile", "fetch_user_stats", "fetch_user_games", "crawl_opponents"}:
        return stripped.lower()
    return stripped


def get_run(conn: sqlite3.Connection, run_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM crawl_runs WHERE id = ?", (run_id,)).fetchone()


def job_ids_for_run(conn: sqlite3.Connection, run_id: int) -> list[int]:
    return [int(row["id"]) for row in conn.execute("SELECT id FROM discovery_jobs WHERE crawl_run_id = ? ORDER BY id", (run_id,))]
