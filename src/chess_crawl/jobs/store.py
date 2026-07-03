"""Durable job and crawl-run persistence helpers."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections.abc import Mapping
from typing import Any

from chess_crawl.jobs.models import DiscoveryJob, EnqueueResult, JobKind, JobState


LIVE_STATES = ("pending", "in_progress", "blocked")
TERMINAL_STATES = ("done", "error", "skipped")


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
    commit: bool = True,
) -> EnqueueResult:
    timestamp = now or int(time.time())
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
    if commit:
        conn.commit()
    return EnqueueResult(job_id=int(cursor.lastrowid), inserted=True)


def claim_next_job(
    conn: sqlite3.Connection,
    *,
    crawl_run_id: int | None = None,
    now: int | None = None,
) -> DiscoveryJob | None:
    timestamp = now or int(time.time())
    row = conn.execute(
        """
        UPDATE discovery_jobs
           SET state = 'in_progress',
               started_at = ?,
               attempts = attempts + 1,
               reason = NULL
         WHERE id = (
           SELECT id
             FROM discovery_jobs
            WHERE state = 'pending'
              AND (? IS NULL OR crawl_run_id = ?)
            ORDER BY priority ASC, depth ASC, enqueued_at ASC, id ASC
            LIMIT 1
         )
         RETURNING *
        """,
        (timestamp, crawl_run_id, crawl_run_id),
    ).fetchone()
    conn.commit()
    return None if row is None else row_to_job(row)


def mark_job(
    conn: sqlite3.Connection,
    job_id: int,
    state: JobState,
    *,
    reason: str | None = None,
    now: int | None = None,
    commit: bool = True,
) -> None:
    timestamp = now or int(time.time())
    done_at = timestamp if state in TERMINAL_STATES else None
    conn.execute(
        """
        UPDATE discovery_jobs
           SET state = ?,
               done_at = CASE WHEN ? IS NULL THEN done_at ELSE ? END,
               reason = ?
         WHERE id = ?
        """,
        (state, done_at, done_at, reason, job_id),
    )
    if commit:
        conn.commit()


def mark_done(conn: sqlite3.Connection, job_id: int, *, reason: str | None = None) -> None:
    mark_job(conn, job_id, "done", reason=reason)


def mark_error(conn: sqlite3.Connection, job_id: int, *, reason: str) -> None:
    mark_job(conn, job_id, "error", reason=reason)


def mark_skipped(conn: sqlite3.Connection, job_id: int, *, reason: str) -> None:
    mark_job(conn, job_id, "skipped", reason=reason)


def mark_blocked(conn: sqlite3.Connection, job_id: int, *, reason: str) -> None:
    mark_job(conn, job_id, "blocked", reason=reason)


def update_job_params(
    conn: sqlite3.Connection,
    job_id: int,
    params: Mapping[str, Any],
    *,
    commit: bool = True,
) -> None:
    conn.execute(
        "UPDATE discovery_jobs SET params_json = ? WHERE id = ?",
        (canonical_params(params), job_id),
    )
    if commit:
        conn.commit()


def resume_stale_in_progress(
    conn: sqlite3.Connection,
    *,
    crawl_run_id: int | None = None,
    stale_seconds: int = 0,
    now: int | None = None,
) -> int:
    timestamp = now or int(time.time())
    cutoff = timestamp - stale_seconds
    cursor = conn.execute(
        """
        UPDATE discovery_jobs
           SET state = 'pending',
               started_at = NULL,
               reason = COALESCE(reason, 'resumed stale in_progress job')
         WHERE state = 'in_progress'
           AND (? IS NULL OR crawl_run_id = ?)
           AND (? = 0 OR started_at IS NULL OR started_at <= ?)
        """,
        (crawl_run_id, crawl_run_id, stale_seconds, cutoff),
    )
    conn.commit()
    return int(cursor.rowcount)


def unblock_jobs(conn: sqlite3.Connection, *, crawl_run_id: int | None = None) -> int:
    cursor = conn.execute(
        """
        UPDATE discovery_jobs
           SET state = 'pending',
               started_at = NULL,
               reason = COALESCE(reason, 'unblocked by jobs resume')
         WHERE state = 'blocked'
           AND (? IS NULL OR crawl_run_id = ?)
        """,
        (crawl_run_id, crawl_run_id),
    )
    conn.commit()
    return int(cursor.rowcount)


def create_crawl_run(
    conn: sqlite3.Connection,
    *,
    provider: str,
    seed_spec: str,
    params: Mapping[str, Any],
    now: int | None = None,
) -> int:
    timestamp = now or int(time.time())
    cursor = conn.execute(
        """
        INSERT INTO crawl_runs(seed_spec, provider, params_json, status, counters, started_at, updated_at)
        VALUES (?, ?, ?, 'running', '{}', ?, ?)
        """,
        (seed_spec, provider, canonical_params(params), timestamp, timestamp),
    )
    conn.commit()
    return int(cursor.lastrowid)


def update_crawl_run(
    conn: sqlite3.Connection,
    crawl_run_id: int,
    *,
    status: str | None = None,
    counters: Mapping[str, Any] | None = None,
    finished: bool = False,
    now: int | None = None,
) -> None:
    timestamp = now or int(time.time())
    conn.execute(
        """
        UPDATE crawl_runs
           SET status = COALESCE(?, status),
               counters = COALESCE(?, counters),
               updated_at = ?,
               finished_at = CASE WHEN ? THEN COALESCE(finished_at, ?) ELSE finished_at END
         WHERE id = ?
        """,
        (
            status,
            None if counters is None else canonical_params(counters),
            timestamp,
            1 if finished else 0,
            timestamp,
            crawl_run_id,
        ),
    )
    conn.commit()


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


def insert_error(
    conn: sqlite3.Connection,
    *,
    provider: str | None,
    error_kind: str,
    message: str,
    status_code: int | None = None,
    url: str | None = None,
    endpoint_type: str | None = None,
    retry_count: int = 0,
    is_dead: bool = True,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO errors(provider, url, endpoint_type, error_kind, status_code, message, occurred_at, retry_count, is_dead)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            provider,
            url,
            endpoint_type,
            error_kind,
            status_code,
            message,
            int(time.time()),
            retry_count,
            int(is_dead),
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


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
