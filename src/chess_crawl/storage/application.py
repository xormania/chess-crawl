"""Permanent submission identities; job and run mutations remain in jobs.state."""

from __future__ import annotations

import json
import time

from chess_crawl.storage.db import Connection, Row, atomic, operation_lock


def get_submission(conn: Connection, key: str, workspace_id: str = "local") -> Row | None:
    return conn.execute(
        "SELECT operation, request_json, crawl_run_id, job_ids_json FROM application_submissions WHERE idempotency_key = %s AND workspace_id = %s",
        (key, workspace_id),
    ).fetchone()


@atomic
def record_submission(
    conn: Connection,
    *,
    key: str,
    operation: str,
    request_json: str,
    run_id: int,
    job_ids: list[int],
    workspace_id: str = "local",
) -> None:
    conn.execute(
        """
        INSERT INTO application_submissions(
          idempotency_key, operation, request_json, crawl_run_id, job_ids_json, created_at, workspace_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (key, operation, request_json, run_id, json.dumps(job_ids), int(time.time()), workspace_id),
    )


def lock_submission(conn: Connection, workspace_id: str, key: str) -> None:
    """Serialize identity lookup and creation, independently of archive writes."""
    operation_lock(conn, "submission", f"{workspace_id}:{key}")
