"""Permanent submission identities; job and run mutations remain in jobs.state."""

from __future__ import annotations

import json
import sqlite3
import time

from chess_crawl.storage.db import atomic


def get_submission(conn: sqlite3.Connection, key: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT operation, request_json, crawl_run_id, job_ids_json FROM application_submissions WHERE idempotency_key = ?",
        (key,),
    ).fetchone()


@atomic
def record_submission(
    conn: sqlite3.Connection,
    *,
    key: str,
    operation: str,
    request_json: str,
    run_id: int,
    job_ids: list[int],
) -> None:
    conn.execute(
        """
        INSERT INTO application_submissions(
          idempotency_key, operation, request_json, crawl_run_id, job_ids_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (key, operation, request_json, run_id, json.dumps(job_ids), int(time.time())),
    )
