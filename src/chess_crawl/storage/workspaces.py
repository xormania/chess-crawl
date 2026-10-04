"""Trusted service ownership: public archive data, private work and outputs."""
from __future__ import annotations

import re
import time

from chess_crawl.application.errors import NotFound, ValidationError
from chess_crawl.storage.db import Connection

_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,100}\Z")


def validate_workspace(workspace_id: str) -> str:
    if not isinstance(workspace_id, str) or _PATTERN.fullmatch(workspace_id) is None or workspace_id == "public":
        raise ValidationError("Invalid trusted workspace identifier", code="invalid_workspace")
    return workspace_id


def submission_context(conn: Connection, workspace_id: str) -> None:
    """Called only inside a submission transaction; worker children inherit ownership."""
    validate_workspace(workspace_id)
    conn.execute(
        "INSERT INTO workspaces(id, created_at) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (workspace_id, int(time.time())),
    )
    conn.execute("SELECT set_config('chess_crawl.workspace_id', %s, true)", (workspace_id,))


def require_run(conn: Connection, run_id: int, workspace_id: str) -> None:
    if conn.execute(
        "SELECT 1 FROM crawl_runs WHERE id = %s AND workspace_id = %s", (run_id, workspace_id),
    ).fetchone() is None:
        raise NotFound("Crawl run not found", code="run_not_found")


def require_job(conn: Connection, job_id: int, workspace_id: str) -> None:
    if conn.execute(
        "SELECT 1 FROM discovery_jobs WHERE id = %s AND workspace_id = %s", (job_id, workspace_id),
    ).fetchone() is None:
        raise NotFound("Job not found", code="job_not_found")


def worker_snapshot(conn: Connection, snapshot: dict, workspace_id: str) -> dict:
    """Global worker liveness is useful; another workspace's job is private."""
    workers = snapshot.get("workers", [])
    rows = [snapshot, *(worker for worker in workers if isinstance(worker,dict))]
    job_ids = [row["current_job_id"] for row in rows if row.get("current_job_id") is not None]
    allowed = {row["id"] for row in conn.execute(
        "SELECT id FROM discovery_jobs WHERE workspace_id=%s AND id=ANY(%s)", (workspace_id,job_ids),
    )} if job_ids else set()

    def visible(row: dict) -> dict:
        if row.get("current_job_id") is not None and row["current_job_id"] not in allowed:
            return {**row,"current_job_id":None}
        return dict(row)

    result=visible(snapshot)
    if "workers" in snapshot:
        result["workers"]=[visible(worker) for worker in workers if isinstance(worker,dict)]
    return result
