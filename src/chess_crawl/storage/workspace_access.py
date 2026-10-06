"""Trusted workspace provisioning and revocable opaque service credentials."""
from __future__ import annotations

import hashlib
import re
import secrets
import time
from dataclasses import asdict
from typing import Any
from uuid import uuid4

from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.storage.db import Connection, atomic, consistent_read, operation_lock
from chess_crawl.storage.work_budgets import install_workspace_policy, monthly_period
from chess_crawl.storage.workspaces import validate_workspace

_TOKEN = re.compile(r"ccw_[A-Za-z0-9_-]{43}\Z")


def valid_credential_token(token: str) -> bool:
    return _TOKEN.fullmatch(token) is not None


def _require_workspace(conn: Connection, workspace_id: str) -> None:
    validate_workspace(workspace_id)
    if conn.execute("SELECT 1 FROM workspaces WHERE id=%s", (workspace_id,)).fetchone() is None:
        raise ValueError("Workspace not found")


def authenticate_token(conn: Connection, token: str) -> str | None:
    """Resolve every request against shared authority; no process-local cache."""
    if not valid_credential_token(token):
        return None
    digest = hashlib.sha256(token.encode("ascii")).hexdigest()
    row = conn.execute(
        "SELECT workspace_id FROM workspace_credentials WHERE token_digest=%s AND revoked_at IS NULL",
        (digest,),
    ).fetchone()
    return str(row[0]) if row is not None else None


@atomic
def issue_credential(conn: Connection, workspace_id: str, *, now: int | None = None) -> dict[str, Any]:
    _require_workspace(conn, workspace_id)
    operation_lock(conn, "workspace-access", workspace_id)
    token = "ccw_" + secrets.token_urlsafe(32)
    credential_id = uuid4().hex
    timestamp = int(time.time()) if now is None else now
    conn.execute(
        "INSERT INTO workspace_credentials(id,workspace_id,token_digest,created_at) VALUES(%s,%s,%s,%s)",
        (credential_id, workspace_id, hashlib.sha256(token.encode("ascii")).hexdigest(), timestamp),
    )
    return {"workspace_id": workspace_id, "credential_id": credential_id, "token": token, "created_at": timestamp}


@atomic
def revoke_credential(
    conn: Connection, workspace_id: str, credential_id: str, *, now: int | None = None,
) -> dict[str, Any]:
    _require_workspace(conn, workspace_id)
    operation_lock(conn, "workspace-access", workspace_id)
    row = conn.execute(
        """UPDATE workspace_credentials SET revoked_at=COALESCE(revoked_at,%s)
             WHERE id=%s AND workspace_id=%s RETURNING revoked_at""",
        (int(time.time()) if now is None else now, credential_id, workspace_id),
    ).fetchone()
    if row is None:
        raise ValueError("Workspace credential not found")
    return {"workspace_id": workspace_id, "credential_id": credential_id, "revoked_at": int(row[0])}


@atomic
def rotate_credentials(conn: Connection, workspace_id: str, *, now: int | None = None) -> dict[str, Any]:
    _require_workspace(conn, workspace_id)
    operation_lock(conn, "workspace-access", workspace_id)
    timestamp = int(time.time()) if now is None else now
    conn.execute(
        "UPDATE workspace_credentials SET revoked_at=%s WHERE workspace_id=%s AND revoked_at IS NULL",
        (timestamp, workspace_id),
    )
    return issue_credential(conn, workspace_id, now=timestamp)


@atomic
def provision_workspace(conn: Connection, workspace_id: str, policy: BudgetPolicy) -> dict[str, Any]:
    validate_workspace(workspace_id)
    operation_lock(conn, "workspace-access", workspace_id)
    created = conn.execute(
        "INSERT INTO workspaces(id,created_at) VALUES(%s,%s) ON CONFLICT DO NOTHING RETURNING id",
        (workspace_id, int(time.time())),
    ).fetchone()
    if created is None:
        raise ValueError("Workspace already exists; inspect it and use set-policy or issue")
    install_workspace_policy(conn, workspace_id, policy, managed=True)
    return {**issue_credential(conn, workspace_id), "policy_version": 1, "policy": asdict(policy)}


@consistent_read
def workspace_snapshot(conn: Connection, workspace_id: str, *, now: int | None = None) -> dict[str, Any]:
    _require_workspace(conn, workspace_id)
    policy = conn.execute("SELECT policy,managed,version,updated_at FROM workspace_budget_policies WHERE workspace_id=%s", (workspace_id,)).fetchone()
    start, end = monthly_period(int(time.time()) if now is None else now)
    period = conn.execute(
        "SELECT remote_requests,remote_bytes,games,normalization_units FROM workspace_budget_periods WHERE workspace_id=%s AND period_start=%s",
        (workspace_id, start),
    ).fetchone()
    credentials = conn.execute(
        "SELECT id,created_at,revoked_at FROM workspace_credentials WHERE workspace_id=%s ORDER BY created_at DESC,id DESC LIMIT 101",
        (workspace_id,),
    ).fetchall()
    return {
        "workspace_id": workspace_id, "policy": dict(policy) if policy is not None else None,
        "period": {"start": start, "reset_at": end, "usage": dict(period) if period is not None else {
            "remote_requests": 0, "remote_bytes": 0, "games": 0, "normalization_units": 0,
        }},
        "credentials": [dict(row) for row in credentials[:100]], "credentials_truncated": len(credentials) > 100,
    }
