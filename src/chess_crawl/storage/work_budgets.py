"""Atomic trusted admission, monthly workspace quotas, and lifetime work usage."""
from __future__ import annotations

import time
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from psycopg.types.json import Jsonb

from chess_crawl.jobs.budget import BudgetExceeded, BudgetPolicy, QuotaExceeded
from chess_crawl.storage.db import Connection, atomic, operation_lock, require_row
from chess_crawl.storage.workspaces import validate_workspace


def monthly_period(now: int) -> tuple[int, int]:
    current = datetime.fromtimestamp(now, UTC)
    start = datetime(current.year, current.month, 1, tzinfo=UTC)
    end = datetime(current.year + (current.month == 12), 1 if current.month == 12 else current.month + 1, 1, tzinfo=UTC)
    return int(start.timestamp()), int(end.timestamp())


def _period(conn: Connection, workspace_id: str, now: int):
    start, end = monthly_period(now)
    conn.execute(
        """INSERT INTO workspace_budget_periods(workspace_id,period_start,period_end)
             VALUES(%s,%s,%s) ON CONFLICT DO NOTHING""", (workspace_id, start, end),
    )
    return require_row(conn.execute(
        "SELECT * FROM workspace_budget_periods WHERE workspace_id=%s AND period_start=%s FOR UPDATE",
        (workspace_id, start),
    ))


def _workspace_policy(
    conn: Connection, workspace_id: str, policy: BudgetPolicy, now: int, *,
    tighten: bool = False, managed: bool = False,
) -> BudgetPolicy:
    conn.execute(
        """INSERT INTO workspace_budget_policies(workspace_id,policy,updated_at,managed)
             VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING""", (workspace_id, Jsonb(asdict(policy)), now, managed),
    )
    row = require_row(conn.execute("SELECT * FROM workspace_budget_policies WHERE workspace_id=%s FOR UPDATE", (workspace_id,)))
    conn.execute(
        """INSERT INTO workspace_policy_history(workspace_id,version,policy,managed,changed_at)
             VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        (workspace_id, row["version"], Jsonb(dict(row["policy"])), row["managed"], row["updated_at"]),
    )
    stored = BudgetPolicy(**dict(row["policy"]))
    if tighten and not row["managed"]:
        effective = {name: min(value, getattr(policy, name)) if name.startswith("workspace_") else value
                     for name, value in asdict(stored).items()}
        if effective != asdict(stored):
            _replace_workspace_policy(conn, workspace_id, BudgetPolicy(**effective), now, managed=False)
        return BudgetPolicy(**effective)
    return stored


def _replace_workspace_policy(
    conn: Connection, workspace_id: str, policy: BudgetPolicy, now: int, *, managed: bool,
) -> int:
    """Caller holds the workspace budget lock; revisions never reset usage."""
    version = int(require_row(conn.execute(
        """UPDATE workspace_budget_policies SET policy=%s,updated_at=%s,managed=%s,version=version+1
             WHERE workspace_id=%s RETURNING version""", (Jsonb(asdict(policy)), now, managed, workspace_id),
    ))[0])
    conn.execute(
        """INSERT INTO workspace_policy_history(workspace_id,version,policy,managed,changed_at)
             VALUES(%s,%s,%s,%s,%s)""", (workspace_id, version, Jsonb(asdict(policy)), managed, now),
    )
    return version


@atomic
def set_workspace_policy(
    conn: Connection, workspace_id: str, policy: BudgetPolicy, *, expected_version: int,
    now: int | None = None,
) -> int:
    """Trusted administration: replace future admission/shared ceilings atomically."""
    validate_workspace(workspace_id)
    if type(expected_version) is not int or not 0 <= expected_version < 2**63:
        raise ValueError("Expected policy version must be a nonnegative PostgreSQL bigint")
    operation_lock(conn, "workspace-budget", workspace_id)
    row = conn.execute("SELECT version FROM workspace_budget_policies WHERE workspace_id=%s FOR UPDATE", (workspace_id,)).fetchone()
    if row is None and expected_version == 0:
        if conn.execute("SELECT 1 FROM workspaces WHERE id=%s", (workspace_id,)).fetchone() is None:
            raise ValueError("Workspace not found")
        _workspace_policy(conn, workspace_id, policy, int(time.time()) if now is None else now, managed=True)
        return 1
    if row is None or row[0] != expected_version:
        raise ValueError("Workspace policy version changed; inspect it before updating")
    return _replace_workspace_policy(conn, workspace_id, policy, int(time.time()) if now is None else now, managed=True)


def _admission_policy(conn: Connection, workspace_id: str, submitted: BudgetPolicy) -> tuple[BudgetPolicy, int]:
    row = require_row(conn.execute("SELECT policy,managed,version FROM workspace_budget_policies WHERE workspace_id=%s", (workspace_id,)))
    return (BudgetPolicy(**dict(row["policy"])) if row["managed"] else submitted), int(row["version"])


@atomic
def install_workspace_policy(
    conn: Connection, workspace_id: str, policy: BudgetPolicy, *, now: int | None = None,
    managed: bool = False,
) -> dict[str, Any]:
    """Persist trusted tightening separately from admission, without resetting usage."""
    timestamp = int(time.time()) if now is None else now
    operation_lock(conn, "workspace-budget", workspace_id)
    return asdict(_workspace_policy(conn, workspace_id, policy, timestamp, tighten=True, managed=managed))


@atomic
def admit_run_budget(
    conn: Connection, run_id: int, workspace_id: str, policy: BudgetPolicy, *, now: int | None = None,
) -> dict[str, Any]:
    timestamp = int(time.time()) if now is None else now
    operation_lock(conn, "workspace-budget", workspace_id)
    run = require_row(conn.execute("SELECT workspace_id,work_budget_id FROM crawl_runs WHERE id=%s FOR UPDATE", (run_id,)))
    if run["workspace_id"] != workspace_id:
        raise ValueError("Work budget owner does not match the run")
    if run["work_budget_id"] is not None:
        return get_run_budget(conn, run_id, workspace_id) or {}
    workspace_policy = _workspace_policy(conn, workspace_id, policy, timestamp, tighten=True)
    policy, policy_version = _admission_policy(conn, workspace_id, policy)
    usage = _period(conn, workspace_id, timestamp)
    for dimension in ("games", "normalization_units", "remote_bytes", "remote_requests"):
        remaining = getattr(workspace_policy, "workspace_max_" + dimension) - int(usage[dimension])
        if remaining <= 0:
            raise QuotaExceeded(dimension, remaining=0, reset_at=int(usage["period_end"]))
    queued = int(require_row(conn.execute(
        "SELECT COUNT(*) FROM discovery_jobs WHERE workspace_id=%s AND state IN ('pending','blocked','in_progress')",
        (workspace_id,),
    ))[0])
    if queued > workspace_policy.workspace_max_queued_jobs + workspace_policy.workspace_max_active_jobs:
        raise QuotaExceeded("queued_jobs", remaining=0, reset_at=int(usage["period_end"]))
    budget_id = int(require_row(conn.execute(
        """INSERT INTO work_budgets(workspace_id,crawl_run_id,policy,created_at,updated_at,workspace_policy_version)
             VALUES(%s,%s,%s,%s,%s,%s) RETURNING id""",
        (workspace_id, run_id, Jsonb(asdict(policy)), timestamp, timestamp, policy_version),
    ))[0])
    conn.execute("UPDATE crawl_runs SET work_budget_id=%s WHERE id=%s", (budget_id, run_id))
    conn.execute("UPDATE discovery_jobs SET work_budget_id=%s WHERE crawl_run_id=%s", (budget_id, run_id))
    return get_run_budget(conn, run_id, workspace_id) or {}


def _budget_snapshot(conn: Connection, row, *, now: int | None = None) -> dict[str, Any]:
    timestamp = int(time.time()) if now is None else now
    start, end = monthly_period(timestamp)
    snapshot = dict(row)
    authority = conn.execute("SELECT policy,version FROM workspace_budget_policies WHERE workspace_id=%s",
                             (row["workspace_id"],)).fetchone()
    workspace_policy = dict(authority[0]) if authority is not None else dict(row["policy"])
    period = conn.execute("SELECT * FROM workspace_budget_periods WHERE workspace_id=%s AND period_start=%s",
                          (row["workspace_id"], start)).fetchone()
    dimensions = ("games", "normalization_units", "remote_bytes", "remote_requests")
    usage = {dimension: int(period[dimension]) if period is not None else 0 for dimension in dimensions}
    snapshot["workspace_policy"] = workspace_policy
    snapshot["current_workspace_policy_version"] = int(authority["version"]) if authority is not None else None
    snapshot["workspace_period"] = {"period_start": start, "reset_at": end, "usage": usage,
        "remaining": {dimension: max(0, int(workspace_policy["workspace_max_"+dimension])-usage[dimension])
                      for dimension in dimensions}}
    snapshot["remaining"] = {dimension: max(0, int(row["policy"]["job_max_"+dimension])-int(row[dimension]))
                             for dimension in dimensions}
    states = conn.execute("SELECT state,COUNT(*) AS count FROM discovery_jobs WHERE work_budget_id=%s GROUP BY state",
                          (row["id"],)).fetchall()
    snapshot["job_states"] = {item["state"]: int(item["count"]) for item in states}
    snapshot["incomplete"] = any(item["state"] != "done" for item in states)
    snapshot["operator_resume_required"] = row["exhausted_dimension"] is not None
    checkpoints = conn.execute(
        """SELECT j.id AS job_id,j.kind,j.state,c.cursor AS checkpoint FROM discovery_jobs j
             JOIN collection_checkpoints c ON c.job_id=j.id WHERE j.work_budget_id=%s ORDER BY j.id LIMIT 33""",
        (row["id"],),
    ).fetchall()
    compact: list[dict[str, Any]] = []
    for item in checkpoints[:32]:
        cursor = dict(item["checkpoint"])
        progress = {key: cursor[key] for key in (
            "request_fingerprint", "unit_index", "since_ms", "until_ms", "upper_ms",
            "requested_since_ms", "limit", "phase", "local_after", "local_high_water", "done",
        ) if key in cursor}
        if isinstance(cursor.get("units"), list):
            progress["unit_count"] = len(cursor["units"])
        compact.append({"job_id": item["job_id"], "kind": item["kind"], "state": item["state"],
                        "checkpoint": progress})
    snapshot["checkpoints"] = compact
    snapshot["checkpoints_truncated"] = len(checkpoints) > 32
    return snapshot


def get_run_budget(conn: Connection, run_id: int, workspace_id: str, *, now: int | None = None) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM work_budgets WHERE crawl_run_id=%s AND workspace_id=%s", (run_id, workspace_id),
    ).fetchone()
    return _budget_snapshot(conn, row, now=now) if row is not None else None


def get_job_budget(conn: Connection, job_id: int, workspace_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """SELECT b.* FROM work_budgets b JOIN discovery_jobs j ON j.work_budget_id=b.id
             WHERE j.id=%s AND j.workspace_id=%s AND b.workspace_id=%s""",
        (job_id, workspace_id, workspace_id),
    ).fetchone()
    return _budget_snapshot(conn, row) if row is not None else None


@atomic
def ensure_job_budget(conn: Connection, job_id: int, policy: BudgetPolicy, *, now: int | None = None) -> int:
    timestamp = int(time.time()) if now is None else now
    job = require_row(conn.execute("SELECT workspace_id,crawl_run_id,work_budget_id FROM discovery_jobs WHERE id=%s", (job_id,)))
    if job["work_budget_id"] is not None:
        return int(job["work_budget_id"])
    if job["crawl_run_id"] is not None:
        return int(admit_run_budget(conn, int(job["crawl_run_id"]), job["workspace_id"], policy, now=timestamp)["id"])
    operation_lock(conn, "workspace-budget", job["workspace_id"])
    _workspace_policy(conn, job["workspace_id"], policy, timestamp, tighten=True)
    policy, policy_version = _admission_policy(conn, job["workspace_id"], policy)
    budget_id = int(require_row(conn.execute(
        """INSERT INTO work_budgets(workspace_id,standalone_job_id,policy,created_at,updated_at,workspace_policy_version)
             VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(standalone_job_id) DO UPDATE SET
             standalone_job_id=EXCLUDED.standalone_job_id RETURNING id""",
        (job["workspace_id"], job_id, Jsonb(asdict(policy)), timestamp, timestamp, policy_version),
    ))[0])
    conn.execute("UPDATE discovery_jobs SET work_budget_id=%s WHERE id=%s", (budget_id, job_id))
    return budget_id


def _locked_budget(conn: Connection, budget_id: int, now: int):
    workspace_id = require_row(conn.execute("SELECT workspace_id FROM work_budgets WHERE id=%s", (budget_id,)))[0]
    operation_lock(conn, "workspace-budget", workspace_id)
    budget = require_row(conn.execute("SELECT * FROM work_budgets WHERE id=%s FOR UPDATE", (budget_id,)))
    policy = BudgetPolicy(**dict(budget["policy"]))
    workspace_policy = _workspace_policy(conn, workspace_id, policy, now)
    return budget, policy, _period(conn, workspace_id, now), workspace_policy


def _remaining(budget, policy: BudgetPolicy, usage, workspace_policy: BudgetPolicy, dimension: str) -> int:
    job_remaining = getattr(policy, "job_max_" + dimension) - int(budget[dimension])
    if job_remaining <= 0:
        raise BudgetExceeded(dimension, budget_id=int(budget["id"]))
    workspace_remaining = getattr(workspace_policy, "workspace_max_" + dimension) - int(usage[dimension])
    if workspace_remaining <= 0:
        raise QuotaExceeded(dimension, reset_at=int(usage["period_end"]), budget_id=int(budget["id"]))
    return min(job_remaining, workspace_remaining)


@atomic
def require_backlog_room(conn: Connection, budget_id: int) -> None:
    budget, _, usage, policy = _locked_budget(conn, budget_id, int(time.time()))
    count = int(require_row(conn.execute(
        "SELECT COUNT(*) FROM discovery_jobs WHERE workspace_id=%s AND state IN ('pending','blocked','in_progress')",
        (budget["workspace_id"],),
    ))[0])
    if count >= policy.workspace_max_queued_jobs + policy.workspace_max_active_jobs:
        raise QuotaExceeded("queued_jobs", reset_at=int(usage["period_end"]), budget_id=budget_id)


@atomic
def reserve_request(conn: Connection, budget_id: int, *, now: int | None = None) -> tuple[int, int]:
    """Reserve network work before I/O; uncertain crashed reservations remain spent."""
    timestamp = int(time.time()) if now is None else now
    budget, policy, usage, workspace_policy = _locked_budget(conn, budget_id, timestamp)
    _remaining(budget, policy, usage, workspace_policy, "remote_requests")
    _remaining(budget, policy, usage, workspace_policy, "games")
    _remaining(budget, policy, usage, workspace_policy, "normalization_units")
    available = _remaining(budget, policy, usage, workspace_policy, "remote_bytes")
    if available < 2:
        raise BudgetExceeded("remote_bytes", remaining=available, budget_id=budget_id)
    limit = min(policy.max_response_bytes, available // 2 if available < 131072 else available - 65536)
    reserved = limit + min(65536, limit)
    for table, where, params in (
        ("work_budgets", "id=%s", (budget_id,)),
        ("workspace_budget_periods", "workspace_id=%s AND period_start=%s", (budget["workspace_id"], usage["period_start"])),
    ):
        conn.execute(
            f"UPDATE {table} SET remote_requests=remote_requests+1,remote_bytes=remote_bytes+%s,normalization_units=normalization_units+1 WHERE {where}",  # nosec B608 # Fixed table/where pairs only.
            (reserved, *params),
        )
    reservation_id = int(require_row(conn.execute(
        """INSERT INTO work_request_reservations(budget_id,workspace_id,period_start,reserved_bytes,created_at)
             VALUES(%s,%s,%s,%s,%s) RETURNING id""",
        (budget_id, budget["workspace_id"], usage["period_start"], reserved, timestamp),
    ))[0])
    return reservation_id, limit


@atomic
def settle_request(conn: Connection, reservation_id: int, consumed_bytes: int) -> None:
    first = require_row(conn.execute("SELECT workspace_id FROM work_request_reservations WHERE id=%s", (reservation_id,)))
    operation_lock(conn, "workspace-budget", first[0])
    row = require_row(conn.execute("SELECT * FROM work_request_reservations WHERE id=%s FOR UPDATE", (reservation_id,)))
    if row["consumed_bytes"] is not None:
        return
    if type(consumed_bytes) is not int or not 0 <= consumed_bytes <= int(row["reserved_bytes"]):
        raise ValueError("Consumed bytes exceed the reserved bounded read allowance")
    refund = int(row["reserved_bytes"]) - consumed_bytes
    conn.execute("UPDATE work_budgets SET remote_bytes=remote_bytes-%s WHERE id=%s", (refund, row["budget_id"]))
    conn.execute(
        "UPDATE workspace_budget_periods SET remote_bytes=remote_bytes-%s WHERE workspace_id=%s AND period_start=%s",
        (refund, row["workspace_id"], row["period_start"]),
    )
    conn.execute("UPDATE work_request_reservations SET consumed_bytes=%s WHERE id=%s", (consumed_bytes, reservation_id))


@atomic
def reserve_normalization(
    conn: Connection, budget_id: int, *, game_key: str | None = None, now: int | None = None,
) -> None:
    timestamp = int(time.time()) if now is None else now
    budget, policy, usage, workspace_policy = _locked_budget(conn, budget_id, timestamp)
    _remaining(budget, policy, usage, workspace_policy, "normalization_units")
    game_units = 0
    if game_key is not None and conn.execute(
        "SELECT 1 FROM budget_game_items WHERE budget_id=%s AND game_key=%s", (budget_id, game_key),
    ).fetchone() is None:
        _remaining(budget, policy, usage, workspace_policy, "games")
        conn.execute("INSERT INTO budget_game_items(budget_id,game_key) VALUES(%s,%s)", (budget_id, game_key))
        game_units = 1
    conn.execute(
        "UPDATE work_budgets SET normalization_units=normalization_units+1,games=games+%s,updated_at=%s WHERE id=%s",
        (game_units, timestamp, budget_id),
    )
    conn.execute(
        """UPDATE workspace_budget_periods SET normalization_units=normalization_units+1,games=games+%s
             WHERE workspace_id=%s AND period_start=%s""", (game_units, budget["workspace_id"], usage["period_start"]),
    )


@atomic
def reserve_payload_read(conn: Connection, budget_id: int, raw_payload_id: int) -> None:
    """Bound retained legacy inputs and bill interpretation before reading objects."""
    budget = require_row(conn.execute("SELECT policy FROM work_budgets WHERE id=%s", (budget_id,)))
    payload = conn.execute("SELECT body_bytes FROM raw_payloads WHERE id=%s", (raw_payload_id,)).fetchone()
    if payload is None:
        raise KeyError(f"raw payload not found: {raw_payload_id}")
    if int(payload[0]) > int(budget["policy"]["max_response_bytes"]):
        raise BudgetExceeded("processing_payload_bytes", budget_id=budget_id)
    if conn._work_payload_read_credits:
        conn._work_payload_read_credits -= 1
    else:
        reserve_normalization(conn, budget_id)


@atomic
def exhaust_budget(conn: Connection, budget_id: int, dimension: str, *, now: int | None = None) -> None:
    timestamp = int(time.time()) if now is None else now
    conn.execute("UPDATE work_budgets SET exhausted_dimension=%s,exhausted_at=%s,updated_at=%s WHERE id=%s",
                 (dimension, timestamp, timestamp, budget_id))


@atomic
def resume_run_budget(conn: Connection, run_id: int, workspace_id: str, policy: BudgetPolicy) -> dict[str, Any]:
    """Trusted administration only: extend ceilings without resetting spent work."""
    operation_lock(conn, "workspace-budget", workspace_id)
    row = conn.execute("SELECT * FROM work_budgets WHERE crawl_run_id=%s AND workspace_id=%s FOR UPDATE", (run_id, workspace_id)).fetchone()
    if row is None:
        raise ValueError("Owned run budget not found")
    previous = BudgetPolicy(**dict(row["policy"]))
    extended = {name: max(value, getattr(policy, name)) for name, value in asdict(previous).items()}
    conn.execute("UPDATE work_budgets SET policy=%s,exhausted_dimension=NULL,exhausted_at=NULL WHERE id=%s", (Jsonb(extended), row["id"]))
    workspace_policy = _workspace_policy(conn, workspace_id, previous, int(time.time()))
    extended_workspace = {name: max(value, getattr(policy, name)) if name.startswith("workspace_") else value
                          for name, value in asdict(workspace_policy).items()}
    if extended_workspace != asdict(workspace_policy):
        managed = bool(require_row(conn.execute("SELECT managed FROM workspace_budget_policies WHERE workspace_id=%s", (workspace_id,)))[0])
        _replace_workspace_policy(conn, workspace_id, BudgetPolicy(**extended_workspace), int(time.time()), managed=managed)
    conn.execute(
        """UPDATE discovery_jobs SET state='pending',reason=NULL,next_attempt_at=NULL
             WHERE work_budget_id=%s AND state='blocked' AND reason LIKE 'budget_exhausted:%%'""", (row["id"],),
    )
    return get_run_budget(conn, run_id, workspace_id) or {}
