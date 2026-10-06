"""Anonymous operational aggregates from one repeatable-read PostgreSQL snapshot."""
from __future__ import annotations

import math
import time
from typing import Any

from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.jobs.models import PROCESSING_JOB_KINDS
from chess_crawl.storage.db import Connection, consistent_read, require_row
from chess_crawl.storage.job_admission import SCHEDULABLE_STATE_SQL, waiting_reason
from chess_crawl.storage.migrations import SCHEMA_VERSION, current_version
from chess_crawl.storage.work_budgets import monthly_period


WAITING_REASONS = ("budget", "paused", "retry", "processing_dependency", "provider_cooldown", "workspace_active_cap", "cancelled")


def _age(timestamp: float, oldest: Any) -> float | None:
    return None if oldest is None else max(0.0, timestamp - float(oldest))


def validate_statement_timeout(timeout_ms: int) -> None:
    if type(timeout_ms) is not int or not 1 <= timeout_ms <= 300000:
        raise ValueError("Metrics statement timeout must be between 1 and 300000 milliseconds")


@consistent_read
def operational_snapshot(
    conn: Connection, *, now: float | None = None, budget_policy: BudgetPolicy | None = None,
    statement_timeout_ms: int | None = None,
) -> dict[str, Any]:
    """Measure live work without locking, mutation, provider requests, or queue I/O.

    Eligible means the database admission checks pass. Another executor may hold
    the provider/job session lock; measurement does not acquire or steal it.
    """
    timestamp = time.time() if now is None else now
    if not math.isfinite(timestamp) or timestamp < 0:
        raise ValueError("Metrics time must be finite and nonnegative")
    if statement_timeout_ms is not None:
        validate_statement_timeout(statement_timeout_ms)
        conn.execute("SELECT set_config('statement_timeout',%s,true)", (f"{statement_timeout_ms}ms",))
    if current_version(conn) != SCHEMA_VERSION:
        raise ValueError("Run chess-crawl-admin migrate before collecting operational metrics")
    policy = budget_policy or BudgetPolicy.from_env()
    reason_sql, reason_parameters = waiting_reason(now=timestamp, workspace_max_active_jobs=policy.workspace_max_active_jobs)
    rows = conn.execute(
        f"""WITH classified AS (
             SELECT CASE WHEN kind=ANY(%s) THEN 'processing' ELSE 'acquisition' END AS stage,
                    state,enqueued_at,
                    CASE WHEN state='in_progress' THEN 'active'
                         ELSE {reason_sql} END AS admission,
                    state='blocked' AND next_attempt_at IS NULL
                      AND reason LIKE 'budget_exhausted:%%' AS budget_paused
               FROM discovery_jobs WHERE state IN ('pending','blocked','in_progress')
           ) SELECT stage,state,CASE WHEN admission='paused' AND budget_paused THEN 'budget'
                                    ELSE admission END AS status,
                    COUNT(*) AS count,MIN(enqueued_at) AS oldest_at
               FROM classified GROUP BY stage,state,status""",  # nosec B608 # Only packaged admission SQL; every value is bound.
        (list(PROCESSING_JOB_KINDS), *reason_parameters),
    ).fetchall()
    stages: dict[str, dict[str, Any]] = {stage: {"eligible_jobs": 0, "oldest_eligible_age_seconds": None,
                      "active_jobs": 0,
                      "pending_jobs": 0, "blocked_jobs": 0,
                      "waiting": dict.fromkeys(WAITING_REASONS, 0)}
              for stage in ("acquisition", "processing")}
    for row in rows:
        stage = stages[row["stage"]]
        count = int(row["count"])
        if row["state"] in {"pending", "blocked"}:
            stage[row["state"] + "_jobs"] += count
        if row["status"] == "active":
            stage["active_jobs"] += count
        elif row["status"] == "eligible":
            stage["eligible_jobs"] += count
            age = _age(timestamp, row["oldest_at"])
            previous = stage["oldest_eligible_age_seconds"]
            if age is not None and (previous is None or age > previous):
                stage["oldest_eligible_age_seconds"] = age
        else:
            stage["waiting"][row["status"]] += count
    return {"schema_version": 1, "observed_at": timestamp, "stages": stages,
            "outboxes": {"dispatch": _dispatch_metrics(conn, timestamp), "events": _event_metrics(conn, timestamp)},
            "usage": _period_usage(conn, timestamp)}


def _dispatch_metrics(conn: Connection, now: float) -> dict[str, Any]:
    row = require_row(conn.execute(
        f"""WITH pending AS (
             SELECT d.available_at,discovery_jobs.enqueued_at,
                    d.job_revision=discovery_jobs.revision AND {SCHEDULABLE_STATE_SQL} AS current,
                    discovery_jobs.next_attempt_at
               FROM dispatch_outbox d JOIN discovery_jobs ON discovery_jobs.id=d.job_id
              WHERE d.delivered_at IS NULL AND d.superseded_at IS NULL
           ), classified AS (
             SELECT *,current AND available_at<=%s
                      AND (next_attempt_at IS NULL OR next_attempt_at<=%s) AS ready FROM pending
           ) SELECT COUNT(*) AS pending,
                    COUNT(*) FILTER (WHERE ready) AS ready,
                    COUNT(*) FILTER (WHERE current AND NOT ready) AS deferred,
                    COUNT(*) FILTER (WHERE NOT current) AS obsolete,
                    MIN(COALESCE(NULLIF(available_at,0),enqueued_at)) FILTER (WHERE ready) AS oldest_ready_at
               FROM classified""",  # nosec B608 # Packaged schedulable-state expression; values are bound.
        (now, now),
    ))
    return {"pending": int(row["pending"]), "ready": int(row["ready"]), "deferred": int(row["deferred"]),
            "obsolete": int(row["obsolete"]), "oldest_ready_age_seconds": _age(now, row["oldest_ready_at"])}


def _event_metrics(conn: Connection, now: float) -> dict[str, Any]:
    row = require_row(conn.execute(
        """SELECT COUNT(*) AS pending,
                  COUNT(*) FILTER (WHERE next_attempt_at<=%s) AS due,
                  MIN(occurred_at) AS oldest_pending_at,
                  (SELECT next_attempt_at FROM event_outbox WHERE delivered_at IS NULL ORDER BY id LIMIT 1) AS head_due_at
             FROM event_outbox WHERE delivered_at IS NULL""", (now,),
    ))
    return {"pending": int(row["pending"]), "due": int(row["due"]),
            "oldest_pending_age_seconds": _age(now, row["oldest_pending_at"]),
            "head_retry_delay_seconds": 0.0 if row["head_due_at"] is None else max(0.0, float(row["head_due_at"]) - now)}


def _period_usage(conn: Connection, now: float) -> dict[str, int]:
    start, end = monthly_period(int(now))
    row = require_row(conn.execute(
        """SELECT COALESCE(SUM(games),0) AS games,
                  COALESCE(SUM(normalization_units),0) AS normalization_units,
                  COALESCE(SUM(remote_bytes),0) AS remote_bytes,
                  COALESCE(SUM(remote_requests),0) AS remote_requests
             FROM workspace_budget_periods WHERE period_start=%s""", (start,),
    ))
    return {"period_start": start, "period_end": end, **{name: int(value) for name, value in row.items()}}
