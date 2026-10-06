"""Shared durable candidate admission for scheduling and aggregate measurement.

Expressions use the discovery_jobs table name and bound parameters. They describe
database eligibility, before the scheduler attempts row and session ownership.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from chess_crawl.jobs.models import PROCESSING_JOB_KINDS


RUN_ALLOWS_WORK_SQL = """NOT EXISTS (
    SELECT 1 FROM crawl_runs
     WHERE crawl_runs.id = discovery_jobs.crawl_run_id
       AND crawl_runs.status = 'cancelled'
)"""
PROCESSING_READY_SQL = """(NOT (
    discovery_jobs.kind IN ('fetch_user_games','crawl_opponents')
    AND COALESCE(discovery_jobs.params_json::jsonb->>'collection_mode','bounded')='bounded'
    AND EXISTS (
        SELECT 1 FROM discovery_jobs child
         WHERE child.parent_job_id=discovery_jobs.id AND child.kind='normalize_payload'
           AND child.state IN ('pending','in_progress','blocked')
    )
) AND (discovery_jobs.kind<>'expand_opponents' OR EXISTS (
    SELECT 1 FROM discovery_jobs parent
     WHERE parent.id=discovery_jobs.parent_job_id AND parent.state IN ('done','error','skipped')
)))"""
SCHEDULABLE_STATE_SQL = """(discovery_jobs.state='pending' OR
    (discovery_jobs.state='blocked' AND discovery_jobs.next_attempt_at IS NOT NULL))"""


@dataclass(frozen=True)
class AdmissionCondition:
    waiting_reason: str
    sql: str
    parameters: tuple[Any, ...] = ()


def admission_conditions(*, now: float, workspace_max_active_jobs: int) -> tuple[AdmissionCondition, ...]:
    """Return checks in the same order used to attribute waiting work.

    A job may have several obstacles; attribution counts it once, at its first
    failing condition. Budget reservations are checked during execution, so only
    already recorded indefinite pauses are identified as budget blocking.
    """
    return (
        AdmissionCondition("cancelled", RUN_ALLOWS_WORK_SQL),
        AdmissionCondition("paused", SCHEDULABLE_STATE_SQL),
        AdmissionCondition("retry", "(discovery_jobs.next_attempt_at IS NULL OR discovery_jobs.next_attempt_at<=%s)", (now,)),
        AdmissionCondition("processing_dependency", PROCESSING_READY_SQL),
        AdmissionCondition("provider_cooldown", """(discovery_jobs.kind=ANY(%s) OR NOT EXISTS (
            SELECT 1 FROM provider_cooldowns p
             WHERE p.provider=discovery_jobs.provider AND p.not_before>%s
        ))""", (list(PROCESSING_JOB_KINDS), now)),
        AdmissionCondition("workspace_active_cap", """(SELECT COUNT(*) FROM discovery_jobs active
            WHERE active.workspace_id=discovery_jobs.workspace_id AND active.state='in_progress')
            < COALESCE((SELECT (p.policy->>'workspace_max_active_jobs')::bigint
                FROM workspace_budget_policies p WHERE p.workspace_id=discovery_jobs.workspace_id),%s)""",
            (workspace_max_active_jobs,)),
    )


def claim_admission(*, now: float, workspace_max_active_jobs: int) -> tuple[str, tuple[Any, ...]]:
    conditions = admission_conditions(now=now, workspace_max_active_jobs=workspace_max_active_jobs)
    return " AND ".join(f"({condition.sql})" for condition in conditions), tuple(
        parameter for condition in conditions for parameter in condition.parameters
    )


def waiting_reason(*, now: float, workspace_max_active_jobs: int) -> tuple[str, tuple[Any, ...]]:
    """A finite label, without job targets, workspace identifiers, or error text."""
    conditions = admission_conditions(now=now, workspace_max_active_jobs=workspace_max_active_jobs)
    clauses = " ".join(f"WHEN NOT ({condition.sql}) THEN '{condition.waiting_reason}'" for condition in conditions)
    return f"CASE {clauses} ELSE 'eligible' END", tuple(
        parameter for condition in conditions for parameter in condition.parameters
    )
