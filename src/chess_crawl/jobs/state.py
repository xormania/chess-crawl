"""Single owner of durable job and crawl-run state."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Mapping
from typing import Any

from chess_crawl.jobs.models import DiscoveryJob, EnqueueResult, JOB_KINDS, JobKind, JobState, PROCESSING_JOB_KINDS
from chess_crawl.jobs.locking import ExecutorLease, parallel_executor_lock
from chess_crawl.jobs.settings import WorkerSettings
from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.storage.db import (
    Connection, Row, atomic, consistent_read, require_row, operation_lock, lock_key,
    acquire_lock_key, release_lock_key, owns_lock_key,
)
from chess_crawl.storage.discovery import discovery_edge_count


LIVE_STATES = ("pending", "in_progress", "blocked")
TERMINAL_STATES = ("done", "error", "skipped")
WORKER_HISTORY_SECONDS = 86400
WORKER_SNAPSHOT_LIMIT = 128
WORKER_PRUNE_LIMIT = 256
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
    conn: Connection,
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
    operation_lock(conn, "job-dedup", dedup)
    existing = conn.execute(
        """
        SELECT id FROM discovery_jobs
         WHERE dedup_key = %s AND state IN ('pending','in_progress','blocked')
         ORDER BY id LIMIT 1
        """,
        (dedup,),
    ).fetchone()
    if existing is not None:
        return EnqueueResult(job_id=int(existing["id"]), inserted=False)

    inherited_budget = conn._work_budget_id
    if crawl_run_id is not None:
        budget_owner = conn.execute("SELECT work_budget_id FROM crawl_runs WHERE id=%s", (crawl_run_id,)).fetchone()
        inherited_budget = budget_owner[0] if budget_owner is not None else None
    elif parent_job_id is not None:
        budget_owner = conn.execute("SELECT work_budget_id FROM discovery_jobs WHERE id=%s", (parent_job_id,)).fetchone()
        inherited_budget = budget_owner[0] if budget_owner is not None else None
    if inherited_budget is not None:
        from chess_crawl.storage.work_budgets import require_backlog_room
        require_backlog_room(conn, int(inherited_budget))

    cursor = conn.execute(
        """
        INSERT INTO discovery_jobs(
          crawl_run_id, parent_job_id, provider, kind, target, params_json,
          state, priority, depth, attempts, dedup_key, enqueued_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, 'pending', %s, %s, 0, %s, %s)
        RETURNING id
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
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("job insert did not return a row id")
    return EnqueueResult(job_id=int(row["id"]), inserted=True)


def _validate_schedulable_job(*, provider: str, kind: str) -> None:
    if kind not in JOB_KINDS:
        raise ValueError(f"unsupported job kind: {kind}")
    if kind == "fetch_user_stats" and provider != "chess.com":
        raise ValueError("fetch_user_stats jobs are supported only for chess.com")
    if kind == "fetch_game_by_id" and provider != "lichess":
        raise ValueError("fetch_game_by_id jobs are supported only for lichess")


@atomic
def claim_next_job(
    conn: Connection, *, crawl_run_id: int | None = None,
    now: float | None = None, worker_id: str | None = None,
    job_id: int | None = None, stage: str = "all",
    budget_policy: BudgetPolicy | None = None,
    respect_fairness: bool = False,
) -> DiscoveryJob | None:
    """Claim row and session ownership together; duplicate delivery is harmless."""
    if stage not in {"all", "acquisition", "processing"}:
        raise ValueError("stage must be all, acquisition, or processing")
    timestamp = time.time() if now is None else now
    policy = budget_policy or BudgetPolicy.from_env()
    # Only admission is serialized; session ownership still spans each job.
    # This short gate makes active limits and workspace turns race-safe.
    operation_lock(conn, "workspace-scheduler", "claim")
    processing = PROCESSING_JOB_KINDS
    excluded_providers: list[str] = []
    while True:
        exclusions_before = len(excluded_providers)
        candidates = conn.execute(
            f"""SELECT id,provider,kind,workspace_id FROM discovery_jobs
                 WHERE (state='pending' OR (state='blocked' AND next_attempt_at IS NOT NULL))
                   AND (next_attempt_at IS NULL OR next_attempt_at<=%s)
                   AND (%s::bigint IS NULL OR crawl_run_id=%s)
                   AND (%s::bigint IS NULL OR id=%s)
                   AND (%s='all' OR (%s='processing' AND kind=ANY(%s))
                        OR (%s='acquisition' AND NOT kind=ANY(%s)))
                   AND (kind=ANY(%s) OR NOT EXISTS(SELECT 1 FROM provider_cooldowns p
                           WHERE p.provider=discovery_jobs.provider AND p.not_before>%s))
                   AND (kind=ANY(%s) OR NOT provider=ANY(%s))
                   AND (SELECT COUNT(*) FROM discovery_jobs active
                         WHERE active.workspace_id=discovery_jobs.workspace_id AND active.state='in_progress')
                       < COALESCE((SELECT (p.policy->>'workspace_max_active_jobs')::bigint
                           FROM workspace_budget_policies p WHERE p.workspace_id=discovery_jobs.workspace_id),%s)
                   AND {_RUN_ALLOWS_WORK}
                 ORDER BY COALESCE((SELECT t.turn FROM workspace_claim_turns t
                     WHERE t.workspace_id=discovery_jobs.workspace_id AND t.stage=%s),0),
                     priority,depth,enqueued_at NULLS FIRST,id
                 LIMIT 100 FOR UPDATE SKIP LOCKED""",  # nosec B608 # Fixed cancellation SQL only.
            (timestamp, crawl_run_id, crawl_run_id, None if respect_fairness else job_id, None if respect_fairness else job_id,
             stage, stage, list(processing), stage, list(processing), list(processing), timestamp, list(processing), excluded_providers,
             policy.workspace_max_active_jobs, stage),
        ).fetchall()
        if not candidates:
            return None
        for candidate in candidates:
            keys: list[int] = []
            if worker_id is not None:
                if candidate["kind"] not in processing:
                    provider_key = lock_key("acquisition-provider", candidate["provider"])
                    if candidate["provider"] in excluded_providers:
                        continue
                    if not acquire_lock_key(conn, provider_key):
                        excluded_providers.append(candidate["provider"])
                        continue
                    keys.append(provider_key)
                job_key = lock_key("executor-job", int(candidate["id"]))
                if not acquire_lock_key(conn, job_key):
                    for key in keys:
                        release_lock_key(conn, key)
                    continue
                keys.append(job_key)
            if respect_fairness and job_id is not None and int(candidate["id"]) != job_id:
                for key in reversed(keys):
                    release_lock_key(conn, key)
                return None
            token = uuid.uuid4().hex if worker_id is not None else None
            try:
                with conn.transaction():
                    conn.execute(
                        """UPDATE discovery_jobs SET state='in_progress', started_at=%s,
                             attempts=attempts+1,reason=NULL,done_at=NULL,next_attempt_at=NULL,
                             owner_worker_id=%s,owner_backend_pid=CASE WHEN %s::text IS NULL THEN NULL ELSE pg_backend_pid() END,
                             ownership_token=%s,ownership_generation=ownership_generation+1 WHERE id=%s""",
                        (int(timestamp), worker_id, worker_id, token, candidate["id"]),
                    )
                    job = get_job(conn, int(candidate["id"]))
                    conn.execute(
                        """INSERT INTO workspace_claim_turns(workspace_id,stage,turn) VALUES(%s,%s,nextval('workspace_claim_turn'))
                             ON CONFLICT(workspace_id,stage) DO UPDATE SET turn=EXCLUDED.turn""",
                        (candidate["workspace_id"], stage),
                    )
            except BaseException:
                for key in keys:
                    release_lock_key(conn, key)
                raise
            if worker_id is not None and token is not None:
                conn._ownership_keys = tuple(keys)
                conn._job_fence = (int(candidate["id"]), token)
            return job
        if len(excluded_providers) == exclusions_before:
            return None


def release_job_ownership(conn: Connection) -> None:
    """Clear local fencing before the next job, releasing all session locks."""
    keys = conn._ownership_keys
    conn._job_fence = None
    conn._ownership_keys = ()
    if not conn.closed:
        for key in reversed(keys):
            release_lock_key(conn, key)


@atomic
def mark_job(
    conn: Connection,
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
           SET state = %s,
               done_at = %s,
               next_attempt_at = NULL,
               reason = %s,
               enqueued_at = CASE WHEN %s = 'pending'
                                  THEN GREATEST(enqueued_at, %s)
                                  ELSE enqueued_at END
         WHERE id = %s
        """,
        (state, done_at, reason, state, timestamp, job_id),
    )


def mark_done(conn: Connection, job_id: int, *, reason: str | None = None) -> None:
    mark_job(conn, job_id, "done", reason=reason)


def mark_error(conn: Connection, job_id: int, *, reason: str) -> None:
    mark_job(conn, job_id, "error", reason=reason)


def mark_skipped(conn: Connection, job_id: int, *, reason: str) -> None:
    mark_job(conn, job_id, "skipped", reason=reason)


def mark_blocked(conn: Connection, job_id: int, *, reason: str) -> None:
    mark_job(conn, job_id, "blocked", reason=reason)


@atomic
def update_job_params(
    conn: Connection,
    job_id: int,
    params: Mapping[str, Any],
) -> None:
    conn.execute(
        "UPDATE discovery_jobs SET params_json = %s WHERE id = %s",
        (canonical_params(params), job_id),
    )


def resume_stale_in_progress(
    conn: Connection, *, crawl_run_id: int | None = None,
    stale_seconds: int = 0, now: int | None = None,
    lease: ExecutorLease | None = None,
) -> int:
    # Legacy explicit archive owners retain their gate. New workers recover
    # each orphan only after obtaining that job's session lock.
    with parallel_executor_lock(conn, lease=lease):
        return _resume_stale_in_progress(conn, crawl_run_id=crawl_run_id,
                                        stale_seconds=stale_seconds, now=now)


@atomic
def _resume_stale_in_progress(
    conn: Connection, *, crawl_run_id: int | None, stale_seconds: int, now: int | None,
) -> int:
    timestamp = int(time.time()) if now is None else now
    cutoff = timestamp - stale_seconds
    rows = conn.execute(
        f"""SELECT id,owner_backend_pid,ownership_token FROM discovery_jobs WHERE state='in_progress'
             AND (%s::bigint IS NULL OR crawl_run_id=%s) AND {_RUN_ALLOWS_WORK}
             AND (%s=0 OR started_at IS NULL OR started_at<=%s)
             ORDER BY id FOR UPDATE SKIP LOCKED""",  # nosec B608 # Fixed cancellation SQL only.
        (crawl_run_id,crawl_run_id,stale_seconds,cutoff),
    ).fetchall()
    resumed = 0
    for row in rows:
        if (conn._job_fence == (int(row["id"]), row["ownership_token"])
                and owns_lock_key(conn, lock_key("executor-job", int(row["id"])))):
            continue
        key = lock_key("executor-job", int(row["id"]))
        if not acquire_lock_key(conn, key):
            continue
        try:
            conn.execute(
                """UPDATE discovery_jobs SET state='pending',started_at=NULL,done_at=NULL,
                     ownership_token=NULL,owner_worker_id=NULL,owner_backend_pid=NULL,
                     reason=COALESCE(reason,'resumed orphaned job') WHERE id=%s""", (row["id"],),
            )
            resumed += 1
        finally:
            release_lock_key(conn,key)
    return resumed


@atomic
def unblock_jobs(conn: Connection, *, crawl_run_id: int | None = None, now: float | None = None) -> int:
    timestamp = time.time() if now is None else now
    cursor = conn.execute(
        f"""
        UPDATE discovery_jobs
           SET state = 'pending',
               started_at = NULL,
               done_at = NULL,
               reason = COALESCE(reason, 'unblocked by jobs resume')
         WHERE state = 'blocked'
           AND (next_attempt_at IS NULL OR next_attempt_at <= %s)
           AND (%s::bigint IS NULL OR crawl_run_id = %s)
           AND {_RUN_ALLOWS_WORK}
        """,  # nosec B608 # Only the fixed _RUN_ALLOWS_WORK fragment is interpolated; values are bound.
        (timestamp, crawl_run_id, crawl_run_id),
    )
    return int(cursor.rowcount)


@atomic
def create_crawl_run(
    conn: Connection,
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
        VALUES (%s, %s, %s, 'running', '{}', %s, %s)
        RETURNING id
        """,
        (seed_spec, provider, canonical_params(params), timestamp, timestamp),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("crawl run insert did not return a row id")
    return int(row["id"])


@atomic
def update_crawl_run(
    conn: Connection,
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
           SET status = COALESCE(%s, status),
               counters = COALESCE(%s, counters),
               updated_at = %s,
               finished_at = CASE
                 WHEN %s THEN COALESCE(finished_at, %s)
                 WHEN %s IN ('running', 'paused') THEN NULL
                 ELSE finished_at
               END
         WHERE id = %s
        """,
        (
            status,
            None if counters is None else canonical_params(counters),
            timestamp,
            finished,
            timestamp,
            status,
            crawl_run_id,
        ),
    )


@atomic
def create_crawl_run_with_root_job(
    conn: Connection,
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
    conn: Connection,
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


def run_counters(conn: Connection, crawl_run_id: int) -> dict[str, int]:
    counters = {
        "jobs_total": total_jobs_for_run(conn, crawl_run_id),
        "users_seen": crawl_user_count(conn, crawl_run_id),
        "edges": discovery_edge_count(conn, crawl_run_id),
    }
    for row in job_state_counts(conn, crawl_run_id=crawl_run_id):
        counters[f"jobs_{row['state']}"] = int(row["count"])
    return counters


@atomic
def refresh_run_status(conn: Connection, crawl_run_id: int) -> None:
    """Derive run state from its jobs without reopening an explicit cancellation."""
    run = conn.execute("SELECT status FROM crawl_runs WHERE id = %s FOR UPDATE", (crawl_run_id,)).fetchone()
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
def refresh_crawl_runs(conn: Connection, *, crawl_run_id: int | None = None) -> None:
    if crawl_run_id is not None:
        refresh_run_status(conn, crawl_run_id)
    else:
        for row in crawl_runs(conn):
            refresh_run_status(conn, int(row["id"]))


@atomic
def finish_attempt(
    conn: Connection,
    job_id: int,
    outcome: JobState,
    *,
    reason: str,
    transient: bool = False,
    retry_after: float | None = None,
    minimum_delay: float = 0,
    settings: WorkerSettings | None = None,
    now: float | None = None,
) -> JobState:
    """Finish an attempt and atomically publish its run's current snapshot."""
    policy = settings or WorkerSettings()
    timestamp = time.time() if now is None else now
    row = conn.execute("SELECT crawl_run_id, retry_count, provider FROM discovery_jobs WHERE id = %s", (job_id,)).fetchone()
    if row is None:
        raise KeyError(f"Job not found: {job_id}")
    result = outcome
    if transient:
        retries = int(row["retry_count"])
        delay = min(policy.job_retry_max_s, policy.job_retry_base_s * (2 ** min(retries, 30)))
        # Persist provider-wide backoff even when this job exhausts its retries.
        # Another job or a restarted executor must not evade the same deadline.
        delay = max(delay, minimum_delay, retry_after or 0)
        defer_provider(conn, row["provider"], not_before=timestamp + delay, reason=reason, now=timestamp)
        if retries < policy.job_max_retries:
            conn.execute(
                """UPDATE discovery_jobs SET state = 'blocked', done_at = NULL,
                          reason = %s, retry_count = retry_count + 1, next_attempt_at = %s
                     WHERE id = %s""",
                (reason, timestamp + delay, job_id),
            )
            result = "blocked"
        else:
            result = "error"
            mark_job(conn, job_id, result, reason=f"Retry limit reached: {reason}", now=int(timestamp))
    else:
        mark_job(conn, job_id, result, reason=reason, now=int(timestamp))
    if row["crawl_run_id"] is not None:
        refresh_run_status(conn, int(row["crawl_run_id"]))
    return result


def provider_ready_at(conn: Connection, provider: str) -> float | None:
    row = conn.execute("SELECT not_before FROM provider_cooldowns WHERE provider = %s", (provider,)).fetchone()
    return None if row is None else float(row["not_before"])


def next_pacing_deadline(conn: Connection, *, crawl_run_id: int | None, now: float) -> float | None:
    row = require_row(conn.execute(
        """SELECT MIN(p.not_before) FROM provider_cooldowns p
             WHERE p.reason='provider request pacing' AND p.not_before>%s
               AND EXISTS(SELECT 1 FROM discovery_jobs j WHERE j.provider=p.provider AND j.state='pending'
                           AND (%s::bigint IS NULL OR j.crawl_run_id=%s))""",
        (now, crawl_run_id, crawl_run_id),
    ))
    return None if row[0] is None else float(row[0])


@atomic
def defer_provider(
    conn: Connection, provider: str, *, not_before: float,
    reason: str, now: float | None = None,
) -> None:
    timestamp = time.time() if now is None else now
    conn.execute(
        """INSERT INTO provider_cooldowns(provider, not_before, reason, updated_at)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT(provider) DO UPDATE SET
             not_before=GREATEST(provider_cooldowns.not_before, excluded.not_before),
             reason=excluded.reason, updated_at=excluded.updated_at""",
        (provider, not_before, reason, timestamp),
    )


def get_run(conn: Connection, run_id: int) -> Row | None:
    return conn.execute("SELECT * FROM crawl_runs WHERE id = %s", (run_id,)).fetchone()


def job_ids_for_run(conn: Connection, run_id: int) -> list[int]:
    return [int(row["id"]) for row in conn.execute("SELECT id FROM discovery_jobs WHERE crawl_run_id = %s ORDER BY id", (run_id,))]


@atomic
def start_worker(
    conn: Connection, worker_id: str, *, lease: ExecutorLease,
    max_age: float, now: float | None = None,
) -> None:
    lease.require(conn)
    timestamp = time.time() if now is None else now
    conn.execute(
        """INSERT INTO executor_heartbeats(worker_id,started_at,heartbeat_at,heartbeat_expires_at,status)
           VALUES(%s,%s,%s,%s,'running') ON CONFLICT(worker_id) DO UPDATE SET
           started_at=excluded.started_at,heartbeat_at=excluded.heartbeat_at,
           heartbeat_expires_at=excluded.heartbeat_expires_at,stopped_at=NULL,status='running'""",
        (worker_id,timestamp,timestamp,timestamp+max_age),
    )
    _prune_worker_heartbeats(conn, timestamp)


@atomic
def heartbeat_worker(
    conn: Connection, worker_id: str, *, max_age: float,
    current_job_id: int | None = None, stopping: bool = False, now: float | None = None,
) -> bool:
    timestamp = time.time() if now is None else now
    cursor = conn.execute(
        """UPDATE executor_heartbeats SET heartbeat_at=%s,heartbeat_expires_at=%s,
             current_job_id=%s,status=%s WHERE worker_id=%s AND status IN ('running','stopping')""",
        (timestamp,timestamp+max_age,current_job_id,'stopping' if stopping else 'running',worker_id),
    )
    owned = cursor.rowcount == 1
    _prune_worker_heartbeats(conn, timestamp)
    return owned


@atomic
def stop_worker(conn: Connection, worker_id: str, *, failed: bool = False, now: float | None = None) -> None:
    timestamp = time.time() if now is None else now
    conn.execute(
        """UPDATE executor_heartbeats SET status=%s,heartbeat_at=%s,heartbeat_expires_at=%s,
             stopped_at=%s,current_job_id=NULL WHERE worker_id=%s""",
        ('failed' if failed else 'stopped',timestamp,timestamp,timestamp,worker_id),
    )
    _prune_worker_heartbeats(conn, timestamp)


def _prune_worker_heartbeats(conn: Connection, timestamp: float) -> int:
    cursor = conn.execute(
        """WITH expired AS (
             SELECT worker_id FROM executor_heartbeats
              WHERE status IN ('stopped','failed') AND heartbeat_at<%s AND heartbeat_expires_at<=%s
              ORDER BY heartbeat_at,worker_id LIMIT %s FOR UPDATE SKIP LOCKED
           ) DELETE FROM executor_heartbeats h USING expired e WHERE h.worker_id=e.worker_id""",
        (timestamp-WORKER_HISTORY_SECONDS, timestamp, WORKER_PRUNE_LIMIT),
    )
    return cursor.rowcount


@atomic
def prune_worker_heartbeats(conn: Connection, *, now: float | None = None) -> int:
    """Remove a finite batch of expired terminal rows; liveness is not ownership."""
    return _prune_worker_heartbeats(conn, time.time() if now is None else now)


@consistent_read
def worker_alive(conn: Connection, worker_id: str, *, now: float | None = None) -> bool:
    """Probe one incarnation; another worker cannot supply its liveness."""
    timestamp = time.time() if now is None else now
    return bool(require_row(conn.execute(
        """SELECT EXISTS(SELECT 1 FROM executor_heartbeats WHERE worker_id=%s
             AND status IN ('running','stopping') AND heartbeat_expires_at>%s)""",
        (worker_id, timestamp),
    ))[0])


@consistent_read
def worker_status(conn: Connection, *, now: float | None = None, max_age: float | None = None) -> dict[str, Any]:
    """Count all live workers while returning a finite, live-first history."""
    timestamp = time.time() if now is None else now
    result = require_row(conn.execute(
        """WITH active AS NOT MATERIALIZED (
             SELECT * FROM executor_heartbeats
              WHERE status IN ('running','stopping') AND heartbeat_expires_at>%s
                AND (%s::double precision IS NULL OR heartbeat_at>%s-%s::double precision)
           ), candidates AS (
             (SELECT true AS alive_priority,h.* FROM active h
               ORDER BY heartbeat_at DESC,worker_id LIMIT %s)
             UNION ALL
             (SELECT false AS alive_priority,h.* FROM executor_heartbeats h
               WHERE heartbeat_at>=%s AND NOT (
                 status IN ('running','stopping') AND heartbeat_expires_at>%s
                 AND (%s::double precision IS NULL OR heartbeat_at>%s-%s::double precision))
               ORDER BY heartbeat_at DESC,worker_id LIMIT %s)
           ), bounded AS (
             SELECT * FROM candidates ORDER BY alive_priority DESC,heartbeat_at DESC,worker_id LIMIT %s
           ) SELECT (SELECT COUNT(*) FROM active) AS active_workers,
             COALESCE((SELECT jsonb_agg(to_jsonb(b)
               ORDER BY alive_priority DESC,heartbeat_at DESC,worker_id) FROM bounded b),'[]'::jsonb) AS workers""",
        (timestamp,max_age,timestamp,max_age,WORKER_SNAPSHOT_LIMIT+1,timestamp-WORKER_HISTORY_SECONDS,
         timestamp,max_age,timestamp,max_age,WORKER_SNAPSHOT_LIMIT+1,WORKER_SNAPSHOT_LIMIT+1),
    ))
    rows = result['workers']
    metadata = {'active_workers':int(result['active_workers']), 'worker_limit':WORKER_SNAPSHOT_LIMIT,
                'workers_truncated':len(rows)>WORKER_SNAPSHOT_LIMIT}
    if not rows:
        return {"alive":False,"status":"absent","worker_id":None,"heartbeat_at":None,"age_seconds":None,
                **metadata,"workers":[]}
    workers = []
    for row in rows[:WORKER_SNAPSHOT_LIMIT]:
        worker = dict(row)
        worker['age_seconds'] = max(0.0,timestamp-float(row['heartbeat_at']))
        worker['alive'] = worker.pop('alive_priority')
        workers.append(worker)
    return {**workers[0],**metadata,'workers':workers}


def get_job(conn: Connection, job_id: int) -> DiscoveryJob | None:
    row = conn.execute("SELECT * FROM discovery_jobs WHERE id = %s", (job_id,)).fetchone()
    return None if row is None else row_to_job(row)


def list_jobs(conn: Connection, *, limit: int = 100) -> list[Row]:
    return list(
        conn.execute(
            """
            SELECT id, crawl_run_id, parent_job_id, provider, kind, target, state,
                   priority, depth, attempts, retry_count, next_attempt_at, enqueued_at, started_at, done_at, reason
              FROM discovery_jobs
             ORDER BY id
             LIMIT %s
            """,
            (limit,),
        )
    )


def crawl_runs(conn: Connection) -> list[Row]:
    return list(conn.execute("SELECT * FROM crawl_runs ORDER BY id"))


def job_state_counts(conn: Connection, *, crawl_run_id: int | None = None) -> list[Row]:
    return list(
        conn.execute(
            """
            SELECT state, COUNT(*) AS count
              FROM discovery_jobs
             WHERE (%s::bigint IS NULL OR crawl_run_id = %s)
             GROUP BY state
             ORDER BY state COLLATE "C"
            """,
            (crawl_run_id, crawl_run_id),
        )
    )


def job_kind_state_counts(conn: Connection, *, crawl_run_id: int | None = None) -> list[Row]:
    return list(
        conn.execute(
            """
            SELECT kind, state, depth, COUNT(*) AS count
              FROM discovery_jobs
             WHERE (%s::bigint IS NULL OR crawl_run_id = %s)
             GROUP BY kind, state, depth
             ORDER BY depth, kind COLLATE "C", state COLLATE "C"
            """,
            (crawl_run_id, crawl_run_id),
        )
    )


def total_jobs_for_run(conn: Connection, crawl_run_id: int) -> int:
    return int(
        require_row(conn.execute(
            "SELECT COUNT(*) FROM discovery_jobs WHERE crawl_run_id = %s",
            (crawl_run_id,),
        ))[0]
    )


def crawl_user_count(conn: Connection, crawl_run_id: int) -> int:
    return int(
        require_row(conn.execute(
            """
            SELECT COUNT(DISTINCT lower(target))
              FROM discovery_jobs
             WHERE crawl_run_id = %s AND kind = 'crawl_opponents'
            """,
            (crawl_run_id,),
        ))[0]
    )


def known_crawl_depth(
    conn: Connection,
    *,
    crawl_run_id: int,
    provider: str,
    username: str,
) -> int | None:
    row = conn.execute(
        """
        SELECT MIN(depth) AS depth
          FROM discovery_jobs
         WHERE crawl_run_id = %s
           AND provider = %s
           AND kind = 'crawl_opponents'
           AND lower(target) = lower(%s)
        """,
        (crawl_run_id, provider, username),
    ).fetchone()
    if row is None or row["depth"] is None:
        return None
    return int(row["depth"])


def row_to_job(row: Row) -> DiscoveryJob:
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
        retry_count=int(row["retry_count"]),
        next_attempt_at=row["next_attempt_at"],
        revision=int(row["revision"]) if "revision" in row.keys() else 0,
        ownership_token=row["ownership_token"] if "ownership_token" in row else None,
        owner_worker_id=row["owner_worker_id"] if "owner_worker_id" in row else None,
        ownership_generation=int(row["ownership_generation"]) if "ownership_generation" in row else 0,
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
