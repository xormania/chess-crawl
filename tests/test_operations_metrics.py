"""Autoscaling signals reflect durable admission and survive executor loss."""
from __future__ import annotations

import json

import pytest

from chess_crawl import metrics_admin
from chess_crawl.jobs import state
from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.storage.db import connection, require_row, transaction
from chess_crawl.storage.job_admission import claim_admission
from chess_crawl.storage import operations_metrics
from chess_crawl.storage.operations_metrics import operational_snapshot
from chess_crawl.storage.work_budgets import install_workspace_policy, monthly_period
from chess_crawl.storage.workspaces import submission_context


NOW = 1_700_000_000


def _enqueue(conn, *, workspace="alpha", kind="fetch_user_profile", provider="chess.com", target="private-player", age=20,
             parent_job_id=None, crawl_run_id=None, params=None):
    with transaction(conn):
        submission_context(conn, workspace)
        return state.enqueue_job(conn, provider=provider, kind=kind, target=target, params=params, now=NOW-age,
                                 parent_job_id=parent_job_id, crawl_run_id=crawl_run_id).job_id


def test_empty_snapshot_is_explicit_and_does_not_create_delivery_work(initialized_conn) -> None:
    snapshot = operational_snapshot(initialized_conn, now=NOW)
    assert snapshot["schema_version"] == 1
    for stage in snapshot["stages"].values():
        assert stage["eligible_jobs"] == stage["active_jobs"] == 0
        assert stage["oldest_eligible_age_seconds"] is None
        assert not any(stage["waiting"].values())
    assert snapshot["outboxes"]["dispatch"]["pending"] == 0
    assert snapshot["outboxes"]["events"]["pending"] == 0
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM workspaces"))[0] == 1


def test_eligible_age_and_wait_reasons_match_shared_admission_without_identifiers(initialized_conn) -> None:
    _enqueue(initialized_conn, age=30)
    _enqueue(initialized_conn, workspace="cooldown", provider="lichess", age=40)
    _enqueue(initialized_conn, kind="normalize_payload", provider="lichess", target="101", age=50)
    _enqueue(initialized_conn, kind="normalize_payload", provider="lichess", target="102", age=-10)
    retry = _enqueue(initialized_conn, workspace="retry", target="retry-player")
    budget = _enqueue(initialized_conn, workspace="budget", target="budget-player")
    paused = _enqueue(initialized_conn, workspace="paused", target="paused-player")
    _enqueue(initialized_conn, workspace="capped", target="capped-player")
    active = _enqueue(initialized_conn, workspace="capped", kind="normalize_payload", target="103")
    with transaction(initialized_conn):
        submission_context(initialized_conn, "cancelled")
        cancelled_run = state.create_crawl_run(initialized_conn, provider="chess.com", seed_spec="cancelled-player", params={})
    _enqueue(initialized_conn, workspace="cancelled", crawl_run_id=cancelled_run)
    state.update_crawl_run(initialized_conn, cancelled_run, status="cancelled", now=NOW)
    state.mark_blocked(initialized_conn, budget, reason="budget_exhausted: remote_requests")
    state.mark_blocked(initialized_conn, paused, reason="unavailable private-player resource")
    state.mark_blocked(initialized_conn, retry, reason="retry later")
    state.mark_job(initialized_conn, active, "in_progress", now=NOW)
    install_workspace_policy(initialized_conn, "capped", BudgetPolicy(workspace_max_active_jobs=1), now=NOW)
    state.defer_provider(initialized_conn, "lichess", not_before=NOW+60, reason="429", now=NOW)
    with transaction(initialized_conn):
        initialized_conn.execute("UPDATE discovery_jobs SET next_attempt_at=%s WHERE id=%s", (NOW+10, retry))
    before = [dict(row) for row in initialized_conn.execute("SELECT * FROM discovery_jobs ORDER BY id")]
    snapshot = operational_snapshot(initialized_conn, now=NOW)
    acquisition, processing = snapshot["stages"]["acquisition"], snapshot["stages"]["processing"]
    assert acquisition["eligible_jobs"] == 1
    assert acquisition["oldest_eligible_age_seconds"] == 30
    assert acquisition["waiting"] == {"budget": 1, "paused": 1, "retry": 1, "processing_dependency": 0,
                                       "provider_cooldown": 1, "workspace_active_cap": 1, "cancelled": 1}
    assert processing["eligible_jobs"] == 2 and processing["active_jobs"] == 1
    assert processing["oldest_eligible_age_seconds"] == 50
    admission_sql, params = claim_admission(now=NOW, workspace_max_active_jobs=2)
    eligible = require_row(initialized_conn.execute(f"SELECT COUNT(*) FROM discovery_jobs WHERE {admission_sql}", params))[0]
    assert eligible == acquisition["eligible_jobs"] + processing["eligible_jobs"]
    rendered = json.dumps(snapshot)
    for private_value in ("private-player", "capped-player", "alpha", "lichess", "workspace_id", "job_id", "reason"):
        assert private_value not in rendered
    assert [dict(row) for row in initialized_conn.execute("SELECT * FROM discovery_jobs ORDER BY id")] == before


def test_waiting_normalization_does_not_inflate_acquisition_scaling_signal(initialized_conn) -> None:
    parent = _enqueue(initialized_conn, kind="fetch_user_games")
    child = _enqueue(initialized_conn, kind="normalize_payload", target="101", parent_job_id=parent)
    snapshot = operational_snapshot(initialized_conn, now=NOW)
    assert snapshot["stages"]["acquisition"]["eligible_jobs"] == 0
    assert snapshot["stages"]["acquisition"]["waiting"]["processing_dependency"] == 1
    assert snapshot["stages"]["processing"]["eligible_jobs"] == 1
    state.mark_done(initialized_conn, child)
    assert operational_snapshot(initialized_conn, now=NOW)["stages"]["acquisition"]["eligible_jobs"] == 1


def test_full_collection_can_pipeline_capture_while_normalization_waits(initialized_conn) -> None:
    parent = _enqueue(initialized_conn, kind="fetch_user_games", params={"collection_mode": "full"})
    _enqueue(initialized_conn, kind="normalize_payload", target="101", parent_job_id=parent)
    snapshot = operational_snapshot(initialized_conn, now=NOW)
    assert snapshot["stages"]["acquisition"]["eligible_jobs"] == 1
    assert snapshot["stages"]["acquisition"]["waiting"]["processing_dependency"] == 0


def test_separate_observer_does_not_steal_live_ownership_and_reports_recovery(database_url) -> None:
    with connection(database_url, mode="rw") as setup:
        first = _enqueue(setup)
        _enqueue(setup, workspace="beta", target="another-private-player")
    with connection(database_url, mode="rw") as owner, connection(database_url, mode="rw") as observer:
        claimed = state.claim_next_job(owner, worker_id="owner", now=NOW, stage="acquisition")
        assert claimed is not None and claimed.id == first
        snapshot = operational_snapshot(observer, now=NOW)
        assert snapshot["stages"]["acquisition"]["active_jobs"] == 1
        assert snapshot["stages"]["acquisition"]["eligible_jobs"] == 1
        assert state.claim_next_job(observer, worker_id="observer", now=NOW, stage="acquisition") is None
        assert state.resume_stale_in_progress(observer, stale_seconds=0, now=NOW+100) == 0
    with connection(database_url, mode="rw") as recovery:
        assert state.resume_stale_in_progress(recovery, stale_seconds=0, now=NOW+100) == 1
        snapshot = operational_snapshot(recovery, now=NOW+100)
        assert snapshot["stages"]["acquisition"]["active_jobs"] == 0
        assert snapshot["stages"]["acquisition"]["eligible_jobs"] == 2


def test_dispatch_obsolete_hints_and_event_head_backoff_are_distinct(initialized_conn) -> None:
    first = _enqueue(initialized_conn, age=50)
    second = _enqueue(initialized_conn, target="another-private-player", age=10)
    state.mark_done(initialized_conn, second)
    with transaction(initialized_conn):
        revision = require_row(initialized_conn.execute("SELECT revision FROM discovery_jobs WHERE id=%s", (first,)))[0]
        initialized_conn.execute("INSERT INTO dispatch_outbox(job_id,job_revision) VALUES(%s,%s)", (first, revision+100))
        initialized_conn.execute("UPDATE event_outbox SET occurred_at=%s", (NOW-80,))
        initialized_conn.execute("UPDATE event_outbox SET next_attempt_at=%s WHERE id=(SELECT MIN(id) FROM event_outbox)", (NOW+30,))
    snapshot = operational_snapshot(initialized_conn, now=NOW)
    dispatch = snapshot["outboxes"]["dispatch"]
    assert dispatch == {"pending": 2, "ready": 1, "deferred": 0, "obsolete": 1, "oldest_ready_age_seconds": 50}
    events = snapshot["outboxes"]["events"]
    assert events["pending"] == 3 and events["due"] == 2
    assert events["oldest_pending_age_seconds"] == 80 and events["head_retry_delay_seconds"] == 30
    with transaction(initialized_conn):
        initialized_conn.execute("UPDATE dispatch_outbox SET available_at=%s WHERE job_revision=%s", (NOW+15, revision))
    dispatch = operational_snapshot(initialized_conn, now=NOW)["outboxes"]["dispatch"]
    assert dispatch["ready"] == 0 and dispatch["deferred"] == 1 and dispatch["oldest_ready_age_seconds"] is None


def test_future_enqueue_times_do_not_produce_negative_age(initialized_conn) -> None:
    _enqueue(initialized_conn, age=-10)
    _enqueue(initialized_conn, target="future", age=-5)
    snapshot = operational_snapshot(initialized_conn, now=NOW)
    stage = snapshot["stages"]["acquisition"]
    assert stage["eligible_jobs"] == 2
    assert stage["oldest_eligible_age_seconds"] == 0


def test_usage_counts_current_period_only_and_never_resets_recorded_charges(initialized_conn) -> None:
    start, end = monthly_period(NOW)
    previous, _ = monthly_period(start-1)
    _enqueue(initialized_conn)
    _enqueue(initialized_conn, workspace="beta")
    with transaction(initialized_conn):
        for workspace, period_start, requests, remote_bytes in (("alpha", start, 2, 30), ("beta", start, 3, 40),
                                                               ("alpha", previous, 100, 1000)):
            initialized_conn.execute(
                """INSERT INTO workspace_budget_periods(workspace_id,period_start,period_end,remote_requests,remote_bytes)
                     VALUES(%s,%s,%s,%s,%s)""", (workspace, period_start, monthly_period(period_start)[1], requests, remote_bytes),
            )
    usage = operational_snapshot(initialized_conn, now=NOW)["usage"]
    assert usage == {"period_start": start, "period_end": end, "games": 0, "normalization_units": 0,
                     "remote_requests": 5, "remote_bytes": 70}
    assert operational_snapshot(initialized_conn, now=end)["usage"]["remote_requests"] == 0
    assert require_row(initialized_conn.execute("SELECT SUM(remote_requests) FROM workspace_budget_periods"))[0] == 105


def test_metrics_command_returns_json_and_redacts_database_errors(database_url, capsys) -> None:
    assert metrics_admin.main(["--database-url", database_url]) == 0
    assert json.loads(capsys.readouterr().out)["stages"]["processing"]["eligible_jobs"] == 0
    assert metrics_admin.main(["--database-url", "secret-not-postgres"]) == 2
    assert "secret-not-postgres" not in capsys.readouterr().err


def test_metrics_requires_current_schema_without_initializing(uninitialized_database_url, capsys) -> None:
    assert metrics_admin.main(["--database-url", uninitialized_database_url]) == 2
    assert "migrate" in capsys.readouterr().err
    with connection(uninitialized_database_url) as conn:
        assert require_row(conn.execute("SELECT to_regclass('schema_migrations')"))[0] is None


def test_metrics_command_bounds_query_deadline_before_connecting(capsys) -> None:
    assert metrics_admin.main(["--statement-timeout-ms", "0"]) == 2
    assert "statement timeout" in capsys.readouterr().err


def test_real_query_timeout_returns_failure_without_partial_snapshot(database_url, capsys, monkeypatch) -> None:
    def stalled_usage(conn, now):
        conn.execute("SELECT pg_sleep(0.1)")
        return {}

    monkeypatch.setattr(operations_metrics, "_period_usage", stalled_usage)
    assert metrics_admin.main(["--database-url", database_url, "--statement-timeout-ms", "20"]) == 1
    captured = capsys.readouterr()
    assert captured.out == "" and "metrics are unavailable" in captured.err
    with connection(database_url) as conn:
        assert require_row(conn.execute("SELECT COUNT(*) FROM event_outbox"))[0] == 0


@pytest.mark.parametrize("now", [-1, float("nan"), float("inf")])
def test_metrics_rejects_invalid_time(initialized_conn, now) -> None:
    with pytest.raises(ValueError, match="finite and nonnegative"):
        operational_snapshot(initialized_conn, now=now)
