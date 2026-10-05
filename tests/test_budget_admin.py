"""Operator administration preserves counters and resumes the exact checkpoints."""
from __future__ import annotations

import json
from dataclasses import replace

import pytest

from chess_crawl.jobs import state
from chess_crawl.jobs.budget import BudgetPolicy, main
from chess_crawl.storage.db import connection, require_row, transaction
from helpers.api import FULL, budget_client as client


def test_show_is_read_only_and_resume_preserves_lifetime_usage_and_checkpoints(database_url, capsys, monkeypatch) -> None:
    policy = replace(BudgetPolicy(), job_max_remote_requests=1, workspace_max_remote_requests=3)
    with client(database_url, policy) as api:
        submitted = api.post("/v1/imports", json=FULL, headers={"Idempotency-Key": "budgeted"}).json()
        run_id = submitted["run_id"]
    with connection(database_url, mode="rw") as conn, transaction(conn):
        profile, games = submitted["job_ids"]
        state.mark_done(conn, profile, reason="profile retained")
        state.mark_blocked(conn, games, reason="budget_exhausted:remote_requests")
        conn.execute("UPDATE work_budgets SET remote_requests=1,exhausted_dimension='remote_requests',exhausted_at=1")
        conn.execute("UPDATE workspace_budget_periods SET remote_requests=1")
        conn.execute("UPDATE discovery_jobs SET params_json=%s WHERE id=%s",
                     (json.dumps({"collection_mode": "full", "cursor_month": "2020-06", "after_game": "abcdefgh"}), games))
        original_job = state.get_job(conn, games)
        assert original_job is not None
        before_params = original_job.params_json
        before_jobs = [dict(row) for row in conn.execute("SELECT * FROM discovery_jobs ORDER BY id")]
    monkeypatch.setenv("CHESS_CRAWL_JOB_MAX_REMOTE_REQUESTS", "2")
    monkeypatch.setenv("CHESS_CRAWL_WORKSPACE_MAX_REMOTE_REQUESTS", "4")
    assert main(["show", "--run-id", str(run_id), "--database-url", database_url]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["workspace_id"] == "alpha"
    assert shown["budget"]["remote_requests"] == 1
    assert shown["budget"]["policy"]["job_max_remote_requests"] == 1
    assert shown["configured_policy"]["job_max_remote_requests"] == 2
    with connection(database_url) as conn:
        assert [dict(row) for row in conn.execute("SELECT * FROM discovery_jobs ORDER BY id")] == before_jobs
    assert main(["resume", "--run-id", str(run_id), "--database-url", database_url]) == 0
    resumed = json.loads(capsys.readouterr().out)
    assert resumed["budget"]["remote_requests"] == 1
    assert resumed["budget"]["policy"]["job_max_remote_requests"] == 2
    assert resumed["budget"]["exhausted_dimension"] is None
    with connection(database_url) as conn:
        resumed_games, retained_profile = state.get_job(conn, games), state.get_job(conn, profile)
        assert resumed_games is not None and retained_profile is not None
        assert resumed_games.state == "pending"
        assert resumed_games.params_json == before_params
        assert retained_profile.state == "done"
        assert require_row(conn.execute("SELECT remote_requests FROM workspace_budget_periods WHERE workspace_id='alpha'"))[0] == 1
    # A lower operator environment cannot shrink the extension or erase usage.
    monkeypatch.setenv("CHESS_CRAWL_JOB_MAX_REMOTE_REQUESTS", "1")
    assert main(["resume", "--run-id", str(run_id), "--database-url", database_url]) == 0
    repeated = json.loads(capsys.readouterr().out)["budget"]
    assert repeated["policy"]["job_max_remote_requests"] == 2 and repeated["remote_requests"] == 1


def test_operator_commands_require_valid_policy_and_do_not_accept_owner_or_limit_overrides(monkeypatch, capsys) -> None:
    monkeypatch.setenv("CHESS_CRAWL_JOB_MAX_GAMES", "0")
    assert main(["resume", "--run-id", "1", "--database-url", "postgresql://unused/unused"]) == 1
    assert "positive PostgreSQL bigint" in capsys.readouterr().err
    with pytest.raises(SystemExit) as exc:
        main(["resume", "--run-id", "1", "--workspace", "beta"])
    assert exc.value.code == 2
    with pytest.raises(SystemExit) as exc:
        main(["resume", "--run-id", "0"])
    assert exc.value.code == 2


def test_operator_show_does_not_create_or_migrate_an_archive(uninitialized_database_url, capsys) -> None:
    assert main(["show", "--run-id", "1", "--database-url", uninitialized_database_url]) == 1
    assert "Migrate the archive" in capsys.readouterr().err
    with connection(uninitialized_database_url) as conn:
        assert require_row(conn.execute("SELECT to_regclass('schema_migrations')"))[0] is None
