"""Admin operations keep maintenance local and preserve product API boundaries."""
from __future__ import annotations

import json

import pytest

from chess_crawl import operations
from chess_crawl.storage.db import connection, require_row
from chess_crawl.storage.migrations import SCHEMA_VERSION, current_version


def test_migrate_initializes_and_repeats_without_provider_calls(
    uninitialized_database_url: str, capsys: pytest.CaptureFixture[str],
) -> None:
    args = ["migrate", "--database-url", uninitialized_database_url]
    assert operations.main(args) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["version"] == SCHEMA_VERSION and first["applied"]
    assert operations.main(args) == 0
    assert json.loads(capsys.readouterr().out)["applied"] == []
    with connection(uninitialized_database_url) as conn:
        assert current_version(conn) == SCHEMA_VERSION


def test_info_does_not_modify_an_initialized_archive(database_url: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert operations.main(["info", "--database-url", database_url]) == 0
    info = json.loads(capsys.readouterr().out)
    assert info["ready"] is True and info["schema_version"] == SCHEMA_VERSION


@pytest.mark.parametrize("command", ["fetch", "submit", "report", "export", "query", "crawl"])
def test_admin_rejects_product_commands(command: str) -> None:
    with pytest.raises(SystemExit) as error:
        operations.main([command])
    assert error.value.code == 2


def test_relocation_requires_explicit_external_storage(database_url: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert operations.main(["relocate", "--database-url", database_url]) == 2
    assert "Select a local or s3" in capsys.readouterr().err


def test_invalid_connection_error_does_not_echo_secrets(capsys: pytest.CaptureFixture[str]) -> None:
    assert operations.main(["info", "--database-url", "not-postgres secret-token-value"]) == 2
    assert "secret-token-value" not in capsys.readouterr().err


def test_info_reads_an_empty_database_without_initializing_it(
    uninitialized_database_url: str, capsys: pytest.CaptureFixture[str],
) -> None:
    assert operations.main(["info", "--database-url", uninitialized_database_url]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema_version"] == 0 and result["ready"] is False
    with connection(uninitialized_database_url) as conn:
        assert require_row(conn.execute("SELECT to_regclass('schema_migrations')"))[0] is None


def test_budget_alias_preserves_usage_and_exact_checkpoint(database_url, capsys, monkeypatch) -> None:
    from chess_crawl.jobs import state
    from chess_crawl.jobs.budget import BudgetPolicy
    from chess_crawl.storage.collection import save_checkpoint
    from chess_crawl.storage.db import transaction
    from chess_crawl.storage.work_budgets import admit_run_budget, exhaust_budget, reserve_request, settle_request
    from chess_crawl.storage.workspaces import submission_context
    policy = BudgetPolicy(job_max_remote_requests=1, workspace_max_remote_requests=3)
    checkpoint = {"units": ["2020/01", "2020/02"], "unit_index": 1, "until_ms": 77}
    with connection(database_url, mode="rw") as conn:
        with transaction(conn):
            submission_context(conn, "alpha")
            run_id = state.create_crawl_run(conn, provider="lichess", seed_spec="target", params={})
            blocked = state.enqueue_job(conn, provider="lichess", kind="fetch_user_games", target="target", crawl_run_id=run_id).job_id
            done = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="target", crawl_run_id=run_id).job_id
            budget_id = admit_run_budget(conn, run_id, "alpha", policy)["id"]
        ticket, _ = reserve_request(conn, budget_id)
        settle_request(conn, ticket, 0)
        state.mark_blocked(conn, blocked, reason="budget_exhausted:remote_requests")
        state.mark_done(conn, done, reason="retained profile")
        save_checkpoint(conn, blocked, checkpoint, now=1)
        exhaust_budget(conn, budget_id, "remote_requests")
        before = [dict(row) for row in conn.execute("SELECT * FROM discovery_jobs ORDER BY id")]
    monkeypatch.setenv("CHESS_CRAWL_JOB_MAX_REMOTE_REQUESTS", "2")
    monkeypatch.setenv("CHESS_CRAWL_WORKSPACE_MAX_REMOTE_REQUESTS", "4")
    arguments = ["--run-id", str(run_id), "--database-url", database_url]
    assert operations.main(["budgets", "show", *arguments]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["workspace_id"] == "alpha"
    assert shown["budget"]["remote_requests"] == 1
    assert shown["budget"]["policy"]["job_max_remote_requests"] == 1
    with connection(database_url) as conn:
        assert [dict(row) for row in conn.execute("SELECT * FROM discovery_jobs ORDER BY id")] == before
    assert operations.main(["budgets", "resume", *arguments]) == 0
    resumed = json.loads(capsys.readouterr().out)
    assert resumed["budget"]["remote_requests"] == 1
    assert resumed["budget"]["policy"]["job_max_remote_requests"] == 2
    with connection(database_url) as conn:
        assert require_row(conn.execute("SELECT state FROM discovery_jobs WHERE id=%s", (blocked,)))[0] == "pending"
        assert require_row(conn.execute("SELECT state FROM discovery_jobs WHERE id=%s", (done,)))[0] == "done"
        assert require_row(conn.execute("SELECT cursor FROM collection_checkpoints WHERE job_id=%s", (blocked,)))[0] == checkpoint
        assert require_row(conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 0


def test_workspace_admin_alias_provisions_and_revokes_shared_access(database_url, tmp_path, capsys) -> None:
    from chess_crawl.storage.workspace_access import authenticate_token
    policy = tmp_path / "workspace-policy.json"
    policy.write_text(json.dumps({"workspace_max_games": 10}), encoding="utf-8")
    base = ["--workspace-id", "alpha", "--database-url", database_url]
    assert operations.main(["workspaces", "provision", *base, "--policy-file", str(policy)]) == 0
    issued = json.loads(capsys.readouterr().out)
    token = issued["token"]
    credential_id = issued["credential_id"]
    with connection(database_url) as conn:
        assert authenticate_token(conn, token) == "alpha"
    assert operations.main(["workspaces", "show", *base]) == 0
    shown = capsys.readouterr().out
    assert token not in shown
    assert json.loads(shown)["policy"]["policy"]["workspace_max_games"] == 10
    assert operations.main(["workspaces", "revoke", *base, "--credential-id", credential_id]) == 0
    assert token not in capsys.readouterr().out
    with connection(database_url) as conn:
        assert authenticate_token(conn, token) is None
