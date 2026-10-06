from __future__ import annotations

import json
import threading
from dataclasses import asdict, replace
from importlib import resources

from fastapi.testclient import TestClient
import pytest

from chess_crawl import application
from chess_crawl.api import create_app
from chess_crawl.api.auth import configured_authenticator
from chess_crawl.jobs.budget import BudgetPolicy, QuotaExceeded
from chess_crawl.storage.db import connection
from chess_crawl.storage import migrations
from chess_crawl.storage.db import transaction
from chess_crawl.storage.workspace_access import (
    authenticate_token, issue_credential, provision_workspace, revoke_credential,
    rotate_credentials, workspace_snapshot,
)
from chess_crawl.storage.work_budgets import get_run_budget, reserve_normalization, set_workspace_policy
from chess_crawl.workspace_admin import main


def _authorization(token: str) -> dict[str, str]:
    return {"Authorization": "Bearer " + token}


def _submit(conn, workspace: str, key: str) -> dict:
    return application.submit_import(
        conn, application.ImportRequest(provider="lichess", username="alice", since=None, until=None, max_games=1, collection_mode="full"),
        idempotency_key=key, workspace_id=workspace,
    )


def test_rotation_and_revocation_are_visible_to_existing_api_replicas(database_url: str) -> None:
    # App construction does not connect or freeze database credentials.
    first = TestClient(create_app(database_url, auth_mode="database"))
    second = TestClient(create_app(database_url, auth_mode="database"))
    with connection(database_url, mode="rw") as conn:
        alpha = provision_workspace(conn, "alpha", BudgetPolicy())
        beta = provision_workspace(conn, "beta", BudgetPolicy())
        extra = issue_credential(conn, "alpha")
        submitted = _submit(conn, "alpha", "first-run")
    with first, second:
        for client in (first, second):
            assert client.get("/health/ready", headers=_authorization(alpha["token"])).status_code == 200
            assert client.get(f"/v1/runs/{submitted['run_id']}", headers=_authorization(beta["token"])).status_code == 404
        with connection(database_url, mode="rw") as conn:
            revoked = revoke_credential(conn, "alpha", extra["credential_id"])
            assert revoke_credential(conn, "alpha", extra["credential_id"]) == revoked
            replacement = rotate_credentials(conn, "alpha")
        for client in (first, second):
            for token in (alpha["token"], extra["token"]):
                assert client.get("/health/ready", headers=_authorization(token)).status_code == 401
            response = client.get(f"/v1/runs/{submitted['run_id']}", headers=_authorization(replacement["token"]))
            assert response.status_code == 200
        # The credential identity cannot select a different workspace by header.
        assert first.get(f"/v1/runs/{submitted['run_id']}", headers={
            **_authorization(beta["token"]), "X-Workspace-Id": "alpha",
        }).status_code == 404
    with connection(database_url) as conn:
        stored = [dict(row) for row in conn.execute("SELECT * FROM workspace_credentials")]
        assert all(secret not in json.dumps(stored) for secret in (alpha["token"], extra["token"], replacement["token"]))
        snapshot = workspace_snapshot(conn, "alpha")
        assert all("token_digest" not in row and "token" not in row for row in snapshot["credentials"])


def test_managed_policy_changes_new_runs_without_resetting_usage_or_existing_budgets(initialized_conn) -> None:
    old_policy = BudgetPolicy(job_max_games=3, workspace_max_games=3)
    account = provision_workspace(initialized_conn, "alpha", old_policy)
    first = _submit(initialized_conn, "alpha", "first")
    budget = get_run_budget(initialized_conn, first["run_id"], "alpha")
    assert budget is not None
    reserve_normalization(initialized_conn, budget["id"], game_key="game-1")
    reserve_normalization(initialized_conn, budget["id"], game_key="game-2")
    upgraded = replace(old_policy, job_max_games=6, workspace_max_games=6)
    version = set_workspace_policy(initialized_conn, "alpha", upgraded, expected_version=account["policy_version"])
    second = _submit(initialized_conn, "alpha", "second")
    original = get_run_budget(initialized_conn, first["run_id"], "alpha")
    following = get_run_budget(initialized_conn, second["run_id"], "alpha")
    assert original is not None and following is not None
    assert original["policy"]["job_max_games"] == 3
    assert original["workspace_period"]["usage"]["games"] == 2
    assert following["policy"] == asdict(upgraded)
    assert following["workspace_policy_version"] == version
    # An API using tighter startup defaults cannot replace managed authority.
    third = application.submit_import(
        initialized_conn, application.ImportRequest(provider="lichess", username="bob", since=None, until=None, max_games=1, collection_mode="full"),
        idempotency_key="third", workspace_id="alpha", budget_policy=BudgetPolicy(workspace_max_games=1),
    )
    third_budget = get_run_budget(initialized_conn, third["run_id"], "alpha")
    assert third_budget is not None and third_budget["policy"] == asdict(upgraded)
    set_workspace_policy(initialized_conn, "alpha", replace(upgraded, workspace_max_games=1), expected_version=version)
    with pytest.raises(QuotaExceeded):
        _submit(initialized_conn, "alpha", "blocked")
    current = workspace_snapshot(initialized_conn, "alpha")
    assert current["period"]["usage"]["games"] == 2
    assert _submit(initialized_conn, "alpha", "first")["replayed"] is True


def test_concurrent_operator_updates_require_current_policy_version(database_url: str) -> None:
    with connection(database_url, mode="rw") as conn:
        workspace = provision_workspace(conn, "alpha", BudgetPolicy())
    barrier = threading.Barrier(2)
    outcomes: list[str] = []
    failures: list[BaseException] = []

    def update(limit: int) -> None:
        try:
            with connection(database_url, mode="rw") as conn:
                barrier.wait(5)
                try:
                    set_workspace_policy(conn, "alpha", BudgetPolicy(workspace_max_games=limit), expected_version=workspace["policy_version"])
                    outcomes.append("updated")
                except ValueError:
                    outcomes.append("stale")
        except BaseException as error:
            failures.append(error)

    threads = [threading.Thread(target=update, args=(limit,)) for limit in (10, 20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    assert not failures
    assert sorted(outcomes) == ["stale", "updated"]
    with connection(database_url) as conn:
        assert workspace_snapshot(conn, "alpha")["policy"]["version"] == workspace["policy_version"] + 1


def test_admin_commands_do_not_disclose_secrets_on_show_or_failure(database_url: str, tmp_path, capsys) -> None:
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"workspace_max_games": 10}), encoding="utf-8")
    arguments = ["--workspace-id", "alpha", "--database-url", database_url]
    assert main(["provision", *arguments, "--policy-file", str(policy)]) == 0
    issued = json.loads(capsys.readouterr().out)
    assert main(["show", *arguments]) == 0
    shown = capsys.readouterr()
    assert issued["token"] not in shown.out
    assert "token_digest" not in shown.out
    assert main(["set-policy", *arguments, "--policy-file", str(policy), "--expected-version", "2"]) == 1
    assert "version changed" in capsys.readouterr().err
    assert main(["provision", *arguments, "--policy-file", str(policy)]) == 1
    assert capsys.readouterr().out == ""


def test_database_auth_is_explicit_and_rejects_static_credentials(monkeypatch) -> None:
    target = "postgresql://test@127.0.0.1:1/unavailable"
    with pytest.raises(ValueError, match="cannot be combined"):
        create_app(target, "static-secret", auth_mode="database")
    monkeypatch.setenv("CHESS_CRAWL_API_AUTH_MODE", "database")
    with TestClient(create_app(target), raise_server_exceptions=False) as client:
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").status_code == 401
        assert client.get("/health/ready", headers=_authorization("malformed")).status_code == 401
        assert client.get("/health/ready", headers=_authorization("ccw_" + "a" * 43)).status_code == 503
    with pytest.raises(ValueError, match="static or database"):
        create_app(target, auth_mode="other")


def test_authenticator_representation_does_not_expose_credentials() -> None:
    target = "postgresql://private-user:private-password@127.0.0.1:1/unavailable"
    configured = configured_authenticator(target, "private-token", None, "static")
    assert "private" not in repr(configured)


def test_explicit_initial_policy_can_adopt_fresh_local_workspace(initialized_conn) -> None:
    assert workspace_snapshot(initialized_conn, "local")["policy"] is None
    version = set_workspace_policy(initialized_conn, "local", BudgetPolicy(workspace_max_games=10), expected_version=0)
    assert version == 1
    assert workspace_snapshot(initialized_conn, "local")["policy"]["managed"] is True
    with pytest.raises(ValueError, match="version changed"):
        set_workspace_policy(initialized_conn, "local", BudgetPolicy(), expected_version=0)


def test_wrong_workspace_cannot_revoke_credentials(initialized_conn) -> None:
    alpha = provision_workspace(initialized_conn, "alpha", BudgetPolicy())
    provision_workspace(initialized_conn, "beta", BudgetPolicy())
    with pytest.raises(ValueError, match="not found"):
        revoke_credential(initialized_conn, "beta", alpha["credential_id"])
    assert authenticate_token(initialized_conn, alpha["token"]) == "alpha"
    assert authenticate_token(initialized_conn, "not-an-issued-token") is None


def test_workspace_access_migration_preserves_existing_policy_and_usage(uninitialized_database_url: str) -> None:
    from psycopg.types.json import Jsonb

    policy = asdict(BudgetPolicy(workspace_max_games=10))
    with connection(uninitialized_database_url, mode="rw") as conn:
        with transaction(conn):
            for version, name, filename in migrations.migration_resources():
                if version > 16:
                    break
                migrations._execute_schema(conn, resources.files("chess_crawl.storage").joinpath(filename).read_text("utf-8"))
                conn.execute("INSERT INTO schema_migrations(version,name,applied_at) VALUES(%s,%s,123)", (version, name))
            conn.execute("INSERT INTO workspace_budget_policies(workspace_id,policy,updated_at) VALUES('local',%s,123)", (Jsonb(policy),))
            conn.execute("INSERT INTO workspace_budget_periods(workspace_id,period_start,period_end,games) VALUES('local',100,200,7)")
        migrations.initialize(conn)
        stored = conn.execute("SELECT policy,managed,version FROM workspace_budget_policies WHERE workspace_id='local'").fetchone()
        assert stored is not None
        assert stored["policy"] == policy and stored["managed"] is False and stored["version"] == 1
        usage = conn.execute("SELECT games FROM workspace_budget_periods WHERE workspace_id='local'").fetchone()
        assert usage is not None and usage[0] == 7
        history = conn.execute("SELECT policy,version FROM workspace_policy_history WHERE workspace_id='local'").fetchone()
        assert history is not None and history["policy"] == policy and history["version"] == 1
