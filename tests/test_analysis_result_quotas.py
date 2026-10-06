"""Cumulative stored-output admission and explicit retention against PostgreSQL."""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from fastapi.testclient import TestClient

from chess_crawl import operations
from chess_crawl.api import create_app
from chess_crawl.application import Limits
from chess_crawl.application.errors import NotFound
from chess_crawl.jobs.budget import QuotaExceeded
from chess_crawl.storage import migrations, working_sets
from chess_crawl.storage.db import connection, require_row, transaction
from helpers.api import TOKENS


def selection(conn, owner="alpha", key="one"):
    return working_sets.create_working_set(
        conn, workspace_id=owner, name=key, filters={}, settings={"key": key}, idempotency_key=key,
    )["id"]


def save(conn, set_id, owner="alpha", version="1", **kwargs):
    return working_sets.save_result(
        conn, set_id, owner, implementation="fixture", implementation_version=version,
        settings=kwargs.pop("settings", {}), output=kwargs.pop("output", {"value": "same"}), **kwargs,
    )


@pytest.mark.parametrize("variant", ["version", "settings", "selection"])
def test_api_quota_cannot_be_reset_and_preserves_replay_and_other_owners(database_url, monkeypatch, variant):
    monkeypatch.setenv("CHESS_CRAWL_MAX_ANALYSIS_RESULTS", "1")
    with TestClient(create_app(database_url, workspace_tokens=TOKENS),
                    headers={"Authorization": "Bearer alpha-secret"}) as api:
        body = {"name": "one", "filters": {}, "settings": {"seed": 1}}
        set_id = api.post("/v1/working-sets", json=body, headers={"Idempotency-Key": "one"}).json()["id"]
        url = f"/v1/working-sets/{set_id}/results"
        result = {"implementation": "fixture", "implementation_version": "1", "settings": {}, "output": {"ok": True}}
        first = api.post(url, json=result)
        assert first.status_code == 201, first.text
        assert api.post(url, json=result).json()["replayed"] is True
        assert api.post(url, json={**result, "output": {"ok": False}}).status_code == 409
        assert api.post(url, json={**result, "max_analysis_results": 9999}).status_code == 422
        assert api.get(f"/v1/results/{first.json()['id']}", headers={"Authorization": "Bearer beta-secret"}).status_code == 404
        changed = {**result}
        if variant == "version":
            changed["implementation_version"] = "2"
        elif variant == "settings":
            changed["settings"] = {"vary": 1}
        else:
            set_id = api.post("/v1/working-sets", json={**body, "settings": {"seed": 2}},
                              headers={"Idempotency-Key": "two"}).json()["id"]
        rejected = api.post(f"/v1/working-sets/{set_id}/results", json=changed)
        assert rejected.status_code == 429, rejected.text
        assert rejected.json()["error"]["quota"]["dimension"] == "analysis_results"
        beta_headers = {"Authorization": "Bearer beta-secret", "Idempotency-Key": "beta"}
        other = api.post("/v1/working-sets", json=body, headers=beta_headers).json()["id"]
        assert api.post(f"/v1/working-sets/{other}/results", json=result, headers=beta_headers).status_code == 201
    with connection(database_url) as conn:
        assert require_row(conn.execute("SELECT COUNT(*) FROM analysis_results"))[0] == 2


@pytest.mark.parametrize("payload", ["output", "settings"])
def test_byte_quota_counts_metadata_and_unicode_at_exact_boundary(database_url, payload):
    kwargs = {payload: {"text": "♟" * 1000}}
    with connection(database_url, mode="rw") as conn:
        set_id = selection(conn)
        first = save(conn, set_id, **kwargs)
        charged = first["stored_bytes"]
        row = require_row(conn.execute(
            "SELECT settings::text, output::text FROM analysis_results WHERE id=%s", (first["id"],),
        ))
        assert charged == sum(len(value.encode("utf-8")) for value in (row[0], row[1], "fixture", "1", "alpha")) + 128
        assert charged > 3000  # UTF-8 bytes, irrespective of compressible JSONB storage.
        limits = Limits(max_analysis_result_bytes=2 * charged)
        assert save(conn, set_id, version="2", limits=limits, **kwargs)["stored_bytes"] == charged
        with pytest.raises(QuotaExceeded) as failure:
            save(conn, set_id, version="3", limits=limits, **kwargs)
        assert failure.value.dimension == "analysis_result_bytes"
        assert failure.value.remaining == 0
        assert save(conn, set_id, limits=Limits(max_analysis_result_bytes=1), **kwargs)["replayed"]
        assert require_row(conn.execute("SELECT COUNT(*) FROM analysis_results"))[0] == 2


@pytest.mark.parametrize("same_result", [False, True])
def test_independent_connections_cannot_overbook_a_workspace(database_url, same_result):
    with connection(database_url, mode="rw") as conn:
        sets = [selection(conn, key=str(index)) for index in range(2)]
    barrier = Barrier(2)

    def write(index):
        with connection(database_url, mode="rw") as conn:
            conn.execute("SET statement_timeout='15s'")
            barrier.wait(timeout=15)
            try:
                return save(conn, sets[0] if same_result else sets[index], limits=Limits(max_analysis_results=1))
            except QuotaExceeded:
                return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(write, index) for index in range(2)]
        results = [future.result(timeout=20) for future in futures]
    if same_result:
        assert sorted(result["replayed"] for result in results) == [False, True]
        assert results[0]["id"] == results[1]["id"]
    else:
        assert sum(result is not None for result in results) == 1
    with connection(database_url) as conn:
        assert require_row(conn.execute("SELECT COUNT(*) FROM analysis_results"))[0] == 1


def test_failed_outer_transaction_releases_result_capacity(initialized_conn):
    conn = initialized_conn
    set_id = selection(conn)
    limits = Limits(max_analysis_results=1)
    with pytest.raises(RuntimeError, match="rollback"):
        with transaction(conn):
            save(conn, set_id, limits=limits)
            raise RuntimeError("rollback")
    assert require_row(conn.execute("SELECT COUNT(*) FROM analysis_results"))[0] == 0
    assert save(conn, set_id, limits=limits)["replayed"] is False


def test_operator_retention_is_bounded_scoped_and_releases_capacity(database_url, capsys):
    limits = Limits(max_analysis_results=2)
    with connection(database_url, mode="rw") as conn:
        set_id, other_set = selection(conn), selection(conn, "beta")
        first = save(conn, set_id, limits=limits)
        second = save(conn, set_id, version="2", limits=limits)
        other = save(conn, other_set, "beta", limits=limits)
        conn.execute("UPDATE analysis_results SET created_at=100")
        with pytest.raises(QuotaExceeded):
            save(conn, set_id, version="3", limits=limits)
        # Deletion must roll back with its caller, including quota capacity.
        with pytest.raises(RuntimeError):
            with transaction(conn):
                working_sets.prune_results(conn, "alpha", before=101, limit=1)
                raise RuntimeError("rollback")
        assert working_sets.read_result(conn, first["id"], "alpha")
    arguments = ["prune-results", "--database-url", database_url, "--workspace-id", "alpha", "--batch-size", "1"]
    assert operations.main([*arguments, "--before", "100"]) == 0
    assert json.loads(capsys.readouterr().out)["deleted"] == 0
    assert operations.main([*arguments, "--before", "101"]) == 0
    assert json.loads(capsys.readouterr().out) == {"workspace_id": "alpha", "deleted": 1, "released_bytes": first["stored_bytes"]}
    with connection(database_url, mode="rw") as conn:
        with pytest.raises(NotFound):
            working_sets.read_result(conn, first["id"], "alpha")
        assert working_sets.read_result(conn, second["id"], "alpha")
        assert working_sets.read_result(conn, other["id"], "beta")
        assert save(conn, set_id, version="3", limits=limits)
        assert require_row(conn.execute("SELECT COUNT(*) FROM working_sets"))[0] == 2


def test_migration_accounts_for_existing_results_without_discarding_them(uninitialized_database_url, monkeypatch):
    all_migrations = migrations.migration_resources()
    with connection(uninitialized_database_url, mode="rw") as conn:
        with monkeypatch.context() as legacy:
            legacy.setattr(migrations, "migration_resources", lambda: tuple(m for m in all_migrations if m[0] < 16))
            migrations.initialize(conn)
        assert migrations.current_version(conn) == 15
        # Seed authentic pre-quota rows: today's submission helper requires the
        # namespace column that migration 0020 adds to this historical schema.
        request = {"name": "one", "filters": {}, "settings": {"key": "one"}}
        with transaction(conn):
            conn.execute("INSERT INTO workspaces(id,created_at) VALUES('alpha',1)")
            set_id = int(require_row(conn.execute(
                """INSERT INTO working_sets(workspace_id,name,filters,settings,input_signature,member_count,created_at)
                   VALUES('alpha','one','{}','{"key":"one"}',%s,0,1) RETURNING id""",
                (working_sets.digest({"schema": 1, "filters": {}}),),
            ))[0])
            conn.execute(
                """INSERT INTO working_set_submissions(workspace_id,idempotency_key,request_json,working_set_id)
                   VALUES('alpha','one',%s,%s)""",
                (working_sets.canonical(request), set_id),
            )
            conn.execute("""INSERT INTO analysis_results(workspace_id,input_signature,implementation,implementation_version,
                         settings,result_signature,output,created_at)
                         VALUES('alpha','inputs','fixture','old','{}','legacy','{"kept":true}',1)""")
        assert migrations.initialize(conn).applied == tuple(m[1] for m in all_migrations if m[0] >= 16)
        assert migrations.initialize(conn).applied == ()
        row = require_row(conn.execute("SELECT output,stored_bytes FROM analysis_results"))
        assert row["output"] == {"kept": True} and row["stored_bytes"] > 0
        submission = require_row(conn.execute("SELECT submission_namespace,working_set_id FROM working_set_submissions"))
        assert submission["submission_namespace"] == "api" and submission["working_set_id"] == set_id
        replayed = working_sets.create_working_set(conn, workspace_id="alpha", idempotency_key="one", **request)
        assert replayed["replayed"] is True and replayed["id"] == set_id
        assert require_row(conn.execute("SELECT COUNT(*) FROM working_set_submissions"))[0] == 1
        with pytest.raises(QuotaExceeded):
            save(conn, set_id, limits=Limits(max_analysis_results=1))


@pytest.mark.parametrize("name", ["MAX_ANALYSIS_RESULTS", "MAX_ANALYSIS_RESULT_BYTES"])
@pytest.mark.parametrize("value", ["0", "-1", "bad", str(2**63)])
def test_invalid_quota_configuration_fails_at_startup(monkeypatch, name, value):
    monkeypatch.setenv("CHESS_CRAWL_" + name, value)
    with pytest.raises(ValueError):
        create_app("postgresql://postgres@localhost/unused", workspace_tokens=TOKENS)


@pytest.mark.parametrize("before,limit", [(-1, 1), (1, 0), (1, 10001)])
def test_invalid_retention_cannot_delete_rows(initialized_conn, before, limit):
    set_id = selection(initialized_conn)
    saved = save(initialized_conn, set_id)
    with pytest.raises(ValueError):
        working_sets.prune_results(initialized_conn, "alpha", before=before, limit=limit)
    assert working_sets.read_result(initialized_conn, saved["id"], "alpha")
