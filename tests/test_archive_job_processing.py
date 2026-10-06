"""Queued archive work reuses providerless processing and immutable publication."""
from __future__ import annotations

import io
import time
from dataclasses import replace

import pytest

from chess_crawl.application.archive_exports import ExportRenderLimits, write_export_snapshot
from chess_crawl.application.archive_jobs import ArchiveJobSettings, submit_archive_job
from chess_crawl.application.errors import Conflict
from chess_crawl.jobs import internal, state
from chess_crawl.jobs.budget import BudgetExceeded, BudgetPolicy, QuotaExceeded
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.storage import artifacts, work_budgets, working_sets
from chess_crawl.storage.archives import prepare_archive_object, read_archive_reference
from chess_crawl.storage.db import DatabaseError, connection, require_row, transaction
from chess_crawl.storage.object_store import LocalObjectStore
from support import seed_game


POLICY = BudgetPolicy()


def submit(conn, operation, request, *, settings=None, key="request", workspace="alpha", policy=POLICY):
    return submit_archive_job(conn, operation=operation, request=request, workspace_id=workspace,
                              idempotency_key=key, settings=settings or ArchiveJobSettings(), budget_policy=policy)


def job_id(result):
    return result["job_ids"][0]


def add_version(conn, game, content):
    row = require_row(conn.execute(
        "INSERT INTO game_versions(game_id,content_hash,parser_version,first_seen_at,variant,parse_status,played_ply_count,move_text_origin) VALUES(%s,%s,'test-v1',1,'standard','complete',2,'provider.moves') RETURNING id", (game, content),
    ))
    conn.execute("UPDATE games SET current_version_id=%s WHERE id=%s", (row[0], game))
    return int(row[0])


def test_providerless_working_set_processing_snapshot_and_immutable_retry(database_url, tmp_path, monkeypatch):
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_DIRECTORY", str(tmp_path))
    with connection(database_url, mode="rw") as conn:
        first, *_ = seed_game(conn, provider="lichess", game_key="first", white="a", black="b")
        first_version = add_version(conn, first, "one")
        result = submit(conn, "build_working_set", {"name": "sample", "filters": {}, "settings": {"engine": "v1"}})
        second, *_ = seed_game(conn, provider="lichess", game_key="second", white="a", black="b")
        second_version = add_version(conn, second, "two")
        assert state.get_job(conn, job_id(result)).provider is None
        assert JobRunner(conn, stage="acquisition").run(max_jobs=1).claimed == 0
        assert JobRunner(conn, stage="processing").run(max_jobs=1).done == 1
        record = artifacts.get_archive_job(conn, job_id(result), "alpha")
        saved = working_sets.get_working_set(conn, record["working_set_id"], "alpha")
        assert saved["member_count"] == 2, "selection occurs when processing begins"
        page = working_sets.member_page(conn, record["working_set_id"], "alpha")
        assert [row["game_version_id"] for row in page["items"]] == [first_version, second_version]
        signature = saved["input_signature"]
        add_version(conn, first, "new")
        state.mark_job(conn, job_id(result), "pending")
        assert JobRunner(conn, stage="processing").run(max_jobs=1).done == 1
        assert working_sets.get_working_set(conn, record["working_set_id"], "alpha")["input_signature"] == signature
        assert require_row(conn.execute("SELECT normalization_units FROM work_budgets WHERE crawl_run_id=%s", (result["run_id"],)))[0] == 2


def test_export_uses_shared_renderer_and_private_bounded_artifacts(database_url, tmp_path, monkeypatch):
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_DIRECTORY", str(tmp_path))
    with connection(database_url, mode="rw") as conn:
        seed_game(conn, provider="lichess", game_key="first", white="a", black="b")
        result = submit(conn, "prepare_export", {"kind": "games", "provider": "lichess"})
        assert JobRunner(conn, stage="processing").run(max_jobs=1).done == 1
        record = artifacts.artifact_record(conn, job_id(result), "alpha")
        manifest = artifacts.validate_artifact_manifest(record)
        references = artifacts.chunk_page(conn, job_id(result))
        assert all(row["object_key"].startswith(artifacts.artifact_prefix("alpha", job_id(result))) for row in references)
        assert all(row["body_bytes"] <= artifacts.CHUNK_BYTES for row in references)
        payload = b"".join(read_archive_reference(row) for row in references)
        spool = io.StringIO()
        snapshot = write_export_snapshot(database_url, "games", "lichess", "alpha", spool=spool,
                                         limits=ExportRenderLimits(1000, 1000000, 2), deadline=time.monotonic() + 60)
        assert payload == spool.getvalue().encode()
        assert manifest["content_hash"] == snapshot.content_hash
        assert manifest["rows"] == 1 and manifest["selection_time"] == "processing"
        with pytest.raises(DatabaseError, match="immutable"):
            with transaction(conn):
                conn.execute("DELETE FROM artifact_chunks WHERE job_id=%s", (job_id(result),))
        with pytest.raises(DatabaseError, match="immutable"):
            with transaction(conn):
                conn.execute("UPDATE archive_jobs SET request='{}' WHERE job_id=%s", (job_id(result),))
        state.mark_job(conn, job_id(result), "pending")
        assert JobRunner(conn, stage="processing").run(max_jobs=1).done == 1
        assert artifacts.artifact_record(conn, job_id(result), "alpha")["manifest"] == manifest


def test_idempotency_and_reserved_workspace_capacity_are_atomic(initialized_conn):
    conn = initialized_conn
    settings = replace(ArchiveJobSettings(), artifact_max_count=1)
    result = submit(conn, "prepare_export", {"kind": "users"}, settings=settings)
    assert submit(conn, "prepare_export", {"kind": "users"}, settings=settings)["replayed"]
    with pytest.raises(Conflict):
        submit(conn, "prepare_export", {"kind": "games"}, settings=settings)
    with pytest.raises(QuotaExceeded, match="artifacts"):
        submit(conn, "prepare_export", {"kind": "users"}, settings=settings, key="another")
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_jobs"))[0] == 1
    other = submit(conn, "prepare_export", {"kind": "users"}, settings=settings, workspace="beta")
    assert job_id(other) != job_id(result)


def test_partial_publication_retry_cleans_only_private_objects(database_url, tmp_path, monkeypatch):
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_DIRECTORY", str(tmp_path))
    with connection(database_url, mode="rw") as conn:
        seed_game(conn, provider="lichess", game_key="first", white="a", black="b")
        result = submit(conn, "prepare_export", {"kind": "users"})
        finalize = artifacts.finish_artifact
        with monkeypatch.context() as crash:
            crash.setattr(artifacts, "finish_artifact", lambda *args: (_ for _ in ()).throw(SystemExit("crash")))
            with pytest.raises(SystemExit, match="crash"):
                JobRunner(conn, stage="processing").run(max_jobs=1)
        old_reference = artifacts.chunk_page(conn, job_id(result))[0]
        monkeypatch.setattr(artifacts, "finish_artifact", finalize)
        assert JobRunner(conn, stage="processing").run(max_jobs=1, resume_stale=True).done == 1
        new_reference = artifacts.chunk_page(conn, job_id(result))[0]
        assert new_reference["object_key"] != old_reference["object_key"]
        # A stale request already in flight can finish after the new owner;
        # its attempt-specific key cannot delete the new immutable bytes.
        LocalObjectStore(old_reference["location"]).delete(old_reference["object_key"])
        assert read_archive_reference(new_reference)
        assert artifacts.artifact_record(conn, job_id(result), "alpha")["state"] == "ready"


def test_bulk_processing_budget_exhaustion_does_not_finalize_selection(database_url, monkeypatch):
    policy = replace(POLICY, job_max_normalization_units=1)
    with connection(database_url, mode="rw") as conn:
        for key in ("one", "two"):
            game, *_ = seed_game(conn, provider="lichess", game_key=key, white="a", black="b")
            add_version(conn, game, key)
        result = submit(conn, "build_working_set", {"name": "sample", "filters": {}, "settings": {}}, policy=policy)
        canonical = working_sets.canonical
        def no_membership_hash_after_budget_overflow(value):
            if isinstance(value, dict) and value.get("schema") == 1 and "filters" in value:
                pytest.fail("Budget overflow must stop before hashing the selection")
            return canonical(value)
        monkeypatch.setattr(working_sets, "canonical", no_membership_hash_after_budget_overflow)
        assert JobRunner(conn, stage="processing", budget_policy=policy).run(max_jobs=1).blocked == 1
        assert artifacts.get_archive_job(conn, job_id(result), "alpha")["working_set_id"] is None
        assert require_row(conn.execute("SELECT COUNT(*) FROM working_sets"))[0] == 0
        budget = require_row(conn.execute("SELECT id FROM work_budgets WHERE crawl_run_id=%s", (result["run_id"],)))[0]
        with pytest.raises(BudgetExceeded):
            work_budgets.reserve_normalization(conn, budget, units=2)
        with pytest.raises(ValueError):
            work_budgets.reserve_normalization(conn, budget, units=0)


def test_pruning_respects_downloads_and_releases_quota_only_after_deletion(database_url, tmp_path, monkeypatch):
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_DIRECTORY", str(tmp_path))
    settings = replace(ArchiveJobSettings(), artifact_max_count=1)
    with connection(database_url, mode="rw") as conn:
        source_store = LocalObjectStore(str(tmp_path))
        source = prepare_archive_object(b"public source evidence", store=source_store)
        result = submit(conn, "prepare_export", {"kind": "graph"}, settings=settings)
        assert JobRunner(conn, stage="processing").run(max_jobs=1).done == 1
        lease, record = artifacts.acquire_download(conn, job_id(result), "alpha", lifetime=300, slots=2)
        assert artifacts.prune_artifacts(conn, "alpha", before=record["expires_at"]) == {"expired": 0, "deleted_chunks": 0, "released_bytes": 0}
        with pytest.raises(QuotaExceeded):
            submit(conn, "prepare_export", {"kind": "users"}, settings=settings, key="another")
        artifacts.release_download(conn, lease, "alpha")
        monkeypatch.setattr(artifacts.time, "time", lambda: record["expires_at"] + 1)
        pruned = artifacts.prune_artifacts(conn, "alpha", before=record["expires_at"])
        assert pruned["expired"] == 1 and pruned["deleted_chunks"] == 1
        assert source_store.read(source.object_key, expected_size=source.stored_bytes)
        assert len(list(tmp_path.rglob("*.gz"))) == 1
        assert submit(conn, "prepare_export", {"kind": "users"}, settings=settings, key="another")


def test_provider_kind_validation_precedes_database_writes(initialized_conn):
    conn = initialized_conn
    with pytest.raises(ValueError):
        state.enqueue_job(conn, provider=None, kind="fetch_user_profile", target="a")
    with pytest.raises(ValueError):
        state.enqueue_job(conn, provider="lichess", kind="prepare_export", target="x")
    with pytest.raises(ValueError):
        state.create_crawl_run(conn, provider=None, seed_spec="bad", params={})
    for provider, kind in ((None, "fetch_user_profile"), ("lichess", "prepare_export")):
        with pytest.raises(DatabaseError, match="service_job_provider"):
            with transaction(conn):
                conn.execute("INSERT INTO discovery_jobs(provider,kind,target,dedup_key,enqueued_at) VALUES(%s,%s,'a','direct-sql',1)", (provider, kind))
    with pytest.raises(DatabaseError, match="service_run_provider"):
        with transaction(conn):
            conn.execute("INSERT INTO crawl_runs(provider,seed_spec,params_json,status,started_at,updated_at) VALUES(NULL,'bad','{}','running',1,1)")
    assert require_row(conn.execute("SELECT COUNT(*) FROM discovery_jobs"))[0] == 0


def test_stopped_internal_handler_is_pending_and_durable(initialized_conn):
    conn = initialized_conn
    result = submit(conn, "build_working_set", {"name": "empty", "filters": {}, "settings": {}})
    job = state.get_job(conn, job_id(result))
    handler = internal.handler_for(job.kind)
    assert handler(conn, job, stop_requested=lambda: True)["state"] == "pending"
    assert artifacts.get_archive_job(conn, job.id, "alpha")["working_set_id"] is None


def test_export_reuses_owned_session_with_one_reserved_heartbeat_connection(database_url, tmp_path, monkeypatch):
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_DIRECTORY", str(tmp_path))
    with connection(database_url, mode="rw") as conn, connection(database_url) as heartbeat:
        seed_game(conn, provider="lichess", game_key="one", white="a", black="b")
        result = submit(conn, "prepare_export", {"kind": "users"})
        assert not heartbeat.closed
        def no_third_connection(*args, **kwargs):
            raise AssertionError("The export cannot open a third worker connection")
        monkeypatch.setattr(type(conn), "connect", no_third_connection)
        assert JobRunner(conn, stage="processing").run(max_jobs=1).done == 1
        assert artifacts.artifact_record(conn, job_id(result), "alpha")["state"] == "ready"


def test_snapshot_capacity_retries_without_exhausting_work_budget(database_url, tmp_path, monkeypatch):
    from chess_crawl.storage.db import lock_key
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_DIRECTORY", str(tmp_path))
    with connection(database_url, mode="rw") as conn, connection(database_url, mode="rw") as busy:
        result = submit(conn, "prepare_export", {"kind": "graph"})
        with transaction(busy):
            for slot in range(2):
                busy.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key("export-preparation", f"alpha:{slot}"),))
            assert JobRunner(conn, stage="processing").run(max_jobs=1).blocked == 1
            job = state.get_job(conn, job_id(result))
            assert job.retry_count == 1 and job.next_attempt_at is not None
            assert require_row(conn.execute("SELECT exhausted_dimension FROM work_budgets WHERE crawl_run_id=%s", (result["run_id"],)))[0] is None
        conn.execute("UPDATE discovery_jobs SET next_attempt_at=0 WHERE id=%s", (job_id(result),))
        assert JobRunner(conn, stage="processing").run(max_jobs=1).done == 1


def test_archive_admission_ignores_exhausted_remote_and_import_game_allowances(database_url, tmp_path, monkeypatch):
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_DIRECTORY", str(tmp_path))
    with connection(database_url, mode="rw") as conn:
        first = submit(conn, "build_working_set", {"name": "empty", "filters": {}, "settings": {}})
        conn.execute("UPDATE workspace_budget_periods SET remote_requests=%s,remote_bytes=%s,games=%s WHERE workspace_id='alpha'", (POLICY.workspace_max_remote_requests, POLICY.workspace_max_remote_bytes, POLICY.workspace_max_games))
        second = submit(conn, "prepare_export", {"kind": "graph"}, key="export")
        assert JobRunner(conn, stage="processing").run(max_jobs=2).done == 2
        assert job_id(first) != job_id(second)
        assert artifacts.artifact_record(conn, job_id(second), "alpha")["state"] == "ready"
