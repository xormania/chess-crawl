"""Snapshot identities let consumers reconcile asynchronous event delivery."""

from __future__ import annotations

import json

import pytest

from chess_crawl import application
from chess_crawl.jobs import state
from chess_crawl.storage import migrations
from chess_crawl.storage.db import connection, open_database, require_row
from chess_crawl.storage.events import archive_id


def create_run(conn) -> tuple[int, int]:
    return state.create_crawl_run_with_root_job(
        conn, provider="lichess", seed_spec="alice", params={},
        root_kind="fetch_user_profile", root_target="alice",
    )


def test_job_and_run_snapshots_match_their_latest_event_revisions(initialized_conn) -> None:
    conn = initialized_conn
    run_id, job_id = create_run(conn)
    initial = application.get_job(conn, job_id)
    state.mark_done(conn, job_id)
    state.refresh_run_status(conn, run_id)
    job = application.get_job(conn, job_id)
    run = application.get_run(conn, run_id)
    for snapshot, event_type, resource_id in (
        (job, "job.updated", job_id), (run, "run.updated", run_id),
    ):
        event = conn.execute(
            "SELECT revision, payload FROM event_outbox WHERE event_type = %s AND resource_id = %s ORDER BY id DESC LIMIT 1",
            (event_type, resource_id),
        ).fetchone()
        assert event is not None
        assert snapshot["archive_id"] == archive_id(conn)
        assert snapshot["revision"] == event["revision"] == 2
        assert json.loads(event["payload"])["status"] == "done"
        json.dumps(snapshot)
    assert initial["revision"] == 1
    assert job["revision"] > initial["revision"]
    assert job["state"] == run["status"] == "done"


def test_claimed_job_matches_persisted_snapshot_and_event_revision(initialized_conn) -> None:
    conn = initialized_conn
    _, job_id = create_run(conn)

    claimed = state.claim_next_job(conn, now=100)
    assert claimed is not None
    snapshot = application.get_job(conn, job_id)
    event = conn.execute(
        "SELECT revision, payload FROM event_outbox WHERE event_type = 'job.updated' AND resource_id = %s ORDER BY id DESC LIMIT 1",
        (job_id,),
    ).fetchone()

    assert event is not None
    assert claimed == state.get_job(conn, job_id)
    assert claimed.revision == snapshot["revision"] == event["revision"] == 2
    assert claimed.state == snapshot["state"] == json.loads(event["payload"])["status"] == "in_progress"
    assert claimed.attempts == snapshot["attempts"] == 1
    assert state.claim_next_job(conn, now=100) is None


def test_archive_identity_disambiguates_matching_local_resource_ids(database_factory) -> None:
    snapshots = []
    for _ in range(2):
        with open_database(database_factory(), writable=True) as conn:
            run_id, job_id = create_run(conn)
            snapshots.append((application.get_run(conn, run_id), application.get_job(conn, job_id)))
    assert snapshots[0][0]["id"] == snapshots[1][0]["id"] == 1
    assert snapshots[0][1]["id"] == snapshots[1][1]["id"] == 1
    assert snapshots[0][0]["archive_id"] != snapshots[1][0]["archive_id"]
    for run, job in snapshots:
        assert run["archive_id"] == job["archive_id"]


def test_archive_identity_and_revision_survive_reopening(database_url: str) -> None:
    with connection(database_url, mode="rw") as conn:
        run_id, job_id = create_run(conn)
        original = application.get_job(conn, job_id)
    with connection(database_url) as reopened:
        assert application.get_job(reopened, job_id) == original
        assert application.get_run(reopened, run_id)["archive_id"] == original["archive_id"]


def test_upgraded_snapshots_start_at_revision_zero_without_fabricated_events(uninitialized_database_url: str, monkeypatch) -> None:
    packaged = migrations.migration_resources()
    with connection(uninitialized_database_url, mode="rwc") as conn:
        with monkeypatch.context() as patch:
            patch.setattr(migrations, "migration_resources", lambda: tuple(item for item in packaged if item[0] < 4))
            migrations.initialize(conn)
            run_id, job_id = create_run(conn)
            job = state.get_job(conn, job_id)
            assert job is not None
            assert job.revision == 0
        migrations.initialize(conn)
        assert application.get_job(conn, job_id)["revision"] == 0
        assert application.get_run(conn, run_id)["revision"] == 0
        assert require_row(conn.execute("SELECT COUNT(*) FROM event_outbox"))[0] == 0


def test_job_snapshot_keeps_revision_and_state_from_one_read(database_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    with connection(database_url, mode="rw") as writer:
        _, job_id = create_run(writer)
        original = state.get_job

        def complete_after_read(conn, target):
            job = original(conn, target)
            state.mark_done(writer, target)
            return job

        with connection(database_url) as reader:
            with monkeypatch.context() as patch:
                patch.setattr(state, "get_job", complete_after_read)
                old = application.get_job(reader, job_id)
            fresh = application.get_job(reader, job_id)
            assert (old["state"], old["revision"]) == ("pending", 1)
            assert (fresh["state"], fresh["revision"]) == ("done", 2)
            assert old["archive_id"] == fresh["archive_id"]
