from __future__ import annotations

import json
import sys

import pytest

from chess_crawl.jobs import state
from chess_crawl.jobs import dispatch as dispatch_module
from chess_crawl.jobs.dispatch import DispatchMaintenance, SqsConsumer, SqsDispatcher
from chess_crawl.jobs.runner import ExecutionOutcome, JobRunner
from chess_crawl.jobs.worker import Worker
from chess_crawl.storage import migrations
from chess_crawl.storage.db import connection, require_row, transaction
from chess_crawl.storage.execution import maintain_dispatch, pending_dispatch


def enqueue(conn, target):
    return state.enqueue_job(conn, provider="lichess", kind="normalize_payload", target=target).job_id


def test_busy_worker_drains_database_and_duplicate_hints_without_long_poll(database_url, monkeypatch):
    clock = [100.0]
    waits = []
    completed = []
    deleted = []
    scans = []
    recover = state.resume_stale_in_progress
    with connection(database_url, mode="rw") as conn:
        jobs = [enqueue(conn, str(index)) for index in range(3)]
    hints = list(jobs)

    class Queue:
        def receive_message(self, **kwargs):
            wait = kwargs["WaitTimeSeconds"]
            waits.append(wait)
            clock[0] += wait
            if hints:
                return {"Messages": [{"Body": json.dumps({"job_id": hints[0]}), "ReceiptHandle": str(hints[0])}]}
            worker.request_stop()
            return {}

        def delete_message(self, **kwargs):
            deleted.append(int(kwargs["ReceiptHandle"]))
            hints.pop(0)

    def execute(self, job):
        completed.append((job.id, clock[0]))
        return ExecutionOutcome("done", "fixture")

    def recover_once(*args, **kwargs):
        scans.append(True)
        return recover(*args, **kwargs)

    monkeypatch.setattr(JobRunner, "_execute", execute)
    monkeypatch.setattr(state, "resume_stale_in_progress", recover_once)
    worker = Worker(database_url, queue_consumer=SqsConsumer(Queue(), "queue"),
                    stage="processing", clock=lambda: clock[0], sleeper=lambda delay: None)
    assert worker.run() == 3
    assert completed == [(identity, 100.0) for identity in jobs]
    assert waits == [0, 0, 0, 20]
    assert deleted == jobs
    assert len(scans) == 4  # One pass per loop, including the final empty poll.


def test_idle_worker_recovers_job_orphaned_during_empty_queue_poll(database_url, monkeypatch):
    job_ids = []

    class Queue:
        def receive_message(self, **kwargs):
            assert kwargs["WaitTimeSeconds"] == 20
            with connection(database_url, mode="rw") as lost_owner:
                identity = enqueue(lost_owner, "orphan")
                job_ids.append(identity)
                assert state.claim_next_job(lost_owner, job_id=identity) is not None
            return {}

        def delete_message(self, **kwargs):
            pytest.fail("An empty queue has nothing to acknowledge")

    def execute(self, job):
        worker.request_stop()
        return ExecutionOutcome("done", "recovered")

    monkeypatch.setattr(JobRunner, "_execute", execute)
    worker = Worker(database_url, queue_consumer=SqsConsumer(Queue(), "queue"), sleeper=lambda delay: None)
    assert worker.run() == 1
    with connection(database_url, mode="rw") as conn:
        job = state.get_job(conn, job_ids[0])
        assert job is not None and job.state == "done" and job.attempts == 2


def test_local_revisions_retire_hints_and_preserve_delayed_retry(initialized_conn):
    conn = initialized_conn
    identity = enqueue(conn, "retry")
    first = require_row(conn.execute("SELECT id FROM dispatch_outbox WHERE job_id=%s", (identity,)))[0]
    # A revision change without a state change must not strand its replacement.
    conn.execute("UPDATE discovery_jobs SET reason='new parameters' WHERE id=%s", (identity,))
    assert require_row(conn.execute("SELECT superseded_at FROM dispatch_outbox WHERE id=%s", (first,)))[0] is not None
    assert state.claim_next_job(conn, job_id=identity, now=100) is not None
    state.finish_attempt(conn, identity, "blocked", reason="retry", transient=True, retry_after=100, now=100)
    state.release_job_ownership(conn)
    rows = list(conn.execute("SELECT * FROM dispatch_outbox WHERE job_id=%s ORDER BY id", (identity,)))
    assert len(rows) == 3
    assert all(row["superseded_at"] is not None for row in rows[:-1])
    assert rows[-1]["superseded_at"] is None and rows[-1]["available_at"] == 200
    maintain_dispatch(conn, now=10**12, retention_seconds=1, limit=256)
    retained = list(conn.execute("SELECT * FROM dispatch_outbox WHERE job_id=%s", (identity,)))
    assert len(retained) == 1 and retained[0]["id"] == rows[-1]["id"]


def test_bounded_legacy_scan_crosses_live_rows_and_wraps(initialized_conn):
    conn = initialized_conn
    live = [enqueue(conn, f"live-{index}") for index in range(3)]
    obsolete = [enqueue(conn, f"old-{index}") for index in range(3)]
    for identity in obsolete:
        state.mark_done(conn, identity)
    # Model the unmigrated backlog: a terminal job's hint was never retired.
    conn.execute("UPDATE dispatch_outbox SET superseded_at=NULL")
    cursor = 0
    retired = []
    for _ in range(4):
        cursor = maintain_dispatch(conn, now=100, retention_seconds=10, limit=2, after_id=cursor)
        retired.append(require_row(conn.execute("SELECT COUNT(*) FROM dispatch_outbox WHERE superseded_at IS NOT NULL"))[0])
    assert retired == [0, 1, 3, 3] and cursor == 0
    cursor = maintain_dispatch(conn, now=111, retention_seconds=10, limit=2)
    assert require_row(conn.execute("SELECT COUNT(*) FROM dispatch_outbox"))[0] == 4
    maintain_dispatch(conn, now=111, retention_seconds=10, limit=2, after_id=cursor)
    assert [row[0] for row in conn.execute("SELECT job_id FROM dispatch_outbox ORDER BY id")] == live


def test_retention_keeps_recent_history_and_skips_locked_rows(database_url):
    with connection(database_url, mode="rw") as conn, connection(database_url, mode="rw") as holder:
        jobs = [enqueue(conn, str(index)) for index in range(4)]
        for identity in jobs:
            state.mark_done(conn, identity)
        conn.execute("UPDATE dispatch_outbox SET superseded_at=CASE WHEN job_id=%s THEN 95 ELSE 1 END", (jobs[-1],))
        conn.execute("UPDATE dispatch_outbox SET delivered_at=1,superseded_at=NULL WHERE job_id=%s", (jobs[1],))
        with transaction(holder):
            holder.execute("SELECT id FROM dispatch_outbox WHERE job_id=%s FOR UPDATE", (jobs[0],))
            maintain_dispatch(conn, now=100, retention_seconds=10, limit=2)
            assert [row[0] for row in conn.execute("SELECT job_id FROM dispatch_outbox ORDER BY id")] == [jobs[0], jobs[-1]]
        maintain_dispatch(conn, now=100, retention_seconds=10, limit=2)
        assert [row[0] for row in conn.execute("SELECT job_id FROM dispatch_outbox")] == [jobs[-1]]


def test_local_worker_prunes_history_without_sqs(database_url):
    with connection(database_url, mode="rw") as conn:
        old = enqueue(conn, "completed")
        state.mark_done(conn, old)
    assert Worker(database_url, clock=lambda: 10**12).run(once=True) == 0
    with connection(database_url, mode="rw") as conn:
        assert require_row(conn.execute("SELECT COUNT(*) FROM dispatch_outbox"))[0] == 0
        assert state.get_job(conn, old).state == "done"


def test_dispatcher_maintenance_runs_while_idle_at_configured_interval(initialized_conn, monkeypatch):
    clock = [100.0]
    calls = []

    def maintain(conn, **kwargs):
        calls.append(kwargs)
        return kwargs["after_id"] + 1

    monkeypatch.setattr("chess_crawl.jobs.dispatch.maintain_dispatch", maintain)
    dispatcher = SqsDispatcher(object(), "queue", clock=lambda: clock[0])
    assert not dispatcher.publish_one(initialized_conn)
    assert not dispatcher.publish_one(initialized_conn)
    clock[0] = 160
    assert not dispatcher.publish_one(initialized_conn)
    assert [call["after_id"] for call in calls] == [0, 1]


def test_migration_leaves_legacy_backlog_for_bounded_cleanup(uninitialized_database_url, monkeypatch):
    available = migrations.migration_resources()
    with connection(uninitialized_database_url, mode="rw") as conn:
        with monkeypatch.context() as old:
            old.setattr(migrations, "migration_resources", lambda: tuple(item for item in available if item[0] < 15))
            old.setattr(migrations, "SCHEMA_VERSION", 14)
            migrations.initialize(conn)
        identity = enqueue(conn, "before-upgrade")
        state.mark_done(conn, identity)
        assert migrations.initialize(conn).applied == tuple(item[1] for item in available if item[0] >= 15)
        assert require_row(conn.execute("SELECT superseded_at FROM dispatch_outbox"))[0] is None
        maintain_dispatch(conn, now=100, retention_seconds=10, limit=1)
        assert require_row(conn.execute("SELECT superseded_at FROM dispatch_outbox"))[0] == 100


def test_revision_hint_replacement_rolls_back_with_job(initialized_conn):
    conn = initialized_conn
    identity = enqueue(conn, "transactional")
    with pytest.raises(RuntimeError, match="rollback"):
        with transaction(conn):
            conn.execute("UPDATE discovery_jobs SET reason='changed' WHERE id=%s", (identity,))
            assert require_row(conn.execute("SELECT COUNT(*) FROM dispatch_outbox"))[0] == 2
            raise RuntimeError("rollback")
    rows = list(conn.execute("SELECT * FROM dispatch_outbox"))
    assert len(rows) == 1 and rows[0]["superseded_at"] is None


def test_job_transition_does_not_wait_for_inflight_dispatch(database_url):
    with connection(database_url, mode="rw") as worker, connection(database_url, mode="rw") as dispatcher:
        identity = enqueue(worker, "inflight")
        with transaction(dispatcher):
            dispatcher.execute("SELECT id FROM dispatch_outbox WHERE job_id=%s FOR UPDATE", (identity,))
            # An SQS request owns this hint row; DB work must still complete.
            worker.execute("SET lock_timeout='200ms'")
            state.mark_done(worker, identity)
            assert state.get_job(worker, identity).state == "done"
            assert require_row(worker.execute("SELECT superseded_at FROM dispatch_outbox"))[0] is None
        maintain_dispatch(worker, now=100, retention_seconds=10, limit=1)
        assert require_row(worker.execute("SELECT superseded_at FROM dispatch_outbox"))[0] == 100


def test_dispatch_bounds_join_work_over_large_legacy_prefix(initialized_conn, monkeypatch):
    conn = initialized_conn
    terminal = enqueue(conn, "legacy")
    state.mark_done(conn, terminal)
    conn.execute(
        "INSERT INTO dispatch_outbox(job_id,job_revision) SELECT %s,-n FROM generate_series(1,5000) n",
        (terminal,),
    )
    enqueue(conn, "ready-after-prefix")
    conn.execute("ANALYZE dispatch_outbox")
    execute = conn.execute
    plans = []

    def explain_select(query, *args, **kwargs):
        if "SELECT id,available_at FROM dispatch_outbox" in str(query) or "WHERE d.id=ANY" in str(query):
            plan = require_row(execute("EXPLAIN (ANALYZE, FORMAT JSON) " + query, *args, **kwargs))[0][0]["Plan"]
            plans.append(plan)
        return execute(query, *args, **kwargs)

    def scans(node):
        found = [node] if node.get("Relation Name") == "dispatch_outbox" else []
        return found + [entry for child in node.get("Plans", []) for entry in scans(child)]

    monkeypatch.setattr(conn, "execute", explain_select)
    with transaction(conn):
        row, cursor = pending_dispatch(conn, now=100, limit=32)
    assert row is None and cursor is not None
    assert len(plans) == 2
    for plan in plans:
        for scan in scans(plan):
            assert "Index" in scan["Node Type"]
            assert (scan["Actual Rows"] + scan.get("Rows Removed by Filter", 0)) * scan["Actual Loops"] <= 32
    assert require_row(conn.execute("SELECT COUNT(*) FROM dispatch_outbox WHERE superseded_at=100"))[0] == 32
    with transaction(conn):
        row, next_cursor = pending_dispatch(conn, now=100, limit=32, after=cursor)
    assert row is None and next_cursor is not None and next_cursor > cursor
    assert require_row(conn.execute("SELECT COUNT(*) FROM dispatch_outbox WHERE superseded_at=100"))[0] == 64


def test_dispatch_advances_past_locked_window_then_returns_to_oldest(database_url):
    published = []

    class Queue:
        def send_message(self, **kwargs):
            published.append(json.loads(kwargs["MessageBody"])["job_id"])

    dispatcher = SqsDispatcher(Queue(), "queue", clock=lambda: 100)
    dispatcher.maintenance = DispatchMaintenance(batch_size=2, clock=lambda: 100)
    with connection(database_url, mode="rw") as conn, connection(database_url, mode="rw") as holder:
        jobs = [enqueue(conn, str(index)) for index in range(3)]
        with transaction(holder):
            holder.execute("SELECT id FROM dispatch_outbox WHERE job_id=ANY(%s) FOR UPDATE", (jobs[:2],))
            assert not dispatcher.publish_one(conn)
            assert dispatcher.has_more
            assert dispatcher.publish_one(conn)
            assert published == [jobs[2]]
        assert dispatcher.publish_one(conn)
        assert dispatcher.publish_one(conn)
        assert published == [jobs[2], jobs[0], jobs[1]]
        assert not dispatcher.publish_one(conn)
        assert not dispatcher.has_more


def test_dispatch_once_traverses_obsolete_windows_and_sends_only_one(database_url, monkeypatch):
    published = []

    class Queue:
        def send_message(self, **kwargs):
            published.append(json.loads(kwargs["MessageBody"])["job_id"])

    with connection(database_url, mode="rw") as conn:
        terminal = enqueue(conn, "old")
        state.mark_done(conn, terminal)
        conn.execute(
            "INSERT INTO dispatch_outbox(job_id,job_revision) SELECT %s,-n FROM generate_series(1,6) n",
            (terminal,),
        )
        ready = [enqueue(conn, str(index)) for index in range(2)]
    monkeypatch.setenv("CHESS_CRAWL_DISPATCH_CLEANUP_BATCH_SIZE", "2")
    monkeypatch.setattr(dispatch_module, "aws_sqs_client", lambda: Queue())
    monkeypatch.setattr(sys, "argv", ["dispatch", "--database-url", database_url, "--queue-url", "queue", "--once"])
    assert dispatch_module.main() == 0
    assert published == ready[:1]
    with connection(database_url, mode="rw") as conn:
        assert require_row(conn.execute("SELECT delivered_at FROM dispatch_outbox WHERE job_id=%s", (ready[1],)))[0] is None


@pytest.mark.parametrize("setting,value", [
    ("RETENTION_SECONDS", "nan"), ("CLEANUP_INTERVAL_SECONDS", "0"),
    ("CLEANUP_BATCH_SIZE", "10001"), ("CLEANUP_BATCH_SIZE", "1.5"),
])
def test_dispatch_maintenance_rejects_invalid_settings(monkeypatch, setting, value):
    monkeypatch.setenv(f"CHESS_CRAWL_DISPATCH_{setting}", value)
    with pytest.raises(ValueError):
        DispatchMaintenance.from_env()
