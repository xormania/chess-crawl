from __future__ import annotations

import os
import json
import signal
import psycopg
from psycopg.conninfo import make_conninfo
from helpers.processes import child_process, expect_output
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.ingest import IngestResult
from chess_crawl.jobs import runner as runner_module, state, worker as worker_module
from chess_crawl.jobs.locking import ExecutorBusy, archive_lock
from chess_crawl.jobs import acquisition as acquisition_module
from chess_crawl.jobs.runner import ExecutionOutcome, JobRunner
from chess_crawl.jobs.settings import WorkerSettings
from chess_crawl.jobs.worker import Worker
from chess_crawl.storage.db import connection, open_database, require_row, transaction


def test_advisory_lock_excludes_live_owner_and_releases_after_process_exit(database_url: str) -> None:
    script = (
        "import sys\nfrom chess_crawl.jobs.locking import archive_lock\n"
        "with archive_lock(sys.argv[1]):\n print('locked', flush=True)\n sys.stdin.readline()\n"
    )
    with child_process(
        [sys.executable, "-c", script, database_url],
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
    ) as child:
        expect_output(child, "locked")
        with pytest.raises(ExecutorBusy):
            with archive_lock(database_url):
                pass
        # Connection spelling/options do not define ownership: the database does.
        alias = make_conninfo(database_url, application_name="alternate-connection")
        with pytest.raises(ExecutorBusy):
            with archive_lock(alias):
                pass
        with open_database(database_url, writable=True) as conn:
            with pytest.raises(ExecutorBusy):
                JobRunner(conn).run(resume_stale=True)
            with pytest.raises(ExecutorBusy):
                state.resume_stale_in_progress(conn, now=10**10)
        # Independent publisher locks do not conflict with execution ownership.
        with archive_lock(database_url, purpose="events"):
            pass
    with archive_lock(database_url):
        pass


def test_simultaneous_contenders_elect_exactly_one_owner(database_url: str) -> None:
    start = threading.Barrier(3)
    release = threading.Event()
    changed = threading.Condition()
    outcomes: list[str] = []
    errors: list[BaseException] = []

    def record(outcome: str) -> None:
        with changed:
            outcomes.append(outcome)
            changed.notify_all()

    def contend() -> None:
        try:
            start.wait(timeout=5)
            try:
                with archive_lock(database_url):
                    record("owner")
                    assert release.wait(5), "ownership test did not release winner"
            except ExecutorBusy:
                record("busy")
        except BaseException as exc:
            with changed:
                errors.append(exc)
                changed.notify_all()

    threads = [threading.Thread(target=contend) for _ in range(2)]
    for thread in threads:
        thread.start()
    try:
        start.wait(timeout=5)
        with changed:
            assert changed.wait_for(lambda: len(outcomes) == 2 or bool(errors), timeout=5), outcomes
        assert errors == []
        assert sorted(outcomes) == ["busy", "owner"]
    finally:
        release.set()
        for thread in threads:
            thread.join(timeout=5)
    assert all(not thread.is_alive() for thread in threads)
    with archive_lock(database_url):
        pass


def test_killed_ownership_session_cannot_recover_or_claim_work(database_url: str) -> None:
    with open_database(database_url, writable=True) as setup:
        _, job_id = state.create_crawl_run_with_root_job(
            setup, provider="lichess", seed_spec="orphan", params={},
            root_kind="fetch_user_profile", root_target="orphan",
        )
        state.claim_next_job(setup)
    with archive_lock(database_url) as lease:
        owner = lease.connection
        with connection(database_url, mode="rw") as terminator:
            assert require_row(terminator.execute("SELECT pg_terminate_backend(%s)", (owner.info.backend_pid,)))[0]
        with pytest.raises((RuntimeError, psycopg.Error)):
            JobRunner(owner, lease=lease).run(resume_stale=True)
        with pytest.raises((RuntimeError, psycopg.Error)):
            state.resume_stale_in_progress(owner, now=10**10, lease=lease)
        with archive_lock(database_url) as successor:
            successor.require(successor.connection)
    with open_database(database_url) as observer:
        job = state.get_job(observer, job_id)
        assert job is not None and job.state == "in_progress" and job.attempts == 1


def test_worker_cannot_complete_or_claim_more_work_after_its_session_dies(
    database_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    with open_database(database_url, writable=True) as setup:
        first = state.enqueue_job(setup, provider="lichess", kind="fetch_user_profile", target="first").job_id
        second = state.enqueue_job(setup, provider="lichess", kind="fetch_user_profile", target="second").job_id
    executed: list[int] = []

    def lose_session(self, job):
        executed.append(job.id)
        with connection(database_url, mode="rw") as terminator:
            assert require_row(terminator.execute("SELECT pg_terminate_backend(%s)", (self.conn.info.backend_pid,)))[0]
        return ExecutionOutcome("done", "fixture completed after connection loss")

    monkeypatch.setattr(JobRunner, "_execute", lose_session)
    worker = Worker(database_url, settings=WorkerSettings(heartbeat_interval=0.02))
    with pytest.raises((RuntimeError, psycopg.Error)):
        worker.run()
    assert executed == [first]
    with open_database(database_url) as observer:
        interrupted = state.get_job(observer, first)
        pending = state.get_job(observer, second)
        assert interrupted is not None and interrupted.state == "in_progress" and interrupted.attempts == 1
        assert pending is not None and pending.state == "pending" and pending.attempts == 0
    with archive_lock(database_url) as successor:
        successor.require(successor.connection)


def test_released_database_lock_invalidates_a_live_connection_lease(database_url: str) -> None:
    with archive_lock(database_url) as lease:
        owner = lease.connection
        owner.execute("SELECT pg_advisory_unlock_all()")
        with archive_lock(database_url) as successor:
            successor.require(successor.connection)
            with pytest.raises(RuntimeError, match="lease|ownership|lock"):
                lease.require(owner)
            with pytest.raises(RuntimeError, match="lease|ownership|lock"):
                JobRunner(owner, lease=lease).run()


@pytest.mark.parametrize("status", [0, 429, 500, 503])
def test_retry_deadline_survives_restart_and_exhaustion_is_bounded(
    database_url: str, monkeypatch: pytest.MonkeyPatch, status: int,
) -> None:
    monkeypatch.setattr(
        runner_module, "fetch_user_profile",
        lambda *args, **kwargs: IngestResult("chess.com", "user_profile", status, None, (), "transient", retry_after=7),
    )
    settings = WorkerSettings(job_max_retries=1, job_retry_base_s=2, job_retry_max_s=10)
    now = [100.0]
    with open_database(database_url, writable=True) as conn:
        run_id, job_id = state.create_crawl_run_with_root_job(
            conn, provider="chess.com", seed_spec="retry", params={},
            root_kind="fetch_user_profile", root_target="test",
        )
        runner = JobRunner(conn, settings=settings, clock=lambda: now[0])
        assert runner.run(max_jobs=1).blocked == 1
        job = state.get_job(conn, job_id)
        assert job is not None and job.next_attempt_at == 107.0 and job.retry_count == 1
        run = state.get_run(conn, run_id)
        assert run is not None and run["status"] == "paused"
        assert runner.run(unblock=True).claimed == 0
    with open_database(database_url, writable=True) as conn:
        runner = JobRunner(conn, settings=settings, clock=lambda: now[0])
        now[0] = 106.9
        assert runner.run().claimed == 0
        now[0] = 107.0
        assert runner.run().errors == 1
        job = state.get_job(conn, job_id)
        assert job is not None and job.state == "error" and job.attempts == 2
        assert job.retry_count == 1 and job.next_attempt_at is None
        assert "Retry limit" in (job.reason or "")
        run = state.get_run(conn, run_id)
        assert run is not None and run["status"] == "failed"
        assert runner.run(unblock=True).claimed == 0


def test_lichess_retry_floor_cannot_be_capped_away(initialized_conn, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runner_module, "fetch_user_profile",
        lambda *args, **kwargs: IngestResult("lichess", "user_profile", 429, None, (), "limited"),
    )
    job_id = state.enqueue_job(initialized_conn, provider="lichess", kind="fetch_user_profile", target="test").job_id
    runner = JobRunner(
        initialized_conn, clock=lambda: 100.0,
        settings=WorkerSettings(job_retry_base_s=1, job_retry_max_s=2),
    )
    runner.run(max_jobs=1)
    job = state.get_job(initialized_conn, job_id)
    assert job is not None and job.next_attempt_at == 160.0


def test_provider_cooldown_survives_restart_and_blocks_other_jobs_after_retry_exhaustion(
    database_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    now = [100.0]

    def fetch(conn, provider, username, **kwargs):
        calls.append(username)
        return IngestResult(
            provider, "user_profile", 429 if username == "limited" else 200,
            None, (), "fixture", retry_after=120 if username == "limited" else None,
        )

    monkeypatch.setattr(runner_module, "fetch_user_profile", fetch)
    settings = WorkerSettings(job_max_retries=0, job_retry_base_s=1, job_retry_max_s=2)
    with open_database(database_url, writable=True) as conn:
        state.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="limited", priority=1)
        pending = state.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="other", priority=2).job_id
        state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="unaffected", priority=3)
        assert JobRunner(conn, settings=settings, clock=lambda: now[0]).run(max_jobs=1).errors == 1
        assert state.provider_ready_at(conn, "chess.com") == 220.0
    with open_database(database_url, writable=True) as conn:
        restarted = JobRunner(conn, settings=settings, clock=lambda: now[0])
        assert restarted.run(max_jobs=1).done == 1
        assert calls == ["limited", "unaffected"]
        now[0] = 219.9
        assert restarted.run().claimed == 0
        job = state.get_job(conn, pending)
        assert job is not None and job.state == "pending"
        now[0] = 220.0
        assert restarted.run().done == 1
        assert calls == ["limited", "unaffected", "other"]


def test_worker_recovers_orphans_once_and_preserves_cancelled_runs(
    database_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(JobRunner, "_execute", lambda self, job: ExecutionOutcome("done", "fixture"))
    with open_database(database_url, writable=True) as conn:
        _, job_id = state.create_crawl_run_with_root_job(
            conn, provider="lichess", seed_spec="orphan", params={},
            root_kind="fetch_user_profile", root_target="orphan",
        )
        state.claim_next_job(conn)
        cancelled, cancelled_job = state.create_crawl_run_with_root_job(
            conn, provider="lichess", seed_spec="cancelled", params={},
            root_kind="fetch_user_profile", root_target="cancelled",
        )
        state.claim_next_job(conn)
        state.update_crawl_run(conn, cancelled, status="cancelled", finished=True)
    assert Worker(database_url).run(once=True) == 1
    with open_database(database_url) as conn:
        job = state.get_job(conn, job_id)
        assert job is not None and job.state == "done" and job.attempts == 2
        job = state.get_job(conn, cancelled_job)
        assert job is not None and job.state == "in_progress" and job.attempts == 1
        assert state.worker_status(conn)["status"] == "stopped"
    assert Worker(database_url).run(once=True) == 0


def test_sqs_worker_falls_back_to_durable_pending_work(
    database_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    class EmptyQueue:
        def run_once(self, conn, execute, *, wait_seconds=None) -> int:
            del conn, execute
            return 0

    monkeypatch.setattr(JobRunner, "_execute", lambda self, job: ExecutionOutcome("done", "fixture"))
    with open_database(database_url, writable=True) as conn:
        job_id = state.enqueue_job(
            conn, provider="lichess", kind="normalize_payload", target="123",
        ).job_id

    assert Worker(database_url, queue_consumer=EmptyQueue()).run(once=True) == 1
    with open_database(database_url) as conn:
        job = state.get_job(conn, job_id)
        assert job is not None and job.state == "done" and job.attempts == 1


def test_snapshot_reflects_previous_completion_before_next_job(initialized_conn, monkeypatch) -> None:
    run_id, first = state.create_crawl_run_with_root_job(
        initialized_conn, provider="lichess", seed_spec="progress", params={},
        root_kind="fetch_user_profile", root_target="first",
    )
    second = state.enqueue_job(
        initialized_conn, provider="lichess", kind="fetch_user_profile", target="second", crawl_run_id=run_id,
    ).job_id

    def execute(self, job):
        if job.id == second:
            run = state.get_run(self.conn, run_id)
            assert run is not None and run["status"] == "running"
            assert json.loads(run["counters"])["jobs_done"] == 1
            assert state.get_job(self.conn, first).state == "done"
        return ExecutionOutcome("done", "fixture")

    monkeypatch.setattr(JobRunner, "_execute", execute)
    assert JobRunner(initialized_conn).run().done == 2


def test_sigterm_finishes_active_job_and_releases_process_ownership(database_url: str) -> None:
    with open_database(database_url, writable=True) as conn:
        first = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="first").job_id
        second = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="second").job_id
    script = (
        "import sys\nfrom chess_crawl.jobs.runner import JobRunner, ExecutionOutcome\n"
        "from chess_crawl.jobs.worker import main\n"
        "def execute(self, job):\n print('fetching', flush=True)\n sys.stdin.readline()\n"
        " return ExecutionOutcome('done', 'fixture')\n"
        "JobRunner._execute = execute\nraise SystemExit(main(['--database-url',sys.argv[1]]))\n"
    )
    with child_process(
        [sys.executable, "-c", script, str(database_url)],
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
    ) as child:
        expect_output(child, "fetching")
        child.send_signal(signal.SIGTERM)
        output, error = child.communicate(input="\n", timeout=3)
        assert child.returncode == 0, error
        assert "fetching" not in output
    with open_database(database_url) as conn:
        completed = state.get_job(conn, first)
        pending = state.get_job(conn, second)
        assert completed is not None and completed.state == "done"
        assert pending is not None and pending.state == "pending"
        assert state.worker_status(conn)["status"] == "stopped"
    with archive_lock(database_url):
        pass


def test_stop_finishes_current_month_and_leaves_checkpoint_for_next_executor(
    initialized_conn, monkeypatch: pytest.MonkeyPatch,
) -> None:
    stopping = threading.Event()
    months: list[int] = []

    def fake_month(conn, username, year, month, **kwargs):
        months.append(month)
        stopping.set()
        return IngestResult("chess.com", "monthly_archive", 200, None, (), "stored")

    monkeypatch.setattr(acquisition_module, "fetch_chesscom_month", fake_month)
    job_id = state.enqueue_job(
        initialized_conn, provider="chess.com", kind="fetch_user_games", target="test",
        params={"since": 1704067200, "until": 1709251200, "max_games": 100},
    ).job_id
    JobRunner(initialized_conn, stop_requested=stopping.is_set).run()
    job = state.get_job(initialized_conn, job_id)
    assert months == [1]
    assert job is not None and job.state == "pending"
    assert state.load_params(job.params_json)["cursor_index"] == 1
    stopping.clear()
    JobRunner(initialized_conn, stop_requested=stopping.is_set).run()
    job = state.get_job(initialized_conn, job_id)
    assert months == [1, 2]
    assert job is not None and job.state == "pending"
    stopping.clear()
    assert JobRunner(initialized_conn, stop_requested=stopping.is_set).run().done == 1
    completed = state.get_job(initialized_conn, job_id)
    assert completed is not None and completed.state == "done"


def test_heartbeat_continues_during_http_and_stop_claims_no_new_job(database_url: str) -> None:
    entered = threading.Event()
    release = threading.Event()
    clock = [100.0]
    errors: list[BaseException] = []

    def transport(request: httpx.Request) -> httpx.Response:
        entered.set()
        assert release.wait(3), "test did not release blocked HTTP"
        return httpx.Response(200, json={"username": "test", "player_id": 1})

    with open_database(database_url, writable=True) as conn:
        first = state.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="test").job_id
        second = state.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="next").job_id
    worker = Worker(
        database_url, clock=lambda: clock[0], transport=httpx.MockTransport(transport),
        config=Config(chesscom_delay_s=0, max_retries=0),
        settings=WorkerSettings(heartbeat_interval=0.02, heartbeat_max_age=0.2),
    )

    def run() -> None:
        try:
            worker.run()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert entered.wait(3)
        clock[0] = 101.0
        worker.request_stop()
        deadline = time.monotonic() + 3
        observed = False
        while time.monotonic() < deadline:
            with open_database(database_url) as conn:
                status = state.worker_status(conn, now=101.0)
            if status.get("heartbeat_at") == 101.0 and status.get("current_job_id") == first:
                assert status["alive"] and status["status"] == "stopping"
                observed = True
                break
            threading.Event().wait(0.01)
        assert observed, errors
    finally:
        release.set()
        worker.request_stop()
        thread.join(timeout=4)
    assert not thread.is_alive()
    assert errors == []
    with open_database(database_url) as conn:
        completed = state.get_job(conn, first)
        pending = state.get_job(conn, second)
        assert completed is not None and completed.state == "done"
        assert pending is not None and pending.state == "pending"
        assert not state.worker_status(conn)["alive"]
    with archive_lock(database_url):
        pass


def test_heartbeat_age_is_bounded_and_old_owner_cannot_change_successor(database_url: str) -> None:
    with archive_lock(database_url) as lease:
        conn = lease.connection
        state.start_worker(conn, "old", lease=lease, max_age=10, now=100)
        assert state.worker_status(conn, now=109.9)["alive"]
        assert not state.worker_status(conn, now=110)["alive"]
        state.start_worker(conn, "new", lease=lease, max_age=10, now=120)
        # Workers have independent liveness rows. Stopping the old identity
        # cannot change the new worker or permit restarting its stopped row.
        state.stop_worker(conn, "old", now=122)
        assert not state.heartbeat_worker(conn, "old", max_age=1000, now=123)
        assert state.worker_status(conn, now=125)["worker_id"] == "new"
        assert state.worker_status(conn, now=125)["alive"]


def test_independent_archive_write_does_not_block_worker_heartbeat(
    database_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = Worker(database_url, clock=lambda: 121.0)
    heartbeats: list[bool] = []
    heartbeat_worker = state.heartbeat_worker

    def heartbeat(*args, **kwargs):
        owned = heartbeat_worker(*args, **kwargs)
        heartbeats.append(owned)
        worker._heartbeat_stop.set()
        return owned

    monkeypatch.setattr(state, "heartbeat_worker", heartbeat)
    with archive_lock(database_url) as lease:
        conn = lease.connection
        state.start_worker(conn, worker.worker_id, lease=lease, max_age=20, now=100)
        with transaction(conn):
            state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="unrelated")
            worker._heartbeat()
            assert state.worker_status(conn, now=121)["alive"]
    assert heartbeats == [True]
    assert worker._heartbeat_error is None
    assert not worker._stop.is_set()


@pytest.mark.parametrize("error_type", [
    psycopg.errors.LockNotAvailable, psycopg.errors.SerializationFailure, psycopg.errors.DeadlockDetected,
])
def test_heartbeat_retries_transient_database_contention(
    database_url: str, monkeypatch: pytest.MonkeyPatch, error_type,
) -> None:
    worker = Worker(database_url, clock=lambda: 101.0)
    attempts: list[bool] = []
    heartbeat_worker = state.heartbeat_worker

    @contextmanager
    def contended_connection(*args, **kwargs):
        attempts.append(True)
        if len(attempts) == 1:
            raise error_type("database contention")
        with connection(*args, **kwargs) as conn:
            yield conn

    def heartbeat(*args, **kwargs):
        owned = heartbeat_worker(*args, **kwargs)
        worker._heartbeat_stop.set()
        return owned

    monkeypatch.setattr(worker_module, "connection", contended_connection)
    monkeypatch.setattr(state, "heartbeat_worker", heartbeat)
    monkeypatch.setattr(worker._heartbeat_stop, "wait", lambda delay: False)
    with archive_lock(database_url) as lease:
        conn = lease.connection
        state.start_worker(conn, worker.worker_id, lease=lease, max_age=20, now=100)
        worker._heartbeat()
        assert state.worker_status(conn, now=101)["heartbeat_at"] == 101
    assert len(attempts) == 2
    assert worker._heartbeat_error is None
    assert not worker._stop.is_set()


@pytest.mark.parametrize("failure", ["disconnect", "ownership_lost"])
def test_heartbeat_still_stops_executor_on_fatal_error(
    database_url: str, monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    worker = Worker(database_url)

    def heartbeat(*args, **kwargs):
        if failure == "disconnect":
            raise psycopg.OperationalError("connection was lost")
        return False

    monkeypatch.setattr(state, "heartbeat_worker", heartbeat)
    worker._heartbeat()
    assert worker._heartbeat_error is not None
    assert worker._stop.is_set()


def test_stopping_worker_preserves_rate_limit_evidence_without_retrying_http(database_url: str) -> None:
    calls: list[str] = []

    def transport(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        worker.request_stop()
        return httpx.Response(429, headers={"Retry-After": "120"})

    def no_sleep(seconds: float) -> None:
        pytest.fail("Shutdown must not enter another provider backoff")

    with open_database(database_url, writable=True) as conn:
        first = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="first").job_id
        second = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="second").job_id
    worker = Worker(
        database_url, transport=httpx.MockTransport(transport), sleeper=no_sleep,
        config=Config(max_retries=3), clock=lambda: 100.0,
    )
    assert worker.run() == 1
    assert len(calls) == 1
    with open_database(database_url) as conn:
        blocked = state.get_job(conn, first)
        pending = state.get_job(conn, second)
        assert blocked is not None and blocked.state == "blocked" and blocked.next_attempt_at == 220.0
        assert pending is not None and pending.state == "pending"
        assert require_row(conn.execute("SELECT COUNT(*) FROM fetch_logs WHERE status_code=429"))[0] == 1
        assert state.provider_ready_at(conn, "lichess") == 220.0
