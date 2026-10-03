from __future__ import annotations

import os
import json
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.ingest import IngestResult
from chess_crawl.jobs import runner as runner_module, state
from chess_crawl.jobs.locking import ExecutorBusy, archive_lock
from chess_crawl.jobs.runner import ExecutionOutcome, JobRunner
from chess_crawl.jobs.settings import WorkerSettings
from chess_crawl.jobs.worker import Worker
from chess_crawl.storage.db import open_database


def test_kernel_lock_excludes_live_owner_and_releases_after_process_exit(archive_path: Path) -> None:
    script = (
        "import sys\nfrom chess_crawl.jobs.locking import archive_lock\n"
        "with archive_lock(sys.argv[1]):\n print('locked', flush=True)\n sys.stdin.readline()\n"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(archive_path)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "locked"
        with pytest.raises(ExecutorBusy):
            with archive_lock(archive_path):
                pass
        alias = archive_path.with_name("hard-link.sqlite")
        alias.hardlink_to(archive_path)
        with pytest.raises(ExecutorBusy):
            with archive_lock(alias):
                pass
        with open_database(archive_path) as conn:
            with pytest.raises(ExecutorBusy):
                JobRunner(conn).run(resume_stale=True)
            with pytest.raises(ExecutorBusy):
                state.resume_stale_in_progress(conn, now=10**10)
        # Independent publisher locks do not conflict with execution ownership.
        with archive_lock(archive_path, purpose="events"):
            pass
    finally:
        child.terminate()
        child.communicate(timeout=3)
    with archive_lock(archive_path):
        pass


@pytest.mark.parametrize("status", [0, 429, 500, 503])
def test_retry_deadline_survives_restart_and_exhaustion_is_bounded(
    archive_path: Path, monkeypatch: pytest.MonkeyPatch, status: int,
) -> None:
    monkeypatch.setattr(
        runner_module, "fetch_user_profile",
        lambda *args, **kwargs: IngestResult("chess.com", "user_profile", status, None, (), "transient", retry_after=7),
    )
    settings = WorkerSettings(job_max_retries=1, job_retry_base_s=2, job_retry_max_s=10)
    now = [100.0]
    with open_database(archive_path, writable=True) as conn:
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
    with open_database(archive_path, writable=True) as conn:
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
    archive_path: Path, monkeypatch: pytest.MonkeyPatch,
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
    with open_database(archive_path, writable=True) as conn:
        state.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="limited", priority=1)
        pending = state.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="other", priority=2).job_id
        state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="unaffected", priority=3)
        assert JobRunner(conn, settings=settings, clock=lambda: now[0]).run(max_jobs=1).errors == 1
        assert state.provider_ready_at(conn, "chess.com") == 220.0
    with open_database(archive_path, writable=True) as conn:
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
    archive_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(JobRunner, "_execute", lambda self, job: ExecutionOutcome("done", "fixture"))
    with open_database(archive_path, writable=True) as conn:
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
    assert Worker(archive_path).run(once=True) == 1
    with open_database(archive_path) as conn:
        job = state.get_job(conn, job_id)
        assert job is not None and job.state == "done" and job.attempts == 2
        job = state.get_job(conn, cancelled_job)
        assert job is not None and job.state == "in_progress" and job.attempts == 1
        assert state.worker_status(conn)["status"] == "stopped"
    assert Worker(archive_path).run(once=True) == 0


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


def test_sigterm_finishes_active_job_and_releases_process_ownership(archive_path: Path) -> None:
    with open_database(archive_path, writable=True) as conn:
        first = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="first").job_id
        second = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="second").job_id
    script = (
        "import sys\nfrom chess_crawl.jobs.runner import JobRunner, ExecutionOutcome\n"
        "from chess_crawl.jobs.worker import main\n"
        "def execute(self, job):\n print('fetching', flush=True)\n sys.stdin.readline()\n"
        " return ExecutionOutcome('done', 'fixture')\n"
        "JobRunner._execute = execute\nraise SystemExit(main(['--db',sys.argv[1]]))\n"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(archive_path)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
    )
    try:
        assert child.stdout is not None and child.stdin is not None
        assert child.stdout.readline().strip() == "fetching"
        child.send_signal(signal.SIGTERM)
        output, error = child.communicate(input="\n", timeout=3)
        assert child.returncode == 0, error
        assert "fetching" not in output
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=3)
    with open_database(archive_path) as conn:
        completed = state.get_job(conn, first)
        pending = state.get_job(conn, second)
        assert completed is not None and completed.state == "done"
        assert pending is not None and pending.state == "pending"
        assert state.worker_status(conn)["status"] == "stopped"
    with archive_lock(archive_path):
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

    monkeypatch.setattr(runner_module, "fetch_chesscom_month", fake_month)
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
    assert job is not None and job.state == "done"


def test_heartbeat_continues_during_http_and_stop_claims_no_new_job(archive_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()
    clock = [100.0]
    errors: list[BaseException] = []

    def transport(request: httpx.Request) -> httpx.Response:
        entered.set()
        assert release.wait(3), "test did not release blocked HTTP"
        return httpx.Response(200, json={"username": "test", "player_id": 1})

    with open_database(archive_path, writable=True) as conn:
        first = state.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="test").job_id
        second = state.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="next").job_id
    worker = Worker(
        archive_path, clock=lambda: clock[0], transport=httpx.MockTransport(transport),
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
            with open_database(archive_path) as conn:
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
    with open_database(archive_path) as conn:
        completed = state.get_job(conn, first)
        pending = state.get_job(conn, second)
        assert completed is not None and completed.state == "done"
        assert pending is not None and pending.state == "pending"
        assert not state.worker_status(conn)["alive"]
    with archive_lock(archive_path):
        pass


def test_heartbeat_age_is_bounded_and_old_owner_cannot_change_successor(archive_path: Path) -> None:
    with archive_lock(archive_path) as lease:
        with open_database(archive_path, writable=True) as conn:
            state.start_worker(conn, "old", lease=lease, max_age=10, now=100)
            assert state.worker_status(conn, now=109.9)["alive"]
            assert not state.worker_status(conn, now=110)["alive"]
            state.start_worker(conn, "new", lease=lease, max_age=10, now=120)
            assert not state.heartbeat_worker(conn, "old", max_age=1000, now=130)
            state.stop_worker(conn, "old", now=140)
            assert state.worker_status(conn, now=125)["worker_id"] == "new"
            assert state.worker_status(conn, now=125)["alive"]


def test_stopping_worker_preserves_rate_limit_evidence_without_retrying_http(archive_path: Path) -> None:
    calls: list[str] = []

    def transport(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        worker.request_stop()
        return httpx.Response(429, headers={"Retry-After": "120"})

    def no_sleep(seconds: float) -> None:
        pytest.fail("Shutdown must not enter another provider backoff")

    with open_database(archive_path, writable=True) as conn:
        first = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="first").job_id
        second = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="second").job_id
    worker = Worker(
        archive_path, transport=httpx.MockTransport(transport), sleeper=no_sleep,
        config=Config(max_retries=3), clock=lambda: 100.0,
    )
    assert worker.run() == 1
    assert len(calls) == 1
    with open_database(archive_path) as conn:
        blocked = state.get_job(conn, first)
        pending = state.get_job(conn, second)
        assert blocked is not None and blocked.state == "blocked" and blocked.next_attempt_at == 220.0
        assert pending is not None and pending.state == "pending"
        assert conn.execute("SELECT COUNT(*) FROM fetch_logs WHERE status_code=429").fetchone()[0] == 1
        assert state.provider_ready_at(conn, "lichess") == 220.0
