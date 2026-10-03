"""Durable, single-process archive executor with independent liveness reporting."""

from __future__ import annotations

import argparse
import signal
import sys
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from pathlib import Path

import httpx

from chess_crawl.config import Config
from chess_crawl.jobs import state
from chess_crawl.jobs.locking import ExecutorBusy, archive_lock
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.jobs.settings import WorkerSettings
from chess_crawl.providers.registry import ProviderSession
from chess_crawl.storage.db import connection, open_database


class Worker:
    """Own one archive until stopped; heartbeat age never grants execution rights."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        settings: WorkerSettings | None = None,
        config: Config | None = None,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] | None = None,
        runner_factory: Callable[..., JobRunner] = JobRunner,
    ) -> None:
        if str(db_path) == ":memory:":
            raise ValueError("A worker requires a file-backed archive")
        self.db_path = Path(db_path).resolve()
        self.settings = settings or WorkerSettings()
        self.config = config or Config.from_env()
        self.transport = transport
        self.clock = clock
        self.sleeper = sleeper
        self.runner_factory = runner_factory
        self.worker_id = uuid.uuid4().hex
        self._stop = threading.Event()
        self._heartbeat_stop = threading.Event()
        self._activity_lock = threading.Lock()
        self._current_job_id: int | None = None
        self._heartbeat_error: BaseException | None = None

    def request_stop(self) -> None:
        """Signal-safe intent: finish the active acquisition, then claim no more work."""
        self._stop.set()

    def _on_job(self, job_id: int | None) -> None:
        with self._activity_lock:
            self._current_job_id = job_id

    def _heartbeat(self) -> None:
        try:
            # SQLite handles are thread-owned. Never share the runner connection.
            with connection(self.db_path, mode="rw") as conn:
                while not self._heartbeat_stop.is_set():
                    with self._activity_lock:
                        current_job = self._current_job_id
                    owned = state.heartbeat_worker(
                        conn, self.worker_id, max_age=self.settings.heartbeat_max_age,
                        current_job_id=current_job, stopping=self._stop.is_set(), now=self.clock(),
                    )
                    if not owned:
                        raise RuntimeError("Worker heartbeat ownership was lost")
                    self._heartbeat_stop.wait(self.settings.heartbeat_interval)
        except BaseException as exc:
            self._heartbeat_error = exc
            self.request_stop()

    def run(self, *, once: bool = False) -> int:
        """Run until stopped, or execute at most one due job with ``once=True``."""
        claimed = 0
        # Lock precedes migration, recovery, and heartbeat mutations. A losing
        # contender cannot disturb the live owner's durable state.
        with archive_lock(self.db_path) as lease:
            with open_database(self.db_path, writable=True) as conn:
                state.resume_stale_in_progress(conn, now=int(self.clock()), lease=lease)
                state.refresh_crawl_runs(conn)
                state.start_worker(
                    conn, self.worker_id, lease=lease,
                    max_age=self.settings.heartbeat_max_age, now=self.clock(),
                )
                heartbeat = threading.Thread(target=self._heartbeat, name="chess-crawl-heartbeat", daemon=True)
                heartbeat.start()
                failed = False
                try:
                    provider_sleeper = self.sleeper if self.sleeper is not None else self._stop.wait
                    with ProviderSession(
                        self.config, transport=self.transport, sleeper=provider_sleeper, clock=self.clock,
                        stop_requested=self._stop.is_set,
                    ) as session:
                        runner = self.runner_factory(
                            conn, config=self.config, transport=self.transport, sleeper=provider_sleeper,
                            settings=self.settings, clock=self.clock, session=session, lease=lease,
                            stop_requested=self._stop.is_set, on_job=self._on_job,
                        )
                        while not self._stop.is_set():
                            result = runner.run(max_jobs=1)
                            claimed += result.claimed
                            if once:
                                break
                            if not result.claimed:
                                if self.sleeper is None:
                                    self._stop.wait(self.settings.poll_interval)
                                else:
                                    self.sleeper(self.settings.poll_interval)
                    if self._heartbeat_error is not None:
                        raise RuntimeError("Worker heartbeat failed") from self._heartbeat_error
                except BaseException:
                    failed = True
                    raise
                finally:
                    self._heartbeat_stop.set()
                    # Heartbeat SQLite writes have the configured finite busy
                    # timeout. Keep ownership until its thread has exited.
                    heartbeat.join()
                    state.stop_worker(conn, self.worker_id, failed=failed, now=self.clock())
        return claimed


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the exclusive serial chess archive worker")
    parser.add_argument("--db", type=Path, required=True, help="File-backed archive path")
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--heartbeat-interval", type=float, default=5.0)
    parser.add_argument("--max-retries", type=int, default=3, help="Durable retries after provider-level retries are exhausted")
    parser.add_argument("--retry-base", type=float, default=30.0, help="Initial durable retry delay in seconds")
    parser.add_argument("--retry-max", type=float, default=3600.0, help="Maximum exponential delay; provider delays remain floors")
    parser.add_argument("--once", action="store_true", help="Recover orphaned work, execute at most one due job, then exit")
    args = parser.parse_args(argv)
    try:
        settings = WorkerSettings(
            poll_interval=args.poll_interval, heartbeat_interval=args.heartbeat_interval,
            heartbeat_max_age=max(20.0, 4 * args.heartbeat_interval), job_max_retries=args.max_retries,
            job_retry_base_s=args.retry_base, job_retry_max_s=args.retry_max,
        )
        worker = Worker(args.db, settings=settings)
        previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
        for sig in previous:
            signal.signal(sig, lambda signum, frame: worker.request_stop())
        try:
            worker.run(once=args.once)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
    except (ExecutorBusy, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
