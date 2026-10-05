"""Durable, single-process archive executor with independent liveness reporting."""

from __future__ import annotations

import argparse
import signal
import sys
import threading
import time
import uuid
import os
from collections.abc import Callable, Sequence
from pathlib import Path

import httpx

from chess_crawl.config import Config
from chess_crawl.jobs import state
from chess_crawl.jobs.dispatch import DispatchMaintenance
from chess_crawl.jobs.locking import ExecutorBusy, parallel_executor_lock
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.jobs.settings import WorkerSettings
from chess_crawl.jobs.worker_identity import write_worker_identity
from chess_crawl.providers.registry import ProviderSession
from chess_crawl.storage.db import DatabaseError, connection, database_url
from chess_crawl.storage.migrations import initialize
from psycopg.errors import DeadlockDetected, LockNotAvailable, SerializationFailure


class Worker:
    """Own one archive until stopped; heartbeat age never grants execution rights."""

    def __init__(
        self,
        db_path: str,
        *,
        settings: WorkerSettings | None = None,
        config: Config | None = None,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] | None = None,
        runner_factory: Callable[..., JobRunner] = JobRunner,
        stage: str = "all",
        queue_consumer=None,
        identity_path: str | Path | None = None,
    ) -> None:
        self.db_path = database_url(db_path)
        self.settings = settings or WorkerSettings()
        self.config = config or Config.from_env()
        self.transport = transport
        self.clock = clock
        self.sleeper = sleeper
        self.runner_factory = runner_factory
        self.worker_id = uuid.uuid4().hex
        self.stage = stage
        self.queue_consumer = queue_consumer
        self.dispatch_maintenance = DispatchMaintenance.from_env(clock=clock)
        self.identity_path = identity_path
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
            # The heartbeat owns its separate session; executor rights stay on the runner session.
            while not self._heartbeat_stop.is_set():
                try:
                    with connection(self.db_path, mode="rw") as conn:
                        while not self._heartbeat_stop.is_set():
                            try:
                                with self._activity_lock:
                                    current_job = self._current_job_id
                                owned = state.heartbeat_worker(
                                    conn, self.worker_id, max_age=self.settings.heartbeat_max_age,
                                    current_job_id=current_job, stopping=self._stop.is_set(), now=self.clock(),
                                )
                                if not owned:
                                    raise RuntimeError("Worker heartbeat ownership was lost")
                            except DatabaseError as exc:
                                if not _is_database_contention(exc):
                                    raise
                                # Normalization may own the transaction write lock.
                                # Liveness may expire, but contention does not
                                # invalidate the process's exclusive lease.
                            self._heartbeat_stop.wait(self.settings.heartbeat_interval)
                except DatabaseError as exc:
                    if not _is_database_contention(exc):
                        raise
                    # Connection setup can also encounter a busy archive.
                    self._heartbeat_stop.wait(self.settings.heartbeat_interval)
        except BaseException as exc:
            self._heartbeat_error = exc
            self.request_stop()

    def run(self, *, once: bool = False) -> int:
        """Run until stopped, or execute at most one due job with ``once=True``."""
        claimed = 0
        # Lock precedes migration, recovery, and heartbeat mutations. A losing
        # contender cannot disturb the live owner's durable state.
        with connection(self.db_path, mode="rw") as conn, parallel_executor_lock(conn) as lease:
            initialize(conn)
            state.refresh_crawl_runs(conn)
            state.start_worker(
                conn, self.worker_id, lease=lease,
                max_age=self.settings.heartbeat_max_age, now=self.clock(),
            )
            heartbeat = threading.Thread(target=self._heartbeat, name="chess-crawl-heartbeat", daemon=True)
            heartbeat.start()
            failed = False
            try:
                if self.identity_path is not None:
                    write_worker_identity(self.identity_path, self.worker_id)
                provider_sleeper = self.sleeper if self.sleeper is not None else self._stop.wait
                with ProviderSession(
                    self.config, transport=self.transport, sleeper=provider_sleeper, clock=self.clock,
                    stop_requested=self._stop.is_set,
                ) as session:
                    runner = self.runner_factory(
                        conn, config=self.config, transport=self.transport, sleeper=provider_sleeper,
                        settings=self.settings, clock=self.clock, session=session, lease=lease,
                        stop_requested=self._stop.is_set, on_job=self._on_job,
                        worker_id=self.worker_id, stage=self.stage,
                    )
                    while not self._stop.is_set():
                        # Recovery never steals a live session. Poll it even
                        # with an empty queue so an interrupted/acked duplicate
                        # cannot strand the durable original job indefinitely.
                        state.resume_stale_in_progress(conn, now=int(self.clock()), lease=lease)
                        self.dispatch_maintenance.run_due(conn)
                        # Fair, stage-eligible durable work takes precedence over
                        # an empty queue's twenty-second long poll.
                        count = runner.run(max_jobs=1).claimed
                        if self.queue_consumer is not None and not self._stop.is_set():
                            if count:
                                # Drain duplicate hints even under continuous DB
                                # load, without executing a second job or waiting.
                                self.queue_consumer.run_once(conn, lambda job_id: 0, wait_seconds=0)
                            else:
                                count = self.queue_consumer.run_once(
                                    conn, lambda job_id: runner.run(max_jobs=1, job_id=job_id,
                                                                   respect_fairness=True).claimed,
                                )
                                # Work may become due during the long poll;
                                # missing/unclaimable hints never strand it.
                                if not count and not self._stop.is_set():
                                    count = runner.run(max_jobs=1).claimed
                        claimed += count
                        if once:
                            break
                        if not count:
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
                # Heartbeat PostgreSQL writes have the configured finite lock
                # timeout. Keep ownership until its thread has exited.
                heartbeat.join()
                state.stop_worker(conn, self.worker_id, failed=failed, now=self.clock())
        return claimed


def _is_database_contention(exc: DatabaseError) -> bool:
    return isinstance(exc, (LockNotAvailable, DeadlockDetected, SerializationFailure))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a concurrent durable chess archive worker")
    parser.add_argument("--database-url", help="PostgreSQL URL; defaults to CHESS_CRAWL_DATABASE_URL")
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--heartbeat-interval", type=float, default=5.0)
    parser.add_argument("--max-retries", type=int, default=3, help="Durable retries after provider-level retries are exhausted")
    parser.add_argument("--retry-base", type=float, default=30.0, help="Initial durable retry delay in seconds")
    parser.add_argument("--retry-max", type=float, default=3600.0, help="Maximum exponential delay; provider delays remain floors")
    parser.add_argument("--stage", choices=("all", "acquisition", "processing"), default="all")
    parser.add_argument("--queue-url", default=os.getenv("CHESS_CRAWL_SQS_QUEUE_URL"),
                        help="SQS queue URL; requires the s3 dependency extra")
    parser.add_argument("--once", action="store_true", help="Recover orphaned work, execute at most one due job, then exit")
    args = parser.parse_args(argv)
    try:
        settings = WorkerSettings(
            poll_interval=args.poll_interval, heartbeat_interval=args.heartbeat_interval,
            heartbeat_max_age=max(20.0, 4 * args.heartbeat_interval), job_max_retries=args.max_retries,
            job_retry_base_s=args.retry_base, job_retry_max_s=args.retry_max,
        )
        consumer = None
        selected_queue = args.queue_url
        if args.stage != "all":
            stage_queue = os.getenv(f"CHESS_CRAWL_SQS_{args.stage.upper()}_QUEUE_URL")
            if args.queue_url and not stage_queue:
                raise ValueError("Stage-specific SQS workers require the corresponding stage queue URL")
            selected_queue = stage_queue or selected_queue
        if selected_queue:
            from chess_crawl.jobs.dispatch import SqsConsumer, aws_sqs_client
            consumer = SqsConsumer(aws_sqs_client(), selected_queue)
        worker = Worker(database_url(args.database_url), settings=settings, stage=args.stage, queue_consumer=consumer,
                        identity_path=os.getenv("CHESS_CRAWL_WORKER_IDENTITY_FILE"))
        previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
        for sig in previous:
            signal.signal(sig, lambda signum, frame: worker.request_stop())
        try:
            worker.run(once=args.once)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
    except DatabaseError:
        print("Worker: the database is unavailable", file=sys.stderr)
        return 1
    except (ExecutorBusy, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
