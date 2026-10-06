"""Durable job runner for independently scheduled acquisition and processing."""

from __future__ import annotations

import time
import uuid
from contextlib import nullcontext
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any

import httpx

from chess_crawl.config import Config
from chess_crawl.ingest import (
    IngestResult,
    fetch_chesscom_stats,
    fetch_lichess_game,
    fetch_user_profile,
    replay_raw_payload,
    installed_parser_target,
)
from chess_crawl.jobs import discovery, state
from chess_crawl.jobs.acquisition import execute_bounded_acquisition
from chess_crawl.normalize.games import NormalizationStopped
from chess_crawl.jobs.collection import execute_collection
from chess_crawl.jobs.budget import BudgetExceeded, BudgetPolicy
from chess_crawl.jobs.models import source_provider, DiscoveryJob, JobState, PROCESSING_JOB_KINDS
from chess_crawl.jobs.locking import ExecutorLease, ExecutorLeaseLost, parallel_executor_lock
from chess_crawl.jobs.settings import WorkerSettings
from chess_crawl.providers.registry import ProviderSession, get_provider_info
from chess_crawl.providers.base import (
    ProviderRequestStopped, ProviderResponseTooLarge, ProviderResponseEncodingError, ProviderResponseDeadlineExceeded,
)
from chess_crawl.storage.acquisition import associate_run_game
from chess_crawl.storage.db import Connection, DatabaseError, transaction
from chess_crawl.storage.repository import insert_error
from chess_crawl.storage.execution import start_upgrade, upgrade_batch, checkpoint_upgrade, fail_upgrade
from chess_crawl.storage import work_budgets


GameFetcher = Callable[[Connection, str, str, Mapping[str, Any], int | None], IngestResult]


@dataclass(frozen=True)
class RunnerResult:
    claimed: int = 0
    done: int = 0
    skipped: int = 0
    blocked: int = 0
    errors: int = 0
    stale_resumed: int = 0
    unblocked: int = 0

    def add(self, *, state: str | None = None) -> "RunnerResult":
        return RunnerResult(
            claimed=self.claimed + 1,
            done=self.done + (1 if state == "done" else 0),
            skipped=self.skipped + (1 if state == "skipped" else 0),
            blocked=self.blocked + (1 if state == "blocked" else 0),
            errors=self.errors + (1 if state == "error" else 0),
            stale_resumed=self.stale_resumed,
            unblocked=self.unblocked,
        )

    def with_resume_counts(self, *, stale_resumed: int, unblocked: int) -> "RunnerResult":
        return RunnerResult(
            claimed=self.claimed,
            done=self.done,
            skipped=self.skipped,
            blocked=self.blocked,
            errors=self.errors,
            stale_resumed=stale_resumed,
            unblocked=unblocked,
        )


@dataclass(frozen=True)
class ExecutionOutcome:
    state: JobState
    reason: str
    transient: bool = False
    retry_after: float | None = None
    status_code: int | None = None


class JobRunner:
    def __init__(
        self,
        conn: Connection,
        *,
        config: Config | None = None,
        transport: httpx.BaseTransport | None = None,
        sleeper=None,
        game_fetcher: GameFetcher | None = None,
        settings: WorkerSettings | None = None,
        clock: Callable[[], float] = time.time,
        session: ProviderSession | None = None,
        lease: ExecutorLease | None = None,
        stop_requested: Callable[[], bool] | None = None,
        on_job: Callable[[int | None], None] | None = None,
        worker_id: str | None = None,
        stage: str = "all",
        budget_policy: BudgetPolicy | None = None,
    ) -> None:
        self.conn = conn
        self.config = config or Config.from_env()
        self.transport = transport
        self.sleeper = sleeper
        self.game_fetcher = game_fetcher
        self.settings = settings or WorkerSettings()
        self.clock = clock
        self.session = session
        self.lease = lease
        self.stop_requested = stop_requested or (lambda: False)
        self.on_job = on_job or (lambda job_id: None)
        self._active_session: ProviderSession | None = None
        self.worker_id = worker_id or uuid.uuid4().hex
        self.stage = stage
        self.budget_policy = budget_policy or BudgetPolicy.from_env()
        self._request_reservation: int | None = None

    def run(
        self,
        *,
        crawl_run_id: int | None = None,
        max_jobs: int | None = None,
        resume_stale: bool = False,
        unblock: bool = False,
        job_id: int | None = None,
        respect_fairness: bool = False,
    ) -> RunnerResult:
        with parallel_executor_lock(self.conn, lease=self.lease) as lease:
            sessions = nullcontext(self.session) if self.session is not None else ProviderSession(
                self.config, transport=self.transport, sleeper=self.sleeper, clock=self.clock,
                stop_requested=self.stop_requested,
            )
            with sessions as session:
                self._active_session = session
                if session is not None:
                    session.before_request = self._before_provider_request
                    session.persist_deadline = self._persist_provider_deadline
                    session.reserve_request = self._reserve_request
                    session.finish_request = self._finish_request
                try:
                    return self._run_owned(
                        lease, crawl_run_id=crawl_run_id, max_jobs=max_jobs,
                        resume_stale=resume_stale, unblock=unblock,
                        job_id=job_id,
                        respect_fairness=respect_fairness,
                    )
                finally:
                    self._active_session = None

    def _run_owned(
        self, lease: ExecutorLease, *, crawl_run_id: int | None,
        max_jobs: int | None, resume_stale: bool, unblock: bool,
        job_id: int | None,
        respect_fairness: bool,
    ) -> RunnerResult:
        stale_count = state.resume_stale_in_progress(
            self.conn, crawl_run_id=crawl_run_id, now=int(self.clock()), lease=lease,
        ) if resume_stale else 0
        unblocked_count = state.unblock_jobs(self.conn, crawl_run_id=crawl_run_id, now=self.clock()) if unblock else 0
        result = RunnerResult().with_resume_counts(stale_resumed=stale_count, unblocked=unblocked_count)
        while (max_jobs is None or result.claimed < max_jobs) and not self.stop_requested():
            lease.require(self.conn)
            job = state.claim_next_job(self.conn, crawl_run_id=crawl_run_id, now=self.clock(),
                                       worker_id=self.worker_id, job_id=job_id, stage=self.stage,
                                       budget_policy=self.budget_policy, respect_fairness=respect_fairness)
            if job is None:
                pacing = state.next_pacing_deadline(self.conn, crawl_run_id=crawl_run_id, now=self.clock())
                if pacing is not None and self.stage != "processing" and job_id is None:
                    (self.sleeper or time.sleep)(max(0, pacing - self.clock()))
                    continue
                break
            if job.id is None:
                raise RuntimeError("claimed job is missing a persisted id")
            self.on_job(job.id)
            if job.crawl_run_id is not None:
                state.refresh_run_status(self.conn, job.crawl_run_id)
            try:
                lease.require(self.conn)
                outcome = self._execute(job)
                lease.require(self.conn)
                minimum_delay = (self.config.provider(source_provider(job)).min_delay_s
                                 if job.kind not in PROCESSING_JOB_KINDS else 0)
                if outcome.status_code == 429:
                    minimum_delay = max(minimum_delay, get_provider_info(source_provider(job)).policy.next_delay(429, outcome.retry_after))
                completed = state.finish_attempt(
                    self.conn, job.id, outcome.state, reason=outcome.reason,
                    transient=outcome.transient, retry_after=outcome.retry_after,
                    minimum_delay=minimum_delay, settings=self.settings, now=self.clock(),
                )
                result = result.add(state=completed)
            finally:
                self.conn._work_budget_id = None
                self.conn._work_payload_read_credits = 0
                self._request_reservation = None
                state.release_job_ownership(self.conn)
                self.on_job(None)
        # Normal completions already refreshed their own run. Avoid rewriting
        # every historical run on each idle daemon poll.
        if crawl_run_id is not None or stale_count or unblocked_count:
            state.refresh_crawl_runs(self.conn, crawl_run_id=crawl_run_id)
        return result

    def _execute(self, job: DiscoveryJob) -> ExecutionOutcome:
        previous = self.conn._defer_normalization
        if job.kind in {"fetch_user_games", "fetch_game_by_id", "crawl_opponents"} and self.game_fetcher is None:
            self.conn._defer_normalization = True
        try:
            if job.id is None:
                raise ValueError("Execution requires a persisted job")
            self.conn._work_budget_id = work_budgets.ensure_job_budget(
                self.conn, job.id, self.budget_policy, now=int(self.clock()),
            )
            from chess_crawl.jobs.internal import handler_for
            handler = handler_for(job.kind)
            if handler is not None:
                return ExecutionOutcome(**handler(
                    self.conn, job, stop_requested=lambda: self.stop_requested() or self._cancelled(job),
                    interruption_requested=self.stop_requested,
                ))
            if job.kind == "normalize_payload":
                params = state.load_params(job.params_json)
                result = replay_raw_payload(self.conn, int(params.get("raw_payload_id", job.target)),
                                            crawl_run_id=job.crawl_run_id, max_games=params.get("max_games"),
                                            fetch_log_id=params.get("fetch_log_id"),
                                            parser_version=params.get("parser_version"),
                                            stop_requested=lambda: self.stop_requested() or self._cancelled(job))
                return _outcome_from_ingest(result)
            if job.kind == "reprocess_archive":
                return self._reprocess_archive(job)
            if job.kind == "expand_opponents":
                parent = state.get_job(self.conn, int(job.target))
                if parent is None or parent.kind != "crawl_opponents" or parent.crawl_run_id != job.crawl_run_id:
                    raise ValueError("Opponent expansion requires its acquisition parent")
                if parent.state in {"error", "skipped"}:
                    return ExecutionOutcome("skipped", f"Acquisition parent finished {parent.state}; frontier retired")
                return self._expand_opponents(parent, depth=job.depth)
            if job.kind == "fetch_user_resource":
                from chess_crawl import ingest
                params = state.load_params(job.params_json)
                fetch = getattr(ingest, "fetch_user_resource")
                result = fetch(self.conn, source_provider(job), job.target, params["resource_key"],
                               parameters=params.get("parameters"), config=self.config,
                               session=self._active_session, job_id=job.id,
                               crawl_run_id=job.crawl_run_id, owner_scope=params.get("owner_scope", "public"))
                return _outcome_from_ingest(result)
            if job.kind == "fetch_user_profile":
                result = fetch_user_profile(
                    self.conn,
                    source_provider(job),
                    job.target,
                    config=self.config,
                    transport=self.transport,
                    sleeper=self.sleeper,
                    session=self._active_session,
                    job_id=job.id,
                    crawl_run_id=job.crawl_run_id,
                )
                return _outcome_from_ingest(result)
            if job.kind == "fetch_user_stats":
                result = self._fetch_stats(job)
                return _outcome_from_ingest(result)
            if job.kind == "fetch_user_games":
                return self._collect_user_games(job)
            if job.kind == "fetch_game_by_id":
                result = self._fetch_game_by_id(job)
                return _outcome_from_ingest(result)
            if job.kind == "crawl_opponents":
                return self._crawl_opponents(job)
            return ExecutionOutcome("error", f"unknown job kind: {job.kind}")
        except (BudgetExceeded, ProviderResponseTooLarge, ProviderResponseEncodingError, ProviderResponseDeadlineExceeded) as exc:
            dimension = exc.dimension if isinstance(exc, BudgetExceeded) else (
                "response_bytes" if isinstance(exc, ProviderResponseTooLarge) else
                "response_encoding" if isinstance(exc, ProviderResponseEncodingError) else "response_deadline"
            )
            if self.conn._work_budget_id is not None:
                work_budgets.exhaust_budget(self.conn, self.conn._work_budget_id, dimension, now=int(self.clock()))
            return ExecutionOutcome("blocked", f"budget_exhausted: {dimension}; requested work remains incomplete")
        except (ProviderRequestStopped, NormalizationStopped) as exc:
            return ExecutionOutcome("pending", str(exc))
        except (DatabaseError, ExecutorLeaseLost):
            # Preserve the claimed job for recovery after the database returns.
            # Storage or ownership loss is not a failed provider acquisition.
            raise
        except Exception as exc:
            if job.kind == "reprocess_archive" and job.id is not None:
                params = state.load_params(job.params_json)
                fail_upgrade(
                    self.conn, str(params.get("upgrade_id", job.target)), job_id=job.id,
                    provider=source_provider(job), owner_scope=str(params.get("owner_scope", "public")),
                    error=str(exc),
                )
            insert_error(
                self.conn,
                provider=job.provider,
                error_kind="other",
                message=str(exc),
                retry_count=job.attempts,
            )
            return ExecutionOutcome("error", str(exc), transient=isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)))

        finally:
            self.conn._defer_normalization = previous

    def _before_provider_request(self, provider: str) -> None:
        # Verify fencing immediately before network work, including each retry.
        with transaction(self.conn):
            ready = state.provider_ready_at(self.conn, provider)
        delay = (ready or 0) - self.clock()
        if delay > 0:
            (self.sleeper or time.sleep)(delay)
            with transaction(self.conn):
                pass  # Recheck ownership after waiting, immediately before HTTP.
        if self.stop_requested():
            raise ProviderRequestStopped("Provider request stopped before acquisition")

    def _persist_provider_deadline(self, provider: str, deadline: float, reason: str) -> None:
        state.defer_provider(self.conn, provider, not_before=deadline, reason=reason, now=self.clock())

    def _reserve_request(self, provider: str) -> int:
        if self.conn._work_budget_id is None:
            raise RuntimeError("Provider acquisition has no durable work budget")
        if self._request_reservation is not None:
            raise RuntimeError("Previous request reservation is unsettled")
        if self.conn._defer_normalization:
            work_budgets.require_backlog_room(self.conn, self.conn._work_budget_id)
        identity, limit = work_budgets.reserve_request(self.conn, self.conn._work_budget_id, now=int(self.clock()))
        self._request_reservation = identity
        self.conn._work_payload_read_credits += 1
        return limit

    def _finish_request(self, provider: str, received: int) -> None:
        if self._request_reservation is None:
            raise RuntimeError("Provider response has no reserved work")
        identity = self._request_reservation
        work_budgets.settle_request(self.conn, identity, received)
        self._request_reservation = None

    def _fetch_stats(self, job: DiscoveryJob) -> IngestResult:
        if source_provider(job) == "chess.com":
            return fetch_chesscom_stats(
                self.conn,
                job.target,
                config=self.config,
                transport=self.transport,
                sleeper=self.sleeper,
                session=self._active_session,
                job_id=job.id,
                crawl_run_id=job.crawl_run_id,
            )
        return IngestResult(
            source_provider(job),
            "user_stats",
            400,
            None,
            (),
            "fetch_user_stats is supported only for chess.com",
        )

    def _collect_user_games(self, job: DiscoveryJob) -> ExecutionOutcome:
        params = state.load_params(job.params_json)
        if job.id is None:
            raise ValueError("Collection requires a persisted job")
        failure = state.processing_failure(self.conn, job.id)
        if failure is not None:
            return ExecutionOutcome("error", failure)
        if self.game_fetcher is not None:
            remaining = discovery.remaining_game_budget(
                self.conn, crawl_run_id=job.crawl_run_id, provider=source_provider(job), params=params,
            )
            if remaining == 0:
                return ExecutionOutcome("done", "max-games cap already reached")
            result = self.game_fetcher(self.conn, source_provider(job), job.target, params, remaining)
            if job.crawl_run_id is not None:
                with transaction(self.conn):
                    for game_id in result.normalized_ids:
                        associate_run_game(self.conn, job.crawl_run_id, game_id)
            return _outcome_from_ingest(result)
        collection = (execute_bounded_acquisition if params.get("collection_mode", "bounded") == "bounded"
                      else execute_collection)
        collected = collection(
            self.conn, job, params, config=self.config,
            session=self._active_session, sleeper=self.sleeper, clock=self.clock,
            transport=self.transport, stop_requested=lambda: self.stop_requested() or self._cancelled(job),
        )
        if collected.status_code not in {200, 304}:
            return _outcome_from_ingest(IngestResult(
                source_provider(job), "collection", collected.status_code, None,
                collected.normalized_ids, collected.message, collected.retry_after,
            ))
        return ExecutionOutcome("done" if collected.done else "pending", collected.message)

    def _reprocess_archive(self, job: DiscoveryJob) -> ExecutionOutcome:
        if job.id is None:
            raise ValueError("Upgrade requires a persisted job")
        params = state.load_params(job.params_json)
        size = int(params.get("batch_size", 25))
        if not 1 <= size <= 100:
            raise ValueError("Upgrade batch_size must be between one and one hundred")
        upgrade_id = str(params.get("upgrade_id", job.target))
        upgrade = start_upgrade(self.conn, upgrade_id=upgrade_id, provider=source_provider(job),
                                parser_version=installed_parser_target(str(params.get("parser_version", "current"))),
                                job_id=job.id,
                                owner_scope=str(params.get("owner_scope", "public")))
        batch = upgrade_batch(self.conn, upgrade, batch_size=size)
        for raw_id in batch:
            if self.stop_requested() or self._cancelled(job):
                return ExecutionOutcome("pending", "Upgrade checkpoint retained")
            replay_raw_payload(self.conn, raw_id,
                               stop_requested=lambda: self.stop_requested() or self._cancelled(job))
            checkpoint_upgrade(self.conn, upgrade_id, raw_id)
        if len(batch) < size:
            checkpoint_upgrade(self.conn, upgrade_id, int(upgrade["high_water_raw_id"]), done=True)
            return ExecutionOutcome("done", f"Upgrade completed: {upgrade_id}")
        return ExecutionOutcome("pending", f"Upgrade checkpointed {len(batch)} source payloads")

    def _cancelled(self, job: DiscoveryJob) -> bool:
        if job.crawl_run_id is None:
            return False
        run = state.get_run(self.conn, job.crawl_run_id)
        return run is not None and run["status"] == "cancelled"

    def _fetch_game_by_id(self, job: DiscoveryJob) -> IngestResult:
        if source_provider(job) != "lichess":
            return IngestResult(
                source_provider(job),
                "game",
                400,
                None,
                (),
                "fetch_game_by_id is supported only for lichess; Chess.com games require monthly archives",
            )
        return fetch_lichess_game(
            self.conn,
            job.target,
            config=self.config,
            transport=self.transport,
            sleeper=self.sleeper,
            session=self._active_session,
            job_id=job.id,
            crawl_run_id=job.crawl_run_id,
        )

    def _crawl_opponents(self, job: DiscoveryJob) -> ExecutionOutcome:
        if job.id is None or job.crawl_run_id is None:
            return ExecutionOutcome("error", "crawl_opponents requires a persisted crawl run")
        outcome = self._collect_user_games(job)
        if outcome.state != "done":
            return outcome
        if self._cancelled(job):
            return ExecutionOutcome("pending", "Crawl cancelled; acquisition retained")
        if self.game_fetcher is not None:
            # Injected local fetchers supply already-normalized data.
            return self._expand_opponents(job)
        state.enqueue_opponent_expansion(self.conn, job)
        return ExecutionOutcome("done", f"captured {job.target}; opponent expansion queued")

    def _expand_opponents(self, job: DiscoveryJob, *, depth: int | None = None) -> ExecutionOutcome:
        if job.crawl_run_id is not None:
            effective = state.known_crawl_depth(self.conn, crawl_run_id=job.crawl_run_id,
                                               provider=source_provider(job), username=job.target)
            job = replace(job, depth=min(job.depth if depth is None else depth,
                                         effective if effective is not None else job.depth))
        return ExecutionOutcome("done", discovery.expand_opponent_frontier(self.conn, job))


def _outcome_from_ingest(result: IngestResult) -> ExecutionOutcome:
    if result.status_code in {200, 304}:
        return ExecutionOutcome("done", result.message)
    if result.status_code in {404, 410}:
        return ExecutionOutcome("skipped", result.message)
    if result.status_code == 429:
        return ExecutionOutcome("blocked", result.message, transient=True, retry_after=result.retry_after, status_code=result.status_code)
    return ExecutionOutcome("error", result.message, transient=result.status_code == 0 or 500 <= result.status_code <= 599, retry_after=result.retry_after, status_code=result.status_code)
