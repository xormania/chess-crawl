"""Serial durable job runner."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from chess_crawl.config import Config
from chess_crawl.ingest import (
    IngestResult,
    fetch_chesscom_month,
    fetch_chesscom_stats,
    fetch_lichess_game,
    fetch_lichess_games,
    fetch_user_profile,
)
from chess_crawl.jobs import discovery, store
from chess_crawl.jobs.models import DiscoveryJob


GameFetcher = Callable[[sqlite3.Connection, str, str, Mapping[str, Any], int | None], IngestResult]


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
    state: str
    reason: str


class JobRunner:
    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        config: Config | None = None,
        transport: httpx.BaseTransport | None = None,
        sleeper=None,
        game_fetcher: GameFetcher | None = None,
    ) -> None:
        self.conn = conn
        self.config = config or Config.from_env()
        self.transport = transport
        self.sleeper = sleeper
        self.game_fetcher = game_fetcher

    def run(
        self,
        *,
        crawl_run_id: int | None = None,
        max_jobs: int | None = None,
        resume_stale: bool = False,
        unblock: bool = False,
    ) -> RunnerResult:
        stale_count = store.resume_stale_in_progress(self.conn, crawl_run_id=crawl_run_id) if resume_stale else 0
        unblocked_count = store.unblock_jobs(self.conn, crawl_run_id=crawl_run_id) if unblock else 0
        result = RunnerResult().with_resume_counts(stale_resumed=stale_count, unblocked=unblocked_count)
        while max_jobs is None or result.claimed < max_jobs:
            job = store.claim_next_job(self.conn, crawl_run_id=crawl_run_id)
            if job is None:
                break
            outcome = self._execute(job)
            assert job.id is not None
            if outcome.state == "done":
                store.mark_done(self.conn, job.id, reason=outcome.reason)
            elif outcome.state == "skipped":
                store.mark_skipped(self.conn, job.id, reason=outcome.reason)
            elif outcome.state == "blocked":
                store.mark_blocked(self.conn, job.id, reason=outcome.reason)
            else:
                store.mark_error(self.conn, job.id, reason=outcome.reason)
            result = result.add(state=outcome.state)
        if crawl_run_id is not None:
            self._refresh_run_status(crawl_run_id)
        else:
            for row in store.crawl_runs(self.conn):
                self._refresh_run_status(int(row["id"]))
        return result

    def _execute(self, job: DiscoveryJob) -> ExecutionOutcome:
        try:
            if job.kind == "fetch_user_profile":
                result = fetch_user_profile(
                    self.conn,
                    job.provider,
                    job.target,
                    config=self.config,
                    transport=self.transport,
                    sleeper=self.sleeper,
                    job_id=job.id,
                    crawl_run_id=job.crawl_run_id,
                )
                return _outcome_from_ingest(result)
            if job.kind == "fetch_user_stats":
                result = self._fetch_stats(job)
                return _outcome_from_ingest(result)
            if job.kind == "fetch_user_games":
                result = self._fetch_user_games(job)
                return _outcome_from_ingest(result)
            if job.kind == "fetch_monthly_archive":
                result = self._fetch_monthly_archive(job)
                return _outcome_from_ingest(result)
            if job.kind == "fetch_game_by_id":
                result = self._fetch_game_by_id(job)
                return _outcome_from_ingest(result)
            if job.kind == "fetch_games_by_ids":
                return ExecutionOutcome("skipped", "bounded batch-by-ids is not implemented in this slice")
            if job.kind == "import_export_dump":
                return ExecutionOutcome("skipped", "local dump import is not implemented in this slice")
            if job.kind == "crawl_opponents":
                return self._crawl_opponents(job)
            if job.kind == "resume":
                resumed = store.resume_stale_in_progress(self.conn, crawl_run_id=job.crawl_run_id)
                return ExecutionOutcome("done", f"resumed {resumed} stale job(s)")
            return ExecutionOutcome("error", f"unknown job kind: {job.kind}")
        except Exception as exc:
            store.insert_error(
                self.conn,
                provider=job.provider,
                error_kind="other",
                message=str(exc),
                retry_count=job.attempts,
            )
            return ExecutionOutcome("error", str(exc))

    def _fetch_stats(self, job: DiscoveryJob) -> IngestResult:
        if job.provider == "chess.com":
            return fetch_chesscom_stats(
                self.conn,
                job.target,
                config=self.config,
                transport=self.transport,
                sleeper=self.sleeper,
                job_id=job.id,
                crawl_run_id=job.crawl_run_id,
            )
        return fetch_user_profile(
            self.conn,
            job.provider,
            job.target,
            config=self.config,
            transport=self.transport,
            sleeper=self.sleeper,
            job_id=job.id,
            crawl_run_id=job.crawl_run_id,
        )

    def _fetch_user_games(self, job: DiscoveryJob) -> IngestResult:
        params = store.load_params(job.params_json)
        remaining = discovery.remaining_game_budget(
            self.conn,
            crawl_run_id=job.crawl_run_id,
            provider=job.provider,
            params=params,
        )
        if remaining == 0:
            return IngestResult(job.provider, "user_games_stream", 304, None, (), "max-games cap already reached")
        if self.game_fetcher is not None:
            return self.game_fetcher(self.conn, job.provider, job.target, params, remaining)
        if job.provider == "chess.com":
            return self._fetch_chesscom_bounded_months(job, params)
        limit = int(params.get("limit") or remaining or params.get("max_games") or 0)
        if limit <= 0:
            return IngestResult(job.provider, "user_games_stream", 400, None, (), "Lichess jobs require a positive limit")
        if remaining is not None:
            limit = min(limit, remaining)
        return fetch_lichess_games(
            self.conn,
            job.target,
            since=_int_or_none(params.get("since")),
            until=_int_or_none(params.get("until")),
            limit=limit,
            config=self.config,
            transport=self.transport,
            sleeper=self.sleeper,
            job_id=job.id,
            crawl_run_id=job.crawl_run_id,
        )

    def _fetch_chesscom_bounded_months(self, job: DiscoveryJob, params: dict[str, Any]) -> IngestResult:
        since = _int_or_none(params.get("since"))
        until = _int_or_none(params.get("until"))
        if since is None or until is None:
            return IngestResult(job.provider, "monthly_archive", 400, None, (), "Chess.com jobs require since/until")
        months = _months_between(since, until)
        cursor_index = int(params.get("cursor_index") or 0)
        last_result = IngestResult(job.provider, "monthly_archive", 304, None, (), "no months in date window")
        for index, (year, month) in enumerate(months[cursor_index:], start=cursor_index):
            remaining = discovery.remaining_game_budget(
                self.conn,
                crawl_run_id=job.crawl_run_id,
                provider=job.provider,
                params=params,
            )
            if remaining == 0:
                break
            last_result = fetch_chesscom_month(
                self.conn,
                job.target,
                year,
                month,
                config=self.config,
                transport=self.transport,
                sleeper=self.sleeper,
                job_id=job.id,
                crawl_run_id=job.crawl_run_id,
            )
            params["cursor_index"] = index + 1
            if job.id is not None:
                store.update_job_params(self.conn, job.id, params)
            if last_result.status_code not in {200, 304}:
                return last_result
        return last_result

    def _fetch_monthly_archive(self, job: DiscoveryJob) -> IngestResult:
        params = store.load_params(job.params_json)
        year = int(params["year"])
        month = int(params["month"])
        return fetch_chesscom_month(
            self.conn,
            job.target,
            year,
            month,
            config=self.config,
            transport=self.transport,
            sleeper=self.sleeper,
            job_id=job.id,
            crawl_run_id=job.crawl_run_id,
        )

    def _fetch_game_by_id(self, job: DiscoveryJob) -> IngestResult:
        if job.provider != "lichess":
            return IngestResult(job.provider, "game", 400, None, (), "Chess.com game-by-id requires archive resolution")
        return fetch_lichess_game(
            self.conn,
            job.target,
            config=self.config,
            transport=self.transport,
            sleeper=self.sleeper,
            job_id=job.id,
            crawl_run_id=job.crawl_run_id,
        )

    def _crawl_opponents(self, job: DiscoveryJob) -> ExecutionOutcome:
        if job.id is None or job.crawl_run_id is None:
            return ExecutionOutcome("error", "crawl_opponents requires a persisted crawl run")
        params = store.load_params(job.params_json)
        user_id = discovery.ensure_local_user(self.conn, provider=job.provider, username=job.target)
        remaining = discovery.remaining_game_budget(
            self.conn,
            crawl_run_id=job.crawl_run_id,
            provider=job.provider,
            params=params,
        )
        if remaining == 0:
            return ExecutionOutcome("skipped", "max-games cap reached before fetching user")
        fetch_result = self._fetch_user_games(job)
        if fetch_result.status_code not in {200, 304}:
            return _outcome_from_ingest(fetch_result)
        child_params = dict(params)
        child_params.pop("cursor_index", None)

        next_depth = job.depth + 1
        max_depth = int(child_params["max_depth"])
        if next_depth > max_depth:
            return ExecutionOutcome("done", f"fetched {job.target}; depth cap reached")

        user_id = discovery.ensure_local_user(self.conn, provider=job.provider, username=job.target)
        edges = discovery.opponents_of_user(
            self.conn,
            provider=job.provider,
            user_id=user_id,
            since=_int_or_none(params.get("since")),
            until=_int_or_none(params.get("until")),
        )
        frontier_edges = [
            edge
            for edge in edges
            if not _known_at_or_before_current_depth(
                self.conn,
                crawl_run_id=job.crawl_run_id,
                provider=job.provider,
                username=edge.opponent_username,
                current_depth=job.depth,
            )
        ]
        edge_count = discovery.record_discovery_edges(
            self.conn,
            crawl_run_id=job.crawl_run_id,
            provider=job.provider,
            from_user_id=user_id,
            depth=next_depth,
            edges=frontier_edges,
        )
        child_count = discovery.enqueue_opponent_children(
            self.conn,
            crawl_run_id=job.crawl_run_id,
            parent_job_id=job.id,
            provider=job.provider,
            params=child_params,
            next_depth=next_depth,
            edges=frontier_edges,
        )
        return ExecutionOutcome("done", f"edges={edge_count}; child_jobs={child_count}")

    def _refresh_run_status(self, crawl_run_id: int) -> None:
        counts = {row["state"]: int(row["count"]) for row in store.job_state_counts(self.conn, crawl_run_id=crawl_run_id)}
        if counts.get("pending", 0) or counts.get("in_progress", 0):
            status = "running"
            finished = False
        elif counts.get("blocked", 0):
            status = "paused"
            finished = False
        elif counts.get("error", 0):
            status = "failed"
            finished = True
        else:
            status = "done"
            finished = True
        store.update_crawl_run(
            self.conn,
            crawl_run_id,
            status=status,
            counters=discovery.run_counters(self.conn, crawl_run_id),
            finished=finished,
        )


def _outcome_from_ingest(result: IngestResult) -> ExecutionOutcome:
    if result.status_code in {200, 304}:
        return ExecutionOutcome("done", result.message)
    if result.status_code in {404, 410}:
        return ExecutionOutcome("skipped", result.message)
    if result.status_code == 429:
        return ExecutionOutcome("blocked", result.message)
    return ExecutionOutcome("error", result.message)


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _known_at_or_before_current_depth(
    conn: sqlite3.Connection,
    *,
    crawl_run_id: int,
    provider: str,
    username: str,
    current_depth: int,
) -> bool:
    known_depth = store.known_crawl_depth(
        conn,
        crawl_run_id=crawl_run_id,
        provider=provider,
        username=username,
    )
    return known_depth is not None and known_depth <= current_depth


def _months_between(since: int, until: int) -> list[tuple[int, int]]:
    start = datetime.fromtimestamp(since, tz=UTC)
    end = datetime.fromtimestamp(until - 1, tz=UTC)
    year, month = start.year, start.month
    months: list[tuple[int, int]] = []
    while (year, month) <= (end.year, end.month):
        months.append((year, month))
        month += 1
        if month == 13:
            year += 1
            month = 1
    return months
