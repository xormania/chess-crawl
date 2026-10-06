"""Bounded game acquisition in durable, independently scheduled source units."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import httpx

from chess_crawl.config import Config
from chess_crawl.ingest import fetch_chesscom_month, fetch_lichess_games
from chess_crawl.jobs import discovery, state
from chess_crawl.jobs.models import source_provider, CollectionResult, DiscoveryJob
from chess_crawl.providers.registry import ProviderSession
from chess_crawl.storage.db import Connection


def execute_bounded_acquisition(
    conn: Connection, job: DiscoveryJob, params: Mapping[str, Any], *,
    config: Config | None = None, transport: httpx.BaseTransport | None = None,
    session: ProviderSession | None = None, sleeper=None,
    clock: Callable[[], float] = time.time,
    stop_requested: Callable[[], bool] | None = None,
) -> CollectionResult:
    """Capture one month or stream; normalization decides the next unit's allowance.

    The scheduler waits for this parent's normalization children before another
    claim. Its next claim either captures another unit or completes collection.
    """
    if job.id is None:
        raise ValueError("Bounded acquisition requires a persisted job")
    if stop_requested is not None and stop_requested():
        return CollectionResult(False, 0, (), "Acquisition stopped; checkpoint retained")
    remaining = discovery.remaining_game_budget(
        conn, crawl_run_id=job.crawl_run_id, provider=source_provider(job), params=params,
    )
    if remaining == 0 or params.get("bounded_capture_complete"):
        return CollectionResult(True, 0, (), "bounded selection complete")
    options = dict(config=config, transport=transport, sleeper=sleeper,
                   session=session, job_id=job.id, crawl_run_id=job.crawl_run_id)
    if source_provider(job) == "chess.com":
        since, until = params.get("since"), params.get("until")
        if type(since) is not int or type(until) is not int:
            raise ValueError("Chess.com jobs require since/until")
        months = _months_between(since, until)
        index = int(params.get("cursor_index") or 0)
        if index >= len(months):
            return CollectionResult(True, 0, (), "monthly selection complete")
        year, month = months[index]
        result = fetch_chesscom_month(conn, job.target, year, month, max_games=remaining, **options)
        state.checkpoint_job(conn, job.id, params, cursor_index=index + 1, status_code=result.status_code)
    elif source_provider(job) == "lichess":
        limit = int(params.get("limit") or remaining or params.get("max_games") or 0)
        if limit <= 0:
            raise ValueError("Lichess jobs require a positive limit")
        if remaining is not None:
            limit = min(limit, remaining)
        result = fetch_lichess_games(
            conn, job.target, since=params.get("since"), until=params.get("until"), limit=limit, **options,
        )
        if result.status_code in {200, 304}:
            state.update_job_params(conn, job.id, {**params, "bounded_capture_complete": True})
    else:
        raise ValueError(f"unsupported collection provider: {source_provider(job)}")
    return CollectionResult(
        False, 1 if result.status_code in {200, 304} else 0, result.normalized_ids,
        result.message, result.status_code, result.retry_after,
    )


def _months_between(since: int, until: int) -> list[tuple[int, int]]:
    if since >= until:
        raise ValueError("since must be earlier than until")
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
