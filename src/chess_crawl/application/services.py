"""Shared application operations for CLI, HTTP, and other adapters."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict
from typing import Any

from chess_crawl.application.errors import Conflict, NotFound
from chess_crawl.application.models import CrawlRequest, ImportRequest, Limits
from chess_crawl.application.validation import (
    validate_crawl,
    validate_idempotency_key,
    validate_import,
    validate_integer,
    validate_page,
    validate_provider,
    validate_username,
)
from chess_crawl.jobs import state
from chess_crawl.jobs.discovery import CrawlBounds, create_opponent_crawl
from chess_crawl.providers.registry import list_provider_infos
from chess_crawl.storage import application as submissions
from chess_crawl.storage import queries
from chess_crawl.storage.db import Connection, Row, consistent_read, operation_lock, transaction
from chess_crawl.storage.events import archive_id


def submit_import(
    conn: Connection,
    request: ImportRequest,
    *,
    idempotency_key: str,
    limits: Limits = Limits(),
) -> dict[str, Any]:
    request = validate_import(request, limits=limits)
    key = validate_idempotency_key(idempotency_key)

    def create() -> tuple[int, list[int]]:
        params = {"strategy": "import", **asdict(request), "limit": request.max_games}
        run_id = state.create_crawl_run(
            conn, provider=request.provider, seed_spec=f"{request.provider}/{request.username} import", params=params,
        )
        profile = state.enqueue_job(
            conn, provider=request.provider, kind="fetch_user_profile", target=request.username,
            params=params, crawl_run_id=run_id, priority=10,
        )
        games = state.enqueue_job(
            conn, provider=request.provider, kind="fetch_user_games", target=request.username,
            params=params, crawl_run_id=run_id, parent_job_id=profile.job_id, priority=20,
        )
        return run_id, [profile.job_id, games.job_id]

    return _submit(conn, key=key, operation="import", request=request, create=create)


def submit_crawl(
    conn: Connection,
    request: CrawlRequest,
    *,
    idempotency_key: str,
    limits: Limits = Limits(),
) -> dict[str, Any]:
    request = validate_crawl(request, limits=limits)
    key = validate_idempotency_key(idempotency_key)

    def create() -> tuple[int, list[int]]:
        run_id, root_job_id = create_opponent_crawl(
            conn, provider=request.provider, username=request.username,
            since=request.since, until=request.until,
            bounds=CrawlBounds(
                max_depth=request.max_depth, max_users=request.max_users,
                max_games=request.max_games, max_jobs=request.max_jobs,
            ),
        )
        return run_id, [root_job_id]

    return _submit(conn, key=key, operation="crawl", request=request, create=create)


def _submit(
    conn: Connection,
    *,
    key: str,
    operation: str,
    request: ImportRequest | CrawlRequest,
    create: Callable[[], tuple[int, list[int]]],
) -> dict[str, Any]:
    canonical = json.dumps(asdict(request), sort_keys=True, separators=(",", ":"))
    # Serialize this submission identity while allowing unrelated work to write.
    # Identity survives terminal job/run states.
    with transaction(conn):
        operation_lock(conn, "submission", key)
        existing = submissions.get_submission(conn, key)
        if existing is not None:
            if existing["operation"] != operation or existing["request_json"] != canonical:
                raise Conflict("Idempotency key already identifies a different request", code="idempotency_conflict")
            return {
                "run_id": int(existing["crawl_run_id"]),
                "job_ids": json.loads(existing["job_ids_json"]),
                "replayed": True,
            }
        run_id, job_ids = create()
        submissions.record_submission(
            conn, key=key, operation=operation, request_json=canonical, run_id=run_id, job_ids=job_ids,
        )
        return {"run_id": run_id, "job_ids": job_ids, "replayed": False}


@consistent_read
def get_run(conn: Connection, run_id: int) -> dict[str, Any]:
    validate_integer(run_id, "run_id", minimum=1, maximum=2**63 - 1)
    row = state.get_run(conn, run_id)
    if row is None:
        raise NotFound("Crawl run not found", code="run_not_found")
    result = dict(row)
    result["archive_id"] = archive_id(conn)
    result["revision"] = int(result["revision"])
    result["params"] = state.load_params(result.pop("params_json"))
    result.pop("counters_json", None)
    result["counters"] = state.run_counters(conn, run_id)
    result["job_ids"] = state.job_ids_for_run(conn, run_id)
    result["freshness"] = queries.archive_freshness(conn, provider=result["provider"])
    return result


@consistent_read
def get_job(conn: Connection, job_id: int) -> dict[str, Any]:
    validate_integer(job_id, "job_id", minimum=1, maximum=2**63 - 1)
    job = state.get_job(conn, job_id)
    if job is None:
        raise NotFound("Job not found", code="job_not_found")
    result = asdict(job)
    result["archive_id"] = archive_id(conn)
    result["params"] = state.load_params(result.pop("params_json"))
    return result


@consistent_read
def list_games(
    conn: Connection,
    *,
    provider: str | None = None,
    after: int | None = None,
    limit: int | None = None,
    limits: Limits = Limits(),
) -> dict[str, Any]:
    provider = None if provider is None else validate_provider(provider)
    cursor, size = validate_page(after=after, limit=limit, limits=limits)
    rows, total = queries.game_page(conn, provider=provider, after=cursor, limit=size + 1)
    page = _page(conn, rows, total=total, size=size, provider=provider)
    for item in page["items"]:
        item["rated"] = bool(item["rated"])
        item["is_live"] = bool(item["is_live"])
    return page


@consistent_read
def list_users(
    conn: Connection,
    *,
    provider: str | None = None,
    after: int | None = None,
    limit: int | None = None,
    limits: Limits = Limits(),
) -> dict[str, Any]:
    provider = None if provider is None else validate_provider(provider)
    cursor, size = validate_page(after=after, limit=limit, limits=limits)
    rows, total = queries.user_page(conn, provider=provider, after=cursor, limit=size + 1)
    return _page(conn, rows, total=total, size=size, provider=provider)


@consistent_read
def list_opponents(
    conn: Connection,
    provider: str,
    username: str,
    *,
    after: int | None = None,
    limit: int | None = None,
    limits: Limits = Limits(),
) -> dict[str, Any]:
    provider = validate_provider(provider)
    username = validate_username(username)
    cursor, size = validate_page(after=after, limit=limit, limits=limits)
    user = queries.query_user(conn, provider, username)
    if user is None:
        raise NotFound("Provider user not found", code="user_not_found")
    rows, total = queries.opponent_page(conn, provider=provider, user_id=user.id, after=cursor, limit=size + 1)
    return _page(conn, rows, total=total, size=size, provider=provider)


def _page(
    conn: Connection,
    rows: list[Row],
    *,
    total: int,
    size: int,
    provider: str | None,
) -> dict[str, Any]:
    items = [dict(row) for row in rows[:size]]
    return {
        "items": items,
        "next_cursor": int(items[-1]["id"]) if len(rows) > size else None,
        "total": total,
        "freshness": queries.archive_freshness(conn, provider=provider),
    }


@consistent_read
def summary(conn: Connection) -> dict[str, Any]:
    report = queries.summary_report(conn)
    return {
        "providers": [dict(row) for row in report["providers"]],
        "raw_payloads": report["raw_payloads"],
        "runs": [dict(row) for row in report["runs"]],
        "jobs": [dict(row) for row in report["jobs"]],
        "freshness": queries.archive_freshness(conn),
    }


def list_providers() -> list[dict[str, Any]]:
    return [asdict(provider) for provider in list_provider_infos()]
