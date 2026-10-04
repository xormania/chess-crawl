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
    validate_game_collection,
    validate_import,
    validate_integer,
    validate_page,
    validate_provider,
    validate_player_refresh,
    validate_username,
)
from chess_crawl.jobs import state
from chess_crawl.jobs.discovery import CrawlBounds, create_opponent_crawl
from chess_crawl.providers.registry import list_provider_infos
from chess_crawl.storage import application as submissions
from chess_crawl.storage import queries, workspaces
from chess_crawl.storage.db import Connection, Row, consistent_read, transaction
from chess_crawl.storage.events import archive_id


def submit_import(
    conn: Connection,
    request: ImportRequest,
    *,
    idempotency_key: str,
    limits: Limits = Limits(),
    workspace_id: str = "local",
) -> dict[str, Any]:
    request = validate_import(request, limits=limits)
    key = validate_idempotency_key(idempotency_key)

    def create() -> tuple[int, list[int]]:
        params = {"strategy": "import", **asdict(request), "limit": request.max_games}
        if request.provider == "lichess" and request.collection_mode != "bounded":
            if request.since is not None:
                params["since_ms"] = request.since * 1000
            if request.until is not None:
                params["until_ms"] = request.until * 1000
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

    return _submit(conn, key=key, operation="import", request=request, create=create, workspace_id=workspace_id)


def submit_crawl(
    conn: Connection,
    request: CrawlRequest,
    *,
    idempotency_key: str,
    limits: Limits = Limits(),
    workspace_id: str = "local",
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

    return _submit(conn, key=key, operation="crawl", request=request, create=create, workspace_id=workspace_id)


def _submit(
    conn: Connection,
    *,
    key: str,
    operation: str,
    request: ImportRequest | CrawlRequest | dict[str, Any],
    create: Callable[[], tuple[int, list[int]]],
    workspace_id: str = "local",
) -> dict[str, Any]:
    canonical = json.dumps(request if isinstance(request, dict) else asdict(request), sort_keys=True, separators=(",", ":"))
    # Serialize this scoped submission identity while unrelated work can write.
    # Identity survives terminal job/run states.
    with transaction(conn):
        workspaces.submission_context(conn, workspace_id)
        submissions.lock_submission(conn, workspace_id, key)
        existing = submissions.get_submission(conn, key, workspace_id)
        if existing is not None:
            stored_request = json.loads(existing["request_json"])
            if operation == "import":
                stored_request.setdefault("collection_mode", "bounded")
                stored_request.setdefault("batch_size", 1)
            if existing["operation"] != operation or stored_request != json.loads(canonical):
                raise Conflict("Idempotency key already identifies a different request", code="idempotency_conflict")
            return {
                "run_id": int(existing["crawl_run_id"]),
                "job_ids": json.loads(existing["job_ids_json"]),
                "replayed": True,
            }
        run_id, job_ids = create()
        submissions.record_submission(
            conn, key=key, operation=operation, request_json=canonical, run_id=run_id, job_ids=job_ids, workspace_id=workspace_id,
        )
        return {"run_id": run_id, "job_ids": job_ids, "replayed": False}


@consistent_read
def get_run(conn: Connection, run_id: int, *, workspace_id: str = "local") -> dict[str, Any]:
    validate_integer(run_id, "run_id", minimum=1, maximum=2**63 - 1)
    workspaces.require_run(conn, run_id, workspace_id)
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
    result["freshness"] = queries.archive_freshness(conn, provider=result["provider"], owner_scope=workspace_id)
    return result


@consistent_read
def get_job(conn: Connection, job_id: int, *, workspace_id: str = "local") -> dict[str, Any]:
    validate_integer(job_id, "job_id", minimum=1, maximum=2**63 - 1)
    workspaces.require_job(conn, job_id, workspace_id)
    job = state.get_job(conn, job_id)
    if job is None:
        raise NotFound("Job not found", code="job_not_found")
    result = asdict(job)
    result["archive_id"] = archive_id(conn)
    result["params"] = state.load_params(result.pop("params_json"))
    result["workspace_id"] = workspace_id
    for internal in ("ownership_token", "owner_worker_id", "ownership_generation", "owner_backend_pid", "worker_id", "claimed_by", "worker_backend", "backend"):
        result.pop(internal,None)
        result["params"].pop(internal,None)
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
def summary(conn: Connection, *, workspace_id: str = "local") -> dict[str, Any]:
    report = queries.summary_report(conn,workspace_id=workspace_id)
    return {
        "providers": [dict(row) for row in report["providers"]],
        "raw_payloads": report["raw_payloads"],
        "runs": [dict(row) for row in report["runs"]],
        "jobs": [dict(row) for row in report["jobs"]],
        "freshness": queries.archive_freshness(conn,owner_scope=workspace_id),
    }


def list_providers() -> list[dict[str, Any]]:
    return [asdict(provider) for provider in list_provider_infos()]


def submit_upgrade(
    conn: Connection, request: dict[str, Any], *, idempotency_key: str, workspace_id: str = "local",
) -> dict[str, Any]:
    """Queue a local replay upgrade; no network work occurs in the request."""
    from typing import cast
    from chess_crawl.jobs.models import JobKind
    from chess_crawl.storage.working_sets import digest
    provider = validate_provider(request["provider"])
    validate_integer(request["batch_size"], "batch_size", minimum=1, maximum=100)
    key = validate_idempotency_key(idempotency_key)
    upgrade_id = "upgrade:" + digest({"workspace":workspace_id, "provider":provider, "name":request["name"], "key":key})
    params = {**request, "provider":provider, "upgrade_id":upgrade_id, "owner_scope":workspace_id}

    def create() -> tuple[int, list[int]]:
        run_id, job_id = state.create_crawl_run_with_root_job(
            conn, provider=provider, seed_spec=f"{provider} local replay", params=params,
            root_kind=cast(JobKind, "reprocess_archive"), root_target=upgrade_id,
        )
        return run_id, [job_id]

    return _submit(conn,key=key,operation="upgrade",request=params,create=create,workspace_id=workspace_id)


def submit_resource(
    conn: Connection, request: dict[str, Any], *, idempotency_key: str, workspace_id: str = "local",
) -> dict[str, Any]:
    from typing import cast
    from chess_crawl.jobs.models import JobKind
    from chess_crawl.providers.resources import get_resource, resource_owner_scope
    provider, username = validate_provider(request["provider"]), validate_username(request["username"])
    key = validate_idempotency_key(idempotency_key)
    try:
        resource = get_resource(provider,request["resource_key"])
        parameters = resource.parameters(request.get("parameters"))
        scope = resource_owner_scope(resource,workspace_id)
    except ValueError as exc:
        from chess_crawl.application.errors import ValidationError
        raise ValidationError(str(exc),code="invalid_resource") from exc
    params = {"provider":provider,"username":username,"resource_key":resource.key,"parameters":parameters,"owner_scope":scope}

    def create() -> tuple[int, list[int]]:
        run_id, job_id = state.create_crawl_run_with_root_job(
            conn,provider=provider,seed_spec=f"{provider}/{username} {resource.key}", params=params,
            root_kind=cast(JobKind,"fetch_user_resource"),root_target=username,
        )
        return run_id,[job_id]

    return _submit(conn,key=key,operation="resource",request=params,create=create,workspace_id=workspace_id)


def submit_player_refresh(
    conn: Connection, *, provider: str, username: str, statistics: bool,
    idempotency_key: str, workspace_id: str = "local",
) -> dict[str, Any]:
    """Queue exactly one known profile or statistics job, without acquisition."""
    from chess_crawl.jobs.models import JobKind
    provider,username = validate_player_refresh(provider,username,statistics=statistics)
    key = validate_idempotency_key(idempotency_key)
    kind: JobKind = "fetch_user_stats" if statistics else "fetch_user_profile"
    params = {"username": username, "refresh": "statistics" if statistics else "profile"}

    def create() -> tuple[int, list[int]]:
        run_id, job_id = state.create_crawl_run_with_root_job(
            conn, provider=provider, seed_spec=f"{provider}/{username} {params['refresh']} refresh",
            params=params, root_kind=kind, root_target=username,
        )
        return run_id, [job_id]

    return _submit(conn, key=key, operation="resource", request={"provider": provider, **params},
                   create=create, workspace_id=workspace_id)


def submit_game_collection(
    conn: Connection, *, provider: str, game_id: str, idempotency_key: str, workspace_id: str = "local",
) -> dict[str, Any]:
    """Queue a public Lichess game export using its public eight-character ID."""
    provider,game_id = validate_game_collection(provider,game_id)
    key = validate_idempotency_key(idempotency_key)
    params = {"provider":provider,"game_id":game_id,"collection":"single_game"}

    def create() -> tuple[int,list[int]]:
        run_id,job_id = state.create_crawl_run_with_root_job(
            conn,provider=provider,seed_spec=f"{provider}/{game_id} game",params=params,
            root_kind="fetch_game_by_id",root_target=game_id,
        )
        return run_id,[job_id]

    return _submit(conn,key=key,operation="resource",request=params,create=create,workspace_id=workspace_id)
