"""Authenticated HTTP requests enqueue work; separate workers execute it."""

from __future__ import annotations

import logging
from typing import Annotated, Any, Literal, Mapping

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from chess_crawl import __version__
from chess_crawl import application
from chess_crawl.api.auth import configured_authenticator
from chess_crawl.jobs import state
from chess_crawl.jobs.budget import BudgetPolicy, QuotaExceeded
from chess_crawl.storage import workspaces
from chess_crawl.storage.db import DatabaseError, connection
from chess_crawl.storage.db import database_url as resolve_database_url
from chess_crawl.storage.migrations import SCHEMA_VERSION, current_version


_bearer = HTTPBearer(auto_error=False)
_logger = logging.getLogger(__name__)


class ImportBody(BaseModel):
    """Provider timestamps are Unix seconds, with an exclusive upper bound."""

    model_config = ConfigDict(extra="forbid", strict=True)

    provider: str
    username: str
    since: int | None = None
    until: int | None = None
    max_games: int
    collection_mode: Literal["bounded", "full", "incremental", "backfill"] = "bounded"
    batch_size: int = 1


class CrawlBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    provider: str
    username: str
    since: int
    until: int
    max_games: int
    max_depth: int
    max_users: int
    max_jobs: int


class Submission(BaseModel):
    run_id: int
    job_ids: list[int]
    replayed: bool


class Page(BaseModel):
    items: list[dict[str, Any]]
    next_cursor: int | None
    total: int
    freshness: dict[str, Any]


class WorkerStatus(BaseModel):
    alive: bool
    status: Literal["absent", "running", "stopping", "stopped", "failed"]
    worker_id: str | None
    started_at: float | None = None
    heartbeat_at: float | None
    heartbeat_expires_at: float | None = None
    stopped_at: float | None = None
    current_job_id: int | None = None
    age_seconds: float | None


class WorkerSnapshot(WorkerStatus):
    active_workers: int = 0
    workers: list[WorkerStatus] = Field(default_factory=list)
    worker_limit: int = 128
    workers_truncated: bool = False


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: list[dict[str, Any]] | None = None
    quota: dict[str, Any] | None = None


class ErrorResponse(BaseModel):
    error: ErrorDetail


def _error(status: int, code: str, message: str, **kwargs: Any) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}}, **kwargs)


def create_app(
    database_url: str | None = None,
    api_token: str | None = None,
    *,
    limits: application.Limits | None = None,
    workspace_tokens: Mapping[str, str] | None = None,
    budget_policy: BudgetPolicy | None = None,
    auth_mode: str | None = None,
) -> FastAPI:
    """Build an API without opening or migrating an archive at startup.

    Initialize the configured PostgreSQL schema through the CLI before serving requests.
    Connections are opened and closed within each synchronous handler so they
    never move across the request thread pool.
    """
    archive = resolve_database_url(database_url)
    authenticator = configured_authenticator(archive, api_token, workspace_tokens, auth_mode)
    request_limits = limits or application.Limits.from_env()
    work_policy = budget_policy if budget_policy is not None else BudgetPolicy.from_env()
    app = FastAPI(
        title="chess-crawl API",
        version=__version__,
        description="Bounded, durable public chess-data collection and archive queries.",
    )

    def authenticate(
        request: Request,
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    ) -> None:
        supplied = "" if credentials is None else credentials.credentials
        workspace_id = authenticator.resolve(supplied)
        if workspace_id is None:
            raise HTTPException(status_code=401, detail="A valid bearer token is required")
        # Ownership comes only from trusted server credential configuration.
        request.state.workspace_id = workspace_id

    @app.exception_handler(application.ApplicationError)
    async def application_error(request: Request, exc: application.ApplicationError) -> JSONResponse:
        status = 404 if isinstance(exc, application.NotFound) else 409 if isinstance(exc, application.Conflict) else 422
        return _error(status, exc.code, exc.message)

    @app.exception_handler(QuotaExceeded)
    async def quota_error(request: Request, exc: QuotaExceeded) -> JSONResponse:
        return JSONResponse(status_code=429, content={"error": {
            "code": exc.code, "message": str(exc), "quota": {
                "dimension": exc.dimension, "remaining": exc.remaining,
                "reset_at": exc.reset_at, "budget_id": exc.budget_id,
            },
        }})

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={"error": {
                "code": "validation_error",
                "message": "The request is invalid",
                "details": [
                    {"location": list(error["loc"]), "message": error["msg"], "type": error["type"]}
                    for error in exc.errors()
                ],
            }},
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        codes = {401: "unauthorized", 404: "not_found", 405: "method_not_allowed", 503: "archive_unavailable"}
        headers = dict(exc.headers or {})
        if exc.status_code == 401:
            headers["WWW-Authenticate"] = "Bearer"
        return _error(exc.status_code, codes.get(exc.status_code, "http_error"), str(exc.detail), headers=headers)

    @app.exception_handler(DatabaseError)
    async def database_error(request: Request, exc: Exception) -> JSONResponse:
        _logger.warning("Archive request failed: %s", type(exc).__name__)
        return _error(503, "archive_unavailable", "The archive is unavailable", headers={"Retry-After": "5"})

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        _logger.exception("Unhandled archive request error", exc_info=exc)
        return _error(500, "internal_error", "The request could not be completed")

    @app.get("/health/live", tags=["health"])
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", tags=["health"], dependencies=[Depends(authenticate)])
    def ready() -> dict[str, str | int]:
        with connection(archive) as conn:
            version = current_version(conn)
            if version != SCHEMA_VERSION:
                raise HTTPException(status_code=503, detail="The archive schema requires initialization")
            return {"status": "ready", "schema_version": version}

    router = APIRouter(
        prefix="/v1",
        dependencies=[Depends(authenticate)],
        responses={code: {"model": ErrorResponse} for code in (401, 404, 409, 422, 429, 503)},
    )
    from chess_crawl.api.compat import register_compat_routes
    # Literal lookup/status paths must precede numeric resource parameters.
    register_compat_routes(router, archive, request_limits, work_policy)

    @router.post("/imports", status_code=202, response_model=Submission, tags=["collection"])
    def submit_import(
        body: ImportBody,
        response: Response,
        http_request: Request,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128)],
    ) -> dict[str, Any]:
        request = application.validate_import(application.ImportRequest(**body.model_dump()), limits=request_limits)
        idempotency_key = application.validate_idempotency_key(idempotency_key)
        with connection(archive, mode="rw") as conn:
            result = application.submit_import(conn, request, idempotency_key=idempotency_key, limits=request_limits, workspace_id=http_request.state.workspace_id, budget_policy=work_policy)
        response.headers["Location"] = f"/v1/runs/{result['run_id']}"
        return result

    @router.post("/crawls", status_code=202, response_model=Submission, tags=["collection"])
    def submit_crawl(
        body: CrawlBody,
        response: Response,
        http_request: Request,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128)],
    ) -> dict[str, Any]:
        request = application.validate_crawl(application.CrawlRequest(**body.model_dump()), limits=request_limits)
        idempotency_key = application.validate_idempotency_key(idempotency_key)
        with connection(archive, mode="rw") as conn:
            result = application.submit_crawl(conn, request, idempotency_key=idempotency_key, limits=request_limits, workspace_id=http_request.state.workspace_id, budget_policy=work_policy)
        response.headers["Location"] = f"/v1/runs/{result['run_id']}"
        return result

    @router.get("/runs/{run_id}", tags=["jobs"])
    def get_run(run_id: int, request: Request) -> dict[str, Any]:
        with connection(archive) as conn:
            return application.get_run(conn, run_id, workspace_id=request.state.workspace_id)

    @router.get("/jobs/{job_id}", tags=["jobs"])
    def get_job(job_id: int, request: Request) -> dict[str, Any]:
        with connection(archive) as conn:
            return application.get_job(conn, job_id, workspace_id=request.state.workspace_id)

    @router.get("/worker", response_model=WorkerSnapshot, tags=["jobs"])
    def worker(request: Request) -> dict[str, Any]:
        with connection(archive) as conn:
            return workspaces.worker_snapshot(conn, state.worker_status(conn), request.state.workspace_id)

    @router.get("/games", response_model=Page, tags=["archive"])
    def games(
        provider: str | None = None,
        after: Annotated[int | None, Query(ge=0)] = None,
        limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> dict[str, Any]:
        with connection(archive) as conn:
            return application.list_games(conn, provider=provider, after=after, limit=limit, limits=request_limits)

    @router.get("/users", response_model=Page, tags=["archive"])
    def users(
        provider: str | None = None,
        after: Annotated[int | None, Query(ge=0)] = None,
        limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> dict[str, Any]:
        with connection(archive) as conn:
            return application.list_users(conn, provider=provider, after=after, limit=limit, limits=request_limits)

    @router.get("/users/{provider}/{username}/opponents", response_model=Page, tags=["archive"])
    def opponents(
        provider: str,
        username: str,
        after: Annotated[int | None, Query(ge=0)] = None,
        limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> dict[str, Any]:
        with connection(archive) as conn:
            return application.list_opponents(conn, provider, username, after=after, limit=limit, limits=request_limits)

    @router.get("/providers", tags=["archive"])
    def providers() -> list[dict[str, Any]]:
        return application.list_providers()

    @router.get("/summary", tags=["archive"])
    def summary(request: Request) -> dict[str, Any]:
        with connection(archive) as conn:
            return application.summary(conn,workspace_id=request.state.workspace_id)

    from chess_crawl.api.archive import register_archive_routes
    register_archive_routes(router, archive, request_limits, work_policy)
    app.include_router(router)
    return app
