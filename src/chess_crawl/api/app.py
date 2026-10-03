"""Authenticated HTTP requests enqueue work; separate workers execute it."""

from __future__ import annotations

import logging
import os
import secrets
from pathlib import Path
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict
from starlette.exceptions import HTTPException as StarletteHTTPException

from chess_crawl import __version__
from chess_crawl import application
from chess_crawl.jobs import state
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
    since: int
    until: int
    max_games: int


class CrawlBody(ImportBody):
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


class WorkerSnapshot(BaseModel):
    alive: bool
    status: Literal["absent", "running", "stopping", "stopped", "failed"]
    worker_id: str | None
    started_at: float | None = None
    heartbeat_at: float | None
    heartbeat_expires_at: float | None = None
    stopped_at: float | None = None
    current_job_id: int | None = None
    age_seconds: float | None


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: list[dict[str, Any]] | None = None


class ErrorResponse(BaseModel):
    error: ErrorDetail


def _error(status: int, code: str, message: str, **kwargs: Any) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}}, **kwargs)


def create_app(
    database_url: str | None = None,
    api_token: str | None = None,
    *,
    limits: application.Limits | None = None,
) -> FastAPI:
    """Build an API without opening or migrating an archive at startup.

    Initialize the configured PostgreSQL schema through the CLI before serving requests.
    Connections are opened and closed within each synchronous handler so they
    never move across the request thread pool.
    """
    archive = resolve_database_url(database_url)
    token = api_token if api_token is not None else os.getenv("CHESS_CRAWL_API_TOKEN", "")
    token_file = os.getenv("CHESS_CRAWL_API_TOKEN_FILE")
    if token and token_file:
        raise ValueError("Set either CHESS_CRAWL_API_TOKEN or CHESS_CRAWL_API_TOKEN_FILE, not both")
    if token_file:
        token = Path(token_file).read_text(encoding="utf-8").strip()
    if not token or not token.strip():
        raise ValueError("CHESS_CRAWL_API_TOKEN must be set before starting the HTTP API")
    if any(character.isspace() for character in token):
        raise ValueError("The HTTP API bearer token must not contain whitespace")
    request_limits = limits or application.Limits.from_env()
    app = FastAPI(
        title="chess-crawl API",
        version=__version__,
        description="Bounded, durable public chess-data collection and archive queries.",
    )

    def authenticate(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    ) -> None:
        if credentials is None or not secrets.compare_digest(
            credentials.credentials.encode("utf-8"), token.encode("utf-8"),
        ):
            raise HTTPException(status_code=401, detail="A valid bearer token is required")

    @app.exception_handler(application.ApplicationError)
    async def application_error(request: Request, exc: application.ApplicationError) -> JSONResponse:
        status = 404 if isinstance(exc, application.NotFound) else 409 if isinstance(exc, application.Conflict) else 422
        return _error(status, exc.code, exc.message)

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
        responses={code: {"model": ErrorResponse} for code in (401, 404, 409, 422, 503)},
    )

    @router.post("/imports", status_code=202, response_model=Submission, tags=["collection"])
    def submit_import(
        body: ImportBody,
        response: Response,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128)],
    ) -> dict[str, Any]:
        request = application.validate_import(application.ImportRequest(**body.model_dump()), limits=request_limits)
        idempotency_key = application.validate_idempotency_key(idempotency_key)
        with connection(archive, mode="rw") as conn:
            result = application.submit_import(conn, request, idempotency_key=idempotency_key, limits=request_limits)
        response.headers["Location"] = f"/v1/runs/{result['run_id']}"
        return result

    @router.post("/crawls", status_code=202, response_model=Submission, tags=["collection"])
    def submit_crawl(
        body: CrawlBody,
        response: Response,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128)],
    ) -> dict[str, Any]:
        request = application.validate_crawl(application.CrawlRequest(**body.model_dump()), limits=request_limits)
        idempotency_key = application.validate_idempotency_key(idempotency_key)
        with connection(archive, mode="rw") as conn:
            result = application.submit_crawl(conn, request, idempotency_key=idempotency_key, limits=request_limits)
        response.headers["Location"] = f"/v1/runs/{result['run_id']}"
        return result

    @router.get("/runs/{run_id}", tags=["jobs"])
    def get_run(run_id: int) -> dict[str, Any]:
        with connection(archive) as conn:
            return application.get_run(conn, run_id)

    @router.get("/jobs/{job_id}", tags=["jobs"])
    def get_job(job_id: int) -> dict[str, Any]:
        with connection(archive) as conn:
            return application.get_job(conn, job_id)

    @router.get("/worker", response_model=WorkerSnapshot, tags=["jobs"])
    def worker() -> dict[str, Any]:
        with connection(archive) as conn:
            return state.worker_status(conn)

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
    def summary() -> dict[str, Any]:
        with connection(archive) as conn:
            return application.summary(conn)

    app.include_router(router)
    return app
