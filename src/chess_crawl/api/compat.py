"""Product API coverage for former archive-query and export commands."""
from __future__ import annotations

import csv
import io
import json
import tempfile
import time
from collections.abc import Generator
from contextlib import closing
from dataclasses import asdict
from typing import Annotated, Any, Literal, TextIO

import anyio
from fastapi import APIRouter, Header, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict
from starlette.concurrency import run_in_threadpool
from starlette.types import Receive, Scope, Send

from chess_crawl import application
from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.api.exports import ExportLimits, ExportSpool, check_export_bounds
from chess_crawl.application.services import submit_game_collection, submit_player_refresh
from chess_crawl.application.validation import (
    validate_game_collection, validate_idempotency_key, validate_page,
    validate_player_refresh, validate_provider, validate_username,
)
from chess_crawl.storage import api_views, queries
from chess_crawl.storage.db import connection, transaction


class PlayerRefreshBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    provider: str
    username: str


class GameCollectionBody(BaseModel):
    model_config = ConfigDict(extra="forbid",strict=True)
    provider: str
    game_id: str


class ArchiveExportResponse(StreamingResponse):
    """Close the database iterator even when ASGI send/cancellation interrupts it."""

    def __init__(self,chunks:Generator[str,None,None]|ExportSpool,*,media_type:str,headers:dict[str,str],
                 download_seconds:int = 300) -> None:
        self.chunks = chunks
        self.download_seconds = download_seconds
        super().__init__(chunks,media_type=media_type,headers=headers)

    async def __call__(self,scope:Scope,receive:Receive,send:Send) -> None:
        try:
            with anyio.fail_after(self.download_seconds):
                await super().__call__(scope,receive,send)
        finally:
            # Wait for any in-flight next() before closing, including disconnect
            # cancellation. Never leave an archive snapshot waiting on GC.
            with anyio.CancelScope(shield=True):
                await run_in_threadpool(self.chunks.close)


def register_compat_routes(router: APIRouter, archive: str, limits: application.Limits, budget_policy: BudgetPolicy) -> None:
    export_limits = ExportLimits.from_env()
    @router.get("/games/lookup", tags=["archive"])
    def lookup_game(provider: str, key: Annotated[str,Query(min_length=1,max_length=2048)]) -> dict[str,Any]:
        provider = validate_provider(provider)
        with connection(archive) as conn,transaction(conn,write=False):
            game = queries.query_game(conn,provider,key)
            if game is None:
                raise application.NotFound("Provider game not found",code="game_not_found")
            return asdict(game)

    @router.get("/users/{provider}/{username}/summary", tags=["reports"])
    def player_summary(provider: str, username: str) -> dict[str,Any]:
        provider,username = validate_provider(provider),validate_username(username)
        with connection(archive) as conn,transaction(conn,write=False):
            report = queries.user_game_summary(conn,provider,username)
            if report is None:
                raise application.NotFound("Provider user not found",code="user_not_found")
            return dict(report)

    @router.get("/reports/games-by-month", tags=["reports"])
    def months(provider: str, after: Annotated[str,Query(max_length=20)] = "",
               limit: Annotated[int|None,Query(ge=1)] = None) -> dict[str,Any]:
        provider = validate_provider(provider)
        _,size = validate_page(after=None,limit=limit,limits=limits)
        with connection(archive) as conn,transaction(conn,write=False):
            return api_views.months_page(conn,provider=provider,after=after,limit=size)

    @router.get("/raw", tags=["archive"])
    def raw_catalog(request: Request, provider: str|None = None,
                    after: Annotated[int|None,Query(ge=0)] = None,
                    limit: Annotated[int|None,Query(ge=1)] = None) -> dict[str,Any]:
        provider = None if provider is None else validate_provider(provider)
        cursor,size = validate_page(after=after,limit=limit,limits=limits)
        with connection(archive) as conn,transaction(conn,write=False):
            return api_views.raw_page(conn,owner_scope=request.state.workspace_id,provider=provider,after=cursor,limit=size)

    @router.get("/jobs", tags=["jobs"])
    def jobs(request: Request, run_id: Annotated[int|None,Query(ge=1)] = None,
             after: Annotated[int|None,Query(ge=0)] = None,
             limit: Annotated[int|None,Query(ge=1)] = None) -> dict[str,Any]:
        cursor,size = validate_page(after=after,limit=limit,limits=limits)
        with connection(archive) as conn,transaction(conn,write=False):
            return api_views.jobs_page(conn,workspace_id=request.state.workspace_id,run_id=run_id,after=cursor,limit=size)

    @router.get("/runs", tags=["jobs"])
    def runs(request: Request, after: Annotated[int|None,Query(ge=0)] = None,
             limit: Annotated[int|None,Query(ge=1)] = None) -> dict[str,Any]:
        cursor,size = validate_page(after=after,limit=limit,limits=limits)
        with connection(archive) as conn,transaction(conn,write=False):
            return api_views.runs_page(conn,workspace_id=request.state.workspace_id,after=cursor,limit=size)

    @router.get("/jobs/status", tags=["jobs"])
    def jobs_status(request: Request, run_id: Annotated[int|None,Query(ge=1)] = None) -> dict[str,Any]:
        with connection(archive) as conn,transaction(conn,write=False):
            return api_views.jobs_status(conn,workspace_id=request.state.workspace_id,run_id=run_id)

    def refresh(body: PlayerRefreshBody, request: Request, response: Response,
                idempotency_key: str, *, statistics: bool) -> dict[str,Any]:
        provider,username = validate_player_refresh(body.provider,body.username,statistics=statistics)
        idempotency_key = validate_idempotency_key(idempotency_key)
        with connection(archive,mode="rw") as conn:
            result = submit_player_refresh(conn,provider=provider,username=username,statistics=statistics,
                                           idempotency_key=idempotency_key,workspace_id=request.state.workspace_id,budget_policy=budget_policy)
        response.headers["Location"] = f"/v1/runs/{result['run_id']}"
        return result

    @router.post("/profiles/refresh", status_code=202, tags=["collection"])
    def refresh_profile(body: PlayerRefreshBody, request: Request, response: Response,
                        idempotency_key: Annotated[str,Header(alias="Idempotency-Key",min_length=1,max_length=128)]) -> dict[str,Any]:
        return refresh(body,request,response,idempotency_key,statistics=False)

    @router.post("/stats/refresh", status_code=202, tags=["collection"])
    def refresh_statistics(body: PlayerRefreshBody, request: Request, response: Response,
                           idempotency_key: Annotated[str,Header(alias="Idempotency-Key",min_length=1,max_length=128)]) -> dict[str,Any]:
        return refresh(body,request,response,idempotency_key,statistics=True)

    @router.post("/games/collect",status_code=202,tags=["collection"])
    def collect_game(body: GameCollectionBody,request: Request,response: Response,
                     idempotency_key: Annotated[str,Header(alias="Idempotency-Key",min_length=1,max_length=128)]) -> dict[str,Any]:
        provider,game_id = validate_game_collection(body.provider,body.game_id)
        idempotency_key = validate_idempotency_key(idempotency_key)
        with connection(archive,mode="rw") as conn:
            result = submit_game_collection(conn,provider=provider,game_id=game_id,idempotency_key=idempotency_key,
                                            workspace_id=request.state.workspace_id,budget_policy=budget_policy)
        response.headers["Location"] = f"/v1/runs/{result['run_id']}"
        return result

    def export(kind: Literal["games","users","graph"], request: Request, provider: str|None) -> StreamingResponse:
        provider = None if provider is None else validate_provider(provider)
        extension = "csv" if kind=="graph" else "jsonl"
        chunks = _prepare_export(archive,kind,provider,request.state.workspace_id,limits=export_limits)
        return ArchiveExportResponse(
            chunks,
            media_type="text/csv" if kind=="graph" else "application/x-ndjson",
            headers={"Content-Disposition":f'attachment; filename="{kind}.{extension}"'},
            download_seconds=export_limits.download_seconds,
        )

    @router.get("/exports/games.jsonl", tags=["exports"])
    def games_export(request: Request, provider: str|None = None) -> StreamingResponse:
        return export("games",request,provider)

    @router.get("/exports/users.jsonl", tags=["exports"])
    def users_export(request: Request, provider: str|None = None) -> StreamingResponse:
        return export("users",request,provider)

    @router.get("/exports/graph.csv", tags=["exports"])
    def graph_export(request: Request, provider: str|None = None) -> StreamingResponse:
        return export("graph",request,provider)


def _export_chunks(archive: str, kind: str, provider: str|None, workspace_id: str) -> Generator[str,None,None]:
    # Compatibility helper: even its first yield occurs after the DB is closed.
    with closing(_prepare_export(archive,kind,provider,workspace_id,limits=ExportLimits.from_env())) as spool:
        yield from spool


def _prepare_export(archive: str, kind: str, provider: str|None, workspace_id: str,
                    *, limits: ExportLimits) -> ExportSpool:
    deadline = time.monotonic()+limits.prepare_seconds
    spool = tempfile.TemporaryFile(mode="w+t",encoding="utf-8",newline="")
    try:
        _write_export(archive,kind,provider,workspace_id,spool=spool,limits=limits,deadline=deadline)
        check_export_bounds(limits=limits,rows=0,bytes_written=0,deadline=deadline)
        spool.seek(0)
        return ExportSpool(spool)
    except BaseException:
        spool.close()
        raise


def _write_export(archive: str, kind: str, provider: str|None, workspace_id: str, *,
                  spool: TextIO, limits: ExportLimits, deadline: float) -> None:
    rows_written = bytes_written = 0

    def write(chunk: str, *, record: bool) -> None:
        nonlocal rows_written,bytes_written
        rows_written += int(record)
        bytes_written += len(chunk.encode("utf-8"))
        check_export_bounds(limits=limits,rows=rows_written,bytes_written=bytes_written,deadline=deadline)
        spool.write(chunk)

    with connection(archive) as conn,transaction(conn,write=False):
        remaining_ms = max(1,int((deadline-time.monotonic())*1000))
        if not api_views.admit_export_snapshot(conn,workspace_id=workspace_id,
                                              slots=limits.workspace_slots,timeout_ms=remaining_ms):
            raise HTTPException(status_code=429,detail="This workspace already has the maximum concurrent export preparations",
                                headers={"Retry-After":"5"})
        api_views.check_export_schema(conn)
        check_export_bounds(limits=limits,rows=0,bytes_written=0,deadline=deadline)
        api_views.set_export_timeout(conn,max(1,int((deadline-time.monotonic())*1000)))
        rows = queries.iter_games(conn,provider=provider) if kind=="games" else (
            queries.iter_users(conn,provider=provider) if kind=="users" else
            api_views.iter_owned_graph(conn,workspace_id=workspace_id,provider=provider)
        )
        with closing(rows):
            if kind!="graph":
                for row in rows:
                    write(json.dumps(dict(row),sort_keys=True,separators=(",",":"))+"\n",record=True)
                return
            buffer = io.StringIO(newline="")
            writer = csv.DictWriter(buffer,fieldnames=(
                "provider","crawl_run_id","from_username","to_username","from_user_id","to_user_id",
                "via_game_id","game_count","depth","edge_kind",
            ))
            writer.writeheader()
            write(buffer.getvalue(),record=False)
            for row in rows:
                buffer.seek(0)
                buffer.truncate(0)
                values = dict(row)
                # Provider-native usernames must not become spreadsheet formulas.
                for key in ("from_username","to_username"):
                    value = values[key]
                    if isinstance(value,str) and value.startswith(("=","+","-","@","\t","\r")):
                        values[key] = "'"+value
                writer.writerow(values)
                write(buffer.getvalue(),record=True)
