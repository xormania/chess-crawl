"""Product API coverage for former archive-query and export commands."""
from __future__ import annotations

import csv
import io
import json
from collections.abc import Generator
from contextlib import closing
from dataclasses import asdict
from typing import Annotated, Any, Literal

import anyio
from fastapi import APIRouter, Header, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict
from starlette.concurrency import run_in_threadpool
from starlette.types import Receive, Scope, Send

from chess_crawl import application
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

    def __init__(self,chunks:Generator[str,None,None],*,media_type:str,headers:dict[str,str]) -> None:
        self.chunks = chunks
        super().__init__(chunks,media_type=media_type,headers=headers)

    async def __call__(self,scope:Scope,receive:Receive,send:Send) -> None:
        try:
            await super().__call__(scope,receive,send)
        finally:
            # Wait for any in-flight next() before closing, including disconnect
            # cancellation. Never leave an archive snapshot waiting on GC.
            with anyio.CancelScope(shield=True):
                await run_in_threadpool(self.chunks.close)


def register_compat_routes(router: APIRouter, archive: str, limits: application.Limits) -> None:
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
                                           idempotency_key=idempotency_key,workspace_id=request.state.workspace_id)
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
                                            workspace_id=request.state.workspace_id)
        response.headers["Location"] = f"/v1/runs/{result['run_id']}"
        return result

    def export(kind: Literal["games","users","graph"], request: Request, provider: str|None) -> StreamingResponse:
        provider = None if provider is None else validate_provider(provider)
        with connection(archive) as conn,transaction(conn,write=False):
            api_views.check_export_schema(conn)
        extension = "csv" if kind=="graph" else "jsonl"
        return ArchiveExportResponse(
            _export_chunks(archive,kind,provider,request.state.workspace_id),
            media_type="text/csv" if kind=="graph" else "application/x-ndjson",
            headers={"Content-Disposition":f'attachment; filename="{kind}.{extension}"'},
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
    # The generator owns its connection/transaction until completion or close.
    # Cursor streaming bounds memory and holds a single repeatable-read snapshot.
    with connection(archive) as conn,transaction(conn,write=False):
        rows = queries.iter_games(conn,provider=provider) if kind=="games" else (
            queries.iter_users(conn,provider=provider) if kind=="users" else
            api_views.iter_owned_graph(conn,workspace_id=workspace_id,provider=provider)
        )
        with closing(rows):
            if kind!="graph":
                for row in rows:
                    yield json.dumps(dict(row),sort_keys=True,separators=(",",":"))+"\n"
                return
            buffer = io.StringIO(newline="")
            writer = csv.DictWriter(buffer,fieldnames=(
                "provider","crawl_run_id","from_username","to_username","from_user_id","to_user_id",
                "via_game_id","game_count","depth","edge_kind",
            ))
            writer.writeheader()
            yield buffer.getvalue()
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
                yield buffer.getvalue()
