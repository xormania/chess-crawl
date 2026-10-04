"""Archive and reproducible-analysis routes; reads never contact providers."""
from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Header, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, model_validator

from chess_crawl import application
from chess_crawl.application.validation import validate_page, validate_provider, validate_username
from chess_crawl.storage import working_sets
from chess_crawl.storage.db import connection, transaction


class SelectionFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    provider: str | None = None
    username: str | None = None
    since: int | None = Field(default=None, ge=0, le=253402300799)
    until: int | None = Field(default=None, ge=1, le=253402300800)
    time_class: str | None = Field(default=None, min_length=1, max_length=100)
    variant: str | None = Field(default=None, min_length=1, max_length=100)
    rated: bool | None = None

    @model_validator(mode="after")
    def normalize(self) -> SelectionFilters:
        if self.provider is not None:
            self.provider = validate_provider(self.provider)
        if self.username is not None:
            if self.provider is None:
                raise ValueError("A player selection requires a provider")
            self.username = validate_username(self.username)
        if self.since is not None and self.until is not None and self.since >= self.until:
            raise ValueError("since must be earlier than until")
        return self


class WorkingSetBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    name: str = Field(min_length=1, max_length=200)
    filters: SelectionFilters
    settings: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def bounded_settings(self) -> WorkingSetBody:
        if len(working_sets.canonical(self.settings).encode()) > 65536:
            raise ValueError("Working-set settings exceed 64 KiB")
        return self


class CalculationBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    implementation: str = Field(min_length=1, max_length=200)
    implementation_version: str = Field(min_length=1, max_length=200)
    settings: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def bounded_json(self) -> CalculationBody:
        if len(working_sets.canonical(self.settings).encode()) > 65536:
            raise ValueError("Calculation settings exceed 64 KiB")
        return self



class ResultBody(CalculationBody):
    output: dict[str, Any]

    @model_validator(mode="after")
    def bounded_output(self) -> ResultBody:
        if len(working_sets.canonical(self.output).encode()) > 1048576:
            raise ValueError("Inline calculation outputs exceed 1 MiB")
        return self


class UpgradeBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    provider: str
    name: str = Field(min_length=1,max_length=100)
    parser_version: str = Field(default="current",min_length=1,max_length=2048)
    batch_size: int = Field(default=100,ge=1,le=100)


class ResourceBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    provider: str
    username: str
    resource_key: str = Field(min_length=1,max_length=100)
    parameters: dict[str,Any] = Field(default_factory=dict)


def register_archive_routes(router: APIRouter, archive: str, limits: application.Limits) -> None:
    @router.post("/working-sets", status_code=201, tags=["analysis"])
    def create_set(
        body: WorkingSetBody, request: Request, response: Response,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128)],
    ) -> dict[str, Any]:
        key = application.validate_idempotency_key(idempotency_key)
        with connection(archive, mode="rw") as conn:
            result = working_sets.create_working_set(
                conn, workspace_id=request.state.workspace_id, name=body.name,
                filters=body.filters.model_dump(exclude_none=True), settings=body.settings, idempotency_key=key,
                max_members=limits.max_working_set_members,
            )
        response.headers["Location"] = f"/v1/working-sets/{result['id']}"
        return result

    @router.get("/working-sets/{working_set_id}", tags=["analysis"])
    def get_set(working_set_id: int, request: Request) -> dict[str, Any]:
        with connection(archive) as conn, transaction(conn, write=False):
            return working_sets.get_working_set(conn, working_set_id, request.state.workspace_id)

    @router.get("/working-sets/{working_set_id}/members", tags=["analysis"])
    def members(
        working_set_id: int, request: Request,
        after: Annotated[int | None, Query(ge=0)] = None, limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> dict[str, Any]:
        cursor, size = validate_page(after=after, limit=limit, limits=limits)
        with connection(archive) as conn, transaction(conn, write=False):
            return working_sets.member_page(conn, working_set_id, request.state.workspace_id, after=cursor, limit=size)

    @router.post("/working-sets/{working_set_id}/results", status_code=201, tags=["analysis"])
    def put_result(working_set_id: int, request: Request, body: ResultBody, response: Response) -> dict[str, Any]:
        with connection(archive, mode="rw") as conn:
            result = working_sets.save_result(conn, working_set_id, request.state.workspace_id, **body.model_dump())
        response.headers["Location"] = f"/v1/results/{result['id']}"
        return result

    @router.get("/results/{result_id}", tags=["analysis"])
    def get_result(result_id: int, request: Request) -> dict[str, Any]:
        with connection(archive) as conn, transaction(conn, write=False):
            return working_sets.read_result(conn, result_id, request.state.workspace_id)

    @router.get("/workspace", tags=["access"])
    def workspace(request: Request) -> dict[str, str]:
        return {"workspace_id": request.state.workspace_id}

    def get_player(conn: Any, provider: str, username: str, workspace_id: str) -> dict[str, Any]:
        from chess_crawl.storage.player_profiles import player_profile
        profile = player_profile(conn, provider, username, owner_scope=workspace_id)
        if profile is None:
            raise application.NotFound("Provider user not found", code="user_not_found")
        return profile

    @router.get("/users/{provider}/{username}", tags=["profiles"])
    def player(provider: str, username: str, request: Request) -> dict[str, Any]:
        provider, username = validate_provider(provider), validate_username(username)
        with connection(archive) as conn, transaction(conn, write=False):
            return get_player(conn, provider, username, request.state.workspace_id)

    @router.get("/users/{provider}/{username}/history", tags=["profiles"])
    def history(
        provider: str, username: str, request: Request,
        after: Annotated[int | None, Query(ge=0)] = None, limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> dict[str, Any]:
        from chess_crawl.storage.player_profiles import profile_history
        provider, username = validate_provider(provider), validate_username(username)
        cursor, size = validate_page(after=after, limit=limit, limits=limits)
        with connection(archive) as conn, transaction(conn, write=False):
            profile = get_player(conn, provider, username, request.state.workspace_id)
            if size > 999:
                raise application.ValidationError("History page limit must be <=999",code="invalid_limit")
            rows = profile_history(conn, profile["id"], after_id=cursor, limit=size+1)
            return {"items":rows[:size], "next_cursor":rows[size-1]["observation_id"] if len(rows)>size else None}

    @router.get("/users/{provider}/{username}/resources", tags=["profiles"])
    def resources(provider: str, username: str, request: Request) -> list[dict[str, Any]]:
        from chess_crawl.storage.player_profiles import resource_current
        provider, username = validate_provider(provider), validate_username(username)
        with connection(archive) as conn, transaction(conn, write=False):
            profile = get_player(conn, provider, username, request.state.workspace_id)
            return resource_current(conn, profile["id"], owner_scope=request.state.workspace_id)

    @router.get("/users/{provider}/{username}/resources/history", tags=["profiles"])
    def resource_observations(
        provider: str, username: str, request: Request, resource_key: str | None = None,
        after: Annotated[int | None, Query(ge=0)] = None, limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> dict[str, Any]:
        from chess_crawl.storage.player_profiles import resource_history
        provider, username = validate_provider(provider), validate_username(username)
        cursor, size = validate_page(after=after, limit=limit, limits=limits)
        with connection(archive) as conn, transaction(conn, write=False):
            profile = get_player(conn, provider, username, request.state.workspace_id)
            if size > 999:
                raise application.ValidationError("History page limit must be <=999",code="invalid_limit")
            rows = resource_history(conn, profile["id"], resource_key=resource_key, after_id=cursor,
                                    limit=size+1, owner_scope=request.state.workspace_id)
            return {"items":rows[:size], "next_cursor":rows[size-1]["observation_id"] if len(rows)>size else None}

    @router.get("/users/{provider}/{username}/coverage", tags=["profiles"])
    def coverage(provider: str, username: str, request: Request) -> dict[str, Any]:
        from chess_crawl.storage.archive_views import player_coverage
        provider, username = validate_provider(provider), validate_username(username)
        with connection(archive) as conn, transaction(conn, write=False):
            profile = get_player(conn, provider, username, request.state.workspace_id)
            return player_coverage(conn, profile["id"])

    @router.get("/users/{provider}/{username}/games", tags=["profiles"])
    def player_games(
        provider: str, username: str, request: Request,
        after: Annotated[int | None, Query(ge=0)] = None, limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> dict[str, Any]:
        from chess_crawl.storage.archive_views import games_for_player
        provider, username = validate_provider(provider), validate_username(username)
        cursor, size = validate_page(after=after, limit=limit, limits=limits)
        with connection(archive) as conn, transaction(conn, write=False):
            profile = get_player(conn, provider, username, request.state.workspace_id)
            return games_for_player(conn, profile["id"], after=cursor, limit=size)

    @router.get("/games/{game_id}", tags=["evidence"])
    def game(game_id: int, version_id: Annotated[int | None, Query(ge=1)] = None) -> dict[str, Any]:
        from chess_crawl.storage.game_evidence import read_game_version
        from chess_crawl.storage.archive_views import version_id as resolve_version
        with connection(archive) as conn, transaction(conn, write=False):
            selected = resolve_version(conn, game_id, version_id)
            evidence = read_game_version(conn, game_id, version_id=selected)
            if evidence is None:
                raise application.NotFound("Game version not found",code="game_version_not_found")
            return evidence

    @router.get("/games/{game_id}/versions", tags=["evidence"])
    def game_versions(
        game_id: int, after: Annotated[int | None, Query(ge=0)] = None,
        limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> dict[str, Any]:
        from chess_crawl.storage.archive_views import versions
        cursor, size = validate_page(after=after, limit=limit, limits=limits)
        with connection(archive) as conn, transaction(conn, write=False):
            return versions(conn, game_id, after=cursor, limit=size)

    @router.get("/games/{game_id}/moves", tags=["evidence"])
    def moves(
        game_id: int, version_id: Annotated[int | None, Query(ge=1)] = None,
        after: Annotated[int | None, Query(ge=0)] = None, limit: Annotated[int | None, Query(ge=1)] = None,
        mainline: bool | None = None,
    ) -> dict[str, Any]:
        from chess_crawl.storage.archive_views import move_page
        cursor, size = validate_page(after=after, limit=limit, limits=limits)
        with connection(archive) as conn, transaction(conn, write=False):
            return move_page(conn, game_id, requested=version_id, after=cursor, limit=size, mainline=mainline)

    @router.get("/games/{game_id}/pgn", response_class=Response, tags=["exports"])
    def pgn(
        game_id: int, version_id: Annotated[int | None, Query(ge=1)] = None, allow_partial: bool = False,
    ) -> Response:
        from chess_crawl.storage.game_evidence import export_game_version_pgn
        from chess_crawl.storage.archive_views import version_id as resolve_version
        with connection(archive) as conn, transaction(conn, write=False):
            selected = resolve_version(conn, game_id, version_id)
            try:
                text = export_game_version_pgn(conn, game_id, version_id=selected, allow_partial=allow_partial)
            except ValueError as exc:
                raise application.Conflict(str(exc), code="incomplete_game_evidence") from exc
        return Response(text, media_type="application/x-chess-pgn",
                        headers={"Content-Disposition":f'attachment; filename="game-{game_id}-v{selected}.pgn"'})


    @router.get("/resources", tags=["collection"])
    def resource_catalog(provider: str | None = None) -> list[dict[str,Any]]:
        from chess_crawl.providers.resources import list_resources
        return list_resources(None if provider is None else validate_provider(provider))

    @router.post("/resources",status_code=202,tags=["collection"])
    def collect_resource(
        body:ResourceBody,request:Request,response:Response,
        idempotency_key:Annotated[str,Header(alias="Idempotency-Key",min_length=1,max_length=128)],
    ) -> dict[str,Any]:
        from chess_crawl.providers.resources import get_resource
        from chess_crawl.application.services import submit_resource
        from chess_crawl.application.errors import ValidationError
        # Validate the provider contract before any database connection.
        normalized = body.model_dump()
        normalized["provider"] = validate_provider(body.provider)
        normalized["username"] = validate_username(body.username)
        try:
            resource = get_resource(normalized["provider"],body.resource_key)
            normalized["parameters"] = resource.parameters(body.parameters)
        except ValueError as exc:
            raise ValidationError(str(exc),code="invalid_resource") from exc
        key = application.validate_idempotency_key(idempotency_key)
        with connection(archive,mode="rw") as conn:
            result = submit_resource(conn,normalized,idempotency_key=key,workspace_id=request.state.workspace_id)
        response.headers["Location"] = f"/v1/runs/{result['run_id']}"
        return result

    @router.post("/upgrades",status_code=202,tags=["maintenance"])
    def collect_upgrade(
        body:UpgradeBody,request:Request,response:Response,
        idempotency_key:Annotated[str,Header(alias="Idempotency-Key",min_length=1,max_length=128)],
    ) -> dict[str,Any]:
        from chess_crawl.application.services import submit_upgrade
        normalized = {**body.model_dump(),"provider":validate_provider(body.provider)}
        key = application.validate_idempotency_key(idempotency_key)
        with connection(archive,mode="rw") as conn:
            result = submit_upgrade(conn,normalized,idempotency_key=key,workspace_id=request.state.workspace_id)
        response.headers["Location"] = f"/v1/upgrades/{result['job_ids'][0]}"
        return result

    @router.get("/upgrades/{job_id}",tags=["maintenance"])
    def upgrade(job_id:int,request:Request) -> dict[str,Any]:
        from chess_crawl.storage.archive_views import upgrade_progress
        with connection(archive) as conn,transaction(conn,write=False):
            return upgrade_progress(conn,job_id,request.state.workspace_id)


    @router.post("/working-sets/{working_set_id}/results/lookup",tags=["analysis"])
    def result_lookup(working_set_id:int,request:Request,body:CalculationBody) -> dict[str,Any]:
        with connection(archive) as conn,transaction(conn,write=False):
            return working_sets.lookup_result(conn,working_set_id,request.state.workspace_id,**body.model_dump())

    @router.get("/users/{provider}/{username}/rating-history",tags=["profiles"])
    def rating_history(
        provider:str,username:str,request:Request,
        snapshot_id:Annotated[int | None,Query(ge=1)]=None,
        performance:Annotated[str | None,Query(min_length=1,max_length=100)]=None,
        after:Annotated[int | None,Query(ge=0)]=None,limit:Annotated[int | None,Query(ge=1)]=None,
    ) -> dict[str,Any]:
        from chess_crawl.storage.archive_views import rating_history_page
        provider,username = validate_provider(provider),validate_username(username)
        cursor,size=validate_page(after=after,limit=limit,limits=limits)
        with connection(archive) as conn,transaction(conn,write=False):
            profile=get_player(conn,provider,username,request.state.workspace_id)
            return rating_history_page(conn,profile["id"],request.state.workspace_id,
                                       snapshot_id=snapshot_id,performance=performance,after=cursor,limit=size)
