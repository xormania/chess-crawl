"""Workspace-owned admission, status and downloads for durable archive jobs."""
from __future__ import annotations

import time
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Header, Path, Request, Response
from pydantic import BaseModel, ConfigDict

from chess_crawl import application
from chess_crawl.api.archive import WorkingSetBody
from chess_crawl.api.artifact_downloads import ArtifactDownload, release_download, reserve_download_memory
from chess_crawl.api.compat import ArchiveExportResponse
from chess_crawl.application.archive_jobs import ArchiveJobSettings, submit_archive_job
from chess_crawl.application.export_limits import ExportLimits
from chess_crawl.application.validation import validate_idempotency_key, validate_provider
from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.storage import artifacts, working_sets
from chess_crawl.storage.db import connection, transaction


class ExportBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    kind: Literal["games", "users", "graph"]
    provider: str | None = None


def register_async_routes(router: APIRouter, archive: str, budget_policy: BudgetPolicy) -> None:
    def submit(operation: str, normalized: dict[str, Any], request: Request, response: Response,
               idempotency_key: str) -> dict[str, Any]:
        settings = ArchiveJobSettings.from_env()
        if not settings.archive_jobs_enabled:
            raise application.Conflict("Durable archive job admission is disabled", code="archive_jobs_disabled")
        key = validate_idempotency_key(idempotency_key)
        with connection(archive, mode="rw") as conn:
            result = submit_archive_job(conn, operation=operation, request=normalized,
                                        workspace_id=request.state.workspace_id, idempotency_key=key,
                                        settings=settings, budget_policy=budget_policy)
        response.headers["Location"] = result["status_url"]
        return result

    @router.post("/archive-jobs/working-sets", status_code=202, tags=["archive jobs"])
    def build_set(body: WorkingSetBody, request: Request, response: Response,
                  idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128)]) -> dict[str, Any]:
        normalized = {"name": body.name, "filters": body.filters.model_dump(exclude_none=True), "settings": body.settings}
        return submit("build_working_set", normalized, request, response, idempotency_key)

    @router.post("/archive-jobs/exports", status_code=202, tags=["archive jobs"])
    def prepare_export(body: ExportBody, request: Request, response: Response,
                       idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128)]) -> dict[str, Any]:
        normalized = body.model_dump(exclude_none=True)
        if body.provider is not None:
            normalized["provider"] = validate_provider(body.provider)
        return submit("prepare_export", normalized, request, response, idempotency_key)

    @router.get("/archive-jobs/{job_id}", tags=["archive jobs"])
    def status(job_id: Annotated[int, Path(ge=1, le=2**63 - 1)], request: Request) -> dict[str, Any]:
        owner = request.state.workspace_id
        with connection(archive) as conn, transaction(conn, write=False):
            record = artifacts.get_archive_job(conn, job_id, owner)
            result = {"contract_version": record["contract_version"], "operation": record["operation"],
                      "job": application.get_job(conn, job_id, workspace_id=owner)}
            if record["operation"] == "build_working_set":
                result["working_set"] = (working_sets.get_working_set(conn, record["working_set_id"], owner)
                                         if record["working_set_id"] is not None else None)
            else:
                result["artifact"] = artifacts.artifact_descriptor(conn, job_id, owner)
        return result

    @router.get("/archive-jobs/{job_id}/download", tags=["archive jobs"])
    def download(job_id: Annotated[int, Path(ge=1, le=2**63 - 1)], request: Request) -> ArchiveExportResponse:
        owner = request.state.workspace_id
        limits = ExportLimits.from_env()
        # Authenticate ownership before either storage admission or object access.
        with connection(archive) as conn:
            record = artifacts.get_archive_job(conn, job_id, owner)
            if record["operation"] != "prepare_export":
                raise application.NotFound("Export artifact not found", code="artifact_not_found")
        release_memory = reserve_download_memory(limits, owner)
        chunks = None
        lease_id = None
        started = time.time()
        try:
            with connection(archive, mode="rw") as conn:
                lease_id, record = artifacts.acquire_download(conn, job_id, owner,
                                                              lifetime=limits.download_seconds, slots=limits.workspace_slots)
            deadline = min(started + limits.download_seconds, record["expires_at"])
            chunks = ArtifactDownload(archive, job_id, owner, record, deadline=deadline,
                                      on_close=lambda: release_download(archive, str(lease_id), owner, release_memory))
            chunks.prime()
            manifest = chunks.manifest
            return ArchiveExportResponse(chunks, media_type=manifest["media_type"],
                                         download_seconds=max(0, deadline - time.time()), headers={
                                             "Content-Disposition": f'attachment; filename="{manifest["filename"]}"',
                                             "Content-Length": str(manifest["body_bytes"]),
                                             "X-Content-Hash": manifest["content_hash"],
                                             "Cache-Control": "private, no-store",
                                         })
        except Exception:
            if chunks is not None:
                chunks.close()
            elif lease_id is not None:
                release_download(archive, lease_id, owner, release_memory)
            else:
                release_memory()
            raise
