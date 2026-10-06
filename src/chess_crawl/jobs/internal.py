"""Internal processing handlers share the durable worker and admission kernel."""
from __future__ import annotations

import tempfile
import io
import time
from collections.abc import Callable
from contextlib import nullcontext
from typing import Any

from chess_crawl.application.archive_exports import ExportRenderLimits, write_export_snapshot
from chess_crawl.application.archive_jobs import ArchiveJobSettings
from chess_crawl.application.export_limits import ExportLimits
from chess_crawl.application.errors import ApplicationError, ValidationError
from chess_crawl.jobs.budget import BudgetExceeded, QuotaExceeded
from chess_crawl.jobs.models import DiscoveryJob
from chess_crawl.storage import artifacts, work_budgets, working_sets
from chess_crawl.storage.archives import prepare_archive_object
from chess_crawl.storage.db import Connection, DatabaseError, ExecutorLeaseLost, transaction


class ArchiveJobStopped(RuntimeError):
    pass


def _request(conn: Connection, job: DiscoveryJob) -> dict[str, Any]:
    if job.id is None or job.provider is not None:
        raise ValueError("Internal archive processing requires a providerless durable job")
    from chess_crawl.storage.artifacts import execution_request
    record = execution_request(conn, job.id)
    if record["operation"] != job.kind or record["contract_version"] != 1:
        raise ValueError("Archive job contract does not match its durable request")
    return record


def _check_stop(stopped: Callable[[], bool]) -> None:
    if stopped():
        raise ArchiveJobStopped("Archive processing stopped; durable request retained")


def _build_working_set(conn: Connection, job: DiscoveryJob, stopped: Callable[[], bool], interrupted: Callable[[], bool]) -> dict[str, Any]:
    record = _request(conn, job)
    if record["working_set_id"] is not None:
        return {"state": "done", "reason": "Immutable working set already finalized"}
    settings = ArchiveJobSettings.from_env()
    _check_stop(stopped)
    request = record["request"]
    allowance = work_budgets.normalization_allowance(conn, conn._work_budget_id) if conn._work_budget_id is not None else settings.async_max_working_set_members
    member_limit = max(1, min(settings.async_max_working_set_members, allowance))
    with transaction(conn):
        from chess_crawl.storage.api_views import set_export_timeout
        set_export_timeout(conn, settings.async_export_prepare_seconds * 1000)
        try:
            selection = working_sets.create_working_set(
                conn, workspace_id=record["workspace_id"], name=request["name"], filters=request["filters"],
                settings=request["settings"], idempotency_key=str(job.id), submission_namespace="archive-job",
                max_members=member_limit,
            )
        except ValidationError as exc:
            if exc.code == "working_set_too_large" and allowance < settings.async_max_working_set_members:
                raise BudgetExceeded("normalization_units", remaining=allowance, budget_id=conn._work_budget_id) from exc
            raise
        _check_stop(stopped)
        if selection["member_count"] and conn._work_budget_id is not None:
            work_budgets.reserve_normalization(conn, conn._work_budget_id, units=int(selection["member_count"]))
        artifacts.complete_working_set_job(conn, record["job_id"], record["workspace_id"], selection["id"])
    return {"state": "done", "reason": "Immutable working set finalized"}


def _prepare_export(conn: Connection, job: DiscoveryJob, stopped: Callable[[], bool], interrupted: Callable[[], bool]) -> dict[str, Any]:
    record = _request(conn, job)
    job_id, owner = record["job_id"], record["workspace_id"]
    artifact = artifacts.artifact_record(conn, job_id, owner)
    if artifact["state"] == "ready":
        return {"state": "done", "reason": "Immutable export already finalized"}
    _check_stop(stopped)
    artifacts.start_artifact(conn, job_id, owner)
    # A crash can leave committed private parts. Delete only those tracked parts
    # before taking another snapshot; source evidence is never in this namespace.
    while artifacts.delete_artifact_parts(conn, job_id, owner):
        _check_stop(stopped)
    settings = ArchiveJobSettings.from_env()
    request = record["request"]
    limits = ExportRenderLimits(settings.async_export_max_rows, settings.async_export_max_bytes,
                                ExportLimits.from_env().workspace_slots)
    # The worker handles one export at a time; scratch is bounded by the admitted
    # maximum. The same renderer and ownership filters serve immediate exports.
    rows_pending = 0
    allowance = work_budgets.normalization_allowance(conn, conn._work_budget_id) if conn._work_budget_id is not None else limits.max_rows

    def rows(amount: int) -> None:
        nonlocal rows_pending
        _check_stop(interrupted)
        if rows_pending + amount > allowance:
            raise BudgetExceeded("normalization_units", remaining=0, budget_id=conn._work_budget_id)
        rows_pending += amount

    with tempfile.TemporaryFile(mode="w+b") as binary, io.TextIOWrapper(binary, encoding="utf-8", newline="") as spool:
        try:
            snapshot = write_export_snapshot(
                conn.info.dsn, request["kind"], request.get("provider"), owner,
                spool=spool, limits=limits, deadline=time.monotonic() + settings.async_export_prepare_seconds,
                connect=lambda _: nullcontext(conn), on_rows=rows,
            )
        except (DatabaseError, ExecutorLeaseLost):
            # Failed/active database state must not be hidden by a budget write.
            raise
        except BaseException:
            if rows_pending and conn._work_budget_id is not None:
                work_budgets.reserve_normalization(conn, conn._work_budget_id, units=rows_pending)
            raise
        else:
            # The owned session remains occupied throughout the snapshot; reserve
            # completed work after its readonly transaction ends, before any upload.
            if rows_pending and conn._work_budget_id is not None:
                work_budgets.reserve_normalization(conn, conn._work_budget_id, units=rows_pending)
        spool.flush()
        binary.seek(0)
        ordinal = 0
        store = settings.store()
        while body := binary.read(artifacts.CHUNK_BYTES):
            _check_stop(stopped)
            ordinal += 1
            prepared = prepare_archive_object(body, store=store, key_prefix=artifacts.artifact_prefix(owner, job_id) + f"attempt-{job.ownership_generation}/")
            artifacts.add_artifact_chunk(conn, job_id, owner, ordinal, prepared)
        _check_stop(stopped)
        extension = "csv" if request["kind"] == "graph" else "jsonl"
        manifest = {
            "contract_version": 1, "renderer_version": "archive-export-v1", "kind": request["kind"],
            "provider": request.get("provider"), "rows": snapshot.rows, "body_bytes": snapshot.body_bytes,
            "content_hash": snapshot.content_hash, "snapshot_started_at": snapshot.snapshot_started_at,
            "selection_time": "processing", "chunk_count": ordinal,
            "filename": f"{request['kind']}.{extension}",
            "media_type": "text/csv" if request["kind"] == "graph" else "application/x-ndjson",
        }
        manifest["artifact_signature"] = working_sets.digest(manifest)
        artifacts.finish_artifact(conn, job_id, owner, manifest)
    return {"state": "done", "reason": "Immutable export finalized"}


def _execute(handler: Callable[..., dict[str, Any]], conn: Connection, job: DiscoveryJob,
             *, stop_requested: Callable[[], bool],
             interruption_requested: Callable[[], bool] | None = None) -> dict[str, Any]:
    try:
        return handler(conn, job, stop_requested, interruption_requested or stop_requested)
    except ArchiveJobStopped as exc:
        return {"state": "pending", "reason": str(exc)}
    except QuotaExceeded as exc:
        if exc.dimension == "export_preparations":
            return {"state": "error", "reason": "Export preparation capacity is temporarily occupied", "transient": True}
        raise
    except (DatabaseError, ExecutorLeaseLost, BudgetExceeded, ApplicationError):
        raise
    except Exception as exc:
        response = getattr(exc, "response", {})
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) if isinstance(response, dict) else 0
        transient = isinstance(exc, OSError) or status == 429 or status >= 500 or type(exc).__name__ in {
            "ReadTimeoutError", "ConnectTimeoutError", "EndpointConnectionError", "ConnectionClosedError",
        }
        return {"state": "error", "reason": f"archive_processing_error: {type(exc).__name__}", "transient": transient}


_HANDLERS = {"build_working_set": _build_working_set, "prepare_export": _prepare_export}


def handler_for(kind: str) -> Callable[..., dict[str, Any]] | None:
    handler = _HANDLERS.get(kind)
    if handler is None:
        return None
    return lambda conn, job, **kwargs: _execute(handler, conn, job, **kwargs)
