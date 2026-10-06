"""Server-owned bounds and admission for queued archive operations."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from chess_crawl.application.errors import ValidationError
from chess_crawl.application.services import _submit
from chess_crawl.application.validation import validate_idempotency_key, validate_provider
from chess_crawl.jobs import state
from chess_crawl.jobs.models import JobKind
from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.settings import dataclass_settings, setting
from chess_crawl.storage.db import Connection
from chess_crawl.storage.object_store import ObjectStore, store_for_reference


@dataclass(frozen=True)
class ArchiveJobSettings:
    archive_jobs_enabled: bool = False
    artifact_backend: str = "local"
    artifact_directory: str = "/var/lib/chess-crawl/artifacts"
    artifact_s3_bucket: str = ""
    async_max_working_set_members: int = 1_000_000
    async_export_max_rows: int = 1_000_000
    async_export_max_bytes: int = 256 * 1024 * 1024
    async_export_prepare_seconds: int = 600
    artifact_max_count: int = 32
    artifact_max_bytes: int = 1024 * 1024 * 1024
    artifact_ttl_seconds: int = 86400

    def __post_init__(self) -> None:
        if self.artifact_backend not in {"local", "s3"}:
            raise ValueError("Artifact backend must be local or s3")
        if self.artifact_backend == "local" and not Path(self.artifact_directory).is_absolute():
            raise ValueError("Artifact directory must be an absolute durable shared path")
        if self.artifact_backend == "s3":
            bucket = self.artifact_s3_bucket or setting("CHESS_CRAWL_ARCHIVE_S3_BUCKET", "")
            object.__setattr__(self, "artifact_s3_bucket", bucket)
        if self.artifact_backend == "s3" and (self.artifact_s3_bucket or self.archive_jobs_enabled):
            # Reuse the adapter's bucket validation without creating an SDK client.
            from chess_crawl.storage.object_store import ArchiveSettings
            ArchiveSettings(backend="s3", s3_bucket=self.artifact_s3_bucket)
        for field, maximum in (
            ("async_max_working_set_members", 10_000_000), ("async_export_max_rows", 10_000_000),
            ("async_export_max_bytes", 4 * 1024**3), ("async_export_prepare_seconds", 3600),
            ("artifact_max_count", 10000), ("artifact_max_bytes", 1024**5),
            ("artifact_ttl_seconds", 365 * 86400),
        ):
            value = getattr(self, field)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"{field} must be an integer between 1 and {maximum}")
        if self.async_export_max_bytes > self.artifact_max_bytes:
            raise ValueError("Async export size must fit within the workspace artifact byte allowance")

    @classmethod
    def from_env(cls) -> ArchiveJobSettings:
        return dataclass_settings(cls)

    def store(self) -> ObjectStore:
        return store_for_reference(self.artifact_backend,
                                   self.artifact_directory if self.artifact_backend == "local" else self.artifact_s3_bucket)


def submit_archive_job(
    conn: Connection, *, operation: str, request: dict[str, Any], workspace_id: str,
    idempotency_key: str, settings: ArchiveJobSettings, budget_policy: BudgetPolicy,
) -> dict[str, Any]:
    """Reuse durable submission identity, fair backlog admission, and run budgets."""
    from chess_crawl.storage.artifacts import reserve_artifact, record_archive_job
    if operation not in {"build_working_set", "prepare_export"}:
        raise ValidationError("Unknown archive operation", code="invalid_archive_operation")
    # Canonical requests are small; no archive selection happens at admission.
    if len(json.dumps(request, allow_nan=False).encode()) > 131072:
        raise ValidationError("Archive request exceeds 128 KiB", code="invalid_archive_request")
    if request.get("provider") is not None:
        validate_provider(request["provider"])
    key = validate_idempotency_key(idempotency_key)

    def create() -> tuple[int, list[int]]:
        run_id = state.create_crawl_run(conn, provider=None, seed_spec=operation,
                                        params={"strategy": operation, "contract_version": 1})
        job_id = state.enqueue_job(conn, provider=None, kind=cast(JobKind, operation), target=operation,
                                    params={"contract_version": 1}, crawl_run_id=run_id).job_id
        record_archive_job(conn, job_id, workspace_id, operation, request)
        if operation == "prepare_export":
            reserve_artifact(conn, job_id, workspace_id, settings)
        return run_id, [job_id]

    result = _submit(conn, key=key, operation=operation, request=request, create=create,
                     workspace_id=workspace_id, budget_policy=budget_policy)
    return {"contract_version": 1, "operation": operation, **result,
            "status_url": f"/v1/archive-jobs/{result['job_ids'][0]}"}
