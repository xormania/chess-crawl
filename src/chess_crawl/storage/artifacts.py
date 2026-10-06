"""Workspace-owned export manifests, bounded retention, and download leases."""
from __future__ import annotations

import hashlib
import re
import time
import uuid
from typing import TYPE_CHECKING, Any

from chess_crawl.application.errors import Conflict, NotFound
from chess_crawl.jobs.budget import QuotaExceeded
from chess_crawl.storage.archives import PreparedArchiveObject, register_archive_object
from chess_crawl.storage.db import Connection, atomic, operation_lock, require_row
from chess_crawl.storage.object_store import store_for_reference
from chess_crawl.storage.working_sets import canonical
from chess_crawl.storage.workspaces import require_job, validate_workspace

if TYPE_CHECKING:
    from chess_crawl.application.archive_jobs import ArchiveJobSettings

CHUNK_BYTES = 1024 * 1024


def artifact_prefix(workspace_id: str, job_id: int) -> str:
    validate_workspace(workspace_id)
    if type(job_id) is not int or job_id < 1:
        raise ValueError("Artifact job identity must be positive")
    return f"artifacts/{hashlib.sha256(workspace_id.encode()).hexdigest()}/{job_id}/"


@atomic
def record_archive_job(conn: Connection, job_id: int, workspace_id: str, operation: str, request: dict[str, Any]) -> None:
    require_job(conn, job_id, workspace_id)
    conn.execute("INSERT INTO archive_jobs(job_id,workspace_id,operation,request) VALUES(%s,%s,%s,%s::jsonb)",
                 (job_id, workspace_id, operation, canonical(request)))


def get_archive_job(conn: Connection, job_id: int, workspace_id: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM archive_jobs WHERE job_id=%s AND workspace_id=%s", (job_id, workspace_id)).fetchone()
    if row is None:
        raise NotFound("Archive job not found", code="archive_job_not_found")
    return dict(row)


def execution_request(conn: Connection, job_id: int) -> dict[str, Any]:
    """Only trusted processing roles access the durable private execution request."""
    row = conn.execute("SELECT * FROM archive_jobs WHERE job_id=%s", (job_id,)).fetchone()
    if row is None:
        raise ValueError("Internal archive job has no durable request")
    return dict(row)


def validate_artifact_manifest(record: dict[str, Any]) -> dict[str, Any]:
    manifest = record.get("manifest")
    if not isinstance(manifest, dict):
        raise ValueError("Export artifact has no valid manifest")
    unsigned = {key: value for key, value in manifest.items() if key != "artifact_signature"}
    from chess_crawl.storage.working_sets import digest
    if manifest.get("artifact_signature") != digest(unsigned):
        raise ValueError("Export artifact manifest checksum mismatch")
    kind = manifest.get("kind")
    if (manifest.get("contract_version") != 1 or manifest.get("renderer_version") != "archive-export-v1"
            or kind not in {"games", "users", "graph"} or manifest.get("selection_time") != "processing"):
        raise ValueError("Unsupported export artifact contract")
    for name, maximum in (("rows", 10_000_000), ("body_bytes", 4 * 1024**3), ("chunk_count", 4096)):
        value = manifest.get(name)
        if type(value) is not int or not 0 <= value <= maximum:
            raise ValueError("Export artifact bounds are invalid")
    if manifest["body_bytes"] > record["reserved_bytes"]:
        raise ValueError("Export artifact exceeds its retained size")
    if (manifest["chunk_count"] != (manifest["body_bytes"] + CHUNK_BYTES - 1) // CHUNK_BYTES
            or re.fullmatch(r"sha256:[0-9a-f]{64}", str(manifest.get("content_hash"))) is None):
        raise ValueError("Export artifact chunk or checksum metadata is invalid")
    extension = "csv" if kind == "graph" else "jsonl"
    if manifest.get("filename") != f"{kind}.{extension}" or manifest.get("media_type") != (
        "text/csv" if kind == "graph" else "application/x-ndjson"
    ):
        raise ValueError("Export artifact download metadata is invalid")
    return manifest


@atomic
def complete_working_set_job(conn: Connection, job_id: int, workspace_id: str, working_set_id: int) -> None:
    get_archive_job(conn, job_id, workspace_id)
    conn.execute("UPDATE archive_jobs SET working_set_id=%s WHERE job_id=%s", (working_set_id, job_id))


@atomic
def reserve_artifact(conn: Connection, job_id: int, workspace_id: str, settings: ArchiveJobSettings) -> None:
    operation_lock(conn, "artifact-workspace", workspace_id)
    usage = require_row(conn.execute(
        "SELECT COUNT(*),COALESCE(SUM(reserved_bytes),0) FROM archive_artifacts WHERE workspace_id=%s AND state<>'expired'",
        (workspace_id,),
    ))
    for dimension, used, requested, ceiling in (
        ("artifacts", int(usage[0]), 1, settings.artifact_max_count),
        ("artifact_bytes", int(usage[1]), settings.async_export_max_bytes, settings.artifact_max_bytes),
    ):
        if used + requested > ceiling:
            raise QuotaExceeded(dimension, remaining=max(0, ceiling - used))
    now = int(time.time())
    conn.execute(
        "INSERT INTO archive_artifacts(job_id,workspace_id,reserved_bytes,created_at,expires_at) VALUES(%s,%s,%s,%s,%s)",
        (job_id, workspace_id, settings.async_export_max_bytes, now, now + settings.artifact_ttl_seconds),
    )


def artifact_record(conn: Connection, job_id: int, workspace_id: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM archive_artifacts WHERE job_id=%s AND workspace_id=%s", (job_id, workspace_id)).fetchone()
    if row is None:
        raise NotFound("Export artifact not found", code="artifact_not_found")
    return dict(row)


def artifact_descriptor(conn: Connection, job_id: int, workspace_id: str, *, now: float | None = None) -> dict[str, Any]:
    record = artifact_record(conn, job_id, workspace_id)
    expired = record["state"] == "expired" or record["expires_at"] <= (time.time() if now is None else now)
    return {"state": "expired" if expired else record["state"], "expires_at": record["expires_at"],
            "manifest": record["manifest"] if record["state"] == "ready" else None,
            "download_url": f"/v1/archive-jobs/{job_id}/download" if record["state"] == "ready" and not expired else None}


@atomic
def start_artifact(conn: Connection, job_id: int, workspace_id: str) -> None:
    record = artifact_record(conn, job_id, workspace_id)
    if record["state"] in {"pruning", "expired"} or record["expires_at"] <= time.time():
        raise Conflict("Export artifact expired", code="artifact_expired")
    if record["state"] == "ready":
        raise Conflict("Export artifact is already complete", code="artifact_complete")
    conn.execute("UPDATE archive_artifacts SET state='building' WHERE job_id=%s", (job_id,))


@atomic
def add_artifact_chunk(conn: Connection, job_id: int, workspace_id: str, ordinal: int, prepared: PreparedArchiveObject) -> None:
    record = artifact_record(conn, job_id, workspace_id)
    if record["state"] != "building" or record["expires_at"] <= time.time():
        raise Conflict("Export artifact no longer accepts publication", code="artifact_unavailable")
    if not prepared.object_key.startswith(artifact_prefix(workspace_id, job_id)) or prepared.body_bytes > CHUNK_BYTES:
        raise ValueError("Artifact publication must use its private bounded object namespace")
    total = int(require_row(conn.execute(
        "SELECT COALESCE(SUM(o.body_bytes),0) FROM artifact_chunks c JOIN archive_objects o ON o.id=c.archive_object_id WHERE c.job_id=%s",
        (job_id,),
    ))[0])
    if total + prepared.body_bytes > record["reserved_bytes"]:
        raise QuotaExceeded("artifact_bytes", remaining=max(0, record["reserved_bytes"] - total))
    object_id = register_archive_object(conn, prepared)
    conn.execute("INSERT INTO artifact_chunks(job_id,ordinal,archive_object_id) VALUES(%s,%s,%s)", (job_id, ordinal, object_id))


@atomic
def finish_artifact(conn: Connection, job_id: int, workspace_id: str, manifest: dict[str, Any]) -> None:
    record = artifact_record(conn, job_id, workspace_id)
    if record["state"] != "building" or record["expires_at"] <= time.time():
        raise Conflict("Export artifact is not being built or has expired", code="artifact_unavailable")
    totals = require_row(conn.execute(
        "SELECT COUNT(*),COALESCE(SUM(o.body_bytes),0) FROM artifact_chunks c JOIN archive_objects o ON o.id=c.archive_object_id WHERE c.job_id=%s",
        (job_id,),
    ))
    size = int(totals[1])
    if (int(totals[0]), size) != (manifest["chunk_count"], manifest["body_bytes"]):
        raise ValueError("Export manifest does not match its committed object parts")
    operation_lock(conn, "artifact-workspace", workspace_id)
    conn.execute("UPDATE archive_artifacts SET state='ready',manifest=%s::jsonb,reserved_bytes=%s WHERE job_id=%s",
                 (canonical(manifest), max(1, int(size)), job_id))


def chunk_page(conn: Connection, job_id: int, *, after: int = 0, limit: int = 64) -> list[dict[str, Any]]:
    if not 1 <= limit <= 256:
        raise ValueError("Artifact chunk page must contain 1 through 256 references")
    return [dict(row) for row in conn.execute(
        "SELECT c.ordinal,o.* FROM artifact_chunks c JOIN archive_objects o ON o.id=c.archive_object_id WHERE c.job_id=%s AND c.ordinal>%s ORDER BY c.ordinal LIMIT %s",
        (job_id, after, limit),
    )]


@atomic
def acquire_download(conn: Connection, job_id: int, workspace_id: str, *, lifetime: float, slots: int) -> tuple[str, dict[str, Any]]:
    operation_lock(conn, "artifact-workspace", workspace_id)
    record = artifact_record(conn, job_id, workspace_id)
    now = time.time()
    if record["state"] != "ready" or record["expires_at"] <= now:
        raise NotFound("Export artifact unavailable or expired", code="artifact_unavailable")
    conn.execute("DELETE FROM artifact_downloads WHERE lease_id IN (SELECT lease_id FROM artifact_downloads WHERE workspace_id=%s AND expires_at<=%s ORDER BY expires_at LIMIT 256)", (workspace_id, now))
    active = int(require_row(conn.execute("SELECT COUNT(*) FROM artifact_downloads WHERE workspace_id=%s AND expires_at>%s", (workspace_id, now)))[0])
    if active >= slots:
        raise QuotaExceeded("artifact_downloads", remaining=0)
    lease_id = uuid.uuid4().hex
    conn.execute("INSERT INTO artifact_downloads(lease_id,job_id,workspace_id,expires_at) VALUES(%s,%s,%s,%s)",
                 (lease_id, job_id, workspace_id, min(now + lifetime, record["expires_at"])))
    return lease_id, record


@atomic
def release_download(conn: Connection, lease_id: str, workspace_id: str) -> None:
    conn.execute("DELETE FROM artifact_downloads WHERE lease_id=%s AND workspace_id=%s", (lease_id, workspace_id))


def delete_artifact_parts(conn: Connection, job_id: int, workspace_id: str, *, limit: int = 256) -> int:
    """Idempotent bounded deletion; references remain until each delete succeeds."""
    if conn.in_transaction:
        raise ValueError("Artifact object deletion must happen outside a transaction")
    rows = chunk_page(conn, job_id, limit=limit)
    for row in rows:
        _authorize_chunk_deletion(conn, job_id, workspace_id)
        if not row["object_key"].startswith(artifact_prefix(workspace_id, job_id)):
            raise ValueError("Refusing to delete a source-evidence object")
        referenced = require_row(conn.execute(
            "SELECT EXISTS(SELECT 1 FROM raw_payloads WHERE archive_object_id=%s) OR EXISTS(SELECT 1 FROM archive_imports WHERE archive_object_id=%s)", (row["id"], row["id"]),
        ))[0]
        if referenced:
            raise ValueError("Refusing to delete an artifact referenced by source evidence")
        store_for_reference(row["backend"], row["location"]).delete(row["object_key"])
        _remove_chunk_reference(conn, job_id, int(row["ordinal"]))
    return len(rows)


@atomic
def _authorize_chunk_deletion(conn: Connection, job_id: int, workspace_id: str) -> None:
    # Atomic's worker fence validates ownership before object I/O; per-attempt
    # keys additionally isolate a DELETE already in flight during lease loss.
    get_archive_job(conn, job_id, workspace_id)
    if artifact_record(conn, job_id, workspace_id)["state"] not in {"building", "pruning"}:
        raise Conflict("Finalized export parts cannot be deleted", code="artifact_complete")


@atomic
def _remove_chunk_reference(conn: Connection, job_id: int, ordinal: int) -> None:
    # The immutable object registry keeps provenance; object retention is scoped
    # to these private artifact references and never used by source readers.
    conn.execute("DELETE FROM artifact_chunks WHERE job_id=%s AND ordinal=%s", (job_id, ordinal))


@atomic
def mark_pruning(conn: Connection, workspace_id: str, *, before: int, limit: int) -> list[int]:
    validate_workspace(workspace_id)
    if type(before) is not int or not 0 <= before <= 253402300799 or not 1 <= limit <= 100:
        raise ValueError("Artifact retention needs a valid cutoff and 1 through 100 artifacts")
    operation_lock(conn, "artifact-workspace", workspace_id)
    rows = conn.execute(
        "SELECT a.job_id FROM archive_artifacts a JOIN discovery_jobs j ON j.id=a.job_id WHERE a.workspace_id=%s AND a.state<>'expired' AND a.expires_at<=%s AND j.state<>'in_progress' AND NOT EXISTS(SELECT 1 FROM artifact_downloads d WHERE d.job_id=a.job_id AND d.expires_at>%s) ORDER BY a.expires_at,a.job_id LIMIT %s FOR UPDATE OF a SKIP LOCKED",
        (workspace_id, min(before, int(time.time())), time.time(), limit),
    ).fetchall()
    ids = [int(row[0]) for row in rows]
    conn.execute("UPDATE archive_artifacts SET state='pruning' WHERE job_id=ANY(%s)", (ids,))
    return ids


@atomic
def expire_if_empty(conn: Connection, job_id: int, workspace_id: str) -> int:
    record = artifact_record(conn, job_id, workspace_id)
    if record["state"] != "pruning" or conn.execute("SELECT 1 FROM artifact_chunks WHERE job_id=%s LIMIT 1", (job_id,)).fetchone():
        return 0
    operation_lock(conn, "artifact-workspace", workspace_id)
    conn.execute("UPDATE archive_artifacts SET state='expired',manifest=NULL,reserved_bytes=1 WHERE job_id=%s", (job_id,))
    conn.execute("DELETE FROM artifact_downloads WHERE job_id=%s", (job_id,))
    return int(record["reserved_bytes"])


def prune_artifacts(conn: Connection, workspace_id: str, *, before: int, limit: int = 1) -> dict[str, int]:
    released = completed = chunks = 0
    for job_id in mark_pruning(conn, workspace_id, before=before, limit=limit):
        chunks += delete_artifact_parts(conn, job_id, workspace_id)
        freed = expire_if_empty(conn, job_id, workspace_id)
        released += freed
        completed += int(freed > 0)
    return {"expired": completed, "deleted_chunks": chunks, "released_bytes": released}
