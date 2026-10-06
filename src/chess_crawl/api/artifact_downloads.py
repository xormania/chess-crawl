"""Bounded artifact downloads release database sessions before reading objects."""
from __future__ import annotations

import hashlib
import logging
import re
import time
from collections.abc import Callable
from dataclasses import replace
from threading import RLock, Timer
from typing import Any

from fastapi import HTTPException

from chess_crawl.api.exports import ExportLimits, export_capacity
from chess_crawl.storage import artifacts
from chess_crawl.storage.archives import read_archive_reference
from chess_crawl.storage.db import DatabaseError, connection
from chess_crawl.storage.object_store import object_key


_logger = logging.getLogger(__name__)
# Allow compressed and decoded buffers plus overlap with the last sent chunk.
DOWNLOAD_MEMORY_BYTES = 4 * artifacts.CHUNK_BYTES


def reserve_download_memory(limits: ExportLimits, workspace_id: str) -> Callable[[], None]:
    if (limits.workspace_outstanding_bytes < DOWNLOAD_MEMORY_BYTES
            or limits.outstanding_bytes < limits.workspace_outstanding_bytes + DOWNLOAD_MEMORY_BYTES):
        raise HTTPException(status_code=429, detail="Export memory capacity cannot fit an artifact download",
                            headers={"Retry-After": "5"})
    return export_capacity.reserve(replace(limits, max_bytes=DOWNLOAD_MEMORY_BYTES), workspace_id=workspace_id)


class ArtifactDownload:
    """One verified chunk at a time, with a fixed download lease and lifetime."""

    def __init__(self, archive: str, job_id: int, workspace_id: str, record: dict[str, Any], *,
                 deadline: float, on_close: Callable[[], None]) -> None:
        self.manifest = artifacts.validate_artifact_manifest(record)
        self.archive, self.job_id, self.workspace_id = archive, job_id, workspace_id
        self.deadline = deadline
        self._lock = RLock()
        self._closed = self._expired = self._complete = False
        self._ordinal = self._body_bytes = 0
        self._hash = hashlib.sha256()
        self._first: bytes | None = None
        self._attempt: str | None = None
        self._on_close: Callable[[], None] | None = on_close
        self._timer = Timer(max(0, deadline - time.time()), self._expire)
        self._timer.daemon = True
        self._timer.start()

    def __iter__(self) -> ArtifactDownload:
        return self

    def prime(self) -> None:
        """Validate the first object before the response sends success headers."""
        with self._lock:
            self._check_lifetime()
            try:
                self._first = self._read_next()
            except Exception:
                self.close()
                raise

    def __next__(self) -> bytes:
        with self._lock:
            self._check_lifetime()
            if self._closed:
                raise StopIteration
            if self._first is not None:
                first, self._first = self._first, None
                return first
            try:
                body = self._read_next()
                if body is None:
                    self.close()
                    raise StopIteration
                return body
            except Exception:
                self.close()
                raise

    def _page(self) -> list[dict[str, Any]]:
        with connection(self.archive) as conn:
            return artifacts.chunk_page(conn, self.job_id, after=self._ordinal, limit=1)

    def _read_next(self) -> bytes | None:
        if self._complete:
            return None
        rows = self._page()
        if not rows:
            self._finish()
            return None
        row = rows[0]
        expected_bytes = min(artifacts.CHUNK_BYTES, self.manifest["body_bytes"] - self._body_bytes)
        if (type(row["ordinal"]) is not int or row["ordinal"] != self._ordinal + 1
                or row["ordinal"] > self.manifest["chunk_count"]
                or type(row["body_bytes"]) is not int or row["body_bytes"] != expected_bytes
                or expected_bytes <= 0 or type(row["stored_bytes"]) is not int
                or not 1 <= row["stored_bytes"] <= artifacts.CHUNK_BYTES + 65536):
            raise ValueError("Artifact chunk order or bounds are invalid")
        prefix = artifacts.artifact_prefix(self.workspace_id, self.job_id)
        key = row["object_key"]
        namespace, _, tail = key.removeprefix(prefix).partition("/")
        if (not key.startswith(prefix) or re.fullmatch(r"attempt-[1-9][0-9]*", namespace) is None
                or tail != object_key(row["stored_hash"])
                or (self._attempt is not None and namespace != self._attempt)):
            raise ValueError("Artifact object is outside its private namespace")
        self._attempt = namespace
        # _page() has already closed its connection. The shared reader checks
        # encoded and decoded sizes/checksums with bounded decompression.
        body = read_archive_reference(row)
        self._check_lifetime()
        self._ordinal = row["ordinal"]
        self._body_bytes += len(body)
        self._hash.update(body)
        if self._ordinal == self.manifest["chunk_count"]:
            if self._page():
                raise ValueError("Artifact has unlisted chunks")
            self._finish()
        return body

    def _finish(self) -> None:
        if (self._ordinal != self.manifest["chunk_count"] or self._body_bytes != self.manifest["body_bytes"]
                or "sha256:" + self._hash.hexdigest() != self.manifest["content_hash"]):
            raise ValueError("Artifact content checksum or size mismatch")
        self._complete = True

    def _check_lifetime(self) -> None:
        if self._expired or time.time() >= self.deadline:
            self._expired = True
            self.close()
            raise TimeoutError("Artifact download lifetime expired")

    def _expire(self) -> None:
        with self._lock:
            if not self._closed:
                self._expired = True
            self.close()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._first = None
            self._timer.cancel()
            if self._on_close is not None:
                callback, self._on_close = self._on_close, None
                callback()


def release_download(archive: str, lease_id: str, workspace_id: str, release_memory: Callable[[], None]) -> None:
    try:
        with connection(archive, mode="rw") as conn:
            artifacts.release_download(conn, lease_id, workspace_id)
    except DatabaseError as exc:
        # The durable lease expires even when the archive cannot acknowledge close.
        _logger.warning("Artifact download lease release failed: %s", type(exc).__name__)
    finally:
        release_memory()
