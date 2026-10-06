"""Finite private export spools that never hold a database while sending bytes."""
from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from threading import RLock, Timer
from typing import TextIO, Protocol

from fastapi import HTTPException

from chess_crawl.application.export_limits import ExportLimits




class ExportChunks(Protocol):
    def __iter__(self) -> Iterator[str | bytes]: ...
    def __next__(self) -> str | bytes: ...
    def close(self) -> None: ...


class ExportCapacity:
    """Process-wide reserved storage, shared by all requests and workspaces.

    Reserve the maximum possible file size before creating a file. Keeping that
    reservation until close bounds both preparing and slow/unconsumed exports.
    Processes/replicas each have their own cap; no database connection is held.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._count = 0
        self._bytes = 0
        self._workspaces: dict[str, tuple[int, int]] = {}

    def reserve(self, limits: ExportLimits, *, workspace_id: str) -> Callable[[], None]:
        with self._lock:
            count, size = self._workspaces.get(workspace_id, (0, 0))
            if (count >= limits.workspace_outstanding_spools
                    or size + limits.max_bytes > limits.workspace_outstanding_bytes):
                raise HTTPException(
                    status_code=429, detail="This workspace's export temporary storage is at capacity",
                    headers={"Retry-After": "5"},
                )
            if (self._count >= limits.outstanding_spools
                    or self._bytes + limits.max_bytes > limits.outstanding_bytes):
                raise HTTPException(
                    status_code=429, detail="Export temporary storage is at capacity",
                    headers={"Retry-After": "5"},
                )
            self._count += 1
            self._bytes += limits.max_bytes
            self._workspaces[workspace_id] = (count + 1, size + limits.max_bytes)
        released = False

        def release() -> None:
            nonlocal released
            with self._lock:
                if not released:
                    self._count -= 1
                    self._bytes -= limits.max_bytes
                    count, size = self._workspaces[workspace_id]
                    if count == 1:
                        del self._workspaces[workspace_id]
                    else:
                        self._workspaces[workspace_id] = (count - 1, size - limits.max_bytes)
                    released = True
        return release


export_capacity = ExportCapacity()


class ExportSpool:
    """A bounded iterator whose storage reservation lasts until its file closes."""

    def __init__(self, file: TextIO, *, on_close: Callable[[], None] | None = None,
                 lifetime_seconds: float | None = None) -> None:
        self.file = file
        self._lock = RLock()
        self._on_close = on_close
        self._expired = False
        self._timer: Timer | None = None
        if lifetime_seconds is not None:
            self._timer = Timer(lifetime_seconds, self._expire)
            self._timer.daemon = True
            self._timer.start()

    def __iter__(self) -> ExportSpool:
        return self

    def __next__(self) -> str:
        with self._lock:
            if self._expired:
                raise TimeoutError("Export download lifetime expired")
            if self.file.closed:
                raise StopIteration
            chunk = self.file.read(65536)
            if not chunk:
                self.close()
                raise StopIteration
            return chunk

    def _expire(self) -> None:
        with self._lock:
            if not self.file.closed:
                self._expired = True
            self.close()

    def close(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            try:
                self.file.close()
            finally:
                if self._on_close is not None:
                    self._on_close()
                    self._on_close = None


def check_export_bounds(*, limits: ExportLimits, rows: int, bytes_written: int, deadline: float) -> None:
    from chess_crawl.application.archive_exports import check_export_bounds as check
    check(limits=limits, rows=rows, bytes_written=bytes_written, deadline=deadline, now=time.monotonic())
