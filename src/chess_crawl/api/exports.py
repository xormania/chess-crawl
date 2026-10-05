"""Finite private export spools that never hold a database while sending bytes."""
from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock, Timer
from typing import TextIO

from fastapi import HTTPException

from chess_crawl.application import ValidationError


@dataclass(frozen=True)
class ExportLimits:
    max_rows: int = 100_000
    max_bytes: int = 64 * 1024 * 1024
    prepare_seconds: int = 60
    download_seconds: int = 300
    workspace_slots: int = 2
    outstanding_spools: int = 4
    outstanding_bytes: int = 256 * 1024 * 1024

    @classmethod
    def from_env(cls) -> ExportLimits:
        defaults = cls()
        values = {}
        for field, maximum in (("max_rows", 10_000_000), ("max_bytes", 4 * 1024**3),
                               ("prepare_seconds", 600), ("download_seconds", 3600),
                               ("workspace_slots", 16), ("outstanding_spools", 128),
                               ("outstanding_bytes", 16 * 1024**3)):
            name = "CHESS_CRAWL_EXPORT_" + field.upper()
            try:
                value = int(os.getenv(name, str(getattr(defaults, field))))
            except ValueError:
                raise ValueError(f"{name} must be a positive integer") from None
            if not 1 <= value <= maximum:
                raise ValueError(f"{name} must be between 1 and {maximum}")
            values[field] = value
        if values["max_bytes"] > values["outstanding_bytes"]:
            raise ValueError("CHESS_CRAWL_EXPORT_MAX_BYTES must not exceed CHESS_CRAWL_EXPORT_OUTSTANDING_BYTES")
        return cls(**values)


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

    def reserve(self, limits: ExportLimits) -> Callable[[], None]:
        with self._lock:
            if (self._count >= limits.outstanding_spools
                    or self._bytes + limits.max_bytes > limits.outstanding_bytes):
                raise HTTPException(
                    status_code=429, detail="Export temporary storage is at capacity",
                    headers={"Retry-After": "5"},
                )
            self._count += 1
            self._bytes += limits.max_bytes
        released = False

        def release() -> None:
            nonlocal released
            with self._lock:
                if not released:
                    self._count -= 1
                    self._bytes -= limits.max_bytes
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
    if rows > limits.max_rows or bytes_written > limits.max_bytes or time.monotonic() >= deadline:
        raise ValidationError(
            "The export exceeds the operator's row, byte or preparation-time limit; narrow the provider filter",
            code="export_limit_exceeded",
        )
