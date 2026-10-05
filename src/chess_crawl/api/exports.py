"""Finite private export spools that never hold a database while sending bytes."""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import TextIO

from chess_crawl.application import ValidationError


@dataclass(frozen=True)
class ExportLimits:
    max_rows: int = 100_000
    max_bytes: int = 64 * 1024 * 1024
    prepare_seconds: int = 60
    download_seconds: int = 300
    workspace_slots: int = 2

    @classmethod
    def from_env(cls) -> ExportLimits:
        defaults = cls()
        values = {}
        for field, maximum in (("max_rows", 10_000_000), ("max_bytes", 4 * 1024**3),
                               ("prepare_seconds", 600), ("download_seconds", 3600),
                               ("workspace_slots", 16)):
            name = "CHESS_CRAWL_EXPORT_" + field.upper()
            try:
                value = int(os.getenv(name, str(getattr(defaults, field))))
            except ValueError:
                raise ValueError(f"{name} must be a positive integer") from None
            if not 1 <= value <= maximum:
                raise ValueError(f"{name} must be between 1 and {maximum}")
            values[field] = value
        return cls(**values)


class ExportSpool:
    """A bounded iterator with explicit close even before its first read."""

    def __init__(self, file: TextIO) -> None:
        self.file = file

    def __iter__(self) -> ExportSpool:
        return self

    def __next__(self) -> str:
        if self.file.closed:
            raise StopIteration
        chunk = self.file.read(65536)
        if not chunk:
            self.close()
            raise StopIteration
        return chunk

    def close(self) -> None:
        self.file.close()


def check_export_bounds(*, limits: ExportLimits, rows: int, bytes_written: int, deadline: float) -> None:
    if rows > limits.max_rows or bytes_written > limits.max_bytes or time.monotonic() >= deadline:
        raise ValidationError(
            "The export exceeds the operator's row, byte or preparation-time limit; narrow the provider filter",
            code="export_limit_exceeded",
        )
