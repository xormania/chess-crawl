"""Framework-neutral bounds for synchronous export preparation and storage."""
from __future__ import annotations

from dataclasses import dataclass

from chess_crawl.settings import dataclass_settings


_LIMIT_MAXIMUMS = {
    "max_rows": 10_000_000, "max_bytes": 4 * 1024**3,
    "prepare_seconds": 600, "download_seconds": 3600, "workspace_slots": 16,
    "outstanding_spools": 128, "outstanding_bytes": 16 * 1024**3,
    "workspace_outstanding_spools": 128, "workspace_outstanding_bytes": 16 * 1024**3,
}


@dataclass(frozen=True)
class ExportLimits:
    max_rows: int = 100_000
    max_bytes: int = 64 * 1024 * 1024
    prepare_seconds: int = 60
    download_seconds: int = 300
    workspace_slots: int = 2
    outstanding_spools: int = 4
    outstanding_bytes: int = 256 * 1024 * 1024
    workspace_outstanding_spools: int = 2
    workspace_outstanding_bytes: int = 128 * 1024 * 1024

    def __post_init__(self) -> None:
        for field, maximum in _LIMIT_MAXIMUMS.items():
            value = getattr(self, field)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"CHESS_CRAWL_EXPORT_{field.upper()} must be an integer between 1 and {maximum}")
        if self.workspace_outstanding_spools >= self.outstanding_spools:
            raise ValueError("CHESS_CRAWL_EXPORT_WORKSPACE_OUTSTANDING_SPOOLS must be less than CHESS_CRAWL_EXPORT_OUTSTANDING_SPOOLS")
        if self.max_bytes > self.workspace_outstanding_bytes:
            raise ValueError("CHESS_CRAWL_EXPORT_MAX_BYTES must not exceed CHESS_CRAWL_EXPORT_WORKSPACE_OUTSTANDING_BYTES")
        if self.workspace_outstanding_bytes + self.max_bytes > self.outstanding_bytes:
            raise ValueError("CHESS_CRAWL_EXPORT_OUTSTANDING_BYTES must allow WORKSPACE_OUTSTANDING_BYTES plus one MAX_BYTES export")

    @classmethod
    def from_env(cls) -> ExportLimits:
        return dataclass_settings(cls, prefix="CHESS_CRAWL_EXPORT_")

