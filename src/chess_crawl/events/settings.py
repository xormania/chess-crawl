"""Settings for optional notifications and their bounded cleanup."""
from __future__ import annotations

from dataclasses import dataclass
import math

from chess_crawl.settings import dataclass_settings


@dataclass(frozen=True)
class EventSettings:
    enabled: bool = True
    retention_seconds: float = 86400.0
    cleanup_batch_size: int = 256

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool:
            raise ValueError("CHESS_CRAWL_EVENTS_ENABLED must be a boolean")
        if (isinstance(self.retention_seconds, bool) or not isinstance(self.retention_seconds, (int, float))
                or not math.isfinite(self.retention_seconds) or self.retention_seconds <= 0):
            raise ValueError("CHESS_CRAWL_EVENTS_RETENTION_SECONDS must be positive")
        if type(self.cleanup_batch_size) is not int or not 1 <= self.cleanup_batch_size <= 10000:
            raise ValueError("CHESS_CRAWL_EVENTS_CLEANUP_BATCH_SIZE must be between 1 and 10000")

    @classmethod
    def from_env(cls) -> EventSettings:
        return dataclass_settings(cls, prefix="CHESS_CRAWL_EVENTS_")
