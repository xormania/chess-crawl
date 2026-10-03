"""Framework-neutral requests and configurable application bounds."""

from __future__ import annotations

import os
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class ImportRequest:
    provider: str
    username: str
    since: int
    until: int
    max_games: int


@dataclass(frozen=True)
class CrawlRequest:
    provider: str
    username: str
    since: int
    until: int
    max_games: int
    max_depth: int
    max_users: int
    max_jobs: int


@dataclass(frozen=True)
class Limits:
    max_games: int = 1000
    max_depth: int = 2
    max_users: int = 100
    max_jobs: int = 200
    page_size: int = 100
    max_date_span_days: int = 366

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            minimum = 0 if field.name == "max_depth" else 1
            if type(value) is not int or value < minimum:
                raise ValueError(f"{field.name} must be an integer >= {minimum}")

    @classmethod
    def from_env(cls) -> "Limits":
        values = {}
        for field in fields(cls):
            name = f"CHESS_CRAWL_{field.name.upper()}"
            value = os.getenv(name)
            if value is not None:
                try:
                    values[field.name] = int(value)
                except ValueError as exc:
                    raise ValueError(f"{name} must be an integer") from exc
        return cls(**values)
