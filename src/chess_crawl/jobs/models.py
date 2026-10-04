"""Job value objects and enums for durable acquisition work."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


JobState = Literal["pending", "in_progress", "done", "error", "skipped", "blocked"]
JobKind = Literal[
    "fetch_user_profile",
    "fetch_user_stats",
    "fetch_user_games",
    "fetch_game_by_id",
    "crawl_opponents",
    "fetch_user_resource",
    "normalize_payload",
    "reprocess_archive",
]

JOB_STATES: tuple[str, ...] = ("pending", "in_progress", "done", "error", "skipped", "blocked")
JOB_KINDS: tuple[str, ...] = (
    "fetch_user_profile",
    "fetch_user_stats",
    "fetch_user_games",
    "fetch_game_by_id",
    "crawl_opponents",
    "fetch_user_resource",
    "normalize_payload",
    "reprocess_archive",
)

PROCESSING_JOB_KINDS: tuple[str, ...] = ("normalize_payload", "reprocess_archive")


@dataclass(frozen=True)
class DiscoveryJob:
    id: int | None
    provider: str
    kind: JobKind
    target: str
    params_json: str = "{}"
    state: JobState = "pending"
    priority: int = 100
    depth: int = 0
    dedup_key: str | None = None
    crawl_run_id: int | None = None
    parent_job_id: int | None = None
    attempts: int = 0
    enqueued_at: int | None = None
    started_at: int | None = None
    done_at: int | None = None
    reason: str | None = None
    retry_count: int = 0
    next_attempt_at: float | None = None
    revision: int = 0
    ownership_token: str | None = None
    owner_worker_id: str | None = None
    ownership_generation: int = 0


@dataclass(frozen=True)
class EnqueueResult:
    job_id: int
    inserted: bool
