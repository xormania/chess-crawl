"""Shared records for provider acquisition and normalization."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping


ProviderKey = Literal["chess.com", "lichess"]
EndpointType = Literal[
    "user_profile",
    "user_stats",
    "user_resource",
    "archives_index",
    "monthly_archive",
    "user_games_stream",
    "game",
]
Outcome = Literal["white_win", "black_win", "draw"]
Color = Literal["white", "black"]


class ProviderRequestStopped(Exception):
    """Shutdown was requested before any provider request was sent."""


@dataclass(frozen=True)
class FetchAttempt:
    provider: str
    endpoint_type: EndpointType
    url: str
    method: str
    status_code: int | None
    attempted_at: int
    attempt: int
    request_headers: Mapping[str, Any] = field(default_factory=dict)
    response_headers: Mapping[str, Any] = field(default_factory=dict)
    retry_after: int | None = None
    bytes_count: int | None = None
    duration_ms: int | None = None
    from_cache: bool = False
    error_kind: str | None = None


@dataclass(frozen=True)
class RawRecord:
    provider: str
    endpoint_type: EndpointType
    request_url: str
    canonical_source_key: str
    request_params: Mapping[str, Any] = field(default_factory=dict)
    http_status: int = 200
    fetched_at: int = 0
    body: bytes | None = None
    media_type: str = "application/octet-stream"
    etag: str | None = None
    last_modified: str | None = None
    body_hash: str | None = None
    target_username: str | None = None
    target_game_id: str | None = None
    archive_unit: str | None = None
    response_headers: Mapping[str, Any] = field(default_factory=dict)
    fetch_attempts: tuple[FetchAttempt, ...] = ()
    owner_scope: str = "public"


@dataclass(frozen=True)
class NormalizedUser:
    provider: str
    provider_user_id: str | None
    username_normalized: str
    display_username: str
    title: str | None = None
    account_status_raw: str | None = None
    created_at: int | None = None
    last_seen_at: int | None = None
    country: str | None = None
    is_verified: bool | None = None


@dataclass(frozen=True)
class NormalizedParticipant:
    color: Color
    provider_user_id: str | None
    username_normalized: str | None
    display_username: str | None
    rating: int | None = None
    rating_diff: int | None = None
    rd: int | None = None
    result_raw: str | None = None
    is_ai: bool = False


@dataclass(frozen=True)
class NormalizedGame:
    provider: str
    provider_game_id: str | None
    canonical_url: str | None
    content_hash: str
    rated: bool | None
    variant_key: str
    variant_raw: str
    time_class: str
    time_control_raw: str | None
    outcome: Outcome | None
    is_live: bool
    status_raw: str | None
    end_time: int | None
    start_time: int | None
    white: NormalizedParticipant
    black: NormalizedParticipant
    eco: str | None = None
    opening_name: str | None = None
    opening_ply: int | None = None
    pgn: str | None = None


@dataclass(frozen=True)
class FetchPolicy:
    min_delay_s: float
    supports_conditional: bool
    honor_retry_after: bool
    fixed_429_backoff_s: float | None
    max_retries: int

    def next_delay(self, status: int, retry_after: float | None = None) -> float:
        if status == 429 and self.fixed_429_backoff_s is not None:
            return max(self.fixed_429_backoff_s, retry_after or 0)
        if status == 429 and self.honor_retry_after and retry_after is not None:
            return max(self.min_delay_s, retry_after)
        return self.min_delay_s
