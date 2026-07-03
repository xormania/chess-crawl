"""Provider-neutral DTOs and protocol definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Literal, Mapping, Protocol, Sequence, runtime_checkable


ProviderKey = Literal["chess.com", "lichess"]
EndpointType = Literal[
    "user_profile",
    "user_stats",
    "archives_index",
    "monthly_archive",
    "user_games_stream",
    "game",
    "games_by_ids",
    "import_dump",
]
Outcome = Literal["white_win", "black_win", "draw"]
Color = Literal["white", "black"]


@dataclass(frozen=True)
class FetchAttempt:
    provider: str
    endpoint_type: EndpointType
    url: str
    method: str
    status_code: int | None
    attempted_at: int
    attempt: int
    response_headers: Mapping[str, Any] = field(default_factory=dict)
    retry_after: int | None = None
    bytes_count: int | None = None
    duration_ms: int | None = None
    from_cache: bool = False


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
class ArchiveUnit:
    provider: str
    username: str
    unit_id: str
    url: str | None
    since: int | None
    until: int | None
    immutable: bool


@dataclass(frozen=True)
class GameFilters:
    rated: bool | None = None
    perf_types: tuple[str, ...] = ()
    color: str | None = None
    include_moves: bool = True
    include_clocks: bool = False
    include_evals: bool = False
    include_opening: bool = True
    max_games: int | None = None


@dataclass(frozen=True)
class FetchPolicy:
    min_delay_s: float
    supports_conditional: bool
    honor_retry_after: bool
    fixed_429_backoff_s: float | None
    max_retries: int

    def next_delay(self, status: int, retry_after: float | None = None) -> float:
        if status == 429 and self.fixed_429_backoff_s is not None:
            return self.fixed_429_backoff_s
        if status == 429 and self.honor_retry_after and retry_after is not None:
            return max(self.min_delay_s, retry_after)
        return self.min_delay_s


@runtime_checkable
class ProviderClient(Protocol):
    def key(self) -> str: ...

    def display_name(self) -> str: ...

    def user_agent(self) -> str: ...

    def get_user_profile(self, username: str) -> RawRecord: ...

    def get_user_stats(self, username: str) -> RawRecord: ...

    def list_archive_units(
        self,
        username: str,
        since: int | None,
        until: int | None,
    ) -> list[ArchiveUnit]: ...

    def iter_user_games(
        self,
        username: str,
        since: int | None,
        until: int | None,
        filters: GameFilters,
    ) -> Iterator[RawRecord]: ...

    def get_game(self, game_ref: str) -> RawRecord: ...

    def get_games_by_ids(self, refs: Sequence[str]) -> Iterator[RawRecord]: ...

    def policy(self) -> FetchPolicy: ...
