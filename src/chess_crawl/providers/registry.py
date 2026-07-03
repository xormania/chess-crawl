"""Static provider registry for Phase 1."""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from chess_crawl.config import Config
from chess_crawl.providers.base import FetchPolicy
from chess_crawl.providers.chesscom.client import ChessComClient
from chess_crawl.providers.lichess.client import LichessClient


@dataclass(frozen=True)
class ProviderInfo:
    key: str
    name: str
    base_url: str
    docs_url: str
    id_model: str
    archive_unit: str
    timestamp_unit: str
    caching: str
    rate_limit: str
    auth: str
    single_game_by_id: bool
    policy: FetchPolicy


_PROVIDERS: dict[str, ProviderInfo] = {
    "chess.com": ProviderInfo(
        key="chess.com",
        name="Chess.com",
        base_url="https://api.chess.com/pub",
        docs_url="https://www.chess.com/news/view/published-data-api",
        id_model="numeric player_id",
        archive_unit="monthly archive",
        timestamp_unit="seconds",
        caching="ETag/304",
        rate_limit="Retry-After",
        auth="none",
        single_game_by_id=False,
        policy=FetchPolicy(
            min_delay_s=1.0,
            supports_conditional=True,
            honor_retry_after=True,
            fixed_429_backoff_s=None,
            max_retries=3,
        ),
    ),
    "lichess": ProviderInfo(
        key="lichess",
        name="Lichess",
        base_url="https://lichess.org/api",
        docs_url="https://lichess.org/api",
        id_model="id == username",
        archive_unit="date-range NDJSON",
        timestamp_unit="milliseconds",
        caching="content_hash",
        rate_limit="wait 60s",
        auth="optional token",
        single_game_by_id=True,
        policy=FetchPolicy(
            min_delay_s=1.5,
            supports_conditional=False,
            honor_retry_after=False,
            fixed_429_backoff_s=60.0,
            max_retries=3,
        ),
    ),
}


class UnknownProvider(KeyError):
    """Raised when a provider key is not registered."""


def known_keys() -> list[str]:
    return sorted(_PROVIDERS)


def list_provider_infos() -> list[ProviderInfo]:
    return [_PROVIDERS[key] for key in known_keys()]


def get_provider_info(key: str) -> ProviderInfo:
    try:
        return _PROVIDERS[key]
    except KeyError as exc:
        raise UnknownProvider(key) from exc


def create_provider_client(
    key: str,
    config: Config,
    *,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
):
    if key == "chess.com":
        return ChessComClient(config.provider(key), transport=transport, sleeper=sleeper)
    if key == "lichess":
        return LichessClient(config.provider(key), transport=transport, sleeper=sleeper)
    raise UnknownProvider(key)
