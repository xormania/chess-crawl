"""Provider registry and client factory."""

from __future__ import annotations

from contextlib import AbstractContextManager, ExitStack
from collections.abc import Callable
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
    clock=None,
    stop_requested=None,
):
    if key == "chess.com":
        return ChessComClient(config.provider(key), transport=transport, sleeper=sleeper, clock=clock, stop_requested=stop_requested)
    if key == "lichess":
        return LichessClient(config.provider(key), transport=transport, sleeper=sleeper, clock=clock, stop_requested=stop_requested)
    raise UnknownProvider(key)


class ProviderSession(AbstractContextManager["ProviderSession"]):
    """Own reusable provider clients and their serial pacing for one worker.

    A session must be used serially. Ingestion borrows clients without closing
    them; leaving the session closes every client even when one close fails.
    """

    def __init__(self, config: Config, *, transport=None, sleeper=None, clock=None, stop_requested=None) -> None:
        self.config = config
        self._transport = transport
        self._sleeper = sleeper
        self._clock = clock
        self._stop_requested = stop_requested
        self._clients: dict[str, ChessComClient | LichessClient] = {}
        self._resources = ExitStack()
        self._closed = False
        self.before_request: Callable[[str], None] | None = None
        self.persist_deadline: Callable[[str, float, str], None] | None = None

    def client(self, provider: str):
        if self._closed:
            raise RuntimeError("provider session is closed")
        if provider not in self._clients:
            client = create_provider_client(
                provider, self.config, transport=self._transport,
                sleeper=self._sleeper, clock=self._clock, stop_requested=self._stop_requested,
            )
            self._resources.callback(client.close)
            self._clients[provider] = client
        client = self._clients[provider]
        if self.before_request is not None:
            client.http.before_request = lambda: self.before_request(provider) if self.before_request is not None else None
        if self.persist_deadline is not None:
            client.http.persist_deadline = lambda deadline, reason: (
                self.persist_deadline(provider, deadline, reason) if self.persist_deadline is not None else None
            )
        return self._clients[provider]

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._resources.close()

    def __enter__(self) -> ProviderSession:
        if self._closed:
            raise RuntimeError("provider session is closed")
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()
