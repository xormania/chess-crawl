"""Lichess public API client."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Mapping

import httpx

from chess_crawl.config import ProviderSettings
from chess_crawl.providers.base import ArchiveUnit, FetchPolicy, GameFilters, RawRecord
from chess_crawl.providers.http import HttpClient, HttpFetchResult
from chess_crawl.providers.lichess import endpoints


PROVIDER = "lichess"


class LichessClient:
    def __init__(
        self,
        settings: ProviderSettings,
        *,
        transport: httpx.BaseTransport | None = None,
        sleeper=None,
        timeout_s: float = 60.0,
    ) -> None:
        self.settings = settings
        self._policy = FetchPolicy(
            min_delay_s=settings.min_delay_s,
            supports_conditional=False,
            honor_retry_after=False,
            fixed_429_backoff_s=60.0,
            max_retries=settings.max_retries,
        )
        kwargs = {}
        if sleeper is not None:
            kwargs["sleeper"] = sleeper
        self.http = HttpClient(
            provider=PROVIDER,
            user_agent=settings.user_agent,
            policy=self._policy,
            timeout_s=timeout_s,
            transport=transport,
            **kwargs,
        )

    def key(self) -> str:
        return PROVIDER

    def display_name(self) -> str:
        return "Lichess"

    def user_agent(self) -> str:
        return self.settings.user_agent

    def policy(self) -> FetchPolicy:
        return self._policy

    def get_user_profile(self, username: str) -> RawRecord:
        normalized = _username(username)
        result = self.http.request(
            "GET",
            endpoints.user_profile(username),
            endpoint_type="user_profile",
            headers=self._headers("application/json"),
        )
        return _raw_record(
            result,
            endpoint_type="user_profile",
            canonical_source_key=f"lichess/user/{normalized}/profile",
            target_username=normalized,
        )

    def get_user_stats(self, username: str) -> RawRecord:
        return self.get_user_profile(username)

    def list_archive_units(
        self,
        username: str,
        since: int | None,
        until: int | None,
    ) -> list[ArchiveUnit]:
        unit_id = _range_unit_id(since, until)
        return [
            ArchiveUnit(
                provider=PROVIDER,
                username=_username(username),
                unit_id=unit_id,
                url=None,
                since=since,
                until=until,
                immutable=False,
            )
        ]

    def iter_user_games(
        self,
        username: str,
        since: int | None,
        until: int | None,
        filters: GameFilters,
    ):
        if filters.max_games is None or filters.max_games <= 0:
            raise ValueError("Lichess game iteration requires a positive max_games bound")
        yield self.get_user_games(username, since=since, until=until, limit=filters.max_games)

    def get_user_games(
        self,
        username: str,
        *,
        since: int | None,
        until: int | None,
        limit: int,
    ) -> RawRecord:
        normalized = _username(username)
        params = {
            "since": _seconds_to_ms(since),
            "until": _seconds_to_ms(until),
            "max": limit,
            "pgnInJson": "true",
            "opening": "true",
            "clocks": "false",
            "evals": "false",
        }
        result = self.http.request(
            "GET",
            endpoints.user_games(username, **params),
            endpoint_type="user_games_stream",
            headers=self._headers("application/x-ndjson"),
        )
        unit = _range_unit_id(since, until, limit)
        return _raw_record(
            result,
            endpoint_type="user_games_stream",
            canonical_source_key=f"lichess/games/user/{normalized}/{unit}",
            target_username=normalized,
            archive_unit=unit,
            request_params={key: value for key, value in params.items() if value is not None},
        )

    def get_game(self, game_ref: str) -> RawRecord:
        game_id = game_ref.strip()
        result = self.http.request(
            "GET",
            endpoints.game(game_id),
            endpoint_type="game",
            headers=self._headers("application/json"),
        )
        return _raw_record(
            result,
            endpoint_type="game",
            canonical_source_key=f"lichess/game/{game_id}",
            target_game_id=game_id,
        )

    def close(self) -> None:
        self.http.close()

    def _headers(self, accept: str) -> Mapping[str, str]:
        headers = {"Accept": accept}
        if self.settings.oauth_token:
            headers["Authorization"] = f"Bearer {self.settings.oauth_token}"
        return headers


def _raw_record(
    result: HttpFetchResult,
    *,
    endpoint_type,
    canonical_source_key: str,
    target_username: str | None = None,
    target_game_id: str | None = None,
    archive_unit: str | None = None,
    request_params: Mapping[str, object] | None = None,
) -> RawRecord:
    return RawRecord(
        provider=PROVIDER,
        endpoint_type=endpoint_type,
        request_url=result.url,
        canonical_source_key=canonical_source_key,
        request_params=request_params or {},
        http_status=result.status_code,
        fetched_at=result.fetched_at,
        body=result.body,
        media_type=result.content_type or ("application/x-ndjson" if endpoint_type == "user_games_stream" else "application/json"),
        etag=result.etag,
        last_modified=result.last_modified,
        body_hash=result.body_hash,
        target_username=target_username,
        target_game_id=target_game_id,
        archive_unit=archive_unit,
        response_headers=result.headers,
        fetch_attempts=result.attempts,
    )


def _username(username: str) -> str:
    return username.strip().lower()


def _seconds_to_ms(value: int | None) -> int | None:
    return None if value is None else value * 1000


def _range_unit_id(since: int | None, until: int | None, limit: int | None = None) -> str:
    def fmt(value: int | None) -> str:
        if value is None:
            return "open"
        return datetime.fromtimestamp(value, tz=UTC).strftime("%Y-%m-%d")

    suffix = f"-limit-{limit}" if limit is not None else ""
    return f"{fmt(since)}..{fmt(until)}{suffix}"
